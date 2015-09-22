/*-------------------------------------------------------------------------
 *
 * src/pg_paxos.c
 *
 * This file contains executor hooks that use the Paxos API to do
 * replicated writes and consistent reads on PostgreSQL tables that are
 * replicated across multiple nodes.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "plpgsql.h"

#include "paxos_api.h"
#include "pg_paxos.h"
#include "table_metadata.h"

#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "lib/stringinfo.h"
#include "nodes/memnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"


/* unique node ID */
static char *PaxosNodeId = NULL;

/* whether writes go through Paxos */
static bool PaxosEnabled = true;


/* executor functions forward declarations */
static void PgPaxosExecutorStart(QueryDesc *queryDesc, int eflags);
static bool IsPgPaxosQuery(QueryDesc *queryDesc);
static char *DeterminePaxosGroup(QueryDesc *queryDesc);
static char* GenerateProposerId(void);
static void PgPaxosProcessUtility(Node *parsetree, const char *queryString,
								  ProcessUtilityContext context, ParamListInfo params,
								  DestReceiver *dest, char *completionTag);
static void ErrorOnDropIfPaxosTablesExist(DropStmt *dropStatement);
static void FinishPaxosTransaction(XactEvent event, void *arg);

/* declarations for dynamic loading */
PG_MODULE_MAGIC;


/* saved hook values in case of unload */
static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;


/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void
_PG_init(void)
{
	PreviousExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = PgPaxosExecutorStart;

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = PgPaxosProcessUtility;

	DefineCustomStringVariable("pg_paxos.node_id",
							   "Unique node ID to use in Paxos interactions", NULL,
							   &PaxosNodeId, NULL, PGC_USERSET, 0, NULL,
							   NULL, NULL);
}


/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void
_PG_fini(void)
{
	ProcessUtility_hook = PreviousProcessUtilityHook;
	ExecutorStart_hook = PreviousExecutorStartHook;

	RegisterXactCallback(FinishPaxosTransaction, NULL);
}


/*
 * PgPaxosExecutorStart blocks until the table is ready to read.
 */
static void
PgPaxosExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (PaxosEnabled && IsPgPaxosQuery(queryDesc))
	{
		CmdType commandType = queryDesc->operation;
		char *sqlQuery = (char *) queryDesc->sourceText;
		char *groupId = NULL; 
		char *proposerId = NULL;
		bool isWrite = false;
		int64 syncRoundId = -1;

		groupId = DeterminePaxosGroup(queryDesc);
		proposerId = GenerateProposerId();

		if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
			commandType == CMD_DELETE)
		{
			isWrite = true;
		}

		if (isWrite)
		{
			/*
 			 * Log the current query through Paxos. In the future, we will want to
 			 * separately log a commit record, since we'd prefer not logging queries
 			 * that fail.
 			 */
			syncRoundId = PaxosLog(groupId, proposerId, sqlQuery) - 1;
			CommandCounterIncrement();
		}
	
		/*
		 * The PaxosApplyLog function will apply all SQL queries in the log
		 * on which there is consensus, except the current query (hence the
		 * minus 1 above). 
		 */
		PaxosEnabled = false;
		PaxosApplyLog(groupId, proposerId, syncRoundId);
		PaxosEnabled = true;
		CommandCounterIncrement();

		if (isWrite)
		{
			/* 
			 * Mark the current query as applied and let the regular executor handle
			 * it. This change be rolled back if the current query fails.
			 */
			PaxosSetApplied(groupId, syncRoundId + 1);
			CommandCounterIncrement();
		}

		queryDesc->snapshot->curcid = GetCurrentCommandId(false);
	}

	/* call into the standard executor start, or hook if set */
	if (PreviousExecutorStartHook != NULL)
	{
		PreviousExecutorStartHook(queryDesc, eflags);
	}
	else
	{
		standard_ExecutorStart(queryDesc, eflags);
	}
}



/*
 * IsPgPaxosQuery returns whether the given query should be handled by pg_paxos.
 */
static bool
IsPgPaxosQuery(QueryDesc *queryDesc)
{
	PlannedStmt *plannedStmt = queryDesc->plannedstmt;
	List *rangeTableList = plannedStmt->rtable;
	ListCell *rangeTableCell = NULL;

	/* if the extension isn't created, it is never a Paxos query */
	bool missingOK = true;
	Oid extensionOid = get_extension_oid(PG_PAXOS_EXTENSION_NAME, missingOK);
	if (extensionOid == InvalidOid)
	{
		return false;
	}

	if (rangeTableList == NIL)
	{
		return false;
	}

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (IsPaxosTable(rangeTableEntry->relid))
		{
			return true;
		}
	}

	return false;
}


/*
 * DeterminePaxosGroup determines the paxos group for the given list of
 * tables. If more than one Paxos group is used, this function errors out.
 */
static char *
DeterminePaxosGroup(QueryDesc *queryDesc)
{
	PlannedStmt *plannedStmt = queryDesc->plannedstmt;
	List *rangeTableList = plannedStmt->rtable;
	ListCell *rangeTableCell = NULL;
	char *queryGroupId = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		Oid rangeTableOid = rangeTableEntry->relid;
		char *tableGroupId = PaxosTableGroup(rangeTableOid);

		if (queryGroupId == NULL)
		{
			queryGroupId = tableGroupId;
		}
		else
		{
			int compareResult = strncmp(tableGroupId, queryGroupId,
										MAX_PAXOS_GROUP_ID_LENGTH);
			if (compareResult != 0)
			{
				ereport(ERROR, (errmsg("cannot run queries spanning more than a single "
									   "Paxos group.")));
			}
		}
	}

	return queryGroupId;
}


static char*
GenerateProposerId(void)
{
	StringInfo proposerId = makeStringInfo();
	MyProcPid = getpid();

	if (PaxosNodeId != NULL)
	{
		appendStringInfo(proposerId, "%s/", PaxosNodeId);
	}

	appendStringInfo(proposerId, "%d/%d", MyProcPid, GetTopTransactionId());

	return proposerId->data;
}


/*
 * PgPaxosProcessUtility intercepts utility statements and errors out for
 * unsupported utility statements on paxos tables.
 */
static void
PgPaxosProcessUtility(Node *parsetree, const char *queryString,
					  ProcessUtilityContext context, ParamListInfo params,
					  DestReceiver *dest, char *completionTag)
{
	NodeTag statementType = nodeTag(parsetree);
	if (statementType == T_DropStmt)
	{
		DropStmt *dropStatement = (DropStmt *) parsetree;
		ErrorOnDropIfPaxosTablesExist(dropStatement);
	}

	if (PreviousProcessUtilityHook != NULL)
	{
		PreviousProcessUtilityHook(parsetree, queryString, context,
								   params, dest, completionTag);
	}
	else
	{
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);
	}
}


/*
 * ErrorOnDropIfPaxosTablesExist prevents attempts to drop the pg_paxos§
 * extension if any paxos tables still exist. This prevention will be
 * circumvented if the user includes the CASCADE option in their DROP command,
 * in which case a notice is printed and the DROP is allowed to proceed.
 */
static void
ErrorOnDropIfPaxosTablesExist(DropStmt *dropStatement)
{
	Oid extensionOid = InvalidOid;
	bool missingOK = true;
	ListCell *dropStatementObject = NULL;
	bool paxosTablesExist = false;

	/* we're only worried about dropping extensions */
	if (dropStatement->removeType != OBJECT_EXTENSION)
	{
		return;
	}

	extensionOid = get_extension_oid(PG_PAXOS_EXTENSION_NAME, missingOK);
	if (extensionOid == InvalidOid)
	{
		/*
		 * Exit early if the extension has not been created (CREATE EXTENSION).
		 * This check is required because it's possible to load the hooks in an
		 * extension without formally "creating" it.
		 */
		return;
	}

	/* nothing to do if no paxos tables are present */
	paxosTablesExist = PaxosTablesExist();
	if (!paxosTablesExist)
	{
		return;
	}

	foreach(dropStatementObject, dropStatement->objects)
	{
		List *objectNameList = lfirst(dropStatementObject);
		char *objectName = NameListToString(objectNameList);

		/* we're only concerned with the pg_paxos extension */
		if (strncmp(PG_PAXOS_EXTENSION_NAME, objectName, NAMEDATALEN) != 0)
		{
			continue;
		}

		if (dropStatement->behavior != DROP_CASCADE)
		{
			/* without CASCADE, error if paxos tables present */
			ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
							errmsg("cannot drop extension " PG_PAXOS_EXTENSION_NAME
								   " because other objects depend on it"),
							errdetail("Existing paxos tables depend on extension "
									  PG_PAXOS_EXTENSION_NAME),
							errhint("Use DROP ... CASCADE to drop the dependent "
									"objects too.")));
		}
	}
}


static void
FinishPaxosTransaction(XactEvent event, void *arg)
{
	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT)
	{
		return;
	}
	
	PaxosEnabled = true;
}
