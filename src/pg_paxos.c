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


/* declarations for dynamic loading */
PG_MODULE_MAGIC;


/* executor functions forward declarations */
static void PgPaxosExecutorStart(QueryDesc *queryDesc, int eflags);
static bool HasPaxosTable(List *rangeTableList);
static char *DeterminePaxosGroup(List *rangeTableList);
static void PrepareConsistentRead(char *groupId, char *proposerId);
static void FinishPaxosTransaction(XactEvent event, void *arg);
static void PgPaxosProcessUtility(Node *parsetree, const char *queryString,
								  ProcessUtilityContext context, ParamListInfo params,
								  DestReceiver *dest, char *completionTag);


/* Enumeration that represents the consistency model to use */
typedef enum
{
	STRONG_CONSISTENCY = 0,
	OPTIMISTIC_CONSISTENCY = 1

} ConsistencyModel;


/* configuration options */
static const struct config_enum_entry consistency_model_options[] = {
	{"strong", STRONG_CONSISTENCY, false},
	{"optimistic", OPTIMISTIC_CONSISTENCY, false},
	{NULL, 0, false}
};

/* whether writes go through Paxos */
static bool PaxosEnabled = true;

/* saved hook values in case of unload */
static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

/**/
static int ReadConsistencyModel = STRONG_CONSISTENCY;



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

	DefineCustomEnumVariable("pg_paxos.consistency_model",
							 "Consistency model to use for reads (strong, optimistic)", 
							 NULL, &ReadConsistencyModel, STRONG_CONSISTENCY,
							 consistency_model_options, PGC_USERSET, 0, NULL, NULL,
							 NULL);
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
	PlannedStmt *plannedStmt = queryDesc->plannedstmt;
	List *rangeTableList = plannedStmt->rtable;

	if (PaxosEnabled && HasPaxosTable(rangeTableList))
	{
		CmdType commandType = queryDesc->operation;
		char *sqlQuery = (char *) queryDesc->sourceText;
		char *groupId = NULL; 
		char *proposerId = NULL;
		bool topLevel = true;

		/* disallow transactions during paxos commands */
		PreventTransactionChain(topLevel, "paxos commands");

		groupId = DeterminePaxosGroup(rangeTableList);
		proposerId = GenerateProposerId();

		if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
			commandType == CMD_DELETE)
		{
			int64 loggedRoundId = 0;

			/*
			 * Log the current query through Paxos.
			 */
			PaxosEnabled = false;
			loggedRoundId = PaxosAppend(groupId, proposerId, sqlQuery);
			PaxosEnabled = true;
			CommandCounterIncrement();

			/* 
			 * Mark the current query as applied and let the regular executor handle
			 * it. This change is rolled back if the current query fails.
			 */
			PaxosSetApplied(groupId, loggedRoundId);
			CommandCounterIncrement();
		}
		else
		{
			PrepareConsistentRead(groupId, proposerId);
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
 * HasPaxosTable returns whether the given list of range tables contains
 * a Paxos table.
 */
static bool
HasPaxosTable(List *rangeTableList)
{
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
 * DeterminePaxosGroup determines the paxos group for the given query.
 * If more than one Paxos group is used, this function errors out.
 */
static char *
DeterminePaxosGroup(List *rangeTableList)
{
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


/*
 * GenerateProposerId attempts to generate a globally unique proposer ID.
 * It mainly relies on the pg_paxos.node_id setting to distinguish hosts,
 * and appends the process ID and transaction ID to ensure local uniqueness.
 */
char *
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
 * PrepareConsistentRead prepares the replicated tables in a Paxos group
 * for a consistent read based on the configured consistency model.
 */
static void
PrepareConsistentRead(char *groupId, char *proposerId)
{
	int64 maxRoundId = -1;
	int64 maxAppliedRoundId = PaxosMaxAppliedRound(groupId);

	if (ReadConsistencyModel == STRONG_CONSISTENCY)
	{
		maxRoundId = PaxosMaxAcceptedRound(groupId);
	}
	else /* ReadyConsistencyModel == OPTIMISTIC_CONSISTENCY */
	{
		maxRoundId = PaxosMaxLocalConsensusRound(groupId);
	}

	if (maxAppliedRoundId < maxRoundId)
	{
		PaxosEnabled = false;
		PaxosApplyLog(groupId, proposerId, maxRoundId);
		PaxosEnabled = true;
		CommandCounterIncrement();
	}
}


/*
 * FinishPaxosTransaction is called at the end of a transaction and
 * mainly serves to reset the PaxosEnabled flag in case of failure.
 */
static void
FinishPaxosTransaction(XactEvent event, void *arg)
{
	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT)
	{
		return;
	}
	
	PaxosEnabled = true;
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
	if (statementType == T_CopyStmt)
	{
		CopyStmt *copyStatement = (CopyStmt *) parsetree;
		RangeVar *relation = copyStatement->relation;
		Node *rawQuery = copyObject(copyStatement->query);

		if (relation != NULL)
		{
			bool failOK = true;
			Oid tableId = RangeVarGetRelid(relation, NoLock, failOK);
			bool isPaxosTable = false;

			Assert(rawQuery == NULL);

			isPaxosTable = IsPaxosTable(tableId);
			if (isPaxosTable)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("COPY commands on paxos tables "
									   "are unsupported")));
			}
		}
		else if (rawQuery != NULL)
		{
			Query *parsedQuery = NULL;
			List *queryList = pg_analyze_and_rewrite(rawQuery, queryString,
													 NULL, 0);

			Assert(relation == NULL);

			if (list_length(queryList) != 1)
			{
				ereport(ERROR, (errmsg("unexpected rewrite result")));
			}

			parsedQuery = (Query *) linitial(queryList);

			/* determine if the query runs on a paxos table */
			if (HasPaxosTable(parsedQuery->rtable))
			{
				char *groupId = DeterminePaxosGroup(parsedQuery->rtable);
				char *proposerId = GenerateProposerId();

				PrepareConsistentRead(groupId, proposerId);
			}
		}
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
