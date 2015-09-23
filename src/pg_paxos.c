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
static void FinishPaxosTransaction(XactEvent event, void *arg);

/* declarations for dynamic loading */
PG_MODULE_MAGIC;


/* saved hook values in case of unload */
static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;


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

		groupId = DeterminePaxosGroup(queryDesc);
		proposerId = GenerateProposerId();

		if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
			commandType == CMD_DELETE)
		{
			/*
 			 * Log the current query through Paxos. In the future, we will want to
 			 * separately log a commit record, since we'd prefer not logging queries
 			 * that fail.
 			 */
			int64 loggedRoundId = PaxosLog(groupId, proposerId, sqlQuery);
			CommandCounterIncrement();
	
			/*
			 * The PaxosApplyLog function will apply all SQL queries in the log
			 * on which there is consensus, except the current query (hence the
			 * minus 1).
			 */
			PaxosEnabled = false;
			PaxosApplyLog(groupId, proposerId, false, loggedRoundId - 1);
			PaxosEnabled = true;
			CommandCounterIncrement();

			/* 
			 * Mark the current query as applied and let the regular executor handle
			 * it. This change be rolled back if the current query fails.
			 */
			PaxosSetApplied(groupId, loggedRoundId);
			CommandCounterIncrement();
		}
		else
		{
			/*
			 * The PaxosApplyLog function will apply all SQL queries in the log
			 * on which there is consensus.
			 */
			PaxosEnabled = false;
			PaxosApplyLog(groupId, proposerId, true, 0);
			PaxosEnabled = true;
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
 * DeterminePaxosGroup determines the paxos group for the given query.
 * If more than one Paxos group is used, this function errors out.
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


/*
 * GenerateProposerId attempts to generate a globally unique proposer ID.
 * It mainly relies on the pg_paxos.node_id setting to distinguish hosts,
 * and appends the process ID and transaction ID to ensure local uniqueness.
 */
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
