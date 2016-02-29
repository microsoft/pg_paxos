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
#include "nodes/plannodes.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"


/* declarations for dynamic loading */
PG_MODULE_MAGIC;


/* executor functions forward declarations */
static PlannedStmt * PgPaxosPlanner(Query *parse, int cursorOptions,
									ParamListInfo boundParams);
static void ErrorIfQueryNotSupported(Query *queryTree);
static void PgPaxosExecutorStart(QueryDesc *queryDesc, int eflags);
static bool HasPaxosTable(List *rangeTableList);
static bool IsPgPaxosActive(void);
static char *DeterminePaxosGroup(List *rangeTableList);
static bool ExtractRangeTableEntryWalker(Node *node, List **rangeTableList);
static Oid ExtractTableOid(Node *node);
static void PrepareConsistentWrite(char *groupId, const char *sqlQuery);
static void PrepareConsistentRead(char *groupId);
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

/* unique node ID to use in Paxos */
char *PaxosNodeId = NULL;

/* consistency model for reads */
static int ReadConsistencyModel = STRONG_CONSISTENCY;

/* whether to allow mutable functions */
static bool AllowMutableFunctions = false;

/* saved hook values in case of unload */
static planner_hook_type PreviousPlannerHook = NULL;
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
	PreviousPlannerHook = planner_hook;
	planner_hook = PgPaxosPlanner;

	PreviousExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = PgPaxosExecutorStart;

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = PgPaxosProcessUtility;

	DefineCustomBoolVariable("pg_paxos.enabled",
							 "If enabled, pg_paxos handles queries on Paxos tables",
							 NULL, &PaxosEnabled, true, PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

	DefineCustomStringVariable("pg_paxos.node_id",
							   "Unique node ID to use in Paxos interactions", NULL,
							   &PaxosNodeId, NULL, PGC_USERSET, 0, NULL,
							   NULL, NULL);

	DefineCustomEnumVariable("pg_paxos.consistency_model",
							 "Consistency model to use for reads (strong, optimistic)",
							 NULL, &ReadConsistencyModel, STRONG_CONSISTENCY,
							 consistency_model_options, PGC_USERSET, 0, NULL, NULL,
							 NULL);

	DefineCustomBoolVariable("pg_paxos.allow_mutable_functions",
							 "If enabled, mutable functions in queries are allowed",
							 NULL, &AllowMutableFunctions, false, PGC_USERSET,
							 0, NULL, NULL, NULL);
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
}


/*
 * PgPaxosPlanner implements custom planning logic for queries involving
 * replicated tables.
 */
static PlannedStmt *
PgPaxosPlanner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *plannedStatement = NULL;
	CmdType commandType = query->commandType;
	List *rangeTableList = NIL;
	bool isModificationQuery = false;

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		isModificationQuery = true;
	}

	if (IsPgPaxosActive() && isModificationQuery)
	{
		ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

		if (HasPaxosTable(rangeTableList))
		{
			/* Build the DML query to log */
			Query *paxosQuery = copyObject(query);

			/* call standard planner first to have Query transformations performed */
			plannedStatement = standard_planner(paxosQuery, cursorOptions, boundParams);

			ErrorIfQueryNotSupported(paxosQuery);
		}
	}

	if (PreviousPlannerHook != NULL)
	{
		plannedStatement = PreviousPlannerHook(query, cursorOptions, boundParams);
	}
	else
	{
		plannedStatement = standard_planner(query, cursorOptions, boundParams);
	}

	return plannedStatement;
}


/*
 * IsPgPaxosActive returns whether pg_paxos should intercept queries.
 */
static bool
IsPgPaxosActive(void)
{
	bool missingOK = true;
	Oid extensionOid = InvalidOid;
	Oid metadataNamespaceOid = InvalidOid;
	Oid tableMetadataTableOid = InvalidOid;

	if (!PaxosEnabled)
	{
		return false;
	}

	extensionOid = get_extension_oid(PG_PAXOS_EXTENSION_NAME, missingOK);
	if (extensionOid == InvalidOid)
	{
		return false;
	}

	metadataNamespaceOid = get_namespace_oid("pgp_metadata", true);
	if (metadataNamespaceOid == InvalidOid)
	{
		return false;
	}

	tableMetadataTableOid = get_relname_relid("replicated_tables", metadataNamespaceOid);
	if (tableMetadataTableOid == InvalidOid)
	{
		return false;
	}

	return true;
}


/*
 * ExtractRangeTableEntryWalker walks over a query tree, and finds all range
 * table entries. For recursing into the query tree, this function uses the
 * query tree walker since the expression tree walker doesn't recurse into
 * sub-queries.
 */
static bool
ExtractRangeTableEntryWalker(Node *node, List **rangeTableList)
{
	bool walkIsComplete = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTable = (RangeTblEntry *) node;
		(*rangeTableList) = lappend(*rangeTableList, rangeTable);
	}
	else if (IsA(node, Query))
	{
		walkIsComplete = query_tree_walker((Query *) node, ExtractRangeTableEntryWalker,
										   rangeTableList, QTW_EXAMINE_RTES);
	}
	else
	{
		walkIsComplete = expression_tree_walker(node, ExtractRangeTableEntryWalker,
												rangeTableList);
	}

	return walkIsComplete;
}


/*
 * HasPaxosTable returns whether the given list of range tables contains
 * a Paxos table.
 */
static bool
HasPaxosTable(List *rangeTableList)
{
	ListCell *rangeTableCell = NULL;

	if (rangeTableList == NIL)
	{
		return false;
	}

	foreach(rangeTableCell, rangeTableList)
	{
		Oid rangeTableOid = ExtractTableOid((Node *) lfirst(rangeTableCell));
		if (IsPaxosTable(rangeTableOid))
		{
			return true;
		}
	}

	return false;
}


/*
 * ErrorIfQueryNotSupported checks if the query contains unsupported features,
 * and errors out if it does.
 */
static void
ErrorIfQueryNotSupported(Query *queryTree)
{
	if (!AllowMutableFunctions && contain_mutable_functions((Node *) queryTree))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan queries on replicated tables "
							   "containing mutable functions"),
						errhint("To allow mutable functions, set "
								"pg_paxos.allow_mutable_functions to on")));
	}
}


/*
 * PgPaxosExecutorStart blocks until the table is ready to read.
 */
static void
PgPaxosExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *plannedStmt = queryDesc->plannedstmt;
	List *rangeTableList = plannedStmt->rtable;
	CmdType commandType = queryDesc->operation;

	if (IsPgPaxosActive() && HasPaxosTable(rangeTableList))
	{
		char *sqlQuery = (char *) queryDesc->sourceText;
		char *groupId = NULL;
		bool topLevel = true;

		/* disallow transactions during paxos commands */
		PreventTransactionChain(topLevel, "paxos commands");

		groupId = DeterminePaxosGroup(rangeTableList);

		if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
			commandType == CMD_DELETE)
		{
			PrepareConsistentWrite(groupId, sqlQuery);
		}
		else
		{
			PrepareConsistentRead(groupId);
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
 * DeterminePaxosGroup determines the paxos group for the given list of relations.
 * If more than one Paxos group is used, this function errors out.
 */
static char *
DeterminePaxosGroup(List *rangeTableList)
{
	ListCell *rangeTableCell = NULL;
	char *queryGroupId = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		char *tableGroupId = NULL;

		Oid rangeTableOid = ExtractTableOid((Node *) lfirst(rangeTableCell));
		if (rangeTableOid == InvalidOid)
		{
			/* range table entry for something other than a relation */
			continue;
		}

		tableGroupId = PaxosTableGroup(rangeTableOid);
		if (tableGroupId == NULL)
		{
			char *relationName = get_rel_name(rangeTableOid);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						   errmsg("relation \"%s\" is not managed by pg_paxos",
						   relationName)));
		}
		else if (queryGroupId == NULL)
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
 * ExtractTableOid attempts to extract a table OID from a node.
 */
static Oid
ExtractTableOid(Node *node)
{
	Oid tableOid = InvalidOid;

	NodeTag nodeType = nodeTag(node);
	if (nodeType == T_RangeTblEntry)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) node;
		tableOid = rangeTableEntry->relid;
	}
	else if(nodeType == T_RangeVar)
	{
		RangeVar *rangeVar = (RangeVar *) node;
		bool failOK = true;
		tableOid = RangeVarGetRelid(rangeVar, NoLock, failOK);
	}

	return tableOid;
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
 * PrepareConsistentWrite prepares a write for execution. After
 * calling this function the write can be executed.
 */
static void
PrepareConsistentWrite(char *groupId, const char *sqlQuery)
{
	int64 loggedRoundId = 0;
	char *proposerId = GenerateProposerId();

	/*
	 * Log the current query through Paxos.
	 */
	loggedRoundId = PaxosAppend(groupId, proposerId, sqlQuery);
	CommandCounterIncrement();

	/*
	 * Mark the current query as applied and let the regular executor handle
	 * it. This change is rolled back if the current query fails.
	 */
	PaxosSetApplied(groupId, loggedRoundId);
	CommandCounterIncrement();
}


/*
 * PrepareConsistentRead prepares the replicated tables in a Paxos group
 * for a consistent read based on the configured consistency model.
 */
static void
PrepareConsistentRead(char *groupId)
{
	int64 maxRoundId = -1;
	int64 membershipVersion = PaxosMembershipVersion(groupId);
	int64 maxAppliedRoundId = PaxosMaxAppliedRound(groupId);
	char *proposerId = GenerateProposerId();

	if (ReadConsistencyModel == STRONG_CONSISTENCY)
	{
		maxRoundId = PaxosMaxAcceptedRound(groupId);
	}
	else /* ReadConsistencyModel == OPTIMISTIC_CONSISTENCY */
	{
		maxRoundId = PaxosMaxLocalConsensusRound(groupId);
	}

	while (maxAppliedRoundId < maxRoundId)
	{
		maxAppliedRoundId = PaxosApplyLog(groupId, proposerId, maxRoundId);
		CommandCounterIncrement();

		if (ReadConsistencyModel == STRONG_CONSISTENCY)
		{
			int64 newMembershipVersion = PaxosMembershipVersion(groupId);
			if(newMembershipVersion > membershipVersion)
			{
				/*
				 * If membership changed a lot, then the original maxRoundId may
				 * not have been accurate. Refresh it to make sure.
				 */
				maxRoundId = PaxosMaxAcceptedRound(groupId);
				membershipVersion = newMembershipVersion;
			}
		}
	}
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
	if (IsPgPaxosActive())
	{
		NodeTag statementType = nodeTag(parsetree);
		if (statementType == T_TruncateStmt)
		{
			TruncateStmt *truncateStatement = (TruncateStmt *) parsetree;
			List *relations = truncateStatement->relations;

			if (HasPaxosTable(relations))
			{
				char *groupId = DeterminePaxosGroup(relations);

				PrepareConsistentWrite(groupId, queryString);
			}
		}
		else if (statementType == T_IndexStmt)
		{
			IndexStmt *indexStatement = (IndexStmt *) parsetree;
			Oid tableOid = ExtractTableOid((Node *) indexStatement->relation);
			if (IsPaxosTable(tableOid))
			{
				char *groupId = PaxosTableGroup(tableOid);

				PrepareConsistentWrite(groupId, queryString);
			}
		}
		else if (statementType == T_AlterTableStmt)
		{
			AlterTableStmt *alterStatement = (AlterTableStmt *) parsetree;
			Oid tableOid = ExtractTableOid((Node *) alterStatement->relation);
			if (IsPaxosTable(tableOid))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("ALTER TABLE commands on paxos tables "
									   "are unsupported")));
			}
		}
		else if (statementType == T_CopyStmt)
		{
			CopyStmt *copyStatement = (CopyStmt *) parsetree;
			RangeVar *relation = copyStatement->relation;
			Node *rawQuery = copyObject(copyStatement->query);

			if (relation != NULL)
			{
				Oid tableOid = ExtractTableOid((Node *) relation);
				if (IsPaxosTable(tableOid))
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

				if (list_length(queryList) != 1)
				{
					ereport(ERROR, (errmsg("unexpected rewrite result")));
				}

				parsedQuery = (Query *) linitial(queryList);

				/* determine if the query runs on a paxos table */
				if (HasPaxosTable(parsedQuery->rtable))
				{
					char *groupId = DeterminePaxosGroup(parsedQuery->rtable);

					PrepareConsistentRead(groupId);
				}
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
