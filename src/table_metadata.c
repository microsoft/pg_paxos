/*-------------------------------------------------------------------------
 *
 * src/table_metadata.c
 *
 * This file contains functions to access and manage the paxos table
 * metadata.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "table_metadata.h"

#include <stddef.h>
#include <string.h>

#include "access/attnum.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "executor/spi.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/memnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"


/*
 * PaxosTableGroup looks up the group ID of a paxos table
 */
char *
PaxosTableGroup(Oid paxosTableOid)
{
	char *groupId = NULL;
	Datum groupIdDatum = 0;
	Oid argTypes[] = { OIDOID };
	Datum argValues[] = { paxosTableOid };
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;
	bool isNull = false;


	/*
	 * SPI_connect switches to its own memory context, which is destroyed by
	 * the call to SPI_finish. SPI_palloc is provided to allocate memory in
	 * the previous ("upper") context, but that is inadequate when we need to
	 * call other functions that themselves use the normal palloc (such as
	 * lappend). So we switch to the upper context ourselves as needed.
	 */
	MemoryContext upperContext = CurrentMemoryContext, oldContext = NULL;

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT group_id "
									  "FROM pgp_metadata.replicated_tables "
									  "WHERE table_oid = $1",
									  1, argTypes, argValues, NULL, false, 1);
	Assert(spiStatus == SPI_OK_SELECT);

	if (SPI_processed != 1)
	{
		char *relationName = get_rel_name(paxosTableOid);

		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("no group id is defined for relation \"%s\"",
							   relationName)));
	}

	groupIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
								 &isNull);

	oldContext = MemoryContextSwitchTo(upperContext);
	groupId = TextDatumGetCString(groupIdDatum);
	MemoryContextSwitchTo(oldContext);

	SPI_finish();

	return groupId;
}


/*
 * IsPaxosTable returns whether the specified table is paxos. It
 * returns false if the input is InvalidOid.
 */
bool
IsPaxosTable(Oid tableOid)
{
	bool isPaxosTable = false;
	Oid metadataNamespaceOid = get_namespace_oid("pgp_metadata", false);
	Oid tableNamespaceOid = get_rel_namespace(tableOid);
	Oid tableMetadataTableOid = InvalidOid;
	Oid argTypes[] = { OIDOID };
	Datum argValues[] = { tableOid };
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	/* short-circuit if the input is invalid */
	if (tableOid == InvalidOid)
	{
		return false;
	}

	/*
	 * The query below hits the replicated_tables table, so if we don't detect
	 * that and short-circuit, we'll get infinite recursion in the planner.
	 */
	tableMetadataTableOid = get_relname_relid("replicated_tables", metadataNamespaceOid);
	if (IsSystemNamespace(tableNamespaceOid) ||
		tableOid == tableMetadataTableOid ||
		tableMetadataTableOid == InvalidOid)
	{
		return false;
	}

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT 1 "
									  "FROM pgp_metadata.replicated_tables "
									  "WHERE table_oid = $1",
									  1, argTypes, argValues, NULL, true, 1);
	Assert(spiStatus == SPI_OK_SELECT);

	isPaxosTable = (SPI_processed == 1);

	SPI_finish();

	return isPaxosTable;
}


/*
 *  PaxosTablesExist returns true if pg_paxos has a record of any
 *  paxos tables; otherwise this function returns false.
 */
bool
PaxosTablesExist(void)
{
	bool paxosTablesExist = false;
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	SPI_connect();

	spiStatus = SPI_exec("SELECT NULL FROM pgp_metadata.replicated_tables", 1);
	Assert(spiStatus == SPI_OK_SELECT);

	paxosTablesExist = (SPI_processed > 0);

	SPI_finish();

	return paxosTablesExist;
}
