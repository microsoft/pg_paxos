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

#include "pg_paxos.h"
#include "table_metadata.h"

#include <stddef.h>
#include <string.h>

#include "access/attnum.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "executor/spi.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "nodes/makefuncs.h"
#include "nodes/memnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/tqual.h"


/* human-readable names for addressing columns of partition table */
#define REPLICATED_TABLES_TABLE_ATTRIBUTE_COUNT 2
#define ATTR_NUM_REPLICATED_TABLES_RELATION_ID 1
#define ATTR_NUM_REPLICATED_TABLES_GROUP 2


/*
 * PaxosTableGroup looks up the group ID of a paxos table
 */
char *
PaxosTableGroup(Oid paxosTableOid)
{
	char *groupId = NULL;
	RangeVar *heapRangeVar = NULL;
	Relation heapRelation = NULL;
	HeapScanDesc scanDesc = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	heapRangeVar = makeRangeVar(PG_PAXOS_METADATA_SCHEMA_NAME,
								REPLICATED_TABLES_TABLE_NAME, -1);
	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_REPLICATED_TABLES_RELATION_ID, InvalidStrategy,
				F_OIDEQ, ObjectIdGetDatum(paxosTableOid));

	scanDesc = heap_beginscan(heapRelation, SnapshotSelf, scanKeyCount, scanKey);

	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		bool isNull = false;

		Datum groupIdDatum = heap_getattr(heapTuple,
										  ATTR_NUM_REPLICATED_TABLES_GROUP,
										  tupleDescriptor, &isNull);
		groupId = TextDatumGetCString(groupIdDatum);
	}
	else
	{
		groupId = NULL;
	}

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);

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
	RangeVar *heapRangeVar = NULL;
	Relation heapRelation = NULL;
	HeapScanDesc scanDesc = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	if (tableOid == InvalidOid)
	{
		return false;
	}

	heapRangeVar = makeRangeVar(PG_PAXOS_METADATA_SCHEMA_NAME,
								REPLICATED_TABLES_TABLE_NAME, -1);
	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_REPLICATED_TABLES_RELATION_ID, InvalidStrategy,
				F_OIDEQ, ObjectIdGetDatum(tableOid));

	scanDesc = heap_beginscan(heapRelation, SnapshotSelf, scanKeyCount, scanKey);

	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);

	isPaxosTable = HeapTupleIsValid(heapTuple);

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);

	return isPaxosTable;
}
