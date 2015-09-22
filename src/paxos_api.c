/*-------------------------------------------------------------------------
 *
 * src/paxos_api.c
 *
 * This file contains functions to run Paxos.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "paxos_api.h"

#include <stddef.h>
#include <string.h>

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"


int64
PaxosLog(char *groupId, char *proposerId, char* value)
{
	int roundId = -1;
	Datum roundIdDatum = 0;
	Oid argTypes[] = {
		TEXTOID,
		TEXTOID,
		TEXTOID
	};
	Datum argValues[] = {
		CStringGetTextDatum(proposerId),
		CStringGetTextDatum(groupId),
		CStringGetTextDatum(value)
	};
	bool isNull = false;
	
	SPI_connect();

	SPI_execute_with_args("SELECT paxos_log($1,$2,$3)",
						  3, argTypes, argValues, NULL, false, 1);

	roundIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
								 &isNull);
	roundId = DatumGetInt64(roundIdDatum);

	SPI_finish();

	return roundId;
}


int64
PaxosApplyLog(char *groupId, char *proposerId, int64 maxRoundId)
{
	int roundId = -1;
	Datum roundIdDatum = 0;
	Oid argTypes[] = {
		TEXTOID,
		TEXTOID,
		INT8OID
	};
	Datum argValues[] = {
		CStringGetTextDatum(proposerId),
		CStringGetTextDatum(groupId),
		Int64GetDatum(maxRoundId)
	};
	bool isNull = false;
	
	SPI_connect();

	SPI_execute_with_args("SELECT paxos_apply_log($1,$2,$3)",
						  3, argTypes, argValues, NULL, false, 1);

	roundIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
								 &isNull);
	roundId = DatumGetInt64(roundIdDatum);

	SPI_finish();

	return roundId;
}


void
PaxosSetApplied(char *groupId, int64 appliedRoundId)
{
	Oid argTypes[] = {
		TEXTOID,
		INT8OID
	};
	Datum argValues[] = {
		CStringGetTextDatum(groupId),
		Int64GetDatum(appliedRoundId)
	};
	
	SPI_connect();

	SPI_execute_with_args("UPDATE pgp_metadata.group SET last_applied_round = $2 WHERE group_id = $1 ",
						  2, argTypes, argValues, NULL, false, 1);

	SPI_finish();
}
