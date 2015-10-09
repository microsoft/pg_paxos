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
PaxosAppend(char *groupId, char *proposerId, char* value)
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

	SPI_execute_with_args("SELECT paxos_apply_and_append($1,$2,$3)",
						  3, argTypes, argValues, NULL, false, 1);

	roundIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
								 &isNull);
	roundId = DatumGetInt64(roundIdDatum);

	SPI_finish();

	return roundId;
}


int64
PaxosMaxAppliedRound(char *groupId)
{
	int roundId = -1;
	Datum roundIdDatum = 0;
	Oid argTypes[] = {
		TEXTOID
	};
	Datum argValues[] = {
		CStringGetTextDatum(groupId)
	};
	bool isNull = false;

	SPI_connect();

	SPI_execute_with_args("SELECT last_applied_round FROM pgp_metadata.group "
						  "WHERE group_id = $1",
						  1, argTypes, argValues, NULL, false, 1);

	roundIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
								 &isNull);
	roundId = DatumGetInt64(roundIdDatum);

	SPI_finish();

	return roundId;
}


int64
PaxosMaxLocalConsensusRound(char *groupId)
{
	int roundId = -1;
	Datum roundIdDatum = 0;
	Oid argTypes[] = {
		TEXTOID
	};
	Datum argValues[] = {
		CStringGetTextDatum(groupId)
	};
	bool isNull = false;

	SPI_connect();

	SPI_execute_with_args("SELECT max(round_id) FROM pgp_metadata.round "
						  "WHERE group_id = $1 AND consensus",
						  1, argTypes, argValues, NULL, false, 1);

	roundIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
								 &isNull);

	if (!isNull)
	{
		roundId = DatumGetInt64(roundIdDatum);
	}
	else
	{
		roundId = -1;
	}

	SPI_finish();

	return roundId;
}


int64
PaxosMaxAcceptedRound(char *groupId)
{
	int roundId = -1;
	Datum roundIdDatum = 0;
	Oid argTypes[] = {
		TEXTOID
	};
	Datum argValues[] = {
		CStringGetTextDatum(groupId)
	};
	bool isNull = false;

	SPI_connect();

	SPI_execute_with_args("SELECT paxos_max_group_round($1,true)",
						  1, argTypes, argValues, NULL, false, 1);

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
