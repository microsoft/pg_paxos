/*-------------------------------------------------------------------------
 *
 * include/paxos_api.h
 *
 * Declarations for public functions that implement Paxos.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_PAXOS_API_H
#define PG_PAXOS_API_H


/* function declarations to run Paxos */
extern int64 PaxosAppend(char *groupId, char *proposerId, char* value);
extern int64 PaxosMaxAppliedRound(char *groupId);
extern int64 PaxosMaxLocalConsensusRound(char *groupId);
extern int64 PaxosMaxAcceptedRound(char *groupId);
extern int64 PaxosApplyLog(char *groupId, char *proposerId, int64 maxRoundId);
extern void PaxosSetApplied(char *groupId, int64 appliedRoundId);

#endif /* PG_PAXOS_API_H */
