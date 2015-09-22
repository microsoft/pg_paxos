/*-------------------------------------------------------------------------
 *
 * pg_paxos.h
 *
 * Declarations for public functions and types needed by the pg_paxos
 * extension.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_PAXOS_H
#define PG_PAXOS_H

/* extension name used to determine if extension has been created */
#define PG_PAXOS_EXTENSION_NAME "pg_paxos"

#define MAX_PAXOS_GROUP_ID_LENGTH 128

/* function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);

#endif /* PG_PAXOS_H */
