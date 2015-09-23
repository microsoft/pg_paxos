/*-------------------------------------------------------------------------
 *
 * include/table_metadata.h
 *
 * Declarations for public functions and types related to metadata handling.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_PAXOS_TABLE_METADATA_H
#define PG_PAXOS_TABLE_METADATA_H

#include "postgres.h"
#include "c.h"


/* function declarations to access and manipulate the metadata */
extern char *PaxosTableGroup(Oid paxosTableOid);
extern bool IsPaxosTable(Oid tableId);
extern bool PaxosTablesExist(void);

#endif /* PG_PAXOS_TABLE_METADATA_H */
