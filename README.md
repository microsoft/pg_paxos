# pg_paxos

This PostgreSQL extension provides a basic implementation of the Paxos algorithm in PL/pgSQL and table replication through Paxos. It is in a very early stage, consider it primarily for educational purposes. 

The table replication method logs every INSERT/UPDATE/DELETE on a table through Paxos. When a query is performed on a the table, pg_paxos first ensures that all preceding queries in the Paxos log have been applied, which guarantees read-your-writes consistency. By using the Paxos algorithm, pg_paxos is also robust to failure of a minority of nodes (1 out of 3, 2 out of 5, etc.). 

## Usage

pg_paxos depends on the dblink extension being installed.

To activate executor hooks, add pg_paxos to the shared_preload_libraries in postgresql.conf and restart postgres. It is also advisable to specify a unique node_id, which is needed to guarantee consistency in certain scenarios.

    # in postgresql.conf
    shared_preload_libraries = 'pg_paxos'
    pg_paxos.node_id = '<some-unique-name>'

An example of setting up a replicated table on 3 servers that run on the same host (ports 5432, 9700, 9701) is given below. After setting up the metadata, all writes to the coordinates table are replicated to the other nodes.

    -- run via psql on each node:
    CREATE TABLE coordinates (
        x int,
        y int
    );

    INSERT INTO pgp_metadata.group (group_id) VALUES ('mokka');
    INSERT INTO pgp_metadata.host VALUES ('mokka', '127.0.0.1', 5432, 0);
    INSERT INTO pgp_metadata.host VALUES ('mokka', '127.0.0.1', 9700, 0);
    INSERT INTO pgp_metadata.host VALUES ('mokka', '127.0.0.1', 9701, 0);
    INSERT INTO pgp_metadata.replicated_tables VALUES ('public','coordinates','mokka');
    
An example of how pg_paxos replicates the metadata:

    [marco@marco-desktop pg_paxos]$ psql
    psql (9.4.4)
    Type "help" for help.

    postgres=# INSERT INTO coordinates VALUES (1,1);
    INSERT 0 1
    postgres=# INSERT INTO coordinates VALUES (2,2);
    INSERT 0 1
    postgres=# SELECT * FROM coordinates ;
     x | y
    ---+---
     1 | 1
     2 | 2
    (2 rows)
    
    postgres=# \q
    [marco@marco-desktop pg_paxos]$ psql -p 9700
    psql (9.4.4)
    Type "help" for help.

    postgres=# SELECT * FROM coordinates ;
    NOTICE:  Executing: INSERT INTO coordinates VALUES (1,1);
    CONTEXT:  SQL statement "SELECT paxos_apply_log($1,$2,$3)"
    NOTICE:  Executing: INSERT INTO coordinates VALUES (2,2);
    CONTEXT:  SQL statement "SELECT paxos_apply_log($1,$2,$3)"
     x | y
    ---+---
     1 | 1
     2 | 2
    (2 rows)
    
    postgres=# UPDATE coordinates SET x = x * 10;
    UPDATE 2
    postgres=# \q
    [marco@marco-desktop pg_paxos]$ psql -p 9701
    psql (9.4.4)
    Type "help" for help.
    
    postgres=# SELECT * FROM coordinates ;
    NOTICE:  Executing: INSERT INTO coordinates VALUES (1,1);
    CONTEXT:  SQL statement "SELECT paxos_apply_log($1,$2,$3)"
    NOTICE:  Executing: INSERT INTO coordinates VALUES (2,2);
    CONTEXT:  SQL statement "SELECT paxos_apply_log($1,$2,$3)"
    NOTICE:  Executing: UPDATE coordinates SET x = x * 10;
    CONTEXT:  SQL statement "SELECT paxos_apply_log($1,$2,$3)"
     x  | y
    ----+---
     10 | 1
     20 | 2
    (2 rows)
    

Copyright © 2015 Citus Data, Inc.
