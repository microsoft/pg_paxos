# pg_paxos

This PostgreSQL extension provides a basic implementation of the Paxos algorithm in PL/pgSQL and basic table replication through Paxos. It is in an early stage, consider it primarily for educational purposes. 

pg_paxos can be used to replicate a table across multiple PostgreSQL servers. Every INSERT/UPDATE/DELETE on a replicated table is logged through Paxos. When a query is performed on the table, pg_paxos first ensures that all preceding queries in the Paxos log have been applied, providing strong consistency. By using the Paxos algorithm, pg_paxos is also robust to failure of a minority of nodes (e.g. 2 out of 5). 

## Installation

The easiest way to install pg_paxos is to build the sources from GitHub.

    git clone https://github.com/citusdata/pg_paxos.git
    cd pg_paxos
    PATH=/usr/local/pgsql/bin/:$PATH make
    sudo PATH=/usr/local/pgsql/bin/:$PATH make install

pg_paxos requires the dblink extension to be installed. After installing both extensions run:

    -- run via psql on each node:
    CREATE EXTENSION dblink;
    CREATE EXTENSION pg_paxos;
    
To do table replication, pg_paxos uses PostgreSQL's executor hooks. To activate executor hooks, add pg_paxos to the shared_preload_libraries in postgresql.conf and restart postgres. It is also advisable to specify a unique node_id, which is needed to guarantee consistency in certain scenarios.

    # in postgresql.conf
    shared_preload_libraries = 'pg_paxos'
    pg_paxos.node_id = '<some-unique-name>'

## Usage
    
The following query appends value 'primary = ip-10-11-204-31.ec2.internal' to the Multi-Paxos log for the group ha_postgres:

    SELECT * FROM paxos_append(
                    current_proposer_id := 'node-a/1247',
                    current_group_id := 'ha_postgres',
                    proposed_value := 'primary = ip-10-11-204-31.ec2.internal');

current_proposer_id is a value that should be unique across the cluster for the given group and round. This is mainly used to determine which proposal was accepted when two proposers use the same value.

The latest value in the Paxos log can be retrieved using:

    SELECT * FROM paxos(
                    current_proposer_id := 'node-a/1248',
                    current_group_id := 'ha_postgres',
                    current_round_num := paxos_max_group_round('ha_postgres'));
                    
## Using Table Replication

An example of setting up a replicated table on 3 servers that run on the same host (ports 5432, 9700, 9701) is given below. After setting up the metadata, all writes to the coordinates table are replicated to the other nodes.

    CREATE TABLE coordinates (
        x int,
        y int
    );

    INSERT INTO pgp_metadata.group (group_id) VALUES ('mokka');
    INSERT INTO pgp_metadata.host VALUES ('mokka', '127.0.0.1', 5432, 0);
    INSERT INTO pgp_metadata.host VALUES ('mokka', '127.0.0.1', 9700, 0);
    INSERT INTO pgp_metadata.host VALUES ('mokka', '127.0.0.1', 9701, 0);
    INSERT INTO pgp_metadata.replicated_tables VALUES ('coordinates','mokka');
    
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
