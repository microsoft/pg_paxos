# pg_paxos

This PostgreSQL extension provides a basic implementation of the [Paxos algorithm](http://research.microsoft.com/en-us/um/people/lamport/pubs/paxos-simple.pdf) in PL/pgSQL and basic table replication through Paxos. 

Warning: pg_paxos is in an early stage, consider it experimental.

pg_paxos can be used to replicate a table across multiple PostgreSQL servers. Every INSERT/UPDATE/DELETE on a replicated table is logged through Paxos. When a query is performed on the table, pg_paxos first ensures that all preceding queries in the Multi-Paxos log have been applied, providing strong consistency. By using the Paxos algorithm, pg_paxos is also robust to failure of a minority of nodes (read: servers), e.g. 2 out of 5. 

## The Paxos Algorithm

paxos(k,v) is a function that returns the same value on all nodes in a group for a certain key (k), and the value is one of the inputs (v). For example, a node might call paxos('leader','primary = node-a') to assign a permanent group leader. paxos(k,v) first picks a proposal number n (usually 0) and then tries to get a value accepted by a majority of nodes in 2 phases:

1. a) ask a majority of nodes to reject proposals for key k with a number smaller than n (or equal with lower node id), and return any previously accepted value and its proposal number<br/>
   b) if a value was already accepted by one or more nodes, then v takes on the value with the highest proposal number
2. ask responders from phase 1 to accept proposal n with value v for key k

If in either phase the proposer cannot get confirmation from the majority, either due to failure or due to a rejection based on the proposal number, then it restarts with n = the highest proposal number in responses + 1, until a proposal succeeds in which case the function returns v. 

- 1a. ensures that the proposer has exclusive access among the majority, it's basically a lock with preemption
- 1b. ensures that a value is never changed once the majority accepts a value, since the proposer cannot get a new majority without at least one node having the existing value
- 2. achieves consensus if the majority lock has not been preempted, which implies that any other proposal still needs to complete 1a for at least one node in the majority and will thus see the value in 1b

Any subsequent proposal will use the same value in phase 2, and therefore paxos always returns the same value on all nodes.

## Multi-Paxos

Once a majority accepts a value, it can never be changed. However, Paxos can be used to implement a distributed log by using the index (round) in the log as a key. The log can be used to replicate a sequence of writes to a common, initial state. This technique is typically referred to as Multi-Paxos.

To append a new write to the log, a node needs to reach consensus on its write in a given round and apply all preceding writes. This may require several attempts if other nodes are trying to reach consensus on the same round. Heavily simplified, writing to the Multi-Paxos log might look as follows:

    round = last_applied_round+1
    
    while((c = paxos(round,v)) != v) 
        execute(c)
        last_applied_round = round
        round++

To perform a consistent read, a node needs to confirm the highest accepted round number in the group and execute all items in the log up to that round number. Some rounds may have been abandoned before consensus was reached, in which case the reader can force consensus by proposing a value that does not change the state.

    round = last_applied_round+1
    max_round_num = max_accepted_round()
    
    while(round <= max_round_num)
        execute(paxos(round,''))
        last_applied_round = round
        round++

While the examples above are heavily simplified, pg_paxos follows the same general approach, using SQL queries as log items.

## Installation

The easiest way to install pg_paxos is to build the sources from GitHub.

    git clone https://github.com/citusdata/pg_paxos.git
    cd pg_paxos
    PATH=/usr/local/pgsql/bin/:$PATH make
    sudo PATH=/usr/local/pgsql/bin/:$PATH make install

pg_paxos requires the dblink extension that you'll find in the contrib directory of PostgreSQL to be installed. After installing both extensions run:

    -- run via psql on each node:
    CREATE EXTENSION dblink;
    CREATE EXTENSION pg_paxos;
    
To do table replication, pg_paxos uses PostgreSQL's executor hooks to log SQL queries performed by the user in the Paxos log. To activate executor hooks, add pg_paxos to the shared_preload_libraries in postgresql.conf and restart postgres. It is also advisable to specify a unique node_id, which is needed to guarantee consistency in certain scenarios.

    # in postgresql.conf
    shared_preload_libraries = 'pg_paxos'
    pg_paxos.node_id = '<some-unique-name>'

## Setting up Table Replication

pg_paxos allows you to replicate a table across a group of servers. When a table is marked as replicated, pg_paxos intercepts all SQL queries on that table via the executor hooks and appends them to the Multi-Paxos log. Before a query is performed, preceding SQL queries in the log are executed to bring the table up-to-date. From the perspective of the user, the table always appears consistent, even though the physical representation of the table on disk may be behind at the start of the read.

To set up Paxos group with a replicated table, first create the table on all the nodes:

    CREATE TABLE coordinates (
        x int,
        y int
    );

Then, on one of the nodes, call paxos_create_group to create a named Paxos group with the node itself (defined as a connection string using its external address) as its sole member, and call paxos_replicate_table to replicate a table within a group:

    SELECT paxos_create_group('mokka', 'host=10.0.0.1');
    SELECT paxos_replicate_table('mokka', 'coordinates');
    
To add another node (e.g. 10.0.0.49), connect to it and call paxos_join_group using the name of the group, the connection string of an existing node, and the connection string of the node itself:
    
    SELECT paxos_join_group('mokka', 'host=10.0.0.1', 'host=10.0.0.49');
    
If you want to replicate an additional table after forming the group, then run paxos_replicate_table on all the nodes.

An example of how pg_paxos replicates the metadata:

    [marco@marco-desktop pg_paxos]$ psql
    psql (9.5.1)
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
    DEBUG:  Executing: INSERT INTO coordinates VALUES (1,1);
    CONTEXT:  SQL statement "SELECT paxos_apply_log($1,$2,$3)"
    DEBUG:  Executing: INSERT INTO coordinates VALUES (2,2);
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
    DEBUG:  Executing: INSERT INTO coordinates VALUES (1,1);
    CONTEXT:  SQL statement "SELECT paxos_apply_log($1,$2,$3)"
    DEBUG:  Executing: INSERT INTO coordinates VALUES (2,2);
    CONTEXT:  SQL statement "SELECT paxos_apply_log($1,$2,$3)"
    DEBUG:  Executing: UPDATE coordinates SET x = x * 10;
    CONTEXT:  SQL statement "SELECT paxos_apply_log($1,$2,$3)"
     x  | y
    ----+---
     10 | 1
     20 | 2
    (2 rows)

By default, pg_paxos asks other nodes for the highest accepted round number in the log before every read. It then applies the SQL queries in the log up to and including the highest accepted round number, which ensures strong consistency. In some cases, low read latencies may be preferable to strong consistency. The pg_paxos.consistency_model setting can be changed to 'optimistic', in which case the node assumes it has already learned about preceding writes. The optimistic consistency model provides read-your-writes consistency in the absence of failure, but may return older results when failures occur.

The consistency model can be changed in the session:

    SET pg_paxos.consistency_model TO 'optimistic';
    
To switch back to strong consistency:

    SET pg_paxos.consistency_model TO 'strong';

## Internal Table Replication functions

The following functions are called automatically when using table replications when a query is performed. We show how to call them explicitly to clarify the internals of pg_paxos.

The paxos_apply_and_append function (called on writes) appends a SQL query to the log after ensuring that all queries that will preceed it in the log have been executed. 

    SELECT * FROM paxos_apply_and_append(
                    current_proposer_id := 'node-a/1251',
                    current_group_id := 'ha_postgres',
                    proposed_value := 'INSERT INTO coordinates VALUES (3,3)');
    
The paxos_apply_log function (called on SELECT) executes all SQL queries in the log for a given group that have not yet been executed up to and including round number max_round_num.

    SELECT * FROM paxos_apply_log(
                    current_proposer_id := 'node-a/1252',
                    current_group_id := 'ha_postgres',
                    max_round_num := 3);

The paxos_max_group_round function queries a majority of hosts for their highest accepted round number. The round number returned by paxos_max_group_round will be greater or equal to any round on which there is consensus (a majority has accepted) at the start of the call to paxos_max_group_round. Therefore, a node is guaranteed to see any preceding write if it applies the log up to that round number. 

    SELECT * FROM paxos_max_group_round(
                    current_group_id := 'ha_postgres');

## Using Paxos functions directly
    
You can also implement an arbitrary distributed log using pg_paxos by calling the paxos functions directly. The following query appends value 'primary = node-a' to the Multi-Paxos log for the group ha_postgres:

    SELECT * FROM paxos_append(
                    current_proposer_id := 'node-a/1253',
                    current_group_id := 'ha_postgres',
                    proposed_value := 'primary = node-a');

current_proposer_id is a value that should be unique across the cluster for the given group and round. This is mainly used to determine which proposal was accepted when two proposers propose the same value.

The latest value in the Multi-Paxos log can be retrieved using:

    SELECT * FROM paxos(
                    current_proposer_id := 'node-a/1254',
                    current_group_id := 'ha_postgres',
                    current_round_num := paxos_max_group_round('ha_postgres'));

Copyright © 2016 Citus Data, Inc.
