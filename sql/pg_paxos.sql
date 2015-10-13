/*
 * Metadata on Paxos groups and replicated tables
 */
CREATE SCHEMA pgp_metadata

	/*
	 * The "group" table keeps track of which items from the log have been
	 * consumed/applied for a particular group.
	 */
	CREATE TABLE "group" (
		group_id text not null,
		last_applied_round bigint not null default -1,
		PRIMARY KEY (group_id)
	)

	/*
	 * The "host" table stores which members are part of a Paxos group during a given
	 * round. A host should be included if the current round lies between min_round_num
	 * and max_round_num.
	 */
	CREATE TABLE "host" (
		group_id text not null,
		node_name text not null,
		node_port int not null,
		min_round_num bigint not null,
		max_round_num bigint,
		PRIMARY KEY (group_id, node_name, node_port, min_round_num),
		FOREIGN KEY (group_id) REFERENCES pgp_metadata.group (group_id)
	)

	/*
	 * The "round" table contains a the Multi-Paxos log for a Paxos group.
	 *
	 * group_id - the name of the group
	 * round_num - the round number
	 * min_proposal_num - do not accept proposals with a lower proposal number than this
	 * proposer_id - the node that made the proposal (to ensure ordering)
	 * value_id - an identifier for the value that is unique for the round
	 * value - the value proposed through Paxos
	 * consensus - whether consensus was confirmed for the value
	 * error - error messages that was gneerated when applying the value
	 */
	CREATE TABLE "round" (
		group_id text not null,
		round_num bigint not null,
		min_proposal_num bigint not null,
		proposer_id text not null,
		value_id text,
		value text,
		consensus bool not null default false,
		error text,
		PRIMARY KEY (group_id, round_num),
		FOREIGN KEY (group_id) REFERENCES pgp_metadata.group (group_id)
	)

	/*
	 * The "replicated_tables" table stores which tables are managed by pg_paxos and
	 * in which Paxos group they are replicated. A table can be replicated within
	 * at most 1 Paxos group.
	 */
	CREATE TABLE "replicated_tables" (
		table_oid regclass not null,
		group_id text not null,
		PRIMARY KEY (table_oid),
		FOREIGN KEY (group_id) REFERENCES pgp_metadata.group (group_id)
	);


/*
 * Response from acceptor to prepare requests.
 *
 * promised - true if the acceptor participates, false otherwise
 * proposer_id - if a higher proposal was received, the proposer id
 * proposal_num - if a higher proposal was received, its number
 * value_id - if a proposal was previously accepted, its value identifier
 * value - if a proposal was previously accepted, its value
 */
CREATE TYPE prepare_response AS (
	promised boolean,
	proposer_id text,
	proposal_num bigint,
	value_id text,
	value text
);

/*
 * Response from acceptor to accept requests.
 *
 * accepted - false if a higher proposal was received, true otherwise
 * proposal_num - if a higher proposal was received, its number
 */
CREATE TYPE accept_response AS (
	accepted boolean,
	proposal_num bigint
);

/*
 * The result of a call to the paxos function.
 *
 * accepted_value - the value on which consensus was reached in the given round
 * value_changed - true if this was the value specified by the user, false otherwise
 */
CREATE TYPE paxos_result AS (
	accepted_value text,
	value_changed boolean
);


/*
 * paxos_request_prepare is a remote procedure that request participation of an
 * acceptor in a proposal. Effectively it grabs a preemptable lock.
 */
CREATE FUNCTION paxos_request_prepare(
								current_proposer_id text,
								current_group_id text,
								current_round_num bigint,
								current_proposal_num bigint)
RETURNS prepare_response
AS $BODY$
DECLARE
	response prepare_response;
	round record;
BEGIN
	/*
	 * My response depends on any preceding prepare and accept requests for the same
	 * group. Ensure prepare and accept requests are serialized through a lock.
	 */
	PERFORM pg_advisory_xact_lock(29020, hashtext(current_group_id));

	/* Get state of the current round */
	SELECT * INTO round
	FROM pgp_metadata.round
	WHERE group_id = current_group_id AND round_num = current_round_num;

	IF NOT FOUND THEN

		/* I have not seen a prepare request for this round */

		INSERT INTO pgp_metadata.round (
				group_id,
				round_num,
				min_proposal_num,
				proposer_id)
		VALUES (current_group_id,
				current_round_num,
				current_proposal_num,
				current_proposer_id);

		SELECT true, current_proposer_id, current_proposal_num, NULL, NULL
		INTO response;

	ELSIF current_proposal_num > round.min_proposal_num OR
		 (current_proposal_num = round.min_proposal_num AND
		 (current_proposer_id > round.proposer_id)) THEN

		/* I have seen a prepare request with a lower proposal number */

		UPDATE pgp_metadata.round
		SET min_proposal_num = current_proposal_num,
			proposer_id = current_proposer_id
		WHERE group_id = current_group_id AND round_num = current_round_num;

		SELECT true, current_proposer_id, current_proposal_num, round.value_id, round.value
		INTO response;

	ELSE
		/*  I have seen a prepare request with a higher proposal number (or same) */

		SELECT false, round.proposer_id, round.min_proposal_num, round.value_id, round.value
		INTO response;
	END IF;

	RETURN response;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_request_accept is a remote procedure that requests an acceptor to accept
 * a value, if the lock is still held by the proposer.
 */
CREATE FUNCTION paxos_request_accept(
							   current_proposer_id text,
							   current_group_id text,
							   current_round_num bigint,
							   current_proposal_num bigint,
							   proposed_value_id text,
							   proposed_value text)
RETURNS accept_response
AS $BODY$
DECLARE
	response accept_response;
	round record;
BEGIN
	/*
	 * My response depends on any preceding prepare and accept requests for the same
	 * group. Ensure prepare and accept requests are serialized through a lock.
	 */
	PERFORM pg_advisory_xact_lock(29020, hashtext(current_group_id));

	/* Get the state of the current round */
	SELECT * INTO round
	FROM pgp_metadata.round
	WHERE group_id = current_group_id AND round_num = current_round_num;

	IF NOT FOUND THEN
		/* I have not seen a prepare request for this proposal */

		RAISE EXCEPTION 'Unknown round';
	ELSIF current_proposal_num = round.min_proposal_num AND
		  current_proposer_id = round.proposer_id THEN

		/* I have indeed promised to participate in this proposal and accept it */

		UPDATE pgp_metadata.round
		SET "value" = proposed_value,
			"value_id" = proposed_value_id
		WHERE group_id = current_group_id AND round_num = current_round_num;

		SELECT true, current_proposal_num INTO response;
	ELSE
		/* I have promised to participate in a different proposal */

		SELECT false, round.min_proposal_num INTO response;
	END IF;

	RETURN response;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_confirm_consensus is a remote procedure that informs a learner that consensus
 * has been reached on a given value.
 */
CREATE FUNCTION paxos_confirm_consensus(
							current_proposer_id text,
							current_group_id text,
							current_round_num bigint,
							accepted_proposal_num bigint,
							accepted_value_id text,
							accepted_value text)
RETURNS boolean
AS $BODY$
DECLARE
BEGIN
	/* No longer accept any new values and confirm consensus */
	UPDATE pgp_metadata.round
	SET consensus = true,
		proposer_id = current_proposer_id,
		min_proposal_num = accepted_proposal_num,
		value_id = accepted_value_id,
		value = accepted_value
	WHERE group_id = current_group_id AND round_num = current_round_num;

	RETURN true;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos proposes value proposed_value for round current_round_num in group named
 * current_group_id through the Paxos algorithm. The value of current_proposer_id
 * uniquely identifies the proposal for the duration of the function call.
 */
CREATE FUNCTION paxos(
							current_proposer_id text,
							current_group_id text,
							current_round_num bigint,
							proposed_value text DEFAULT NULL)
RETURNS paxos_result
AS $BODY$
DECLARE
	num_hosts int;
	num_open_connections int;
	majority_size int;
	current_proposal_num int := 0;
	num_prepare_responses int;
	max_prepare_response prepare_response;
	num_accept_responses int;
	max_accept_response accept_response;
	num_accepted int;
	initial_value text := proposed_value;
	initial_value_id text := current_proposer_id;
	proposed_value_id text := initial_value_id;
	accepted_value_id text;
	accepted_value text;
	value_changed boolean := false;
	start_time double precision := extract(EPOCH FROM clock_timestamp());
	done boolean := false;
	inform_learners boolean := true;
	result paxos_result;
BEGIN
	/* Find the hosts to use for the current round */
	SELECT paxos_find_hosts(current_group_id, current_round_num) INTO num_hosts;

	majority_size = num_hosts / 2 + 1;

	CREATE TEMPORARY TABLE IF NOT EXISTS prepare_responses (
		promised boolean,
		proposer_id text,
		proposal_num bigint,
		value_id text,
		value text
	);

	CREATE TEMPORARY TABLE IF NOT EXISTS accept_responses (
		accepted boolean,
		proposal_num bigint
	);

	WHILE NOT done LOOP
		TRUNCATE prepare_responses;
		TRUNCATE accept_responses;

		/* Try to open connections to all hosts in the group */
		SELECT paxos_open_connections(num_hosts) INTO num_open_connections;

		IF num_open_connections < majority_size THEN
			PERFORM paxos_close_connections();
			RAISE 'could only open % out of % connections', num_open_connections, majority_size;
		END IF;

		/* Phase 1 of Paxos: prepare */
		INSERT INTO prepare_responses SELECT * FROM paxos_prepare(
							current_proposer_id,
							current_group_id,
							current_round_num,
							current_proposal_num);

		/* Check whether majority responded */
		SELECT count(*) INTO num_prepare_responses FROM prepare_responses;

		IF num_prepare_responses < majority_size THEN
			RAISE NOTICE 'could only get % out of % prepare responses, retrying after 1 sec',
						 num_prepare_responses, majority_size;

			PERFORM pg_sleep(1);
			current_proposal_num := current_proposal_num + 1;
			CONTINUE;
		END IF;

		/* Find whether consensus was already reached */
		SELECT value_id, value INTO accepted_value_id, accepted_value
		FROM prepare_responses
		WHERE value_id IS NOT NULL
		GROUP BY value_id, value
		HAVING count(*) >= majority_size;

		IF FOUND THEN
			IF accepted_value_id <> initial_value_id OR accepted_value <> initial_value THEN
				/* There is consensus on someone else's value */
				value_changed := true;
			ELSE
				/* It's actually my value, apparently I already had consensus on it */
				value_changed := false;
			END IF;

			proposed_value := accepted_value;
			proposed_value_id := accepted_value_id;
			inform_learners := false;

			EXIT;
		END IF;

		/* Find highest existing proposal */
		SELECT * INTO max_prepare_response
		FROM prepare_responses
		ORDER BY proposal_num DESC, proposer_id DESC LIMIT 1;

		IF NOT max_prepare_response.promised THEN
			/* Another proposal with a higher proposal number exists */

			IF max_prepare_response.proposal_num = current_proposal_num
			AND current_proposal_num > 0 THEN
				RAISE NOTICE 'competing with %, retrying after random back-off',
							 max_prepare_response.proposer_id;
				PERFORM pg_sleep(trunc(random() * (EXTRACT(EPOCH FROM clock_timestamp())-start_time)));
			END IF;

			current_proposal_num := max_prepare_response.proposal_num + 1;
			CONTINUE;
		ELSIF max_prepare_response.value_id IS NOT NULL THEN
			/* A value was already accepted, I change my proposal to this value */

			IF max_prepare_response.value_id <> initial_value_id
			OR max_prepare_response.value <> initial_value THEN
				/* I will use a value from a different proposer */
				value_changed := true;
			ELSE
				/* I will revert to my own value, which was previously accepted by someone else */
				value_changed := false;
			END IF;

			proposed_value := max_prepare_response.value;
			proposed_value_id := max_prepare_response.value_id;
		END IF;

		/* Phase 1 of Paxos: accept */
		INSERT INTO accept_responses SELECT * FROM paxos_accept(
							current_proposer_id,
							current_group_id,
							current_round_num,
							current_proposal_num,
							proposed_value_id,
							proposed_value);

		/* Check whether majority responded */
		SELECT count(*) INTO num_accept_responses FROM accept_responses;

		IF num_accept_responses < majority_size THEN
			RAISE NOTICE 'could not get accept responses from majority, retrying after 1 sec';

			PERFORM pg_sleep(1);
			current_proposal_num := current_proposal_num + 1;
			CONTINUE;
		END IF;

		/* Check whether majority accepted */
		SELECT count(*) INTO num_accepted FROM accept_responses WHERE accepted;

		IF num_accepted < majority_size THEN
			RAISE NOTICE 'could not get accepted by majority, retrying after 1 sec';

			SELECT * INTO max_accept_response
			FROM accept_responses
			ORDER BY proposal_num DESC LIMIT 1;

			IF NOT max_accept_response.proposal_num > current_proposal_num THEN
				/* If a previous proposal has a higher proposal number, use that + 1 */
				current_proposal_num := max_accept_response.proposal_num + 1;
			ELSE
				current_proposal_num := current_proposal_num + 1;
			END IF;

			CONTINUE;
		END IF;

		done := true;
	END LOOP;

	/* I now know consensus was reached on proposed_value */
	result.accepted_value := proposed_value;
	result.value_changed := value_changed;

	IF inform_learners THEN
		/* Inform acceptors of the consensus */
		PERFORM paxos_inform_learners(
							current_proposer_id,
							current_group_id,
							current_round_num,
							current_proposal_num,
							proposed_value_id,
							proposed_value);
	END IF;

	DROP TABLE prepare_responses;
	DROP TABLE accept_responses;

	RETURN result;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_prepare performs a paxos_request_prepare RPC on all connected hosts.
 */
CREATE FUNCTION paxos_prepare(
							current_proposer_id text,
							current_group_id text,
							current_round_num bigint,
							current_proposal_num bigint)
RETURNS SETOF prepare_response
AS $BODY$
DECLARE
	prepare_query text;
	host record;
num_conn int;
BEGIN
	prepare_query := format('SELECT paxos_request_prepare(%s,%s,%s,%s)',
							quote_literal(current_proposer_id),
							quote_literal(current_group_id),
							current_round_num,
							current_proposal_num);

	PERFORM paxos_broadcast_query(prepare_query);

	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		RETURN QUERY
		SELECT (resp).* FROM dblink_get_result(host.connection_name, false)
							 AS (resp prepare_response);
	END LOOP;

	PERFORM paxos_clear_connections();
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_accept performs a paxos_request_accept RPC on all connected hosts.
 */
CREATE FUNCTION paxos_accept(
							current_proposer_id text,
							current_group_id text,
							current_round_num bigint,
							current_proposal_num bigint,
							proposed_value_id text,
							proposed_value text)
RETURNS SETOF accept_response
AS $BODY$
DECLARE
	accept_query text;
	host record;
BEGIN
	accept_query := format('SELECT paxos_request_accept(%s,%s,%s,%s,%s,%s)',
							quote_literal(current_proposer_id),
							quote_literal(current_group_id),
							current_round_num,
							current_proposal_num,
							quote_literal(proposed_value_id),
							quote_literal(proposed_value));

	PERFORM paxos_broadcast_query(accept_query);

	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		RETURN QUERY
		SELECT (resp).* FROM dblink_get_result(host.connection_name)
							 AS (resp accept_response);
	END LOOP;

	PERFORM paxos_clear_connections();
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_inform_learners performs a paxos_confirm_consensus RPC on all connected hosts.
 */
CREATE FUNCTION paxos_inform_learners(
							current_proposer_id text,
							current_group_id text,
							current_round_num bigint,
							current_proposal_num bigint,
							proposed_value_id text,
							proposed_value text)
RETURNS void
AS $BODY$
DECLARE
	confirm_query text;
	host record;
BEGIN
	confirm_query := format('SELECT paxos_confirm_consensus(%s,%s,%s,%s,%s,%s)',
							quote_literal(current_proposer_id),
							quote_literal(current_group_id),
							current_round_num,
							current_proposal_num,
							quote_literal(proposed_value_id),
							quote_literal(proposed_value));

	PERFORM paxos_broadcast_query(confirm_query);

	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		PERFORM * FROM dblink_get_result(host.connection_name, false) AS (resp boolean);
	END LOOP;

	PERFORM paxos_clear_connections();
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_append appends proposed_value to the log of the group whose name is given by
 * current_group_id.
 */
CREATE FUNCTION paxos_append(
							proposer_id text,
							current_group_id text,
							proposed_value text)
RETURNS bigint
AS $BODY$
DECLARE
	current_round_num bigint;
	accepted_value text;
	value_written boolean := false;
BEGIN

	/*
	 * Start with a round ID that is higher than the highest round ID in
	 * a majority of nodes meaning higher than any round ID on which
	 * consensus was reached. Since I'll use the same nodes as acceptors,
	 * I have a good chance of getting my proposal accepted.
     */
	SELECT paxos_max_group_round(current_group_id) INTO current_round_num;

	/*
	 * Another node could be using the same or higher round ID, but
	 * if that node reaches consensus on its value for that round we
	 * will retry paxos with round ID + 1, until we succeed.

	 * An optimization would be to resume from the competing round ID + 1,
	 * which may be relevant when another node is performing a large number
	 * of writes.
	 */
	WHILE NOT value_written LOOP
		current_round_num := current_round_num + 1;

		SELECT paxos(
						proposer_id,
						current_group_id,
						current_round_num,
						proposed_value) INTO accepted_value, value_written;
	END LOOP;

	RETURN current_round_num;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_max_group_round returns the highest round number used among a majority of
 * nodes. If a majority cannot be reached, the function fails. If accepted_only is
 * true, the function returns the highest accepted round number among the nodes.
 */
CREATE FUNCTION paxos_max_group_round(
							current_group_id text,
							accepted_only boolean DEFAULT false)
RETURNS bigint
AS $BODY$
DECLARE
	num_hosts int;
	majority_size int;
	round_query text;
	max_round_num bigint := -1;
	num_responses int := 0;
	remote_round_num bigint;
	host record;
BEGIN
	/* Set up connections */
	SELECT paxos_init_group(current_group_id) INTO num_hosts;

	majority_size = num_hosts / 2 + 1;

	/* Ask nodes for highest round number in their log */
	round_query := format('SELECT max(round_num) '||
						  'FROM pgp_metadata.round '||
						  'WHERE group_id = %s',
						  quote_literal(current_group_id));

	IF accepted_only THEN
		round_query := round_query || ' AND value_id IS NOT NULL';
	END IF;

	PERFORM paxos_broadcast_query(round_query);

	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		SELECT resp INTO remote_round_num
		FROM dblink_get_result(host.connection_name, false) AS (resp bigint);

		IF remote_round_num IS NOT NULL AND remote_round_num > max_round_num THEN
			max_round_num := remote_round_num;
		END IF;

		num_responses := num_responses + 1;
	END LOOP;

	PERFORM paxos_clear_connections();

	IF num_responses < majority_size THEN
		RAISE 'could only get % out of % responses', num_responses, majority_size;
	END IF;

	RETURN max_round_num;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_apply_log implements table replication through Paxos. It interprets the log of
 * the Paxos group whose name is in current_group_id as a sequence of SQL commands and
 * executes all commands up to and including the round with number max_round_num.
 */
CREATE FUNCTION paxos_apply_log(
							current_proposer_id text,
							current_group_id text,
							max_round_num bigint)
RETURNS bigint
AS $BODY$
DECLARE
	last_applied_round_num bigint;
	current_round_num bigint;
	query text;
	noop_written boolean;
BEGIN
	/* Prevent other processes from applying the same log */
	PERFORM pg_advisory_xact_lock(29030, hashtext(current_group_id));

	/* Temporarily prevent pg_paxos from intercepting and logging queries */
	SET pg_paxos.enabled TO false;

	SELECT last_applied_round INTO current_round_num
	FROM pgp_metadata.group
	WHERE group_id = current_group_id;

	WHILE current_round_num < max_round_num LOOP

		current_round_num := current_round_num + 1;

		SELECT value INTO query
		FROM pgp_metadata.round
		WHERE group_id = current_group_id AND round_num = current_round_num AND consensus;

		IF NOT FOUND THEN
			SELECT * FROM paxos(
						current_proposer_id,
						current_group_id,
						current_round_num,
						'') INTO query, noop_written;
		END IF;

		RAISE NOTICE 'Executing: %', query;

		BEGIN
			EXECUTE query;
		EXCEPTION WHEN others THEN
			UPDATE pgp_metadata.round
			SET error = SQLERRM
			WHERE group_id = current_group_id AND round_num = current_round_num;
		END;

	END LOOP;

	UPDATE pgp_metadata.group
	SET last_applied_round = max_round_num
	WHERE group_id = current_group_id;

	SET pg_paxos.enabled TO true;
	RETURN max_round_num;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_apply_and_append appends a new command to the log and applies all preceding
 * commands in lockstep fashion.
 */
CREATE FUNCTION paxos_apply_and_append(
							current_proposer_id text,
							current_group_id text,
							proposed_value text)
RETURNS bigint
AS $BODY$
DECLARE
	current_round_num bigint;
	value_written bool := false;
	query text;
BEGIN
	SELECT paxos_max_group_round(current_group_id) INTO current_round_num;

	WHILE NOT value_written LOOP
		PERFORM paxos_apply_log(
						current_proposer_id,
						current_group_id,
						current_round_num);

		current_round_num := current_round_num + 1;

		SELECT paxos(
					current_proposer_id,
					current_group_id,
					current_round_num,
					proposed_value) INTO query, value_written;
	END LOOP;

	RETURN current_round_num;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_add_host appends a command that adds the given host to the Paxos group
 * starting from the round in which this command is logged + 1.
 */
CREATE FUNCTION paxos_add_host(
							current_proposer_id text,
							current_group_id text,
							hostname text,
							port int)
RETURNS bigint
AS $BODY$
DECLARE
	current_round_num bigint;
	proposed_value text;
	accepted_value text;
	value_written boolean := false;
BEGIN
	SELECT paxos_max_group_round(current_group_id) INTO current_round_num;

	WHILE NOT value_written LOOP
		PERFORM paxos_apply_log(
						current_proposer_id,
						current_group_id,
						current_round_num);

		current_round_num := current_round_num + 1;

		proposed_value := format('INSERT INTO pgp_metadata.host VALUES (%s,%s,%s,%s)',
								 quote_literal(current_group_id),
								 quote_literal(hostname),
								 port,
								 current_round_num+1);
		SELECT paxos(
						current_proposer_id,
						current_group_id,
						current_round_num,
						proposed_value) INTO accepted_value, value_written;
	END LOOP;

	RETURN current_round_num;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_remove_host appends a command that removes the given host from the Paxos group
 * starting from the round in which this command is logged + 1.
 */
CREATE FUNCTION paxos_remove_host(
							current_proposer_id text,
							current_group_id text,
							hostname text,
							port int)
RETURNS bigint
AS $BODY$
DECLARE
	current_round_num bigint;
	proposed_value text;
	accepted_value text;
	value_written boolean := false;
BEGIN
	SELECT paxos_max_group_round(current_group_id) INTO current_round_num;

	WHILE NOT value_written LOOP
		PERFORM paxos_apply_log(
						current_proposer_id,
						current_group_id,
						current_round_num);

		current_round_num := current_round_num + 1;

		proposed_value := format('UPDATE pgp_metadata.host '||
								 'SET max_round_num = %s '||
								 'WHERE group_id = %s '||
								 'AND node_name = %s '||
								 'AND node_port = %s',
								 current_round_num+1,
								 quote_literal(current_group_id),
								 quote_literal(hostname),
								 port);

		SELECT paxos(
						current_proposer_id,
						current_group_id,
						current_round_num,
						proposed_value) INTO accepted_value, value_written;
	END LOOP;

	RETURN current_round_num;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_init_group finds a current set of hosts for a given Paxos group opens
 * connections to them.
 */
CREATE FUNCTION paxos_init_group(
							current_group_id text)
RETURNS int
AS $BODY$
DECLARE
	max_local_round bigint;
	num_hosts int;
	num_open_connections int;
	round_query text;
	host record;
BEGIN
	/*
	 * We call this function before we know the current round number so we have to make
	 * a guess based on local information to get the current set of hosts. It is not
	 * critical that we get the set of hosts right, since it is not critical that we
	 * get the round number right, but getting it wrong causes a lot of redundant work.
	 */
	SELECT max(round_num) INTO max_local_round
	FROM pgp_metadata.round
	WHERE group_id = current_group_id;

	IF max_local_round IS NULL THEN
		max_local_round = 0;
	END IF;

	SELECT paxos_find_hosts(current_group_id, max_local_round) INTO num_hosts;

	/* Try to open connections to a majority of hosts */
	SELECT paxos_open_connections(num_hosts) INTO num_open_connections;

	return num_hosts;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_find_hosts stores a snapshot of the hosts for a given round in a temporary
 * table named "hosts".
 */
CREATE FUNCTION paxos_find_hosts(
							current_group_id text,
							current_round_num bigint)
RETURNS int
AS $BODY$
DECLARE
	num_hosts int;
BEGIN

	IF NOT EXISTS (
		SELECT relname
		FROM pg_class
		WHERE relnamespace = pg_my_temp_schema() AND relname = 'hosts') THEN

		CREATE TEMPORARY TABLE IF NOT EXISTS hosts (
			connection_name text,
			node_name text,
			node_port int,
			connected boolean
		);

	END IF;

	TRUNCATE hosts;

	INSERT INTO hosts
	SELECT format('%s:%s', node_name, node_port) AS connection_name,
		   node_name, node_port,
		   false AS connected
	FROM pgp_metadata.host
	WHERE group_id = current_group_id
	  AND min_round_num <= current_round_num
	  AND (max_round_num IS NULL OR current_round_num <= max_round_num)
	GROUP BY node_name, node_port;

	SELECT count(*) INTO num_hosts FROM hosts;

	RETURN num_hosts;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_open_connections tries to open connections to all the hosts in the "hosts"
 * table. If there are at least min_connections connections open at the start of this
 * function, then no new connections are opened.
 */
CREATE FUNCTION paxos_open_connections(min_connections int)
RETURNS int
AS $BODY$
DECLARE
	num_open_connections int;
	connection_string text;
	host record;
	connection_open boolean;
BEGIN
	num_open_connections := 0;

	FOR host IN
	SELECT h.connection_name, h.node_name, h.node_port, c.connected
	FROM hosts h LEFT OUTER JOIN
		 (SELECT unnest AS connected
		  FROM unnest(dblink_get_connections())) c ON (h.connection_name = c.connected) LOOP

		IF host.connected IS NOT NULL THEN
			IF dblink_error_message(host.connection_name) = 'OK' THEN
				/* We previously opened this connection */
				num_open_connections := num_open_connections + 1;
				UPDATE hosts SET connected = true WHERE connection_name = host.connection_name;
			ELSE
				/* Close connections that have errored out, will not be used next round */
				RAISE NOTICE 'connection error: %', dblink_error_message(host.connection_name);
				PERFORM dblink_disconnect(host.connection_name);
				UPDATE hosts SET connected = false WHERE connection_name = host.connection_name;
			END IF;
		END IF;
	END LOOP;

	/* If we already have the minimum number of connections open, we're done */
	IF num_open_connections >= min_connections THEN
		RETURN num_open_connections;
	END IF;

	/* Otherwise, try to open as many connections as possible (bias towards reads) */
	FOR host IN
	SELECT h.connection_name, h.node_name, h.node_port, c.connected
	FROM hosts h LEFT OUTER JOIN
		 (SELECT unnest AS connected
		  FROM unnest(dblink_get_connections())) c ON (h.connection_name = c.connected) LOOP

		IF host.connected IS NULL THEN
			/* Open new connection */
			connection_string := format('hostaddr=%s port=%s connect_timeout=10',
										host.node_name,
										host.node_port);

			BEGIN
				PERFORM dblink_connect(host.connection_name, connection_string);
				num_open_connections := num_open_connections + 1;
				UPDATE hosts SET connected = true WHERE connection_name = host.connection_name;
			EXCEPTION WHEN OTHERS THEN
				RAISE NOTICE 'failed to connect to %:%', host.node_name, host.node_port;
				UPDATE hosts SET connected = false WHERE connection_name = host.connection_name;
			END;
		END IF;
	END LOOP;

	RETURN num_open_connections;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_broadcast_query sends a query to all connected hosts.
 */
CREATE FUNCTION paxos_broadcast_query(query_string text)
RETURNS void
AS $BODY$
DECLARE
	host record;
BEGIN
	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		BEGIN
			PERFORM dblink_send_query(host.connection_name, query_string);
		EXCEPTION WHEN OTHERS THEN
			PERFORM dblink_disconnect(host.connection_name);
			UPDATE hosts SET connected = false WHERE connection_name = host.connection_name;
		END;
	END LOOP;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_clear_connections calls dblink_get_result on open connections to ensure that
 * the connection can be reused.
 */
CREATE FUNCTION paxos_clear_connections()
RETURNS void
AS $BODY$
DECLARE
	host record;
BEGIN
	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		/* Need to call get_result until it returns NULL to clear the connection */
		PERFORM * FROM dblink_get_result(host.connection_name, false) AS (resp int);
	END LOOP;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_close_connections closes all open connections.
 */
CREATE FUNCTION paxos_close_connections()
RETURNS void
AS $BODY$
DECLARE
	host record;
BEGIN
	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		PERFORM dblink_disconnect(host.connection_name);
	END LOOP;
END;
$BODY$ LANGUAGE 'plpgsql';


/*
 * paxos_replicate_table replicates the table identified by new_table_oid within
 * the Paxos group identified by current_group_id.
 */
CREATE FUNCTION paxos_replicate_table(
								current_group_id text,
								new_table_oid regclass)
RETURNS void
AS $BODY$
BEGIN
	INSERT INTO pgp_metadata.replicated_tables (
			table_name,
			group_id)
	VALUES (new_table_oid,
			current_group_id);
END;
$BODY$ LANGUAGE plpgsql;


