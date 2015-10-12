-- Metadata storage
CREATE SCHEMA pgp_metadata

	-- Stores group metadata
	CREATE TABLE "group" (
		group_id text not null,
		last_applied_round bigint not null default -1,
		PRIMARY KEY (group_id)
	)

	-- Stores the hosts for each group and in which round the host joined/left
	CREATE TABLE "host" (
		group_id text not null,
		node_name text not null,
		node_port int not null,
		min_round_id bigint not null,
		max_round_id bigint,
		PRIMARY KEY (group_id, node_name, node_port, min_round_id),
		FOREIGN KEY (group_id) REFERENCES pgp_metadata.group (group_id)
	)

	-- Stores round metadata necessary for Paxos
	CREATE TABLE "round" (
		group_id text not null,
		round_id bigint not null,
		min_proposal_id bigint not null,
		proposer_id text not null,
		value_proposer_id text,
		value text,
		consensus bool not null default false,
		error text,
		PRIMARY KEY (group_id, round_id),
		FOREIGN KEY (group_id) REFERENCES pgp_metadata.group (group_id)
	)

	-- Stores which tables are managed by pg_paxos and the group ID
	CREATE TABLE "replicated_tables" (
		table_oid regclass not null,
		group_id text not null,
		PRIMARY KEY (table_oid),
		FOREIGN KEY (group_id) REFERENCES pgp_metadata.group (group_id)
	);


-- Response message types
CREATE TYPE prepare_response AS (
	promised boolean,
	proposer_id text,
	proposal_id bigint,
	value_proposer_id text,
	value text
);

CREATE TYPE accept_response AS (
	accepted boolean,
	proposal_id bigint
);

CREATE TYPE paxos_result AS (
	accepted_value text,
	value_changed boolean
);


-- Administrative functions

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


-- Acceptor functions

-- Phase 1 of Paxos on the Acceptor
CREATE FUNCTION paxos_request_prepare(
								current_proposer_id text,
								current_group_id text,
								current_round_id bigint,
								current_proposal_id bigint)
RETURNS prepare_response
AS $BODY$
DECLARE
	response prepare_response;
	round record;
BEGIN
	-- My response depends on any preceding prepare and accept requests
	PERFORM pg_advisory_xact_lock(29020, hashtext(current_group_id));

	-- Get state of the current round
	SELECT * INTO round
	FROM pgp_metadata.round
	WHERE group_id = current_group_id AND round_id = current_round_id;

	IF NOT FOUND THEN

		-- I've not seen a prepare request for this proposal

		INSERT INTO pgp_metadata.round (
				group_id,
				round_id,
				min_proposal_id,
				proposer_id)
		VALUES (current_group_id,
				current_round_id,
				current_proposal_id,
				current_proposer_id);

		SELECT true, current_proposer_id, current_proposal_id, NULL, NULL INTO response;

	ELSIF current_proposal_id > round.min_proposal_id OR
		 (current_proposal_id = round.min_proposal_id AND
		 (current_proposer_id > round.proposer_id)) THEN
		-- I've seen a prepare request with a lower ID for this proposal

		UPDATE pgp_metadata.round
		SET min_proposal_id = current_proposal_id,
			proposer_id = current_proposer_id
		WHERE group_id = current_group_id AND round_id = current_round_id;

		SELECT true, current_proposer_id, current_proposal_id, round.value_proposer_id, round.value INTO response;

	ELSE
		-- I've seen a prepare request with a higher ID (or same) for this proposal

		SELECT false, round.proposer_id, round.min_proposal_id, round.value_proposer_id, round.value INTO response;
	END IF;

	RETURN response;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Phase 2 of Paxos on the Acceptor
CREATE FUNCTION paxos_request_accept(
							   current_proposer_id text,
							   current_group_id text,
							   current_round_id bigint,
							   current_proposal_id bigint,
							   proposed_value text)
RETURNS accept_response
AS $BODY$
DECLARE
	response accept_response;
	round record;
BEGIN
	-- My response depends on any preceding prepare request
	PERFORM pg_advisory_xact_lock(29020, hashtext(current_group_id));

	-- Get the state of the current round
	SELECT * INTO round
	FROM pgp_metadata.round
	WHERE group_id = current_group_id AND round_id = current_round_id;

	IF NOT FOUND THEN
		-- I have not seen a prepare request for this proposal

		RAISE EXCEPTION 'Unknown round';
	ELSIF current_proposal_id = round.min_proposal_id AND
		  current_proposer_id = round.proposer_id THEN

		-- I have indeed promised to participate in this proposal and accept it

		UPDATE pgp_metadata.round
		SET "value" = proposed_value,
			"value_proposer_id" = current_proposer_id
		WHERE group_id = current_group_id AND round_id = current_round_id;

		SELECT true, current_proposal_id INTO response;
	ELSE
		-- I've promised to participate in a different proposal

		SELECT false, round.min_proposal_id INTO response;
	END IF;

	RETURN response;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Learner functions
CREATE FUNCTION paxos_confirm_consensus(
							current_proposer_id text,
							current_group_id text,
							current_round_id bigint,
							accepted_proposal_id bigint,
							accepted_value text)
RETURNS boolean
AS $BODY$
DECLARE
BEGIN
	-- No longer accept any new values and confirm consensus
	UPDATE pgp_metadata.round
	SET consensus = true,
		min_proposal_id = accepted_proposal_id,
		value_proposer_id = current_proposer_id,
		value = accepted_value
	WHERE group_id = current_group_id AND round_id = current_round_id;

	RETURN true;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Proposer functions

-- Propose a value through Paxos
CREATE FUNCTION paxos(
							current_proposer_id text,
							current_group_id text,
							current_round_id bigint,
							proposed_value text DEFAULT NULL)
RETURNS paxos_result
AS $BODY$
DECLARE
	num_hosts int;
	num_open_connections int;
	majority_size int;
	current_proposal_id int := 0;
	num_prepare_responses int;
	max_prepare_response prepare_response;
	num_accept_responses int;
	max_accept_response accept_response;
	num_accepted int;
	accepted_proposer_id text;
	accepted_value text;
	value_changed boolean := false;
	start_time double precision := extract(EPOCH FROM clock_timestamp());
	done boolean := false;
	inform_learners boolean := true;
	result paxos_result;
BEGIN
	-- Snapshot of hosts to use
	SELECT paxos_find_hosts(current_group_id, current_round_id) INTO num_hosts;

	majority_size = num_hosts / 2 + 1;

	CREATE TEMPORARY TABLE IF NOT EXISTS prepare_responses (
		promised boolean,
		proposer_id text,
		proposal_id bigint,
		value_proposer_id text,
		value text
	);

	CREATE TEMPORARY TABLE IF NOT EXISTS accept_responses (
		accepted boolean,
		proposal_id bigint
	);

	WHILE NOT done LOOP
		TRUNCATE prepare_responses;
		TRUNCATE accept_responses;

		-- Open connections to hosts in group
		SELECT paxos_open_connections(num_hosts) INTO num_open_connections;

		IF num_open_connections < majority_size THEN
			PERFORM paxos_close_connections();
			RAISE 'could only open % out of % connections', num_open_connections, majority_size;
		END IF;

		-- Phase 1: prepare
		INSERT INTO prepare_responses SELECT * FROM paxos_prepare(
							current_proposer_id,
							current_group_id,
							current_round_id,
							current_proposal_id);

		-- Check whether majority responded
		SELECT count(*) INTO num_prepare_responses FROM prepare_responses;

		IF num_prepare_responses < majority_size THEN
			RAISE NOTICE 'could only get % out of % prepare responses, retrying after 1 sec',
						 num_prepare_responses, majority_size;

			PERFORM pg_sleep(1);
			current_proposal_id := current_proposal_id + 1;
			CONTINUE;
		END IF;

		-- Find whether consensus was already reached
		SELECT value_proposer_id, value INTO accepted_proposer_id, accepted_value
		FROM prepare_responses
		WHERE value_proposer_id IS NOT NULL
		GROUP BY value_proposer_id, value
		HAVING count(*) >= majority_size;

		IF FOUND THEN
			proposed_value := accepted_value;
			inform_learners := false;

			IF accepted_proposer_id <> current_proposer_id THEN
				-- There is consensus on someone else's value
				value_changed := true;
			ELSE
				-- It's actually my value, apparently I already had consensus on it
				value_changed := false;
			END IF;
			EXIT;
		END IF;

		-- Find highest existing proposal
		SELECT * INTO max_prepare_response
		FROM prepare_responses
		ORDER BY proposal_id DESC, proposer_id DESC LIMIT 1;

		IF NOT max_prepare_response.promised THEN
			-- Another proposal with a higher ID exists
			IF max_prepare_response.proposal_id = current_proposal_id AND current_proposal_id > 0 THEN
				RAISE NOTICE 'competing with %, retrying after random back-off', max_prepare_response.proposer_id;
				PERFORM pg_sleep(trunc(random() * (EXTRACT(EPOCH FROM clock_timestamp())-start_time)));
			END IF;

			current_proposal_id := max_prepare_response.proposal_id + 1;
			CONTINUE;
		ELSIF max_prepare_response.value_proposer_id IS NOT NULL THEN
			proposed_value := max_prepare_response.value;

			IF max_prepare_response.value_proposer_id <> current_proposer_id THEN
				-- I use a value from a different proposer
				value_changed := true;
			ELSE
				-- I use my own value, which was previously accepted
				value_changed := false;
			END IF;
		END IF;

		-- Phase 2: accept
		INSERT INTO accept_responses SELECT * FROM paxos_accept(
							current_proposer_id,
							current_group_id,
							current_round_id,
							current_proposal_id,
							proposed_value);

		-- Check whether majority responded
		SELECT count(*) INTO num_accept_responses FROM accept_responses;

		IF num_accept_responses < majority_size THEN
			RAISE NOTICE 'could not get accept responses from majority, retrying after 1 sec';

			PERFORM pg_sleep(1);
			current_proposal_id := current_proposal_id + 1;
			CONTINUE;
		END IF;

		-- Check whether majority accepted
		SELECT count(*) INTO num_accepted FROM accept_responses WHERE accepted;

		IF num_accepted < majority_size THEN
			RAISE NOTICE 'could not get accepted by majority, retrying after 1 sec';

			SELECT * INTO max_accept_response FROM accept_responses ORDER BY proposal_id DESC LIMIT 1;

			IF NOT max_accept_response.proposal_id > current_proposal_id THEN
				-- If a previous proposal has a higher proposal ID, use that + 1
				current_proposal_id := max_accept_response.proposal_id + 1;
			ELSE
				current_proposal_id := current_proposal_id + 1;
			END IF;

			CONTINUE;
		END IF;

		done := true;
	END LOOP;

	result.accepted_value := proposed_value;
	result.value_changed := value_changed;

	-- I now know consensus was reached, inform acceptors of this discovery
	IF inform_learners THEN
		PERFORM paxos_inform_learners(
							current_proposer_id,
							current_group_id,
							current_round_id,
							current_proposal_id,
							result.accepted_value);
	END IF;

	DROP TABLE prepare_responses;
	DROP TABLE accept_responses;

	RETURN result;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Phase 1 of Paxos on the proposer
CREATE FUNCTION paxos_prepare(
							current_proposer_id text,
							current_group_id text,
							current_round_id bigint,
							current_proposal_id bigint)
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
							current_round_id,
							current_proposal_id);

	PERFORM paxos_broadcast_query(prepare_query);

	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		RETURN QUERY
		SELECT (resp).* FROM dblink_get_result(host.connection_name, false) AS (resp prepare_response);
	END LOOP;

	PERFORM paxos_clear_connections();
END;
$BODY$ LANGUAGE 'plpgsql';


-- Phase 2 of Paxos on the proposer
CREATE FUNCTION paxos_accept(
							current_proposer_id text,
							current_group_id text,
							current_round_id bigint,
							current_proposal_id bigint,
							proposed_value text)
RETURNS SETOF accept_response
AS $BODY$
DECLARE
	accept_query text;
	host record;
BEGIN
	accept_query := format('SELECT paxos_request_accept(%s,%s,%s,%s,%s)',
							quote_literal(current_proposer_id),
							quote_literal(current_group_id),
							current_round_id,
							current_proposal_id,
							quote_literal(proposed_value));

	PERFORM paxos_broadcast_query(accept_query);

	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		RETURN QUERY
		SELECT (resp).* FROM dblink_get_result(host.connection_name) AS (resp accept_response);
	END LOOP;

	PERFORM paxos_clear_connections();
END;
$BODY$ LANGUAGE 'plpgsql';


-- Phase 3 of Paxos on the proposer
CREATE FUNCTION paxos_inform_learners(
							current_proposer_id text,
							current_group_id text,
							current_round_id bigint,
							current_proposal_id bigint,
							proposed_value text)
RETURNS void
AS $BODY$
DECLARE
	confirm_query text;
	host record;
BEGIN
	-- For now, only acceptors are learners to avoid re-connecting to failed nodes
	confirm_query := format('SELECT paxos_confirm_consensus(%s,%s,%s,%s,%s)',
							quote_literal(current_proposer_id),
							quote_literal(current_group_id),
							current_round_id,
							current_proposal_id,
							quote_literal(proposed_value));

	PERFORM paxos_broadcast_query(confirm_query);

	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		PERFORM * FROM dblink_get_result(host.connection_name, false) AS (resp boolean);
	END LOOP;

	PERFORM paxos_clear_connections();
END;
$BODY$ LANGUAGE 'plpgsql';


-- Multi-Paxos functions

-- Primitive multi-paxos implementation
CREATE FUNCTION paxos_append(
							proposer_id text,
							current_group_id text,
							proposed_value text)
RETURNS bigint
AS $BODY$
DECLARE
	current_round_id bigint;
	accepted_value text;
	value_written boolean := false;
BEGIN

	-- Start with a round ID that is higher than the highest round ID in
	-- a majority of nodes meaning higher than any round ID on which
	-- consensus was reached. Since I'll use the same nodes as acceptors,
	-- I have a good chance of getting my proposal accepted.
	SELECT paxos_max_group_round(current_group_id) INTO current_round_id;

	-- Another node could be using the same or higher round ID, but
	-- if that node reaches consensus on its value for that round we
	-- will retry paxos with round ID + 1, until we succeed.

	-- An optimization would be to resume from the competing round ID + 1,
	-- which may be relevant when another node is performing a large number
	-- of writes.

	WHILE NOT value_written LOOP
		current_round_id := current_round_id + 1;

		SELECT paxos(
						proposer_id,
						current_group_id,
						current_round_id,
						proposed_value) INTO accepted_value, value_written;
	END LOOP;

	RETURN current_round_id;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Get the highest round ID in the majority
CREATE FUNCTION paxos_max_group_round(
							current_group_id text,
							accepted_only boolean DEFAULT false)
RETURNS bigint
AS $BODY$
DECLARE
	num_hosts int;
	majority_size int;
	round_query text;
	max_round_id bigint := -1;
	num_responses int := 0;
	remote_round_id bigint;
	host record;
BEGIN
	-- Set up connections
	SELECT paxos_init_group(current_group_id) INTO num_hosts;

	majority_size = num_hosts / 2 + 1;

	round_query := format('SELECT max(round_id) '||
						  'FROM pgp_metadata.round '||
						  'WHERE group_id = %s',
						  quote_literal(current_group_id));

	IF accepted_only THEN
		round_query := round_query || ' AND value_proposer_id IS NOT NULL';
	END IF;

	-- Ask majority for highest round
	PERFORM paxos_broadcast_query(round_query);

	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		SELECT resp INTO remote_round_id
		FROM dblink_get_result(host.connection_name, false) AS (resp bigint);

		IF remote_round_id IS NOT NULL AND remote_round_id > max_round_id THEN
			max_round_id := remote_round_id;
		END IF;

		num_responses := num_responses + 1;
	END LOOP;

	PERFORM paxos_clear_connections();

	IF num_responses < majority_size THEN
		RAISE 'could only get % out of % responses', num_responses, majority_size;
	END IF;

	RETURN max_round_id;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Table replication functions

-- Apply all pending log items
CREATE FUNCTION paxos_apply_log(
							current_proposer_id text,
							current_group_id text,
							max_round_id bigint)
RETURNS bigint
AS $BODY$
DECLARE
	last_applied_round_id bigint;
	current_round_id bigint;
	query text;
	noop_written boolean;
BEGIN
	-- We don't want multiple processes applying the same log
	PERFORM pg_advisory_xact_lock(29030, hashtext(current_group_id));
	SET pg_paxos.enabled TO false;

	SELECT last_applied_round INTO current_round_id
	FROM pgp_metadata.group
	WHERE group_id = current_group_id;

	WHILE current_round_id < max_round_id LOOP

		current_round_id := current_round_id + 1;

		SELECT value INTO query
		FROM pgp_metadata.round
		WHERE group_id = current_group_id AND round_id = current_round_id AND consensus;

		IF NOT FOUND THEN
			SELECT * FROM paxos(
						current_proposer_id,
						current_group_id,
						current_round_id,
						'') INTO query, noop_written;
		END IF;

		RAISE NOTICE 'Executing: %', query;

		BEGIN
			EXECUTE query;
		EXCEPTION WHEN others THEN
			UPDATE pgp_metadata.round
			SET error = SQLERRM
			WHERE group_id = current_group_id AND round_id = current_round_id;
		END;

	END LOOP;

	UPDATE pgp_metadata.group
	SET last_applied_round = max_round_id
	WHERE group_id = current_group_id;

	SET pg_paxos.enabled TO true;
	RETURN max_round_id;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Apply all pending log items and append a new value
CREATE FUNCTION paxos_apply_and_append(
							current_proposer_id text,
							current_group_id text,
							proposed_value text)
RETURNS bigint
AS $BODY$
DECLARE
	current_round_id bigint;
	value_written bool := false;
	query text;
BEGIN
	SELECT paxos_max_group_round(current_group_id) INTO current_round_id;

	WHILE NOT value_written LOOP
		PERFORM paxos_apply_log(
						current_proposer_id,
						current_group_id,
						current_round_id);

		current_round_id := current_round_id + 1;

		SELECT paxos(
					current_proposer_id,
					current_group_id,
					current_round_id,
					proposed_value) INTO query, value_written;
	END LOOP;

	RETURN current_round_id;
END;
$BODY$ LANGUAGE 'plpgsql';


CREATE FUNCTION paxos_add_host(
							current_proposer_id text,
							current_group_id text,
							hostname text,
							port int)
RETURNS bigint
AS $BODY$
DECLARE
	current_round_id bigint;
	proposed_value text;
	accepted_value text;
	value_written boolean := false;
BEGIN
	SELECT paxos_max_group_round(current_group_id) INTO current_round_id;

	WHILE NOT value_written LOOP
		PERFORM paxos_apply_log(
						current_proposer_id,
						current_group_id,
						current_round_id);

		current_round_id := current_round_id + 1;

		proposed_value := format('INSERT INTO pgp_metadata.host VALUES (%s,%s,%s,%s)',
								 quote_literal(current_group_id),
								 quote_literal(hostname),
								 port,
								 current_round_id+1);
		SELECT paxos(
						current_proposer_id,
						current_group_id,
						current_round_id,
						proposed_value) INTO accepted_value, value_written;
	END LOOP;

	RETURN current_round_id;
END;
$BODY$ LANGUAGE 'plpgsql';


CREATE FUNCTION paxos_remove_host(
							current_proposer_id text,
							current_group_id text,
							hostname text,
							port int)
RETURNS bigint
AS $BODY$
DECLARE
	current_round_id bigint;
	proposed_value text;
	accepted_value text;
	value_written boolean := false;
BEGIN
	SELECT paxos_max_group_round(current_group_id) INTO current_round_id;

	WHILE NOT value_written LOOP
		PERFORM paxos_apply_log(
						current_proposer_id,
						current_group_id,
						current_round_id);

		current_round_id := current_round_id + 1;

		proposed_value := format('UPDATE pgp_metadata.host '||
								 'SET max_round_id = %s '||
								 'WHERE group_id = %s '||
								 'AND node_name = %s '||
								 'AND node_port = %s',
								 current_round_id,
								 quote_literal(current_group_id),
								 quote_literal(hostname),
								 port);

		SELECT paxos(
						current_proposer_id,
						current_group_id,
						current_round_id,
						proposed_value) INTO accepted_value, value_written;
	END LOOP;

	RETURN current_round_id;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Connect to majority of hosts in group
CREATE FUNCTION paxos_init_group(
							current_group_id text)
RETURNS int
AS $BODY$
DECLARE
	num_hosts int;
	majority_size int;
	num_open_connections int;
	round_query text;
	host record;
BEGIN
	-- Find the hosts for the current group
	SELECT paxos_find_hosts(current_group_id, 0) INTO num_hosts;

	majority_size = num_hosts / 2 + 1;

	-- Try to open connections to a majority of hosts
	SELECT paxos_open_connections(num_hosts) INTO num_open_connections;

	IF num_open_connections < majority_size THEN
		PERFORM paxos_close_connections();
		RAISE 'could only open % out of % connections', num_open_connections, majority_size;
	END IF;

	return num_hosts;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Take a snapshot of the current hosts in the group
CREATE FUNCTION paxos_find_hosts(
							current_group_id text,
							current_round_id bigint)
RETURNS int
AS $BODY$
DECLARE
	num_hosts int;
BEGIN

	IF NOT EXISTS (SELECT relname FROM pg_class WHERE relnamespace = pg_my_temp_schema() AND relname = 'hosts') THEN

		CREATE TEMPORARY TABLE IF NOT EXISTS hosts (
			connection_name text,
			node_name text,
			node_port int,
			connected boolean,
			participating boolean
		);

	END IF;

	TRUNCATE hosts;

	INSERT INTO hosts
	SELECT format('%s:%s', node_name, node_port) AS connection_name, node_name, node_port, false AS connected, true AS participating
	FROM pgp_metadata.host
	WHERE group_id = current_group_id
	  AND min_round_id <= current_round_id
	  AND (max_round_id IS NULL OR current_round_id <= max_round_id)
	GROUP BY node_name, node_port;

	SELECT count(*) INTO num_hosts FROM hosts;

	RETURN num_hosts;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Open at least min_connections connections
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
		 (SELECT unnest AS connected FROM unnest(dblink_get_connections())) c ON (h.connection_name = c.connected) LOOP

		IF host.connected IS NOT NULL THEN
			IF dblink_error_message(host.connection_name) = 'OK' THEN
				-- We previously opened this connection
				num_open_connections := num_open_connections + 1;
				UPDATE hosts SET connected = true WHERE connection_name = host.connection_name;
			ELSE
				-- Close connections that have errored out, will not be used next round
				RAISE NOTICE 'connection error: %', dblink_error_message(host.connection_name);
				PERFORM dblink_disconnect(host.connection_name);
				UPDATE hosts SET connected = false WHERE connection_name = host.connection_name;
			END IF;
		END IF;
	END LOOP;

	-- If we already have the minimum number of connections open, we're done
	IF num_open_connections >= min_connections THEN
		RETURN num_open_connections;
	END IF;

	-- Otherwise, try to open as many connections as possible (bias towards reads)
	FOR host IN
	SELECT h.connection_name, h.node_name, h.node_port, c.connected
	FROM hosts h LEFT OUTER JOIN
		 (SELECT unnest AS connected FROM unnest(dblink_get_connections())) c ON (h.connection_name = c.connected) LOOP

		IF host.connected IS NULL THEN
			-- Open new connection
			connection_string := format('hostaddr=%s port=%s connect_timeout=10', host.node_name, host.node_port);

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


-- Send a query to all connected hosts in the group
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


-- Clear all connections after a broadcast
CREATE FUNCTION paxos_clear_connections()
RETURNS void
AS $BODY$
DECLARE
	host record;
BEGIN
	FOR host IN SELECT * FROM hosts WHERE connected LOOP
		-- Need to call get_result until it returns NULL to clear the connection
		PERFORM * FROM dblink_get_result(host.connection_name, false) AS (resp int);
	END LOOP;
END;
$BODY$ LANGUAGE 'plpgsql';


-- Close all open connections
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
