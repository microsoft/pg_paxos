/*
 * paxos_execute executes a query locally, by-passing the Paxos query logging logic.
 */
CREATE FUNCTION paxos_execute(INTERNAL)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', $$paxos_execute$$;
