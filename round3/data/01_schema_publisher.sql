-- =============================================================================
-- Round 3 - PUBLISHER schema.  PostgreSQL 18 only.
--
-- Run in pgAdmin: right-click the 'pub' database -> Query Tool,
--   then File -> Open this file, and press F5 to execute.
--
-- Replaces the Round 2 single-payload-column table with a wide, mixed-type
-- table carrying three secondary indexes, so that WAL generated per row and
-- apply cost per row both resemble a real OLTP table. Index maintenance is a
-- large part of real apply cost and was absent from every earlier round.
-- =============================================================================

DO $$
DECLARE v int := current_setting('server_version_num')::int / 10000;
BEGIN
    IF v <> 18 THEN
        RAISE EXCEPTION 'Round 3 requires PostgreSQL 18, found %. Do not mix versions.', v;
    END IF;
END $$;

DROP TABLE IF EXISTS ingest_data CASCADE;

CREATE TABLE ingest_data (
    id              bigserial     PRIMARY KEY,
    account_id      integer       NOT NULL,
    region_code     varchar(8)    NOT NULL,
    status          varchar(16)   NOT NULL,
    event_type      varchar(32)   NOT NULL,
    quantity        integer       NOT NULL,
    unit_price      numeric(12,4) NOT NULL,
    total_amount    numeric(14,4) NOT NULL,
    external_ref    uuid          NOT NULL,
    attributes      jsonb         NOT NULL,
    description     text          NOT NULL,   -- variable-width payload column
    created_at      timestamptz   NOT NULL DEFAULT now(),
    updated_at      timestamptz   NOT NULL DEFAULT now()
);

CREATE INDEX idx_ingest_account   ON ingest_data (account_id);
CREATE INDEX idx_ingest_created   ON ingest_data (created_at);
CREATE INDEX idx_ingest_status_rc ON ingest_data (status, region_code);

ALTER TABLE ingest_data REPLICA IDENTITY DEFAULT;   -- uses the primary key

DROP PUBLICATION IF EXISTS mypub;
CREATE PUBLICATION mypub FOR TABLE ingest_data;

-- Called by orchestrate.py before every level to return to a defined state.
-- TRUNCATE is replicated, so the subscriber empties too.
CREATE OR REPLACE FUNCTION reset_workload_table() RETURNS void AS $$
BEGIN
    TRUNCATE TABLE ingest_data;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- Verification - one query, one result grid. Paste the output into your run log.
-- ---------------------------------------------------------------------------
SELECT version()                                        AS full_version,
       current_setting('server_version')                AS pg_version,
       current_setting('wal_level')                     AS wal_level,
       current_setting('wal_compression')               AS wal_compression,
       current_setting('synchronous_commit')            AS synchronous_commit,
       current_setting('max_wal_size')                  AS max_wal_size,
       current_setting('shared_buffers')                AS shared_buffers,
       (SELECT count(*) FROM pg_publication
         WHERE pubname = 'mypub')                       AS publication_ok,
       (SELECT count(*) FROM pg_publication_tables
         WHERE pubname = 'mypub')                       AS published_tables,
       (SELECT count(*) FROM pg_indexes
         WHERE tablename = 'ingest_data')               AS index_count,
       (to_regproc('reset_workload_table') IS NOT NULL) AS reset_function_ok;
