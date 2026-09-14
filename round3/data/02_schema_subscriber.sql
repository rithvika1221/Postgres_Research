-- =============================================================================
-- Round 3 - SUBSCRIBER schema.  PostgreSQL 18 only.
--
-- !! READ THIS BEFORE RUNNING - the file is in FOUR parts and CANNOT be run
-- !! all at once in pgAdmin.
--
-- pgAdmin sends a multi-statement execution as ONE implicit transaction, but
-- CREATE SUBSCRIPTION and DROP SUBSCRIPTION create and drop a replication slot
-- on the publisher, which PostgreSQL refuses to do inside a transaction block.
-- Running the whole file gives:
--     ERROR: CREATE SUBSCRIPTION ... cannot run inside a transaction block
-- and because the transaction aborts, the table and indexes are rolled back too.
--
-- Instead: select each PART and press F5, in order. Parts 2 and 3 must each be
-- run completely alone.
--
-- FIRST replace PUBLISHER_PRIVATE_IP and ******** in PART 3.
--
-- Schema and indexes match the publisher exactly. Indexes are created here on
-- purpose: index maintenance is part of apply cost and omitting them would
-- understate how much work the apply worker does.
-- =============================================================================


-- =============================================================================
-- PART 1 - version guard, table and indexes.  Select all of PART 1, press F5.
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
    id              bigint        PRIMARY KEY,
    account_id      integer       NOT NULL,
    region_code     varchar(8)    NOT NULL,
    status          varchar(16)   NOT NULL,
    event_type      varchar(32)   NOT NULL,
    quantity        integer       NOT NULL,
    unit_price      numeric(12,4) NOT NULL,
    total_amount    numeric(14,4) NOT NULL,
    external_ref    uuid          NOT NULL,
    attributes      jsonb         NOT NULL,
    description     text          NOT NULL,
    created_at      timestamptz   NOT NULL,
    updated_at      timestamptz   NOT NULL
);

CREATE INDEX idx_ingest_account   ON ingest_data (account_id);
CREATE INDEX idx_ingest_created   ON ingest_data (created_at);
CREATE INDEX idx_ingest_status_rc ON ingest_data (status, region_code);


-- =============================================================================
-- PART 2 - drop any existing subscription.  Select ONLY the next line, press F5.
-- Skip this on a first install; it is here for re-runs.
-- =============================================================================

DROP SUBSCRIPTION IF EXISTS mysub;


-- =============================================================================
-- PART 3 - create the subscription.  Select ONLY this statement, press F5.
--
-- streaming is set EXPLICITLY rather than inherited, so the value is a recorded
-- experimental choice rather than a default that might change. Keep it the same
-- for every run in the study and report it in Methods.
--   off      = buffer each transaction until commit, then apply
--   on       = stream in-progress transactions to a temporary file
--   parallel = apply large in-progress transactions in parallel workers
-- =============================================================================

CREATE SUBSCRIPTION mysub
    CONNECTION 'host=PUBLISHER_PRIVATE_IP port=5432 dbname=pub user=repl_user password=********'
    PUBLICATION mypub
    WITH (
        copy_data          = false,
        streaming          = off,
        synchronous_commit = off,
        binary             = false
    );


-- =============================================================================
-- PART 4 - verification.  One query, one result grid.
-- Paste the output into your run log.
-- =============================================================================

SELECT version()                                                  AS full_version,
       current_setting('server_version')                          AS pg_version,
       current_setting('max_logical_replication_workers')         AS lr_workers,
       current_setting('max_parallel_apply_workers_per_subscription') AS parallel_apply,
       current_setting('max_sync_workers_per_subscription')       AS sync_workers,
       current_setting('shared_buffers')                          AS shared_buffers,
       (SELECT subenabled FROM pg_subscription
         WHERE subname = 'mysub')                                 AS subscription_enabled,
       (SELECT substream::text FROM pg_subscription
         WHERE subname = 'mysub')                                 AS streaming_mode,
       (SELECT count(*) FROM pg_indexes
         WHERE tablename = 'ingest_data')                         AS index_count;
