CREATE DATABASE pub;
-- Connect to the database
\c pub;

-- 1) Drop table if it exists (optional, for clean experiments)
DROP TABLE IF EXISTS ingest_data;

-- 2) Create table used for replication experiments
CREATE TABLE ingest_data (
    id BIGSERIAL PRIMARY KEY,
    payload TEXT,
    big_payload TEXT,
    created_at TIMESTAMP DEFAULT now()
);

-- 3) Drop publication if it already exists (safe reset)
DROP PUBLICATION IF EXISTS mypub;

-- 4) Create publication for this table ONLY
CREATE PUBLICATION mypub
FOR TABLE ingest_data;


select  * from ingest_data


INSERT INTO ingest_data (payload, big_payload)
SELECT
  'payload_' || g,
  'big_payload_' || g
FROM generate_series(1, 100) AS g;


INSERT INTO ingest_data (payload, big_payload)
SELECT
  'payload_' || g,
  'big_payload_' || g
FROM generate_series(1, 100) AS g;


SELECT
  pid,
  client_addr,
  application_name,
  state,
  sync_state,
  sent_lsn,
  write_lsn,
  flush_lsn,
  replay_lsn,
  write_lag,
  flush_lag,
  replay_lag
FROM pg_stat_replication;




SELECT
  slot_name,
  slot_type,
  active,
  active_pid,
  database,
  restart_lsn,
  confirmed_flush_lsn,
  wal_status
FROM pg_replication_slots;


SELECT
  p.pubname,
  schemaname,
  tablename
FROM pg_publication_tables p
ORDER BY 1,2,3;


SHOW wal_level;
SHOW max_replication_slots;
SHOW max_wal_senders;


INSERT INTO ingest_data (payload, big_payload)
VALUES ('rep_test', 'from publisher at ' || now()::text);



SELECT pubname, schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'mypub'
ORDER BY 1,2,3;



SELECT srrelid::regclass AS table_name, srsubstate
FROM pg_subscription_rel
ORDER BY 1;


SELECT pubname, schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'mypub'
ORDER BY 1,2,3;






