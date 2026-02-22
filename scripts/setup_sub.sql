-- Create database if it does not exist
CREATE DATABASE sub;



-- Connect to testdb
\c sub;

ingest_data


-- 1) Drop subscription if it already exists (safe reset)
DROP SUBSCRIPTION IF EXISTS mysub;

CREATE SUBSCRIPTION mysub
CONNECTION 'host=172.16.0.4 port=5432 dbname=pub user=postgres password=Aarush@123'
PUBLICATION mypub;



DROP TABLE IF EXISTS ingest_data;

-- 2) Create table used for replication experiments
CREATE TABLE ingest_data (
    id BIGSERIAL PRIMARY KEY,
    payload TEXT,
    big_payload TEXT,
    created_at TIMESTAMP DEFAULT now()
);




select  * from ingest_data


SELECT
  subname,
  subenabled,
  subconninfo,
  subslotname,
  subsynccommit
FROM pg_subscription;




SELECT
  subname,
  pid,
  relid::regclass AS table_name,
  received_lsn,
  latest_end_lsn,
  last_msg_send_time,
  last_msg_receipt_time,
  latest_end_time
FROM pg_stat_subscription;



SELECT
  srsubid,
  srrelid::regclass AS table_name,
  srsubstate
FROM pg_subscription_rel
ORDER BY 2;



SELECT pid, backend_type, application_name, state, query
FROM pg_stat_activity
WHERE backend_type ILIKE '%logical replication%';




SELECT subname, subenabled, subslotname, subconninfo
FROM pg_subscription;

SELECT *
FROM pg_stat_subscription;


SELECT srrelid::regclass AS table_name, srsubstate
FROM pg_subscription_rel
ORDER BY 1;



SELECT current_database();
SELECT subname, subenabled, subslotname FROM pg_subscription;
SELECT * FROM pg_stat_subscription;



SELECT srrelid::regclass AS table_name, srsubstate
FROM pg_subscription_rel
ORDER BY 1;


ALTER SUBSCRIPTION mysub REFRESH PUBLICATION;









