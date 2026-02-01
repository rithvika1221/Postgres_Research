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
