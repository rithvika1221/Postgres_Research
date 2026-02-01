-- Create database if it does not exist
CREATE DATABASE sub;

-- Connect to testdb
\c sub;

-- Table must already exist (schema must match publisher)
CREATE TABLE IF NOT EXISTS ingest_data (
    id BIGSERIAL PRIMARY KEY,
    payload TEXT,
    big_payload TEXT,
    created_at TIMESTAMP DEFAULT now()
);


-- 1) Drop subscription if it already exists (safe reset)
DROP SUBSCRIPTION IF EXISTS mysub;

-- 2) Create subscription
CREATE SUBSCRIPTION mysub
CONNECTION 'host=pg-source2-westus2.postgres.database.azure.com port=5432 dbname=pub user=postgres password=MathCodeLab@2025'
PUBLICATION mypub;

