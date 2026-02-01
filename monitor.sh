echo "timestamp,application_name,sender_lag_bytes" > sender_lag.csv

while true; do
  ts=$(date "+%Y-%m-%d %H:%M:%S")

  psql -p 5432 -h pg-source2-westus2.postgres.database.azure.com -d postgres -At -c "
  SELECT
    '$ts',
    application_name,
    pg_wal_lsn_diff(sent_lsn, replay_lsn)
  FROM pg_stat_replication;" >> sender_lag.csv

  sleep 5
done
