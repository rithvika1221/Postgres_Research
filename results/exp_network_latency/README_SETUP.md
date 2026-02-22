# Network Latency Experiment — Setup Guide

## Overview
This experiment measures how network latency between publisher and subscriber affects replication lag.
We test 5 latency levels: **0ms, 10ms, 25ms, 50ms, 100ms** while ramping TPS from 100 to 5,000.

## Prerequisites
- Both VMs in the **same Azure region** (East US recommended for lowest baseline latency ~1ms)
- PostgreSQL configured with logical replication on both VMs
- Python with psycopg2-binary and openpyxl installed on both VMs

## Step 1: Install clumsy on the Subscriber VM

1. Download clumsy from: https://jagt.github.io/clumsy/
2. Extract the zip file to a folder (e.g., `C:\clumsy`)
3. Run `clumsy.exe` as **Administrator** (right-click → Run as administrator)

## Step 2: Configure clumsy

In the clumsy window:
- **Filter:** `ip.DstAddr == 172.16.0.4 or ip.SrcAddr == 172.16.0.4`
  (Replace 172.16.0.4 with your publisher's IP address)
- **Functions:** Check only **Lag** (uncheck everything else)
- **Lag → Inbound:** Set to your desired latency (e.g., 50)
- **Lag → Outbound:** Set to your desired latency (e.g., 50)
- Click **Start** to begin adding latency

> **Note:** Setting both inbound and outbound to 50ms gives ~100ms round-trip time.
> For the experiment, set each direction to HALF the desired RTT:
> - 0ms RTT → Don't start clumsy
> - 10ms RTT → Set Lag to 5ms each direction
> - 25ms RTT → Set Lag to 12ms each direction (or 13ms)
> - 50ms RTT → Set Lag to 25ms each direction
> - 100ms RTT → Set Lag to 50ms each direction

## Step 3: Verify Latency

Before each run, verify the latency from the subscriber to publisher:
```powershell
# On the subscriber VM:
ping 172.16.0.4 -n 10
```
Record the average ping time. It should be close to your configured latency.

## Step 4: Run the Experiment

You run the experiment **5 times**, once per latency level. Before each run:
1. Set clumsy to the desired latency (or stop it for 0ms)
2. Verify with ping
3. Clean the ingest_data table on publisher:
   ```sql
   TRUNCATE ingest_data;
   ```
4. Run the scripts

### On the Publisher VM:
```powershell
# Terminal 1: Start the load generator
python load_generator.py --latency 50ms --workers 16

# Terminal 2: Start the publisher monitor
python monitor_publisher_xlsx.py --latency 50ms
```

### On the Subscriber VM:
```powershell
# Terminal 1: Start the subscriber monitor
python monitor_subscriber_xlsx.py --latency 50ms
```

### Repeat for each latency level:
```
Run 1: --latency 0ms    (clumsy OFF)
Run 2: --latency 10ms   (clumsy Lag: 5ms each direction)
Run 3: --latency 25ms   (clumsy Lag: 12ms each direction)
Run 4: --latency 50ms   (clumsy Lag: 25ms each direction)
Run 5: --latency 100ms  (clumsy Lag: 50ms each direction)
```

## Step 5: Output Files

Each run produces 3 files with the latency label in the filename:
- `publisher_metrics_latency_50ms_20260215_143000.xlsx`
- `subscriber_metrics_latency_50ms_20260215_143000.xlsx`

All Excel files include a "Network Latency" column in every sheet for easy filtering.

## Tips
- Always run experiments in order from lowest to highest latency
- Wait 30 seconds between runs for replication to catch up
- Keep clumsy running for the ENTIRE duration of each run
- If you need to change the number of workers, use `--workers N` on the load generator
- Default is 16 workers — a good middle ground from your concurrency experiments

## Troubleshooting

**clumsy not working?**
- Make sure you're running as Administrator
- Check the filter matches your publisher's actual IP
- Try using WinDivert directly if clumsy has issues

**Latency too variable?**
- Close other applications on the VM
- Use a dedicated Azure VM size (avoid burstable B-series)
- Run during off-peak hours

**Connection timeout with high latency?**
- Increase PostgreSQL's `wal_sender_timeout` on the publisher
- Increase `wal_receiver_timeout` on the subscriber
- Both should be > 2x your max latency (e.g., 60s for 100ms experiments)
