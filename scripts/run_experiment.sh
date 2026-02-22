#!/bin/bash
# Automated Experiment Runner
# Runs pgbench workload while monitoring replication metrics
#
# Usage:
#   ./run_experiment.sh <experiment_name> <pgbench_params>
#
# Examples:
#   ./run_experiment.sh test_1000tps "-c 50 -j 8 -T 600 -R 1000"
#   ./run_experiment.sh test_max_throughput "-c 100 -j 16 -T 600"

set -e  # Exit on error

# =============================================================================
# CONFIGURATION
# =============================================================================

PGHOST="pg-source2-westus2.postgres.database.azure.com"
PGDATABASE="pub"
PGUSER="postgres"
# PGPASSWORD should be set in environment

# Check password
if [ -z "$PGPASSWORD" ]; then
    echo "Error: PGPASSWORD environment variable not set"
    echo "Run: export PGPASSWORD='your_password'"
    exit 1
fi

# =============================================================================
# PARSE ARGUMENTS
# =============================================================================

if [ $# -lt 2 ]; then
    echo "Usage: $0 <experiment_name> <pgbench_params>"
    echo ""
    echo "Examples:"
    echo "  $0 test_1000tps \"-c 50 -j 8 -T 600 -R 1000\""
    echo "  $0 test_max_load \"-c 100 -j 16 -T 600\""
    echo ""
    echo "Common pgbench parameters:"
    echo "  -c N    : Number of concurrent clients"
    echo "  -j N    : Number of worker threads"
    echo "  -T N    : Duration in seconds"
    echo "  -R N    : Target transaction rate (TPS)"
    exit 1
fi

EXPERIMENT_NAME=$1
PGBENCH_PARAMS=$2

# =============================================================================
# SETUP EXPERIMENT DIRECTORY
# =============================================================================

EXPERIMENT_DIR="experiments/${EXPERIMENT_NAME}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$EXPERIMENT_DIR"

echo "=========================================="
echo "PostgreSQL Replication Experiment"
echo "=========================================="
echo "Experiment: $EXPERIMENT_NAME"
echo "Directory: $EXPERIMENT_DIR"
echo "pgbench params: $PGBENCH_PARAMS"
echo "=========================================="
echo ""

# Save experiment metadata
cat > "$EXPERIMENT_DIR/experiment_info.txt" <<EOF
Experiment: $EXPERIMENT_NAME
Start Time: $(date)
pgbench Parameters: $PGBENCH_PARAMS
Host: $PGHOST
Database: $PGDATABASE
User: $PGUSER
EOF

# =============================================================================
# START MONITORING
# =============================================================================

echo "Starting replication monitor..."

# Update replication_monitor.py to output to experiment directory
# Create a temporary copy with updated OUTPUT_DIR
sed "s|OUTPUT_DIR = '.'|OUTPUT_DIR = '$EXPERIMENT_DIR'|" replication_monitor.py > "$EXPERIMENT_DIR/monitor_temp.py"

# Start monitor in background
python "$EXPERIMENT_DIR/monitor_temp.py" > "$EXPERIMENT_DIR/monitor.log" 2>&1 &
MONITOR_PID=$!

echo "Monitor started (PID: $MONITOR_PID)"
echo "Waiting 5 seconds for monitor initialization..."
sleep 5

# =============================================================================
# RUN PGBENCH
# =============================================================================

echo ""
echo "Starting pgbench workload..."
echo "Command: pgbench $PGBENCH_PARAMS -h $PGHOST -U $PGUSER $PGDATABASE"
echo ""

# Run pgbench and save output
pgbench $PGBENCH_PARAMS -h "$PGHOST" -U "$PGUSER" "$PGDATABASE" \
    > "$EXPERIMENT_DIR/pgbench_output.txt" 2>&1

PGBENCH_EXIT=$?

echo ""
echo "pgbench completed (exit code: $PGBENCH_EXIT)"

# =============================================================================
# WAIT FOR LAG TO SETTLE
# =============================================================================

echo ""
echo "Waiting 60 seconds for replication lag to settle..."
sleep 60

# =============================================================================
# STOP MONITORING
# =============================================================================

echo "Stopping monitor..."
kill -INT $MONITOR_PID 2>/dev/null || true

# Wait for monitor to finish writing
sleep 5

# =============================================================================
# ANALYZE RESULTS
# =============================================================================

echo ""
echo "Analyzing results..."
cd "$EXPERIMENT_DIR"
python ../analyze_results.py > analysis_summary.txt 2>&1
cd - > /dev/null

# =============================================================================
# GENERATE SUMMARY
# =============================================================================

echo ""
echo "=========================================="
echo "Experiment Complete!"
echo "=========================================="
echo ""
echo "Results saved to: $EXPERIMENT_DIR"
echo ""
echo "Output files:"
ls -lh "$EXPERIMENT_DIR"/*.csv "$EXPERIMENT_DIR"/*.txt 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}'
echo ""
echo "Quick summary:"
echo "----------------------------------------"

# Extract key metrics from pgbench output
if [ -f "$EXPERIMENT_DIR/pgbench_output.txt" ]; then
    echo "pgbench results:"
    grep -E "tps =|latency average|number of transactions" "$EXPERIMENT_DIR/pgbench_output.txt" | sed 's/^/  /'
fi

echo ""
echo "To view detailed analysis:"
echo "  cat $EXPERIMENT_DIR/analysis_summary.txt"
echo ""
echo "To import into Excel:"
echo "  Open Excel -> Import Data -> Select CSV files in $EXPERIMENT_DIR"
echo ""

# Cleanup temp file
rm -f "$EXPERIMENT_DIR/monitor_temp.py"

echo "Done!"
