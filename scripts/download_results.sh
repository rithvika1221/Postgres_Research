#!/bin/bash
# Download Results Script
# Downloads monitoring CSV files from both VMs to your local machine

set -e  # Exit on error

# =============================================================================
# CONFIGURATION - UPDATE THESE
# =============================================================================

PUBLISHER_VM="postgres@pg-source2-westus2.postgres.database.azure.com"
SUBSCRIBER_VM="postgres@pg-replica2-westus2.postgres.database.azure.com"  # UPDATE THIS

# =============================================================================
# DOWNLOAD
# =============================================================================

if [ $# -eq 0 ]; then
    echo "Usage: $0 <experiment_name>"
    echo ""
    echo "Example: $0 test_1000tps"
    echo ""
    echo "This will:"
    echo "  1. Download all CSV files from both VMs"
    echo "  2. Create directory: experiments/<experiment_name>/"
    echo "  3. Move CSV files into that directory"
    exit 1
fi

EXPERIMENT_NAME=$1
EXPERIMENT_DIR="experiments/${EXPERIMENT_NAME}"

echo "=========================================="
echo "Downloading Results: $EXPERIMENT_NAME"
echo "=========================================="
echo ""

# Create experiment directory
mkdir -p "$EXPERIMENT_DIR"

echo "Step 1: Downloading from Publisher VM..."
echo "-----------------------------------------"

# Download publisher CSV files
scp "$PUBLISHER_VM:~/monitoring/publisher_*.csv" "$EXPERIMENT_DIR/" 2>/dev/null || {
    echo "⚠ Warning: Could not download publisher CSV files"
    echo "  Make sure the monitor ran and created CSV files"
}

# Count publisher files
PUB_COUNT=$(ls "$EXPERIMENT_DIR"/publisher_*.csv 2>/dev/null | wc -l)
echo "✓ Downloaded $PUB_COUNT publisher CSV files"

echo ""
echo "Step 2: Downloading from Subscriber VM..."
echo "------------------------------------------"

# Download subscriber CSV files
scp "$SUBSCRIBER_VM:~/monitoring/subscriber_*.csv" "$EXPERIMENT_DIR/" 2>/dev/null || {
    echo "⚠ Warning: Could not download subscriber CSV files"
    echo "  Make sure the monitor ran and created CSV files"
}

# Count subscriber files
SUB_COUNT=$(ls "$EXPERIMENT_DIR"/subscriber_*.csv 2>/dev/null | wc -l)
echo "✓ Downloaded $SUB_COUNT subscriber CSV files"

echo ""
echo "Step 3: Downloading log files (if any)..."
echo "------------------------------------------"

# Download monitor logs
scp "$PUBLISHER_VM:~/monitoring/monitor.log" "$EXPERIMENT_DIR/publisher_monitor.log" 2>/dev/null || true
scp "$SUBSCRIBER_VM:~/monitoring/monitor.log" "$EXPERIMENT_DIR/subscriber_monitor.log" 2>/dev/null || true

echo ""
echo "=========================================="
echo "Download Complete!"
echo "=========================================="
echo ""
echo "Results saved to: $EXPERIMENT_DIR/"
echo ""
echo "Files downloaded:"
ls -lh "$EXPERIMENT_DIR"/*.csv 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}'
echo ""
echo "Next steps:"
echo "  1. Analyze results:"
echo "     cd $EXPERIMENT_DIR"
echo "     python ../../analyze_results.py"
echo ""
echo "  2. Import CSV files into Excel for graphs"
echo ""
echo "  3. Clean up VM CSV files (optional):"
echo "     ssh $PUBLISHER_VM 'rm ~/monitoring/*.csv'"
echo "     ssh $SUBSCRIBER_VM 'rm ~/monitoring/*.csv'"
echo ""
