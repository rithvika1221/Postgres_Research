#!/usr/bin/env python3
"""
Quick Analysis of Replication Monitoring Results

Generates summary statistics and simple visualizations from CSV files
produced by replication_monitor.py

Usage:
    python analyze_results.py
"""

import pandas as pd
import sys
from pathlib import Path

# CSV files to analyze
CSV_FILES = [
    'replication_lag.csv',
    'wal_stats.csv',
    'activity_stats.csv',
    'database_stats.csv',
    'subscription_stats.csv'
]

def analyze_file(filename):
    """Generate summary statistics for a CSV file"""

    if not Path(filename).exists():
        print(f"⚠ File not found: {filename}")
        return

    try:
        df = pd.read_csv(filename)

        if df.empty:
            print(f"⚠ No data in {filename}")
            return

        print("\n" + "="*70)
        print(f"Analysis: {filename}")
        print("="*70)
        print(f"Total samples: {len(df)}")
        print(f"Columns: {', '.join(df.columns)}")

        # Convert timestamp to datetime if present
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            duration = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
            print(f"Duration: {duration:.0f} seconds ({duration/60:.1f} minutes)")
            print(f"Start: {df['timestamp'].min()}")
            print(f"End: {df['timestamp'].max()}")

        print("\n" + "-"*70)
        print("Summary Statistics:")
        print("-"*70)

        # Numeric columns only
        numeric_cols = df.select_dtypes(include=['number']).columns

        if len(numeric_cols) > 0:
            summary = df[numeric_cols].describe()
            print(summary.to_string())

        # File-specific analysis
        if 'lag_bytes' in df.columns:
            print("\n" + "-"*70)
            print("Replication Lag Analysis:")
            print("-"*70)
            lag_mb = df['lag_bytes'] / (1024*1024)
            print(f"Average lag: {lag_mb.mean():.2f} MB")
            print(f"Max lag: {lag_mb.max():.2f} MB")
            print(f"Min lag: {lag_mb.min():.2f} MB")
            print(f"Std dev: {lag_mb.std():.2f} MB")

        if 'wal_bytes_per_sec' in df.columns:
            print("\n" + "-"*70)
            print("WAL Generation Rate:")
            print("-"*70)
            wal_mb_s = df['wal_bytes_per_sec'] / (1024*1024)
            print(f"Average: {wal_mb_s.mean():.2f} MB/s")
            print(f"Max: {wal_mb_s.max():.2f} MB/s")
            print(f"Total WAL generated: {(wal_mb_s.sum() * 5):.2f} MB")  # Assuming 5s interval

        if 'tps' in df.columns:
            print("\n" + "-"*70)
            print("Transaction Throughput:")
            print("-"*70)
            print(f"Average TPS: {df['tps'].mean():.1f}")
            print(f"Max TPS: {df['tps'].max():.1f}")
            print(f"Min TPS: {df['tps'].min():.1f}")
            print(f"Total transactions: {df['tps'].sum() * 5:.0f}")  # Assuming 5s interval

        if 'cache_hit_ratio' in df.columns:
            print("\n" + "-"*70)
            print("Cache Performance:")
            print("-"*70)
            print(f"Average cache hit ratio: {df['cache_hit_ratio'].mean():.2f}%")
            print(f"Min cache hit ratio: {df['cache_hit_ratio'].min():.2f}%")

        if 'subscriber_lag_bytes' in df.columns:
            print("\n" + "-"*70)
            print("Subscriber Lag Analysis:")
            print("-"*70)
            sub_lag_mb = df['subscriber_lag_bytes'] / (1024*1024)
            print(f"Average lag: {sub_lag_mb.mean():.2f} MB")
            print(f"Max lag: {sub_lag_mb.max():.2f} MB")

        if 'active_connections' in df.columns:
            print("\n" + "-"*70)
            print("Connection Activity:")
            print("-"*70)
            print(f"Average active connections: {df['active_connections'].mean():.1f}")
            print(f"Max active connections: {df['active_connections'].max():.0f}")
            print(f"Average total connections: {df['total_connections'].mean():.1f}")

    except Exception as e:
        print(f"✗ Error analyzing {filename}: {e}")

def main():
    print("\n" + "="*70)
    print("Replication Monitoring Results Analysis")
    print("="*70)

    found_files = 0
    for filename in CSV_FILES:
        if Path(filename).exists():
            found_files += 1
            analyze_file(filename)

    if found_files == 0:
        print("\n✗ No CSV files found!")
        print("  Make sure you're in the directory with the monitoring output")
        print("  Expected files:")
        for f in CSV_FILES:
            print(f"    - {f}")
        sys.exit(1)

    print("\n" + "="*70)
    print(f"Analysis complete! Analyzed {found_files} files.")
    print("="*70)
    print("\nNext steps:")
    print("  1. Import CSV files into Excel for detailed analysis")
    print("  2. Create time-series plots (lag vs time, TPS vs time)")
    print("  3. Calculate correlations (e.g., TPS vs lag)")
    print("  4. Generate graphs for your research paper")
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user")
        sys.exit(0)
