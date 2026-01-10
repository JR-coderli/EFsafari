#!/usr/bin/env python3
"""
Unified ETL Runner
Runs Clickflare ETL first to establish base data, then MTG ETL to supplement.
"""
import os
import sys
import subprocess
from datetime import datetime
import argparse

def run_clickflare_etl(date: str) -> bool:
    """Run Clickflare ETL for the specified date."""
    print("=" * 60)
    print("STEP 1: Running Clickflare ETL (base data)")
    print("=" * 60)

    etl_dir = os.path.join(os.path.dirname(__file__), "clickflare_etl")
    etl_script = os.path.join(etl_dir, "cf_etl.py")

    if not os.path.exists(etl_script):
        print(f"Error: Clickflare ETL script not found: {etl_script}")
        return False

    try:
        result = subprocess.run(
            [sys.executable, etl_script, "-d", date],
            cwd=etl_dir,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running Clickflare ETL: {e}")
        return False

def run_mtg_etl(date: str) -> bool:
    """Run MTG ETL to supplement Clickflare data."""
    print("\n" + "=" * 60)
    print("STEP 2: Running MTG ETL (supplemental data)")
    print("=" * 60)

    etl_dir = os.path.join(os.path.dirname(__file__), "mtg_etl")
    etl_script = os.path.join(etl_dir, "mtg_etl.py")

    if not os.path.exists(etl_script):
        print(f"Warning: MTG ETL script not found: {etl_script}")
        return True  # Not an error if MTG ETL doesn't exist

    try:
        result = subprocess.run(
            [sys.executable, etl_script, "-d", date],
            cwd=etl_dir,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running MTG ETL: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Unified ETL Runner")
    parser.add_argument(
        "-d", "--date",
        type=str,
        help="Report date in YYYY-MM-DD format (default: yesterday)"
    )
    args = parser.parse_args()

    # Determine date to process
    if args.date:
        try:
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD.")
            sys.exit(1)
        report_date = args.date
    else:
        # Default to yesterday
        report_date = (datetime.now() - __import__('datetime').timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"No date specified, using yesterday: {report_date}")

    # Run ETLs in sequence
    success = True

    # Step 1: Clickflare (base data)
    if not run_clickflare_etl(report_date):
        print("\nERROR: Clickflare ETL failed!")
        success = False

    # Step 2: MTG (supplemental data)
    if not run_mtg_etl(report_date):
        print("\nWARNING: MTG ETL failed (but Clickflare succeeded)")
        # Don't fail the whole job if MTG fails

    print("\n" + "=" * 60)
    if success:
        print("ETL completed successfully!")
    else:
        print("ETL completed with errors!")
    print("=" * 60)

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
