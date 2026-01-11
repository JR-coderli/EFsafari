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

    # Run ETLs in sequence and track results
    results = {
        "clickflare": False,
        "mtg": False
    }

    # Step 1: Clickflare (base data)
    results["clickflare"] = run_clickflare_etl(report_date)
    if not results["clickflare"]:
        print("\nERROR: Clickflare ETL failed!")

    # Step 2: MTG (supplemental data)
    results["mtg"] = run_mtg_etl(report_date)
    if not results["mtg"]:
        print("\nWARNING: MTG ETL failed!")

    # Summary
    print("\n" + "=" * 60)
    print("ETL SUMMARY")
    print("=" * 60)
    print(f"Report Date:    {report_date}")
    print(f"Clickflare ETL: {'✓ SUCCESS' if results['clickflare'] else '✗ FAILED'}")
    print(f"MTG ETL:        {'✓ SUCCESS' if results['mtg'] else '✗ FAILED'}")
    print("=" * 60)

    # Only report overall success if ALL ETLs succeeded
    all_success = all(results.values())
    if all_success:
        print("ETL completed successfully!")
    else:
        print("ETL completed with errors!")
    print("=" * 60)

    sys.exit(0 if all_success else 1)

if __name__ == "__main__":
    main()
