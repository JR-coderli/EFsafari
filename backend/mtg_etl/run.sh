#!/bin/bash
# MTG ETL Runner Script
# Usage: ./run.sh [-d YYYY-MM-DD]

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default values
DATE_ARG=""

# Parse arguments
while getopts "d:" opt; do
    case $opt in
        d)
            DATE_ARG="-d $OPTARG"
            ;;
        \?)
            echo "Usage: $0 [-d YYYY-MM-DD]"
            exit 1
            ;;
    esac
done

# Activate virtual environment if exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Run ETL
echo "Starting MTG ETL..."
python mtg_etl.py $DATE_ARG

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "ETL completed successfully"
else
    echo "ETL failed with exit code $EXIT_CODE"
fi

exit $EXIT_CODE
