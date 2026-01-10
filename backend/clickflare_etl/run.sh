#!/bin/bash
# Clickflare ETL Runner

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Parse command line arguments
DATE_ARG=""
CONFIG_ARG=""

while getopts "d:c:" opt; do
    case $opt in
        d)
            DATE_ARG="-d $OPTARG"
            ;;
        c)
            CONFIG_ARG="-c $OPTARG"
            ;;
        \?)
            echo "Usage: $0 [-d YYYY-MM-DD] [-c config.yaml]"
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
echo "Starting Clickflare ETL..."
python cf_etl.py $DATE_ARG $CONFIG_ARG

EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "Clickflare ETL completed successfully"
else
    echo "Clickflare ETL failed with exit code $EXIT_CODE"
fi

exit $EXIT_CODE
