#!/bin/bash
# Development startup script for Bicode Backend API
# Features:
# - Checks and kills processes using the port
# - Starts with multiple workers for better concurrency
# - Preserves reload mode for development

set -e

# Configuration
HOST="0.0.0.0"
PORT=8000
WORKERS=2  # Development: use 2 workers (production should use more)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Bicode Backend API Development Startup ===${NC}"

# Get script directory
cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo -e "${GREEN}Activating virtual environment...${NC}"
    source venv/bin/activate
else
    echo -e "${YELLOW}Warning: venv not found, using system Python${NC}"
fi

# Check if port is in use
if command -v lsof >/dev/null 2>&1; then
    PID=$(lsof -ti:$PORT 2>/dev/null || true)
    if [ -n "$PID" ]; then
        echo -e "${YELLOW}Port $PORT is in use (PID: $PID), killing...${NC}"
        kill -9 $PID 2>/dev/null || true
        sleep 1
    fi
elif command -v netstat >/dev/null 2>&1; then
    PID=$(netstat -ano | findstr ":$PORT" | awk '{print $5}' | head -1 || true)
    if [ -n "$PID" ]; then
        echo -e "${YELLOW}Port $PORT is in use (PID: $PID), killing...${NC}"
        kill -9 $PID 2>/dev/null || true
        sleep 1
    fi
fi

# Start with uvicorn
echo -e "${GREEN}Starting uvicorn with $WORKERS workers...${NC}"
echo -e "${GREEN}Host: $HOST, Port: $PORT${NC}"
echo ""

uvicorn api.main:app \
    --host $HOST \
    --port $PORT \
    --workers $WORKERS \
    --log-level info \
    --reload
