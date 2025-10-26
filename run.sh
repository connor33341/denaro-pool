#!/bin/bash

# Stellaris Pool Runner Script
# This script starts the Stellaris mining pool server

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Stellaris Mining Pool${NC}"
echo -e "${GREEN}========================================${NC}"

# Check for .env file
if [ ! -f ".env" ]; then
    echo -e "${RED}Warning: .env file not found!${NC}"
    echo -e "${YELLOW}Please create a .env file with the following variables:${NC}"
    echo "  POOL_WALLET_ADDRESS=<your_pool_wallet_address>"
    echo "  POOL_PRIVATE_KEY=<your_pool_private_key>"
    echo "  STELLARIS_NODE_URL=<stellaris_node_url> (optional)"
    echo "  POOL_FEE_PERCENT=2.0 (optional)"
    echo "  FINDER_BONUS_PERCENT=5.0 (optional)"
    echo "  MIN_PAYOUT_AMOUNT=1.0 (optional)"
    echo "  DATA_DIR=./data/pool (optional)"
    exit 1
fi

# Create data directory if it doesn't exist
DATA_DIR=${DATA_DIR:-"./data/pool"}
if [ ! -d "$DATA_DIR" ]; then
    echo -e "${YELLOW}Creating data directory: $DATA_DIR${NC}"
    mkdir -p "$DATA_DIR"
fi

# Get host and port from arguments or use defaults
HOST=${1:-"0.0.0.0"}
PORT=${2:-"8572"}

echo -e "${GREEN}Starting Stellaris Pool on $HOST:$PORT${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
echo ""

# Run the FastAPI application with uvicorn
python -m uvicorn stellaris_pool.main:app --host "$HOST" --port "$PORT" --reload
