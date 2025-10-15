#!/bin/bash

# Stellaris Mining Pool Setup Script

set -e

echo "🚀 Stellaris Mining Pool Setup"
echo "=============================="
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found. Creating from template..."
    cp .env.example .env
    echo "✅ Created .env file. Please edit it with your configuration:"
    echo "   - POOL_WALLET_ADDRESS"
    echo "   - POOL_PRIVATE_KEY"
    echo "   - STELLARIS_NODE_URL"
    echo ""
    echo "❌ Please configure .env and run this script again."
    exit 1
fi

# Load environment variables
source .env

# Check required variables
if [ -z "$POOL_WALLET_ADDRESS" ] || [ "$POOL_WALLET_ADDRESS" = "your_pool_wallet_address_here" ]; then
    echo "❌ POOL_WALLET_ADDRESS not configured in .env"
    exit 1
fi

echo "📋 Configuration:"
echo "   Pool Address: $POOL_WALLET_ADDRESS"
echo "   Node URL: $STELLARIS_NODE_URL"
echo "   Pool Fee: ${POOL_FEE_PERCENT:-2.0}%"
echo "   Finder Bonus: ${FINDER_BONUS_PERCENT:-5.0}%"
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed"
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"

# Install Python dependencies
echo ""
echo "📦 Installing Python dependencies..."
pip install -r requirements.txt

echo ""
echo "✅ Setup complete!"
echo ""
echo "To start the pool:"
echo "  1. Using Docker: docker-compose up -d"
echo "  2. Using Python: python main.py"
echo ""
echo "To start mining:"
echo "  python pool_miner.py YOUR_WALLET_ADDRESS --pool http://localhost:8000 --workers 4"
echo ""
