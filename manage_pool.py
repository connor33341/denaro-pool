#!/usr/bin/env python3
"""
Stellaris Mining Pool Management CLI

Utility for managing and monitoring the mining pool.
"""

import sys
import os
import requests
import json
from decimal import Decimal
from datetime import datetime

def print_header(text):
    """Print a formatted header"""
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60)

def get_pool_url():
    """Get pool URL from environment or use default"""
    return os.getenv("POOL_URL", "http://localhost:8000")

def call_api(endpoint, method="GET", data=None):
    """Make API call to pool"""
    pool_url = get_pool_url()
    url = f"{pool_url}{endpoint}"
    
    try:
        if method == "GET":
            response = requests.get(url, timeout=10)
        elif method == "POST":
            response = requests.post(url, json=data, timeout=10)
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error connecting to pool: {e}")
        return None

def cmd_info():
    """Show pool information"""
    print_header("Pool Information")
    
    data = call_api("/")
    if data:
        print(f"Name: {data.get('name')}")
        print(f"Version: {data.get('version')}")
        print(f"Pool Address: {data.get('pool_address')}")
        print(f"Pool Fee: {data.get('pool_fee')}")
        print(f"Finder Bonus: {data.get('finder_bonus')}")
        print(f"Min Payout: {data.get('min_payout')}")

def cmd_stats():
    """Show pool statistics"""
    print_header("Pool Statistics")
    
    data = call_api("/api/stats")
    if data:
        print(f"Total Hashrate: {data['total_hashrate']:,.0f} H/s ({data['total_hashrate']/1000000:.2f} MH/s)")
        print(f"Active Miners: {data['active_miners']}")
        print(f"Current Block: #{data['current_block_height']}")
        print(f"Difficulty: {data['current_difficulty']:.2f}")
        print(f"Shares This Round: {data['shares_this_round']:,}")
        print(f"Pool Fee: {data['pool_fee_percent']}%")
        print(f"Finder Bonus: {data['finder_bonus_percent']}%")

def cmd_miner(miner_id):
    """Show miner statistics"""
    print_header(f"Miner Statistics: {miner_id}")
    
    data = call_api(f"/api/miner/{miner_id}")
    if data:
        print(f"Wallet Address: {data['wallet_address']}")
        print(f"Current Hashrate: {data['current_hashrate']:,.0f} H/s ({data['current_hashrate']/1000:.2f} kH/s)")
        print(f"Total Shares: {data['shares_submitted']:,}")
        print(f"Shares This Round: {data['shares_this_round']}")
        print(f"Blocks Found: {data['blocks_found']}")
        print(f"Total Paid: {data['total_paid']} DNR")
        print(f"Pending Balance: {data['pending_balance']} DNR")
        if data['last_share_time']:
            print(f"Last Share: {data['last_share_time']}")

def cmd_health():
    """Check pool health"""
    print_header("Pool Health Check")
    
    data = call_api("/health")
    if data:
        status = data.get('status', 'unknown')
        symbol = "✅" if status == "healthy" else "❌"
        
        print(f"{symbol} Status: {status.upper()}")
        print(f"Database: {data.get('database', 'unknown')}")
        print(f"Stellaris Node: {data.get('stellaris_node', 'unknown')}")
        print(f"Current Work: Block #{data.get('current_work', 'unknown')}")
        
        if 'error' in data:
            print(f"\n⚠️  Error: {data['error']}")

def cmd_help():
    """Show help message"""
    print_header("Stellaris Pool Management CLI")
    print("""
Usage: python manage_pool.py <command> [options]

Commands:
  info              Show pool information
  stats             Show pool statistics
  miner <miner_id>  Show specific miner statistics
  health            Check pool health
  help              Show this help message

Environment Variables:
  POOL_URL          Pool server URL (default: http://localhost:8000)

Examples:
  python manage_pool.py stats
  python manage_pool.py miner E32vDuz39Dj_worker-1
  POOL_URL=http://remote-pool:8000 python manage_pool.py health
    """)

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        cmd_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == "info":
        cmd_info()
    elif command == "stats":
        cmd_stats()
    elif command == "miner":
        if len(sys.argv) < 3:
            print("❌ Error: miner_id required")
            print("Usage: python manage_pool.py miner <miner_id>")
            return
        cmd_miner(sys.argv[2])
    elif command == "health":
        cmd_health()
    elif command == "help":
        cmd_help()
    else:
        print(f"❌ Unknown command: {command}")
        print("Run 'python manage_pool.py help' for usage information")

if __name__ == "__main__":
    main()
