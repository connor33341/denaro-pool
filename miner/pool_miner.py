"""
Stellaris Pool Miner Client

This miner connects to a Stellaris mining pool and contributes hashpower.
Supports multi-processing for better performance.
"""

import hashlib
import sys
import time
import requests
from math import ceil
from multiprocessing import Process, Queue, Value
import uuid
import argparse
import base58


def string_to_bytes(string: str) -> bytes:
    """Convert address string to bytes, supporting both hex and base58 formats"""
    try:
        return bytes.fromhex(string)
    except ValueError:
        return base58.b58decode(string)


def get_transactions_merkle_tree(transactions):
    """Calculate merkle tree root from transaction hashes"""
    return hashlib.sha256(b''.join(bytes.fromhex(transaction) for transaction in transactions)).hexdigest()


class PoolMiner:
    def __init__(self, pool_url: str, wallet_address: str, worker_name: str = None, workers: int = 1):
        self.pool_url = pool_url.rstrip('/')
        self.wallet_address = wallet_address
        self.worker_name = worker_name or f"worker-{uuid.uuid4().hex[:8]}"
        self.miner_id = f"{wallet_address[:12]}_{self.worker_name}"
        self.workers = workers
        self.session = requests.Session()
        
    def register(self):
        """Register with the pool"""
        try:
            response = self.session.post(
                f"{self.pool_url}/api/register",
                json={
                    "miner_id": self.miner_id,
                    "wallet_address": self.wallet_address,
                    "worker_name": self.worker_name
                },
                timeout=10
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get('success'):
                print(f"✅ Registered with pool: {self.miner_id}")
                return True
            else:
                print(f"❌ Registration failed: {result}")
                return False
        except Exception as e:
            print(f"❌ Registration error: {e}")
            return False
    
    def get_work(self):
        """Request work from the pool"""
        try:
            response = self.session.post(
                f"{self.pool_url}/api/work",
                json={"miner_id": self.miner_id},
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Error getting work: {e}")
            return None
    
    def submit_share(self, block_height: int, nonce: int, block_content_hex: str, 
                     block_hash: str, is_valid_block: bool = False):
        """Submit a share to the pool"""
        try:
            response = self.session.post(
                f"{self.pool_url}/api/share",
                json={
                    "miner_id": self.miner_id,
                    "block_height": block_height,
                    "nonce": nonce,
                    "block_content_hex": block_content_hex,
                    "block_hash": block_hash,
                    "is_valid_block": is_valid_block
                },
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Error submitting share: {e}")
            return None
    
    def mine(self):
        """Main mining loop"""
        print(f"🚀 Starting mining with {self.workers} worker(s)")
        print(f"   Miner ID: {self.miner_id}")
        print(f"   Wallet: {self.wallet_address}")
        
        # Register with pool
        if not self.register():
            print("Failed to register with pool. Exiting.")
            return
        
        # Start worker processes
        if self.workers > 1:
            self.mine_multiprocess()
        else:
            self.mine_worker(0, 1)
    
    def mine_multiprocess(self):
        """Mine using multiple processes"""
        processes = []
        
        for worker_id in range(self.workers):
            p = Process(target=self.mine_worker, args=(worker_id, self.workers))
            p.start()
            processes.append(p)
        
        # Wait for all processes
        for p in processes:
            p.join()
    
    def mine_worker(self, worker_id: int, total_workers: int):
        """Worker mining function"""
        print(f"Worker {worker_id + 1}/{total_workers} started")
        
        while True:
            try:
                # Get work from pool
                work = self.get_work()
                if not work:
                    print(f"Worker {worker_id + 1}: No work available, waiting...")
                    time.sleep(5)
                    continue
                
                block_height = work['block_height']
                difficulty = work['difficulty']
                previous_hash = work['previous_hash']
                merkle_root = work['merkle_root']
                timestamp = work['timestamp']
                nonce_start = work['nonce_start']
                nonce_end = work['nonce_end']
                pool_address = work['pool_address']
                transactions = work['transactions']
                
                print(f"Worker {worker_id + 1}: Mining block #{block_height}, difficulty {difficulty}")
                print(f"   Nonce range: {nonce_start:,} - {nonce_end:,}")
                
                # Validate nonce range
                if nonce_start < 0 or nonce_end < 0:
                    print(f"❌ Worker {worker_id + 1}: Invalid nonce range (negative values)")
                    time.sleep(5)
                    continue
                
                # Stellaris uses 4-byte nonce, max value is 2^32 - 1
                if nonce_end > 2**32 - 1:
                    print(f"❌ Worker {worker_id + 1}: Nonce range exceeds 32-bit limit (Stellaris uses 4-byte nonce)")
                    time.sleep(5)
                    continue
                
                # Mine the assigned range
                result = self.mine_range(
                    worker_id,
                    block_height,
                    difficulty,
                    previous_hash,
                    pool_address,
                    merkle_root,
                    timestamp,
                    nonce_start,
                    nonce_end,
                    transactions
                )
                
                if result:
                    nonce, block_content_hex, block_hash, is_valid_block = result
                    
                    # Submit share
                    response = self.submit_share(
                        block_height,
                        nonce,
                        block_content_hex,
                        block_hash,
                        is_valid_block
                    )
                    
                    if response:
                        if response.get('block_found'):
                            print(f"🎉🎉🎉 BLOCK FOUND by Worker {worker_id + 1}! 🎉🎉🎉")
                        elif response.get('success'):
                            shares = response.get('shares_this_round', 0)
                            print(f"✅ Worker {worker_id + 1}: Share accepted ({shares} shares this round)")
                
            except KeyboardInterrupt:
                raise
            except Exception as e:
                import traceback
                print(f"❌ Worker {worker_id + 1} error: {e}")
                print(f"   Traceback: {traceback.format_exc()}")
                time.sleep(5)
    
    def mine_range(self, worker_id: int, block_height: int, difficulty: float,
                   previous_hash: str, pool_address: str, merkle_root: str,
                   timestamp: int, nonce_start: int, nonce_end: int, transactions: list):
        """Mine within a specific nonce range"""
        
        # Setup difficulty checking
        chunk = previous_hash[-int(difficulty):]
        decimal = difficulty % 1
        
        if decimal > 0:
            charset = '0123456789abcdef'
            count = ceil(16 * (1 - decimal))
            charset = charset[:count]
            idifficulty = int(difficulty)
            
            def check_block_is_valid(block_hash: str) -> bool:
                return block_hash.startswith(chunk) and block_hash[idifficulty] in charset
        else:
            def check_block_is_valid(block_hash: str) -> bool:
                return block_hash.startswith(chunk)
        
        # For pool mining, also check for shares (easier difficulty)
        pool_difficulty = max(1.0, difficulty * 0.1)
        pool_chunk = chunk[-int(pool_difficulty):] if pool_difficulty < difficulty else chunk
        
        def check_pool_share(block_hash: str) -> bool:
            return block_hash.startswith(pool_chunk)
        
        # Prepare block header
        address_bytes = string_to_bytes(pool_address)
        
        # Build prefix
        prefix_parts = [bytes.fromhex(previous_hash)]
        
        # Add version byte if needed (for compressed addresses)
        if len(address_bytes) == 33:
            prefix_parts.insert(0, (2).to_bytes(1, 'little'))
        
        prefix_parts.extend([
            address_bytes,
            bytes.fromhex(merkle_root),
            timestamp.to_bytes(4, byteorder='little'),
            int(difficulty * 10).to_bytes(2, 'little')
        ])
        
        prefix = b''.join(prefix_parts)
        
        # Mining loop
        t = time.time()
        i = nonce_start
        check_interval = 50000  # Check for shares every N hashes
        
        while i < nonce_end:
            # Use 4 bytes for nonce (Stellaris format)
            block_content = prefix + i.to_bytes(4, 'little')
            block_hash = hashlib.sha256(block_content).hexdigest()
            
            # Check if valid block (network difficulty)
            if check_block_is_valid(block_hash):
                print(f"🎉 Worker {worker_id + 1}: VALID BLOCK FOUND!")
                return (i, block_content.hex(), block_hash, True)
            
            # Check if valid share (pool difficulty)
            if (i - nonce_start) % check_interval == 0 and check_pool_share(block_hash):
                elapsed = time.time() - t
                if elapsed > 0:
                    hashrate = (i - nonce_start) / elapsed / 1000
                    print(f"Worker {worker_id + 1}: {hashrate:.1f} kH/s - Submitting share")
                return (i, block_content.hex(), block_hash, False)
            
            i += 1
            
            # Print hashrate periodically
            if (i - nonce_start) % 500000 == 0:
                elapsed = time.time() - t
                if elapsed > 0:
                    hashrate = (i - nonce_start) / elapsed / 1000
                    print(f"Worker {worker_id + 1}: {hashrate:.1f} kH/s")
        
        # Range exhausted without finding share
        print(f"Worker {worker_id + 1}: Nonce range exhausted, requesting new work")
        return None


def main():
    parser = argparse.ArgumentParser(description='Stellaris Pool Miner')
    parser.add_argument('wallet_address', help='Your Stellaris wallet address')
    parser.add_argument('--pool', default='https://stellaris-pool.connor33341.dev', 
                       help='Pool URL (default: https://stellaris-pool.connor33341.dev)')
    parser.add_argument('--workers', type=int, default=1, 
                       help='Number of worker processes (default: 1)')
    parser.add_argument('--name', help='Worker name (optional)')
    
    args = parser.parse_args()
    
    # Validate wallet address
    if len(args.wallet_address) < 40:
        print("❌ Invalid wallet address")
        return
    
    # Create and start miner
    miner = PoolMiner(
        pool_url=args.pool,
        wallet_address=args.wallet_address,
        worker_name=args.name,
        workers=args.workers
    )
    
    try:
        miner.mine()
    except KeyboardInterrupt:
        print("\n⚠️  Mining stopped by user")
    except Exception as e:
        print(f"\n❌ Mining error: {e}")


if __name__ == '__main__':
    main()
