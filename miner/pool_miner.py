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
        """Submit a valid block to the pool"""
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
    
    def submit_work_proof(self, block_height: int, nonce_start: int, nonce_end: int,
                         best_nonce: int, best_hash: str, hashes_computed: int):
        """Submit proof that work was completed"""
        try:
            response = self.session.post(
                f"{self.pool_url}/api/work_proof",
                json={
                    "miner_id": self.miner_id,
                    "block_height": block_height,
                    "nonce_start": nonce_start,
                    "nonce_end": nonce_end,
                    "best_nonce": best_nonce,
                    "best_hash": best_hash,
                    "hashes_computed": hashes_computed
                },
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Error submitting work proof: {e}")
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
                    result_type = result[0]
                    
                    if result_type == 'block':
                        # Valid block found!
                        _, nonce, block_content_hex, block_hash = result
                        
                        response = self.submit_share(
                            block_height,
                            nonce,
                            block_content_hex,
                            block_hash,
                            True  # is_valid_block
                        )
                        
                        if response and response.get('block_found'):
                            print(f"🎉🎉🎉 BLOCK FOUND by Worker {worker_id + 1}! 🎉🎉🎉")
                    
                    elif result_type == 'proof':
                        # Range completed, submit work proof
                        _, best_nonce, best_hash, hashes_computed = result
                        
                        response = self.submit_work_proof(
                            block_height,
                            nonce_start,
                            nonce_end,
                            best_nonce,
                            best_hash,
                            hashes_computed
                        )
                        
                        if response and response.get('success'):
                            work_units = response.get('work_units', 0)
                            print(f"✅ Worker {worker_id + 1}: Work proof accepted ({work_units} work units this round)")
                        else:
                            print(f"❌ Work proof error {response} {best_nonce}")
                
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
        """Mine within a specific nonce range at FULL network difficulty"""
        
        # Setup difficulty checking (matching Stellaris miner.py exactly)
        chunk = previous_hash[-int(difficulty):]
        decimal = difficulty % 1
        
        if decimal > 0:
            charset = '0123456789abcdef'
            count = ceil(16 * (1 - decimal))
            charset = charset[:count]
            idifficulty = int(difficulty)
            
            def check_block_is_valid(block_content: bytes) -> bool:
                block_hash = hashlib.sha256(block_content).hexdigest()
                return block_hash.startswith(chunk) and block_hash[idifficulty] in charset
        else:
            def check_block_is_valid(block_content: bytes) -> bool:
                return hashlib.sha256(block_content).hexdigest().startswith(chunk)
        
        # Prepare block header (MUST match Stellaris format exactly)
        address_bytes = string_to_bytes(pool_address)
        
        # Build prefix: previous_hash + address + merkle_root + timestamp + difficulty
        prefix = (
            bytes.fromhex(previous_hash) +
            address_bytes +
            bytes.fromhex(merkle_root) +
            timestamp.to_bytes(4, byteorder='little') +
            int(difficulty * 10).to_bytes(2, 'little')
        )
        
        # Add version byte at the BEGINNING if compressed address (33 bytes)
        if len(address_bytes) == 33:
            prefix = (2).to_bytes(1, 'little') + prefix
        
        # Mining loop - mine at FULL network difficulty
        # Track best hash found for proof of work
        t = time.time()
        i = nonce_start
        hashrate_check = 5_000_000  # Report hashrate every 5M hashes (like Stellaris)
        timeout = 280  # Timeout after 280 seconds (like Stellaris)
        
        best_nonce = nonce_start
        best_hash = 'f' * 64  # Start with worst possible hash
        hashes_computed = 0
        
        while i < nonce_end:
            # Use 4 bytes for nonce (Stellaris format)
            block_content = prefix + i.to_bytes(4, 'little')
            block_hash = hashlib.sha256(block_content).hexdigest()
            hashes_computed += 1
            
            # Track best hash (hash that's closest to meeting difficulty)
            if block_hash < best_hash:
                best_hash = block_hash
                best_nonce = i
            
            # Check if valid block (FULL network difficulty)
            if check_block_is_valid(block_content):
                elapsed = time.time() - t
                hashrate = hashes_computed / elapsed / 1000 if elapsed > 0 else 0
                print(f"🎉🎉🎉 Worker {worker_id + 1}: VALID BLOCK FOUND! 🎉🎉🎉")
                print(f"   Nonce: {i:,}")
                print(f"   Hash: {block_hash}")
                print(f"   Hashrate: {hashrate:.1f} kH/s")
                return ('block', i, block_content.hex(), block_hash)
            
            i += 1
            
            # Print hashrate periodically
            if hashes_computed % hashrate_check == 0 and hashes_computed > 0:
                elapsed = time.time() - t
                if elapsed > 0:
                    hashrate = hashes_computed / elapsed / 1000
                    progress = (i - nonce_start) / (nonce_end - nonce_start) * 100
                    print(f"Worker {worker_id + 1}: {hashrate:.1f} kH/s ({progress:.1f}% of range)")
            
            # Timeout check (like Stellaris miner)
            if time.time() - t > timeout:
                print(f"Worker {worker_id + 1}: Timeout ({timeout}s), submitting work proof")
                return ('proof', best_nonce, best_hash, hashes_computed)
        
        # Range exhausted without finding block - submit proof of work
        elapsed = time.time() - t
        hashrate = hashes_computed / elapsed / 1000 if elapsed > 0 else 0
        print(f"Worker {worker_id + 1}: Range exhausted ({hashrate:.1f} kH/s), submitting work proof")
        print(f"   Best hash: {best_hash[:16]}...")
        return ('proof', best_nonce, best_hash, hashes_computed)


def main():
    parser = argparse.ArgumentParser(description='Stellaris Pool Miner')
    parser.add_argument('wallet_address', help='Your Stellaris wallet address')
    parser.add_argument('--pool', default='https://denaro-pool.connor33341.dev', 
                       help='Pool URL (default: https://denaro-pool.connor33341.dev)')
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
