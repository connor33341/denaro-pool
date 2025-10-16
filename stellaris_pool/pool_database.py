"""
Stellaris Mining Pool - Storage Layer

JSON + gzip based storage similar to Stellaris blockchain database.
Production-ready with atomic writes and corruption recovery.
"""

import gzip
import json
import asyncio
from pathlib import Path
from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Optional
import shutil


class PoolDatabase:
    """Gzip-compressed JSON storage for pool data"""
    
    def __init__(self, data_dir: str = './data/pool'):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # File paths
        self.miners_file = self.data_dir / 'miners.json.gz'
        self.shares_file = self.data_dir / 'shares.json.gz'
        self.blocks_file = self.data_dir / 'blocks.json.gz'
        self.payouts_file = self.data_dir / 'payouts.json.gz'
        self.stats_file = self.data_dir / 'stats.json.gz'
        
        # In-memory data
        self.miners: Dict[str, Dict] = {}
        self.shares: List[Dict] = []
        self.blocks: Dict[int, Dict] = {}
        self.payouts: List[Dict] = []
        self.stats: Dict = {
            'total_blocks_found': 0,
            'total_shares_submitted': 0,
            'total_paid_out': '0',
            'pool_start_time': datetime.utcnow().isoformat()
        }
        
        self._lock = asyncio.Lock()
        
    async def initialize(self):
        """Load all data from disk"""
        await self._load_all()
        print(f"✅ Pool database initialized")
        print(f"   Miners: {len(self.miners)}")
        print(f"   Shares: {len(self.shares)}")
        print(f"   Blocks: {len(self.blocks)}")
        print(f"   Payouts: {len(self.payouts)}")
    
    async def _load_from_file(self, file_path: Path) -> dict:
        """Load data from gzip JSON file with recovery"""
        if not file_path.exists():
            return {}
        
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError, EOFError) as e:
            print(f"⚠️  Error loading {file_path.name}: {e}")
            
            # Create backup
            backup_path = file_path.with_suffix(f'.{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.bak')
            try:
                shutil.copy2(file_path, backup_path)
                print(f"   Created backup: {backup_path}")
            except Exception:
                pass
            
            # Try recovery
            try:
                with open(file_path, 'rb') as f:
                    import zlib
                    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
                    data = b''
                    while chunk := f.read(1024):
                        try:
                            data += decompressor.decompress(chunk)
                        except:
                            break
                    
                    if data:
                        text = data.decode('utf-8')
                        last_brace = text.rfind('}')
                        if last_brace > 0:
                            recovered = json.loads(text[:last_brace+1])
                            print(f"   ✅ Recovered {len(recovered)} entries")
                            return recovered
            except Exception:
                pass
            
            print(f"   ⚠️  Using empty data for {file_path.name}")
            return {}
    
    async def _save_to_file(self, file_path: Path, data):
        """Save data to gzip JSON file atomically"""
        async with self._lock:
            # Write to temp file first
            temp_path = file_path.with_suffix('.tmp')
            try:
                with gzip.open(temp_path, 'wt', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, default=str)
                
                # Atomic replace
                temp_path.replace(file_path)
            except Exception as e:
                if temp_path.exists():
                    temp_path.unlink()
                raise e
    
    async def _load_all(self):
        """Load all data files"""
        self.miners = await self._load_from_file(self.miners_file)
        
        shares_data = await self._load_from_file(self.shares_file)
        self.shares = shares_data.get('shares', []) if isinstance(shares_data, dict) else []
        
        self.blocks = await self._load_from_file(self.blocks_file)
        
        payouts_data = await self._load_from_file(self.payouts_file)
        self.payouts = payouts_data.get('payouts', []) if isinstance(payouts_data, dict) else []
        
        loaded_stats = await self._load_from_file(self.stats_file)
        if loaded_stats:
            self.stats.update(loaded_stats)
    
    # ==================== Miners ====================
    
    async def register_miner(self, miner_id: str, wallet_address: str, worker_name: Optional[str] = None):
        """Register or update miner"""
        if miner_id not in self.miners:
            self.miners[miner_id] = {
                'miner_id': miner_id,
                'wallet_address': wallet_address,
                'worker_name': worker_name,
                'registered_at': datetime.utcnow().isoformat(),
                'last_seen': datetime.utcnow().isoformat(),
                'total_shares': 0,
                'total_paid': '0',
                'pending_balance': '0',
                'blocks_found': 0
            }
        else:
            self.miners[miner_id]['wallet_address'] = wallet_address
            self.miners[miner_id]['worker_name'] = worker_name
            self.miners[miner_id]['last_seen'] = datetime.utcnow().isoformat()
        
        await self._save_to_file(self.miners_file, self.miners)
    
    async def get_miner(self, miner_id: str) -> Optional[Dict]:
        """Get miner by ID"""
        return self.miners.get(miner_id)
    
    async def update_miner_stats(self, miner_id: str, shares_delta: int = 0):
        """Update miner statistics"""
        if miner_id in self.miners:
            self.miners[miner_id]['total_shares'] += shares_delta
            self.miners[miner_id]['last_seen'] = datetime.utcnow().isoformat()
            await self._save_to_file(self.miners_file, self.miners)
    
    async def get_miners_with_balance(self, min_balance: Decimal) -> List[Dict]:
        """Get miners with pending balance above threshold"""
        result = []
        for miner_id, miner in self.miners.items():
            balance = Decimal(miner['pending_balance'])
            if balance >= min_balance:
                result.append(miner)
        return result
    
    # ==================== Shares ====================
    
    async def record_share(self, miner_id: str, block_height: int, is_valid_block: bool,
                          block_hash: Optional[str], difficulty: float):
        """Record a share submission"""
        share = {
            'id': len(self.shares) + 1,
            'miner_id': miner_id,
            'block_height': block_height,
            'submitted_at': datetime.utcnow().isoformat(),
            'is_valid_block': is_valid_block,
            'block_hash': block_hash,
            'difficulty': difficulty
        }
        self.shares.append(share)
        
        # Update miner stats
        await self.update_miner_stats(miner_id, shares_delta=1)
        
        # Update global stats
        self.stats['total_shares_submitted'] += 1
        
        # Save periodically (every 100 shares to reduce I/O)
        if len(self.shares) % 100 == 0:
            await self._save_to_file(self.shares_file, {'shares': self.shares})
            await self._save_to_file(self.stats_file, self.stats)
    
    async def get_shares_for_block(self, block_height: int) -> List[Dict]:
        """Get all shares for a specific block"""
        return [s for s in self.shares if s['block_height'] == block_height]
    
    # ==================== Blocks ====================
    
    async def record_block(self, block_height: int, block_hash: str, 
                          finder_miner_id: str, reward: Decimal):
        """Record a found block"""
        if block_height not in self.blocks:
            self.blocks[str(block_height)] = {
                'block_height': block_height,
                'block_hash': block_hash,
                'finder_miner_id': finder_miner_id,
                'reward': str(reward),
                'found_at': datetime.utcnow().isoformat(),
                'confirmed': False,
                'paid_out': False
            }
            
            # Update miner stats
            if finder_miner_id in self.miners:
                self.miners[finder_miner_id]['blocks_found'] += 1
                await self._save_to_file(self.miners_file, self.miners)
            
            # Update global stats
            self.stats['total_blocks_found'] += 1
            
            await self._save_to_file(self.blocks_file, self.blocks)
            await self._save_to_file(self.stats_file, self.stats)
    
    async def get_block(self, block_height: int) -> Optional[Dict]:
        """Get block by height"""
        return self.blocks.get(str(block_height))
    
    async def get_recent_blocks(self, limit: int = 10) -> List[Dict]:
        """Get most recent blocks"""
        sorted_blocks = sorted(
            self.blocks.values(),
            key=lambda b: b['block_height'],
            reverse=True
        )
        return sorted_blocks[:limit]
    
    # ==================== Payouts ====================
    
    async def add_payout_records(self, miner_payouts: List[Dict], block_height: int):
        """Add payout records for a block"""
        for payout in miner_payouts:
            # Quantize amount to 6 decimal places for blockchain compatibility
            amount = Decimal(str(payout['amount'])).quantize(Decimal('0.000001'))
            
            record = {
                'id': len(self.payouts) + 1,
                'miner_id': payout['miner_id'],
                'amount': str(amount),
                'tx_hash': None,
                'block_height': block_height,
                'paid_at': datetime.utcnow().isoformat(),
                'status': 'pending'
            }
            self.payouts.append(record)
            
            # Update miner pending balance
            if payout['miner_id'] in self.miners:
                current = Decimal(self.miners[payout['miner_id']]['pending_balance'])
                new_balance = current + amount
                self.miners[payout['miner_id']]['pending_balance'] = str(
                    new_balance.quantize(Decimal('0.000001'))
                )
        
        await self._save_to_file(self.payouts_file, {'payouts': self.payouts})
        await self._save_to_file(self.miners_file, self.miners)
    
    async def complete_payouts(self, miner_ids: List[str], tx_hash: str):
        """Mark payouts as completed"""
        for payout in self.payouts:
            if payout['miner_id'] in miner_ids and payout['status'] == 'pending':
                payout['status'] = 'completed'
                payout['tx_hash'] = tx_hash
        
        # Update miners
        for miner_id in miner_ids:
            if miner_id in self.miners:
                pending = Decimal(self.miners[miner_id]['pending_balance'])
                total_paid = Decimal(self.miners[miner_id]['total_paid'])
                
                self.miners[miner_id]['total_paid'] = str(total_paid + pending)
                self.miners[miner_id]['pending_balance'] = '0'
                
                # Update global stats
                self.stats['total_paid_out'] = str(
                    Decimal(self.stats['total_paid_out']) + pending
                )
        
        await self._save_to_file(self.payouts_file, {'payouts': self.payouts})
        await self._save_to_file(self.miners_file, self.miners)
        await self._save_to_file(self.stats_file, self.stats)
    
    async def get_miner_payouts(self, miner_id: str, limit: int = 10) -> List[Dict]:
        """Get payout history for a miner"""
        miner_payouts = [p for p in self.payouts if p['miner_id'] == miner_id]
        miner_payouts.sort(key=lambda p: p['paid_at'], reverse=True)
        return miner_payouts[:limit]
    
    # ==================== Stats ====================
    
    async def get_pool_stats(self) -> Dict:
        """Get overall pool statistics"""
        return self.stats.copy()
    
    async def save_all(self):
        """Force save all data to disk"""
        await self._save_to_file(self.miners_file, self.miners)
        await self._save_to_file(self.shares_file, {'shares': self.shares})
        await self._save_to_file(self.blocks_file, self.blocks)
        await self._save_to_file(self.payouts_file, {'payouts': self.payouts})
        await self._save_to_file(self.stats_file, self.stats)
        print("💾 All pool data saved to disk")
