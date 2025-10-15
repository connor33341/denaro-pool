"""
Stellaris Mining Pool - FastAPI Application

This mining pool coordinates multiple miners to work together on mining Stellaris blocks.
Rewards are distributed based on contribution with a bonus for the miner who finds the solution.
"""

import asyncio
import hashlib
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict
from collections import defaultdict
import os
import sys
import json

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add stellaris to path for importing wallet utilities
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'stellaris'))

# Import pool database
from stellaris_pool.pool_database import PoolDatabase

from stellaris.utils.general import string_to_bytes, sha256
from stellaris.constants import ENDIAN
from stellaris.transactions import Transaction, TransactionInput, TransactionOutput
from stellaris.manager import get_block_reward
from fastecdsa import keys, curve

# ==================== Configuration ====================
STELLARIS_NODE_URL = os.getenv("STELLARIS_NODE_URL", "https://stellaris-node.connor33341.dev/")
POOL_WALLET_ADDRESS = os.getenv("POOL_WALLET_ADDRESS")
POOL_PRIVATE_KEY = os.getenv("POOL_PRIVATE_KEY")  # Hex string
POOL_FEE_PERCENT = float(os.getenv("POOL_FEE_PERCENT", "2.0"))  # Pool fee percentage
FINDER_BONUS_PERCENT = float(os.getenv("FINDER_BONUS_PERCENT", "5.0"))  # Extra bonus for finding block
MIN_PAYOUT_AMOUNT = Decimal(os.getenv("MIN_PAYOUT_AMOUNT", "1.0"))  # Minimum payout threshold
DATA_DIR = os.getenv("DATA_DIR", "./data/pool")  # Data storage directory

# Validate required configuration
if not POOL_WALLET_ADDRESS:
    raise ValueError("POOL_WALLET_ADDRESS must be set in environment")

# ==================== FastAPI App ====================
app = FastAPI(
    title="Stellaris Mining Pool",
    description="Decentralized mining pool for Stellaris blockchain",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== Database Connection ====================
pool_db: Optional[PoolDatabase] = None

async def get_db():
    """Get pool database instance"""
    global pool_db
    if pool_db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    return pool_db


# ==================== Global State ====================
class PoolState:
    def __init__(self):
        self.current_work: Optional[Dict] = None
        self.current_work_height: int = 0
        self.work_start_time: float = 0
        self.nonce_ranges: Dict[str, tuple] = {}  # miner_id -> (start, end)
        self.next_nonce_start: int = 0
        self.round_shares: Dict[str, int] = defaultdict(int)  # miner_id -> share_count
        self.total_pool_hashrate: float = 0
        self.active_miners: Dict[str, Dict] = {}  # miner_id -> {last_seen, hashrate, shares}
        self.http_client: Optional[httpx.AsyncClient] = None
    
    def reset_round(self):
        """Reset round data when starting new work"""
        self.nonce_ranges.clear()
        self.next_nonce_start = 0
        self.round_shares.clear()
        self.work_start_time = time.time()
    
    def update_miner_activity(self, miner_id: str, shares: int = 0, hashrate: float = 0):
        """Update miner activity tracking"""
        if miner_id not in self.active_miners:
            self.active_miners[miner_id] = {
                'first_seen': time.time(),
                'last_seen': time.time(),
                'total_shares': 0,
                'hashrate': 0
            }
        
        self.active_miners[miner_id]['last_seen'] = time.time()
        if shares > 0:
            self.active_miners[miner_id]['total_shares'] += shares
        if hashrate > 0:
            self.active_miners[miner_id]['hashrate'] = hashrate
    
    def get_active_miners(self, timeout: float = 300) -> List[str]:
        """Get list of miners active within timeout seconds"""
        current_time = time.time()
        return [
            miner_id for miner_id, data in self.active_miners.items()
            if current_time - data['last_seen'] < timeout
        ]
    
    def calculate_pool_hashrate(self):
        """Calculate total pool hashrate from active miners"""
        active = self.get_active_miners()
        self.total_pool_hashrate = sum(
            self.active_miners[m]['hashrate'] for m in active if 'hashrate' in self.active_miners[m]
        )
        return self.total_pool_hashrate


pool_state = PoolState()


# ==================== Pydantic Models ====================
class MinerRegistration(BaseModel):
    miner_id: str = Field(..., description="Unique miner identifier")
    wallet_address: str = Field(..., description="Miner's wallet address for payouts")
    worker_name: Optional[str] = Field(None, description="Optional worker name")


class WorkRequest(BaseModel):
    miner_id: str = Field(..., description="Unique miner identifier")


class WorkAssignment(BaseModel):
    block_height: int
    difficulty: float
    previous_hash: str
    merkle_root: str
    timestamp: int
    nonce_start: int
    nonce_end: int
    pool_address: str
    transactions: List[str]


class ShareSubmission(BaseModel):
    miner_id: str
    block_height: int
    nonce: int
    block_content_hex: str
    block_hash: str
    is_valid_block: bool = False


class PoolStats(BaseModel):
    total_hashrate: float
    active_miners: int
    current_block_height: int
    current_difficulty: float
    shares_this_round: int
    pool_fee_percent: float
    finder_bonus_percent: float


class MinerStats(BaseModel):
    miner_id: str
    wallet_address: str
    current_hashrate: float
    shares_submitted: int
    shares_this_round: int
    total_paid: str
    pending_balance: str
    blocks_found: int
    last_share_time: Optional[datetime]


# ==================== Database Functions ====================
async def init_database():
    """Initialize pool database"""
    global pool_db
    
    pool_db = PoolDatabase(data_dir=DATA_DIR)
    await pool_db.initialize()


async def register_miner_db(miner_id: str, wallet_address: str, worker_name: Optional[str] = None):
    """Register or update miner in database"""
    db = await get_db()
    await db.register_miner(miner_id, wallet_address, worker_name)


async def record_share_db(miner_id: str, block_height: int, is_valid_block: bool, 
                          block_hash: Optional[str], difficulty: float):
    """Record a share submission"""
    db = await get_db()
    await db.record_share(miner_id, block_height, is_valid_block, block_hash, difficulty)


async def record_block_db(block_height: int, block_hash: str, finder_miner_id: str, reward: Decimal):
    """Record a found block"""
    db = await get_db()
    await db.record_block(block_height, block_hash, finder_miner_id, reward)


async def get_miner_stats_db(miner_id: str) -> Optional[Dict]:
    """Get miner statistics from database"""
    db = await get_db()
    return await db.get_miner(miner_id)


# ==================== Stellaris Node Communication ====================
async def fetch_mining_info() -> Dict:
    """Fetch mining information from Stellaris node"""
    if not pool_state.http_client:
        pool_state.http_client = httpx.AsyncClient(timeout=30.0)
    
    try:
        response = await pool_state.http_client.get(f"{STELLARIS_NODE_URL}get_mining_info")
        response.raise_for_status()
        data = response.json()
        
        if data.get('ok'):
            return data['result']
        else:
            raise HTTPException(status_code=500, detail="Node returned error")
    except httpx.HTTPError as e:
        raise HTTPException(status_code=503, detail=f"Cannot connect to Stellaris node: {str(e)}")


async def submit_block_to_node(block_content: str, txs: List[str], block_height: int) -> Dict:
    """Submit a found block to the Stellaris node"""
    if not pool_state.http_client:
        pool_state.http_client = httpx.AsyncClient(timeout=60.0)
    
    try:
        response = await pool_state.http_client.post(
            f"{STELLARIS_NODE_URL}push_block",
            json={
                'block_content': block_content,
                'txs': txs,
                'id': block_height
            },
            timeout=30.0
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=503, detail=f"Cannot submit block to node: {str(e)}")


def get_transactions_merkle_tree(transactions):
    """Calculate merkle tree root from transaction hashes"""
    return hashlib.sha256(b''.join(bytes.fromhex(tx) for tx in transactions)).hexdigest()


async def get_pool_utxos() -> List[Dict]:
    """Get available UTXOs from pool wallet to spend"""
    if not pool_state.http_client:
        pool_state.http_client = httpx.AsyncClient(timeout=30.0)
    
    try:
        response = await pool_state.http_client.get(
            f"{STELLARIS_NODE_URL}get_address_info",
            params={"address": POOL_WALLET_ADDRESS}
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get('ok'):
            return data['result']['spendable_outputs']
        return []
    except Exception as e:
        print(f"❌ Error fetching pool UTXOs: {e}")
        return []


async def create_payout_transaction(payouts: List[Dict]) -> Optional[str]:
    """
    Create a transaction to pay out miners
    payouts: List of {'address': str, 'amount': Decimal}
    Returns: transaction hex string or None on error
    """
    if not POOL_PRIVATE_KEY:
        print("❌ Pool private key not configured, cannot create payout transaction")
        return None
    
    try:
        # Parse pool private key
        pool_private_key_int = int(POOL_PRIVATE_KEY, 16) if isinstance(POOL_PRIVATE_KEY, str) else POOL_PRIVATE_KEY
        
        # Get pool's UTXOs
        utxos = await get_pool_utxos()
        if not utxos:
            print("❌ No UTXOs available in pool wallet")
            return None
        
        # Calculate total payout amount needed
        total_payout = sum(payout['amount'] for payout in payouts)
        
        # Select UTXOs to cover the payout (simple first-fit)
        selected_utxos = []
        total_input = Decimal(0)
        for utxo in utxos:
            selected_utxos.append(utxo)
            total_input += Decimal(utxo['amount'])
            if total_input >= total_payout:
                break
        
        if total_input < total_payout:
            print(f"❌ Insufficient funds in pool wallet: need {total_payout}, have {total_input}")
            return None
        
        # Create transaction inputs
        inputs = []
        for utxo in selected_utxos:
            tx_input = TransactionInput(
                input_tx_hash=utxo['tx_hash'],
                index=utxo['index'],
                private_key=pool_private_key_int,
                amount=Decimal(utxo['amount'])
            )
            inputs.append(tx_input)
        
        # Create transaction outputs (payouts)
        outputs = []
        for payout in payouts:
            output = TransactionOutput(
                address=payout['address'],
                amount=payout['amount']
            )
            outputs.append(output)
        
        # Add change output if needed (0.01 DNR fee for transaction)
        fee = Decimal("0.01")
        change = total_input - total_payout - fee
        if change > Decimal("0.001"):  # Only add change if significant
            change_output = TransactionOutput(
                address=POOL_WALLET_ADDRESS,
                amount=change
            )
            outputs.append(change_output)
        
        # Create transaction
        transaction = Transaction(inputs=inputs, outputs=outputs)
        
        # Sign the transaction
        tx_hex = transaction.hex(full=False)
        for tx_input in inputs:
            tx_input.sign(tx_hex, pool_private_key_int)
        
        # Get full signed transaction hex
        full_tx_hex = transaction.hex(full=True)
        
        return full_tx_hex
        
    except Exception as e:
        print(f"❌ Error creating payout transaction: {e}")
        import traceback
        traceback.print_exc()
        return None


async def submit_transaction_to_node(tx_hex: str) -> Dict:
    """Submit a transaction to the Stellaris node"""
    if not pool_state.http_client:
        pool_state.http_client = httpx.AsyncClient(timeout=30.0)
    
    try:
        response = await pool_state.http_client.post(
            f"{STELLARIS_NODE_URL}push_tx",
            json={'tx_hex': tx_hex},
            timeout=10.0
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        print(f"❌ Error submitting transaction: {e}")
        return {"ok": False, "error": str(e)}


# ==================== Mining Work Management ====================
async def update_work():
    """Fetch new work from Stellaris node and update pool state"""
    try:
        mining_info = await fetch_mining_info()
        
        new_height = mining_info['last_block']['id'] + 1
        
        # Check if we have new work
        if pool_state.current_work_height != new_height:
            print(f"📦 New work available: Block #{new_height}, Difficulty: {mining_info['difficulty']}")
            pool_state.current_work = mining_info
            pool_state.current_work_height = new_height
            pool_state.reset_round()
            
        return True
    except Exception as e:
        print(f"❌ Error updating work: {e}")
        return False


async def get_work_for_miner(miner_id: str) -> WorkAssignment:
    """Assign work to a miner with unique nonce range"""
    if not pool_state.current_work:
        await update_work()
    
    if not pool_state.current_work:
        raise HTTPException(status_code=503, detail="No work available from Stellaris node")
    
    work = pool_state.current_work
    
    # Assign nonce range (1 million nonces per request)
    nonce_range_size = 1_000_000
    nonce_start = pool_state.next_nonce_start
    nonce_end = nonce_start + nonce_range_size
    
    pool_state.nonce_ranges[miner_id] = (nonce_start, nonce_end)
    pool_state.next_nonce_start = nonce_end
    
    # Update miner activity
    pool_state.update_miner_activity(miner_id)
    
    return WorkAssignment(
        block_height=pool_state.current_work_height,
        difficulty=work['difficulty'],
        previous_hash=work['last_block'].get('hash', (30_06_2005).to_bytes(32, ENDIAN).hex()),
        merkle_root=work['merkle_root'],
        timestamp=int(time.time()),
        nonce_start=nonce_start,
        nonce_end=nonce_end,
        pool_address=POOL_WALLET_ADDRESS,
        transactions=work['pending_transactions_hashes']
    )


# ==================== Share Validation ====================
def validate_share(block_hash: str, difficulty: float, target_difficulty: Optional[float] = None) -> tuple[bool, bool]:
    """
    Validate if a share meets pool difficulty and/or network difficulty
    Returns: (is_valid_share, is_valid_block)
    """
    # For pool shares, we can use a lower difficulty (e.g., 1/10th of network difficulty)
    pool_difficulty = max(1.0, difficulty * 0.1)  # Pool accepts easier shares
    
    # Get the previous block hash chunk needed for validation
    if not pool_state.current_work:
        return False, False
    
    last_block = pool_state.current_work['last_block']
    chunk = last_block.get('hash', (30_06_2005).to_bytes(32, ENDIAN).hex())[-int(difficulty):]
    
    # Check if hash meets pool difficulty (easier)
    pool_chunk = chunk[-int(pool_difficulty):] if pool_difficulty < difficulty else chunk
    is_valid_share = block_hash.startswith(pool_chunk)
    
    # Check if hash meets network difficulty (harder - actual block)
    is_valid_block = block_hash.startswith(chunk)
    
    return is_valid_share, is_valid_block


# ==================== API Endpoints ====================
@app.on_event("startup")
async def startup_event():
    """Initialize pool on startup"""
    print("🚀 Starting Stellaris Mining Pool...")
    
    # Initialize database
    await init_database()
    
    # Initialize HTTP client
    pool_state.http_client = httpx.AsyncClient(timeout=30.0)
    
    # Fetch initial work
    await update_work()
    
    # Start background task to update work periodically
    asyncio.create_task(work_updater_task())
    asyncio.create_task(payout_processor_task())
    
    print(f"✅ Pool initialized successfully")
    print(f"   Pool Address: {POOL_WALLET_ADDRESS}")
    print(f"   Pool Fee: {POOL_FEE_PERCENT}%")
    print(f"   Finder Bonus: {FINDER_BONUS_PERCENT}%")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if pool_state.http_client:
        await pool_state.http_client.aclose()
    
    if pool_db:
        await pool_db.save_all()
        print("💾 Pool database saved on shutdown")


@app.get("/")
async def root():
    """Pool information"""
    return {
        "name": "Stellaris Mining Pool",
        "version": "1.0.0",
        "pool_address": POOL_WALLET_ADDRESS,
        "pool_fee": f"{POOL_FEE_PERCENT}%",
        "finder_bonus": f"{FINDER_BONUS_PERCENT}%",
        "min_payout": str(MIN_PAYOUT_AMOUNT),
        "endpoints": {
            "register": "/api/register",
            "get_work": "/api/work",
            "submit_share": "/api/share",
            "pool_stats": "/api/stats",
            "miner_stats": "/api/miner/{miner_id}"
        }
    }


@app.post("/api/register")
async def register_miner(registration: MinerRegistration):
    """Register a new miner"""
    try:
        await register_miner_db(
            registration.miner_id,
            registration.wallet_address,
            registration.worker_name
        )
        
        return {
            "success": True,
            "message": "Miner registered successfully",
            "miner_id": registration.miner_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")


@app.post("/api/work")
async def get_work(request: WorkRequest):
    """Get mining work assignment"""
    try:
        work = await get_work_for_miner(request.miner_id)
        return work
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cannot assign work: {str(e)}")


@app.post("/api/share")
async def submit_share(share: ShareSubmission, background_tasks: BackgroundTasks):
    """Submit a mining share"""
    try:
        # Validate share is for current work
        if share.block_height != pool_state.current_work_height:
            return {
                "success": False,
                "message": "Share is for old work",
                "current_height": pool_state.current_work_height
            }
        
        # Validate nonce is in assigned range
        if share.miner_id in pool_state.nonce_ranges:
            nonce_start, nonce_end = pool_state.nonce_ranges[share.miner_id]
            if not (nonce_start <= share.nonce < nonce_end):
                return {
                    "success": False,
                    "message": "Nonce outside assigned range"
                }
        
        # Validate the share
        difficulty = pool_state.current_work['difficulty']
        is_valid_share, is_valid_block = validate_share(share.block_hash, difficulty)
        
        if not is_valid_share:
            return {
                "success": False,
                "message": "Share does not meet difficulty requirement"
            }
        
        # Record share in database
        await record_share_db(
            share.miner_id,
            share.block_height,
            is_valid_block,
            share.block_hash if is_valid_block else None,
            difficulty
        )
        
        # Update round stats
        pool_state.round_shares[share.miner_id] += 1
        pool_state.update_miner_activity(share.miner_id, shares=1)
        
        # If this is a valid block, submit it!
        if is_valid_block:
            print(f"🎉 BLOCK FOUND by {share.miner_id}! Block #{share.block_height}")
            background_tasks.add_task(
                handle_found_block,
                share.miner_id,
                share.block_height,
                share.block_content_hex,
                share.block_hash
            )
            
            return {
                "success": True,
                "message": "BLOCK FOUND! Submitting to network...",
                "block_found": True,
                "block_height": share.block_height
            }
        
        return {
            "success": True,
            "message": "Share accepted",
            "shares_this_round": pool_state.round_shares[share.miner_id]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Share submission failed: {str(e)}")


@app.get("/api/stats")
async def get_pool_stats():
    """Get pool statistics"""
    pool_state.calculate_pool_hashrate()
    active_miners = pool_state.get_active_miners()
    
    return PoolStats(
        total_hashrate=pool_state.total_pool_hashrate,
        active_miners=len(active_miners),
        current_block_height=pool_state.current_work_height,
        current_difficulty=pool_state.current_work['difficulty'] if pool_state.current_work else 0,
        shares_this_round=sum(pool_state.round_shares.values()),
        pool_fee_percent=POOL_FEE_PERCENT,
        finder_bonus_percent=FINDER_BONUS_PERCENT
    )


@app.get("/api/miner/{miner_id}")
async def get_miner_stats(miner_id: str):
    """Get individual miner statistics"""
    stats = await get_miner_stats_db(miner_id)
    
    if not stats:
        raise HTTPException(status_code=404, detail="Miner not found")
    
    # Get current round stats
    current_shares = pool_state.round_shares.get(miner_id, 0)
    current_hashrate = pool_state.active_miners.get(miner_id, {}).get('hashrate', 0)
    
    return MinerStats(
        miner_id=stats['miner_id'],
        wallet_address=stats['wallet_address'],
        current_hashrate=current_hashrate,
        shares_submitted=stats['total_shares'],
        shares_this_round=current_shares,
        total_paid=str(stats['total_paid']),
        pending_balance=str(stats['pending_balance']),
        blocks_found=stats['blocks_found'],
        last_share_time=stats['last_seen'] if stats['last_seen'] else None
    )


# ==================== Background Tasks ====================
async def work_updater_task():
    """Background task to periodically check for new work"""
    while True:
        try:
            await asyncio.sleep(5)  # Check every 5 seconds
            await update_work()
        except Exception as e:
            print(f"❌ Error in work updater: {e}")


async def handle_found_block(miner_id: str, block_height: int, block_content: str, block_hash: str):
    """Handle a found block - submit to network and calculate payouts"""
    try:
        # Submit block to Stellaris network
        txs = pool_state.current_work['pending_transactions_hashes']
        result = await submit_block_to_node(block_content, txs, block_height)
        
        if result.get('ok'):
            print(f"✅ Block #{block_height} accepted by network!")
            
            # Get the actual block reward using Stellaris reward calculation
            block_reward = get_block_reward(block_height)
            print(f"💰 Block reward: {block_reward} DNR")
            
            # Record block in database
            await record_block_db(block_height, block_hash, miner_id, block_reward)
            
            # Calculate payouts
            await calculate_and_record_payouts(miner_id, block_reward, block_height)
            
            # Reset work after successful block
            await update_work()
        else:
            print(f"❌ Block #{block_height} rejected by network: {result}")
            
    except Exception as e:
        print(f"❌ Error handling found block: {e}")


async def calculate_and_record_payouts(finder_id: str, reward: Decimal, block_height: int):
    """Calculate payout distribution based on shares"""
    try:
        # Deduct pool fee
        pool_fee = reward * Decimal(POOL_FEE_PERCENT) / Decimal(100)
        distributable = reward - pool_fee
        
        # Calculate finder bonus
        finder_bonus_amount = distributable * Decimal(FINDER_BONUS_PERCENT) / Decimal(100)
        remaining = distributable - finder_bonus_amount
        
        # Get all shares for this round
        total_shares = sum(pool_state.round_shares.values())
        
        if total_shares == 0:
            print("⚠️  No shares in round, all reward goes to finder")
            finder_bonus_amount = distributable
            remaining = Decimal(0)
        
        # Calculate payout per share
        payouts = {}
        
        if remaining > 0:
            for miner_id, shares in pool_state.round_shares.items():
                share_reward = (remaining * Decimal(shares)) / Decimal(total_shares)
                payouts[miner_id] = payouts.get(miner_id, Decimal(0)) + share_reward
        
        # Add finder bonus
        payouts[finder_id] = payouts.get(finder_id, Decimal(0)) + finder_bonus_amount
        
        # Update pending balances in database
        db = await get_db()
        payout_records = []
        for miner_id, amount in payouts.items():
            payout_records.append({
                'miner_id': miner_id,
                'amount': amount
            })
        
        await db.add_payout_records(payout_records, block_height)
        
        print(f"💰 Payouts calculated for block #{block_height}")
        print(f"   Pool Fee: {pool_fee}")
        print(f"   Finder Bonus: {finder_bonus_amount} -> {finder_id}")
        print(f"   Total Payouts: {len(payouts)} miners")
        
    except Exception as e:
        print(f"❌ Error calculating payouts: {e}")


async def payout_processor_task():
    """Background task to process pending payouts"""
    while True:
        try:
            await asyncio.sleep(300)  # Check every 5 minutes
            
            # Get miners with pending balance above threshold
            db = await get_db()
            miners = await db.get_miners_with_balance(MIN_PAYOUT_AMOUNT)
            
            if miners:
                print(f"💸 Processing payouts for {len(miners)} miners...")
                
                # Prepare payout list
                payouts = []
                miner_ids = []
                for miner in miners:
                    payouts.append({
                        'address': miner['wallet_address'],
                        'amount': Decimal(str(miner['pending_balance']))
                    })
                    miner_ids.append(miner['miner_id'])
                
                # Create payout transaction
                tx_hex = await create_payout_transaction(payouts)
                
                if tx_hex:
                    # Submit transaction to network
                    result = await submit_transaction_to_node(tx_hex)
                    
                    if result.get('ok'):
                        tx_hash = sha256(tx_hex)
                        print(f"✅ Payout transaction submitted: {tx_hash}")
                        
                        # Update database
                        await db.complete_payouts(miner_ids, tx_hash)
                        
                        print(f"💰 Paid out {sum(p['amount'] for p in payouts)} DNR to {len(miners)} miners")
                    else:
                        print(f"❌ Failed to submit payout transaction: {result}")
                else:
                    print("❌ Failed to create payout transaction")
                
        except Exception as e:
            print(f"❌ Error in payout processor: {e}")
            import traceback
            traceback.print_exc()


# ==================== Health Check ====================
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check database
        db = await get_db()
        
        # Check Stellaris node
        await fetch_mining_info()
        
        return {
            "status": "healthy",
            "database": "connected",
            "stellaris_node": "connected",
            "current_work": pool_state.current_work_height,
            "storage": "json+gzip"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
