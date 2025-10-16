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
from math import ceil
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
        self.work_assignments: Dict[str, Dict] = {}  # "miner_id:start:end" -> {range, timestamp, completed}
        self.next_nonce_start: int = 0
        self.round_shares: Dict[str, int] = defaultdict(int)  # miner_id -> share_count (blocks found)
        self.round_work: Dict[str, int] = defaultdict(int)  # miner_id -> work_units (ranges completed with proof)
        self.total_pool_hashrate: float = 0
        self.active_miners: Dict[str, Dict] = {}  # miner_id -> {last_seen, hashrate, shares}
        self.submitted_blocks: set = set()  # Track submitted block hashes to prevent duplicates
        self.block_found_for_height: Optional[int] = None  # Track if a block was already found for current height
        self.http_client: Optional[httpx.AsyncClient] = None
    
    def reset_round(self):
        """Reset round data when starting new work"""
        self.nonce_ranges.clear()
        self.work_assignments.clear()  # Clear work assignments
        self.next_nonce_start = 0
        self.round_shares.clear()
        self.round_work.clear()  # Reset work tracking
        self.submitted_blocks.clear()  # Clear submitted blocks for new round
        self.block_found_for_height = None  # Reset block found tracker
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


class WorkProof(BaseModel):
    miner_id: str
    block_height: int
    nonce_start: int
    nonce_end: int
    best_nonce: int  # Nonce that produced the best hash
    best_hash: str  # Best hash found in the range
    hashes_computed: int  # Number of hashes actually computed


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
            # Quantize amount to 6 decimal places to match SMALLEST (1000000)
            amount = Decimal(str(payout['amount'])).quantize(Decimal('0.000001'))
            output = TransactionOutput(
                address=payout['address'],
                amount=amount
            )
            outputs.append(output)
        
        # Add change output if needed (0.01 DNR fee for transaction)
        fee = Decimal("0.01")
        change = total_input - total_payout - fee
        if change > Decimal("0.001"):  # Only add change if significant
            change = change.quantize(Decimal('0.000001'))
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
        new_merkle = mining_info['merkle_root']
        
        # Check if we have new work (new block height OR new merkle root)
        # Merkle root changes when pending transactions change
        current_merkle = pool_state.current_work.get('merkle_root') if pool_state.current_work else None
        
        if pool_state.current_work_height != new_height or current_merkle != new_merkle:
            if pool_state.current_work_height != new_height:
                print(f"📦 New work available: Block #{new_height}, Difficulty: {mining_info['difficulty']}")
                pool_state.current_work = mining_info
                pool_state.current_work_height = new_height
                pool_state.reset_round()
            elif current_merkle != new_merkle:
                print(f"📝 Updated merkle root for block #{new_height} (transactions changed)")
                pool_state.current_work = mining_info
                # Don't reset round for merkle root changes - miners can continue with new work
            
        return True
    except Exception as e:
        print(f"❌ Error updating work: {e}")
        return False


async def get_work_for_miner(miner_id: str) -> WorkAssignment:
    """Assign work to a miner with unique nonce range"""
    # Always update work to ensure we have the latest merkle_root and transactions
    # Even if block height hasn't changed, pending transactions may have changed
    await update_work()
    
    if not pool_state.current_work:
        raise HTTPException(status_code=503, detail="No work available from Stellaris node")
    
    work = pool_state.current_work
    
    # Assign nonce range - larger ranges since miners work at full difficulty
    # 10 million nonces per range (about 10-30 seconds of work at typical speeds)
    nonce_range_size = 10_000_000
    max_nonce = 2**32 - 1  # Maximum value for 4-byte nonce (Stellaris uses 4 bytes)
    
    # Reset if we're approaching the limit
    if pool_state.next_nonce_start > max_nonce - nonce_range_size:
        print(f"⚠️  Nonce space exhausted at {pool_state.next_nonce_start:,}, resetting to 0")
        pool_state.next_nonce_start = 0
    
    nonce_start = pool_state.next_nonce_start
    nonce_end = min(nonce_start + nonce_range_size, max_nonce)
    
    # Store current timestamp for this work assignment
    current_timestamp = int(time.time())
    
    # Track work assignment with timestamp for validation
    # Use unique key: miner_id:nonce_start:nonce_end to support multiple ranges per miner
    assignment_key = f"{miner_id}:{nonce_start}:{nonce_end}"
    pool_state.work_assignments[assignment_key] = {
        'miner_id': miner_id,
        'nonce_start': nonce_start,
        'nonce_end': nonce_end,
        'block_height': pool_state.current_work_height,
        'timestamp': current_timestamp,
        'assigned_at': time.time(),
        'completed': False
    }
    
    pool_state.nonce_ranges[miner_id] = (nonce_start, nonce_end)
    pool_state.next_nonce_start = nonce_end
    
    # Don't credit work units yet - must submit proof first
    
    # Update miner activity
    pool_state.update_miner_activity(miner_id)
    
    return WorkAssignment(
        block_height=pool_state.current_work_height,
        difficulty=work['difficulty'],
        previous_hash=work['last_block'].get('hash', (30_06_2005).to_bytes(32, ENDIAN).hex()),
        merkle_root=work['merkle_root'],
        timestamp=current_timestamp,  # Send the timestamp to miner
        nonce_start=nonce_start,
        nonce_end=nonce_end,
        pool_address=POOL_WALLET_ADDRESS,
        transactions=work['pending_transactions_hashes']
    )


# ==================== Share Validation ====================
def validate_share(block_hash: str, difficulty: float, target_difficulty: Optional[float] = None) -> tuple[bool, bool]:
    """
    Validate if a share meets network difficulty
    For pool mining, we only accept VALID BLOCKS at full network difficulty
    Returns: (is_valid_share, is_valid_block)
    """
    if not pool_state.current_work:
        return False, False
    
    last_block = pool_state.current_work['last_block']
    previous_hash = last_block.get('hash', (30_06_2005).to_bytes(32, ENDIAN).hex())
    
    # Network difficulty: hash must start with last N chars of previous hash
    chunk = previous_hash[-int(difficulty):]
    
    # Check decimal part for partial character matching
    decimal = difficulty % 1
    if decimal > 0:
        charset = '0123456789abcdef'
        count = ceil(16 * (1 - decimal))
        charset = charset[:count]
        idifficulty = int(difficulty)
        
        is_valid = (
            block_hash.startswith(chunk) and 
            len(block_hash) > idifficulty and
            block_hash[idifficulty] in charset
        )
    else:
        is_valid = block_hash.startswith(chunk)
    
    # For pool mining: valid share = valid block (no easy shares)
    return is_valid, is_valid


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


@app.post("/api/work_proof")
async def submit_work_proof(proof: WorkProof):
    """
    Submit proof that a miner completed their assigned work range.
    This prevents miners from spamming work requests without actually mining.
    """
    try:
        # Validate proof is for current work
        if proof.block_height != pool_state.current_work_height:
            return {
                "success": False,
                "message": "Proof is for old work"
            }
        
        # Check if this miner was assigned this work
        assignment_key = f"{proof.miner_id}:{proof.nonce_start}:{proof.nonce_end}"
        if assignment_key not in pool_state.work_assignments:
            return {
                "success": False,
                "message": "No work assignment found for this miner and range"
            }
        
        assignment = pool_state.work_assignments[assignment_key]
        
        # Check if already completed
        if assignment.get('completed'):
            return {
                "success": False,
                "message": "Work already submitted"
            }
        
        # Validate block height matches (proof might be stale if new block found)
        if proof.block_height != assignment['block_height']:
            return {
                "success": False,
                "message": "Proof is for wrong block height"
            }
        
        # Validate best_nonce is within assigned range
        if not (proof.nonce_start <= proof.best_nonce < proof.nonce_end):
            return {
                "success": False,
                "message": "Best nonce is outside assigned range"
            }
        
        # Validate timing (prevent impossibly fast completion)
        time_elapsed = time.time() - assignment['assigned_at']
        range_size = proof.nonce_end - proof.nonce_start
        
        # Minimum time check: can't hash faster than 1 billion H/s (very generous)
        min_time_required = range_size / 1_000_000_000  # seconds
        if time_elapsed < min_time_required:
            print(f"⚠️  Suspicious timing from {proof.miner_id}: {time_elapsed:.2f}s for {range_size:,} hashes")
            return {
                "success": False,
                "message": "Work completed suspiciously fast"
            }
        
        # Verify the hash is correct by recomputing it
        if not pool_state.current_work:
            return {"success": False, "message": "No current work"}
        
        work = pool_state.current_work
        difficulty = work['difficulty']
        previous_hash = work['last_block'].get('hash', (30_06_2005).to_bytes(32, ENDIAN).hex())
        merkle_root = work['merkle_root']
        timestamp = assignment.get('timestamp', int(time.time()))
        
        # Reconstruct the block content and verify the hash
        from stellaris.utils.general import string_to_bytes
        address_bytes = string_to_bytes(POOL_WALLET_ADDRESS)
        
        prefix = (
            bytes.fromhex(previous_hash) +
            address_bytes +
            bytes.fromhex(merkle_root) +
            timestamp.to_bytes(4, byteorder='little') +
            int(difficulty * 10).to_bytes(2, 'little')
        )
        
        if len(address_bytes) == 33:
            prefix = (2).to_bytes(1, 'little') + prefix
        
        block_content = prefix + proof.best_nonce.to_bytes(4, 'little')
        computed_hash = hashlib.sha256(block_content).hexdigest()
        
        if computed_hash != proof.best_hash:
            print(f"⚠️  Hash verification failed for {proof.miner_id}")
            print(f"   Expected: {proof.best_hash}")
            print(f"   Computed: {computed_hash}")
            return {
                "success": False,
                "message": "Hash verification failed - proof is invalid"
            }
        
        # Valid proof! Credit the miner with work units
        assignment['completed'] = True
        pool_state.round_work[proof.miner_id] += 1
        
        # Calculate hashrate
        hashrate = proof.hashes_computed / time_elapsed if time_elapsed > 0 else 0
        
        # Update miner activity with hashrate
        pool_state.update_miner_activity(proof.miner_id, shares=0, hashrate=hashrate)
        
        print(f"✅ Valid work proof from {proof.miner_id}")
        print(f"   Range: {proof.nonce_start:,} - {proof.nonce_end:,}")
        print(f"   Time: {time_elapsed:.1f}s")
        print(f"   Hashrate: {hashrate/1000:.1f} kH/s")
        print(f"   Work units this round: {pool_state.round_work[proof.miner_id]}")
        
        return {
            "success": True,
            "message": "Work proof accepted",
            "work_units": pool_state.round_work[proof.miner_id]
        }
        
    except Exception as e:
        print(f"❌ Work proof error from {proof.miner_id}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Work proof processing failed: {str(e)}"
        }


@app.post("/api/share")
async def submit_share(share: ShareSubmission, background_tasks: BackgroundTasks):
    """
    Submit a mining result (only valid blocks are submitted in this pool model)
    Miners mine at full difficulty and submit only when they find a valid block
    """
    try:
        # Validate nonce range (Stellaris uses 4-byte nonce)
        if share.nonce < 0 or share.nonce > 2**32 - 1:
            return {
                "success": False,
                "message": "Invalid nonce value (must be 32-bit)"
            }
        
        # Validate share is for current work
        if share.block_height != pool_state.current_work_height:
            return {
                "success": False,
                "message": "Share is for old work",
                "current_height": pool_state.current_work_height
            }
        
        # Check if a block has already been found and accepted for this height
        if pool_state.block_found_for_height == share.block_height:
            return {
                "success": False,
                "message": "Block already found for this height"
            }
        
        # Validate the block
        difficulty = pool_state.current_work['difficulty']
        is_valid_share, is_valid_block = validate_share(share.block_hash, difficulty)
        
        if not is_valid_block:
            # This means the miner submitted an invalid block
            # In production pool mining, we only accept valid blocks
            print(f"⚠️  {share.miner_id} submitted invalid block: {share.block_hash}")
            return {
                "success": False,
                "message": "Block does not meet network difficulty"
            }
        
        # Check if this block has already been submitted (prevent duplicate submissions)
        if share.block_hash in pool_state.submitted_blocks:
            print(f"⚠️  {share.miner_id} submitted duplicate block: {share.block_hash}")
            return {
                "success": False,
                "message": "Block already submitted"
            }
        
        # Mark this block as submitted
        pool_state.submitted_blocks.add(share.block_hash)
        
        # Mark that a block has been found for this height (prevent multiple blocks for same height)
        pool_state.block_found_for_height = share.block_height
        
        # Valid block found!
        print(f"🎉 VALID BLOCK FOUND by {share.miner_id}! Block #{share.block_height}")
        print(f"   Hash: {share.block_hash}")
        print(f"   Rejecting further submissions for this height")
        
        # Record block in database (track who found it for rewards)
        await record_share_db(
            share.miner_id,
            share.block_height,
            True,  # is_valid_block
            share.block_hash,
            difficulty
        )
        
        # Update round stats - this miner found the block!
        pool_state.round_shares[share.miner_id] += 1
        
        # Automatically grant 10 work units for finding the block
        pool_state.round_work[share.miner_id] += 10
        print(f"   Awarded 10 work units for finding block (total: {pool_state.round_work[share.miner_id]})")
        
        pool_state.update_miner_activity(share.miner_id, shares=1)
        
        # Submit block to network
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
        
    except Exception as e:
        print(f"❌ Share submission error from {share.miner_id}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Share processing failed: {str(e)}"
        }


@app.get("/api/stats")
async def get_pool_stats():
    """Get pool statistics"""
    pool_state.calculate_pool_hashrate()
    active_miners = pool_state.get_active_miners()
    
    # Calculate total work units this round
    total_work_units = sum(pool_state.round_work.values())
    
    return PoolStats(
        total_hashrate=pool_state.total_pool_hashrate,
        active_miners=len(active_miners),
        current_block_height=pool_state.current_work_height,
        current_difficulty=pool_state.current_work['difficulty'] if pool_state.current_work else 0,
        shares_this_round=total_work_units,  # Now represents work units, not shares
        pool_fee_percent=POOL_FEE_PERCENT,
        finder_bonus_percent=FINDER_BONUS_PERCENT
    )


@app.get("/api/miner/{miner_id}")
async def get_miner_stats(miner_id: str):
    """Get individual miner statistics"""
    stats = await get_miner_stats_db(miner_id)
    
    if not stats:
        raise HTTPException(status_code=404, detail="Miner not found")
    
    # Get current round stats (work units, not just blocks found)
    current_work_units = pool_state.round_work.get(miner_id, 0)
    current_hashrate = pool_state.active_miners.get(miner_id, {}).get('hashrate', 0)
    
    return MinerStats(
        miner_id=stats['miner_id'],
        wallet_address=stats['wallet_address'],
        current_hashrate=current_hashrate,
        shares_submitted=stats['total_shares'],
        shares_this_round=current_work_units,  # Work units this round
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
            print(f"💰 Block reward: {block_reward} STR")
            
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
    """Calculate payout distribution based on work contributed"""
    try:
        # Deduct pool fee
        pool_fee = reward * Decimal(POOL_FEE_PERCENT) / Decimal(100)
        distributable = reward - pool_fee
        
        # Calculate finder bonus (extra reward for finding the block)
        finder_bonus_amount = distributable * Decimal(FINDER_BONUS_PERCENT) / Decimal(100)
        remaining = distributable - finder_bonus_amount
        
        # Get all work units contributed this round
        total_work = sum(pool_state.round_work.values())
        
        if total_work == 0:
            print("⚠️  No work units in round, all reward goes to finder")
            finder_bonus_amount = distributable
            remaining = Decimal(0)
        
        # Calculate payout per work unit
        # Each miner gets paid proportionally to the work they contributed
        payouts = {}
        
        if remaining > 0 and total_work > 0:
            for miner_id, work_units in pool_state.round_work.items():
                share_reward = (remaining * Decimal(work_units)) / Decimal(total_work)
                # Quantize to 6 decimal places (SMALLEST = 1000000 supports 6 decimals)
                share_reward = share_reward.quantize(Decimal('0.000001'))
                payouts[miner_id] = payouts.get(miner_id, Decimal(0)) + share_reward
        
        # Add finder bonus to the miner who found the block
        finder_bonus_amount = finder_bonus_amount.quantize(Decimal('0.000001'))
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
        print(f"   Pool Fee: {pool_fee} DNR")
        print(f"   Finder Bonus: {finder_bonus_amount} DNR -> {finder_id}")
        print(f"   Total Work Units: {total_work}")
        print(f"   Miners Rewarded: {len(payouts)}")
        for miner_id, amount in payouts.items():
            work_units = pool_state.round_work.get(miner_id, 0)
            print(f"     {miner_id}: {amount} DNR ({work_units} work units)")
        
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
                    # Quantize amount to 6 decimal places to match SMALLEST (1000000)
                    amount = Decimal(str(miner['pending_balance'])).quantize(Decimal('0.000001'))
                    payouts.append({
                        'address': miner['wallet_address'],
                        'amount': amount
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
