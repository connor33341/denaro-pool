#!/usr/bin/env python3
"""
Test mining logic to verify block structure is correct
"""
import hashlib
import sys
sys.path.insert(0, 'miner')
from pool_miner import string_to_bytes

# Test data (similar to real network)
previous_hash = "0000000a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d"
pool_address = "DWMVFcRTZ8UMaWr2vsb7XkTmh7zaA57BQaDRGiAKB6qX6"
merkle_root = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
timestamp = 1234567890
difficulty = 7.9

# Build block content
address_bytes = string_to_bytes(pool_address)

prefix = (
    bytes.fromhex(previous_hash) +
    address_bytes +
    bytes.fromhex(merkle_root) +
    timestamp.to_bytes(4, byteorder='little') +
    int(difficulty * 10).to_bytes(2, 'little')
)

# Add version byte if compressed address
if len(address_bytes) == 33:
    prefix = (2).to_bytes(1, 'little') + prefix

print("Testing mining block structure")
print("=" * 60)
print(f"Previous hash: {previous_hash}")
print(f"Pool address: {pool_address}")
print(f"Address bytes length: {len(address_bytes)}")
print(f"Merkle root: {merkle_root}")
print(f"Timestamp: {timestamp}")
print(f"Difficulty: {difficulty}")
print(f"Block prefix length: {len(prefix)} bytes")
print()

# Test with nonce 0
nonce = 0
block_content = prefix + nonce.to_bytes(4, 'little')
block_hash = hashlib.sha256(block_content).hexdigest()

print(f"Nonce: {nonce}")
print(f"Block content (hex): {block_content.hex()}")
print(f"Block content length: {len(block_content)} bytes")
print(f"Block hash: {block_hash}")
print()

# Test difficulty check
chunk = previous_hash[-int(difficulty):]
pool_difficulty = max(1.0, difficulty * 0.1)
pool_chunk = previous_hash[-int(pool_difficulty):]

print(f"Network difficulty: {difficulty}")
print(f"Network chunk (last {int(difficulty)} chars): {chunk}")
print(f"Hash starts with network chunk: {block_hash.startswith(chunk)}")
print()
print(f"Pool difficulty: {pool_difficulty}")
print(f"Pool chunk (last {int(pool_difficulty)} chars): {pool_chunk}")
print(f"Hash starts with pool chunk: {block_hash.startswith(pool_chunk)}")
print()

# Try a few nonces to see if we can find a pool share
print("Testing first 100,000 nonces for pool share...")
for i in range(100000):
    block_content = prefix + i.to_bytes(4, 'little')
    block_hash = hashlib.sha256(block_content).hexdigest()
    
    if block_hash.startswith(pool_chunk):
        print(f"✅ Found pool share at nonce {i}!")
        print(f"   Hash: {block_hash}")
        break
    
    if i % 10000 == 0 and i > 0:
        print(f"   Tried {i:,} nonces...")
else:
    print("❌ No pool share found in first 100,000 nonces")
    print("   This is expected if pool difficulty is still relatively high")
