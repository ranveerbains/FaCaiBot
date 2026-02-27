#!/usr/bin/env bash
#
# One-time on-chain approvals for Polymarket EOA trading (Polygon mainnet).
#
# Run this ONCE from your VPS before starting the bot. Requires:
#   - cast (Foundry): curl -L https://foundry.paradigm.xyz | bash && foundryup
#   - POL in your wallet for gas (~0.01 POL total for all 3 txns)
#   - Your private key (same one in .env PRIVATE_KEY)
#
# Usage:
#   chmod +x examples/approve_contracts.sh
#   PRIVATE_KEY=0x... ./examples/approve_contracts.sh
#
# To verify approvals without sending txns (dry run):
#   PRIVATE_KEY=0x... DRY_RUN=1 ./examples/approve_contracts.sh

set -euo pipefail

# ─── Contract addresses (Polygon mainnet, chain ID 137) ──────────────────────

RPC_URL="https://polygon-rpc.com"
CHAIN_ID=137

USDC_E="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF="0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_EXCHANGE="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_CTF_EXCHANGE="0xC5d563A36AE78145C45a50134d48A1215220f80a"

MAX_UINT256="115792089237316195423570985008687907853269984665640564039457584007913129639935"

# ─── Validate inputs ─────────────────────────────────────────────────────────

if [ -z "${PRIVATE_KEY:-}" ]; then
    echo "ERROR: PRIVATE_KEY env var is required."
    echo "Usage: PRIVATE_KEY=0x... ./examples/approve_contracts.sh"
    exit 1
fi

if ! command -v cast &> /dev/null; then
    echo "ERROR: 'cast' not found. Install Foundry:"
    echo "  curl -L https://foundry.paradigm.xyz | bash && foundryup"
    exit 1
fi

# Derive wallet address from private key
WALLET=$(cast wallet address "$PRIVATE_KEY")
echo "Wallet:  $WALLET"
echo "Chain:   Polygon mainnet ($CHAIN_ID)"
echo ""

# ─── Check balances ──────────────────────────────────────────────────────────

POL_BALANCE=$(cast balance "$WALLET" --rpc-url "$RPC_URL" --ether)
USDC_BALANCE=$(cast call "$USDC_E" "balanceOf(address)(uint256)" "$WALLET" --rpc-url "$RPC_URL")
# USDC.e has 6 decimals
USDC_HUMAN=$(echo "scale=2; $USDC_BALANCE / 1000000" | bc 2>/dev/null || echo "$USDC_BALANCE raw")

echo "POL balance:    $POL_BALANCE"
echo "USDC.e balance: $USDC_HUMAN USDC.e"
echo ""

if [ "$(echo "$POL_BALANCE < 0.005" | bc 2>/dev/null || echo 0)" = "1" ]; then
    echo "WARNING: Low POL balance. You need ~0.01 POL for gas (3 transactions)."
    echo "Send POL to $WALLET before proceeding."
    echo ""
fi

# ─── Check existing approvals ────────────────────────────────────────────────

echo "Checking existing approvals..."

USDC_ALLOWANCE=$(cast call "$USDC_E" "allowance(address,address)(uint256)" "$WALLET" "$CTF" --rpc-url "$RPC_URL")
CTF_APPROVED=$(cast call "$CTF" "isApprovedForAll(address,address)(bool)" "$WALLET" "$CTF_EXCHANGE" --rpc-url "$RPC_URL")
NEG_RISK_APPROVED=$(cast call "$CTF" "isApprovedForAll(address,address)(bool)" "$WALLET" "$NEG_RISK_CTF_EXCHANGE" --rpc-url "$RPC_URL")

echo "  USDC.e → CTF allowance:           $USDC_ALLOWANCE (need max uint256)"
echo "  CTF → CTF Exchange approved:       $CTF_APPROVED"
echo "  CTF → NegRisk Exchange approved:   $NEG_RISK_APPROVED"
echo ""

# ─── Dry run mode ────────────────────────────────────────────────────────────

if [ "${DRY_RUN:-}" = "1" ]; then
    echo "DRY_RUN=1 — no transactions sent. Remove DRY_RUN to execute."
    exit 0
fi

# ─── Send approval transactions ──────────────────────────────────────────────

NEEDED=0

# 1. USDC.e → CTF: approve(spender, amount)
if [ "$USDC_ALLOWANCE" = "0" ] || [ "$USDC_ALLOWANCE" != "$MAX_UINT256" ]; then
    echo "[1/3] Approving USDC.e for CTF contract..."
    cast send "$USDC_E" \
        "approve(address,uint256)" "$CTF" "$MAX_UINT256" \
        --private-key "$PRIVATE_KEY" \
        --rpc-url "$RPC_URL" \
        --chain "$CHAIN_ID"
    echo "  ✓ USDC.e → CTF approved"
    NEEDED=1
else
    echo "[1/3] USDC.e → CTF already approved. Skipping."
fi

# 2. CTF → CTF Exchange: setApprovalForAll(operator, approved)
if [ "$CTF_APPROVED" = "false" ]; then
    echo "[2/3] Approving CTF tokens for CTF Exchange..."
    cast send "$CTF" \
        "setApprovalForAll(address,bool)" "$CTF_EXCHANGE" true \
        --private-key "$PRIVATE_KEY" \
        --rpc-url "$RPC_URL" \
        --chain "$CHAIN_ID"
    echo "  ✓ CTF → CTF Exchange approved"
    NEEDED=1
else
    echo "[2/3] CTF → CTF Exchange already approved. Skipping."
fi

# 3. CTF → Neg Risk CTF Exchange: setApprovalForAll(operator, approved)
if [ "$NEG_RISK_APPROVED" = "false" ]; then
    echo "[3/3] Approving CTF tokens for Neg Risk CTF Exchange..."
    cast send "$CTF" \
        "setApprovalForAll(address,bool)" "$NEG_RISK_CTF_EXCHANGE" true \
        --private-key "$PRIVATE_KEY" \
        --rpc-url "$RPC_URL" \
        --chain "$CHAIN_ID"
    echo "  ✓ CTF → Neg Risk CTF Exchange approved"
    NEEDED=1
else
    echo "[3/3] CTF → Neg Risk Exchange already approved. Skipping."
fi

echo ""
if [ "$NEEDED" = "1" ]; then
    echo "All approvals complete. Your wallet is ready for Polymarket trading."
else
    echo "All approvals were already in place. Nothing to do."
fi
echo ""
echo "You can verify by re-running with DRY_RUN=1:"
echo "  PRIVATE_KEY=0x... DRY_RUN=1 ./examples/approve_contracts.sh"
