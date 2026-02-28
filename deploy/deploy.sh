#!/bin/bash
# Deploy/update FaCaiBot on the EC2 instance.
# Run as ec2-user: bash deploy.sh
set -euo pipefail

export PATH="$HOME/.cargo/bin:$PATH"

REPO_DIR="/home/ec2-user/FaCaiBot"
INSTALL_DIR="/opt/facaibot"

echo "=== FaCaiBot Deploy ==="

# Pull latest code
cd "$REPO_DIR"
echo "[1/4] Pulling latest code..."
git pull origin main

# Build release binary with native CPU optimizations
echo "[2/4] Building release binary (this may take a few minutes)..."
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Stop, copy, start
echo "[3/4] Deploying binary and config..."
sudo systemctl stop facaibot 2>/dev/null || true
cp target/release/facaibot "$INSTALL_DIR/facaibot"
cp config.toml "$INSTALL_DIR/config.toml"
sudo systemctl start facaibot

# Verify
echo "[4/4] Verifying..."
sleep 2
if systemctl is-active --quiet facaibot; then
    echo "FaCaiBot is running."
    echo ""
    echo "Recent logs:"
    journalctl -u facaibot -n 10 --no-pager
else
    echo "ERROR: FaCaiBot failed to start!"
    journalctl -u facaibot -n 30 --no-pager
    exit 1
fi

echo ""
echo "=== Deploy complete ($(date)) ==="
