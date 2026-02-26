#!/bin/bash
# One-time server setup for FaCaiBot on Amazon Linux 2023 (c7i.xlarge)
# Run as: sudo bash setup.sh
set -euo pipefail

echo "=== FaCaiBot Server Setup ==="

# ── System packages ─────────────────────────────────────────────────
echo "[1/8] Installing build dependencies..."
yum install -y gcc gcc-c++ make cmake pkg-config openssl-devel \
    docker ethtool chrony

# ── Rust toolchain (as ec2-user) ────────────────────────────────────
echo "[2/8] Installing Rust toolchain..."
if ! command -v rustup &>/dev/null; then
    sudo -u ec2-user bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'
fi

# ── Docker ──────────────────────────────────────────────────────────
echo "[3/8] Configuring Docker..."
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

# ── QuestDB (pinned to core 3) ─────────────────────────────────────
echo "[4/8] Starting QuestDB..."
docker rm -f questdb 2>/dev/null || true
docker run -d \
    --name questdb \
    --restart unless-stopped \
    -p 127.0.0.1:9009:9009 \
    -p 127.0.0.1:9000:9000 \
    -p 127.0.0.1:8812:8812 \
    -v questdb-data:/var/lib/questdb \
    --cpuset-cpus="3" \
    questdb/questdb:latest

# ── Kernel network tuning ──────────────────────────────────────────
echo "[5/8] Applying kernel network tuning..."
cp /home/ec2-user/FaCaiBot/deploy/sysctl.conf /etc/sysctl.d/99-facaibot.conf
sysctl -p /etc/sysctl.d/99-facaibot.conf

# ── ENA NIC tuning ──────────────────────────────────────────────────
echo "[6/8] Tuning ENA network adapter..."
ethtool -G eth0 rx 4096 tx 4096 2>/dev/null || echo "  (ring buffer resize not supported — skipping)"
ethtool -C eth0 rx-usecs 0 tx-usecs 0 rx-frames 1 tx-frames 1 2>/dev/null || echo "  (coalescing tune not supported — skipping)"

# ── Disable Transparent Huge Pages ──────────────────────────────────
echo "[7/8] Disabling THP..."
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag

# Make THP disable persistent across reboots
cat > /etc/tmpfiles.d/disable-thp.conf << 'EOF'
w /sys/kernel/mm/transparent_hugepage/enabled - - - - never
w /sys/kernel/mm/transparent_hugepage/defrag - - - - never
EOF

# ── CPU frequency governor ──────────────────────────────────────────
echo "[8/8] Setting CPU performance governor..."
for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    echo performance > "$cpu" 2>/dev/null || true
done

# ── IRQ affinity (move network IRQs to cores 2-3) ──────────────────
systemctl disable irqbalance 2>/dev/null || true
systemctl stop irqbalance 2>/dev/null || true
for irq in $(grep eth0 /proc/interrupts | awk '{print $1}' | tr -d ':'); do
    echo "2-3" > /proc/irq/$irq/smp_affinity_list 2>/dev/null || true
done

# ── Application directory ──────────────────────────────────────────
mkdir -p /opt/facaibot
chown ec2-user:ec2-user /opt/facaibot

# ── Clock sync verification ─────────────────────────────────────────
systemctl enable chronyd
systemctl restart chronyd
echo ""
echo "Clock sync status:"
chronyc tracking | head -5

# ── Systemd service ────────────────────────────────────────────────
cp /home/ec2-user/FaCaiBot/deploy/facaibot.service /etc/systemd/system/facaibot.service
systemctl daemon-reload

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Build:  cd /home/ec2-user/FaCaiBot && RUSTFLAGS=\"-C target-cpu=native\" cargo build --release"
echo "  2. Deploy: cp target/release/facaibot /opt/facaibot/"
echo "  3. Config: cp config.toml /opt/facaibot/"
echo "  4. Secrets: Create /opt/facaibot/.env (see .env.example) and chmod 600 it"
echo "  5. Start:  sudo systemctl enable --now facaibot"
echo "  6. Logs:   journalctl -u facaibot -f"
echo ""
echo "Optional: Add CPU isolation to grub for maximum latency reduction:"
echo "  Edit /etc/default/grub, add to GRUB_CMDLINE_LINUX:"
echo "    isolcpus=0,1 nohz_full=0,1 rcu_nocbs=0,1 intel_pstate=disable processor.max_cstate=1 idle=poll"
echo "  Then: grub2-mkconfig -o /boot/grub2/grub.cfg && reboot"
