# FaCaiBot

High-frequency Polymarket arbitrage bot for BTC/ETH 15-minute prediction markets. Detects Binance price spikes via SBE binary feeds, enters cheap directional shares on the Polymarket CLOB before repricing, then hedges with the opposite side — locking in a sub-$1.00 pair that resolves to $1.00.

## Quick Start (Local)

```bash
# Prerequisites: Rust 1.85+ toolchain, Docker

# Start QuestDB (analytics only, not on execution path)
docker-compose up -d

# Configure
cp .env.example .env
# Edit .env — set MODE=simulation, add Binance Ed25519 key + Telegram creds

# Build and run
cargo build
cargo run
```

## Architecture

Three-layer lock-free pipeline connected by crossbeam SPSC channels:

```
Ingestor (gateway/)        Engine (engine/)           Executor (executor/)
───────────────────        ──────────────             ────────────────────
Dedicated OS thread        tokio task (core 1-2)      tokio task (core 1-2)
CPU-pinned core 0          Manual runtime (2 workers)

Binance SBE WS ──┐
  @depth20 (50ms)│
  @bestBidAsk    ├──► IngestorEvent ──► Strategy evaluation
Polymarket WS ───┤                      Spike detection
  Market + User  │                      Erosion cascade
Gamma API ───────┘                           │
                                       TradeSignal
                                             │
                                       Live: CLOB REST (SDK)
                                       Sim:  Telegram + QuestDB
```

- **Allocator**: jemalloc (predictable latency, no glibc arena contention)
- **Ingestor**: Dedicated OS thread, CPU-pinned core 0, single-threaded tokio runtime
- **Engine + Executor**: Manual tokio runtime with 2 worker threads pinned to cores 1-2
- **Decimal arithmetic**: All pricing uses `rust_decimal::Decimal` — never floats

See [ARCHITECTURE.md](ARCHITECTURE.md) for full system design, trade lifecycle, and risk controls.

## Build Commands

```bash
cargo build                # Debug build
cargo build --release      # Release (LTO, single codegen unit, stripped)
cargo run                  # Run the bot
cargo test                 # Run all tests (136 tests)
cargo clippy               # Lint
cargo fmt                  # Format
```

## Configuration

**`config.toml`** — All tuning parameters (spike detection, entry guards, capital, confidence, risk/erosion). All params have `#[serde(default)]` with production defaults.

**`.env`** — Secrets and infrastructure. Copy `.env.example` and fill in:

| Variable | Required | Description |
|----------|----------|-------------|
| `MODE` | Yes | `simulation` or `live` |
| `BINANCE_ED25519_API_KEY` | Yes | Ed25519 API key for SBE binary streams |
| `PRIVATE_KEY` | Live only | Hex wallet private key (EIP-712 signing) |
| `POLYMARKET_API_KEY/SECRET/PASSPHRASE` | Live only | L2 HMAC auth credentials |
| `TELEGRAM_BOT_TOKEN/CHAT_ID` | Sim only | Telegram reporting |
| `QUESTDB_URL` | No | Default `127.0.0.1:9009` |

## Key Documents

| File | Purpose |
|------|---------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design, trade lifecycle, risk controls, deployment |
| [TRADING_LOGIC.md](TRADING_LOGIC.md) | Complete trading logic reference |
| [CLAUDE.md](CLAUDE.md) | Compact project guide for AI-assisted development |
| `config.toml` | All tunable parameters with comments |
| `queries.sql` | QuestDB analytics queries (15 queries) |

---

## AWS Production Deployment

### Target

Sub-50ms from Binance spike detection to Polymarket Leg 1 order placement (excluding Binance's 50ms tick delivery). Achieved by co-locating with Polymarket's CLOB in AWS eu-west-2 (London).

### Expected Latency Budget

| Segment | Time |
|---------|------|
| SBE decode + spike detect | <0.1ms |
| crossbeam channel hops | <0.01ms |
| Engine evaluate() | <0.5ms |
| EIP-712 sign (SDK) | ~1ms |
| HTTPS POST to CLOB (same region) | 5-15ms |
| **Total** | **~7-17ms** |

### Infrastructure

| Component | Spec |
|-----------|------|
| **Instance** | `c7i.xlarge` — 4 vCPU (Ice Lake, 3.6 GHz), 8 GiB RAM, 12.5 Gbps ENA |
| **Region** | `eu-west-2` (London) — co-located with Polymarket CLOB |
| **OS** | Amazon Linux 2023 (kernel 6.1+, minimal footprint, ENA + chrony pre-configured) |
| **Storage** | 30 GiB gp3 (3000 IOPS baseline) |
| **QuestDB** | Docker on same instance, pinned to core 3 |
| **Cost** | ~$125/mo on-demand, ~$80/mo reserved (1yr, no upfront) |

**Core allocation**: core 0 = ingestor, cores 1-2 = engine+executor (tokio), core 3 = QuestDB + system

### Step 1: Launch EC2

1. Create a VPC with a single public subnet in `eu-west-2a`
2. Create a security group:
   - Inbound: SSH (22) from your IP only, QuestDB console (9000) from your IP only (optional)
   - Outbound: all (Binance WS, Polymarket WS+REST, Telegram, Gamma API)
3. Launch `c7i.xlarge` with Amazon Linux 2023 AMI, 30 GiB gp3, your SSH key

**Pre-flight checklist** (from your local machine):
- [ ] Have the EC2 SSH private key file (`.pem`) saved locally with `chmod 400` permissions
- [ ] Know the instance's public IP or DNS name from the AWS console
- [ ] Have SSH access working (test: `ssh -i key.pem ec2-user@<ip> "echo ok"` should print "ok")
- [ ] Have a way to authenticate with your private GitHub repo:
  - Option A: Local GitHub SSH key + `ssh -A` (SSH agent forwarding)
  - Option B: GitHub deploy key created and added to EC2
  - Option C: GitHub personal access token (for HTTPS cloning)

### Step 2: Server Setup

**From your local machine (macOS):**

```bash
# Make the key file private
chmod 400 /Users/ranveerbains/Documents/keypairs/facaibotkeypair.pem

# SSH in with -A to forward your local GitHub SSH key to EC2
ssh -A -i /Users/ranveerbains/Documents/keypairs/facaibotkeypair.pem \
  ec2-user@ec2-3-8-210-31.eu-west-2.compute.amazonaws.com
```

> **Note**: The `-A` flag (SSH agent forwarding) passes your local GitHub SSH key to the EC2 session so you can clone the private repo without adding a key to the server. If this doesn't work, see "Git Clone Failures" in Troubleshooting.

**On the EC2 instance — run these in order:**

```bash
# 1. Install git (not pre-installed on Amazon Linux 2023)
sudo yum install -y git

# 2. Clone the repo

# Option A: SSH (requires ssh -A or a deploy key set up)
git clone git@github.com:ranveerbains/FaCaiBot.git
cd FaCaiBot

# Option B: HTTPS (requires personal access token)
git clone https://<TOKEN>@github.com/ranveerbains/FaCaiBot.git
cd FaCaiBot
# 3. Run server setup — installs Rust, Docker, QuestDB, kernel tuning, systemd
#    Takes ~5-10 minutes. Safe to re-run if interrupted.
sudo bash deploy/setup.sh

# 4. Load Rust into your current shell (setup.sh installs it but doesn't reload)
source $HOME/.cargo/env
cargo --version   # Should print: cargo 1.xx.x
```

**What setup.sh installs:**
- `git`, `gcc`, `cmake`, `openssl-devel`, `docker`, `ethtool`, `chrony`
- Rust toolchain (as ec2-user via rustup)
- Docker daemon + QuestDB container (pinned to core 3)
- Kernel network tuning, THP disabled, CPU governor set to `performance`
- `/etc/systemd/system/facaibot.service` systemd unit

### Step 3: On-Chain Approvals (One-Time)

Before the bot can trade, your EOA wallet needs 3 on-chain token approvals on Polygon mainnet. This is a **one-time** operation — never needs to be repeated (approvals are persistent on-chain).

#### 3a. Install Foundry (Local Machine)

Foundry is a CLI toolkit for Ethereum. We use its `cast` command to send approval transactions.

```bash
# Download and install Foundry
curl -L https://foundry.paradigm.xyz | bash

# Add Foundry to your current shell session
source ~/.bashrc
# (or: source ~/.zshrc if you use zsh)

# Install the Rust toolchain Foundry needs
foundryup

# Verify installation
cast --version
```

**Expected output**: `cast 0.3.0 (abc1234 ...)`

If you get "command not found", reload your shell: `source ~/.bashrc`

#### 3b. Check Your Polygon Wallet

1. Go to [Polygonscan.com](https://polygonscan.com/) → search your EOA address
2. **POL balance** must be ≥ 0.01 (for gas on 3 transactions, costs ~$0.005)
3. **USDC.e balance** must be > 0 (this is your trading capital)
4. Copy your private key (hex format, starting with `0x`) — same one from `.env`

#### 3c. Dry Run First (Test Without Sending)

From your FaCaiBot directory, run the approval script in dry-run mode:

```bash
PRIVATE_KEY=0x<your-hex-private-key> DRY_RUN=1 ./examples/approve_contracts.sh
```

Replace `<your-hex-private-key>` with your actual key (e.g., `0x1234567890abcdef...`).

**Expected output:**
```
Wallet:  0x1234...abcd
Chain:   Polygon mainnet (137)

POL balance:    0.025
USDC.e balance: 100.00 USDC.e

Checking existing approvals...
  USDC.e → CTF allowance:           0 (need max uint256)
  CTF → CTF Exchange approved:       false
  CTF → NegRisk Exchange approved:   false

DRY_RUN=1 — no transactions sent. Remove DRY_RUN to execute.
```

If this works, your setup is correct. Proceed to step 3d.

#### 3d. Execute Approvals (Send 3 Transactions)

```bash
PRIVATE_KEY=0x<your-hex-private-key> ./examples/approve_contracts.sh
```

This will send 3 transactions to Polygon mainnet. Each takes ~10-30 seconds to confirm.

**Expected output:**
```
[1/3] Approving USDC.e for CTF contract...
[tx hash]: 0xabc123...
  ✓ USDC.e → CTF approved
[2/3] Approving CTF tokens for CTF Exchange...
[tx hash]: 0xdef456...
  ✓ CTF → CTF Exchange approved
[3/3] Approving CTF tokens for Neg Risk CTF Exchange...
[tx hash]: 0xghi789...
  ✓ CTF → Neg Risk CTF Exchange approved

All approvals complete. Your wallet is ready for Polymarket trading.
```

**If some were already approved**, the script skips those automatically.

#### 3e. Verify Approvals Are Set

Run the dry-run again to confirm all three are now approved:

```bash
PRIVATE_KEY=0x<your-hex-private-key> DRY_RUN=1 ./examples/approve_contracts.sh
```

All should show as `true` or with max uint256 value:
```
  USDC.e → CTF allowance:           115792089...933129639935 ✓
  CTF → CTF Exchange approved:       true ✓
  CTF → NegRisk Exchange approved:   true ✓
```

#### What Each Approval Does

1. **USDC.e → CTF (ConditionalTokens)**: Lets the CTF contract convert your USDC.e into YES/NO outcome tokens
2. **CTF → CTF Exchange**: Lets the standard Polymarket exchange settle your YES/NO trades
3. **CTF → Neg Risk CTF Exchange**: Lets the neg-risk exchange (used for BTC/ETH 15-min markets) settle your positions

Once set, these approvals never expire and never need to be repeated (unless you change wallets).

### Step 4: Build

```bash
# Build with native CPU optimizations (AVX-512 on Ice Lake)
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### Step 5: Deploy

```bash
# Copy binary and config to install directory
cp target/release/facaibot /opt/facaibot/
cp config.toml /opt/facaibot/

# Create .env with production secrets
 # Set MODE=live, fill all credentials
cp .env.example /opt/facaibot/.env
nano /opt/facaibot/.env  
chmod 600 /opt/facaibot/.env

# Start the service
sudo systemctl enable --now facaibot

# To stop the service
sudo systemctl stop facaibot
```

### Step 6: Verify

```bash
# Check service status
sudo systemctl status facaibot

# Live logs (press `Ctrl+C` to exit)
journalctl -u facaibot -f

# Last hour
journalctl -u facaibot --since "1 hour ago"
```

Also verify Telegram alerts are arriving (startup message, spike diagnostics every 60s).

#### Clock Sync Check (Critical)

The bot compares Binance SBE event timestamps and Polymarket book timestamps against wall-clock time. If the system clock drifts, events are discarded as stale even when the feed is healthy — silently blocking all entries.

**Check chrony is locked to the AWS Time Sync Service:**

```bash
chronyc tracking
```

Expected output (healthy):

```
Reference ID    : A9FEA97B (169.254.169.123)   ← AWS Time Sync (on-metal)
System time     : 0.000000123 seconds fast of NTP time
Last offset     : +0.000000089 seconds
RMS offset      : 0.000000112 seconds
Frequency       : 12.345 ppm fast
Stratum         : 3
```

Key fields:
- **Reference ID** must be `169.254.169.123` (AWS Time Sync). If it shows a public NTP server, latency is higher.
- **System time / Last offset** must be `< 1ms`. Anything above 10ms will cause `stale_book_ms` breaches; above 50ms will cause `binance_stale_event_ms` breaches.

If chrony is not running:

```bash
sudo systemctl enable --now chronyd
chronyc tracking   # verify it locks within ~30s
```

**Verify no stale events are being discarded after startup:**

```bash
# Binance SBE — check `stale` field in the "spike 60s" log (should be 0 or near-zero)
journalctl -u facaibot | grep "spike 60s"

# Polymarket book — stale book blocks entry, counted as `rej_stale` in "engine 60s" log
journalctl -u facaibot | grep "engine 60s"
```

In the `spike 60s` log, the `stale` field is a cumulative count of Binance SBE events discarded because their timestamp exceeded `binance_stale_event_ms` (100ms). On a correctly synced c7i.xlarge in eu-west-2, SBE round-trip should be <10ms and `stale` should not grow. In the `engine 60s` log, `rej_stale` counts spikes that were blocked because the Polymarket book snapshot was older than `stale_book_ms` (150ms). Raise these thresholds only after ruling out a clock drift issue with `chronyc tracking`.

### Updating

After pushing changes to `main`:

```bash
bash deploy/deploy.sh
```

This pulls latest code, rebuilds with `target-cpu=native`, restarts the service, and tails recent logs.

### Health Monitoring

**Built-in** (already in the bot):
- 60-second diagnostic logs: spike detector, engine, executor
- Telegram alerts: trade completions, emergency exits, market summaries, session summaries

**External** (deploy scripts):
- `deploy/healthcheck.sh` — cron job (every 5 min) that sends a Telegram alert if the service is down
- `journalctl -u facaibot` — full structured logs via systemd journal

Add the health check to cron:
```bash
echo "*/5 * * * * /opt/facaibot/healthcheck.sh" | crontab -
```

### Optional: CPU Isolation (Maximum Latency Reduction)

For sub-microsecond jitter reduction, add kernel boot parameters:

```bash
# Edit /etc/default/grub, add to GRUB_CMDLINE_LINUX:
#   isolcpus=0,1 nohz_full=0,1 rcu_nocbs=0,1 intel_pstate=disable processor.max_cstate=1 idle=poll

sudo grub2-mkconfig -o /boot/grub2/grub.cfg
sudo reboot
```

This removes cores 0-1 from the kernel scheduler entirely — only explicitly pinned processes (ingestor, tokio workers) run on them. Eliminates timer tick interrupts, RCU callbacks, and CPU sleep state wake-up latency.

### Security Notes

- `.env` file: `chmod 600`, never committed to git. Contains wallet private key and API credentials
- SSH: key-based auth only, root login disabled
- No inbound ports needed for bot operation (all connections are outbound)
- QuestDB console (port 9000): bind to your IP only, or skip exposing it entirely

### Deployment Files

```
deploy/
├── facaibot.service   # systemd unit (auto-restart, CPU affinity, security hardening)
├── sysctl.conf        # Kernel network tuning (/etc/sysctl.d/99-facaibot.conf)
├── setup.sh           # One-time server provisioning
├── deploy.sh          # Update deployment (git pull, build, restart)
└── healthcheck.sh     # Cron health check with Telegram alerts
```

---

## Troubleshooting

### Git Clone Failures ("Permission denied (publickey)")

**Root cause**: EC2 instance cannot authenticate with your private GitHub repo. SSH key is on your local machine, not on EC2.

**Solution 1: SSH Agent Forwarding (Simplest, one-time setup)**

From your local machine, use `ssh -A` to forward your SSH key:
```bash
ssh -A -i /path/to/facaibotkeypair.pem ec2-user@<ec2-ip>
```

Then on EC2, clone normally:
```bash
git clone git@github.com:ranveerbains/FaCaiBot.git
```

This works because `-A` forwards your local SSH agent to the EC2 instance, so GitHub sees your credentials.

**Solution 2: GitHub Deploy Key (More secure, persistent)**

If SSH forwarding doesn't work, create a deploy key on the EC2 instance:

On EC2:
```bash
# Generate an ED25519 key pair (without passphrase)
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ""

# Print the public key
cat ~/.ssh/id_ed25519.pub
```

Then:
1. Go to GitHub repo → Settings → Deploy keys
2. Click "Add deploy key"
3. Paste the output from `cat ~/.ssh/id_ed25519.pub`
4. Check "Allow write access" (needed for deployments)
5. Click "Add key"

Back on EC2, clone should now work:
```bash
git clone git@github.com:ranveerbains/FaCaiBot.git
```

**Solution 3: HTTPS + Personal Access Token (No SSH setup)**

From your local machine, generate a [GitHub personal access token](https://github.com/settings/tokens) with `repo` scope.

On EC2, clone with HTTPS:
```bash
git clone https://<your-github-username>:<personal-access-token>@github.com/ranveerbains/FaCaiBot.git
```

Or use the interactive prompt:
```bash
git clone https://github.com/ranveerbains/FaCaiBot.git
# GitHub will prompt: Username? → <github-username>
#                     Password? → <personal-access-token>
```

**Our recommendation**: Use **Solution 1 (SSH forwarding)** for one-time setup, or **Solution 2 (deploy key)** if you plan to re-deploy via `bash deploy/deploy.sh` later.

### "command not found: git" or "docker"

**Root cause**: These tools are not pre-installed on Amazon Linux 2023. The `setup.sh` script installs them, but you need git available *before* running setup.sh (to clone the repo).

**Solution**: Follow the Step 2 instructions exactly. After SSHing in, run:
```bash
sudo yum install -y git
```
*before* cloning. Then you can proceed with the rest of the setup. The `setup.sh` script will re-install git along with everything else, so running it again is safe.

### "command not found: cargo" or "rustup"

**Root cause**: Rust was installed for the `ec2-user` account, but your shell environment isn't loading it. Amazon Linux shells don't source `.bashrc` by default.

**Solution**: Add Rust to your PATH manually (this is a one-time fix):
```bash
source $HOME/.cargo/env
```

Or verify it was installed:
```bash
which rustup   # Should print /home/ec2-user/.cargo/bin/rustup
```

If it still doesn't work, re-run the setup script:
```bash
sudo bash deploy/setup.sh   # Re-runs steps 1-9, idempotent
```

### "permission denied: /opt/facaibot" or similar

**Root cause**: File permissions after deployment.

**Solution**:
```bash
# Check ownership
ls -la /opt/facaibot/

# Fix if needed (as root)
sudo chown ec2-user:ec2-user /opt/facaibot/*
sudo chmod 600 /opt/facaibot/.env
```

### systemd service fails to start

Check the logs:
```bash
journalctl -u facaibot -n 50   # Last 50 lines
journalctl -u facaibot -f      # Follow live
```

Common issues:
- **`.env` not found**: Verify `/opt/facaibot/.env` exists and is readable
- **Binary not found**: Verify `/opt/facaibot/facaibot` exists and is executable
- **Clock skew**: Check `chronyc tracking` — if offset > 1ms, time sync is broken
- **Port conflict**: Verify QuestDB port 9009 is available: `sudo netstat -tulpn | grep 9009`
