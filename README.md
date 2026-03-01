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
cargo test                 # Run all tests (142 tests)
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
| `TELEGRAM_ALLOWED_USER_ID` | No | Enable Telegram bot control (get from `@userinfobot`) |
| `QUESTDB_URL` | No | Default `127.0.0.1:9009` |

## Telegram Bot Control

When `TELEGRAM_ALLOWED_USER_ID` is set in `.env`, the bot accepts commands via Telegram — no SSH required.

**Setup**: Message `@userinfobot` on Telegram to get your user ID. Add it to `.env`:
```
TELEGRAM_ALLOWED_USER_ID=123456789
```

**Commands**:

| Command | Example | Action |
|---------|---------|--------|
| `/trades on\|off` | `/trades off` | Toggle trade notifications |
| `/summary on\|off` | `/summary off` | Toggle market summary notifications |
| `/stop` | `/stop` | Graceful shutdown (drains open position first) |
| `/set <param> <value>` | `/set spike_detection.multiplier 4.0` | Update config + restart |
| `/config [section]` | `/config risk` | Show current config values |
| `/status` | `/status` | Show uptime, mode, market, counters |
| `/help` | `/help` | List all commands |

**Drain mode**: `/stop` and `/set` never abandon open positions. If a trade is in progress, the bot blocks new entries, lets the current Leg 2 resolve through its normal erosion/emergency cycle, then exits. `/stop` exits cleanly (no restart). `/set` restarts with the new config via systemd.

**Security**: Commands are only accepted from the configured user ID and chat ID. `/set` validates against a strict allowlist of 27 parameters with min/max range checks.

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

Do this via Polygonscan using MetaMask (or any browser wallet). No CLI tools needed.

**Prerequisites:**
- MetaMask connected to **Polygon Mainnet** with your trading wallet
- POL balance ≥ 0.01 for gas (~$0.005 total for all 3 transactions)
- USDC.e balance > 0 (your trading capital)

#### Approval 1 of 3: USDC.e → CTF Contract

Lets the CTF contract convert your USDC.e into YES/NO outcome tokens.

1. Open: `https://polygonscan.com/address/0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174#writeProxyContract`
2. Click **"Connect to Web3"** → connect MetaMask → confirm you're on Polygon Mainnet
3. Find the **`approve`** function and expand it
4. Fill in:
   - `spender (address)`: `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`
   - `amount (uint256)`: `115792089237316195423570985008687907853269984665640564039457584007913129639935`
5. Click **Write** → confirm in MetaMask → wait for the transaction to confirm (~10-30s)

#### Approval 2 of 3: CTF → CTF Exchange

Lets the standard Polymarket exchange settle your YES/NO trades.

1. Open: `https://polygonscan.com/address/0x4D97DCd97eC945f40cF65F87097ACe5EA0476045#writeContract`
2. Click **"Connect to Web3"** (stays connected from above)
3. Find the **`setApprovalForAll`** function and expand it
4. Fill in:
   - `operator (address)`: `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`
   - `approved (bool)`: `true`
5. Click **Write** → confirm in MetaMask → wait for confirmation

#### Approval 3 of 3: CTF → Neg Risk CTF Exchange

Lets the neg-risk exchange (used for BTC/ETH 15-min markets) settle your positions.

1. Stay on the same CTF contract page from Approval 2
2. Find **`setApprovalForAll`** again
3. Fill in:
   - `operator (address)`: `0xC5d563A36AE78145C45a50134d48A1215220f80b`
   - `approved (bool)`: `true`
4. Click **Write** → confirm in MetaMask → wait for confirmation

#### Verify All 3 Are Set

**Approval 1** (USDC.e allowance for CTF):
1. Open: `https://polygonscan.com/address/0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174#readProxyContract`
2. Find **`allowance`** → fill in your wallet as `owner`, `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` as `spender`
3. Click **Query** → should return `115792089237316195423570985008687907853269984665640564039457584007913129639935`

**Approvals 2 & 3** (CTF setApprovalForAll):
1. Open: `https://polygonscan.com/address/0x4D97DCd97eC945f40cF65F87097ACe5EA0476045#readContract`
2. Find **`isApprovedForAll`** → fill in your wallet as `owner`, `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` as `operator`
3. Click **Query** → should return `true`
4. Repeat with operator `0xC5d563A36AE78145C45a50134d48A1215220f80b` → should return `true`

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
