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

### Step 2: Server Setup

```bash
ssh -i your-key.pem ec2-user@<public-ip>

# Clone the repo
git clone git@github.com:ranveerbains/FaCaiBot.git
cd FaCaiBot

# Run the one-time setup script (installs Rust, Docker, QuestDB, kernel tuning)
sudo bash deploy/setup.sh
```

The setup script handles:
- Rust toolchain + build dependencies
- Docker + QuestDB (pinned to core 3)
- Kernel network tuning (TCP low-latency, buffer sizes, THP disabled)
- ENA NIC tuning (ring buffers, interrupt coalescing disabled)
- IRQ affinity (network interrupts moved to cores 2-3)
- CPU frequency locked to max (`performance` governor)
- Clock sync verified (Amazon Time Sync Service, sub-microsecond accuracy)
- systemd service installed

### Step 3: On-Chain Approvals (One-Time)

Before the bot can trade, your EOA wallet needs 3 on-chain approvals. This is a **one-time** operation — never needs to be repeated.

**Prerequisites:**
- [Foundry](https://getfoundry.sh/) installed: `curl -L https://foundry.paradigm.xyz | bash && foundryup`
- USDC.e in your wallet on Polygon (your trading capital)
- ~0.01 POL for gas (3 small transactions)

```bash
# Dry run first — checks balances and existing approvals, sends nothing
PRIVATE_KEY=0x... DRY_RUN=1 ./examples/approve_contracts.sh

# Execute approvals (idempotent — skips any already set)
PRIVATE_KEY=0x... ./examples/approve_contracts.sh
```

This approves:
1. **USDC.e → CTF contract** — so CTF can split your USDC.e into outcome tokens
2. **CTF tokens → CTF Exchange** — so the exchange can settle standard trades
3. **CTF tokens → Neg Risk CTF Exchange** — for neg-risk markets (BTC/ETH 15-min)

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
cp .env.example /opt/facaibot/.env
nano /opt/facaibot/.env   # Set MODE=live, fill all credentials
chmod 600 /opt/facaibot/.env

# Start the service
sudo systemctl enable --now facaibot
```

### Step 6: Verify

```bash
# Check service status
sudo systemctl status facaibot

# Live logs
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
