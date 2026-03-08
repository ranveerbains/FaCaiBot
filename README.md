# FaCaiBot

Polymarket arbitrage bot for BTC 5-minute prediction markets. Detects Binance price spikes via SBE binary feeds, enters cheap directional shares on the CLOB before repricing, then hedges the opposite side — locking in a sub-$1.00 pair that resolves to $1.00.

See [ARCHITECTURE.md](ARCHITECTURE.md) for full system design and [TRADING_LOGIC.md](TRADING_LOGIC.md) for trading logic details.

---

## Production Deployment (EC2)

### Step 1: Launch EC2

- Instance: `c7i.xlarge`, Amazon Linux 2023, 30 GiB gp3, `eu-west-2` (London)
- Security group: SSH (22) inbound from your IP only, all outbound
- use ed25519 keypair encryption

### Step 2: SSH In

```bash
chmod 400 /Users/ranveerbains/Documents/keypairs/facaibotkeypair.pem

ssh -i /Users/ranveerbains/Documents/keypairs/facaibotkeypair.pem \
ec2-user@ec2-34-245-184-42.eu-west-1.compute.amazonaws.com

(ec2questdb)
 ssh -i /Users/ranveerbains/Documents/keypairs/facaibotkeypair.pem -L 9000:localhost:9000 ec2-user@ec2-34-245-184-42.eu-west-1.compute.amazonaws.com
```

> `-A` forwards your local GitHub SSH key so you can clone without adding a key to the server
### Step 3: Server Setup (One-Time)

```bash
sudo yum install -y git
git clone https://github.com/ranveerbains/FaCaiBot.git
cd FaCaiBot

sudo bash deploy/setup.sh     # installs Rust, Docker, QuestDB, systemd unit (~5-10 min)
source $HOME/.cargo/env
cargo --version               # verify Rust loaded
```

### Step 4: On-Chain Approvals (One-Time)

Your wallet needs 3 approvals on Polygon mainnet before the bot can trade. Do this once via Polygonscan with MetaMask. You need a small POL balance for gas (~$0.005 total).

**Approval 1 — USDC.e → CTF Contract**

1. Go to: `https://polygonscan.com/address/0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174#writeProxyContract`
2. Connect to Web3 → MetaMask (Polygon Mainnet)
3. Find `approve`, fill in:
   - `spender`: `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`
   - `amount`: `115792089237316195423570985008687907853269984665640564039457584007913129639935`
4. Write → confirm in MetaMask

**Approval 2 — CTF → CTF Exchange**

1. Go to: `https://polygonscan.com/address/0x4D97DCd97eC945f40cF65F87097ACe5EA0476045#writeContract`
2. Find `setApprovalForAll`, fill in:
   - `operator`: `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`
   - `approved`: `true`
3. Write → confirm

**Approval 3 — CTF → Neg Risk CTF Exchange**

1. Same page as Approval 2, find `setApprovalForAll` again:
   - `operator`: `0xC5d563A36AE78145C45a50134d48A1215220f80b`
   - `approved`: `true`
2. Write → confirm

These never expire. Skip this step on future deployments.

### Step 5: Configure

```bash
cp .env.example /opt/facaibot/.env
nano /opt/facaibot/.env       # fill in all credentials (see table below)
chmod 600 /opt/facaibot/.env
cp config.toml /opt/facaibot/
```

**`.env` variables:**

| Variable | Required | Description |
|----------|----------|-------------|
| `MODE` | Yes | `live` or `simulation` |
| `BINANCE_ED25519_API_KEY` | Yes | Ed25519 key for Binance SBE binary streams |
| `PRIVATE_KEY` | Live only | Hex wallet private key (EIP-712 signing) |
| `POLYMARKET_API_KEY` | Live only | L2 HMAC API key (UUID) |
| `POLYMARKET_SECRET` | Live only | L2 HMAC secret |
| `POLYMARKET_PASSPHRASE` | Live only | L2 HMAC passphrase |
| `TELEGRAM_BOT_TOKEN` | Sim only | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Sim only | Telegram chat ID |
| `TELEGRAM_ALLOWED_USER_ID` | Optional | Enables Telegram bot control (get from `@userinfobot`) |

**`config.toml`** — all tuning parameters (spike detection, capital, entry guards, erosion). Defaults are production-ready.

### Step 5b: Recovery of Stuck Positions (One-Time if Applicable)

If you have unresolved positions from prior sessions that the Data API can't find (because markets have resolved), create `redeems.txt` in the bot's working directory:

```bash
cd /opt/facaibot
cat > redeems.txt << 'EOF'
0x<condition_id_1>
0x<condition_id_2>
...
EOF
```

Find stuck condition IDs by:
1. Checking your bot's Telegram trade history (look for completed trades)
2. Checking Polygonscan for ERC-1155 CTF holdings: `https://polygonscan.com/address/<YOUR_WALLET>#tokentxnsErc1155`

The bot will automatically:
- **Write** condition IDs to `redeems.txt` once per traded market (at rotation or shutdown)
- **Read** the file during `/redeem` and merge with Data API positions
- **Clean up** successfully redeemed IDs from the file

You can manually trigger redemption any time with `/redeem` or `/redeem <condition_id>` via Telegram (won't affect trading logic).

### Step 6: Smoke Test

Verifies your credentials and on-chain approvals work before going live.

```bash
cd ~/FaCaiBot
cp /opt/facaibot/.env .env
cargo run --example smoke_test_clob
#if it doesnt work try this
  ln -s /opt/facaibot/.env .env
  cargo run --example smoke_test_clob
```

Expected output: `=== SMOKE TEST PASSED ===`

### Step 7: Build & Start

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
cp target/release/facaibot /opt/facaibot/
sudo systemctl enable --now facaibot
```

### Step 8: Verify

```bash
sudo systemctl status facaibot
journalctl -u facaibot -f          # live logs (Ctrl+C to exit)
```

Expect a Telegram startup message and 60s diagnostic logs (`spike 60s`, `engine 60s`) within the first minute.

**Clock sync check** — if the clock drifts, events are silently discarded as stale:

```bash
chronyc tracking
# Reference ID should be 169.254.169.123 (AWS Time Sync)
# System time offset should be < 1ms
```

---

## Updating

After pushing changes to `main`:

```bash
bash deploy/deploy.sh    # pulls, rebuilds, restarts the service
```

---

## Telegram Bot Control

Set `TELEGRAM_ALLOWED_USER_ID` in `.env` to enable remote control — no SSH needed.

| Command | Action |
|---------|--------|
| `/status` | Uptime, mode, market, counters |
| `/config [section]` | Show current config |
| `/set <param> <value>` | Update config + restart (e.g. `/set spike_detection.multiplier 4.0`) |
| `/trades on\|off` | Toggle trade notifications |
| `/summary on\|off` | Toggle market summary notifications |
| `/diag on\|off` | Toggle 60s diagnostic forwarding |
| `/stop` | Pause trading (keeps connections alive for `/balance`, `/status`, etc.) |
| `/resume` | Resume trading after `/stop` |
| `/shutdown` | Graceful shutdown (drains open position first) |
| `/balance` | Wallet USDC.e + POL balance |
| `/polybalance` | Polymarket positions and value |
| `/redeem` | Redeem all resolved positions to USDC.e (merges Data API + persistent file) |
| `/redeem <condition_id>` | Redeem a specific condition ID (30s timeout) |
| `/help` | List all commands |

`/shutdown` and `/set` never abandon open positions — they wait for the current trade to resolve first.

---

## Troubleshooting

**Service fails to start**
```bash
journalctl -u facaibot -n 50 --no-pager
```
- `.env` missing → verify `/opt/facaibot/.env` exists
- Binary missing → verify `/opt/facaibot/facaibot` exists and is executable
- Clock skew → check `chronyc tracking`

**`cargo: command not found`**
```bash
source $HOME/.cargo/env
```

**Git clone fails (Permission denied)**

Use `ssh -A` when connecting (forwards your local GitHub key), or clone via HTTPS with a personal access token:
```bash
git clone https://<TOKEN>@github.com/ranveerbains/FaCaiBot.git
```

**Stale events blocking entries**

Check `stale` in `spike 60s` logs and `rej_stale` in `engine 60s` logs. If non-zero, check clock sync with `chronyc tracking`.

**Permission denied on `/opt/facaibot`**
```bash
sudo chown ec2-user:ec2-user /opt/facaibot/*
sudo chmod 600 /opt/facaibot/.env
```
