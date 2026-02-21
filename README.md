# FaCaiBot

High-frequency Polymarket arbitrage bot for BTC/ETH 15-minute prediction markets.

## Quick Start

```bash
# Prerequisites: Rust toolchain, Docker

# Start infrastructure
docker-compose up -d

# Configure
cp .env.example .env
# Edit .env with your Polymarket API credentials and wallet private key

# Build and run
cargo run
```

## Architecture

Three-layer lock-free pipeline:

1. **Ingestor** — CPU-pinned WebSocket listener for Polymarket CLOB and Binance spot feeds
2. **Strategy Engine** — Fixed-point arbitrage evaluation comparing CLOB prices to Binance reference
3. **Executor** — EIP-712 order signing, CLOB submission, Redis caching, QuestDB logging

See [CLAUDE.md](CLAUDE.md) for full technical details.
