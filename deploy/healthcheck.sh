#!/bin/bash
# FaCaiBot health check — add to crontab:
#   */5 * * * * /opt/facaibot/healthcheck.sh
#
# Sends a Telegram alert if the facaibot service is down.
# Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from /opt/facaibot/.env

set -euo pipefail

ENV_FILE="/opt/facaibot/.env"

if ! systemctl is-active --quiet facaibot; then
    # Load Telegram creds from .env
    if [[ -f "$ENV_FILE" ]]; then
        TELEGRAM_BOT_TOKEN=$(grep '^TELEGRAM_BOT_TOKEN=' "$ENV_FILE" | cut -d'=' -f2-)
        TELEGRAM_CHAT_ID=$(grep '^TELEGRAM_CHAT_ID=' "$ENV_FILE" | cut -d'=' -f2-)
    fi

    if [[ -n "${TELEGRAM_BOT_TOKEN:-}" && -n "${TELEGRAM_CHAT_ID:-}" ]]; then
        HOSTNAME=$(hostname)
        TIMESTAMP=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
        curl -s -X POST \
            "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
            -d "chat_id=${TELEGRAM_CHAT_ID}" \
            -d "text=CRITICAL: FaCaiBot service is DOWN on ${HOSTNAME} at ${TIMESTAMP}" \
            -d "parse_mode=HTML" \
            >/dev/null 2>&1
    fi

    # Also log locally
    logger -t facaibot-healthcheck "ALERT: facaibot service is not running"
fi
