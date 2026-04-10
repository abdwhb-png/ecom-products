#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
export FAST_FASHION_HOST="${FAST_FASHION_HOST:-127.0.0.1}"
export FAST_FASHION_PORT="${FAST_FASHION_PORT:-8765}"
exec python3 server.py
