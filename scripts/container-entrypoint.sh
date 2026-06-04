#!/usr/bin/env sh
set -eu

cd /app

export FAST_FASHION_HOST="${FAST_FASHION_HOST:-0.0.0.0}"
export FAST_FASHION_PORT="${FAST_FASHION_PORT:-8765}"
export FAST_FASHION_DB_PATH="${FAST_FASHION_DB_PATH:-/app/runtime/catalog.db}"

mkdir -p "$(dirname "$FAST_FASHION_DB_PATH")" /app/data/downloads /app/runtime

if [ "$(id -u)" = "0" ]; then
  echo "[entrypoint] Correcting ownership of /app/runtime and /app/data to appuser..."
  chown -R appuser:appuser /app/runtime /app/data /app/public /app/backend

  if [ ! -f "$FAST_FASHION_DB_PATH" ]; then
    echo "[entrypoint] catalog missing -> building at $FAST_FASHION_DB_PATH"
    su -s /bin/sh -c "python3 build_catalog.py" appuser
  else
    echo "[entrypoint] reusing existing catalog at $FAST_FASHION_DB_PATH"
  fi

  echo "[entrypoint] starting server as appuser..."
  exec su -s /bin/sh -c "exec python3 server.py" appuser
else
  if [ ! -f "$FAST_FASHION_DB_PATH" ]; then
    echo "[entrypoint] catalog missing -> building at $FAST_FASHION_DB_PATH"
    python3 build_catalog.py
  else
    echo "[entrypoint] reusing existing catalog at $FAST_FASHION_DB_PATH"
  fi
  exec python3 server.py
fi
