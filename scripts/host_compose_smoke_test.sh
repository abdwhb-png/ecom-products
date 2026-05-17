#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RUNTIME_DIR="$ROOT/.tmp-compose-runtime"
DB_PATH="$RUNTIME_DIR/catalog.db"
PORT="${FAST_FASHION_TEST_PORT:-8877}"
TOKEN="$(grep '^FAST_FASHION_API_TOKEN=' .env | cut -d= -f2-)"

rm -rf "$RUNTIME_DIR"
mkdir -p "$RUNTIME_DIR" "$ROOT/data/downloads"

export FAST_FASHION_HOST=0.0.0.0
export FAST_FASHION_PORT="$PORT"
export FAST_FASHION_DB_PATH="$DB_PATH"

if [ ! -f "$DB_PATH" ]; then
  echo "[host-smoke] catalog missing -> building at $DB_PATH"
  python3 build_catalog.py >/tmp/ffd-host-smoke-build.log 2>&1
fi

python3 server.py >/tmp/ffd-host-smoke-server.log 2>&1 &
PID=$!
cleanup() {
  kill "$PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$PORT/healthz" >/tmp/ffd-host-healthz.json 2>/dev/null; then
    curl -fsS -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$PORT/api/datasets" >/tmp/ffd-host-smoke-api.json
    python3 - <<'PY'
import json
from pathlib import Path
health=json.loads(Path('/tmp/ffd-host-healthz.json').read_text())
payload=json.loads(Path('/tmp/ffd-host-smoke-api.json').read_text())
print(json.dumps({
  'healthz_ok': health.get('data', {}).get('ok'),
  'datasets_count': len(payload.get('datasets', [])),
  'dataset_ids': [d.get('id') for d in payload.get('datasets', [])]
}, ensure_ascii=False))
PY
    echo "HTTP_STATUS=200"
    ls -lh "$DB_PATH"
    exit 0
  fi
  sleep 2
done

echo "API/healthz did not become ready in time" >&2
exit 1
