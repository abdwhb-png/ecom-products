#!/usr/bin/env python3
import concurrent.futures
import csv
import random
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / 'catalog.db'
TIMEOUT = 8
USER_AGENT = 'Mozilla/5.0'


def url_ok(url: str):
    if not url:
        return False, None, 'empty'
    try:
        req = urllib.request.Request(url, method='HEAD', headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return 200 <= resp.status < 400, resp.status, resp.headers.get_content_type()
    except Exception:
        try:
            req = urllib.request.Request(url, method='GET', headers={'User-Agent': USER_AGENT})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return 200 <= resp.status < 400, resp.status, resp.headers.get_content_type()
        except Exception as exc:
            return False, None, exc.__class__.__name__


def ensure_table(conn):
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS image_status (
            dataset_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            image_url TEXT,
            ok INTEGER NOT NULL,
            status_code INTEGER,
            content_type TEXT,
            checked_at REAL NOT NULL,
            PRIMARY KEY (dataset_id, product_id)
        )
        '''
    )
    conn.execute('CREATE INDEX IF NOT EXISTS idx_image_status_dataset_ok ON image_status(dataset_id, ok)')
    conn.commit()


def sample_products(conn, dataset_id: str, sample_size: int):
    rows = conn.execute(
        'SELECT dataset_id, id, image FROM products WHERE dataset_id = ? AND image <> ""',
        (dataset_id,),
    ).fetchall()
    if not rows:
        return []
    if sample_size >= len(rows):
        return rows
    random.seed(42)
    return random.sample(rows, sample_size)


def update_statuses(conn, dataset_id: str, limit: int):
    rows = conn.execute(
        '''
        SELECT p.dataset_id, p.id, p.image
        FROM products p
        LEFT JOIN image_status s ON s.dataset_id = p.dataset_id AND s.product_id = p.id
        WHERE p.dataset_id = ? AND p.image <> '' AND s.product_id IS NULL
        LIMIT ?
        ''',
        (dataset_id, limit),
    ).fetchall()
    if not rows:
        return 0

    def worker(row):
        ok, status, content_type = url_ok(row[2])
        return (row[0], row[1], row[2], int(ok), status, content_type, time.time())

    payload = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for item in executor.map(worker, rows):
            payload.append(item)

    conn.executemany(
        '''
        INSERT OR REPLACE INTO image_status (dataset_id, product_id, image_url, ok, status_code, content_type, checked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        payload,
    )
    conn.commit()
    return len(payload)


def main():
    dataset_id = sys.argv[1] if len(sys.argv) > 1 else 'sheinKaggle'
    sample_size = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    warm_limit = int(sys.argv[3]) if len(sys.argv) > 3 else 1500

    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)

    checked = update_statuses(conn, dataset_id, warm_limit)
    print(f'warmed_image_checks={checked}')

    sampled = sample_products(conn, dataset_id, sample_size)
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for result in executor.map(lambda row: (row[0], row[1], row[2], *url_ok(row[2])), sampled):
            results.append(result)

    ok = sum(1 for *_rest, is_ok, _status, _ctype in results if is_ok)
    total = len(results)
    pct = (ok / total * 100) if total else 0
    print(f'sample_total={total}')
    print(f'sample_ok={ok}')
    print(f'sample_pct={pct:.2f}')

    failures = [r for r in results if not r[3]]
    print('sample_failures_preview=')
    for row in failures[:10]:
        print({'product_id': row[1], 'image': row[2][:140], 'status': row[4], 'detail': row[5]})

    db_ok, db_total = conn.execute(
        'SELECT COALESCE(SUM(ok),0), COUNT(*) FROM image_status WHERE dataset_id = ?',
        (dataset_id,),
    ).fetchone()
    print(f'db_checked_total={db_total}')
    print(f'db_checked_ok={db_ok}')
    print(f'db_checked_pct={(db_ok / db_total * 100) if db_total else 0:.2f}')
    conn.close()


if __name__ == '__main__':
    main()
