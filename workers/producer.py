#!/usr/bin/env python3
"""
workers/producer.py

Read rows from MySQL (bids where todayscan = 0) in batches,
mark them as queued (todayscan = 3, status='queued'), and push tasks
to Redis queue (using utils.redis_helpers.enqueue_batch).

Usage (examples):
  # Run once and enqueue everything (batch 500)
  python workers/producer.py --batch 500 --once

  # Run repeatedly (sleep 10s), processing up to N rows each loop
  python workers/producer.py --batch 500 --sleep 10

Notes:
 - This script marks rows as queued BEFORE pushing to Redis, and will try to revert
   the rows back to 'new' if Redis push fails for any reason.
 - Task format pushed to Redis: {"id": <db id>, "bid_number": "...", "detail_url": "...", "page": ...}
"""
import time
import json
import argparse
import logging
from typing import List, Dict, Any, Tuple

from utils.redis_helpers import get_redis, enqueue_batch, QUEUE_NAME
from utils.db_helpers import get_db_conn

# status dashboard helpers
try:
    from utils.status_helpers import increment, push_recent, snapshot_counts
except Exception:
    # define no-op fallbacks if helpers missing to avoid crashing producer
    def increment(key, delta=1): pass
    def push_recent(item, max_items=50): pass
    def snapshot_counts(**kwargs): pass

# DB marker value for "queued" (we use 3)
QUEUED_TODAYSCAN = 3

def fetch_and_mark_batch(conn, batch_size: int) -> List[Dict[str, Any]]:
    """
    Atomically select up to batch_size rows with todayscan=0 and mark them queued.
    Returns list of selected rows (id, bid_number, detail_url, page).
    Implementation:
     - Start transaction
     - SELECT id, bid_number, detail_url, page FROM bids WHERE todayscan = 0 LIMIT %s FOR UPDATE
     - UPDATE those ids set todayscan = QUEUED_TODAYSCAN, status='queued'
     - commit
    """
    rows = []
    ids = []
    with conn.cursor() as cur:
        # select rows for update to claim them
        cur.execute("SELECT id, bid_number, detail_url, page FROM bids WHERE todayscan = 0 LIMIT %s FOR UPDATE", (batch_size,))
        rows = cur.fetchall()
        if not rows:
            conn.rollback()
            return []
        ids = [r["id"] for r in rows]
        # mark them queued
        # build placeholders and params safely
        placeholders = ",".join(["%s"] * len(ids))
        params = [QUEUED_TODAYSCAN, "queued"] + ids
        cur.execute(
            f"UPDATE bids SET todayscan = %s, status = %s WHERE id IN ({placeholders})",
            tuple(params)
        )
        conn.commit()
    return rows

def push_tasks_to_redis(rows: List[Dict[str, Any]]) -> bool:
    """
    Push tasks to Redis as JSON strings (LPUSH).
    Uses enqueue_batch from redis_helpers for efficiency.
    Returns True if succeeded.
    """
    if not rows:
        return True
    tasks = []
    for r in rows:
        t = {"id": r.get("id"), "bid_number": r.get("bid_number"), "detail_url": r.get("detail_url"), "page": r.get("page")}
        tasks.append(t)
    try:
        # use redis_helpers.enqueue_batch for bulk push
        enqueue_batch(tasks)
        return True
    except Exception as e:
        logging.exception("Failed to push tasks to Redis: %s", e)
        return False

def revert_rows_to_new(conn, ids: List[int]):
    """If pushing to Redis failed, revert rows back to todayscan=0 and status='new'"""
    if not ids:
        return
    with conn.cursor() as cur:
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(f"UPDATE bids SET todayscan = 0, status = 'new' WHERE id IN ({placeholders})", tuple(ids))
        conn.commit()

def run_loop(batch_size:int=500, sleep_seconds:float=10.0, once:bool=False):
    logging.info("Producer starting: batch_size=%s sleep=%s once=%s", batch_size, sleep_seconds, once)
    while True:
        conn = None
        try:
            conn = get_db_conn()
            rows = fetch_and_mark_batch(conn, batch_size)
            if not rows:
                logging.debug("No new rows to enqueue.")
            else:
                ids = [r["id"] for r in rows]
                ok = push_tasks_to_redis(rows)
                if ok:
                    logging.info("Enqueued %d tasks (ids: %s...)", len(ids), ids[:6])
                    # update dashboard status safely (non-blocking)
                    try:
                        increment('enqueued', len(ids))
                        push_recent({
                            'ts': None,
                            'bid_number': 'BATCH',
                            'status': 'enqueued',
                            'message': f'enqueued {len(ids)} tasks (ids: {ids[:6]})'
                        })
                        # optionally snapshot enqueued count (if you want a single authoritative value),
                        # here we set enqueued to incremented value by reading status.json inside snapshot_counts,
                        # but snapshot_counts can accept enqueued directly if you compute it externally.
                        snapshot_counts(enqueued=None, message=f'producer enqueued {len(ids)}')
                    except Exception:
                        logging.debug("status_helpers update failed (non-fatal).")
                else:
                    logging.warning("Push to Redis failed — reverting %d rows to new", len(ids))
                    revert_rows_to_new(conn, ids)
            try:
                conn.close()
            except Exception:
                pass
        except Exception as e:
            logging.exception("Producer loop error: %s", e)
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

        if once:
            break
        time.sleep(sleep_seconds)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--batch", type=int, default=500, help="Number of rows to mark+enqueue per loop")
    p.add_argument("--sleep", type=float, default=10.0, help="Sleep seconds between loops")
    p.add_argument("--once", action="store_true", help="Run a single iteration then exit")
    p.add_argument("--log", default="info", choices=["debug","info","warning","error"])
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    run_loop(batch_size=args.batch, sleep_seconds=args.sleep, once=args.once)
