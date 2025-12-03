#!/usr/bin/env python3
"""
workers/consumer_redis.py

Redis consumer (BRPOP loop):
 - BRPOP a JSON task from Redis queue (task contains id, bid_number, detail_url, page)
 - Download the PDF to PDF/<bid_number>.pdf
 - Run url_pdf_extraction.process_single_pdf_file_deep(pdf_path, json_out, use_ocr_if_needed)
 - Update DB via utils.db_helpers.mark_done / mark_error

Run as:
  python -m workers.consumer_redis --name worker1 --no-ocr --log info
"""
import os
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from typing import Optional

# make sure project root is on path when running as module
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# --- imports from your utils ---
from utils.redis_helpers import get_redis, QUEUE_NAME
# prefer these three from db_helpers; if increment_attempts_for_bid is missing we'll provide a fallback
from utils.db_helpers import mark_done, mark_error, get_db_conn

try:
    from utils.db_helpers import increment_attempts_for_bid
    _HAS_INCREMENT = True
except Exception:
    increment_attempts_for_bid = None
    _HAS_INCREMENT = False

# extraction module (existing in your project)
import url_pdf_extraction as extractor
import requests

# dashboard/status helpers (optional)
try:
    from utils.status_helpers import increment, push_recent, snapshot_counts, read_status
except Exception:
    def increment(k, d=1): pass
    def push_recent(item, max_items=50): pass
    def snapshot_counts(**kwargs): pass
    def read_status(): return {}

# folders
PDF_FOLDER = Path("PDF")
OUT_FOLDER = Path("OUTPUT")
PDF_FOLDER.mkdir(parents=True, exist_ok=True)
OUT_FOLDER.mkdir(parents=True, exist_ok=True)

# downloader
def download_pdf_stream(url: str, dest: Path, timeout: int = 60, max_retries: int = 2) -> Optional[Path]:
    session = requests.Session()
    session.headers.update({"User-Agent": "gem-pdf-downloader/1.0"})
    for attempt in range(max_retries + 1):
        try:
            with session.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            return dest
        except Exception as e:
            logging.warning("Download attempt %d failed for %s: %s", attempt, url, e)
            time.sleep(0.5)
            continue
    return None

# fallback increment attempts if not provided by db_helpers
def _increment_attempts_fallback(bid_number: str) -> int:
    """
    Try to increment attempts in DB directly; returns new attempts value or -1.
    """
    if not bid_number:
        return -1
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute("UPDATE bids SET attempts = COALESCE(attempts, 0) + 1 WHERE bid_number = %s", (bid_number,))
            cur.execute("SELECT attempts FROM bids WHERE bid_number = %s", (bid_number,))
            row = cur.fetchone()
            conn.commit()
            if row:
                if isinstance(row, dict):
                    return int(row.get("attempts") or -1)
                elif isinstance(row, (list, tuple)):
                    return int(row[0] or -1)
                else:
                    return int(row or -1)
            return -1
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return -1
    finally:
        try:
            conn.close()
        except Exception:
            pass

def safe_increment_attempts(bid_number: str) -> int:
    if _HAS_INCREMENT and callable(increment_attempts_for_bid):
        try:
            return increment_attempts_for_bid(bid_number)
        except Exception:
            logging.exception("increment_attempts_for_bid raised — falling back")
            return _increment_attempts_fallback(bid_number)
    else:
        return _increment_attempts_fallback(bid_number)

def _snapshot_queue_and_workers(r):
    """Helper to snapshot redis queue length and (approx) workers_active to status.json"""
    try:
        qlen = None
        workers_active = None
        try:
            qlen = r.llen(QUEUE_NAME)
        except Exception:
            qlen = None
        # simple heuristic: we don't have global worker list here, so read current status and preserve workers_active
        s = read_status() or {}
        workers_active = s.get("workers_active", None)
        snapshot_counts(queue_len=qlen, workers_active=workers_active)
    except Exception:
        pass

def process_task(task: dict, use_ocr: bool, r):
    """
    Process a single task dict: {id, bid_number, detail_url, page}
    Returns True if succeeded, False otherwise.
    """
    bid = str(task.get("bid_number") or task.get("Bid Number") or "")
    db_id = task.get("id") or task.get("db_id") or None
    url = task.get("detail_url") or task.get("Detail URL") or task.get("url")

    if not bid or not url:
        logging.error("Invalid task, missing bid or url: %s", task)
        return False

    # safe filenames
    safe_base = bid.replace("/", "_").replace("\\", "_")
    pdf_path = PDF_FOLDER / f"{safe_base}.pdf"
    json_out = OUT_FOLDER / f"{safe_base}.json"

    # 1) download
    dl = download_pdf_stream(url, pdf_path)
    if not dl:
        logging.error("Download failed for %s", bid)
        try:
            safe_increment_attempts(bid)
        except Exception:
            logging.debug("safe_increment_attempts failed for %s", bid)
        # mark error in DB (best-effort)
        try:
            mark_error(bid, err_msg="download_failed", increment_attempts=True, release_lock=False)
        except Exception:
            logging.debug("mark_error call failed (signature mismatch?) for %s", bid)
        return False

    # 2) extract (deep)
    try:
        # extractor.process_single_pdf_file_deep should return ok flag and paths; adapt if your signature differs
        result_pdf, outp, ok, msg = extractor.process_single_pdf_file_deep(str(pdf_path), str(json_out), use_ocr_if_needed=use_ocr)
        if not ok:
            logging.error("Extraction failed for %s: %s", bid, msg)
            try:
                safe_increment_attempts(bid)
            except Exception:
                logging.debug("safe_increment_attempts failed for %s", bid)
            try:
                mark_error(bid, err_msg=f"extract_failed:{msg}", increment_attempts=True, release_lock=False)
            except Exception:
                logging.debug("mark_error call failed for %s", bid)
            return False

        # 3) mark done in DB
        try:
            # Try the common mark_done signature: (bid_number, ...)
            try:
                mark_done(bid, pdf_path=str(pdf_path), json_path=str(json_out), parse_confidence=None)
            except TypeError:
                # fallback: maybe expects id first
                mark_done(db_id or bid, pdf_path=str(pdf_path), json_path=str(json_out), parse_confidence=None)
        except Exception:
            logging.exception("mark_done failed for %s", bid)
            try:
                mark_error(bid, err_msg="mark_done_failed", increment_attempts=True, release_lock=False)
            except Exception:
                logging.debug("mark_error call failed in mark_done exception for %s", bid)
            return False

        logging.info("Successfully processed bid %s -> %s", bid, json_out)
        return True
    except Exception as e:
        logging.exception("Unexpected exception processing %s : %s", bid, e)
        try:
            safe_increment_attempts(bid)
        except Exception:
            logging.debug("safe_increment_attempts failed for %s", bid)
        try:
            mark_error(bid, err_msg=str(e), increment_attempts=True, release_lock=False)
        except Exception:
            logging.debug("mark_error call failed for %s", bid)
        return False

def consumer_loop(worker_name: str, use_ocr: bool, redis_block_timeout: int = 5):
    r = get_redis()
    logging.info("Consumer '%s' started. Listening on Redis queue '%s' ...", worker_name, QUEUE_NAME)
    while True:
        try:
            item = r.brpop(QUEUE_NAME, timeout=redis_block_timeout)
            if not item:
                # timeout -> continue loop
                continue
            # item is (queue_name, payload)
            payload = item[1] if isinstance(item, (list, tuple)) else item
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            try:
                task = json.loads(payload)
            except Exception:
                logging.exception("Failed to parse task payload: %s", payload)
                continue

            bid_display = task.get("bid_number") or task.get("id") or "unknown"
            logging.info("Claimed task: id=%s bid=%s", task.get("id"), bid_display)

            # update dashboard: increment in_progress and push recent event
            try:
                increment('in_progress', 1)
                push_recent({'bid_number': bid_display, 'status': 'started', 'ts': None, 'message': f'started by {worker_name}'})
                # snapshot queue/workers
                _snapshot_queue_and_workers(r)
            except Exception:
                logging.debug("status_helpers update failed on start (non-fatal)")

            success = False
            try:
                success = process_task(task, use_ocr=use_ocr, r=r)
            except Exception:
                logging.exception("process_task raised unexpected exception for %s", bid_display)
                success = False

            # finalize dashboard updates based on result
            try:
                if success:
                    increment('processed', 1)
                    increment('done', 1)
                    push_recent({'bid_number': bid_display, 'status': 'done', 'ts': None, 'json_path': str(OUT_FOLDER / f"{bid_display}.json")})
                else:
                    increment('processed', 1)
                    increment('failed', 1)
                    push_recent({'bid_number': bid_display, 'status': 'failed', 'ts': None, 'message': 'task failed'})
                # decrement in_progress
                try:
                    increment('in_progress', -1)
                except Exception:
                    # Some status helper implementations may not support negative increments; instead snapshot
                    snapshot_counts()  # best-effort refresh
                # snapshot queue length / workers
                _snapshot_queue_and_workers(r)
            except Exception:
                logging.debug("status_helpers final update failed (non-fatal)")

            # loop continues; BRPOP already removed the task from queue (atomic)
        except KeyboardInterrupt:
            logging.info("Consumer '%s' interrupted by user. Exiting.", worker_name)
            break
        except Exception:
            logging.exception("Unexpected error in consumer loop; sleeping briefly.")
            time.sleep(1.0)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--name", type=str, default=None, help="Worker name (for logs)")
    p.add_argument("--no-ocr", action="store_true", help="Disable OCR (faster)")
    p.add_argument("--log", default="info", choices=["debug","info","warning","error"])
    p.add_argument("--timeout", type=int, default=5, help="BRPOP timeout (seconds)")
    return p.parse_args()

def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    worker_name = args.name or f"redis_worker_{int(time.time())}"
    consumer_loop(worker_name, use_ocr=(not args.no_ocr), redis_block_timeout=args.timeout)

if __name__ == "__main__":
    main()
