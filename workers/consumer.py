"""
workers/consumer.py

Consumer worker:
 - pops small messages from Redis (list or stream)
 - atomically claims the corresponding bids row (by bid_number)
 - downloads PDF -> saves to PDF_FOLDER with deterministic name
 - calls parser to extract JSON + structured fields
 - updates DB: mark_done / upsert_final_bid, or mark_failed on error

Usage:
  python workers/consumer.py --mode list --use-ocr --download-timeout 60

Notes:
 - Uses utils.db_helpers for DB operations
 - Uses utils.redis_helpers for queue operations (list mode by default)
 - Uses workers.parser for PDF->JSON extraction (parser must be provided)
"""

import os
import time
import json
import logging
import argparse
import requests
from typing import Optional

from config import PDF_FOLDER, WORKER_ID, DOWNLOAD_TIMEOUT, RETRY_LIMIT
from utils import db_helpers, redis_helpers

LOG = logging.getLogger("consumer")
LOG.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
LOG.addHandler(handler)


# -------------------------
# Helpers
# -------------------------
def safe_filename_for_bid(bid_number: str) -> str:
    """
    Deterministic PDF filename for a bid_number
    """
    name = f"bid_{bid_number}.pdf"
    # ensure folder exists
    os.makedirs(PDF_FOLDER, exist_ok=True)
    return os.path.join(str(PDF_FOLDER), name)


def download_pdf(detail_url: str, dest_path: str, timeout: int = DOWNLOAD_TIMEOUT) -> (bool, str):
    """
    Download a PDF to dest_path.
    Returns (ok: bool, msg: str)
    """
    try:
        # minimal head check to respect redirect/filename; we skip HEAD for simplicity
        LOG.info("Downloading %s -> %s", detail_url, dest_path)
        with requests.get(detail_url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            # try to get filename from content-disposition if present (but we use deterministic name)
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return True, "downloaded"
    except Exception as e:
        LOG.exception("download_pdf failed for %s: %s", detail_url, e)
        return False, str(e)


def attempt_claim_by_bid(bid_number: str, worker_id: str = WORKER_ID) -> Optional[dict]:
    """
    Attempt to atomically claim the given bid_number row.
    Returns the claimed row dict if success, else None.
    """
    conn = None
    # implement atomic claim by updating specific row if it is unprocessed.
    try:
        # Build an UPDATE ... WHERE ... and check affected rows
        # Using a direct connection to perform the update-check-read in one connection
        conn = db_helpers.get_connection()
        with conn.cursor() as cur:
            update_sql = """
            UPDATE bids
            SET status = 'processing',
                locked_by = %s,
                processing_started_ts = NOW(),
                attempts = attempts + 1
            WHERE bid_number = %s AND todayscan = 0 AND status IN ('new','queued','failed')
            """
            cur.execute(update_sql, (worker_id, bid_number))
            conn.commit()
            if cur.rowcount == 0:
                return None
            # fetch the row we just updated
            cur.execute("SELECT * FROM bids WHERE bid_number=%s", (bid_number,))
            row = cur.fetchone()
            return row
    except Exception:
        LOG.exception("attempt_claim_by_bid encountered DB error for %s", bid_number)
        return None
    finally:
        if conn:
            conn.close()


def process_task(bid_number: str, detail_url: str, use_ocr: bool = True, download_timeout: int = DOWNLOAD_TIMEOUT):
    """
    Full processing for a single task.
    """
    LOG.info("Processing task bid=%s url=%s", bid_number, detail_url)

    # 1) Try to claim the row in DB
    claimed = attempt_claim_by_bid(bid_number, WORKER_ID)
    if not claimed:
        LOG.info("Could not claim bid %s (maybe already processing or done). Skipping.", bid_number)
        return

    bid_id = claimed["id"]
    LOG.info("Claimed bid id=%s for processing", bid_id)

    # 2) Download PDF
    pdf_path = safe_filename_for_bid(bid_number)
    ok, msg = download_pdf(detail_url, pdf_path, timeout=download_timeout)
    if not ok:
        LOG.error("Download failed for bid %s : %s", bid_number, msg)
        db_helpers.mark_failed(bid_id, f"download_error:{msg}", max_attempts=RETRY_LIMIT)
        return

    # 3) Call parser to extract JSON and structured fields
    # parser module should provide `extract_pdf_to_json(pdf_path, out_json_path, use_ocr)` or similar
    try:
        # Lazy import the parser to allow module to be developed separately
        from workers import parser as pdf_parser
    except Exception:
        LOG.exception("Parser module not found. Place your parser at workers/parser.py and implement extract_pdf_to_json().")
        db_helpers.mark_failed(bid_id, "parser_missing", max_attempts=RETRY_LIMIT)
        return

    # define output json path deterministically
    json_filename = os.path.splitext(os.path.basename(pdf_path))[0] + ".json"
    json_out_path = os.path.join(str(os.path.abspath(os.path.join(PDF_FOLDER, "..", "OUTPUT"))), json_filename)
    os.makedirs(os.path.dirname(json_out_path), exist_ok=True)

    try:
        # parser should return a dict with keys: ok (bool), json_path, structured (dict), confidence (float)
        parser_res = pdf_parser.extract_pdf_to_json(pdf_path, json_out_path, use_ocr_if_needed=use_ocr)
    except Exception as e:
        LOG.exception("Parser extraction raised exception for %s: %s", pdf_path, e)
        db_helpers.mark_failed(bid_id, f"parser_exception:{e}", max_attempts=RETRY_LIMIT)
        return

    if not isinstance(parser_res, dict):
        LOG.error("Parser returned unexpected result for %s: %s", pdf_path, parser_res)
        db_helpers.mark_failed(bid_id, "parser_bad_result", max_attempts=RETRY_LIMIT)
        return

    ok = parser_res.get("ok", False)
    json_saved = parser_res.get("json_path") or json_out_path
    structured = parser_res.get("structured") or parser_res.get("fields") or {}
    confidence = parser_res.get("confidence", None)

    if not ok:
        LOG.error("Parser reported failure for %s : %s", pdf_path, parser_res.get("message") or "unknown")
        db_helpers.mark_failed(bid_id, f"parser_failed:{parser_res.get('message')}", max_attempts=RETRY_LIMIT)
        return

    # 4) Persist results to DB
    try:
        # update bids row (mark done)
        db_helpers.mark_done(bid_id, pdf_uri=pdf_path, json_uri=json_saved, parse_confidence=confidence)

        # insert/update final_bids
        db_helpers.upsert_final_bid(bid_id=bid_id, bid_number=bid_number, fields=structured, raw_json_uri=json_saved)

        LOG.info("Processing completed for bid %s (bid_id=%s)", bid_number, bid_id)
    except Exception as e:
        LOG.exception("DB update failed for bid %s : %s", bid_number, e)
        # mark failed so it can be retried
        db_helpers.mark_failed(bid_id, f"db_update_exception:{e}", max_attempts=RETRY_LIMIT)
        return


# -------------------------
# Worker main loop
# -------------------------
def run_worker(list_mode: bool = True, stream_mode: bool = False, stream_group: str = None, stream_consumer: str = None,
               use_ocr: bool = True, download_timeout: int = DOWNLOAD_TIMEOUT, poll_interval: float = 0.5):
    """
    Main worker loop.

    list_mode: use Redis list (RPUSH / BLPOP)
    stream_mode: use Redis Streams (consumer group). If stream_mode True, provide stream_group and stream_consumer.
    If both False, worker will fallback to DB-poll mode (claim_batch).
    """
    LOG.info("Worker starting. WORKER_ID=%s list_mode=%s stream_mode=%s", WORKER_ID, list_mode, stream_mode)

    # compute output folders
    output_folder = os.path.abspath(os.path.join(str(PDF_FOLDER), "..", "OUTPUT"))
    os.makedirs(output_folder, exist_ok=True)

    while True:
        try:
            msg = None
            if list_mode:
                payload = redis_helpers.pop_message_list(timeout=5)
                if not payload:
                    # nothing to do; small sleep
                    time.sleep(poll_interval)
                    continue
                msg = payload
            elif stream_mode:
                # stream consumption: we expect redis_helpers.stream_consume_once to return list of (id, data)
                msgs = redis_helpers.stream_consume_once(group=stream_group, consumer=stream_consumer)
                if not msgs:
                    time.sleep(poll_interval)
                    continue
                # take first
                msg_id, data = msgs[0]
                msg = data
            else:
                # DB-poll fallback: claim a batch and process each
                rows = db_helpers.claim_batch(batch_size=1, worker_id=WORKER_ID)
                if not rows:
                    time.sleep(poll_interval)
                    continue
                row = rows[0]
                # process directly using row info
                bid_number = row["bid_number"]
                detail_url = row.get("detail_url") or ""
                process_task(bid_number, detail_url, use_ocr=use_ocr, download_timeout=download_timeout)
                continue

            # At this point, we have a message dict from Redis
            if not isinstance(msg, dict):
                LOG.warning("Received non-dict message, skipping: %s", msg)
                continue

            bid_number = msg.get("bid_number") or msg.get("Bid Number") or msg.get("bidNo")
            detail_url = msg.get("detail_url") or msg.get("detail_url") or msg.get("Detail URL") or msg.get("details_url") or ""

            if not bid_number or not detail_url:
                LOG.warning("Message missing bid_number or detail_url, skipping: %s", msg)
                continue

            # process the single task
            process_task(bid_number, detail_url, use_ocr=use_ocr, download_timeout=download_timeout)

            # if using stream mode, ack the message (consumer must ack after successful processing)
            if stream_mode and 'msg_id' in locals():
                try:
                    redis_helpers.ack_message_stream(stream_name=None, group=stream_group, msg_id=msg_id)
                except Exception:
                    LOG.exception("Failed to ack stream message %s", msg_id)

        except KeyboardInterrupt:
            LOG.info("Worker interrupted by user; exiting.")
            break
        except Exception as e:
            LOG.exception("Worker loop error: %s", e)
            # sleep briefly before continuing to avoid tight error loop
            time.sleep(2)


# -------------------------
# CLI entrypoint
# -------------------------
def parse_args_and_run():
    p = argparse.ArgumentParser(description="Consumer worker for PDF extraction pipeline")
    p.add_argument("--mode", choices=["list", "stream", "db"], default="list", help="Queue mode: list (RPUSH/BLPOP), stream (Redis streams), or db (DB-poll fallback)")
    p.add_argument("--stream-group", default="pdf_consumers", help="Redis stream consumer group name (if using stream)")
    p.add_argument("--stream-consumer", default=None, help="Redis stream consumer name (default=WORKER_ID)")
    p.add_argument("--no-ocr", action="store_true", help="Disable OCR fallback")
    p.add_argument("--download-timeout", type=int, default=DOWNLOAD_TIMEOUT, help="Timeout seconds per download")
    p.add_argument("--poll-interval", type=float, default=0.5, help="Poll interval seconds when idle")
    args = p.parse_args()

    mode = args.mode
    stream_group = args.stream_group
    stream_consumer = args.stream_consumer or WORKER_ID

    if mode == "stream":
        # ensure group exists
        redis_helpers.ensure_stream_group(stream_name="pdf_stream", group_name=stream_group, mk_stream=True)

    run_worker(list_mode=(mode == "list"), stream_mode=(mode == "stream"),
               stream_group=stream_group, stream_consumer=stream_consumer,
               use_ocr=(not args.no_ocr), download_timeout=args.download_timeout, poll_interval=args.poll_interval)


if __name__ == "__main__":
    parse_args_and_run()
