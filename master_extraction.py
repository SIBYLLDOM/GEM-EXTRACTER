#!/usr/bin/env python3
"""
master_extraction.py (updated)

Orchestrates:
 - starts a tiny HTTP server serving ./dashboard (index.html + status.json)
 - optionally starts N consumer worker processes (workers/consumer.py)
 - runs DataExtraction.run_and_save(...) to produce gem_full_fixed.csv and enqueue rows
 - keeps dashboard server and workers running until interrupted
"""

import os
import sys
import time
import json
import logging
import argparse
import threading
import subprocess
import signal
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

# ensure project root on path
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# imports from project
try:
    import DataExtraction
except Exception:
    DataExtraction = None

from config import WORKER_ID

# Dashboard config
DASHBOARD_DIR = os.path.join(HERE, "dashboard")
STATUS_JSON = os.path.join(DASHBOARD_DIR, "status.json")
HTTP_PORT = 8000  # default; can be overridden via CLI


LOG = logging.getLogger("master")
LOG.setLevel(logging.INFO)
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    LOG.addHandler(h)


def init_dashboard(status_path: str):
    os.makedirs(os.path.dirname(status_path), exist_ok=True)
    # initial status (include log array)
    status = {
        "stage": "idle",            # idle, scraping, downloading, extracting, done, error
        "message": "Waiting to start",
        "scraped_records": 0,
        "scraped_pages": 0,
        "download_total": 0,
        "download_done": 0,
        "extract_total": 0,
        "extract_done": 0,
        "errors": [],
        "log": []
    }
    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)


def write_status(status_path: str, updates: dict):
    # read-modify-write (best-effort)
    try:
        s = {}
        if os.path.exists(status_path):
            try:
                with open(status_path, "r", encoding="utf-8") as f:
                    s = json.load(f)
            except Exception:
                s = {}
        log = s.get("log", [])
        msg = updates.get("message")
        if msg:
            # include a timestamp on message for timeline
            tmsg = f"{time.strftime('%H:%M:%S')} - {msg}"
            log.append(tmsg)
            log = log[-500:]
            updates["log"] = log
        s.update(updates)
        with open(status_path, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=2)
    except Exception:
        # best-effort only
        pass


def start_dashboard_server(port: int = HTTP_PORT, serve_dir: str = DASHBOARD_DIR):
    """
    Start a simple HTTP server to serve the dashboard folder.
    Returns (httpd_instance, thread)
    """
    # ensure directory exists
    os.makedirs(serve_dir, exist_ok=True)
    # create initial status.json if missing
    if not os.path.exists(os.path.join(serve_dir, "status.json")):
        init_dashboard(os.path.join(serve_dir, "status.json"))

    # change directory for the server thread only
    cwd = os.getcwd()
    os.chdir(serve_dir)
    handler = SimpleHTTPRequestHandler
    httpd = ThreadingHTTPServer(("0.0.0.0", port), handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    LOG.info("Dashboard server started -> http://localhost:%s/ (serving %s)", port, serve_dir)
    # restore working dir
    os.chdir(cwd)
    return httpd, t


def spawn_workers(num_workers: int, extra_args: list = None):
    """
    Spawn multiple worker subprocesses (python workers/consumer.py).
    Returns list of subprocess.Popen objects.
    """
    procs = []
    extra_args = extra_args or []
    for i in range(num_workers):
        # create a distinct consumer name for each worker
        worker_name = f"{WORKER_ID}-w{i+1}"
        cmd = [sys.executable, os.path.join(HERE, "workers", "consumer.py"),
               "--mode", "list",
               "--stream-group", "pdf_consumers",
               "--stream-consumer", worker_name,
               "--download-timeout", "60"]
        # add any extra_args passed from CLI
        cmd.extend(extra_args)
        LOG.info("Starting worker: %s", " ".join(cmd))
        # start process (stdout/stderr inherited)
        p = subprocess.Popen(cmd)
        procs.append(p)
        # small stagger to avoid thundering startup
        time.sleep(0.2)
    return procs


def terminate_procs(procs):
    for p in procs:
        try:
            LOG.info("Terminating pid=%s", getattr(p, "pid", None))
            p.terminate()
        except Exception:
            pass
    # give them a moment to exit gracefully
    time.sleep(1.0)
    # force kill if still alive
    for p in procs:
        if p.poll() is None:
            try:
                LOG.info("Killing pid=%s", getattr(p, "pid", None))
                p.kill()
            except Exception:
                pass


def main(args):
    # Ensure dashboard exists and status.json initialized
    init_dashboard(args.status_path)

    # Start dashboard HTTP server
    httpd, server_thread = start_dashboard_server(port=args.port, serve_dir=os.path.dirname(args.status_path))

    # optionally spawn worker processes
    workers = []
    if args.workers and args.workers > 0:
        workers = spawn_workers(args.workers, extra_args=args.worker_extra_args or [])
        write_status(args.status_path, {"message": f"Started {len(workers)} worker(s)", "stage": "workers_started"})

    # Run scraper (DataExtraction) if module present
    if DataExtraction is None:
        LOG.error("DataExtraction module not importable. Make sure DataExtraction.py exists and is on PYTHONPATH.")
        write_status(args.status_path, {"stage": "error", "message": "DataExtraction missing"})
        # keep servers/workers alive so you can debug
    else:
        try:
            write_status(args.status_path, {"stage": "scraping", "message": "Starting scraper (DataExtraction)...", "scraped_records": 0})
            # run scraper; it will enqueue bids as it scrapes
            csv_path, total_records, total_pages = DataExtraction.run_and_save(
                output_csv="gem_full_fixed.csv",
                headless=args.headless,
                status_path=args.status_path,
                enqueue=not args.no_enqueue,
                save_csv=not args.no_csv
            )
            LOG.info("Scraping finished: csv=%s records=%s pages=%s", csv_path, total_records, total_pages)
            write_status(args.status_path, {"stage": "scraping_done", "message": f"Scraping finished. {total_records} records", "scraped_records": total_records, "scraped_pages": total_pages})
        except Exception as e:
            LOG.exception("Scraper failed: %s", e)
            write_status(args.status_path, {"stage": "error", "message": f"Scraper exception: {e}", "errors": [str(e)]})

    # Now pipeline is running with workers; wait until interrupted
    try:
        write_status(args.status_path, {"stage": "running", "message": "Pipeline running. Press Ctrl+C to stop."})
        LOG.info("Pipeline running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
            # could add health checks here (e.g., check workers' p.poll or status.json)
    except KeyboardInterrupt:
        LOG.info("Interrupted by user; shutting down...")
    finally:
        # terminate worker subprocesses
        if workers:
            write_status(args.status_path, {"message": "Shutting down workers...", "stage": "stopping"})
            terminate_procs(workers)
        # shutdown http server
        try:
            httpd.shutdown()
            LOG.info("Dashboard server shut down.")
        except Exception:
            pass
        write_status(args.status_path, {"stage": "stopped", "message": "Master stopped"})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master orchestrator: dashboard + optional workers + scrape-and-enqueue")
    parser.add_argument("--workers", type=int, default=0, help="Number of consumer worker processes to spawn (default 0)")
    parser.add_argument("--port", type=int, default=8000, help="Dashboard HTTP port (default 8000)")
    parser.add_argument("--headless", action="store_true", help="Run Playwright in headless mode for scraping")
    parser.add_argument("--no-enqueue", action="store_true", help="Run scraper but do not enqueue to Redis/DB (CSV-only)")
    parser.add_argument("--no-csv", action="store_true", help="Do not save CSV backup")
    parser.add_argument("--status-path", default=STATUS_JSON, help="Path to status.json for dashboard")
    parser.add_argument("--worker-extra-args", nargs="*", help="Extra CLI args to append to each worker process")
    args = parser.parse_args()

    main(args)
