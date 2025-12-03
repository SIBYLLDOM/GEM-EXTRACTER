#!/usr/bin/env python3
"""
master_gem_extraction.py (updated, robust)

ONE-COMMAND FULL PIPELINE RUNNER for GeM Tender Extraction
 - starts dashboard (if not running)
 - enqueues tasks (producer)
 - spawns consumer workers
 - monitors Redis queue until empty (stable)
 - merges OUTPUT -> FULLDATA
 - status writes are non-fatal (Windows-safe)
"""
import os
import sys
import subprocess
import time
import argparse
import socket
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Try to import helpers; use safe fallbacks if missing
try:
    from utils.redis_helpers import get_redis, QUEUE_NAME
except Exception:
    def get_redis():
        raise RuntimeError("redis_helpers missing")
    QUEUE_NAME = "gem_bid_queue"

# status_helpers functions should be tolerant; we'll wrap them to avoid crashes
try:
    from utils.status_helpers import snapshot_counts as _snapshot_counts, push_recent as _push_recent, set_field as _set_field
except Exception:
    def _snapshot_counts(**kwargs): return False
    def _push_recent(item): return False
    def _set_field(k, v): return False

# safe wrappers: call status helpers but ignore/print any exception (non-fatal)
def safe_set_field(k, v):
    try:
        return _set_field(k, v)
    except Exception as e:
        print("⚠️ status.set_field failed (non-fatal):", e)
        return False

def safe_push_recent(item):
    try:
        return _push_recent(item)
    except Exception as e:
        print("⚠️ status.push_recent failed (non-fatal):", e)
        return False

def safe_snapshot_counts(**kwargs):
    try:
        return _snapshot_counts(**kwargs)
    except Exception as e:
        print("⚠️ status.snapshot_counts failed (non-fatal):", e)
        return False

DASHBOARD_PORT = 8000
DASHBOARD_SCRIPT = str(ROOT / "dashboard" / "app.py")

def is_port_in_use(port: int = DASHBOARD_PORT, host: str = "127.0.0.1") -> bool:
    """Return True if TCP port is in use on host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.4)
        try:
            s.connect((host, port))
            return True
        except Exception:
            return False

def start_dashboard():
    """
    Start the dashboard server if nothing is listening on DASHBOARD_PORT.
    Start it with cwd set to dashboard folder and without piping stdout/stderr.
    Returns subprocess.Popen or None if skipped.
    """
    if is_port_in_use(DASHBOARD_PORT):
        print(f"ℹ️ Dashboard already running on port {DASHBOARD_PORT}, skipping start.")
        return None

    dash_cwd = str(Path(DASHBOARD_SCRIPT).resolve().parent)
    print(f"🚀 Starting Dashboard on http://localhost:{DASHBOARD_PORT} ... (cwd={dash_cwd})")
    proc = subprocess.Popen([sys.executable, DASHBOARD_SCRIPT], cwd=dash_cwd)
    time.sleep(1.2)  # give it a moment to bind
    if not is_port_in_use(DASHBOARD_PORT):
        print("⚠️ Warning: Dashboard did not bind to port quickly. Check logs in the dashboard terminal.")
    return proc

def run_producer(batch: int):
    """Run producer as a blocking subprocess (waits until it finishes)."""
    print(f"📦 Running producer to enqueue up to {batch} tasks...")
    cmd = [sys.executable, "-m", "workers.producer", "--batch", str(batch), "--once", "--log", "info"]
    try:
        res = subprocess.run(cmd, check=False)
        if res.returncode != 0:
            print(f"⚠️ Producer exited with code {res.returncode}")
        else:
            print("✅ Producer finished.")
    except KeyboardInterrupt:
        print("Producer interrupted by user.")
        raise
    except Exception as e:
        print("Producer failed to start:", e)
        raise

def start_consumers(num_workers: int, use_ocr: bool = False):
    """Start consumers and return list of (name, Popen) tuples."""
    procs = []
    print(f"🔧 Starting {num_workers} consumer worker(s)...")
    for i in range(num_workers):
        name = f"worker{i+1}"
        cmd = [sys.executable, "-m", "workers.consumer_redis", "--name", name, "--log", "info"]
        if not use_ocr:
            cmd.append("--no-ocr")
        p = subprocess.Popen(cmd, cwd=str(ROOT))
        procs.append((name, p))
        time.sleep(0.35)  # slight stagger
    return procs

def kill_processes(procs):
    """Gracefully terminate spawned processes."""
    for item in procs:
        name, p = item if isinstance(item, tuple) else (str(item), item)
        try:
            if p and p.poll() is None:
                print(f"⛔ Terminating {name} (pid={p.pid})")
                p.terminate()
                try:
                    p.wait(timeout=6)
                except Exception:
                    p.kill()
        except Exception:
            pass

def monitor_queue_and_wait(poll_interval: float = 3.0, empty_stable_cycles: int = 3):
    """Poll Redis queue until empty and stable for a few cycles."""
    r = get_redis()
    last_len = None
    stable = 0
    print("📡 Monitoring Redis queue (poll interval: {}s)...".format(poll_interval))

    while True:
        try:
            qlen = r.llen(QUEUE_NAME)
        except Exception as e:
            print("❌ Could not read Redis queue length:", e)
            qlen = last_len

        if qlen != last_len:
            print(f"📊 Queue length: {qlen}")
            safe_snapshot_counts(queue_len=qlen)
            last_len = qlen
            stable = 0
        else:
            if qlen == 0:
                stable += 1
                print(f"⏳ Queue empty stable count: {stable}/{empty_stable_cycles}")
            else:
                stable = 0

        if qlen == 0 and stable >= empty_stable_cycles:
            print("🎉 Queue empty and stable - assuming processing complete.")
            time.sleep(1.0)
            return

        time.sleep(poll_interval)

def merge_fulldata():
    print("🧩 Merging OUTPUT -> FULLDATA/data.json ...")
    try:
        from url_pdf_extraction import build_fulldata_json
        out, ok, msg = build_fulldata_json("OUTPUT", "FULLDATA")
        print("➡️ Merge result:", msg)
        safe_push_recent({"status": "merge_done", "message": msg})
    except Exception as e:
        print("❌ Merge failed:", e)
        safe_push_recent({"status": "merge_failed", "message": str(e)})

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4, help="Number of consumer workers")
    parser.add_argument("--batch", type=int, default=500, help="Producer batch size (use 0 to skip producing)")
    parser.add_argument("--scrape", action="store_true", help="Run scraper first (if needed)")
    parser.add_argument("--ocr", action="store_true", help="Enable OCR mode for consumers")
    args = parser.parse_args()

    # non-fatal initial status updates
    safe_set_field("stage", "initializing")
    safe_push_recent({"status": "init", "message": "Master runner started"})

    dash_proc = None
    consumer_procs = []
    try:
        # start dashboard only if not running
        dash_proc = start_dashboard()

        # optional scrape
        if args.scrape:
            print("🕸 Running scraper DataExtraction.py ...")
            subprocess.run([sys.executable, "DataExtraction.py"], cwd=str(ROOT))
            safe_push_recent({"status": "scrape_done", "message": "Scraper finished"})

        # run producer only if batch > 0
        if args.batch and args.batch > 0:
            run_producer(args.batch)
            safe_push_recent({"status": "producer_done", "message": f"{args.batch} tasks enqueued"})
        else:
            print("ℹ️ Skipping producer (batch=0) — assuming existing queue will be processed.")

        # start consumers
        consumer_procs = start_consumers(args.workers, use_ocr=args.ocr)
        try:
            safe_set_field("workers_active", args.workers)
        except Exception:
            pass

        # monitor until queue empty & stable
        monitor_queue_and_wait()

        # stop consumers
        print("🛑 Stopping consumers...")
        kill_processes(consumer_procs)

        # merge outputs
        merge_fulldata()

        # final status updates
        safe_set_field("stage", "done")
        safe_push_recent({"status": "done", "message": "Pipeline completed successfully!"})

        print("\n✅ ALL DONE! Check dashboard → http://localhost:8000/")
        print("➡️ Full data at FULLDATA/data.json")

    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user. Shutting down processes...")
    except Exception as e:
        # do not crash the process on status.json errors — log and continue
        print("❌ Master runner error (non-fatal):", e)
    finally:
        try:
            kill_processes(consumer_procs)
        except Exception:
            pass
        # if we started the dashboard, try to stop it
        try:
            if dash_proc and dash_proc.poll() is None:
                print("⛔ Stopping dashboard process...")
                dash_proc.terminate()
                try:
                    dash_proc.wait(timeout=5)
                except Exception:
                    dash_proc.kill()
        except Exception:
            pass

        print("Master runner exiting.")

if __name__ == "__main__":
    main()
