#!/usr/bin/env python3
"""
master_extraction.py

Orchestrates:
 - starts a tiny HTTP server serving ./dashboard (index.html + status.json)
 - runs DataExtraction.run_and_save(...) to produce gem_full_fixed.csv
 - runs url_pdf_extraction.run_pipeline(...), passing status file path so it can update progress
"""
import os
import sys
import time
import json
import logging
import threading
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

# ensure project root on path
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# modules (must exist in project)
import DataExtraction
import url_pdf_extraction

# dashboard config
DASHBOARD_DIR = os.path.join(HERE, "dashboard")
STATUS_JSON = os.path.join(DASHBOARD_DIR, "status.json")
HTTP_PORT = 8000  # you can change this

def init_dashboard():
    os.makedirs(DASHBOARD_DIR, exist_ok=True)
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
    with open(STATUS_JSON, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

def write_status(updates: dict):
    # read-modify-write (best-effort)
    try:
        with open(STATUS_JSON, "r", encoding="utf-8") as f:
            s = json.load(f)
    except Exception:
        s = {}
    log = s.get("log", [])
    msg = updates.get("message")
    if msg:
        log.append(msg)
        log = log[-300:]
        updates["log"] = log
    s.update(updates)
    with open(STATUS_JSON, "w", encoding="utf-8") as f:
        json.dump(s, f, indent=2)

def start_dashboard_server(port=HTTP_PORT):
    # serve the dashboard folder
    os.chdir(DASHBOARD_DIR)
    handler = SimpleHTTPRequestHandler
    httpd = ThreadingHTTPServer(("0.0.0.0", port), handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd, t

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print("MASTER: Initializing dashboard...")
    init_dashboard()

    httpd, thread = start_dashboard_server(HTTP_PORT)
    print(f"MASTER: Dashboard server started -> http://localhost:{HTTP_PORT}/")
    write_status({"stage": "starting", "message": "Dashboard up — starting pipeline"})

    try:
        # 1) Run scraper (DataExtraction) and pass status path to capture page-level logs
        write_status({"stage": "scraping", "message": "Running scraper (DataExtraction)...", "scraped_records": 0})
        csv_path, total_records, total_pages = DataExtraction.run_and_save(output_csv="gem_full_fixed.csv", headless=True, status_path=STATUS_JSON)
        write_status({
            "stage": "scraping_done",
            "message": f"Scraping done. Saved {csv_path}",
            "scraped_records": total_records,
            "scraped_pages": total_pages
        })

        # 2) Run url_pdf_extraction pipeline; pass status path so it can update progress
        write_status({"stage": "downloading", "message": "Starting downloads...", "download_total": 0, "download_done": 0})
        res = url_pdf_extraction.run_pipeline(
            csv_path=csv_path,
            pdf_folder="PDF",
            out_folder="OUTPUT",
            fulldata_folder="FULLDATA",
            download_workers=8,
            download_timeout=60,
            extract_workers=None,
            no_ocr=False,
            skip_download=False,
            download_only=False,
            log_level="info",
            status_path=STATUS_JSON  # <-- pass status file so module can update
        )

        # final status
        if res.get("status") == "done":
            write_status({"stage": "done", "message": "Pipeline completed successfully", "full_data_path": res.get("full_data_path")})
            print("MASTER: Pipeline completed successfully.")
        else:
            write_status({"stage": "error", "message": f"Pipeline finished with status: {res.get('status')}"})
            print("MASTER: Pipeline finished with non-done status:", res.get("status"))

    except Exception as e:
        logging.exception("MASTER: Pipeline failed: %s", e)
        write_status({"stage": "error", "message": f"Exception: {e}", "errors": [str(e)]})
    finally:
        # keep server running for a short while so user can view final page
        print(f"MASTER: Dashboard available at http://localhost:{HTTP_PORT}/ — server will keep running while this process is alive.")
        try:
            # wait until interrupted
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("MASTER: Shutting down dashboard server...")
            httpd.shutdown()

if __name__ == "__main__":
    main()
