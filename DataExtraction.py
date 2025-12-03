import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import time
import re
import os
import json
import logging
from typing import Optional, List, Dict, Any

from config import WORKER_ID

BASE_URL = "https://bidplus.gem.gov.in"

# When None -> no page limit (scrape all pages). Set to an int for testing.
MAX_PAGES = None

# Configure logging for this module
LOG = logging.getLogger("DataExtraction")
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    LOG.addHandler(h)
LOG.setLevel(logging.INFO)

# Try to import producer.enqueue_bid; if unavailable, we'll fall back to CSV-only mode.
try:
    from workers.producer import enqueue_bid
    _HAS_PRODUCER = True
    LOG.info("Producer available: will enqueue bids to DB/Redis.")
except Exception:
    enqueue_bid = None
    _HAS_PRODUCER = False
    LOG.warning("workers.producer not available. Running scraper in CSV-only mode.")


# --- status helper (best-effort, same format used by older pipeline) ---
def write_status_file(status_path: Optional[str], updates: dict):
    if not status_path:
        return
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
            # include a timestamp in the message for timeline readability
            tmsg = f"{time.strftime('%H:%M:%S')} - {msg}"
            log.append(tmsg)
            log = log[-200:]
            updates["log"] = log

        s.update(updates)

        with open(status_path, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=2)
    except Exception:
        pass


async def apply_sorting(page, status_path=None):
    write_status_file(status_path, {"message": "Applying sort: Bid Start Date → Latest First", "stage": "scraping"})
    dropdown_btn = await page.query_selector("#currentSort")
    if dropdown_btn:
        await dropdown_btn.click()
        await asyncio.sleep(1)

    sort_option = await page.query_selector("#Bid-Start-Date-Latest")
    if not sort_option:
        write_status_file(status_path, {"message": "Sort option not found!", "stage": "scraping"})
    else:
        await sort_option.click()
        await asyncio.sleep(2)
        write_status_file(status_path, {"message": "Sorting applied", "stage": "scraping"})


async def extract_total_counts(page, status_path=None):
    await page.goto(f"{BASE_URL}/all-bids", timeout=0, wait_until="networkidle")
    await asyncio.sleep(2)

    await apply_sorting(page, status_path=status_path)

    # extract record count
    records_el = await page.query_selector("span.pos-bottom")
    total_records = 0
    if records_el:
        try:
            text = await records_el.inner_text()
            m = re.search(r"of\s+(\d+)\s+records", text)
            if m:
                total_records = int(m.group(1))
        except:
            pass

    # extract total pages
    last_page_el = await page.query_selector("#light-pagination a.page-link:nth-last-child(2)")
    total_pages = 1
    if last_page_el:
        try:
            t = (await last_page_el.inner_text()).strip()
            if t.isdigit():
                total_pages = int(t)
        except:
            pass

    write_status_file(status_path, {"message": f"Site reports {total_records} records across {total_pages} pages", "stage": "scraping"})
    return total_records, total_pages


async def _enqueue_row_async(row: Dict[str, Any], status_path: Optional[str] = None):
    """
    Run enqueue_bid in a thread to avoid blocking the event loop.
    """
    if not _HAS_PRODUCER or enqueue_bid is None:
        # Producer not available; nothing to do
        return None
    try:
        # Enqueue in a thread so it doesn't block Playwright loop
        res = await asyncio.to_thread(
            enqueue_bid,
            bid_number=row.get("Bid Number"),
            detail_url=row.get("Detail URL"),
            page=row.get("Page"),
            items=row.get("Items"),
            quantity=row.get("Quantity"),
            department=row.get("Department"),
            start_date=row.get("Start Date"),
            end_date=row.get("End Date"),
            redis_mode="list"
        )
        # update status file lightly
        write_status_file(status_path, {"message": f"Enqueued bid {row.get('Bid Number')} (db_id={res.get('bid_id')})", "stage": "scraping"})
        return res
    except Exception as e:
        LOG.exception("enqueue_bid failed for %s: %s", row.get("Bid Number"), e)
        write_status_file(status_path, {"message": f"Failed to enqueue {row.get('Bid Number')}: {e}", "stage": "scraping"})
        return None


async def scrape_single_page(page, page_no, status_path=None, enqueue=True, collect_list: Optional[List[Dict]] = None):
    write_status_file(status_path, {"message": f"🔵 Scraping PAGE {page_no}", "stage": "scraping", "scraped_pages": page_no})

    # scroll to trigger lazy-load cards
    for _ in range(5):
        await page.mouse.wheel(0, 3000)
        await asyncio.sleep(0.3)

    cards = await page.query_selector_all("div.card")
    found = len(cards)

    write_status_file(status_path, {"message": f"   → Found {found} tenders on page {page_no}", "stage": "scraping"})

    results = []

    for c in cards:
        try:
            bid_link = await c.query_selector(".block_header a.bid_no_hover")
            bid_no = (await bid_link.inner_text()) if bid_link else ""

            href = await bid_link.get_attribute("href") if bid_link else None
            detail_url = BASE_URL + "/" + href.lstrip("/") if href else ""

            item_el = await c.query_selector(".card-body .col-md-4 .row:nth-child(1) a")
            items = (await item_el.inner_text()) if item_el else ""

            qty_el = await c.query_selector(".card-body .col-md-4 .row:nth-child(2)")
            quantity = (await qty_el.inner_text()).replace("Quantity:", "").strip() if qty_el else ""

            dept_el = await c.query_selector(".card-body .col-md-5 .row:nth-child(2)")
            department = (await dept_el.inner_text()) if dept_el else ""

            start_el = await c.query_selector("span.start_date")
            start_date = (await start_el.inner_text()) if start_el else ""

            end_el = await c.query_selector("span.end_date")
            end_date = (await end_el.inner_text()) if end_el else ""

            row = {
                "Page": page_no,
                "Bid Number": bid_no,
                "Detail URL": detail_url,
                "Items": items,
                "Quantity": quantity,
                "Department": department,
                "Start Date": start_date,
                "End Date": end_date
            }

            results.append(row)
            if collect_list is not None:
                collect_list.append(row)

            # enqueue immediately (non-blocking via asyncio.to_thread)
            if enqueue:
                # schedule enqueue but don't await synchronously here (fire-and-forget)
                # we still await the enqueuing to capture errors in status file, but if
                # you prefer pure fire-and-forget, remove the await.
                await _enqueue_row_async(row, status_path=status_path)

        except Exception:
            # ignore individual-card errors but log them
            LOG.exception("Error while parsing a card on page %s", page_no)
            write_status_file(status_path, {"message": f"Error parsing card on page {page_no}", "stage": "scraping"})
            continue

    return results


async def scrape_all(headless=False, status_path=None, enqueue=True, save_csv=True):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            channel="chrome",
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context()
        page = await context.new_page()

        total_records, total_pages = await extract_total_counts(page, status_path=status_path)

        # If MAX_PAGES is set (int), limit; otherwise scrape all pages.
        if isinstance(MAX_PAGES, int) and MAX_PAGES > 0:
            total_pages = min(total_pages, MAX_PAGES)
            write_status_file(status_path, {
                "message": f"Starting scraping: LIMIT = first {MAX_PAGES} pages",
                "stage": "scraping"
            })
        else:
            write_status_file(status_path, {
                "message": f"Starting scraping: ALL {total_pages} pages",
                "stage": "scraping"
            })

        all_data: List[Dict[str, Any]] = []

        # scrape page 1
        page_no = 1
        page_results = await scrape_single_page(page, page_no, status_path=status_path, enqueue=enqueue, collect_list=all_data)
        write_status_file(status_path, {"scraped_records": len(all_data), "scraped_pages": page_no})

        # pages 2..total_pages
        while page_no < total_pages:
            next_btn = await page.query_selector("#light-pagination a.next")
            if not next_btn:
                write_status_file(status_path, {"message": "No NEXT button — stopping", "stage": "scraping"})
                break

            page_no += 1
            write_status_file(status_path, {"message": f"➡ Clicking NEXT → Page {page_no}", "stage": "scraping"})
            await next_btn.click()
            await asyncio.sleep(2)

            page_results = await scrape_single_page(page, page_no, status_path=status_path, enqueue=enqueue, collect_list=all_data)
            write_status_file(status_path, {"scraped_records": len(all_data), "scraped_pages": page_no})

        await browser.close()

        write_status_file(status_path, {"message": f"Scraping complete. Total scraped: {len(all_data)}", "stage": "scraped"})

        # optionally save CSV as backup
        if save_csv:
            try:
                out_path = os.path.join(os.getcwd(), "gem_full_fixed.csv")
                pd.DataFrame(all_data).to_csv(out_path, index=False)
                write_status_file(status_path, {"message": f"Saved CSV backup: {out_path}", "stage": "scraped", "scraped_records": len(all_data)})
            except Exception:
                LOG.exception("Failed to save CSV backup")
                write_status_file(status_path, {"message": "Failed to save CSV backup", "stage": "scraped"})

        return all_data, total_records, total_pages


def run_and_save(output_csv="gem_full_fixed.csv", headless=False, status_path=None, enqueue=True, save_csv=True):
    start = time.time()

    lim_text = f"limit {MAX_PAGES} pages" if isinstance(MAX_PAGES, int) and MAX_PAGES > 0 else "no page limit (all pages)"
    write_status_file(status_path, {"stage": "starting", "message": f"Starting scraper ({lim_text})..."})

    data, total_records, total_pages = asyncio.run(scrape_all(headless=headless, status_path=status_path, enqueue=enqueue, save_csv=save_csv))

    out_path = os.path.join(os.getcwd(), output_csv)
    # CSV already saved inside scrape_all if save_csv True; this keeps compatibility
    if save_csv and os.path.exists(out_path):
        write_status_file(status_path, {
            "stage": "scraping_done",
            "message": f"Saved CSV: {out_path}",
            "scraped_records": len(data),
            "scraped_pages": total_pages
        })

    print("\n-------------------------------------")
    print(f"SCRAPED RECORDS: {len(data)}")
    print(f"SITE-REPORTED RECORDS: {total_records}")
    print(f"PAGES SCRAPED: {total_pages}")
    print("-------------------------------------")
    print(f"Time: {round(time.time() - start, 2)} sec")
    if save_csv:
        print(f"Saved: {out_path}")

    return out_path, total_records, total_pages


if __name__ == "__main__":
    # simple CLI
    import argparse
    p = argparse.ArgumentParser(description="Scrape GeM all-bids and enqueue to pipeline (DataExtraction upgraded).")
    p.add_argument("--headless", action="store_true", help="Run browser headless")
    p.add_argument("--no-enqueue", action="store_true", help="Do not enqueue to Redis/DB (CSV-only)")
    p.add_argument("--no-csv", action="store_true", help="Do not write CSV backup")
    args = p.parse_args()

    run_and_save(output_csv="gem_full_fixed.csv", headless=args.headless, status_path=None, enqueue=(not args.no_enqueue), save_csv=(not args.no_csv))
