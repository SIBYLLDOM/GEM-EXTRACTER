import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import time
import re
import os
import json

BASE_URL = "https://bidplus.gem.gov.in"

# LIMIT SCRAPING TO FIRST 2 PAGES
MAX_PAGES = 2

# --- status helper (best-effort, same format used by url_pdf_extraction) ---
def write_status_file(status_path: str, updates: dict):
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
            log.append(msg)
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



async def scrape_single_page(page, page_no, status_path=None):
    write_status_file(status_path, {"message": f"🔵 Scraping PAGE {page_no}", "stage": "scraping", "scraped_pages": page_no})

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

            results.append({
                "Page": page_no,
                "Bid Number": bid_no,
                "Detail URL": detail_url,
                "Items": items,
                "Quantity": quantity,
                "Department": department,
                "Start Date": start_date,
                "End Date": end_date
            })
        except:
            pass

    return results



async def scrape_all(headless=False, status_path=None):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            channel="chrome",
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context()
        page = await context.new_page()

        total_records, total_pages = await extract_total_counts(page, status_path=status_path)

        # LIMIT to MAX_PAGES
        total_pages = min(total_pages, MAX_PAGES)

        write_status_file(status_path, {
            "message": f"Starting scraping: LIMIT = first {MAX_PAGES} pages",
            "stage": "scraping"
        })

        all_data = []

        # scrape page 1
        page_no = 1
        page_results = await scrape_single_page(page, page_no, status_path=status_path)
        all_data.extend(page_results)
        write_status_file(status_path, {"scraped_records": len(all_data)})

        # pages 2..MAX_PAGES
        while page_no < total_pages:
            next_btn = await page.query_selector("#light-pagination a.next")
            if not next_btn:
                write_status_file(status_path, {"message": "No NEXT button — stopping", "stage": "scraping"})
                break

            page_no += 1
            write_status_file(status_path, {"message": f"➡ Clicking NEXT → Page {page_no}", "stage": "scraping"})
            await next_btn.click()
            await asyncio.sleep(2)

            page_results = await scrape_single_page(page, page_no, status_path=status_path)
            all_data.extend(page_results)
            write_status_file(status_path, {"scraped_records": len(all_data)})

        await browser.close()

        write_status_file(status_path, {"message": f"Scraping complete. Total scraped: {len(all_data)}", "stage": "scraped"})

        return all_data, total_records, total_pages



def run_and_save(output_csv="gem_full_fixed.csv", headless=False, status_path=None):
    start = time.time()

    write_status_file(status_path, {"stage": "starting", "message": f"Starting scraper (limit {MAX_PAGES} pages)..."})

    data, total_records, total_pages = asyncio.run(scrape_all(headless=headless, status_path=status_path))

    out_path = os.path.join(os.getcwd(), output_csv)
    pd.DataFrame(data).to_csv(out_path, index=False)

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
    print(f"Saved: {out_path}")

    return out_path, total_records, total_pages



if __name__ == "__main__":
    run_and_save(output_csv="gem_full_fixed.csv", headless=False, status_path=None)
