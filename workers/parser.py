"""
workers/parser.py

PDF -> JSON parser used by workers/consumer.py

Provides:
    extract_pdf_to_json(pdf_path, out_json_path, use_ocr_if_needed=True) -> dict

Dependencies:
 - pdfplumber
 - pytesseract (optional; only used when text is very small)
 - PyPDF2 (optional - not strictly required here)

Notes:
 - This parser uses heuristic rules for extracting canonical fields from GeM PDFs.
 - It produces a rich page-level JSON (cleaned text, tables) and also a compact `structured` dict.
 - Confidence is computed as fraction-of-key-fields-found and is a simple heuristic.
"""

import os
import re
import json
import logging
from typing import List, Dict, Any, Optional

import pdfplumber
import pytesseract
from PIL import Image

LOG = logging.getLogger("parser")
LOG.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
LOG.addHandler(handler)

# heuristics / thresholds
OCR_THRESHOLD_CHARS = 40
CLEAN_LINE_MIN_LEN = 2

# regex patterns (case-insensitive)
RE_EMD = re.compile(r"\bEMD\b|\bEarnest Money\b|\bEMD Amount\b|\bEarnest Money Deposit\b", re.I)
RE_EPBG = re.compile(r"\b(e-?PBG|EPBG|PBG|Performance Bank Guarantee)\b", re.I)
RE_QTY = re.compile(r"\bQty(?:uantity)?[:\s]*([0-9,\.]+)\b", re.I)
RE_TOTAL_QTY = re.compile(r"\bTotal Quantity[:\s]*([0-9,\,\.]+)\b", re.I)
RE_UNIT = re.compile(r"\bUnit[:\s]*([A-Za-z0-9\/\-\s]+)\b", re.I)
RE_EST_VALUE = re.compile(r"\bEstimated Value[:\s]*([A-Za-z0-9\.,\-\s₹RsINR]+)\b", re.I)
RE_BID_NO = re.compile(r"\b(Bid No(?:\.|:)?|Bid Number[:\s])\s*([A-Za-z0-9\-/]+)", re.I)
RE_BID_END = re.compile(r"\bBid End Date[:\s]*([^\n\r]+)", re.I)
RE_ITEM = re.compile(r"\bItem(?:s)?[:\s]*(.+)", re.I)
RE_CONSIGNEE = re.compile(r"\bConsignee[:\s]*(.+)", re.I)


def sanitize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    # remove (cid:nnn) tokens common in pdfplumber outputs, collapse whitespace
    text = re.sub(r"\(cid:\d+\)", "", text)
    text = re.sub(r"[ \t\f\r\v]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    return text.strip()


def ocr_page_image(page, resolution=200) -> str:
    """
    Render pdfplumber page to image and run pytesseract.
    Return OCR text (may be empty).
    """
    try:
        pil = page.to_image(resolution=resolution).original
        text = pytesseract.image_to_string(pil)
        return text or ""
    except Exception as e:
        LOG.debug("OCR failed: %s", e)
        return ""


def extract_tables_from_page(page) -> List[List[List[str]]]:
    out = []
    try:
        raw_tables = page.extract_tables() or []
        for t in raw_tables:
            cleaned = []
            for row in t:
                cleaned_row = [sanitize_text(str(c)) if c is not None else "" for c in row]
                cleaned.append(cleaned_row)
            out.append(cleaned)
    except Exception as e:
        LOG.debug("extract_tables_from_page error: %s", e)
    return out


def page_to_struct(page, page_no: int, use_ocr_if_needed: bool) -> Dict[str, Any]:
    """
    Convert a pdfplumber page to a structured dict with cleaned_text, lines and tables.
    """
    text = ""
    used_ocr = False
    try:
        text = page.extract_text() or ""
    except Exception:
        text = ""

    if (not text or len(text.strip()) < OCR_THRESHOLD_CHARS) and use_ocr_if_needed:
        ocr_text = ocr_page_image(page)
        if ocr_text and len(ocr_text.strip()) >= OCR_THRESHOLD_CHARS:
            text = ocr_text
            used_ocr = True

    cleaned = sanitize_text(text)
    lines = [ln.strip() for ln in cleaned.splitlines() if ln.strip() and len(ln.strip()) >= CLEAN_LINE_MIN_LEN]
    tables = extract_tables_from_page(page)

    return {
        "page_number": page_no,
        "used_ocr": used_ocr,
        "cleaned_text": cleaned,
        "lines": lines,
        "tables": tables
    }


def extract_key_values_from_text(text: str) -> Dict[str, Optional[str]]:
    """
    Run regex heuristics over the entire combined text to find common fields.
    Returns dict of candidate values (may be None).
    """
    out = {
        "bid_number": None,
        "bid_end": None,
        "items": None,
        "total_quantity": None,
        "unit": None,
        "emd_amount": None,
        "epbg_required": None,
        "buyer": None,
        "consignee": None,
        "estimated_value": None
    }

    # simple finds
    m = RE_BID_NO.search(text)
    if m:
        # group 2 holds the actual id in our pattern
        out["bid_number"] = (m.group(2) or "").strip()

    m = RE_BID_END.search(text)
    if m:
        out["bid_end"] = m.group(1).strip()

    m = RE_ITEM.search(text)
    if m:
        out["items"] = m.group(1).strip()

    m = RE_TOTAL_QTY.search(text)
    if m:
        out["total_quantity"] = m.group(1).strip()
    else:
        m = RE_QTY.search(text)
        if m:
            out["total_quantity"] = m.group(1).strip()

    m = RE_UNIT.search(text)
    if m:
        out["unit"] = m.group(1).strip()

    m = RE_EMD.search(text)
    if m:
        # try to capture nearby amount text
        # find EMD occurrence and take following 100 chars for amount search
        idx = m.start()
        snippet = text[idx: idx + 200]
        amt_m = re.search(r"([₹RsINR\s]*[0-9\.,]+(?:\s*[lL]akh|[lL]ac[h]?|[cC]rore)?)", snippet)
        if amt_m:
            out["emd_amount"] = amt_m.group(1).strip()
        else:
            out["emd_amount"] = ""  # present but amount not found

    m = RE_EPBG.search(text)
    if m:
        out["epbg_required"] = "yes"

    m = RE_EST_VALUE.search(text)
    if m:
        out["estimated_value"] = m.group(1).strip()

    m = RE_CONSIGNEE.search(text)
    if m:
        out["consignee"] = m.group(1).strip()

    # As fallback: try to pick buyer from top of doc (first 3 non-empty lines)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not out["buyer"] and lines:
        out["buyer"] = lines[0].strip()

    return out


def tables_to_kv(table: List[List[str]]) -> Dict[str, str]:
    """
    Convert a two-column table into key->value dict if it looks like kv pairs.
    """
    kv = {}
    # If table has two columns and many rows, map first col->second col
    if not table:
        return kv
    # detect if most rows have at least 2 columns with non-empty first column
    two_col_like = sum(1 for r in table if len(r) >= 2 and r[0].strip()) >= max(1, len(table)//2)
    if two_col_like:
        for row in table:
            if len(row) >= 2:
                k = row[0].strip()
                v = row[1].strip()
                if k and (v or v == ""):
                    kv[k] = v
    return kv


def extract_structured_from_pages(pages: List[Dict[str, Any]]) -> (Dict[str, Any], float):
    """
    Given page structs (cleaned_text, lines, tables), produce:
      - structured dict with canonical fields
      - confidence score (0..1) based on how many key fields found
    """
    combined_text = "\n\n".join(p.get("cleaned_text", "") for p in pages if p.get("cleaned_text"))
    candidates = extract_key_values_from_text(combined_text)

    # parse tables heuristically to extract technical specs and other key values
    technical_specs = {}
    for pg in pages:
        for tbl in pg.get("tables", []):
            kv = tables_to_kv(tbl)
            if kv:
                # merge kv into technical_specs with some key normalization
                for k, v in kv.items():
                    nk = re.sub(r'[:\s]+$', '', k).strip()
                    technical_specs[nk] = v

    # if technical_specs empty, but there are tables, try to use first table as list of specs
    if not technical_specs:
        for pg in pages:
            if pg.get("tables"):
                # take first table and if it has header and 2+rows, create key/value pairs roughly
                tbl = pg["tables"][0]
                kv = tables_to_kv(tbl)
                if kv:
                    technical_specs.update(kv)
                    break

    # compile final structured dict
    structured = {
        "buyer": candidates.get("buyer"),
        "item_description": candidates.get("items"),
        "total_quantity": candidates.get("total_quantity"),
        "unit": candidates.get("unit"),
        "emd_amount": candidates.get("emd_amount"),
        "epbg_required": (candidates.get("epbg_required") == "yes"),
        "technical_specs": technical_specs or None,
        "consignee": candidates.get("consignee"),
        "estimated_value": candidates.get("estimated_value"),
        "bid_number_extracted": candidates.get("bid_number"),
        "bid_end": candidates.get("bid_end")
    }

    # compute simple confidence: fraction of the most important fields present
    important_fields = ["item_description", "total_quantity", "emd_amount", "technical_specs"]
    found = sum(1 for f in important_fields if structured.get(f))
    confidence = found / len(important_fields) if important_fields else 0.0

    # small boost if bid_number was found
    if structured.get("bid_number_extracted"):
        confidence = min(1.0, confidence + 0.2)

    return structured, confidence


def extract_pdf_to_json(pdf_path: str, out_json_path: str, use_ocr_if_needed: bool = True) -> Dict[str, Any]:
    """
    Main entrypoint used by the worker.

    Returns:
      {
        "ok": True/False,
        "json_path": <path>,
        "structured": { ... canonical fields ... },
        "confidence": 0..1,
        "message": "optional details"
      }
    """
    try:
        if not os.path.exists(pdf_path):
            return {"ok": False, "message": f"pdf_missing:{pdf_path}"}

        pages_structs = []
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                pg_struct = page_to_struct(page, page_no=i, use_ocr_if_needed=use_ocr_if_needed)
                pages_structs.append(pg_struct)

        combined = "\n\n".join(p.get("cleaned_text", "") for p in pages_structs if p.get("cleaned_text"))
        out = {
            "source_file": os.path.basename(pdf_path),
            "num_pages": len(pages_structs),
            "pages": pages_structs,
            "combined_cleaned_text": combined
        }

        # Extract structured canonical fields
        structured, confidence = extract_structured_from_pages(pages_structs)
        out["structured"] = structured

        # Ensure output dir exists and write JSON
        os.makedirs(os.path.dirname(out_json_path) or ".", exist_ok=True)
        # Remove heavy page word bboxes (we didn't include words here) — safe to write
        with open(out_json_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

        return {"ok": True, "json_path": out_json_path, "structured": structured, "confidence": round(float(confidence), 3)}
    except Exception as e:
        LOG.exception("extract_pdf_to_json failed: %s", e)
        return {"ok": False, "message": str(e)}
