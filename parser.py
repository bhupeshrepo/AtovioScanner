# parser.py
# -----------------------------------------------------------------------------
# Extracts: order_date, customer_name, contact_number, product_name, sku,
#           quantity, awb, product_id("")
#
# Fixes:
# - Stronger segmentation: split on Proship footer, 2nd PREPAID/COD, or 2nd AWB.
# - Contact number is chosen near DELIVERY ADDRESS, not global.
# - Canonical product_name from SKU map; SKU normalization (AT0003 -> AT003).
# - product_id ALWAYS "" (filled later when scanning barcode).
# -----------------------------------------------------------------------------
from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Any, Tuple  # <-- ensure Tuple is imported
import fitz  # PyMuPDF
# parser.py (top of file)
import json, os, re
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _load_sku_map():
    path = os.path.join(BASE_DIR, "skus.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            m = json.load(f) or {}
        # normalize keys to AT0000 form
        out = {}
        for k, v in m.items():
            kk = re.sub(r"[^A-Za-z0-9]", "", k.upper())
            # pad trailing digits to 4 (AT0001)
            m2 = re.match(r"^([A-Z]+)(\d+)$", kk)
            if m2:
                out[f"{m2.group(1)}{m2.group(2).zfill(4)}"] = v
            else:
                out[kk] = v
        return out
    except Exception:
        return {}

# ---------------- Canonical SKU -> product_name ----------------
SKU_CANON = _load_sku_map() or {
    "AT0001": "atovio Pebble- Portable Air Purifier_Moonlight Black",
    "AT0002": "atovio Pebble- Portable Air Purifier_Sky blue",
    "AT0003": "atovio Pebble- Portable Air Purifier_Cloud White",
    "AT0004": "atovio Pebble- Portable Air Purifier_Blush Pink",
    "AT0020": "Round Neck Strap",
    "AT0021": "Black Metallic Chain",
    "AT0022": "Silver Metallic Chain",
    "AT0100": "Strips (Black, P-10)",
    "AT0101": "Strips (Black, P-20)",
    "AT0102": "Strips (Black, P-30)",
    "AT0103": "Strips (Trans, P-10)",
    "AT0104": "Strips (Trans, P-20)",
    "AT0105": "Strips (Trans, P-30)",
    "AT0150": "Mask (Black)",
    "AT0151": "Mask (Grey)",
    "AT0200": "Filter (Pack of 10)"
}

def clean_line(s: str) -> str:
    """Strip zero-width chars, quotes/backticks, collapse spaces."""
    if not s:
        return ""
    s = re.sub(r"[\u200B-\u200D\uFEFF]", "", s)     # zero-width
    s = s.replace("`", "").replace("´", "").replace("’", "").replace("‘", "").replace('"', "")
    s = re.sub(r"[ \t]+", " ", s).strip()
    return s

def normalize_sku(raw: str) -> str:
    """
    Normalize SKU like 'AT1', 'AT0001', 'at0001`' -> 'AT0001'.
    Removes all non-alphanumerics before matching.
    """
    if not raw:
        return ""
    s = clean_line(raw).upper()
    s = re.sub(r"[^A-Z0-9]", "", s)  # <- THIS fixes the backtick / punctuation case
    m = re.match(r"^([A-Z]+)(\d{1,6})$", s)
    if not m:
        return ""
    prefix, digits = m.groups()
    # pad to 4 for AT-series (adjust if you have other families)
    if prefix.startswith("AT"):
        return f"AT{digits.zfill(4)}"
    return f"{prefix}{digits}"

def name_from_sku(raw: str) -> str:
    sk = normalize_sku(raw)
    return SKU_CANON.get(sk, "")

# ---------------- Regexes ----------------
ORDER_END_TOKEN = "powered by proship"   # case-insensitive prefix check
PAYMENT_HEADER_RX = re.compile(
    r"^(?:prepaid|cash\s+on\s+delivery|cod)(?:\s*\|\|\s*cod amount:.*)?$",
    re.IGNORECASE
)
DATE_LINE_RX = re.compile(
    r"order\s*date\s*[:\-]\s*(\d{4}-\d{2}-\d{2}|\d{2}[/-]\d{2}[/-]\d{4})",
    re.IGNORECASE
)
CONTACT_RX = re.compile(r"contact\s*number\s*:\s*([0-9]{6,})", re.IGNORECASE)
AWB_RX = re.compile(r"courier\s*awb\s*no\s*:\s*([0-9A-Z/]+)", re.IGNORECASE)

HEADER_DESC = re.compile(r"^description\s*$", re.IGNORECASE)
HEADER_SKU  = re.compile(r"^sku\s*$",         re.IGNORECASE)
HEADER_QTY  = re.compile(r"^qty\s*$",         re.IGNORECASE)
# SKUs like AT0001, AT021, AT22 (we'll normalize later)
SKU_TOKEN_RX = re.compile(r"^[A-Z]{2,}[A-Z0-9]*\d{1,}$")

# Lines that end a Description block
BREAKERS_PREFIX = tuple(s.lower() for s in [
    "tracking id:", "order id:", "return address", "handover to",
    "sold by:", "gstin:", "delivery address:", "courier awb no:",
    "mode of shipping:", "total price:", ORDER_END_TOKEN
])

# ---------------- Utilities ----------------
def _norm_date(s: str) -> str:
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d-%m-%Y")
        except ValueError:
            pass
    return s

# ---------------- Segmentation (strong) ----------------
def _segment_labels(lines_all: List[str]) -> List[List[str]]:
    """
    Segment using:
      1) Proship footer -> boundary
      2) 2nd payment header since last boundary -> boundary
      3) 2nd AWB since last boundary -> boundary
    The triggering header/AWB line belongs to the *new* label.
    """
    orders: List[List[str]] = []
    buf: List[str] = []
    pay_seen = False
    awb_seen = False

    def flush():
        nonlocal buf, pay_seen, awb_seen
        if buf and any((ln or "").strip() for ln in buf):
            orders.append(buf)
        buf = []
        pay_seen = False
        awb_seen = False

    for ln in lines_all:
        raw = (ln or "")
        low = raw.strip().lower()

        # Proship footer -> hard split (footer belongs to old label)
        if low.startswith(ORDER_END_TOKEN):
            buf.append(raw)
            flush()
            continue

        # Payment header ?
        if PAYMENT_HEADER_RX.match(raw.strip()):
            if pay_seen:
                # start new label; move this header to new buffer
                last = raw
                flush()
                buf.append(last)
                pay_seen = True
            else:
                buf.append(raw)
                pay_seen = True
            continue

        # AWB line ?
        if AWB_RX.search(raw):
            if awb_seen:
                # second AWB before a footer -> split before this line
                last = raw
                flush()
                buf.append(last)
                awb_seen = True
            else:
                buf.append(raw)
                awb_seen = True
            continue

        # default
        buf.append(raw)

    flush()
    return orders

# ---------------- Product block parsing ----------------
def _parse_products_block(seg: List[str], start_idx: int) -> Tuple[List[Dict[str, Any]], int]:
    """
    Stacked block:
      name line
      optional SKU line
      qty line (digits)
    Repeats until breaker or next header.
    """
    items: List[Dict[str, Any]] = []
    i = start_idx + 1

    # Skip header lines SKU/Qty within a small window
    limit = min(start_idx + 6, len(seg))
    while i < limit:
        token = (seg[i] or "").strip()
        if not token:
            i += 1
            continue
        if HEADER_SKU.match(token) or HEADER_QTY.match(token):
            i += 1
            continue
        break

    cur = {"name": None, "sku": "", "qty": None}

    def finalize():
        nonlocal cur
        if cur["name"]:
            # Canonicalize SKU and name
            norm = normalize_sku(cur["sku"])
            pname = name_from_sku(norm) or cur["name"]
            items.append({
                "product_name": pname,
                "sku": norm,
                "quantity": cur["qty"] if cur["qty"] is not None else 1
            })
        cur = {"name": None, "sku": "", "qty": None}

    while i < len(seg):
        token = (seg[i] or "").strip()
        low = token.lower()

        if not token:
            i += 1
            continue

        # Block breaker?
        if any(low.startswith(b) for b in BREAKERS_PREFIX) or HEADER_DESC.match(token):
            break

        # Qty-only line
        if token.isdigit():
            cur["qty"] = int(token)
            finalize()
            i += 1
            continue

        # SKU-only line
        if SKU_TOKEN_RX.fullmatch(token):
            cur["sku"] = token
            i += 1
            continue

        # Otherwise a product name
        if cur["name"] and cur["qty"] is None:
            # New name before qty -> assume previous qty=1
            finalize()
        cur["name"] = token
        i += 1

    # Flush tail
    if cur["name"]:
        finalize()

    return items, i

# ---------------- Order parsing ----------------
def _contact_near_delivery(order_lines: List[str]) -> str:
    """
    Prefer the contact number that appears within a short window
    *after* 'DELIVERY ADDRESS:' (common in your labels).
    Fall back to first contact anywhere in the segment.
    """
    for idx, raw in enumerate(order_lines):
        if (raw or "").strip().lower() == "delivery address:":
            window = order_lines[idx: idx + 14]
            for ln in window:
                m = CONTACT_RX.search(ln or "")
                if m:
                    return m.group(1)
            break
    for ln in order_lines:
        m = CONTACT_RX.search(ln or "")
        if m:
            return m.group(1)
    return ""

def _parse_single_order(order_lines: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    order_date = ""
    customer_name = ""
    awb = ""
    contact_number = _contact_near_delivery(order_lines)

    # Date, AWB, name near Delivery Address
    for idx, raw in enumerate(order_lines):
        ln = (raw or "").strip()
        low = ln.lower()

        m = DATE_LINE_RX.search(ln)
        if m:
            order_date = _norm_date(m.group(1)); continue

        if not awb:
            m = AWB_RX.search(ln)
            if m:
                awb = (m.group(1) or "").strip()
                continue

        if low == "delivery address:" and not customer_name:
            j = idx + 1
            while j < len(order_lines) and not (order_lines[j] or "").strip():
                j += 1
            if j < len(order_lines):
                customer_name = (order_lines[j] or "").strip()
            continue

    # Parse all Description blocks in this order
    i = 0
    seen = set()
    while i < len(order_lines):
        if HEADER_DESC.match((order_lines[i] or "").strip()):
            items, j = _parse_products_block(order_lines, i)
            for it in items:
                key = (it["product_name"], it["sku"], it["quantity"])
                if key in seen:
                    continue
                seen.add(key)
                rows.append({
                    "order_date": order_date,
                    "customer_name": customer_name,
                    "contact_number": contact_number,
                    "product_name": it["product_name"],
                    "sku": it["sku"],
                    "quantity": it["quantity"],
                    "awb": awb,
                    "product_id": ""  # filled later by barcode scan
                })
            i = j
        else:
            i += 1

    if not rows:
        rows.append({
            "order_date": order_date,
            "customer_name": customer_name,
            "contact_number": contact_number,
            "product_name": "",
            "sku": "",
            "quantity": "",
            "awb": awb,
            "product_id": ""
        })
    return rows

# ---------------- Public entrypoint ----------------
def parse_labels_from_pdf(pdf_path: str):
    """
    Page-wise parsing: 1 page == 1 label.
    Adds 'page_index' (1-based) to each row.
    """
    doc = fitz.open(pdf_path)
    all_rows = []

    for p in range(len(doc)):
        t = doc.load_page(p).get_text("text")
        # print(t)
        lines = [(ln or "").strip() for ln in t.splitlines()]
        if not any(lines):
            continue

        # If a rare page has multiple labels separated by the footer, split
        footer_hits = sum(1 for ln in lines if (ln or "").lower().startswith(ORDER_END_TOKEN))
        segments = []
        if footer_hits >= 2:
            buf = []
            for ln in lines:
                buf.append(ln)
                if (ln or "").lower().startswith(ORDER_END_TOKEN):
                    if any((s or "").strip() for s in buf):
                        segments.append(buf)
                    buf = []
            if buf and any(buf):
                segments.append(buf)
        else:
            segments = [lines]

        for seg in segments:
            rows = _parse_single_order(seg)
            # inject page_index for downstream print
            for r in rows:
                r["page_index"] = str(p + 1)  # 1-based page
            all_rows.extend(rows)

    doc.close()
    return all_rows
