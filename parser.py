# parser.py
# -----------------------------------------------
# PDF → rows extractor for atovio labels.
# Rules:
#  - 1 label == 1 PDF PAGE (primary rule)
#  - Fallback: if a page happens to contain >1 label, split inside the page
#    by "Powered By Proship"
#  - Extracts: order_date, customer_name, contact_number, product_name, sku,
#              quantity, awb, page_index (1-based)
#  - Product names are canonicalized from SKU master (if present)
# -----------------------------------------------

from __future__ import annotations
from typing import List, Dict, Any, Tuple
from datetime import datetime
import fitz
import re
import os

# --- Page delimiter fallback token ---
ORDER_END_TOKEN = "powered by proship"  # case-insensitive check

# --- Regexes for fields; made resilient to spacing/case variations ---
DATE_LINE_RX = re.compile(
    r"order\s*date\s*[:\-]\s*(\d{4}-\d{2}-\d{2}|\d{2}[/-]\d{2}[/-]\d{4})",
    re.IGNORECASE
)
# Capture the first 10-digit number appearing after "Contact Number"
CONTACT_RX = re.compile(
    r"(?i)contact\s*number[^0-9]{0,20}(\d{10})"
)

ORDER_ID_RX = re.compile(r"(?i)order\s*id[^0-9]{0,10}(\d{4,5})")

AWB_RX = re.compile(r"courier\s*awb\s*no\s*:\s*([A-Z0-9]+)", re.IGNORECASE)

# --- Local SKU master loader (kept local to avoid circular import with db.py) ---
MASTER_DIR = os.path.join(os.path.dirname(__file__), "data")
SKU_MASTER_CSV = os.path.join(MASTER_DIR, "sku_master.csv")

SKU_TOKEN_RX = re.compile(r"\b([A-Z]{2,}\s*\d{1,})\b")

# Lightweight fallback when SKU line is missing but description hints colour/variant
_DEF_SKU_FROM_DESC = [
    ("moonlight black", "AT0001"),
    ("sky blue",        "AT0002"),
    ("cloud white",     "AT0003"),
    ("blush pink",      "AT0004"),
    # extend here if you want: ("transparent", "AT0103"), etc.
]

def _guess_sku_from_description(desc: str) -> str:
    d = (desc or "").lower()
    for needle, sku in _DEF_SKU_FROM_DESC:
        if needle in d:
            return sku
    return ""

def _sku_norm(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", (s or "").upper())

def _normalize_sku_for_db(s: str) -> str:
    if not s:
        return ""
    s = s.strip().upper()
    # --- Ignore non-real SKUs ---
    if s == "NIL" or not s.startswith("AT"):
        return ""
    return s

def _load_sku_master() -> Dict[str, Dict[str, str]]:
    """
    Returns: { SKU_NORM : {"name": <canonical name>, "type": "Compulsory"|"Loose"} }
    We only need the name here; type is used in db.py during scanning.
    """
    m: Dict[str, Dict[str, str]] = {}
    if os.path.exists(SKU_MASTER_CSV):
        with open(SKU_MASTER_CSV, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                line = line.rstrip("\n")
                if not line:
                    continue
                if i == 0 and "sku" in line.lower():
                    continue  # header
                parts = [p.strip() for p in line.split(",")]
                if len(parts) < 3:
                    continue
                sku = parts[0]
                typ = parts[-1]
                name = ",".join(parts[1:-1]).strip()
                m[_sku_norm(sku)] = {"name": name, "type": typ}
    return m

SKU_MASTER = _load_sku_master()

def _canonical_name_for_sku(sku: str, fallback: str) -> str:
    info = SKU_MASTER.get(_sku_norm(sku))
    return info["name"] if (info and info.get("name")) else (fallback or "")

def _norm_date(s: str) -> str:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d-%m-%Y")
        except ValueError:
            continue
    return s  # leave as-is if unexpected

def _parse_product_block(lines: List[str], i: int) -> Tuple[List[Dict[str, Any]], int]:
    """
    Robust 3-line row parser:
      desc
      sku?      (may be missing)
      qty
    - Searches up to the next 3 lines for SKU/qty.
    - If SKU line is missing, tries _guess_sku_from_description(desc).
    - NEVER emits a duplicate: if a SKU is found/guessed, we DO NOT also make a blank-SKU row.
    - Advances i by exactly how many lines were consumed (2 or 3), not just +1.
    """
    items: List[Dict[str, Any]] = []
    seen = set()
    breakers = {
        "tracking id:", "order id:", "return address", "handover to",
        "sold by:", "gstin:", "prepaid", "cash on delivery", "delivery address:",
        "courier awb no:", "mode of shipping:", "total price:", ORDER_END_TOKEN
    }

    # Must begin at the header trio
    if not (i + 2 < len(lines)
            and lines[i].lower() == "description"
            and lines[i+1].lower() == "sku"
            and lines[i+2].lower() == "qty"):
        return items, i

    i += 3  # skip headers

    def is_break(ln: str) -> bool:
        low = (ln or "").strip().lower()
        return (not low) or any(low.startswith(b) for b in breakers)

    n = len(lines)
    while i < n:
        # Skip empties
        while i < n and not (lines[i] or "").strip():
            i += 1
        if i >= n:
            break
        if is_break(lines[i]):
            break

        # Merge multi-line descriptions if next line looks like continuation
        desc = (lines[i] or "").strip()
        while (
            i + 1 < n
            and not is_break(lines[i + 1])
            and not SKU_TOKEN_RX.search(lines[i + 1])  # not a SKU line
            and not re.fullmatch(r"\d+", (lines[i + 1] or "").strip())  # not qty line
            and len((lines[i + 1] or "").strip().split()) <= 3  # small trailing word(s)
        ):
            desc += " " + (lines[i + 1] or "").strip()
            i += 1
        # --- NEW FIX END ---

        l1 = (lines[i + 1] or "").strip() if i + 1 < n else ""
        l2 = (lines[i + 2] or "").strip() if i + 2 < n else ""

        # Try detect SKU (in l1 first, else in window i..i+2)
        sku = ""
        used_sku_line = None
        m = SKU_TOKEN_RX.search(l1)
        if m:
            sku = _normalize_sku_for_db(m.group(1))
            used_sku_line = i+1
        else:
            for j in range(i, min(i+3, n)):
                mm = SKU_TOKEN_RX.search((lines[j] or ""))
                if mm:
                    sku = _normalize_sku_for_db(mm.group(1))
                    used_sku_line = j
                    break

        # Detect qty (prefer pure-digit line; else trailing integer)
        qty = None
        used_qty_line = None
        for j in range(i, min(i+3, n)):
            ln = (lines[j] or "").strip()
            if re.fullmatch(r"\d+", ln):
                qty = int(ln)
                used_qty_line = j
                break
        if qty is None:
            for j in range(i, min(i+3, n)):
                ln = (lines[j] or "").strip()
                mqty = re.search(r"\b(\d+)\b\s*$", ln)
                if mqty:
                    qty = int(mqty.group(1))
                    used_qty_line = j
                    break

        # If SKU line is missing but desc suggests a variant, guess it
        guessed = False
        if not sku:
            guessed_code = _guess_sku_from_description(desc)
            if guessed_code:
                sku = _normalize_sku_for_db(guessed_code)
                guessed = True

        # If we still have neither SKU nor qty, this isn't a row—advance safely by 1
        if not sku and qty is None:
            i += 1
            continue

        if qty is None:
            qty = 1  # defensible default

        product = _canonical_name_for_sku(sku, desc)

        key = (product, sku, qty)
        # --- Optional safety addition ---
        # Avoid ghost lines when a long description caused duplication
        if not sku and "atovio" in desc.lower() and any(ch.isdigit() for ch in desc):
            # Skip likely duplicate/no-SKU fragment line
            i += 1
            continue
        # --- End optional safety addition ---

        # IMPORTANT: do NOT create a blank-SKU row if we've found/guessed a SKU.
        if (sku or product) and key not in seen:
            items.append({"product_name": product, "sku": sku, "quantity": qty})
            seen.add(key)

        # Decide how many lines we consumed and advance exactly past them
        consumed_end = i  # last index we should consume
        # If we had a distinct SKU line, count it
        if used_sku_line is not None:
            consumed_end = max(consumed_end, used_sku_line)
        # If we had a distinct qty line, count it
        if used_qty_line is not None:
            consumed_end = max(consumed_end, used_qty_line)
        # If neither SKU nor qty line existed (e.g., desc + guessed SKU + qty on desc),
        # we at least consumed desc; if qty was on l1, we consumed i+1, etc.
        if used_sku_line is None and used_qty_line is None:
            # Try to detect if l1 looked like qty to consume it too
            if re.fullmatch(r"\d+", l1):
                consumed_end = max(consumed_end, i+1)

        # Move to the next line after what we consumed
        i = consumed_end + 1

        # Stop if next line is a breaker
        if i < n and is_break(lines[i]):
            break

    return items, i

def _parse_single_order(order_lines: List[str]) -> List[Dict[str, Any]]:
    """
    Parse a single order's lines (everything before 'Powered By Proship').
    Returns list of row dicts:
      order_date, customer_name, contact_number, product_name, sku, quantity, awb
    """
    rows: List[Dict[str, Any]] = []
    order_date = ""
    customer_name = ""
    contact_number = ""
    awb = ""
    order_id = ""

    # Find fields
    for idx, raw in enumerate(order_lines):
        ln = (raw or "").strip()
        low = ln.lower()

        # Order Date
        mdate = DATE_LINE_RX.search(ln)
        if mdate:
            order_date = _norm_date(mdate.group(1))
            continue
        
        # Order ID
        mid = ORDER_ID_RX.search(ln)
        if mid:
            order_id = mid.group(1)
            continue

        # AWB
        maw = AWB_RX.search(ln)
        if maw:
            awb = (maw.group(1) or "").strip()
            continue

        # Delivery Address -> next non-empty line is usually the name
        if low == "delivery address:":
            j = idx + 1
            while j < len(order_lines) and not (order_lines[j] or "").strip():
                j += 1
            if j < len(order_lines):
                customer_name = (order_lines[j] or "").strip()
            continue

        # Contact Number
        mcont = CONTACT_RX.search(ln)
        if mcont:
            num = mcont.group(1)
            # Explicitly ignore our own office number
            if num != "9996642108":
                contact_number = num
            continue

    # Scan for product blocks
    i = 0
    seen_in_order = set()  # (product, sku, qty) across ALL blocks
    while i < len(order_lines):
        if (order_lines[i] or "").strip().lower() == "description":
            items, j = _parse_product_block(order_lines, i)
            for it in items:
                key = (it["product_name"], it["sku"], it["quantity"])
                if key in seen_in_order:
                    continue
                seen_in_order.add(key)
                rows.append({
                    "order_date": order_date,
                    "order_id": order_id,
                    "customer_name": customer_name,
                    "contact_number": contact_number,
                    "product_name": it["product_name"],
                    "sku": it["sku"],
                    "quantity": it["quantity"],
                    "awb": awb,
                })
            i = j
        else:
            i += 1

    return rows

def parse_labels_from_pdf(pdf_path: str) -> List[Dict[str, Any]]:
    """
    New logic: treat each PDF PAGE as a single label/order.
    Still keeps a fallback: if a *single* page contains multiple labels,
    we split that page by 'Powered By Proship' inside the page.
    Adds page_index (1-based).
    """
    doc = fitz.open(pdf_path)
    all_rows: List[Dict[str, Any]] = []

    for p in range(len(doc)):
        try:
            t = doc.load_page(p).get_text("text")
            print(t)
        except Exception:
            # If page text extraction fails, skip safely
            continue

        lines = [ (ln or "").strip() for ln in t.splitlines() ]
        if not any(lines):
            continue

        # Fallback: detect multiple labels in a single page
        # --- Improved multi-order page splitter ---
        # --- Improved multi-order page splitter ---
        SPLIT_MARKERS = (
            "powered by proship",
            "handover to bluedart air",
            "handover to bluedart",
        )

        segments: List[List[str]] = []
        buf: List[str] = []

        def is_split_marker(ln: str) -> bool:
            low = (ln or "").strip().lower()
            return any(low.startswith(m) for m in SPLIT_MARKERS)

        for ln in lines:
            buf.append(ln)
            if is_split_marker(ln):
                # end current order segment
                if any((s or "").strip() for s in buf):
                    segments.append(buf)
                buf = []

        # catch any trailing lines after last marker
        if buf and any((s or "").strip() for s in buf):
            segments.append(buf)

        
        for seg in segments:
            # Skip empty or incomplete sub-blocks (must contain an AWB)
            if not any("awb" in (ln or "").lower() for ln in seg):
                continue
            rows = _parse_single_order(seg)
            for r in rows:
                r["page_index"] = str(p + 1)  # 1-based
                r["source_file"] = ""         # filled in app.py on upload
            all_rows.extend(rows)

    doc.close()
    return all_rows
