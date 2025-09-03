import fitz
import re
from datetime import datetime

# -------- Regexes --------
DATE_LINE_RX = re.compile(
    r"order\s*date\s*[:\-]\s*(\d{4}-\d{2}-\d{2}|\d{2}[/-]\d{2}[/-]\d{4})",
    re.IGNORECASE
)
PAYMENT_HEADER_RX = re.compile(r"^(?:prepaid|cash\s+on\s+delivery|cod)\s*$", re.IGNORECASE)
CONTACT_RX = re.compile(r"contact\s*number\s*:\s*([0-9]{6,})", re.IGNORECASE)
AWB_RX = re.compile(r"courier\s*awb\s*no\s*:\s*([A-Z0-9]+)", re.IGNORECASE)

# Treat this as the order terminator
ORDER_END_TOKEN = "powered by proship"   # case-insensitive check

def _norm_date(s: str) -> str:
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d-%m-%Y")
        except ValueError:
            continue
    return s  # leave as-is if unexpected

def _parse_product_block(lines, i):
    """
    Expect headers:
        Description
        SKU
        Qty
    then 1..N product rows. Stop if a breaker starts.
    """
    items = []
    seen = set()  # (product, sku, qty) within THIS block
    # conservative breakers to avoid drifting into next sections
    breakers = {
        "tracking id:", "order id:", "return address", "handover to",
        "sold by:", "gstin:", "prepaid", "delivery address:",
        "courier awb no:", "mode of shipping:", "total price:", ORDER_END_TOKEN
    }

    if not (i + 2 < len(lines)
            and lines[i].lower() == "description"
            and lines[i+1].lower() == "sku"
            and lines[i+2].lower() == "qty"):
        return items, i

    i += 3
    while i < len(lines):
        ln = (lines[i] or "").strip()
        low = ln.lower()

        if (not ln) or any(low.startswith(b) for b in breakers):
            break

        # Merge current + next line—common split layout
        window = " ".join([
            lines[j].strip()
            for j in range(i, min(i+2, len(lines)))
            if (lines[j] or "").strip()
        ])

        mqty = re.search(r"\b(\d+)\b\s*$", window)
        if not mqty:
            # If we see a bare integer and we already captured a product without qty
            if re.fullmatch(r"\d+", ln) and items and not items[-1].get("quantity"):
                try:
                    items[-1]["quantity"] = int(ln)
                except Exception:
                    items[-1]["quantity"] = 1
                i += 1
                continue
            break

        qty = int(mqty.group(1))
        text_wo_qty = window[:mqty.start()].strip()

        # Heuristic SKU (e.g., AT0003, KS0123, etc.)
        msku = re.search(r"\b([A-Z]{2,}[A-Z0-9]*\d{2,})\b", text_wo_qty)
        if msku:
            sku = msku.group(1).strip()
            product = text_wo_qty[:msku.start()].strip()
        else:
            product = text_wo_qty
            sku = text_wo_qty

        key = (product, sku, qty)
        if key not in seen:
            items.append({"product_name": product, "sku": sku, "quantity": qty})
            seen.add(key)

        i += 1

    return items, i

def _parse_single_order(order_lines):
    """
    Parse a single order's lines (everything before 'Powered By Proship').
    Returns list of row dicts:
      order_date, customer_name, contact_number, product_name, sku, quantity, awb
    """
    rows = []
    # Active order fields
    order_date = ""
    customer_name = ""
    contact_number = ""
    awb = ""

    # Find top-level fields anywhere in the order segment
    for idx, raw in enumerate(order_lines):
        ln = (raw or "").strip()
        low = ln.lower()

        # Order Date
        mdate = DATE_LINE_RX.search(ln)
        if mdate:
            order_date = _norm_date(mdate.group(1))
            continue

        # AWB
        maw = AWB_RX.search(ln)
        if maw:
            awb = (maw.group(1) or "").strip()
            continue

        # Delivery Address -> next line is usually name (in your layouts)
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
            contact_number = mcont.group(1)
            continue

    # Now scan for product blocks (there can be multiple per order)
    i = 0
    seen_in_order = set()  # (product, sku, qty) across ALL blocks in this order
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

def parse_labels_from_pdf(pdf_path: str):
    """
    Split the PDF into orders using 'Powered By Proship' as the delimiter.
    Each segment becomes one order (with possibly multiple products).
    """
    doc = fitz.open(pdf_path)
    lines_all = []
    for p in range(len(doc)):
        t = doc.load_page(p).get_text("text")
        # keep even empty lines (as ""), we trim later; helps with "next non-empty" logic
        lines_all += [ln for ln in t.splitlines()]
        lines_all.append("<<<PAGE>>>")
    doc.close()

    # Segment into orders by 'Powered By Proship'
    orders = []
    buf = []
    for ln in lines_all:
        buf.append(ln)
        if (ln or "").strip().lower().startswith(ORDER_END_TOKEN):
            orders.append(buf)
            buf = []
    # In case the last order doesn't have a trailing token, include the remainder
    if buf:
        orders.append(buf)

    # Parse each order independently
    all_rows = []
    for seg in orders:
        # Strip trailing/leading empties
        seg_clean = [(s or "").strip() for s in seg]
        # Skip empty segments
        if not any(seg_clean):
            continue
        all_rows.extend(_parse_single_order(seg_clean))

    return all_rows
