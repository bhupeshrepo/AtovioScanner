# db.py
# -----------------------------------------------------------------------------
# CSV-backed "DB" for order rows.
# Schema (CSV headers):
#   order_date, customer_name, contact_number, product_name, sku, quantity,
#   awb, product_id, row_id, created_at, source_file
#
# product_id holds a comma-separated list of barcodes for multi-qty rows.
#
# Public API:
#   - DB_PATH
#   - init_db(DB_PATH)
#   - get_all() -> list[dict]
#   - upsert_orders(rows, source_file=None) -> int
#   - assign_product_id(awb, product_id) -> (ok, msg, http_status)    [legacy]
#   - assign_barcode(barcode, active_awb=None) -> (ok, payload|msg, http_status)
# -----------------------------------------------------------------------------

from __future__ import annotations
import csv
import os
import time
import hashlib
import re
from typing import List, Dict, Any, Tuple
from collections import Counter

DB_PATH = os.path.join(os.path.dirname(__file__), "orders_db.csv")

HEADERS = [
    "order_date", "customer_name", "contact_number", "product_name", "sku",
    "quantity", "awb", "product_id", "row_id", "created_at", "source_file"
]

# ------------- helpers -------------
def _now_iso() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _norm(s) -> str:
    return (s or "").strip()

def _normalize_sku_for_db(raw: str) -> str:
    """Normalize SKUs to 4 digits: AT1->AT0001, at0003->AT0003."""
    s = re.sub(r"[^A-Za-z0-9]", "", (raw or "").upper())
    m = re.match(r"^([A-Z]+)(\d+)$", s)
    if not m:
        return s
    prefix, digits = m.groups()
    tail = digits[-4:] if len(digits) >= 4 else digits.zfill(4)
    return f"{prefix}{tail}"

def _pid_list(raw: str) -> List[str]:
    """Parse comma-separated product_id(s) into a clean list."""
    if not raw:
        return []
    out = []
    for tok in raw.split(","):
        t = tok.strip().upper()
        if t:
            out.append(t)
    return out

def _pid_list_to_str(pids: List[str]) -> str:
    return ",".join(pids)

def _row_qty(row: Dict[str, Any]) -> int:
    try:
        return int(str(row.get("quantity","")).strip() or "0")
    except ValueError:
        return 0

def _row_done_units(row: Dict[str, Any]) -> int:
    return len(_pid_list(row.get("product_id","")))

def _row_remaining_units(row: Dict[str, Any]) -> int:
    qty = _row_qty(row)
    done = _row_done_units(row)
    return max(0, qty - done)

def _norm_key(r: Dict[str, Any]) -> str:
    """
    Stable dedupe key per row:
      (awb, product_name, sku(4d), quantity)
    """
    parts = [
        _norm(r.get("awb", "")).upper(),
        _norm(r.get("product_name", "")).lower(),
        _normalize_sku_for_db(r.get("sku", "")),
        str(r.get("quantity", "")).strip(),
    ]
    return hashlib.sha1(("||".join(parts)).encode("utf-8")).hexdigest()

def _read_all() -> List[Dict[str, str]]:
    if not os.path.exists(DB_PATH):
        return []
    rows: List[Dict[str, str]] = []
    with open(DB_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({k: r.get(k, "") for k in HEADERS})
    return rows

def _write_all(rows: List[Dict[str, Any]]) -> None:
    tmp = DB_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for r in rows:
            out = {h: r.get(h, "") for h in HEADERS}
            writer.writerow(out)
    os.replace(tmp, DB_PATH)

# ------------- public API -------------
def init_db(path: str | None = None) -> None:
    """Create the CSV with headers if missing."""
    global DB_PATH
    if path:
        DB_PATH = path
    if not os.path.exists(DB_PATH):
        with open(DB_PATH, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS)
            writer.writeheader()

def get_all() -> List[Dict[str, str]]:
    """Return all rows (as strings) sorted newest-first."""
    rows = _read_all()
    rows.sort(key=lambda r: (r.get("created_at") or "", r.get("awb") or ""), reverse=True)
    return rows

def upsert_orders(rows: List[Dict[str, Any]], source_file: str | None = None) -> int:
    """
    Insert new rows (one per product). Existing dedupe keys are ignored.
    Returns count of newly added rows.
    """
    if not rows:
        return 0

    db_rows = _read_all()
    existing = { _norm_key(r): True for r in db_rows }

    added = 0
    now = _now_iso()

    for r in rows:
        newr = {
            "order_date": _norm(r.get("order_date", "")),
            "customer_name": _norm(r.get("customer_name", "")),
            "contact_number": _norm(r.get("contact_number", "")),
            "product_name": _norm(r.get("product_name", "")),
            "sku": _normalize_sku_for_db(r.get("sku", "")),
            "quantity": str(r.get("quantity", "")).strip(),
            "awb": _norm(r.get("awb", "")),
            "product_id": "",      # blank at ingest; will hold comma-separated list on scans
            "row_id": "",          # set below
            "created_at": now,
            "source_file": source_file or "",
        }
        k = _norm_key(newr)
        if k in existing:
            continue
        newr["row_id"] = hashlib.md5(f"{k}|{now}".encode("utf-8")).hexdigest()[:12]
        db_rows.append(newr)
        existing[k] = True
        added += 1

    if added:
        _write_all(db_rows)
    return added

def assign_product_id(awb: str, product_id: str):
    """
    Legacy: assign a product_id to all rows with given AWB.
    CAUTION: not multi-qty aware; kept for backward compatibility.
    """
    awb = _norm(awb)
    product_id = _norm(product_id).upper()
    if not awb or not product_id:
        return False, "awb and product_id are required", 400

    rows = _read_all()
    idxs = [i for i, r in enumerate(rows) if _norm(r.get("awb", "")) == awb]
    if not idxs:
        return False, f"AWB '{awb}' not found", 404

    # reject if any row already has a DIFFERENT single product_id (legacy behavior)
    existing = { _norm(rows[i].get("product_id", "")) for i in idxs if _norm(rows[i].get("product_id", "")) }
    if existing and (len(existing) > 1 or (len(existing) == 1 and product_id not in existing)):
        return False, f"Conflicting product_id already set for AWB '{awb}'", 409

    changed = 0
    for i in idxs:
        if not _norm(rows[i].get("product_id", "")):
            rows[i]["product_id"] = product_id
            changed += 1

    if changed:
        _write_all(rows)
        return True, f"product_id set to '{product_id}' for AWB '{awb}' ({changed} rows)", 200
    else:
        return True, f"No rows needed update for AWB '{awb}'", 200

# ================= Barcode assignment (multi-qty aware) =================

_BARCODE_RX = re.compile(r"^\s*([A-Za-z]{2,}\d+)\s*-\s*([A-Za-z])\s*(\d{1,3})\s*$")

def _parse_barcode(code: str) -> Tuple[bool, str | Tuple[str, str], int]:
    """
    Return (ok, (sku_4d, pid_canon) or error_message, http_status)
    pid_canon is 'A001' (letter + zero-padded 3 digits).
    """
    code = (code or "").strip()
    m = _BARCODE_RX.fullmatch(code)
    if not m:
        return False, "Invalid barcode. Expect like 'AT0001-A001'", 400
    sku_raw, letter, digits = m.groups()
    sku_4d = _normalize_sku_for_db(sku_raw)
    pid_canon = f"{letter.upper()}{digits.zfill(3)}"
    return True, (sku_4d, pid_canon), 200

def _product_id_exists_anywhere(pid: str, rows: List[Dict[str, str]]) -> bool:
    """Global uniqueness: returns true if pid appears in ANY row's list."""
    pid = _norm(pid).upper()
    if not pid:
        return False
    for r in rows:
        if pid in _pid_list(r.get("product_id","")):
            return True
    return False

def _contact_group_units(rows: List[Dict[str, str]], contact: str) -> Tuple[int,int]:
    """
    Returns (done_units, total_units) across SKU rows for a contact_number.
    - total_units = sum(quantity)
    - done_units  = sum(len(product_id list))
    """
    c = _norm(contact)
    total = 0
    done = 0
    for r in rows:
        if _norm(r.get("contact_number","")) != c:
            continue
        if _norm(r.get("sku","")) == "":
            continue  # extras (no SKU) do not block shipment
        total += _row_qty(r)
        done  += _row_done_units(r)
    return done, total

def assign_barcode(barcode: str, active_awb: str | None = None) -> Tuple[bool, Dict[str, Any] | str, int]:
    """
    Assign a scanned barcode (SKU-PID) to ONE unit:
      - Parses barcode and normalizes (AT1->AT0001, a001->A001)
      - Prefers single-product customers (1 SKU row with qty=1) first
      - Otherwise assigns to the first multi-product row with remaining units
      - Appends PID to that row's product_id list
      - Returns group completeness in terms of UNITS (not rows)
    """
    ok, parsed, status = _parse_barcode((barcode or "").upper())
    if not ok:
        return False, parsed, status

    sku_4d, pid_canon = parsed
    rows = _read_all()

    # global uniqueness of this barcode
    if _product_id_exists_anywhere(pid_canon, rows):
        return False, f"Barcode {pid_canon} already assigned", 409

    # candidate rows: SKU matches, row still has remaining units
    cand = []
    for r in rows:
        if _normalize_sku_for_db(r.get("sku","")) != sku_4d:
            continue
        if _row_remaining_units(r) <= 0:
            continue
        cand.append(r)

    if active_awb:
        awb = _norm(active_awb)
        cand = [r for r in cand if _norm(r.get("awb","")) == awb]

    if not cand:
        suffix = f" under AWB {active_awb}" if active_awb else ""
        return False, f"No unassigned unit for SKU {sku_4d}{suffix}", 404

    # Classify contacts among SKU rows (by ROWS)
    sku_rows = [r for r in rows if _norm(r.get("sku","")) != ""]
    cnt_by_contact = Counter(_norm(r["contact_number"]) for r in sku_rows)

    # singles first: exactly one row for that contact AND that row qty == 1
    singles = [r for r in cand if cnt_by_contact[_norm(r["contact_number"])] == 1 and _row_qty(r) == 1]
    target = singles[0] if singles else cand[0]

    # Append the PID
    pids = _pid_list(target.get("product_id",""))
    pids.append(pid_canon)
    target["product_id"] = _pid_list_to_str(pids)
    _write_all(rows)

    # Compute group completeness in UNITS
    contact = _norm(target["contact_number"])
    done, total = _contact_group_units(rows, contact)
    group_complete = (total > 0 and done >= total)

    payload = {
        "message": f"Assigned {pid_canon} → {sku_4d}",
        "contact_number": contact,
        "group_complete": group_complete,
        "awb": _norm(target.get("awb","")),
        "row_progress": {"scanned": _row_done_units(target), "qty": _row_qty(target)},
        "group_progress": {"scanned": done, "qty": total}
    }
    return True, payload, 200
