# db.py
# -----------------------------------------------
# CSV "DB" layer + barcode assignment engine.
# Master-data driven behavior via:
#   - data/sku_master.csv      (sku, product_name, type: Compulsory|Loose)
#   - data/extras_noscan.csv   (sku) → always in Extras, never scanned
#
# Key Features:
#  - Canonical product names (prevents duplicate rows from multiline names)
#  - Multi-qty units: product_id stores comma-separated tokens
#  - Group completion ignores NoScan rows; includes Loose + Compulsory
#  - Returns print_info when a contact’s label completes
# -----------------------------------------------

from __future__ import annotations

import csv
import os
import re
import time
import hashlib
from typing import List, Dict, Any, Tuple
from collections import Counter
from functools import lru_cache

# -------------------------
# Storage
# -------------------------
DB_PATH = os.path.join(os.path.dirname(__file__), "orders_db.csv")

HEADERS = [
    "order_date",
    "order_id",
    "customer_name",
    "contact_number",
    "product_name",
    "sku",
    "quantity",
    "awb",
    "product_id",     # comma-separated tokens for multi-qty
    "row_id",
    "created_at",
    "source_file",    # uploads_<file>.pdf
    "page_index",     # 1-based page number inside source_file
]

# -------------------------
# Master data (CSV-driven)
# -------------------------
MASTER_DIR = os.path.join(os.path.dirname(__file__), "data")
SKU_MASTER_CSV = os.path.join(MASTER_DIR, "sku_master.csv")       # sku,product_name,type(Compulsory|Loose)
EXTRAS_NOSCAN_CSV = os.path.join(MASTER_DIR, "extras_noscan.csv") # sku (NoScan list)

def _now_iso() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _norm(s) -> str:
    return (s or "").strip()

def _sku_norm(s: str) -> str:
    """Uppercase alnum only (no padding)."""
    return re.sub(r"[^A-Za-z0-9]", "", (s or "").upper())

def _normalize_sku_for_db(raw: str) -> str:
    """
    Canonicalize like 'AT1' -> 'AT0001'.
    Keeps alpha prefix; zero-pads last 4 digits (or all digits if <4).
    """
    s = _sku_norm(raw)
    m = re.match(r"^([A-Z]+)(\d+)$", s)
    if not m:
        return s
    prefix, digits = m.groups()
    tail = digits[-4:] if len(digits) >= 4 else digits.zfill(4)
    return f"{prefix}{tail}"

@lru_cache(maxsize=1)
def _load_sku_master() -> Dict[str, Dict[str, str]]:
    """
    Loads sku_master.csv as a dict {sku: {name, display_name, type}}.
    Supports both 3-column (old) and 4-column (new) formats.
    """
    path = "data\sku_master.csv"
    m = {}

    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for parts in reader:
            # Skip empty or malformed rows
            if not parts or len(parts) < 3:
                continue

            # Header detection
            if parts[0].lower().strip() == "sku":
                continue

            sku = parts[0].strip().upper()
            product_name = ""
            display_name = ""
            sku_type = ""

            if len(parts) >= 4:
                # new format: sku, product_name, display_name, type
                product_name = parts[1].strip()
                display_name = parts[3].strip()
                sku_type = parts[2].strip().capitalize()
            else:
                # old format: sku, product_name, type
                product_name = parts[1].strip()
                sku_type = parts[3].strip().capitalize()

            m[_sku_norm(sku)] = {
                "name": product_name,
                "display_name": display_name,
                "type": sku_type,
            }

    return m

@lru_cache(maxsize=1)
def _load_extras_noscan() -> set:
    """
    Returns set of SKU_NORM that should never be scanned and must appear in Extras.
    """
    s: set = set()
    if os.path.exists(EXTRAS_NOSCAN_CSV):
        with open(EXTRAS_NOSCAN_CSV, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                if i == 0 and "sku" in line.lower():
                    continue
                sku = line.split(",")[0].strip()
                if sku:
                    s.add(_sku_norm(sku))
    return s

def reload_masters():
    """Call this if you update CSVs without restarting the server."""
    _load_sku_master.cache_clear()
    _load_extras_noscan.cache_clear()

def _sku_type(sku: str) -> str:
    """
    Returns "Compulsory" | "Loose" | "NoScan" | "Unknown".
    NoScan takes precedence over master type.
    """
    k = _sku_norm(sku)
    if not k:
        return "Unknown"
    if k in _load_extras_noscan():
        return "NoScan"
    info = _load_sku_master().get(k)
    return info.get("type") if info else "Unknown"

def _canonical_name_for_sku(sku: str, fallback: str) -> str:
    """
    Returns display_name if available, else name, else fallback.
    """
    info = _load_sku_master().get(_sku_norm(sku))
    if not info:
        return fallback or ""
    return info.get("display_name") or info.get("name") or (fallback or "")


# -------------------------
# CSV "DB" helpers
# -------------------------
def init_db(path: str | None = None) -> None:
    global DB_PATH
    if path:
        DB_PATH = path
    if not os.path.exists(DB_PATH):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        with open(DB_PATH, "w", encoding="utf-8", newline="") as f:
            csv.DictWriter(f, fieldnames=HEADERS).writeheader()

def _read_all() -> List[Dict[str, str]]:
    if not os.path.exists(DB_PATH):
        return []
    rows: List[Dict[str, str]] = []
    with open(DB_PATH, "r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            # normalize missing fields
            rows.append({h: r.get(h, "") for h in HEADERS})
    return rows

def _write_all(rows: List[Dict[str, Any]]) -> None:
    tmp = DB_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, "") for h in HEADERS})
    os.replace(tmp, DB_PATH)

def get_all() -> List[Dict[str, str]]:
    """
    Returns rows + annotates:
      - sku_type (from master/extras)
      - canonical product_name (if sku present in master)
    """
    rows = _read_all()
    for r in rows:
        r["sku_type"] = _sku_type(r.get("sku", ""))
        if r.get("sku"):
            r["product_name"] = _canonical_name_for_sku(r["sku"], r.get("product_name", ""))
    # newest first
    rows.sort(key=lambda r: (r.get("created_at") or "", r.get("awb") or ""), reverse=True)
    return rows

def _row_qty(row: Dict[str, Any]) -> int:
    try:
        return int(str(row.get("quantity", "")).strip() or "0")
    except ValueError:
        return 0

def _pid_list(raw: str) -> List[str]:
    if not raw:
        return []
    return [tok.strip().upper() for tok in str(raw).split(",") if tok.strip()]

def _pid_list_to_str(pids: List[str]) -> str:
    return ",".join(pids)

def _row_done_units(row: Dict[str, Any]) -> int:
    return len(_pid_list(row.get("product_id", "")))

def _row_remaining_units(row: Dict[str, Any]) -> int:
    return max(0, _row_qty(row) - _row_done_units(row))

def _norm_key(r: Dict[str, Any]) -> str:
    """
    Dedup key: (awb, canonical product, canonical sku, qty).
    Canonical product name prevents duplicate-rows due to multiline/random names.
    """
    parts = [
        _norm(r.get("awb", "")).upper(),
        _canonical_name_for_sku(r.get("sku", ""), _norm(r.get("product_name", ""))).lower(),
        _normalize_sku_for_db(r.get("sku", "")),
        str(r.get("quantity", "")).strip(),
    ]
    return hashlib.sha1(("||".join(parts)).encode("utf-8")).hexdigest()

def upsert_orders(rows: List[Dict[str, Any]], source_file: str | None = None) -> int:
    """
    Insert rows that are not already present (by _norm_key).
    Normalizes SKU and product_name via master.
    """
    if not rows:
        return 0
    db_rows = _read_all()
    existing = { _norm_key(r): True for r in db_rows }
    added, now = 0, _now_iso()

    for r in rows:
        sku_norm = _normalize_sku_for_db(r.get("sku", ""))
        prod_canon = _canonical_name_for_sku(sku_norm, _norm(r.get("product_name", "")))
        newr = {
            "order_date": _norm(r.get("order_date", "")),
            "order_id": _norm(r.get("order_id", "")),
            "customer_name": _norm(r.get("customer_name", "")),
            "contact_number": _norm(r.get("contact_number", "")),
            "product_name": prod_canon,
            "sku": sku_norm,
            "quantity": str(r.get("quantity", "")).strip(),
            "awb": _norm(r.get("awb", "")),
            "product_id": "",
            "row_id": "",
            "created_at": now,
            "source_file": source_file or _norm(r.get("source_file", "")),
            "page_index": str(r.get("page_index", "")).strip(),
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

# -------------------------
# Legacy (set same product_id on all rows for a given AWB)
# -------------------------
def assign_product_id(awb: str, product_id: str):
    awb, product_id = _norm(awb), _norm(product_id).upper()
    if not awb or not product_id:
        return False, "awb and product_id are required", 400

    rows = _read_all()
    idxs = [i for i, r in enumerate(rows) if _norm(r.get("awb", "")) == awb]
    if not idxs:
        return False, f"AWB '{awb}' not found", 404

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
        return True, f"product_id set '{product_id}' for AWB '{awb}' ({changed})", 200
    return True, "No rows needed update", 200

# -------------------------
# Barcode parsing & assignment (CSV masters drive behavior)
# -------------------------
# Device-style: AT0001-A001 (letter A-Z + 1..3 digits zero-padded to 3)
_BARCODE_RX_PRIMARY = re.compile(
    r"^\s*([A-Za-z]{2,}\d+)\s*-\s*([A-Za-z])\s*(\d{1,4})\s*$")
# Generic (loose): AT0100 or AT0100-ABC123 (suffix optional 1..10)
_BARCODE_RX_GENERIC = re.compile(r"^\s*([A-Za-z]{2,}\d+)(?:\s*-\s*([A-Za-z0-9]{1,10}))?\s*$")

def _product_id_exists_anywhere(pid: str, rows: List[Dict[str, str]]) -> bool:
    pid = _norm(pid).upper()
    if not pid:
        return False
    for r in rows:
        if pid in _pid_list(r.get("product_id", "")):
            return True
    return False

def _parse_barcode(code: str) -> Tuple[bool, Tuple[str, str] | str, int]:
    """
    Returns (ok, (sku4, pid)) or (False, error_message, 4xx)
    Rules (from master):
      - Compulsory/Unknown: must use device format ideally; we enforce uniqueness later.
      - Loose: accept bare SKU (auto-token) or with token.
      - NoScan: reject (should be in Extras UI).
    """
    code = (code or "").strip()

    # Device format
    m = _BARCODE_RX_PRIMARY.fullmatch(code)
    if m:
        sku_raw, letter, digits = m.groups()
        sku4 = _normalize_sku_for_db(sku_raw)
        pid = f"{letter.upper()}{digits.zfill(4)}"
        if _sku_type(sku4) == "NoScan":
            return False, "This SKU is configured as NoScan (Extras). Do not scan.", 409
        return True, (sku4, pid), 200

    # Generic (suffix optional)
    m2 = _BARCODE_RX_GENERIC.fullmatch(code)
    if m2:
        sku_raw, token = m2.groups()
        sku4 = _normalize_sku_for_db(sku_raw)
        t = _sku_type(sku4)
        if t == "NoScan":
            return False, "This SKU is configured as NoScan (Extras). Do not scan.", 409
        if t == "Loose":
            if token and token.strip():
                return True, (sku4, token.strip().upper()), 200
            auto = f"L{int(time.time()*1000)%1000000:06d}"  # e.g., L123456
            return True, (sku4, auto), 200
        # Compulsory / Unknown:
        if token:
            # User attempted generic token for device SKU
            return False, "Compulsory SKU requires device format (AT0001-A001).", 400
        return False, "Invalid barcode. Use AT0001-A001 or AT0100 / AT0100-ABC", 400

    return False, "Invalid barcode.", 400

def _contact_group_units(rows: List[Dict[str, str]], contact: str) -> Tuple[int, int]:
    """
    Completion across all SKU rows for a contact_number, but:
      - exclude rows with sku_type == 'NoScan'
      - exclude rows with blank SKU
    """
    c = _norm(contact)
    total = 0
    done = 0
    for r in rows:
        if _norm(r.get("contact_number", "")) != c:
            continue
        sku = _norm(r.get("sku", ""))
        if not sku:
            continue
        if _sku_type(sku) == "NoScan":
            continue
        total += _row_qty(r)
        done += _row_done_units(r)
    return done, total

def assign_barcode(barcode: str, active_awb: str | None = None) -> Tuple[bool, Dict[str, Any] | str, int]:
    """
    Parses barcode, finds a candidate row for the SKU with remaining units (optionally under active AWB),
    appends the token into product_id, enforces global uniqueness for Compulsory/Unknown SKUs.
    Returns progress + print_info when that contact's label completes.
    """
    ok, parsed, status = _parse_barcode((barcode or "").upper())
    if not ok:
        return False, parsed, status

    sku4, pid = parsed
    sku_type = _sku_type(sku4)

    rows = _read_all()

    # Global uniqueness for Compulsory/Unknown SKUs
    if sku_type in ("Compulsory", "Unknown") and _product_id_exists_anywhere(pid, rows):
        return False, f"Barcode {pid} already assigned", 409

    # Candidates: same SKU, with remaining units
    cand: List[Dict[str, str]] = []
    for r in rows:
        if _normalize_sku_for_db(r.get("sku", "")) != sku4:
            continue
        if _row_remaining_units(r) <= 0:
            continue
        cand.append(r)

    # If Active AWB provided, constrain to that
    if active_awb:
        awb = _norm(active_awb)
        cand = [r for r in cand if _norm(r.get("awb", "")) == awb]

    if not cand:
        suffix = f" under AWB {active_awb}" if active_awb else ""
        return False, f"No unassigned unit for SKU {sku4}{suffix}", 404

    # Preference: single-row contacts (by rows with a SKU), then first-fit
    sku_rows_all = [r for r in rows if _norm(r.get("sku", ""))]
    cnt_by_contact = Counter(_norm(r["contact_number"]) for r in sku_rows_all)
    singles = [r for r in cand if cnt_by_contact[_norm(r["contact_number"])] == 1 and _row_qty(r) == 1]
    target = singles[0] if singles else cand[0]

    # Append PID
    pids = _pid_list(target.get("product_id", ""))
    pids.append(pid)
    target["product_id"] = _pid_list_to_str(pids)
    _write_all(rows)

    # Compute completion for this contact
    contact = _norm(target["contact_number"])
    done, total = _contact_group_units(rows, contact)
    group_complete = (total > 0 and done >= total)

    # Gather print info when complete (source_file + page_index)
    src = page = ""
    if group_complete:
        grp_rows = [
            r for r in rows
            if _norm(r.get("contact_number", "")) == contact and _norm(r.get("sku", ""))
            and _sku_type(r.get("sku", "")) != "NoScan"
        ]
        if grp_rows:
            src = _norm(target.get("source_file", "")) or _norm(grp_rows[0].get("source_file", ""))
            page = _norm(target.get("page_index", "")) or _norm(grp_rows[0].get("page_index", ""))

    payload = {
        "message": f"Assigned {pid} \u2192 {sku4}",
        "contact_number": contact,
        "group_complete": group_complete,
        "awb": _norm(target.get("awb", "")),
        "row_progress": {"scanned": _row_done_units(target), "qty": _row_qty(target)},
        "group_progress": {"scanned": done, "qty": total},
        "print_info": {"source_file": src, "page_index": page} if (group_complete and src and page) else None,
        "sku_type": sku_type,
    }
    return True, payload, 200

# -------------------------
# Manual confirm for Extras (no-SKU or NoScan)
# -------------------------
def confirm_extra(row_id: str) -> Tuple[bool, str, int]:
    """
    Marks a row as confirmed shipped (only for: blank SKU OR sku_type == NoScan),
    by setting product_id = 'CONFIRMED_EXTRA'.
    """
    row_id = _norm(row_id)
    if not row_id:
        return False, "row_id required", 400

    rows = _read_all()
    for r in rows:
        if _norm(r.get("row_id", "")) == row_id:
            sku = _norm(r.get("sku", ""))
            if sku and _sku_type(sku) != "NoScan":
                return False, "Not eligible for manual confirm", 409
            r["product_id"] = "CONFIRMED_EXTRA"
            _write_all(rows)
            return True, "Confirmed and hidden", 200
    return False, "row_id not found", 404
