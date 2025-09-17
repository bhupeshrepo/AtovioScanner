# db.py
from __future__ import annotations
import csv, os, time, hashlib, re
from typing import List, Dict, Any, Tuple
from collections import Counter

DB_PATH = os.path.join(os.path.dirname(__file__), "orders_db.csv")

HEADERS = [
    "order_date","customer_name","contact_number","product_name","sku",
    "quantity","awb","product_id","row_id","created_at","source_file",
    "page_index"  # NEW
]

# ---------- helpers ----------
def _now_iso() -> str: return time.strftime("%Y-%m-%d %H:%M:%S")
def _norm(s) -> str: return (s or "").strip()

def _normalize_sku_for_db(raw: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]", "", (raw or "").upper())
    m = re.match(r"^([A-Z]+)(\d+)$", s)
    if not m: return s
    prefix, digits = m.groups()
    tail = digits[-4:] if len(digits) >= 4 else digits.zfill(4)
    return f"{prefix}{tail}"

def _pid_list(raw: str) -> List[str]:
    if not raw: return []
    return [tok.strip().upper() for tok in raw.split(",") if tok.strip()]

def _pid_list_to_str(pids: List[str]) -> str: return ",".join(pids)

def _row_qty(row: Dict[str, Any]) -> int:
    try: return int(str(row.get("quantity","")).strip() or "0")
    except ValueError: return 0

def _row_done_units(row: Dict[str, Any]) -> int:
    return len(_pid_list(row.get("product_id","")))

def _row_remaining_units(row: Dict[str, Any]) -> int:
    return max(0, _row_qty(row) - _row_done_units(row))

def _norm_key(r: Dict[str, Any]) -> str:
    parts = [
        _norm(r.get("awb", "")).upper(),
        _norm(r.get("product_name", "")).lower(),
        _normalize_sku_for_db(r.get("sku", "")),
        str(r.get("quantity", "")).strip(),
    ]
    return hashlib.sha1(("||".join(parts)).encode("utf-8")).hexdigest()

def _read_all() -> List[Dict[str, str]]:
    if not os.path.exists(DB_PATH): return []
    rows: List[Dict[str, str]] = []
    with open(DB_PATH, "r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            rows.append({k: r.get(k, "") for k in HEADERS})
    return rows

def _write_all(rows: List[Dict[str, Any]]) -> None:
    tmp = DB_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS); w.writeheader()
        for r in rows: w.writerow({h: r.get(h, "") for h in HEADERS})
    os.replace(tmp, DB_PATH)

# ---------- public API ----------
def init_db(path: str | None = None) -> None:
    global DB_PATH
    if path: DB_PATH = path
    if not os.path.exists(DB_PATH):
        with open(DB_PATH, "w", encoding="utf-8", newline="") as f:
            csv.DictWriter(f, fieldnames=HEADERS).writeheader()

def get_all() -> List[Dict[str, str]]:
    rows = _read_all()
    rows.sort(key=lambda r: (r.get("created_at") or "", r.get("awb") or ""), reverse=True)
    return rows

def upsert_orders(rows: List[Dict[str, Any]], source_file: str | None = None) -> int:
    if not rows: return 0
    db_rows = _read_all()
    existing = { _norm_key(r): True for r in db_rows }
    added, now = 0, _now_iso()
    for r in rows:
        newr = {
            "order_date": _norm(r.get("order_date", "")),
            "customer_name": _norm(r.get("customer_name", "")),
            "contact_number": _norm(r.get("contact_number", "")),
            "product_name": _norm(r.get("product_name", "")),
            "sku": _normalize_sku_for_db(r.get("sku", "")),
            "quantity": str(r.get("quantity", "")).strip(),
            "awb": _norm(r.get("awb", "")),
            "product_id": "",
            "row_id": "",
            "created_at": now,
            "source_file": source_file or "",
            "page_index": str(r.get("page_index", "")).strip(),
        }
        k = _norm_key(newr)
        if k in existing: continue
        newr["row_id"] = hashlib.md5(f"{k}|{now}".encode("utf-8")).hexdigest()[:12]
        db_rows.append(newr); existing[k] = True; added += 1
    if added: _write_all(db_rows)
    return added

def assign_product_id(awb: str, product_id: str):
    # legacy
    awb, product_id = _norm(awb), _norm(product_id).upper()
    if not awb or not product_id: return False, "awb and product_id are required", 400
    rows = _read_all()
    idxs = [i for i, r in enumerate(rows) if _norm(r.get("awb","")) == awb]
    if not idxs: return False, f"AWB '{awb}' not found", 404
    existing = { _norm(rows[i].get("product_id","")) for i in idxs if _norm(rows[i].get("product_id","")) }
    if existing and (len(existing) > 1 or (len(existing)==1 and product_id not in existing)):
        return False, f"Conflicting product_id already set for AWB '{awb}'", 409
    changed = 0
    for i in idxs:
        if not _norm(rows[i].get("product_id","")):
            rows[i]["product_id"] = product_id; changed += 1
    if changed: _write_all(rows); return True, f"product_id set '{product_id}' for AWB '{awb}' ({changed})", 200
    return True, "No rows needed update", 200

# ---------- Barcode assignment (multi-qty; devices unique; loose not unique) ----------
PRIMARY_SKUS = {"AT0001","AT0002","AT0003","AT0004"}
LOOSE_SKUS   = {"AT0020","AT0021","AT0022"}  # now scannable & kept in with-SKU table

_BARCODE_RX_PRIMARY = re.compile(r"^\s*([A-Za-z]{2,}\d+)\s*-\s*([A-Za-z])\s*(\d{1,3})\s*$")
_BARCODE_RX_GENERIC = re.compile(r"^\s*([A-Za-z]{2,}\d+)\s*-\s*([A-Za-z0-9]{1,10})\s*$")
_BARCODE_RX_PRIMARY = re.compile(r"^\s*([A-Za-z]{2,}\d+)\s*-\s*([A-Za-z])\s*(\d{1,3})\s*$")
_BARCODE_RX_GENERIC = re.compile(r"^\s*([A-Za-z]{2,}\d+)\s*-\s*([A-Za-z0-9]{1,10})\s*$")
_BARCODE_RX_SKUONLY = re.compile(r"^\s*([A-Za-z]{2,}\d+)\s*$")  # e.g., AT0020

def _parse_barcode(code: str) -> Tuple[bool, str | Tuple[str, str], int]:
    code = (code or "").strip()
    m = _BARCODE_RX_PRIMARY.fullmatch(code)
    if m:
        sku_raw, letter, digits = m.groups()
        sku_4d = _normalize_sku_for_db(sku_raw)
        pid_canon = f"{letter.upper()}{digits.zfill(3)}"
        return True, (sku_4d, pid_canon), 200
    m2 = _BARCODE_RX_GENERIC.fullmatch(code)
    if m2:
        sku_raw, token = m2.groups()
        sku_4d = _normalize_sku_for_db(sku_raw)
        return True, (sku_4d, token.upper()), 200
    m3 = _BARCODE_RX_SKUONLY.fullmatch(code)
    if m3:
        sku_raw = m3.group(1)
        sku_4d = _normalize_sku_for_db(sku_raw)
        if sku_4d in LOOSE_SKUS:
            # Use the SKU itself as the unit marker; no global uniqueness enforced
            return True, (sku_4d, sku_4d), 200
        # For devices, still require a token (e.g., -A001)
        return False, "Device barcodes need a token like 'AT0001-A001'.", 400
    return False, "Invalid barcode. Try 'AT0001-A001', 'AT0020-ABC123', or just 'AT0020' for loose items.", 400

def _product_id_exists_anywhere(pid: str, rows: List[Dict[str, str]]) -> bool:
    pid = _norm(pid).upper()
    if not pid: return False
    for r in rows:
        if pid in _pid_list(r.get("product_id","")):
            return True
    return False

def _contact_group_units(rows: List[Dict[str, str]], contact: str) -> Tuple[int,int]:
    """All SKU rows (including loose) count toward shipment completeness."""
    c = _norm(contact); total = done = 0
    for r in rows:
        if _norm(r.get("contact_number","")) != c: continue
        if not _norm(r.get("sku","")):              continue  # no-SKU rows do not block
        total += _row_qty(r)
        done  += _row_done_units(r)
    return done, total

def assign_barcode(barcode: str, active_awb: str | None = None) -> Tuple[bool, Dict[str, Any] | str, int]:
    ok, parsed, status = _parse_barcode((barcode or "").upper())
    if not ok: return False, parsed, status
    sku_4d, pid = parsed

    rows = _read_all()

    # Devices must be unique globally; loose can repeat
    is_loose = _normalize_sku_for_db(sku_4d) in LOOSE_SKUS
    if not is_loose and _product_id_exists_anywhere(pid, rows):
        return False, f"Barcode {pid} already assigned", 409

    # Candidates: SKU matches, units remaining
    cand = []
    for r in rows:
        if _normalize_sku_for_db(r.get("sku","")) != sku_4d: continue
        if _row_remaining_units(r) <= 0: continue
        cand.append(r)
    if active_awb:
        awb = _norm(active_awb)
        cand = [r for r in cand if _norm(r.get("awb","")) == awb]
    if not cand:
        suffix = f" under AWB {active_awb}" if active_awb else ""
        return False, f"No unassigned unit for SKU {sku_4d}{suffix}", 404

    # Prefer single-row, qty=1 contacts first (by ROWS; all SKUs count)
    sku_rows = [r for r in rows if _norm(r.get("sku",""))]
    cnt_by_contact = Counter(_norm(r["contact_number"]) for r in sku_rows)
    singles = [r for r in cand if cnt_by_contact[_norm(r["contact_number"])] == 1 and _row_qty(r) == 1]
    target = singles[0] if singles else cand[0]

    # Append PID (even for loose; may repeat)
    pids = _pid_list(target.get("product_id",""))
    pids.append(pid)
    target["product_id"] = _pid_list_to_str(pids)
    _write_all(rows)

    contact = _norm(target["contact_number"])
    done, total = _contact_group_units(rows, contact)
    group_complete = (total > 0 and done >= total)

    # pick the group's page+file (assume same page/file for all rows in a label)
    src = ""
    page = ""
    if group_complete:
        grp_rows = [r for r in rows if _norm(r.get("contact_number","")) == contact and _norm(r.get("sku",""))]
        if grp_rows:
            # prefer target's own values; fallback to first row in group
            src = _norm(target.get("source_file","")) or _norm(grp_rows[0].get("source_file",""))
            page = _norm(target.get("page_index","")) or _norm(grp_rows[0].get("page_index",""))

    payload = {
        "message": f"Assigned {pid} → {sku_4d}",
        "contact_number": contact,
        "group_complete": group_complete,
        "awb": _norm(target.get("awb","")),
        "row_progress": {"scanned": _row_done_units(target), "qty": _row_qty(target)},
        "group_progress": {"scanned": done, "qty": total},
        "print_info": {"source_file": src, "page_index": page} if (group_complete and src and page) else None
    }
    return True, payload, 200


# ---------- Manual confirm for NO-SKU rows ----------
def confirm_extra(row_id: str) -> Tuple[bool, str, int]:
    row_id = _norm(row_id)
    if not row_id: return False, "row_id required", 400
    rows = _read_all()
    for r in rows:
        if _norm(r.get("row_id","")) == row_id:
            if _norm(r.get("sku","")):
                return False, "Not a no-SKU row", 409
            r["product_id"] = "CONFIRMED_EXTRA"
            _write_all(rows)
            return True, "Confirmed and hidden", 200
    return False, "row_id not found", 404
