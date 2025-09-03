import csv, hashlib, os, time, json, re
from typing import List, Dict, Tuple

DB_PATH = "orders_db.csv"
INVENTORY_MAP = "inventory_rules.json"  # NEW
FIELDS = [
    "order_date","customer_name","contact_number","product_name","sku",
    "quantity","awb","product_id","row_id","created_at","source_file"
]
DELIM = "|"

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _row_id(row: Dict) -> str:
    key = f"{row.get('order_date','')}|{row.get('customer_name','')}|{row.get('contact_number','')}|" \
          f"{row.get('product_name','')}|{row.get('sku','')}|{row.get('quantity','')}|{row.get('awb','')}"
    return _sha1(key)

def _file_needs_header(path: str) -> bool:
    return (not os.path.exists(path)) or os.path.getsize(path) == 0

def _atomic_write(path: str, rows: List[Dict]):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            r = {**r}
            try:
                r["quantity"] = int(r.get("quantity") or 0)
            except Exception:
                r["quantity"] = 0
            w.writerow(r)
    os.replace(tmp, path)

def init_db(path: str = DB_PATH):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if _file_needs_header(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writeheader()

def get_all(path: str = DB_PATH) -> List[Dict]:
    init_db(path)
    out: List[Dict] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                row["quantity"] = int(row.get("quantity") or 0)
            except Exception:
                row["quantity"] = 0
            out.append(row)
    return out

def upsert_orders(rows_in: List[Dict], source_file: str = "", path: str = DB_PATH) -> int:
    init_db(path)
    existing = {row["row_id"]: row for row in get_all(path)}
    ts = time.strftime("%Y-%m-%d %H:%M:%S")

    added = 0
    need_header = _file_needs_header(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if need_header: w.writeheader()
        for r in rows_in:
            base = {
                "order_date": r.get("order_date",""),
                "customer_name": r.get("customer_name",""),
                "contact_number": r.get("contact_number",""),
                "product_name": r.get("product_name",""),
                "sku": r.get("sku",""),
                "quantity": int(r.get("quantity") or 0),
                "awb": r.get("awb",""),
                "product_id": "",
                "created_at": ts,
                "source_file": source_file,
            }
            rid = _row_id(base)
            base["row_id"] = rid
            if rid not in existing:
                w.writerow(base)
                existing[rid] = base
                added += 1
    return added

def _load_inventory_rules() -> Dict[str, List[str]]:
    if not os.path.exists(INVENTORY_MAP):
        return {}
    with open(INVENTORY_MAP, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Normalize to list of regex strings per SKU or "*"
    norm = {}
    for k, v in data.items():
        if isinstance(v, str): norm[k] = [v]
        elif isinstance(v, list): norm[k] = [str(x) for x in v]
    return norm

def _pid_allowed_for_sku(product_id: str, sku: str, rules: Dict[str, List[str]]) -> bool:
    # 1) SKU-specific rules; 2) fallback "*" rules; 3) allow-all if no rules
    plist = rules.get(sku) or rules.get("*")
    if not plist: return True
    for pat in plist:
        try:
            if re.fullmatch(pat, product_id): return True
        except re.error:
            # Treat as prefix if invalid regex
            if product_id.startswith(pat): return True
    return False

def assign_product_id(awb: str, product_id: str, path: str = DB_PATH) -> Tuple[bool, str, int]:
    rows = get_all(path)
    rules = _load_inventory_rules()
    changed = False

    for row in rows:
        if row.get("awb","") != awb: continue
        qty = int(row.get("quantity") or 0)
        pid_cell = (row.get("product_id","") or "").strip()
        assigned_list = [x for x in pid_cell.split(DELIM) if x.strip()] if pid_cell else []

        # Inventory validation against SKU
        sku = row.get("sku","") or ""
        if not _pid_allowed_for_sku(product_id, sku, rules):
            return (False, f"product_id '{product_id}' not allowed for SKU '{sku}'", 403)

        if product_id in assigned_list:
            return (False, "This product_id is already assigned for this AWB", 409)

        if len(assigned_list) < qty:
            assigned_list.append(product_id)
            row["product_id"] = DELIM.join(assigned_list)
            changed = True
            break

    if not changed:
        return (False, "No matching row with remaining quantity for this AWB", 409)

    _atomic_write(path, rows)
    return (True, "Assigned", 200)
