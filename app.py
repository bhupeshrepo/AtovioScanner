import os, sys, io, logging
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename

# Ensure local imports resolve (db.py, parser.py in same folder)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from db import init_db, get_all, upsert_orders, assign_product_id, assign_barcode, DB_PATH  # make sure db.py has assign_barcode
from parser import parse_labels_from_pdf

# Optional: XLSX export utils
import openpyxl
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

app = Flask(
    __name__,
    static_folder=os.path.join(BASE_DIR, "static"),
    template_folder=os.path.join(BASE_DIR, "templates"),
)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("atovio")

init_db(DB_PATH)

def _json_error(message, code=400):
    log.error(message)
    return jsonify({"ok": False, "error": message}), code

@app.after_request
def nocache(resp):
    try:
        if request.path in ("/data", "/download_csv", "/download_xlsx"):
            resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            resp.headers["Pragma"] = "no-cache"
    except Exception:
        pass
    return resp

# ---------- Routes ----------
@app.get("/")
def index():
    return render_template("index.html")

@app.post("/upload")
def upload():
    if "file" not in request.files:
        return _json_error("No file provided", 400)
    f = request.files["file"]
    if not f or not f.filename:
        return _json_error("Empty filename", 400)
    if not f.filename.lower().endswith(".pdf"):
        return _json_error("Only PDF files are allowed", 415)
    try:
        os.chdir(BASE_DIR)
        safe_name = "uploads_" + secure_filename(os.path.basename(f.filename))
        f.save(safe_name)
        rows = parse_labels_from_pdf(safe_name)
        added = upsert_orders(rows, source_file=os.path.basename(safe_name))
        return jsonify({"ok": True, "parsed": len(rows), "added": added})
    except Exception as e:
        return _json_error(f"Failed to process PDF: {e}", 500)

@app.get("/data")
def data():
    try:
        rows = get_all()
        return jsonify(rows)
    except Exception as e:
        return _json_error(f"Failed to read data: {e}", 500)

@app.post("/assign")
def assign():
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception:
        return _json_error("Invalid JSON payload", 400)
    awb = (payload or {}).get("awb", "").strip()
    product_id = (payload or {}).get("product_id", "").strip()
    if not awb or not product_id:
        return _json_error("awb and product_id are required", 400)
    ok, msg, status = assign_product_id(awb, product_id)
    if not ok:
        return _json_error(msg, status)
    return jsonify({"ok": True, "message": msg})

@app.post("/assign_barcode")
def assign_barcode_route():
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception:
        return _json_error("Invalid JSON payload", 400)
    barcode = (payload or {}).get("barcode", "")
    active_awb = (payload or {}).get("awb", None)
    ok, result, status = assign_barcode(barcode, active_awb)
    if not ok:
        return _json_error(result, status)
    return jsonify({"ok": True, **result})

@app.get("/download_csv")
def download_csv():
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            data = f.read()
        mem = io.BytesIO(data.encode("utf-8"))
        mem.seek(0)
        return send_file(mem, mimetype="text/csv", as_attachment=True,
                         download_name=os.path.basename(DB_PATH))
    except Exception as e:
        return _json_error(f"Failed to download CSV: {e}", 500)

@app.get("/download_xlsx")
def download_xlsx():
    try:
        rows = get_all()
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "orders"
        headers = ["order_date","customer_name","contact_number","product_name","sku",
                   "quantity","awb","product_id","row_id","created_at","source_file"]
        ws.append(headers)
        for r in rows:
            ws.append([r.get(h, "") for h in headers])
        bold = Font(bold=True)
        for c in range(1, len(headers)+1):
            ws.cell(row=1, column=c).font = bold
            ws.column_dimensions[get_column_letter(c)].width = 22
        ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(rows)+1}"
        mem = io.BytesIO(); wb.save(mem); mem.seek(0)
        return send_file(mem,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True, download_name="orders_db.xlsx")
    except Exception as e:
        return _json_error(f"Failed to generate XLSX: {e}", 500)

@app.errorhandler(404)
def not_found(_):
    return _json_error("Route not found", 404)

@app.errorhandler(500)
def server_error(e):
    log.exception("Unhandled 500: %s", e)
    return _json_error("Server error", 500)

if __name__ == "__main__":
    os.chdir(BASE_DIR)
    app.run("127.0.0.1", 5000, debug=True)
