import os, sys, io, logging, base64, fitz, subprocess, tempfile, platform
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename

PDFTOPRINTER = os.path.join(os.path.dirname(__file__), "bin", "PDFtoPrinter.exe")

# Ensure local imports resolve (db.py, parser.py in same folder)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# add to imports
from db import init_db, get_all, upsert_orders, assign_product_id, assign_barcode, confirm_extra, DB_PATH
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
@app.post("/label_print_silent")
def label_print_silent():
    """
    Body: { "source": "uploads_*.pdf", "page": 1, "printer": "Name of Printer" }
    Windows: uses PDFtoPrinter.exe /s to print silently to the specific printer.
    macOS/Linux fallback: uses `lp -d <printer>` if available.
    """
    try:
        data = request.get_json(force=True)
    except Exception:
        return _json_error("Invalid JSON", 400)

    src = (data or {}).get("source", "").strip()
    page = (data or {}).get("page", "")
    printer = (data or {}).get("printer", "").strip()
    if not (src and str(page).isdigit() and int(page) >= 1 and printer):
        return _json_error("source, page (>=1), printer are required", 400)

    if not src.lower().endswith(".pdf") or not src.startswith("uploads_"):
        return _json_error("Invalid source", 400)

    pdf_path = os.path.join(BASE_DIR, src)
    if not os.path.exists(pdf_path):
        return _json_error("Source file not found", 404)

    page_i = int(page) - 1
    try:
        # Extract 1 page to a temp PDF
        doc = fitz.open(pdf_path)
        if page_i >= len(doc):
            doc.close()
            return _json_error("Page out of range", 404)
        one = fitz.open()
        one.insert_pdf(doc, from_page=page_i, to_page=page_i)
        tmpdir = tempfile.mkdtemp(prefix="print_")
        one_pdf = os.path.join(tmpdir, "label.pdf")
        one.save(one_pdf); one.close(); doc.close()

        if platform.system() == "Windows":
            if not os.path.exists(PDFTOPRINTER):
                return _json_error("PDFtoPrinter.exe not found. Put it in ./bin", 500)
            # Silent, to specific printer
            # Syntax: PDFtoPrinter file.pdf "Printer Name" /s
            cmd = [PDFTOPRINTER, one_pdf, printer, "/s"]
        else:
            # Fallback: CUPS lp (best effort, may need printer set up)
            cmd = ["lp", "-d", printer, one_pdf]

        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            return _json_error(f"Print failed: {res.stderr or res.stdout or res.returncode}", 500)

        return jsonify({"ok": True, "message": f"Sent to printer '{printer}'"})
    except Exception as e:
        return _json_error(f"Render/print failed: {e}", 500)

@app.get("/label_print")
def label_print():
    """
    Render a single PDF page as PNG inside an HTML that auto-calls window.print().
    Query: ?source=<uploads_filename.pdf>&page=<1-based int>
    """
    src = (request.args.get("source") or "").strip()
    page = (request.args.get("page") or "").strip()

    # Basic safety: only allow files we created in /uploads_*.pdf
    if not src or not src.lower().endswith(".pdf") or not src.startswith("uploads_"):
        return _json_error("Invalid source", 400)
    try:
        page_i = int(page)
        assert page_i >= 1
    except Exception:
        return _json_error("Invalid page", 400)

    pdf_path = os.path.join(BASE_DIR, src)
    if not os.path.exists(pdf_path):
        return _json_error("File not found", 404)

    try:
        doc = fitz.open(pdf_path)
        if page_i > len(doc):
            doc.close()
            return _json_error("Page out of range", 404)
        pg = doc.load_page(page_i - 1)
        # Render ~300 DPI: scale 3 (72*3=216), adjust if you want finer print
        pix = pg.get_pixmap(matrix=fitz.Matrix(3, 3), alpha=False)
        img_b = pix.tobytes("png")
        doc.close()

        b64 = base64.b64encode(img_b).decode("ascii")
        html = f"""<!doctype html>
<html><head>
<meta charset="utf-8"><title>Print Label</title>
<style>
  html,body{{margin:0;background:#fff}}
  img{{width:100%;height:auto;display:block}}
  @media print{{ img{{ page-break-after:avoid }} }}
</style>
</head>
<body>
<img src="data:image/png;base64,{b64}" alt="label"/>
<script>
  window.onload = function(){{
    try {{ window.print(); }} catch(e) {{}}
  }};
</script>
</body></html>"""
        return html, 200, {"Content-Type":"text/html; charset=utf-8"}
    except Exception as e:
        return _json_error(f"Render failed: {e}", 500)

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

# add this route
@app.post("/confirm_extra")
def confirm_extra_route():
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception:
        return _json_error("Invalid JSON payload", 400)

    row_id = (payload or {}).get("row_id", "").strip()
    product_id = (payload or {}).get("product_id", "").strip()

    ok, msg, status = confirm_extra(row_id, product_id)
    if not ok:
        return _json_error(msg, status)
    return jsonify({"ok": True, "message": msg})



if __name__ == "__main__":
    os.chdir(BASE_DIR)
    app.run("127.0.0.1", 5000, debug=True)
