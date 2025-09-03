import fitz, re

pdf_path = "websiteorder.pdf"   # your file
doc = fitz.open(pdf_path)
text = doc.load_page(0).get_text("text")
doc.close()
# print(text)

lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
# print(lines)
# AWB
awb = next((m.group(1) for ln in lines
            if (m:=re.search(r"Courier AWB No:\s*(\d+)", ln, re.I))), None)

# Product + Qty (after Description / SKU / Qty headers)
product = qty = None
for i in range(len(lines)-3):
    if lines[i].lower()=="description" and lines[i+1].lower()=="sku" and lines[i+2].lower()=="qty":
        product = lines[i+3] if i+3 < len(lines) else None
        for j in range(i+4, min(i+8, len(lines))):
            if re.fullmatch(r"\d+", lines[j]):
                qty = int(lines[j]); break
        break

print({"awb": awb, "product_name": product, "quantity": qty})
