import PyPDF2
import fitz

file="websiteorder2.pdf"
# pdf_reader = PyPDF2.PdfReader(file)
# num_pages = len(pdf_reader.pages)
# pages = pdf_reader.pages
# num_pages = len(pages)
# for i in range(num_pages):
#     page = pages[i]
#     text = page.extract_text()
#     print(text)
    
doc = fitz.open(file)
lines_all = []
for p in range(16):
    t = doc.load_page(p).get_text("text")
    print(t)
doc.close()