try:
    from PIL import Image
except ImportError:
    import Image
import pytesseract

# If you don't have tesseract executable in your PATH, include the following:
#pytesseract.pytesseract.tesseract_cmd = r'<full_path_to_your_tesseract_executable>'
# Example tesseract_cmd = r'C:\Program Files (x86)\Tesseract-OCR\tesseract'

# Simple image to string
print(pytesseract.image_to_string(Image.open('jpg3.jpg')))

# French text image to string
print(pytesseract.image_to_string(Image.open('jpg3.jpg'), lang='chi_sim'))
#testing

# In order to bypass the image conversions of pytesseract, just use relative or absolute image path
# NOTE: In this case you should provide tesseract supported images or tesseract will return error
#print(pytesseract.image_to_string('jpg3.jpg'))

# Batch processing with a single file containing the list of multiple image file paths
#print(pytesseract.image_to_string('images.txt'))

# Get bounding box estimates
#print(pytesseract.image_to_boxes(Image.open('test.png')))

# Get verbose data including boxes, confidences, line and page numbers
#print(pytesseract.image_to_data(Image.open('test.png')))

# Get information about orientation and script detection
#print(pytesseract.image_to_osd(Image.open('test.png')))

# Get a searchable PDF
#pdf = pytesseract.image_to_pdf_or_hocr('test.png', extension='pdf')

# Get HOCR output
#hocr = pytesseract.image_to_pdf_or_hocr('test.png', extension='hocr')
