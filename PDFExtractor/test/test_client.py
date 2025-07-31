import requests

# Define the URL where the function is running
url = "http://localhost:7071/api/ExtractPDFDetails"

# Path to your test PDF file
pdf_path = "DDRReport_New-1-100.pdf"

try:
    with open(pdf_path, "rb") as pdf_file:
        files = {"file": pdf_file}  # Send as multipart/form-data
        response = requests.post(url, files=files)
    print("Response Status Code:", response.status_code)
    if response is not None and hasattr(response, "text"):
        print("Response Body:", response.text)
    else:
        print("No response body or response is None")
except FileNotFoundError:
    print(f"File not found: {pdf_path}")
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")