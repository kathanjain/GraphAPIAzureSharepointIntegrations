# Azure PDF Extractor DDR

This project extracts details from PDF files using Python.

## Project Structure
- `PDFExtractor/ExtractPDFDetails/` - Core extraction logic
- `PDFExtractor/test/` - Test cases

## Getting Started
1. Clone the repository
2. (Recommended) Create and activate a Python virtual environment:
   ```sh
   python -m venv venv
   venv\Scripts\activate  # On Windows
   # Or on macOS/Linux: source venv/bin/activate
   ```
3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
4. Run the extraction scripts or tests

## Usage

To start the Azure Function locally, open one terminal and run:
```sh
func start
```

In another terminal, you can run the test client:
```sh
python PDFExtractor/test/test_client.py
```

Update the test client or function code as needed for your use case.

## License
Specify your license here.
