# COA File Extraction & ERP Integration Pipeline

A production-ready Python system that automates the ingestion, extraction, validation, and ERP upload of Certificate of Analysis (COA) documents for a manufacturing company. This pipeline eliminates manual reading of hundreds of COA files by converting unstructured documents into validated, searchable data inside the ERP system.

## Overview

Manufacturers receive hundreds of COA documents weekly, each containing critical information such as:

- microbial results  
- heavy metals  
- allergens  
- manufacturing/expiration dates  
- lot numbers  
- material specifications  

Traditionally, answering questions like “Does this product contain any allergens?” requires:

1. Identifying all raw materials in the finished good  
2. Locating ~5–15 COA files  
3. Manually reading each file  

This is slow, inconsistent, and unscalable.

### Solution

This pipeline automates the entire COA workflow:

- Automatically downloads supplier COAs  
- Extracts text with OCR  
- Parses structured fields using GPT-4o-mini  
- Validates & matches results to raw material codes  
- Uploads the final data into Deacom ERP  

Users can now retrieve material specifications instantly from within the ERP—without reading any documents manually.

## Workflow

```
1. Supplier COA Uploads (Smartsheet)
      ↓  (coa_automation.py – Selenium)
2. File Collection → ZIP Archive
      ↓  (coa_ocr.py – Google Cloud Vision)
3. OCR Extraction → Raw Text stored in Google Sheets
      ↓  (coa_data_extraction.py – GPT-4o-mini)
4. JSON Parsing (lot, dates, microbials, metals, allergens)
      ↓  (coa_data_validation.py)
5. Validation + RM Code Matching → Clean Dataset
      ↓  (coa_api.py / Deacom Web Automation)
6. Upload to Deacom ERP → Searchable COA Database
```
```

## Modules

| Module | Description |
|--------|-------------|
| `coa_ocr.py` | Extracts raw text from COA files using Google Cloud Vision OCR |
| `coa_data_extraction.py` | Parses OCR text into structured JSON using GPT-4o-mini |
| `coa_data_validation.py` | Validates data, matches with RM codes, extracts test results |
| `coa_api.py` | Handles Deacom ERP API integration |
| `coa_automation.py` | Browser automation for Smartsheet/Deacom using Selenium |

---

## Installation

### Prerequisites

- Python 3.8+
- Google Cloud account with Vision API enabled
- OpenAI API key
- Deacom API credentials
- Smartsheet account
- Chrome browser (for automation)

### Dependencies

```bash
pip install pymupdf python-docx mammoth striprtf
pip install google-cloud-vision google-auth google-api-python-client
pip install gspread oauth2client gspread-dataframe
pip install openai
pip install pandas numpy fuzzywuzzy python-Levenshtein
pip install selenium tqdm requests
```

---

## Configuration

Each module has a configuration section at the top. Update the following placeholders:

### Google Services
```python
SERVICE_ACCOUNT_FILE = "service_account.json"  # Path to your service account JSON
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
```

### OpenAI (coa_data_extraction.py)
```python
OPENAI_API_KEY = "sk-..."
```

### Deacom API (coa_data_validation.py, coa_api.py)
```python
DEACOM_API_USERNAME = "your-api-username"
DEACOM_API_PASSWORD = "your-api-password"
DEACOM_API_BASE_URL = "https://famehealthapi.deacomcloud.com"
```

### Smartsheet (coa_automation.py)
```python
SMARTSHEET_EMAIL = "your-email@example.com"
SMARTSHEET_PASSWORD = "your-password"
SMARTSHEET_URL = "https://app.smartsheet.com/sheets/YOUR_SHEET_ID"
```

### Deacom Web (coa_automation.py)
```python
DEACOM_USERNAME = "your-username"
DEACOM_PASSWORD = "your-password"
DEACOM_URL = "https://famehealth.deacomcloud.com/"
```

---

## Module Details

### 1. coa_ocr.py

Extracts raw text from COA documents using Google Cloud Vision API.

**Supported File Types:**
- PDF
- JPG, JPEG, PNG, TIF, TIFF
- DOCX
- RTF

**Key Functions:**
- `extract_combined_text_from_file_local()` - OCR extraction for a single file
- `process_ocr()` - Main processing function for batch OCR
- `load_files_from_zip()` - Load file metadata from ZIP archive

**Usage:**
```python
from coa_ocr import process_ocr

result_df = process_ocr(SPREADSHEET_URL, ZIP_FILE_PATH)
```

**Output:** Raw OCR text stored in Google Sheets "Raw Data" worksheet

---

### 2. coa_data_extraction.py

Parses OCR text into structured JSON using GPT-4o-mini.

**Extracted Fields:**
- `lot_number` - Lot/batch number
- `general_info` - Product name, company, dates, etc.
- `test_results` - Array of test results with limits and methods
- `coa_sufficient_content` - Quality score (0-10)
- `shipping_details_sufficient_content` - Quality score (0-10)

**Key Functions:**
- `run_gpt4o_mini()` - Send prompt to GPT-4o-mini
- `process_extraction()` - Main extraction function

**Usage:**
```python
from coa_data_extraction import process_extraction

result_df = process_extraction(SPREADSHEET_URL, OPENAI_API_KEY)
```

**Output:** Structured JSON stored in Google Sheets "Extracted Data" worksheet

---

### 3. coa_data_validation.py

Validates extracted data and matches with Raw Material (RM) codes.

**Processing Steps:**
1. Parse lot numbers from JSON
2. Extract test results (heavy metals, microbials)
3. Match with RM codes from order data
4. Calculate name similarity scores
5. Apply quality filters
6. Rank and select best matches

**Extracted Test Results:**
| Chemical | Microbial |
|----------|-----------|
| Arsenic (ppm) | E. coli |
| Cadmium (ppm) | Salmonella |
| Chromium (ppm) | Staphylococcus aureus |
| Lead (ppm) | |
| Mercury (ppm) | |
| Total Plate Count | |
| Yeast and Mold | |

**Quality Filters:**
- Name similarity >= 50% (matched) OR >= 80% (unmatched)
- COA sufficient content score >= 8

**Key Functions:**
- `extract_chem_info()` / `extract_micro_info()` - Extract test results
- `find_best_match()` - Fuzzy matching for RM codes
- `process_validation()` - Main validation function

**Usage:**
```python
from coa_data_validation import process_validation

result_df = process_validation(SPREADSHEET_URL)
```

**Output:** Excel file with validated data ready for Deacom upload

---

### 4. coa_automation.py

Browser automation for Smartsheet and Deacom using Selenium.

**Smartsheet Automation:**
- Login to Smartsheet
- Apply saved filters (e.g., "L3M Data")
- Download all attachments as ZIP

**Deacom Automation:**
- Login to Deacom ERP
- Download "Lots Received" reports
- Upload price updates in batches

**Key Classes:**
- `SmartsheetAutomation` - Smartsheet browser automation
- `DeacomAutomation` - Deacom ERP browser automation

**Usage:**
```python
from coa_automation import (
    download_coa_files_from_smartsheet,
    download_purchasing_report_from_deacom,
    upload_price_updates_to_deacom
)

# Download COA files from Smartsheet
download_coa_files_from_smartsheet()

# Download purchasing report
download_purchasing_report_from_deacom()

# Upload price updates
upload_price_updates_to_deacom("path/to/file.xlsx")
```

---

## Complete Workflow

```python
# Step 1: Download COA files from Smartsheet
from coa_automation import download_coa_files_from_smartsheet
download_coa_files_from_smartsheet()

# Step 2: Run OCR on downloaded files
from coa_ocr import process_ocr
process_ocr(SPREADSHEET_URL, ZIP_FILE_PATH)

# Step 3: Extract structured data using GPT
from coa_data_extraction import process_extraction
process_extraction(SPREADSHEET_URL, OPENAI_API_KEY)

# Step 4: Validate and match with RM codes
from coa_data_validation import process_validation
process_validation(SPREADSHEET_URL)

# Step 5: Upload validated data to Deacom
from coa_automation import upload_price_updates_to_deacom
upload_price_updates_to_deacom(OUTPUT_FILE)
```

---

## Google Sheets Structure

### Raw Data Worksheet
| Column | Description |
|--------|-------------|
| filename | Original file name |
| extension | File extension |
| filename_clean | Filename without extension |
| rank | Processing rank |
| raw_text | OCR extracted text |

### Extracted Data Worksheet
| Column | Description |
|--------|-------------|
| filename | Original file name |
| extension | File extension |
| filename_clean | Filename without extension |
| rank | Processing rank |
| json_values | Structured JSON from GPT |

---

## Output Format

The final output Excel file contains:

| Column | Deacom Field |
|--------|--------------|
| pr_codenum | Part number |
| u_Arsenic_PPM | Arsenic content |
| u_Cadmium_PPM | Cadmium content |
| u_Chromium | Chromium content |
| u_Lead_PPM | Lead content |
| u_Mercury_PPM | Mercury content |
| u_Total_Plate_Count | Total plate count |
| u_Yeast_and_Mold | Yeast and mold count |
| u_Escherichia_Coli | E. coli result |
| u_Salmonella | Salmonella result |
| u_Staphylococcus_Aureus | Staph aureus result |

---
