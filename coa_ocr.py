"""
COA File Extraction - OCR Module
Extracts raw text from Certificate of Analysis (COA) files using Google Cloud Vision OCR.
"""

import io
import os
import zipfile
import pandas as pd
from tqdm import tqdm
from urllib.parse import quote

import fitz  # PyMuPDF
from PIL import Image, ImageDraw
import mammoth
import tempfile
from striprtf.striprtf import rtf_to_text

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from google.cloud import vision_v1
from googleapiclient.discovery import build

# =============================================================================
# CONFIGURATION - Replace with your own values
# =============================================================================
SERVICE_ACCOUNT_FILE = "service_account.json"
SPREADSHEET_URL = "SPREADSHEET_URL"
ZIP_FILE_PATH = r"Path\COA Files.zip"

# Google API Scopes
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

# Processing settings
BATCH_SIZE = 100
MAX_CELL_CHARS = 49000


# =============================================================================
# GOOGLE SHEETS SETUP
# =============================================================================
def get_gspread_client():
    """Initialize and return authorized gspread client."""
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
    return gspread.authorize(creds)


def get_spreadsheet(client, url):
    """Open spreadsheet by URL."""
    return client.open_by_url(url)


def get_raw_data_df(spreadsheet):
    """Load raw data worksheet into DataFrame."""
    raw_sheet = spreadsheet.worksheet("Raw Data")
    raw_data = raw_sheet.get_all_values()
    return pd.DataFrame(raw_data[1:], columns=raw_data[0])


# =============================================================================
# GOOGLE VISION SETUP
# =============================================================================
def get_vision_client():
    """Initialize Google Cloud Vision client."""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return vision_v1.ImageAnnotatorClient(credentials=creds)


# =============================================================================
# FILE PROCESSING UTILITIES
# =============================================================================
def render_text_to_image(text: str) -> bytes:
    """Render text to an image for OCR processing."""
    img = Image.new("RGB", (2480, 3508), color="white")  # A4 size
    draw = ImageDraw.Draw(img)
    draw.text((100, 100), text, fill="black")
    output = io.BytesIO()
    img.save(output, format="PNG")
    return output.getvalue()


def extract_combined_text_from_file_local(file_bytes: bytes, file_name: str, vision_client) -> str:
    """
    Extract text from a file using Google Cloud Vision OCR.

    Supports: PDF, JPG, JPEG, PNG, TIF, TIFF, DOCX, RTF
    """
    combined_text = ""
    file_ext = file_name.split('.')[-1].lower()

    def run_vision_on_images(image_list):
        nonlocal combined_text
        for i, img_bytes in enumerate(image_list):
            image = vision_v1.Image(content=img_bytes)
            response = vision_client.document_text_detection(image=image)
            if response.error.message:
                print(f"Vision error on image {i + 1}: {response.error.code} / {response.error.message}")
                continue
            combined_text += f"\n=== OCR Text (Page {i + 1}) ===\n"
            combined_text += response.full_text_annotation.text.strip() + "\n"

    if file_ext == "pdf":
        pdf_doc = fitz.open(stream=io.BytesIO(file_bytes), filetype="pdf")
        images = [page.get_pixmap(dpi=300).tobytes("png") for page in pdf_doc]
        run_vision_on_images(images)

    elif file_ext in ["jpg", "jpeg", "png", "tif", "tiff"]:
        run_vision_on_images([file_bytes])

    elif file_ext == "docx":
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "doc.docx")
            with open(path, "wb") as f:
                f.write(file_bytes)
            with open(path, "rb") as f:
                result = mammoth.extract_raw_text(f)
            rendered_image = render_text_to_image(result.value)
            run_vision_on_images([rendered_image])

    elif file_ext == "rtf":
        text = rtf_to_text(file_bytes.decode("utf-8", errors="ignore"))
        rendered_image = render_text_to_image(text)
        run_vision_on_images([rendered_image])

    else:
        print(f"Unsupported file type: {file_ext}")

    return combined_text.strip()


def escape_formula_like(text):
    """Escape text that looks like a formula for Google Sheets."""
    if isinstance(text, str) and text.strip().startswith(('=', '+', '-', '@')):
        return "'" + text
    return text


# =============================================================================
# ZIP FILE PROCESSING
# =============================================================================
def load_files_from_zip(zip_path: str) -> pd.DataFrame:
    """Load file information from a ZIP archive."""
    file_info_list = []

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file in zip_ref.infolist():
            if file.is_dir():
                continue

            filename = os.path.basename(file.filename)
            if not filename:
                continue

            extension = filename.split('.')[-1] if '.' in filename else ''
            file_info_list.append({
                "filename": filename,
                "extension": extension
            })

    df = pd.DataFrame(file_info_list)
    df['filename_clean'] = df['filename'].apply(
        lambda x: x.rsplit('.', 1)[0] if '.' in x else x
    )
    return df


# =============================================================================
# MAIN OCR PROCESSING
# =============================================================================
def process_ocr(spreadsheet_url: str, zip_path: str):
    """
    Main function to process COA files with OCR.

    Args:
        spreadsheet_url: URL of the Google Spreadsheet
        zip_path: Path to the ZIP file containing COA files
    """
    # Initialize clients
    client = get_gspread_client()
    spreadsheet = get_spreadsheet(client, spreadsheet_url)
    vision_client = get_vision_client()

    # Load existing data
    text_df = get_raw_data_df(spreadsheet)

    # Load new files from ZIP
    new_data = load_files_from_zip(zip_path)

    # Find new files not in existing data
    new_rows = new_data[~new_data['filename'].isin(text_df['filename'])]
    text_df = pd.concat([text_df, new_rows], ignore_index=True)
    text_df.reset_index(drop=True, inplace=True)

    # Escape formula-like text
    text_df["raw_text"] = text_df["raw_text"].apply(escape_formula_like)

    # Get sheet metadata for updates
    raw_sheet = spreadsheet.worksheet("Raw Data")
    raw_headers = raw_sheet.row_values(1)
    raw_filename_col_index = raw_headers.index("filename")
    raw_text_col_index = raw_headers.index("raw_text")
    raw_data = raw_sheet.get_all_values()

    raw_filename_to_row = {
        row[raw_filename_col_index].strip(): idx
        for idx, row in enumerate(raw_data[1:], start=2)
    }

    # Process files
    pending_updates = []
    zip_archive = zipfile.ZipFile(zip_path, 'r')
    zip_filenames = set(zip_archive.namelist())

    for i in tqdm(range(1, len(text_df)), desc="Processing OCR"):
        if pd.isna(text_df.at[i, 'raw_text']) or text_df.at[i, 'raw_text'].strip() == '':
            filename = text_df.loc[i, 'filename']
            filename = filename.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
            filename = filename.replace("'", "\\'")

            if filename not in zip_filenames:
                print(f"[{i}] File '{filename}' not found in ZIP.")
                continue

            try:
                with zip_archive.open(filename) as f:
                    file_bytes = f.read()

                combined_text = extract_combined_text_from_file_local(
                    file_bytes, filename, vision_client
                )
                text_df.at[i, 'raw_text'] = combined_text

                # Prepare Google Sheets update
                sheet_row = raw_filename_to_row.get(filename)
                if sheet_row is None:
                    print(f"[{i}] Warning: sheet row not found for '{filename}'")
                    continue

                sheet_col = raw_text_col_index + 1
                safe_text = combined_text[:MAX_CELL_CHARS]
                safe_text = f"'{safe_text}" if safe_text.strip().startswith("=") else safe_text

                pending_updates.append((sheet_row, sheet_col, safe_text))

                # Upload in batches
                if len(pending_updates) >= BATCH_SIZE:
                    for row, col, val in pending_updates:
                        raw_sheet.update_cell(row, col, val)
                    pending_updates = []

            except Exception as e:
                print(f"[{i}] Error processing '{filename}': {e}")

    # Final flush of remaining rows
    for row, col, val in pending_updates:
        raw_sheet.update_cell(row, col, val)

    zip_archive.close()

    return text_df


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    # Run OCR processing
    result_df = process_ocr(SPREADSHEET_URL, ZIP_FILE_PATH)
    print(f"Processed {len(result_df)} files")
