"""
COA File Extraction - Data Extraction Module
Parses OCR text from COA files into structured JSON using GPT-4o-mini.
"""

import json
import pandas as pd
from tqdm import tqdm

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from openai import OpenAI

# =============================================================================
# CONFIGURATION - Replace with your own values
# =============================================================================
SERVICE_ACCOUNT_FILE = "service_account.json"
SPREADSHEET_URL = "SPREADSHEET_URL"
OPENAI_API_KEY = "OPENAI_API_KEY"

# Google API Scopes
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

# Processing settings
BATCH_SIZE = 20


# =============================================================================
# STRUCTURED PROMPT FOR GPT
# =============================================================================
STRUCTURED_PROMPT = """
You are an expert in parsing Certificates of Analysis (COAs). Given a COA, extract as much structured information as possible.

You must return a valid JSON object using only the information explicitly stated in the document. Do NOT guess, infer, or assume values. If a field is not clearly present in the document, OMIT it completely — do not fabricate, estimate, or include default values.

If the country of origin can be directly inferred from the company address (e.g., "Brazil"), include it. Otherwise, omit it.

Important rules:
- DO NOT guess values such as solvent, formulation_claim, or any allergens unless they are explicitly listed.
- DO NOT infer values like color, pH, strength, or dates — only extract them if clearly present.
- DO NOT generate company_latin_name unless a Romanized/Latin spelling of a non-Latin script (e.g., Chinese) is explicitly provided.
- If a numeric value (e.g. arsenic_ppm: 0.8) is visibly stated in the table or document, YOU MUST extract it — even if it's embedded in a table. Do not omit numeric test results that are present.
- If a value is clearly labeled but has no result, you may omit or leave the result field empty, but do not guess.

Return a JSON in the following format, using explicit types. Return null if you cannot find the value of this field:

{ "lot_number":
  "general_info": {
    "product_name": string,
    "product_code": string,
    "solvent": string,
    "loss_on_drying": float,                # % value, e.g., 1.5
    "strength": int,                        # in mg or as stated
    "formulation_claim": string,
    "color": string,
    "country_origin": string,
    "company": string,
    "company_website": string,
    "company_email": string,
    "company_phone": string,
    "company_address":string,
    "company_latin_name": string, // If the company name is written in a non-Latin script (e.g., Chinese), extract its Romanized or Latinized version, if present. This is not the product or botanical name.
    "part_used": string,
    "supplier_batch_size": float,
    "supplier_batch_size_unit": string,
    "supplier_batch_id": string,
    "manufacture_date": string (ISO format, e.g. "2024-04-04"),
    "expiry_date": string (ISO format),
    "appearance_and_color": string,
    "odor_and_taste": string,
    "mesh_size": string,
    "ash_content": float,                  # in %
    "packing": string,
    "storage": string
  },
  "test_results": [
    {
      "test": string,
      "unit": string,
      "limit": string,
      "method": string,
      "result": string  // Use the result exactly as stated in the document, or if no result is given, use the limit string.
    }
  ],
  "coa_sufficient_content": int               # Make a judgment: is the COA information in the document sufficient? give scoring from 0-10
  "shipping_details_sufficient_content": int               # Make a judgment: is the COA information in the document sufficient? give scoring from 0-10
}

Strictly follow this format. Output only valid JSON — no additional explanation or comments. Do not include any text before or after the JSON.
"""


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


def get_extracted_data_df(spreadsheet):
    """Load extracted data worksheet into DataFrame."""
    extracted_sheet = spreadsheet.worksheet("Extracted Data")
    extracted_data = extracted_sheet.get_all_values()
    return pd.DataFrame(extracted_data[1:], columns=extracted_data[0])


# =============================================================================
# GPT INTEGRATION
# =============================================================================
def get_gpt_client(api_key: str):
    """Initialize OpenAI client."""
    return OpenAI(api_key=api_key)


def run_gpt4o_mini(gpt_client, prompt: str) -> str:
    """
    Run GPT-4o-mini to extract structured data from COA text.

    Args:
        gpt_client: OpenAI client instance
        prompt: The full prompt including OCR text

    Returns:
        Extracted JSON string
    """
    response = gpt_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": "You are an expert in parsing Certificates of Analysis (COAs). Return only structured JSON."
            },
            {"role": "user", "content": prompt}
        ],
        temperature=0.3,
        max_tokens=2048
    )
    return response.choices[0].message.content.strip()


# =============================================================================
# DATA EXTRACTION
# =============================================================================
def process_extraction(spreadsheet_url: str, openai_api_key: str):
    """
    Main function to extract structured data from OCR text.

    Args:
        spreadsheet_url: URL of the Google Spreadsheet
        openai_api_key: OpenAI API key
    """
    # Initialize clients
    client = get_gspread_client()
    spreadsheet = get_spreadsheet(client, spreadsheet_url)
    gpt_client = get_gpt_client(openai_api_key)

    # Load data
    text_df = get_raw_data_df(spreadsheet)
    extracted_df = get_extracted_data_df(spreadsheet)

    # Find new files to process
    new_rows = text_df[~text_df['filename'].isin(extracted_df['filename'])]
    extracted_df = pd.concat([extracted_df, new_rows.drop(columns='raw_text')], ignore_index=True)

    # Update extracted sheet with new rows
    extracted_sheet = spreadsheet.worksheet("Extracted Data")
    set_with_dataframe(extracted_sheet, extracted_df)

    # Get sheet metadata
    extracted_data = extracted_sheet.get_all_values()
    extracted_headers = extracted_sheet.row_values(1)
    extracted_filename_col_index = extracted_headers.index("filename")
    extracted_text_col_index = extracted_headers.index("json_values")

    extracted_filename_to_row = {
        row[extracted_filename_col_index].strip(): idx
        for idx, row in enumerate(extracted_data[1:], start=2)
    }

    # Process files
    batch_updates = []

    for idx, row in tqdm(text_df.iterrows(), total=len(text_df), desc="Processing COA files"):
        filename = row['filename'].strip()

        # Skip if already processed
        match = extracted_df[extracted_df['filename'] == filename]
        if match.empty or (pd.notna(match.iloc[0]['json_values']) and str(match.iloc[0]['json_values']).strip() != ""):
            continue

        ocr_output = row['raw_text']

        # Run GPT extraction
        try:
            final_output = run_gpt4o_mini(gpt_client, STRUCTURED_PROMPT + ocr_output)
        except Exception as e:
            print(f"[ERROR] GPT failed for '{filename}': {e}")
            continue

        # Prepare for upload
        sheet_row = extracted_filename_to_row.get(filename)
        if sheet_row:
            final_json = f"'{final_output}" if final_output.strip().startswith('=') else final_output
            batch_updates.append((sheet_row, extracted_text_col_index + 1, final_json))
        else:
            print(f"[WARN] No matching row found for filename '{filename}'")

        # Upload in batches
        if len(batch_updates) >= BATCH_SIZE or idx == len(text_df) - 1:
            for row_num, col_num, value in batch_updates:
                try:
                    extracted_sheet.update_cell(row_num, col_num, value)
                except Exception as e:
                    print(f"[ERROR] Failed to upload to row {row_num}: {e}")
            batch_updates = []

    return extracted_df


def pretty_print_json(json_string: str) -> str:
    """Pretty print a JSON string."""
    parsed_json = json.loads(json_string)
    return json.dumps(parsed_json, indent=2, ensure_ascii=False)


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    # Run data extraction
    result_df = process_extraction(SPREADSHEET_URL, OPENAI_API_KEY)
    print(f"Processed {len(result_df)} files")
