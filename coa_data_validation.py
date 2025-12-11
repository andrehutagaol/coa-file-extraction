"""
COA File Extraction - Data Validation Module
Validates extracted COA data, matches with RM codes, and extracts test results.
"""

import json
import ast
import re
import os
import numpy as np
import pandas as pd
from typing import Any, Dict, List
from itertools import chain
from tqdm import tqdm
from fuzzywuzzy import fuzz

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import requests

# =============================================================================
# CONFIGURATION - Replace with your own values
# =============================================================================
SERVICE_ACCOUNT_FILE = "service_account.json"
SPREADSHEET_URL = "SPREADSHEET_URL"
DEACOM_API_USERNAME = "USERNAME"
DEACOM_API_PASSWORD = "PASSWORD"
DEACOM_API_BASE_URL = "URL"

PURCHASE_ORDER_FILE = r"Path\PurchaseOrderLots.xlsx"
OUTPUT_FILE = r"Path\rm_clean_filtered.xlsx"

# Google API Scopes
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]


# =============================================================================
# REGEX PATTERNS FOR TEST EXTRACTION
# =============================================================================
# Heavy metals and chemical tests
CHEM_PATTERNS = {
    'arsenic_ppm': r'\b(ars(?:enic)?|as ppm|as mg|as content)\b',
    'cadmium_ppm': r'\b(cadmium|cd)\b',
    'chromium_ppm': r'\bchromium\b',
    'lead_ppm': r'\b(lead|pb)\b',
    'mercury_ppm': r'\b(mercury|hg|mecury)\b',
    'total_plate_count': r'\b(plate|tpc|aerobic count|total aerobic|total count)\b',
    'yeast_and_mold': r'\b(yeast|yeaste|yeasts|mold|molds|mould|moulds)\b',
}

# Microbial tests
MICRO_PATTERNS = {
    'e_coli': r'\bcoli\b',
    'salmonella': r'\bsalmonella\b',
    'staphylococcus_aureus': r'\b(aureus|staph|staphylococcus)\b'
}

# Compile patterns
COMPILED_CHEM = {k: re.compile(v, re.IGNORECASE) for k, v in CHEM_PATTERNS.items()}
COMPILED_MICRO = {k: re.compile(v, re.IGNORECASE) for k, v in MICRO_PATTERNS.items()}


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


# =============================================================================
# JSON PARSING UTILITIES
# =============================================================================
def safe_parse(cell: Any) -> Dict[str, Any]:
    """
    Parse a Google-Sheets style JSON-looking string safely.
    Returns {} on any failure.
    """
    if pd.isna(cell):
        return {}

    text = str(cell).strip()

    # Remove the single quote Google Sheets prepends to escape formulas
    if text.startswith("'") and text[1:].lstrip().startswith("{"):
        text = text.lstrip("'")

    # First try normal JSON
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Some rows might be Python-repr strings
    try:
        return ast.literal_eval(text)
    except Exception:
        return {}


def clean_test_name(name: str) -> str:
    """
    Clean test name: lowercase, remove punctuation, collapse spaces.
    """
    s = name.lower()
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def clean_text(text: str) -> str:
    """Clean text: lowercase + remove special characters."""
    if pd.isnull(text):
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    return text.strip()


def clean_and_extract_lot(x) -> str:
    """Extract M-number (lot number) from text."""
    cleaned = re.sub(r'\W+', '', str(x))
    match = re.search(r'M\d{4,6}', cleaned)
    return match.group() if match else None


# =============================================================================
# TEST RESULT EXTRACTION
# =============================================================================
def extract_chem_info(data: dict, key: str) -> float:
    """Extract chemical test info (heavy metals, etc.)."""
    info = next((item for item in data.get('test_results', [])
                 if COMPILED_CHEM[key].search(item.get('test', ''))), None)
    if not info:
        return None

    result = info.get('result', '')
    limit = info.get('limit', '')

    # First priority: numeric result
    m = re.search(r'[\d]*\.?\d+', result)
    if m:
        return float(m.group())

    # Second: numeric limit
    m = re.search(r'[\d]*\.?\d+', limit)
    if m:
        return float(m.group())

    # Third: non-detectable result
    if re.search(r'\b(not detected|conform|conforms|negative|absent|none)\b', result.lower()):
        return 0.0

    return None


def extract_micro_info(data: dict, key: str) -> float:
    """Extract microbial test info."""
    info = next((item for item in data.get('test_results', [])
                 if COMPILED_MICRO[key].search(item.get('test', ''))), None)
    if not info:
        return None

    result = info.get('result', '') or ''

    # First priority: number before "cfu"
    m = re.search(r'[\d]*\.?\d+(?=\s*cfu)', result, re.IGNORECASE)
    if m:
        return float(m.group())

    # Second: non-detectable
    if re.search(r'\b(not detected|absent|none|negative|conform|conforms)\b', result.lower()):
        return 0.0

    return None


def extract_field(row, field: str, extractor_func) -> float:
    """Apply extraction function to a row."""
    try:
        data = json.loads(row['json_values'])
        return extractor_func(data, field)
    except Exception:
        return None


def extract_general_info(row) -> pd.Series:
    """Extract general info fields from JSON."""
    try:
        data = json.loads(row['json_values'])
        general_info = data.get("general_info", {})
        return pd.Series({
            "product_name": general_info.get("product_name"),
            "manufacture_date": general_info.get("manufacture_date"),
            "expiry_date": general_info.get("expiry_date")
        })
    except Exception:
        return pd.Series({"product_name": None, "manufacture_date": None, "expiry_date": None})


def extract_coa_sufficient_content(x) -> int:
    """Extract COA sufficient content score."""
    if pd.isna(x) or not str(x).strip():
        return None
    try:
        data = json.loads(x)
        return data.get('coa_sufficient_content')
    except json.JSONDecodeError:
        return None


# =============================================================================
# DEACOM API INTEGRATION
# =============================================================================
def get_access_token(username: str, password: str, base_url: str) -> str:
    """Get authentication token from Deacom API."""
    url = f"{base_url}/api/authenticate"
    data = {
        "Username": username,
        "Password": password
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/plain"
    }

    response = requests.post(url, data=data, headers=headers)
    response.raise_for_status()
    return response.json().get("access_token")


def get_raw_material_items(username: str, password: str, base_url: str) -> pd.DataFrame:
    """Fetch all raw material items from Deacom API."""
    token = get_access_token(username, password, base_url)

    all_items = []
    skip = 0
    take = 1000
    has_more = True

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    search_filters = {
        "active": True,
        "category": {"name": "Raw Materials"}
    }

    while has_more:
        params = {"skip": skip, "take": take}
        response = requests.post(
            f"{base_url}/api/inventory/item/search",
            json=search_filters,
            headers=headers,
            params=params
        )

        response.raise_for_status()
        results = response.json().get("results", [])

        if not results:
            has_more = False
        else:
            all_items.extend(results)
            skip += take

    df = pd.json_normalize(all_items)
    return df[['retailCode', 'description']].rename(
        columns={'retailCode': 'Part Number', 'description': 'Description'}
    )


# =============================================================================
# MATCHING FUNCTIONS
# =============================================================================
def find_best_match(product_name: str, rm_df: pd.DataFrame) -> pd.Series:
    """Find best matching RM from rm_df for a product name."""
    best_score = -1
    best_row = None

    for _, row in rm_df.iterrows():
        score = fuzz.token_set_ratio(product_name, str(row['Description']))
        if score > best_score:
            best_score = score
            best_row = row

    if best_row is not None:
        return pd.Series({
            'matched_part_number': best_row['Part Number'],
            'matched_description': best_row['Description'],
            'similarity_score': best_score
        })
    return pd.Series({
        'matched_part_number': None,
        'matched_description': None,
        'similarity_score': 0
    })


def custom_rank(group: pd.DataFrame) -> pd.DataFrame:
    """Custom ranking based on manufacture_date, then expiry_date, then row order."""
    group = group.copy()
    group['row_order'] = range(len(group))

    # Three partitions
    with_mfg = group[group['manufacture_date'].notna()]
    with_expiry = group[(group['manufacture_date'].isna()) & (group['expiry_date'].notna())]
    fallback = group[(group['manufacture_date'].isna()) & (group['expiry_date'].isna())]

    # Sort and assign ranks
    with_mfg = with_mfg.sort_values('manufacture_date', ascending=False)
    with_expiry = with_expiry.sort_values('expiry_date', ascending=False)
    fallback = fallback.sort_values('row_order')

    # Assign ranks
    with_mfg['rank'] = range(1, len(with_mfg) + 1)
    with_expiry['rank'] = range(len(with_mfg) + 1, len(with_mfg) + len(with_expiry) + 1)
    fallback['rank'] = range(len(with_mfg) + len(with_expiry) + 1, len(group) + 1)

    # Combine and drop helper column
    combined = pd.concat([with_mfg, with_expiry, fallback])
    return combined.drop(columns='row_order')


# =============================================================================
# MAIN DATA VALIDATION PIPELINE
# =============================================================================
def process_validation(spreadsheet_url: str):
    """
    Main function to validate and process extracted COA data.

    Args:
        spreadsheet_url: URL of the Google Spreadsheet
    """
    # Initialize clients
    client = get_gspread_client()
    spreadsheet = get_spreadsheet(client, spreadsheet_url)

    # Load raw and extracted data
    raw_sheet = spreadsheet.worksheet("Raw Data")
    raw_data = raw_sheet.get_all_values()
    text_df = pd.DataFrame(raw_data[1:], columns=raw_data[0])

    extracted_sheet = spreadsheet.worksheet("Extracted Data")
    extracted_data = extracted_sheet.get_all_values()
    extracted_df = pd.DataFrame(extracted_data[1:], columns=extracted_data[0])

    # ==========================================================================
    # STEP 1: Parse lot numbers from JSON
    # ==========================================================================
    extracted_df["lot_numbers"] = (
        extracted_df["json_values"]
        .apply(safe_parse)
        .apply(lambda d: d.get("lot_number") or d.get("lot_numbers") or [0])
    )

    # Explode lot numbers
    extracted_df2 = (
        extracted_df
        .explode("lot_numbers")
        .rename(columns={"lot_numbers": "lot_number"})
        .dropna(subset=["lot_number"])
        .reset_index(drop=True)
    )

    # Clean lot numbers
    extracted_df2['lot_number'] = extracted_df2['lot_number'].apply(clean_and_extract_lot)

    # Extract M-numbers from filename
    extracted_df2["m_numbers"] = (
        extracted_df2["filename_clean"]
        .str.replace(r'\W+', '', regex=True)
        .str.findall(r"M\d{4,6}")
        .apply(lambda lst: list(dict.fromkeys(lst)))
        .apply(lambda lst: lst if lst else [0])
    )

    # Explode M-numbers
    extracted_df3 = (
        extracted_df2
        .explode("m_numbers")
        .rename(columns={"m_numbers": "m_number"})
        .dropna(subset=["m_number"])
        .reset_index(drop=True)
    )

    # ==========================================================================
    # STEP 2: Extract test results
    # ==========================================================================
    # Add chemical fields
    for field in CHEM_PATTERNS:
        extracted_df3[field] = extracted_df3.apply(
            lambda row: extract_field(row, field, extract_chem_info), axis=1
        )

    # Add microbial fields
    for field in MICRO_PATTERNS:
        extracted_df3[field] = extracted_df3.apply(
            lambda row: extract_field(row, field, extract_micro_info), axis=1
        )

    # Extract general info
    extracted_df4 = pd.concat(
        [extracted_df3, extracted_df3.apply(extract_general_info, axis=1)],
        axis=1
    )

    # ==========================================================================
    # STEP 3: Match with RM codes
    # ==========================================================================
    # Load order data
    order_df = pd.read_excel(PURCHASE_ORDER_FILE)
    order_df['lot_number_order'] = order_df['User Lot'].apply(
        lambda x: re.sub(r'\s+', '', os.path.splitext(os.path.basename(str(x)))[0])
        if pd.notnull(x) else None
    )

    # Get RM data from API
    rm_df = get_raw_material_items(
        DEACOM_API_USERNAME,
        DEACOM_API_PASSWORD,
        DEACOM_API_BASE_URL
    )

    # Clean lot numbers for matching
    order_df['lot_number_order_clean'] = order_df['lot_number_order'].str.lower().apply(
        lambda x: re.sub(r'\W+', '', str(x))
    )
    extracted_df4['lot_number_clean'] = extracted_df4['lot_number'].str.lower().apply(
        lambda x: re.sub(r'\W+', '', str(x))
    )
    extracted_df4['m_number_clean'] = extracted_df4['m_number'].str.lower().apply(
        lambda x: re.sub(r'\W+', '', str(x))
    )

    # Merge with order data
    merged_extracted_df = pd.merge(
        extracted_df4,
        order_df[['lot_number_order_clean', 'Part Number', 'Description']].rename(
            columns={'Part Number': 'rm_code1', 'Description': 'rm_name1'}
        ),
        how='left',
        left_on='lot_number_clean',
        right_on='lot_number_order_clean'
    ).drop(columns=['lot_number_order_clean'])

    merged_extracted_df2 = pd.merge(
        merged_extracted_df,
        order_df[['lot_number_order_clean', 'Part Number', 'Description']].rename(
            columns={'Part Number': 'rm_code2', 'Description': 'rm_name2'}
        ),
        how='left',
        left_on='m_number_clean',
        right_on='lot_number_order_clean'
    ).drop(columns=['lot_number_order_clean'])

    # Calculate similarity scores
    merged_extracted_df2['clean_product_name'] = merged_extracted_df2['product_name'].apply(clean_text)
    merged_extracted_df2['clean_rm_name1'] = merged_extracted_df2['rm_name1'].apply(clean_text)
    merged_extracted_df2['clean_rm_name2'] = merged_extracted_df2['rm_name2'].apply(clean_text)

    merged_extracted_df2['name_similarity1'] = merged_extracted_df2.apply(
        lambda row: fuzz.token_set_ratio(row['clean_product_name'], row['clean_rm_name1']),
        axis=1
    )
    merged_extracted_df2['name_similarity2'] = merged_extracted_df2.apply(
        lambda row: fuzz.token_set_ratio(row['clean_product_name'], row['clean_rm_name2']),
        axis=1
    )

    merged_extracted_df2['name_similarity'] = merged_extracted_df2[[
        'name_similarity1', 'name_similarity2'
    ]].max(axis=1)

    # ==========================================================================
    # STEP 4: Handle unmatched files
    # ==========================================================================
    grouped_df = merged_extracted_df2.groupby(['filename', 'clean_product_name'])['name_similarity'].max().reset_index()
    unmatched_file = grouped_df[grouped_df['name_similarity'] == 0]

    # Find best match for unmatched files
    rm_df['clean_description'] = rm_df['Description'].apply(clean_text)
    tqdm.pandas()
    matched_info = unmatched_file['clean_product_name'].progress_apply(
        lambda name: find_best_match(name, rm_df)
    )
    unmatched_file_merged = pd.concat([unmatched_file, matched_info], axis=1)
    unmatched_file_merged.rename(columns={
        "matched_part_number": "rm_code3",
        "matched_description": "rm_name3",
        "similarity_score": "name_similarity3"
    }, inplace=True)

    # Merge unmatched results back
    merged_extracted_df3 = merged_extracted_df2.merge(
        unmatched_file_merged[['filename', 'rm_code3', 'rm_name3', 'name_similarity3']],
        on="filename",
        how="left"
    )

    # ==========================================================================
    # STEP 5: Final processing and ranking
    # ==========================================================================
    merged_extracted_df3["rank1"] = (
        merged_extracted_df3
        .groupby("filename")["name_similarity"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    merged_extracted_df3["max_name_similarity"] = (
        merged_extracted_df3
        .groupby("filename")["name_similarity"]
        .transform("max")
    )

    merged_extracted_df4 = merged_extracted_df3[merged_extracted_df3['rank1'] == 1]

    # Determine final RM ID
    merged_extracted_df4['rm_id_final'] = np.where(
        (merged_extracted_df4['max_name_similarity'] != 0) &
        (merged_extracted_df4['name_similarity1'] == merged_extracted_df4['name_similarity']),
        merged_extracted_df4['rm_code1'],
        np.where(
            (merged_extracted_df4['max_name_similarity'] != 0) &
            (merged_extracted_df4['name_similarity2'] == merged_extracted_df4['name_similarity']),
            merged_extracted_df4['rm_code2'],
            merged_extracted_df4['rm_code3']
        )
    )

    merged_extracted_df4['rm_name_final'] = np.select(
        [
            (merged_extracted_df4['max_name_similarity'] != 0) &
            (merged_extracted_df4['name_similarity1'] == merged_extracted_df4['name_similarity']),
            (merged_extracted_df4['max_name_similarity'] != 0) &
            (merged_extracted_df4['name_similarity2'] == merged_extracted_df4['name_similarity'])
        ],
        [
            merged_extracted_df4['rm_name1'],
            merged_extracted_df4['rm_name2']
        ],
        default=merged_extracted_df4['rm_name3']
    )

    merged_extracted_df4['name_similarity_final'] = np.where(
        merged_extracted_df4['max_name_similarity'] == 0,
        merged_extracted_df4['name_similarity3'],
        merged_extracted_df4['max_name_similarity']
    )

    # ==========================================================================
    # STEP 6: Apply quality filters
    # ==========================================================================
    rm_final = merged_extracted_df4[[
        'rm_id_final', 'rm_name_final', 'name_similarity_final', 'product_name',
        'max_name_similarity', 'filename', 'json_values',
        'arsenic_ppm', 'cadmium_ppm', 'chromium_ppm', 'lead_ppm', 'mercury_ppm',
        'total_plate_count', 'yeast_and_mold', 'e_coli', 'salmonella',
        'staphylococcus_aureus', 'manufacture_date', 'expiry_date'
    ]]

    rm_final['coa_sufficient_content'] = rm_final['json_values'].apply(extract_coa_sufficient_content)

    # Apply quality filters:
    # - max_name_similarity >= 50 OR name_similarity_final >= 80
    # - coa_sufficient_content >= 8
    rm_final2 = rm_final[
        ((rm_final['max_name_similarity'] >= 50) | (rm_final['name_similarity_final'] >= 80)) &
        (rm_final['coa_sufficient_content'] >= 8)
    ]

    # Apply custom ranking
    rm_final2 = rm_final2.groupby('rm_id_final', group_keys=False).apply(custom_rank)

    # ==========================================================================
    # STEP 7: Prepare final output
    # ==========================================================================
    rm_clean = rm_final2[rm_final2['rank'] == 1].drop(
        columns=[
            'name_similarity_final', 'product_name', 'max_name_similarity',
            'filename', 'json_values', 'coa_sufficient_content', 'rank'
        ]
    )

    # Rename columns for Deacom
    rm_clean = rm_clean.rename(columns={
        'rm_id_final': 'pr_codenum',
        'arsenic_ppm': 'u_Arsenic_PPM',
        'cadmium_ppm': 'u_Cadmium_PPM',
        'chromium_ppm': 'u_Chromium',
        'lead_ppm': 'u_Lead_PPM',
        'mercury_ppm': 'u_Mercury_PPM',
        'total_plate_count': 'u_Total_Plate_Count',
        'yeast_and_mold': 'u_Yeast_and_Mold',
        'e_coli': 'u_Escherichia_Coli',
        'salmonella': 'u_Salmonella',
        'staphylococcus_aureus': 'u_Staphylococcus_Aureus'
    })

    # Export final output
    rm_clean.drop(
        columns=['rm_name_final', 'manufacture_date', 'expiry_date']
    ).to_excel(OUTPUT_FILE, index=False)

    print(f"Output saved to: {OUTPUT_FILE}")
    return rm_clean


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    result_df = process_validation(SPREADSHEET_URL)
    print(f"Validated {len(result_df)} records")
