import os
import re
from typing import List
import pandas as pd
import numpy as np
import openpyxl

# ========== USER CONFIG ==========
INPUT_DIR = "data"   # folder with your Excel files
VALID_EXTS = {".xlsx", ".xlsm", ".xls"}  # add .xlsb if needed (requires pyxlsb)
SCAN_ROWS = 20  # how many rows to search for header
TARGET_COLUMNS = [
        'JobNumber', 'Office', 'Office (Div)', 'ProjectTitle', 'Client', 
        'Location (Country)', 'Gross Fee (USD)', 'Fee Earned (USD)', 
        'Gross Fee Yet To Be Earned (USD)', 'Currency', 'GrossFee', 
        'GrossFeeEarned', 'GrossFeeYetToBeEarned', 'Status', 'NewProject', 
        'StartDate', 'Anticipated EndDate', 'ProjectType'
    ]

COLUMN_ALIASES = {
    'Location (Country)': ['Location (Country)', 'Location', 'Country'],
    'NewProject': ['NewProject', 'New Project', 'New_Project', 'IsNew', 'Is New'],
}

# def list_excel_files(folder: str, exts=VALID_EXTS) -> List[str]:
#     files = []
#     for root, _, filenames in os.walk(folder):
#         for fn in filenames:
#             if os.path.splitext(fn)[1].lower() in exts:
#                 files.append(os.path.join(root, fn))
#     return files

def normalize(name: str) -> str:
    """
    Normalize column names so:
    - 'Project Title' -> 'projecttitle'
    - 'ProjectTitle'  -> 'projecttitle'
    - 'Project_Type'  -> 'projecttype'
    - 'Client'        -> 'client'
    - 'Gross Fee Yet To Be Earned (USD)' -> 'grossfeeyettobeearnedusd'
    """
    if name is None or type(name) is not str:
        return ""
    s = name.strip().lower()
    s = re.sub(r"[\s_\-]+", "", s)     # remove spaces/underscores/dashes
    s = re.sub(r"[^\w]", "", s)        # remove other punctuation
    return s

def find_header_row(df_no_header):
    """
    Return the earliest (0-based) row index where BOTH 'Client' and 'Currency' appear.
    Case-insensitive; whitespace/punct ignored.
    """
    import builtins
    max_r = min(len(df_no_header), SCAN_ROWS)
    for r in range(max_r):
        row = [normalize(v) for v in df_no_header.iloc[r].tolist()]
        if "jobnumber" in row and "currency" in row:
            return r
    return None  # not found

def _to_number(series: pd.Series) -> pd.Series:
    # Keep digits, sign, and decimal; turn "(123)" into "-123"
    s = series.astype(str).fillna("")
    s = s.str.replace(r"\(([\d\.,]+)\)", r"-\1", regex=True)
    s = s.str.replace(r"[^\d\.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")

def read_sheet(path, sheet):
    raw = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str, engine=None)
    print(f"Processing sheet : {sheet}")
    if raw.empty:
        return pd.DataFrame()

    hdr = find_header_row(raw)
    if hdr is None:
        print(f"no headers found for {sheet} - skipping..")
        return pd.DataFrame()

    cols = raw.iloc[hdr].tolist()
    
    # Clean up column names
    cleaned_cols = []
    seen_cols = {}
    
    for i, col in enumerate(cols):
        if col is None or (isinstance(col, float) and pd.isna(col)) or str(col).strip() == '':
            clean_col = f"unnamed_col_{i}"
        else:
            clean_col = str(col).strip()
        
        if clean_col in seen_cols:
            seen_cols[clean_col] += 1
            clean_col = f"{clean_col}_duplicate_{seen_cols[clean_col]}"
        else:
            seen_cols[clean_col] = 0
        
        cleaned_cols.append(clean_col)
    
    data = raw.iloc[hdr+1:].reset_index(drop=True)
    data.columns = cleaned_cols
    
    print(f"Columns in sheet {sheet} : {cleaned_cols}")
    
    # Create normalized mapping: normalized name -> actual column name
    normalized_map = {}
    for actual_col in data.columns:
        normalized_map[normalize(actual_col)] = actual_col
    
    out = pd.DataFrame()
    
    # Map each target column, checking aliases
    for target_col in TARGET_COLUMNS:
        found = False
        matched_col = None
        
        # Check if this column has aliases
        if target_col in COLUMN_ALIASES:
            possible_names = COLUMN_ALIASES[target_col]
        else:
            possible_names = [target_col]
        
        # Try to find any of the possible names
        for possible_name in possible_names:
            normalized_possible = normalize(possible_name)
            if normalized_possible in normalized_map:
                actual_col = normalized_map[normalized_possible]
                out[target_col] = data[actual_col]
                matched_col = actual_col
                found = True
                break
        
        if found:
            print(f"✓ Found: {target_col} (mapped from '{matched_col}')")
        else:
            print(f"✗ Missing: {target_col} (tried: {', '.join(possible_names)})")
    
    # Ensure the DataFrame has ONLY the target columns in the exact order
    out = out.reindex(columns=TARGET_COLUMNS)
    
    if out.empty:
        print(f"No valid data found in {sheet}")
        return pd.DataFrame(columns=TARGET_COLUMNS)

    # Convert numeric columns
    numeric_columns = [
        'Gross Fee (USD)', 'Fee Earned (USD)', 'Gross Fee Yet To Be Earned (USD)',
        'GrossFee', 'GrossFeeEarned', 'GrossFeeYetToBeEarned'
    ]
    
    for col in numeric_columns:
        if col in out.columns and not out[col].isna().all():
            try:
                out[col] = _to_number(out[col])
            except:
                pass

    print(f"Successfully processed {sheet}: {len(out)} rows")
    return out


def read_file(path):
    """Read all sheets from an Excel file."""
    # Convert volume path to proper format for pandas
    if path.startswith('/Volumes/'):
        # Volume paths are already accessible as local filesystem paths
        local_path = path
    elif path.startswith('dbfs:/Volumes/'):
        # Remove dbfs: prefix
        local_path = path.replace('dbfs:', '')
    else:
        local_path = path
    
    print(f"Attempting to read from: {local_path}")
    
    try:
        xl = pd.ExcelFile(local_path, engine='openpyxl')
    except Exception as e:
        print(f"Could not read file: {e}")
        print(f"Path attempted: {local_path}")
        return pd.DataFrame(columns=TARGET_COLUMNS)
    
    wb = openpyxl.load_workbook(local_path, read_only=True, data_only=True)
    
    # Filter out hidden sheets
    visible_sheets = []
    for sheet_name in xl.sheet_names:
        sheet = wb[sheet_name]
        # sheet.sheet_state can be 'visible', 'hidden', or 'veryHidden'
        if sheet.sheet_state == 'visible':
            visible_sheets.append(sheet_name)
        else:
            print(f"Skipping hidden sheet: {sheet_name}")
    
    wb.close()

    frames = []
    for s in visible_sheets:
        try:
            df = read_sheet(local_path, s)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"Error reading sheet {s}: {e}")
            continue
    
    if not frames:
        return pd.DataFrame(columns=TARGET_COLUMNS)
    
    return pd.concat(frames, ignore_index=True)


# def extract_all(input_dir=INPUT_DIR):
#     dfs = []
#     for root, _, files in os.walk(input_dir):
#         for f in files:
#             if os.path.splitext(f)[1].lower() in VALID_EXTS:
#                 dfs.append(read_file(os.path.join(root, f)))
#     return pd.concat(dfs, ignore_index=True)
