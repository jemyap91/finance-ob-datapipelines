import re
import hashlib
import pandas as pd
from datetime import datetime  # If you're working with datetime objects
from data_extraction import read_file

def extract_date_from_filename(filename):
    """
    Extract year, month name, and month number from filename.
    Examples:
      "1_Order_Book_Mar_2025.xlsx" -> (2025, "Mar", 3)
      "1. Order Book (Jun 2025).xlsm" -> (2025, "Jun", 6)
    """
    month_map = {
        'jan': (1, 'Jan'), 'january': (1, 'Jan'),
        'feb': (2, 'Feb'), 'february': (2, 'Feb'),
        'mar': (3, 'Mar'), 'march': (3, 'Mar'),
        'apr': (4, 'Apr'), 'april': (4, 'Apr'),
        'may': (5, 'May'),
        'jun': (6, 'Jun'), 'june': (6, 'Jun'),
        'jul': (7, 'Jul'), 'july': (7, 'Jul'),
        'aug': (8, 'Aug'), 'august': (8, 'Aug'),
        'sep': (9, 'Sep'), 'sept': (9, 'Sep'), 'september': (9, 'Sep'),
        'oct': (10, 'Oct'), 'october': (10, 'Oct'),
        'nov': (11, 'Nov'), 'november': (11, 'Nov'),
        'dec': (12, 'Dec'), 'december': (12, 'Dec')
    }
    
    # Extract year (4 digits)
    year_match = re.search(r'20\d{2}', filename)
    year = int(year_match.group()) if year_match else None
    
    # Extract month
    month_num = None
    month_name = None
    
    filename_lower = filename.lower()
    for month_key, (num, name) in month_map.items():
        if month_key in filename_lower:
            month_num = num
            month_name = name
            break
    
    return year, month_name, month_num

def calculate_row_hash(row_dict):
    """
    Calculate a hash for deduplication based on key fields.
    """
    key_fields = ['JobNumber', 'ProjectTitle', 'Client', 'Office']
    hash_string = '|'.join([str(row_dict.get(f, '')) for f in key_fields])
    return hashlib.md5(hash_string.encode()).hexdigest()


def process_excel_file(file_info):
    """
    Process a single Excel file using data_extraction.py module.
    """
    file_path = file_info['path']
    file_name = file_info['name']
    
    try:
        print(f"\n{'='*70}")
        print(f"Processing: {file_name}")
        print(f"{'='*70}")
        
        # Convert volume path to local path for pandas
        # Volume paths are already accessible as local paths in Databricks
        local_path = file_path.replace("dbfs:", "")
        print(local_path)
        # Use your existing read_file function
        df = read_file(local_path)
        
        if df.empty:
            print(f"⚠ No data extracted from {file_name}")
            return pd.DataFrame()
        
        print(f"✓ Successfully extracted {len(df)} rows")
        
        # Add metadata columns
        df['source_file'] = file_name
        df['source_mtime'] = file_info['mtime']
        
        # Extract date information from filename
        year, month_name, month_num = extract_date_from_filename(file_name)
        df['data_year'] = year
        df['data_month'] = month_name
        df['data_month_num'] = month_num
        
        # Set collection date (file modification time or current date)
        df['data_collection_date'] = file_info['mtime'].date()
        
        # Calculate row hash for each row
        df['row_hash'] = df.apply(lambda row: calculate_row_hash(row.to_dict()), axis=1)
        
        return df
        
    except Exception as e:
        print(f"✗ Error processing {file_name}: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    
    
def normalize_column_names(df):
    """
    Normalize pandas DataFrame column names to match the Delta table schema.
    """
    column_mapping = {
        'Office (Div)': 'office_div',
        'Location (Country)': 'location_country',
        'Gross Fee (USD)': 'gross_fee_usd',
        'Fee Earned (USD)': 'fee_earned_usd',
        'Gross Fee Yet To Be Earned (USD)': 'gross_fee_yet_to_be_earned_usd',
        'Anticipated EndDate': 'anticipated_end_date',
        'StartDate': 'StartDate',
        'ProjectType': 'ProjectType'
    }
    
    # Rename columns
    df = df.rename(columns=column_mapping)
    
    # Convert column names to lowercase where needed
    final_columns = {}
    for col in df.columns:
        if col in ['JobNumber', 'Office', 'ProjectTitle', 'Client', 'Currency', 
                   'GrossFee', 'GrossFeeEarned', 'GrossFeeYetToBeEarned', 
                   'Status', 'NewProject', 'StartDate', 'ProjectType']:
            final_columns[col] = col
        else:
            final_columns[col] = col.lower()
    
    df = df.rename(columns=final_columns)
    
    return df