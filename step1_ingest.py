import pandas as pd
import numpy as np

def standardize_columns(df):
    """
    Standardizes column names.
    Includes a SAFETY NET: If a column is missing, it creates it with 'Unknown' 
    instead of crashing the server.
    """
    
    # 1. FIND THE HEADER ROW (Aggressive Search)
    # We look for ANY common column name to identify the header row
    # Added "w/h" specifically for your PRF files
    search_terms = ["planned qty", "item number", "w/h", "station", "stationname", "qty", "material"]
    
    found_header = False
    
    # Check if current headers are already correct
    current_headers = [str(c).lower().strip() for c in df.columns]
    if any(term in h for h in current_headers for term in search_terms):
        found_header = True
    
    if not found_header:
        # Loop through first 20 rows to find a header
        for i, row in df.head(20).iterrows():
            row_str = row.astype(str).str.lower().tolist()
            # If this row contains at least 1 of our search terms, it's the header
            matches = sum(1 for term in search_terms if any(term in s for s in row_str))
            if matches >= 1:
                df.columns = row # Set this row as header
                df = df.iloc[i+1:] # Keep data after this row
                df = df.reset_index(drop=True)
                break

    # 2. CLEAN COLUMN NAMES (Strip spaces and make string)
    # This fixes the "W/H " vs "W/H" issue
    df.columns = [str(c).strip() for c in df.columns]
    
    # 3. DEFINE MAPPINGS (Expanded to include API Data styles)
    column_map = {
        'Machine': ['W/H', 'WorkCenter', 'Work Center', 'Station', 'Resource', 'stationName', 'Station Name'],
        'Product': ['Item Number', 'Item', 'Material', 'Description', 'Product Code', 'productName', 'Product Name'],
        'Qty': ['Planned Qty', 'Quantity', 'OrderQty', 'Amount', 'Target Qty', 'totalQty', 'Quantity'],
        'Order_ID': ['Order', 'Production Order', 'ID', 'productionOrderNumber', 'Order Number'] 
    }

    # 4. APPLY MAPPING
    for standard_col, potential_names in column_map.items():
        if standard_col in df.columns:
            continue
        for alias in potential_names:
            # Case-insensitive precise match
            match = next((c for c in df.columns if str(c).lower() == alias.lower()), None)
            if match:
                df = df.rename(columns={match: standard_col})
                break
    
    # 5. THE SAFETY NET (Crucial Fix for 500 Error)
    # If mapping failed, create the column anyway with default values
    required_cols = ['Machine', 'Product', 'Qty', 'Order_ID', 'Site']
    
    for col in required_cols:
        if col not in df.columns:
            print(f"⚠️ Warning: Missing column '{col}'. Filling with defaults.")
            if col == 'Qty':
                df[col] = 0
            else:
                df[col] = f"Unknown_{col}"

    # 6. REMOVE GARBAGE ROWS
    # Ensure Qty is numeric
    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce').fillna(0)
    df = df[df['Qty'] > 0] # Drop zero quantity rows

    # 7. FINAL SANITIZATION
    if 'Assigned_Team' not in df.columns:
        df['Assigned_Team'] = 'Unassigned'

    if 'Site' not in df.columns:
        df['Site'] = 'Unknown'

    return df
