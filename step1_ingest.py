import pandas as pd

def standardize_columns(df):
    """
    Standardizes column names from various client Excel formats (BXB, PRF, UGI)
    into the strict format required by the Zestflow Optimizer.
    """
    
    # 1. CLEAN COLUMN NAMES (Remove spaces, make lowercase for matching)
    df.columns = [c.strip() for c in df.columns]
    
    # 2. DEFINE MAPPINGS
    # Key = The Standard Name we need
    # Value = List of possible headers in the client's Excel files
    column_map = {
        'Order_ID': ['Order', 'Production Order', 'Order No', 'ID'],
        'Product': ['Item Name', 'Material', 'Description', 'Product Code', 'Item'],
        'Qty': ['Quantity', 'OrderQty', 'Amount', 'Target Qty', 'Qty'],
        'Machine': ['WorkCenter', 'Work Center', 'Station', 'Resource', 'Machine Name'],
        'Assigned_Team': ['Team', 'Worker', 'Operator', 'Personnel']
    }

    # 3. APPLY MAPPING
    for standard_col, potential_names in column_map.items():
        # Check if the standard column already exists
        if standard_col in df.columns:
            continue
            
        # Look for a match in the potential names
        for alias in potential_names:
            # Case-insensitive check
            match = next((c for c in df.columns if c.lower() == alias.lower()), None)
            if match:
                df = df.rename(columns={match: standard_col})
                break
    
    # 4. DATA SANITIZATION
    
    # Fill missing Team (Critical for logic not to crash)
    if 'Assigned_Team' not in df.columns:
        df['Assigned_Team'] = 'Unassigned'
    else:
        df['Assigned_Team'] = df['Assigned_Team'].fillna('Unassigned')

    # Ensure Site is present (It should be tagged in main.py, but just in case)
    if 'Site' not in df.columns:
        df['Site'] = 'Unknown'

    # Filter out Zero Qty or invalid rows
    if 'Qty' in df.columns:
        df = df[df['Qty'] > 0]

    return df
