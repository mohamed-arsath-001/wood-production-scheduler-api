import pandas as pd
import numpy as np

def standardize_columns(df):
    """
    Cleans the Excel file but STRICTLY preserves the original column names.
    """
    search_terms = ["item description", "item number", "w/h", "planned qty", "material"]
    
    # 1. Find the real header row (ignoring the weird blank rows at the top)
    found_header = False
    current_headers = [str(c).lower().strip() for c in df.columns]
    if any(term in h for h in current_headers for term in search_terms):
        found_header = True
    
    if not found_header:
        for i, row in df.head(20).iterrows():
            row_str = row.astype(str).str.lower().tolist()
            if sum(1 for term in search_terms if any(term in s for s in row_str)) >= 1:
                df.columns = row
                df = df.iloc[i+1:].reset_index(drop=True)
                break

    # Clean up the column names so they don't have weird spaces
    df.columns = [str(c).strip() for c in df.columns]
    
    # 2. DELETE ghost columns (Unnamed: 1, Unnamed: 6, etc.)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False, na=False)]
    
    # 3. DELETE empty rows (only keep rows that actually have an order quantity)
    # Find whatever column they use for Quantity (Planned Qty, Target Qty, etc.)
    qty_col = next((c for c in df.columns if 'qty' in str(c).lower()), None)
    
    if qty_col:
        df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
        df = df[df[qty_col] > 0] # Drop rows with 0 or empty quantity
        
    # Find whatever column they use for Description to ensure it's a real order row
    desc_col = next((c for c in df.columns if 'description' in str(c).lower() or 'item' in str(c).lower()), None)
    if desc_col:
        df = df.dropna(subset=[desc_col])

    return df
