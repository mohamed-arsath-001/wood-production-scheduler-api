import pandas as pd
import numpy as np

def standardize_columns(df):
    search_terms = ["planned qty", "item number", "w/h", "station", "stationname", "qty", "material"]
    
    # Header detection logic
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

    df.columns = [str(c).strip() for c in df.columns]
    
    column_map = {
        'Machine': ['W/H', 'WorkCenter', 'Work Center', 'Station', 'Resource'],
        'Product': ['Item Number', 'Item', 'Material', 'Description', 'Product Code'],
        'Qty': ['Planned Qty', 'Quantity', 'OrderQty', 'Target Qty']
    }

    for standard_col, potential_names in column_map.items():
        if standard_col not in df.columns:
            for alias in potential_names:
                match = next((c for c in df.columns if str(c).lower() == alias.lower()), None)
                if match:
                    df = df.rename(columns={match: standard_col})
                    break
    
    # --- SMART ID & SAFETY NET ---
    if 'Order_ID' not in df.columns:
        # Generate a unique ID based on Machine and Row Number
        df['Order_ID'] = [f"ORD-{str(m)[:3]}-{i+100}" for i, m in enumerate(df.get('Machine', 'UNK'))]

    if 'Qty' not in df.columns: df['Qty'] = 0
    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce').fillna(0)
    df = df[df['Qty'] > 0]

    return df
