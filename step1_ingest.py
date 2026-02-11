import pandas as pd
import os

def run_ingest():
    print("--- STEP 1: INGESTING DATA ---")
    
    # 1. Load File
    if os.path.exists("DummyData.xlsx"):
        df = pd.read_excel("DummyData.xlsx")
        print("   > Detected Format: EXCEL")
    elif os.path.exists("DummyData.csv"):
        df = pd.read_csv("DummyData.csv", encoding='latin1', low_memory=False)
        print("   > Detected Format: CSV")
    else:
        print("❌ Error: No file found.")
        return

    # 2. SMART MAPPING (The Fix)
    # We look for the columns that actually exist in your 'API Data' file
    
    # FACTORY NAME (Site)
    if 'factoryName' in df.columns:
        df['Plant'] = df['factoryName']
    elif 'factory' in df.columns:
         df['Plant'] = df['factory']
    else:
        df['Plant'] = "Unknown_Site"

    # MACHINE NAME (Station)
    if 'stationName' in df.columns:
        df['Machine_Name'] = df['stationName']
    elif 'station' in df.columns:
        df['Machine_Name'] = df['station']
    elif 'stationId' in df.columns:
        df['Machine_Name'] = df['stationId']
    else:
        df['Machine_Name'] = "Main_Line" # Fallback

    # PRODUCT & QTY
    if 'productName' in df.columns:
        df['productGroupName'] = df['productName']
    
    if 'totalQty' not in df.columns:
        # Try to find any quantity column
        for col in df.columns:
            if 'qty' in col.lower():
                df['totalQty'] = df[col]
                break
    
    # 3. SELECT & CLEAN
    # We only keep the columns our Optimizer needs
    required_cols = ['totalQty', 'productGroupName', 'Plant', 'Machine_Name', 'productionOrderNumber']
    
    # Ensure all exist
    for col in required_cols:
        if col not in df.columns:
            df[col] = "Unknown"

    clean_df = df[required_cols].copy()
    
    # Filter valid orders
    clean_df['totalQty'] = pd.to_numeric(clean_df['totalQty'], errors='coerce').fillna(0)
    clean_df = clean_df[clean_df['totalQty'] > 0]

    # Save
    OUTPUT_FILE = "cleaned_production_data.csv"
    clean_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Data Cleaned. Rows: {len(clean_df)}")
    print(f"   Sites Found: {clean_df['Plant'].unique()}")
    print(f"   Machines: {clean_df['Machine_Name'].unique()[:5]}")

if __name__ == "__main__":
    run_ingest()