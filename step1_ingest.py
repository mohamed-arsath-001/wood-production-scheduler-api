import pandas as pd
import os

def run_ingest():
    print("--- STEP 1: INGESTING DATA (WITH TEAMS) ---")
    
    # 1. Load File
    if os.path.exists("DummyData.xlsx"):
        df = pd.read_excel("DummyData.xlsx")
    elif os.path.exists("DummyData.csv"):
        df = pd.read_csv("DummyData.csv", encoding='latin1', low_memory=False)
    else:
        print("❌ Error: No file found.")
        return

    # 2. SMART MAPPING
    
    # FACTORY (Site)
    if 'factoryName' in df.columns: df['Plant'] = df['factoryName']
    elif 'factory' in df.columns: df['Plant'] = df['factory']
    else: df['Plant'] = "Unknown_Site"

    # MACHINE
    if 'stationName' in df.columns: df['Machine_Name'] = df['stationName']
    elif 'station' in df.columns: df['Machine_Name'] = df['station']
    elif 'stationId' in df.columns: df['Machine_Name'] = df['stationId']
    else: df['Machine_Name'] = "Main_Line"

    # TEAM / WORKER (NEW)
    if 'operatorTeam' in df.columns: df['Assigned_Team'] = df['operatorTeam']
    elif 'operator' in df.columns: df['Assigned_Team'] = df['operator']
    elif 'team' in df.columns: df['Assigned_Team'] = df['team']
    else: df['Assigned_Team'] = "TBD"

    # PRODUCT & QTY
    if 'productName' in df.columns: df['productGroupName'] = df['productName']
    
    if 'totalQty' not in df.columns:
        for col in df.columns:
            if 'qty' in col.lower():
                df['totalQty'] = df[col]
                break
    
    # 3. SELECT & CLEAN
    # Added 'Assigned_Team' to this list
    required_cols = ['totalQty', 'productGroupName', 'Plant', 'Machine_Name', 'productionOrderNumber', 'Assigned_Team']
    
    for col in required_cols:
        if col not in df.columns: df[col] = "Unknown"

    clean_df = df[required_cols].copy()
    
    clean_df['totalQty'] = pd.to_numeric(clean_df['totalQty'], errors='coerce').fillna(0)
    clean_df = clean_df[clean_df['totalQty'] > 0]

    clean_df.to_csv("cleaned_production_data.csv", index=False)
    print(f"✅ Data Cleaned. Preserved Teams for {len(clean_df)} orders.")

if __name__ == "__main__":
    run_ingest()
