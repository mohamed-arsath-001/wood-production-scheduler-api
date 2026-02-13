from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import pandas as pd
import io
import requests
import step1_ingest
import step2_optimizer
import xlsxwriter
import os 
import traceback

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- LOAD MASTER DATABASE (The Brain) ---
# Tries to load the file if it exists
history_df = pd.DataFrame() 
if os.path.exists("API Data.xlsx"):
    try:
        history_df = pd.read_excel("API Data.xlsx")
        print("✅ Master Database Loaded: API Data.xlsx")
    except Exception as e:
        print(f"⚠️ Failed to read API Data.xlsx: {e}")
elif os.path.exists("4. API Data.xlsx"):
    try:
        history_df = pd.read_excel("4. API Data.xlsx")
        print("✅ Master Database Loaded: 4. API Data.xlsx")
    except Exception as e:
        print(f"⚠️ Failed to read 4. API Data.xlsx: {e}")
else:
    print("⚠️ Master Database Not Found. Using Defaults.")

@app.post("/optimize")
async def optimize_schedule(files: List[UploadFile] = File(...)):
    try:
        combined_data = []

        # --- STEP 1: INGEST ALL FILES ---
        print(f"📥 Processing {len(files)} files...")
        
        for file in files:
            content = await file.read()
            filename = file.filename
            
            # Read File
            try:
                if filename.endswith('.csv'):
                    df = pd.read_csv(io.BytesIO(content))
                else:
                    df = pd.read_excel(io.BytesIO(content))
            except:
                print(f"❌ Failed to read file: {filename}")
                continue 

            # Clean the individual file
            cleaned_df = step1_ingest.standardize_columns(df)
            
            # Tag with filename for the Excel tab later
            short_name = filename.split('.')[0]
            if len(filename.split('.')) > 1:
                 short_name += "_" + filename.split('.')[1]
            
            # Excel sheet name limit is 31 chars
            if len(short_name) > 30: short_name = short_name[:30] 
            
            cleaned_df['Source_File'] = short_name
            combined_data.append(cleaned_df)

        if not combined_data:
            raise HTTPException(status_code=400, detail="No valid data found in uploads")

        # Merge for Optimization
        master_df = pd.concat(combined_data, ignore_index=True)

        # --- STEP 2: RUN INTELLIGENT OPTIMIZER ---
        optimized_master = step2_optimizer.run_optimizer(master_df, history_df)

        # --- STEP 3: SPLIT BACK INTO SHEETS ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            
            if 'Source_File' in optimized_master.columns:
                unique_sources = optimized_master['Source_File'].unique()
                for source in unique_sources:
                    # Filter data for this file
                    sheet_data = optimized_master[optimized_master['Source_File'] == source]
                    # Write to its own tab
                    sheet_data.to_excel(writer, index=False, sheet_name=str(source)[:31])
            else:
                optimized_master.to_excel(writer, index=False, sheet_name="Optimized_Schedule")

        output.seek(0)

        # --- STEP 4: SEND TO N8N ---
        # UPDATED URL
        n8n_url = "https://arsath26.app.n8n.cloud/webhook/process-schedule"
        
        files_payload = {'data': ('Optimized_Schedule.xlsx', output, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
        n8n_status = "Skipped"
        
        try:
            response = requests.post(n8n_url, files=files_payload)
            if response.status_code == 200:
                 n8n_status = "Email Sent"
                 print("✅ Sent to n8n")
            else:
                 n8n_status = f"n8n Failed: {response.status_code}"
                 print(f"❌ n8n returned {response.status_code}")
        except Exception as e:
            n8n_status = "Email Failed"
            print(f"❌ n8n connection error: {e}")

        # --- STEP 5: RETURN JSON ---
        # CRITICAL FIX: .fillna("") prevents the "Out of range float values" error
        result_json = optimized_master.head(100).fillna("").to_dict(orient="records")
        
        return {
            "status": "success",
            "files_processed": len(files),
            "n8n_delivery": n8n_status,
            "data": result_json
        }

    except Exception as e:
        print(f"Server Error: {e}")
        traceback.print_exc() # Helps debug in logs
        raise HTTPException(status_code=500, detail=str(e))
