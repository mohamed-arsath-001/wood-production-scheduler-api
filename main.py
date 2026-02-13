from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import pandas as pd
import io
import requests
import step1_ingest
import step2_optimizer
import xlsxwriter

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
# Ensure 'API Data.xlsx' is in your GitHub repo root folder!
try:
    history_df = pd.read_excel("API Data.xlsx") 
    # Or "4. API Data.xlsx" depending on your filename
    print("✅ Master Database Loaded Successfully")
except:
    print("⚠️ Master Database Not Found. Using Defaults.")
    history_df = pd.DataFrame() # Empty fallback

@app.post("/optimize")
async def optimize_schedule(files: List[UploadFile] = File(...)):
    try:
        # Dictionary to store data for each file (Dynamic Tabs)
        file_data_map = {} 
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
                continue # Skip bad files

            # Clean the individual file
            cleaned_df = step1_ingest.standardize_columns(df)
            
            # Tag with filename for the Excel tab later
            # Shorten name: "1. BXB_PLANS3.xlsx" -> "BXB_PLANS3"
            short_name = filename.split('.')[0] + "_" + filename.split('.')[1]
            if len(short_name) > 30: short_name = short_name[:30] # Excel limit
            
            cleaned_df['Source_File'] = short_name
            combined_data.append(cleaned_df)

        if not combined_data:
            raise HTTPException(status_code=400, detail="No valid data found")

        # Merge for Optimization
        master_df = pd.concat(combined_data, ignore_index=True)

        # --- STEP 2: RUN INTELLIGENT OPTIMIZER ---
        # We pass the history_df (Database) here!
        optimized_master = step2_optimizer.run_optimizer(master_df, history_df)

        # --- STEP 3: SPLIT BACK INTO SHEETS ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            
            # Group by the source filename we saved earlier
            unique_sources = optimized_master['Source_File'].unique()
            
            for source in unique_sources:
                # Filter data for this file
                sheet_data = optimized_master[optimized_master['Source_File'] == source]
                # Write to its own tab
                sheet_data.to_excel(writer, index=False, sheet_name=source)

        output.seek(0)

        # --- STEP 4: SEND TO N8N ---
        # (Same n8n code as before...)
        n8n_url = "https://arsath26.app.n8n.cloud/webhook/process-schedule"
        files_payload = {'data': ('Optimized_Schedule.xlsx', output, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
        try:
            requests.post(n8n_url, files=files_payload)
            n8n_status = "Email Sent"
        except:
            n8n_status = "Email Failed"

        # --- STEP 5: RETURN JSON ---
        result_json = optimized_master.head(100).to_dict(orient="records")
        return {
            "status": "success",
            "files_processed": len(files),
            "n8n_delivery": n8n_status,
            "data": result_json
        }

    except Exception as e:
        print(f"Server Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
