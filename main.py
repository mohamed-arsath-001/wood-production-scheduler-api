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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- LOAD MASTER DATABASE ---
history_df = pd.DataFrame() 
if os.path.exists("API Data.xlsx"):
    try: history_df = pd.read_excel("API Data.xlsx")
    except: pass
elif os.path.exists("4. API Data.xlsx"):
    try: history_df = pd.read_excel("4. API Data.xlsx")
    except: pass

@app.post("/optimize")
async def optimize_schedule(files: List[UploadFile] = File(...)):
    try:
        combined_data = []
        print(f"📥 Processing {len(files)} files...")
        
        for file in files:
            content = await file.read()
            filename = file.filename
            try:
                if filename.endswith('.csv'): df = pd.read_csv(io.BytesIO(content))
                else: df = pd.read_excel(io.BytesIO(content))
            except: continue 

            # Step 1: Ingest & Clean
            cleaned_df = step1_ingest.standardize_columns(df)
            cleaned_df['Source_File'] = filename 
            combined_data.append(cleaned_df)

        if not combined_data:
            raise HTTPException(status_code=400, detail="No valid data found")

        master_df = pd.concat(combined_data, ignore_index=True)

        # Step 2: Run AI Optimizer (With Real Names & Smart Matching)
        optimized_master = step2_optimizer.run_optimizer(master_df, history_df)

        # --- STEP 3: GENERATE MULTI-SHEET EXCEL FILE ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            
            # TAB 1: DASHBOARD
            total_orders = len(optimized_master)
            total_units = optimized_master['Qty'].sum() if 'Qty' in optimized_master else 0
            
            dashboard_data = [
                {'Metric': 'ZESTFLOW PILOT DASHBOARD', 'Value': ''},
                {'Metric': '---------------------------', 'Value': '---'},
                {'Metric': 'Total Orders Scheduled', 'Value': total_orders},
                {'Metric': 'Total Units Produced', 'Value': total_units},
                {'Metric': '', 'Value': ''},
                {'Metric': 'BATCHES PER SITE', 'Value': ''}
            ]
            
            if 'Site' in optimized_master.columns:
                counts = optimized_master['Site'].value_counts().to_dict()
                for site, count in counts.items():
                    dashboard_data.append({'Metric': site, 'Value': count})
            
            pd.DataFrame(dashboard_data).to_excel(writer, index=False, sheet_name='Dashboard')

            # TAB 2, 3, 4: FACTORY SHEETS
            # We filter the master list and save to separate tabs
            factories = ['Boksburg', 'Piet Retief', 'Ugie']
            
            for factory in factories:
                if 'Site' in optimized_master.columns:
                    # Filter data for this specific factory
                    sheet_data = optimized_master[optimized_master['Site'] == factory]
                    
                    # Sort by Date/Time for readability
                    if 'Start_Time' in sheet_data.columns:
                        sheet_data = sheet_data.sort_values('Start_Time')
                else:
                    sheet_data = pd.DataFrame() 
                
                # Write to Excel Tab
                sheet_data.to_excel(writer, index=False, sheet_name=factory)

        output.seek(0)

        # --- STEP 4: SEND EXCEL FILE TO N8N ---
        # This sends the .xlsx file (which supports sheets) instead of a CSV
        n8n_url = "https://arsath26.app.n8n.cloud/webhook/process-schedule"
        files_payload = {'data': ('Optimized_Schedule.xlsx', output, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
        
        n8n_status = "Skipped"
        try:
            requests.post(n8n_url, files=files_payload)
            n8n_status = "Email Sent"
        except:
            n8n_status = "Email Failed"

        # --- STEP 5: RETURN JSON PREVIEW ---
        # The frontend still gets JSON, but the file sent to N8N is the multi-sheet Excel
        result_json = optimized_master.head(50).fillna("").to_dict(orient="records")
        
        return {
            "status": "success",
            "files_processed": len(files),
            "n8n_delivery": n8n_status,
            "message": "Excel file with 4 sheets created.",
            "data": result_json
        }

    except Exception as e:
        print(f"Server Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
