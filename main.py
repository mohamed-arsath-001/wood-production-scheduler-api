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

# Load Master DB
history_df = pd.DataFrame() 
if os.path.exists("4.API Data.xlsx"):
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

            cleaned_df = step1_ingest.standardize_columns(df)
            cleaned_df['Source_File'] = filename # Tagging filename is crucial for site detection
            combined_data.append(cleaned_df)

        if not combined_data:
            raise HTTPException(status_code=400, detail="No valid data found")

        master_df = pd.concat(combined_data, ignore_index=True)

        # Run Optimizer
        optimized_master = step2_optimizer.run_optimizer(master_df, history_df)

        # --- GENERATE EXCEL WITH SPECIFIC TABS ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            
            # 1. DASHBOARD SHEET
            total_orders = len(optimized_master)
            total_units = optimized_master['Qty'].sum() if 'Qty' in optimized_master else 0
            
            dashboard_data = [
                {'Metric': 'ZESTFLOW DASHBOARD', 'Value': ''},
                {'Metric': '-----------------', 'Value': '---'},
                {'Metric': 'Total Orders', 'Value': total_orders},
                {'Metric': 'Total Units', 'Value': total_units},
                {'Metric': '', 'Value': ''},
                {'Metric': 'BREAKDOWN BY SITE', 'Value': ''}
            ]
            
            if 'Site' in optimized_master.columns:
                counts = optimized_master['Site'].value_counts().to_dict()
                for site, count in counts.items():
                    dashboard_data.append({'Metric': site, 'Value': count})
            
            pd.DataFrame(dashboard_data).to_excel(writer, index=False, sheet_name='Dashboard')

            # 2. FACTORY SHEETS (Forced Creation)
            target_factories = ['Boksburg', 'Piet Retief', 'Ugie']
            
            for factory in target_factories:
                if 'Site' in optimized_master.columns:
                    # Filter for this factory
                    sheet_data = optimized_master[optimized_master['Site'] == factory]
                    # Drop helper columns to keep it clean
                    sheet_data = sheet_data.drop(columns=['Source_File'], errors='ignore')
                else:
                    sheet_data = pd.DataFrame() # Empty sheet
                
                # Write to Excel (Sheet exists even if empty)
                sheet_data.to_excel(writer, index=False, sheet_name=factory)

        output.seek(0)

        # --- SEND TO N8N ---
        # Updated URL
        n8n_url = "https://arsath26.app.n8n.cloud/webhook/process-schedule"
        files_payload = {'data': ('Optimized_Schedule.xlsx', output, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
        
        n8n_status = "Skipped"
        try:
            requests.post(n8n_url, files=files_payload)
            n8n_status = "Email Sent"
        except:
            n8n_status = "Email Failed"

        # --- RETURN JSON ---
        result_json = optimized_master.head(100).fillna("").to_dict(orient="records")
        
        return {
            "status": "success",
            "files_processed": len(files),
            "n8n_delivery": n8n_status,
            "data": result_json
        }

    except Exception as e:
        print(f"Server Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
