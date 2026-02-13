from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
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

        # Step 2: Run AI Optimizer
        optimized_master = step2_optimizer.run_optimizer(master_df, history_df)

        # --- STEP 3: CREATE MULTI-SHEET EXCEL FILE ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            
            # TAB 1: DASHBOARD
            total_orders = len(optimized_master)
            total_units = optimized_master['Qty'].sum() if 'Qty' in optimized_master else 0
            
            dashboard_data = [
                {'Metric': 'ZESTFLOW PRODUCTION PLAN', 'Value': ''},
                {'Metric': '---------------------------', 'Value': '---'},
                {'Metric': 'Total Orders', 'Value': total_orders},
                {'Metric': 'Total Units', 'Value': total_units},
                {'Metric': '', 'Value': ''},
                {'Metric': 'BREAKDOWN BY FACTORY', 'Value': ''}
            ]
            
            if 'Site' in optimized_master.columns:
                counts = optimized_master['Site'].value_counts().to_dict()
                for site, count in counts.items():
                    dashboard_data.append({'Metric': site, 'Value': count})
            
            pd.DataFrame(dashboard_data).to_excel(writer, index=False, sheet_name='Dashboard')

            # TAB 2: BOKSBURG
            if 'Site' in optimized_master.columns:
                df_bxb = optimized_master[optimized_master['Site'] == 'Boksburg']
                df_bxb.to_excel(writer, index=False, sheet_name='Boksburg')

            # TAB 3: PIET RETIEF
            if 'Site' in optimized_master.columns:
                df_prf = optimized_master[optimized_master['Site'] == 'Piet Retief']
                df_prf.to_excel(writer, index=False, sheet_name='Piet Retief')

            # TAB 4: UGIE
            if 'Site' in optimized_master.columns:
                df_ugi = optimized_master[optimized_master['Site'] == 'Ugie']
                df_ugi.to_excel(writer, index=False, sheet_name='Ugie')

        output.seek(0)

        # --- STEP 4: SEND EXCEL TO N8N (EMAIL) ---
        n8n_url = "https://arsath26.app.n8n.cloud/webhook/process-schedule"
        files_payload = {'data': ('Production_Plan.xlsx', output.getvalue(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
        
        try:
            requests.post(n8n_url, files=files_payload)
            print("✅ Excel file sent to N8N")
        except Exception as e:
            print(f"❌ Failed to send to N8N: {e}")

        # --- STEP 5: DIRECT BROWSER DOWNLOAD ---
        output.seek(0)
        headers = {
            'Content-Disposition': 'attachment; filename="Production_Plan.xlsx"'
        }
        return StreamingResponse(output, headers=headers, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except Exception as e:
        print(f"Server Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
