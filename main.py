from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List
import pandas as pd
import io
import step1_ingest
import step2_optimizer
import traceback
from datetime import datetime

app = FastAPI()

# --- CORS SETTINGS (Updated to expose the filename to the frontend) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"] # Crucial for dynamic frontend downloads
)

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

            # Step 1: Ingest & Clean (Preserving exact format)
            cleaned_df = step1_ingest.standardize_columns(df)
            combined_data.append(cleaned_df)

        if not combined_data:
            raise HTTPException(status_code=400, detail="No valid data found")

        master_df = pd.concat(combined_data, ignore_index=True)

        # Step 2: Run the new Machine-Centric Optimizer
        optimized_master = step2_optimizer.run_optimizer(master_df)

        # --- ORGANIZE COLUMNS ---
        if 'Production_Line' in optimized_master.columns:
            new_cols = ['Production_Line', 'Batch_ID', 'Planned_Day', 'Start_Time', 'End_Time', 'Setup_Time_Mins', 'Run_Time_Mins']
            
            valid_new_cols = [c for c in new_cols if c in optimized_master.columns]
            existing_cols = [c for c in optimized_master.columns if c not in valid_new_cols and c != 'Site']
            
            final_cols = valid_new_cols + existing_cols
            if 'Site' in optimized_master.columns:
                final_cols.append('Site')
                
            optimized_master = optimized_master[final_cols]

        # --- STEP 3: CREATE MULTI-SHEET EXCEL FILE ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            
            # TAB 1: DASHBOARD
            total_orders = len(optimized_master)
            qty_col = next((c for c in optimized_master.columns if 'qty' in str(c).lower()), None)
            total_units = optimized_master[qty_col].sum() if qty_col else 0
            
            dashboard_data = [
                {'Metric': 'MASTER PRODUCTION PLAN', 'Value': ''},
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
                if not df_bxb.empty: df_bxb.to_excel(writer, index=False, sheet_name='Boksburg')

            # TAB 3: PIET RETIEF
            if 'Site' in optimized_master.columns:
                df_prf = optimized_master[optimized_master['Site'] == 'Piet Retief']
                if not df_prf.empty: df_prf.to_excel(writer, index=False, sheet_name='Piet Retief')

            # TAB 4: UGIE
            if 'Site' in optimized_master.columns:
                df_ugi = optimized_master[optimized_master['Site'] == 'Ugie']
                if not df_ugi.empty: df_ugi.to_excel(writer, index=False, sheet_name='Ugie')

        output.seek(0)

        # --- STEP 4: GENERATE DYNAMIC FILENAME & DOWNLOAD ---
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_name = f"plan [{current_date}].xlsx" # Default fallback
        
        if 'Site' in optimized_master.columns:
            # Get a list of unique sites, ignoring empty/unknown ones if needed
            unique_sites = [str(s) for s in optimized_master['Site'].dropna().unique() if str(s) != "Unknown"]
            
            if len(unique_sites) > 0:
                sites_string = ", ".join(unique_sites)
                file_name = f"plan({sites_string}) [{current_date}].xlsx"

        headers = {
            'Content-Disposition': f'attachment; filename="{file_name}"'
        }
        return StreamingResponse(output, headers=headers, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except Exception as e:
        print(f"Server Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
