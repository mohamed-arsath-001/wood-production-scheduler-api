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
import xlsxwriter
from xlsxwriter.utility import xl_col_to_name

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"]
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

            cleaned_df = step1_ingest.standardize_columns(df)
            combined_data.append(cleaned_df)

        if not combined_data:
            raise HTTPException(status_code=400, detail="No valid data found")

        master_df = pd.concat(combined_data, ignore_index=True)
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

        # --- STEP 3: CREATE MULTI-SHEET EXCEL FILE WITH COLORS ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Define our custom colors
            mega_order_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}) # Red
            setup_format = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})      # Yellow
            
            # TAB 1: DASHBOARD
            total_orders = len(optimized_master)
            qty_col_name = next((c for c in optimized_master.columns if 'qty' in str(c).lower() and 'actual' not in str(c).lower()), None)
            total_units = optimized_master[qty_col_name].sum() if qty_col_name else 0
            
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

            # TABS 2-4: FACTORY SHEETS WITH CONDITIONAL FORMATTING
            sites = ['Boksburg', 'Piet Retief', 'Ugie']
            for site in sites:
                if 'Site' in optimized_master.columns:
                    df_site = optimized_master[optimized_master['Site'] == site]
                    if not df_site.empty:
                        df_site.to_excel(writer, index=False, sheet_name=site)
                        worksheet = writer.sheets[site]
                        
                        max_row = len(df_site)
                        max_col = len(df_site.columns) - 1
                        
                        # Apply RED highlighting to Mega Orders (> 5000 units)
                        if qty_col_name:
                            qty_idx = df_site.columns.get_loc(qty_col_name)
                            qty_letter = xl_col_to_name(qty_idx)
                            
                            worksheet.conditional_format(1, 0, max_row, max_col, {
                                'type': 'formula',
                                'criteria': f'=${qty_letter}2>=5000',
                                'format': mega_order_format
                            })

                        # Apply YELLOW highlighting just to the Setup Time cell if there is a 30 min penalty
                        if 'Setup_Time_Mins' in df_site.columns:
                            setup_idx = df_site.columns.get_loc('Setup_Time_Mins')
                            worksheet.conditional_format(1, setup_idx, max_row, setup_idx, {
                                'type': 'cell',
                                'criteria': '>',
                                'value': 0,
                                'format': setup_format
                            })

        output.seek(0)

        # --- STEP 4: GENERATE DYNAMIC FILENAME & DOWNLOAD ---
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_name = f"plan [{current_date}].xlsx" 
        
        if 'Site' in optimized_master.columns:
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
