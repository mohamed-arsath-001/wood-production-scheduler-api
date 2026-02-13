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

# --- CORS SETTINGS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    """Health check endpoint to wake up Render."""
    return {"message": "Zestflow Scheduler API is Running"}

@app.post("/optimize")
async def optimize_schedule(files: List[UploadFile] = File(...)):
    """
    1. Receives multiple Excel files.
    2. Merges and Cleans them.
    3. Runs Optimizer.
    4. Emails via n8n.
    5. Returns JSON to Frontend.
    """
    # START OF MAIN TRY BLOCK
    try:
        combined_data = []
        file_count = 0

        # --- STEP 1: READ & MERGE FILES ---
        print(f"📥 Receiving {len(files)} files...")
        
        for file in files:
            content = await file.read()
            filename = file.filename.lower()
            file_count += 1
            
            try:
                if filename.endswith('.csv'):
                    df = pd.read_csv(io.BytesIO(content))
                else:
                    df = pd.read_excel(io.BytesIO(content))
            except Exception as e:
                print(f"⚠️ Could not read {filename}: {e}")
                continue 

            # Smart Site Tagging
            if "bxb" in filename:
                df['Site'] = 'Boksburg'
            elif "ugi" in filename or "prf" in filename or "mkd" in filename:
                df['Site'] = 'Mkhondo'
            else:
                df['Site'] = 'Unknown'

            combined_data.append(df)

        if not combined_data:
            raise HTTPException(status_code=400, detail="No valid files received")

        master_df = pd.concat(combined_data, ignore_index=True)
        print(f"✅ Merged {file_count} files. Total rows: {len(master_df)}")

        # --- STEP 2: CLEAN & MAP COLUMNS ---
        cleaned_df = step1_ingest.standardize_columns(master_df)

        # --- STEP 3: RUN OPTIMIZER ---
        optimized_df = step2_optimizer.run_optimizer(cleaned_df)
        
        # --- STEP 4: GENERATE EXCEL FOR N8N ---
        output = io.BytesIO()
        # Using xlsxwriter to save the dataframe to memory
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            optimized_df.to_excel(writer, index=False, sheet_name='Optimized_Schedule')
        
        output.seek(0)

        # --- STEP 5: SEND TO N8N ---
        n8n_url = "https://abi2026.app.n8n.cloud/webhook/process-schedule"
        
        files_payload = {
            'data': ('Final_Schedule.xlsx', output, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        }
        
        n8n_status = "Skipped"
        try:
            response = requests.post(n8n_url, files=files_payload)
            if response.status_code == 200:
                n8n_status = "Email Sent Successfully"
                print("✅ Sent to n8n")
            else:
                n8n_status = f"n8n Error: {response.status_code}"
                print(f"❌ n8n returned {response.status_code}")
        except Exception as e:
            n8n_status = f"Connection Failed: {str(e)}"
            print(f"❌ Could not connect to n8n: {e}")

        # --- STEP 6: RETURN JSON ---
        result_json = optimized_df.head(100).to_dict(orient="records")
        
        return {
            "status": "success",
            "files_processed": file_count,
            "total_orders": len(optimized_df),
            "n8n_delivery": n8n_status,
            "data": result_json
        }

    # THIS EXCEPT BLOCK MUST BE ALIGNED WITH THE 'try' AT THE TOP
    except Exception as e:
        print(f"❌ Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
