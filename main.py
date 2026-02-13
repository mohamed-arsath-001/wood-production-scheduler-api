from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import pandas as pd
import io
import step1_ingest
import step2_optimizer
import requests

app = FastAPI()

# --- CORS SETTINGS (Connecting to Vercel) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all origins for the Pilot Phase
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"message": "Zestflow Scheduler API is Running"}

@app.post("/optimize")
async def optimize_schedule(files: List[UploadFile] = File(...)):
    """
    1. Receives multiple Excel files (BXB, PRF, UGI).
    2. Tags them with the correct Site.
    3. Merges them into one Master Dataset.
    4. Runs the Deterministic Optimizer.
    5. Returns the JSON result to the Frontend.
    """
    try:
        combined_data = []
        file_count = 0

        # --- STEP 1: READ & MERGE FILES ---
        for file in files:
            content = await file.read()
            filename = file.filename.lower()
            file_count += 1
            
            # Read the Excel file
            try:
                df = pd.read_excel(io.BytesIO(content))
            except Exception:
                # Fallback for CSV if they upload mixed formats
                df = pd.read_csv(io.BytesIO(content))

            # Smart Site Tagging (Crucial for the Logic)
            if "bxb" in filename:
                df['Site'] = 'Boksburg'
            elif "ugi" in filename or "prf" in filename or "mkd" in filename:
                df['Site'] = 'Mkhondo'
            else:
                df['Site'] = 'Unknown' # Will be handled by optimizer default

            combined_data.append(df)

        if not combined_data:
            raise HTTPException(status_code=400, detail="No valid files received")

        # Create Master DataFrame
        master_df = pd.concat(combined_data, ignore_index=True)
        print(f"✅ Successfully merged {file_count} files. Total rows: {len(master_df)}")

        # --- STEP 2: CLEAN & MAP COLUMNS ---
        cleaned_df = step1_ingest.standardize_columns(master_df)

        # --- STEP 3: RUN OPTIMIZER ---
        # We assume step2 returns the processed DataFrame. 
        # If your step2 returns bytes/file, we might need to adjust, but usually it returns a DF.
        optimized_df = step2_optimizer.run_optimizer(cleaned_df)
        
        # --- STEP 4: PREPARE OUTPUT ---
        # Convert to Dictionary for the Frontend Dashboard
        result_json = optimized_df.head(50).to_dict(orient="records") # Send first 50 rows for preview
        
        # Note: We are NOT calling n8n here yet, per your instruction. 
        # We focus on getting the logic right first.

        return {
            "status": "success",
            "files_processed": file_count,
            "total_orders": len(optimized_df),
            "preview": result_json
        }

    except Exception as e:
        print(f"❌ Critical Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
