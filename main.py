from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import pandas as pd
import io
import requests
import step1_ingest
import step2_optimizer

app = FastAPI()

# --- CORS SETTINGS ---
# Allows your Vercel frontend to talk to this Render backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all for Pilot Phase
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
    1. Receives multiple Excel files (BXB, PRF, UGI).
    2. Merges them into one Master Dataset.
    3. Runs the Deterministic Optimizer.
    4. Sends the result to n8n for email.
    5. Returns JSON to the Frontend.
    """
    try:
        combined_data = []
        file_count = 0

        # --- STEP 1: READ & MERGE FILES ---
        print(f"📥 Receiving {len(files)} files...")
        
        for file in files:
            content = await file.read()
            filename = file.filename.lower()
            file_count += 1
            
            # Read the file (Handle Excel or CSV)
            try:
                if filename.endswith('.csv'):
                    df = pd.read_csv(io.BytesIO(content))
                else:
                    df = pd.read_excel(io.BytesIO(content))
            except Exception as e:
                print(f"⚠️ Could not read {filename}: {e}")
                continue # Skip bad files

            # Smart Site Tagging (Crucial for the Logic)
            # This handles the client's specific naming convention
            if "bxb" in filename:
                df['Site'] = 'Boksburg'
            elif "ugi" in filename or "prf" in filename or "mkd" in filename:
                df['Site'] = 'Mkhondo'
            else:
                df['Site'] = 'Unknown'

            combined_data.append(df)

        if not combined_data:
            raise HTTPException(status_code=400, detail="No valid files received")

        # Create Master DataFrame
        master_df = pd.concat(combined_data, ignore_index=True)
        print(f"✅ Merged {file_count} files. Total rows: {len(master_df)}")

        # --- STEP 2: CLEAN & MAP COLUMNS ---
        # Using the standardized logic from step1_ingest
        cleaned_df = step1_ingest.standardize_columns(master_df)

        # --- STEP 3: RUN OPTIMIZER ---
        # Run the core logic
        optimized_df = step2_optimizer.run_optimizer(cleaned_df)
        
        # --- STEP 4: GENERATE EXCEL FOR N8N ---
        # We create the Excel file in memory to send it to n8n
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            optimized_df.to_excel(writer, index=False, sheet_name='Optimized_Schedule')
            # You can add a Dashboard sheet here if your step2 provides it
