from flask import Flask, request, jsonify, send_file
import os
import step1_ingest
import step2_optimizer

app = Flask(__name__)

# --- CONFIGURATION ---
STANDARD_FILENAME_EXCEL = "DummyData.xlsx"
STANDARD_FILENAME_CSV = "DummyData.csv"
OUTPUT_FILENAME = "Final_POC_Schedule.xlsx"

@app.route('/', methods=['GET'])
def home():
    return "AI Scheduler is Running!", 200

@app.route('/step1-ingest', methods=['POST'])
def ingest():
    print("--- STEP 1: RECEIVING FILE ---")
    if 'data' not in request.files:
        return jsonify({"status": "error", "message": "No file part named 'data'"}), 400

    file = request.files['data']
    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400

    filename = file.filename.lower()
    if filename.endswith('.csv'):
        save_path = STANDARD_FILENAME_CSV
        if os.path.exists(STANDARD_FILENAME_EXCEL): os.remove(STANDARD_FILENAME_EXCEL)
    else:
        save_path = STANDARD_FILENAME_EXCEL
        if os.path.exists(STANDARD_FILENAME_CSV): os.remove(STANDARD_FILENAME_CSV)

    file.save(save_path)
    print(f"✅ File saved internally as: {save_path}")

    try:
        step1_ingest.run_ingest()
        return jsonify({"status": "success", "message": "File ingested and cleaned"}), 200
    except Exception as e:
        print(f"❌ Error in Step 1: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/step2-optimize', methods=['POST'])
def optimize():
    print("--- STEP 2: STARTING OPTIMIZATION ---")
    try:
        step2_optimizer.run_optimizer()
        if not os.path.exists(OUTPUT_FILENAME):
             return jsonify({"status": "error", "message": "Optimization failed"}), 500
        return send_file(OUTPUT_FILENAME, as_attachment=True)
    except Exception as e:
        print(f"❌ Error in Step 2: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # Cloud uses Gunicorn, but this runs locally if needed
    app.run(host='0.0.0.0', port=5000)