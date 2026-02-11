import streamlit as st
import pandas as pd
import step1_ingest
import step2_optimizer
import os
import requests # Moved to the top for clean code

# --- PAGE CONFIG ---
st.set_page_config(page_title="Zestflow AI Scheduler", layout="wide")

# --- LOGIN LOGIC ---
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

def login():
    st.title("🔐 Zestflow Client Portal")
    user = st.text_input("Username")
    pw = st.text_input("Password", type="password")
    if st.button("Login"):
        if user == "admin" and pw == "zestflow2026": 
            st.session_state['logged_in'] = True
            st.rerun()
        else:
            st.error("Invalid credentials")

# --- MAIN APP ---
if not st.session_state['logged_in']:
    login()
else:
    st.sidebar.title("Zestflow Navigation")
    page = st.sidebar.radio("Go to", ["Upload & Schedule", "Plan Dashboard", "AI Planner Chat"])

    # PAGE 1: UPLOAD & N8N TRIGGER
    if page == "Upload & Schedule":
        st.header("📤 Upload Production Data")
        uploaded_file = st.file_uploader("Choose an Excel/CSV file", type=['xlsx', 'csv'])
        
        if uploaded_file:
            with open("DummyData.xlsx", "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            if st.button("🚀 Run AI Optimizer"):
                with st.spinner("Processing 1000+ orders..."):
                    # 1. Run the Brain
                    step1_ingest.run_ingest()
                    step2_optimizer.run_optimizer()
                    st.success("Schedule Generated Successfully!")

                    # 2. Trigger n8n (This must be INSIDE the button block)
                    st.info("🔄 Sending plan to n8n for email distribution...")
                    n8n_webhook_url = "https://YOUR_N8N_URL_HERE" # Replace with your real URL
                    
                    try:
                        with open("Final_POC_Schedule.xlsx", "rb") as f:
                            files = {'data': f}
                            response = requests.post(n8n_webhook_url, files=files)
                            
                        if response.status_code == 200:
                            st.success("✅ n8n Notified! Emails are being sent.")
                        else:
                            st.error(f"⚠️ n8n returned error code: {response.status_code}")
                    except Exception as e:
                        st.error(f"❌ Connection failed: {str(e)}")

    # PAGE 2: DASHBOARD
    elif page == "Plan Dashboard":
        st.header("📊 Current Production Plan")
        if os.path.exists("Final_POC_Schedule.xlsx"):
            df_dash = pd.read_excel("Final_POC_Schedule.xlsx", sheet_name="Dashboard")
            st.table(df_dash)
            with open("Final_POC_Schedule.xlsx", "rb") as file:
                st.download_button("Download Full Excel", data=file, file_name="Zestflow_Schedule.xlsx")
        else:
            st.warning("No plan found. Please upload data first.")

    # PAGE 3: AI CHAT
    elif page == "AI Planner Chat":
        st.header("🤖 AI Planner Assistant")
        if os.path.exists("cleaned_production_data.csv"):
            user_question = st.text_input("Ask about worker assignments or bottlenecks:")
            if user_question:
                if "worker" in user_question.lower() or "team" in user_question.lower():
                    st.write("🔍 **AI Analysis:** Assignments are based on 'Assigned_Team' data to ensure shift continuity.")
                elif "why" in user_question.lower():
                    st.write("🔍 **AI Analysis:** Optimization prioritized the 45-min changeover rule to maximize uptime.")
        else:
            st.error("Please generate a plan first.")
