import pandas as pd
from datetime import datetime, timedelta
import os

INPUT_FILE = "cleaned_production_data.csv"
OUTPUT_FILE = "Final_POC_Schedule.xlsx"
START_DATE = datetime(2026, 2, 14, 6, 0, 0) # Pilot Start Date
CHANGEOVER_MINS = 45 

# Rules: (Texture, Banned Machine)
RESTRICTED_PAIRS = [("PND", "BXB-2"), ("PND", "MKD_MFB_3")]

def get_shift(dt):
    # --- 4 SHIFTS LOGIC (6 Hours Each) ---
    h = dt.hour
    if 6 <= h < 12:
        return "Morning (06:00 - 12:00)"
    elif 12 <= h < 18:
        return "Afternoon (12:00 - 18:00)"
    elif 18 <= h <= 23:
        return "Evening (18:00 - 00:00)"
    else:
        return "Night (00:00 - 06:00)"

def create_dashboard(all_orders, writer):
    print("   ... Generating Executive Dashboard")
    if not all_orders: return

    df_all = pd.DataFrame(all_orders)
    
    # CALCULATIONS
    total_orders = len(df_all)
    total_qty = df_all['Qty'].sum()
    avg_duration = df_all['Duration_Mins'].mean()
    
    # 1. Shift Distribution
    shift_counts = df_all['Shift'].value_counts()
    
    # 2. Machine Load
    machine_counts = df_all['Machine'].value_counts()

    # 3. Worker Assignment Check (Count how many have assigned teams)
    assigned_teams_count = df_all[df_all['Assigned_Team'] != "TBD"].shape[0]

    # BUILD DASHBOARD DATA
    dashboard_data = [
        {"Metric": "ZESTFLOW PILOT DASHBOARD", "Value": ""},
        {"Metric": "---------------------------", "Value": "---"},
        {"Metric": "Total Orders Scheduled", "Value": total_orders},
        {"Metric": "Total Units Produced", "Value": total_qty},
        {"Metric": "Avg Batch Duration (mins)", "Value": round(avg_duration, 1)},
        {"Metric": "Orders with Assigned Teams", "Value": f"{assigned_teams_count} / {total_orders}"},
        {"Metric": "", "Value": ""}, 
        {"Metric": "BATCHES PER SHIFT", "Value": ""},
        {"Metric": "Morning (06-12)", "Value": shift_counts.get("Morning (06:00 - 12:00)", 0)},
        {"Metric": "Afternoon (12-18)", "Value": shift_counts.get("Afternoon (12:00 - 18:00)", 0)},
        {"Metric": "Evening (18-00)", "Value": shift_counts.get("Evening (18:00 - 00:00)", 0)},
        {"Metric": "Night (00-06)", "Value": shift_counts.get("Night (00:00 - 06:00)", 0)},
        {"Metric": "", "Value": ""},
        {"Metric": "MACHINE UTILIZATION (Batches)", "Value": ""}
    ]
    
    for machine, count in machine_counts.items():
        dashboard_data.append({"Metric": machine, "Value": count})

    # Write Dashboard
    pd.DataFrame(dashboard_data).to_excel(writer, sheet_name="Dashboard", index=False)

def solve_lite(site_name, df_site, writer, all_orders_accumulator):
    print(f"   ... Scheduling Site: {site_name}")
    if len(df_site) == 0: return False

    # --- INTELLIGENT SORTING (The AI Substitute) ---
    # Group by Product to minimize changeovers, then by Machine preference
    df_site['sort_key'] = df_site['productGroupName'].fillna('')
    df_site = df_site.sort_values(by=['Machine_Name', 'sort_key'])

    machines = df_site['Machine_Name'].unique()
    machine_clocks = {m: START_DATE for m in machines}
    last_product_on_machine = {m: None for m in machines}
    
    schedule = []

    for idx, row in df_site.iterrows():
        try:
            qty = float(row['totalQty'])
            if qty <= 0: continue
            
            product_desc = str(row['productGroupName'])
            target_machine = str(row['Machine_Name'])
            # Grab Team Name (passed from Step 1)
            assigned_team = str(row.get('Assigned_Team', 'TBD'))

            # --- BAN RULES ---
            is_banned = False
            for texture, banned_machine in RESTRICTED_PAIRS:
                if texture in product_desc and banned_machine == target_machine:
                    is_banned = True
                    break
            if is_banned: continue

            # --- CHANGEOVER LOGIC (45 Mins) ---
            if target_machine not in machine_clocks: 
                machine_clocks[target_machine] = START_DATE
            
            # If product changed, add delay
            if last_product_on_machine[target_machine] is not None:
                if last_product_on_machine[target_machine] != product_desc:
                    machine_clocks[target_machine] += timedelta(minutes=CHANGEOVER_MINS)
            
            last_product_on_machine[target_machine] = product_desc

            # --- DURATION & TIME ---
            duration_mins = max(15, int((qty / 100) * 1.5))
            start_time = machine_clocks[target_machine]
            end_time = start_time + timedelta(minutes=duration_mins)
            
            # --- SHIFT ASSIGNMENT ---
            shift_name = get_shift(start_time)
            
            # Update Clock
            machine_clocks[target_machine] = end_time

            entry = {
                'Machine': target_machine,
                'Order_ID': row.get('productionOrderNumber'),
                'Product': product_desc,
                'Qty': qty,
                'Start_Time': start_time.strftime("%Y-%m-%d %H:%M"),
                'End_Time': end_time.strftime("%Y-%m-%d %H:%M"),
                'Duration_Mins': duration_mins,
                'Assigned_Team': assigned_team,   # <--- WORKER INCLUDED
                'Shift': shift_name,              # <--- SHIFT INCLUDED
                'Status': "Scheduled"
            }
            schedule.append(entry)
            all_orders_accumulator.append(entry)

        except Exception as e:
            continue

    if schedule:
        safe_name = str(site_name)[:30].replace("/", "-")
        pd.DataFrame(schedule).to_excel(writer, sheet_name=safe_name, index=False)
        return True
    return False

def run_optimizer():
    print("--- STEP 2: MULTI-SITE OPTIMIZATION (FINAL DASHBOARD) ---")
    try:
        if not os.path.exists(INPUT_FILE): return

        df = pd.read_csv(INPUT_FILE)
        all_orders_accumulator = [] 
        
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            unique_sites = df['Plant'].unique()
            sheets_created = 0
            
            for site in unique_sites:
                if str(site) != "nan" and str(site) != "Unknown":
                    # Pass the accumulator to collect data for Dashboard
                    if solve_lite(str(site), df[df['Plant'] == site].copy(), writer, all_orders_accumulator):
                        sheets_created += 1
            
            # --- CREATE DASHBOARD SHEET ---
            if sheets_created > 0:
                create_dashboard(all_orders_accumulator, writer)
            else:
                pd.DataFrame({'Error': ['No Valid Schedules']}).to_excel(writer, sheet_name="Report")

        print(f"✅ FINAL SUCCESS: {OUTPUT_FILE} is ready.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_optimizer()
