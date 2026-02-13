import pandas as pd
from datetime import datetime, timedelta

START_DATE = datetime(2026, 2, 14, 6, 0, 0)  # Pilot Start Date
CHANGEOVER_MINS = 45

# Rules: (Texture, Banned Machine)
RESTRICTED_PAIRS = [("PND", "BXB-2"), ("PND", "MKD_MFB_3")]

def get_shift(dt):
    """4 SHIFTS LOGIC (6 Hours Each)"""
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

    # 3. Worker Assignment Check
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

    pd.DataFrame(dashboard_data).to_excel(writer, sheet_name="Dashboard", index=False)

def solve_lite(site_name, df_site, all_orders_accumulator):
    """
    Schedule orders for a single site.
    Uses standardized column names from step1_ingest:
      - Product (was productGroupName)
      - Machine (was Machine_Name)
      - Qty (was totalQty)
      - Order_ID (was productionOrderNumber)
      - Assigned_Team
    """
    print(f"   ... Scheduling Site: {site_name}")
    if len(df_site) == 0:
        return []

    # --- INTELLIGENT SORTING ---
    df_site = df_site.copy()
    df_site['sort_key'] = df_site['Product'].fillna('')

    # Sort by Machine then Product to minimize changeovers
    sort_cols = ['Machine', 'sort_key']
    available_cols = [c for c in sort_cols if c in df_site.columns]
    if available_cols:
        df_site = df_site.sort_values(by=available_cols)

    machines = df_site['Machine'].unique() if 'Machine' in df_site.columns else []
    machine_clocks = {m: START_DATE for m in machines}
    last_product_on_machine = {m: None for m in machines}

    schedule = []

    for idx, row in df_site.iterrows():
        try:
            qty = float(row.get('Qty', 0))
            if qty <= 0:
                continue

            product_desc = str(row.get('Product', 'Unknown'))
            target_machine = str(row.get('Machine', 'Default'))
            assigned_team = str(row.get('Assigned_Team', 'TBD'))
            order_id = row.get('Order_ID', f'ORD-{idx}')

            # --- BAN RULES ---
            is_banned = False
            for texture, banned_machine in RESTRICTED_PAIRS:
                if texture in product_desc and banned_machine == target_machine:
                    is_banned = True
                    break
            if is_banned:
                continue

            # --- CHANGEOVER LOGIC (45 Mins) ---
            if target_machine not in machine_clocks:
                machine_clocks[target_machine] = START_DATE

            if last_product_on_machine.get(target_machine) is not None:
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
                'Order_ID': order_id,
                'Product': product_desc,
                'Qty': qty,
                'Start_Time': start_time.strftime("%Y-%m-%d %H:%M"),
                'End_Time': end_time.strftime("%Y-%m-%d %H:%M"),
                'Duration_Mins': duration_mins,
                'Assigned_Team': assigned_team,
                'Shift': shift_name,
                'Status': "Scheduled"
            }
            schedule.append(entry)
            all_orders_accumulator.append(entry)

        except Exception as e:
            continue

    return schedule


def run_optimizer(df):
    """
    Accepts a cleaned DataFrame from step1_ingest.standardize_columns().
    Runs multi-site optimization and returns an optimized DataFrame.
    
    Uses standardized column names: Order_ID, Product, Qty, Machine, Assigned_Team, Site
    """
    print("--- STEP 2: MULTI-SITE OPTIMIZATION ---")
    
    all_orders_accumulator = []

    # Determine site column (step1_ingest uses 'Site', tagged in main.py)
    site_col = 'Site' if 'Site' in df.columns else ('Plant' if 'Plant' in df.columns else None)

    if site_col:
        unique_sites = df[site_col].unique()
        
        for site in unique_sites:
            site_str = str(site)
            if site_str != "nan" and site_str != "Unknown":
                df_site = df[df[site_col] == site].copy()
                solve_lite(site_str, df_site, all_orders_accumulator)
    else:
        # No site column — treat everything as one site
        solve_lite("All", df, all_orders_accumulator)

    if not all_orders_accumulator:
        print("⚠️ No orders were scheduled. Returning empty DataFrame.")
        return pd.DataFrame(columns=[
            'Machine', 'Order_ID', 'Product', 'Qty',
            'Start_Time', 'End_Time', 'Duration_Mins',
            'Assigned_Team', 'Shift', 'Status'
        ])

    optimized_df = pd.DataFrame(all_orders_accumulator)
    print(f"✅ Optimization complete. {len(optimized_df)} orders scheduled.")
    return optimized_df


if __name__ == "__main__":
    # Standalone mode: read from CSV file (for local testing)
    import os
    INPUT_FILE = "cleaned_production_data.csv"
    if os.path.exists(INPUT_FILE):
        df = pd.read_csv(INPUT_FILE)
        result = run_optimizer(df)
        print(result)
    else:
        print(f"❌ {INPUT_FILE} not found")
