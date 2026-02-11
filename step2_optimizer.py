import pandas as pd
from datetime import datetime, timedelta
from ortools.sat.python import cp_model
import os

INPUT_FILE = "cleaned_production_data.csv"
OUTPUT_FILE = "Final_POC_Schedule.xlsx"
START_DATE = datetime(2026, 2, 14, 6, 0, 0)

RESTRICTED_PAIRS = [("PND", "BXB-2"), ("PND", "MKD_MFB_3")]

def solve_batch(batch_df, machine_start_times, horizon_mins):
    model = cp_model.CpModel()
    machines = list(machine_start_times.keys())
    
    intervals = {m: [] for m in machines}
    machine_ends = {m: [] for m in machines}
    results = []

    # Create variables for this batch
    for idx, row in batch_df.iterrows():
        try:
            qty = float(row['totalQty'])
            if qty <= 0: continue
            
            product_desc = str(row['productGroupName'])
            target_machine = str(row['Machine_Name'])
            
            # Rule Check
            is_banned = False
            for texture, banned_machine in RESTRICTED_PAIRS:
                if texture in product_desc and banned_machine == target_machine:
                    is_banned = True
                    break
            if is_banned: continue

            if target_machine not in machines:
                # New machine found in this batch? Add it.
                machines.append(target_machine)
                intervals[target_machine] = []
                machine_ends[target_machine] = []
                machine_start_times[target_machine] = 0

            duration = max(15, int((qty / 100) * 1.5))
            min_start = machine_start_times[target_machine]
            
            # Variable: Start must be AFTER the previous batch finished on this machine
            start = model.NewIntVar(min_start, min_start + horizon_mins, f's_{idx}')
            end = model.NewIntVar(min_start, min_start + horizon_mins + duration, f'e_{idx}')
            interval = model.NewIntervalVar(start, duration, end, f'i_{idx}')
            
            intervals[target_machine].append(interval)
            machine_ends[target_machine].append(end)
            results.append({'idx': idx, 'm': target_machine, 's': start, 'dur': duration, 'row': row})
        except:
            continue

    # Constraints
    for m in machines:
        if intervals[m]: model.AddNoOverlap(intervals[m])

    # Minimize Makespan (Finish as early as possible)
    makespan = model.NewIntVar(0, 999999, 'makespan')
    all_ends = [e for m in machines for e in machine_ends[m]]
    if all_ends:
        model.AddMaxEquality(makespan, all_ends)
        model.Minimize(makespan)

    # Solve this batch
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 10.0
    solver.parameters.num_search_workers = 1 # Single thread for stability
    status = solver.Solve(model)

    batch_results = []
    if status in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
        # Extract results and update machine finish times for next batch
        for r in results:
            start_val = solver.Value(r['s'])
            end_val = start_val + r['dur']
            
            # Update the "Ready Time" for this machine
            if end_val > machine_start_times[r['m']]:
                machine_start_times[r['m']] = end_val

            real_start = START_DATE + timedelta(minutes=start_val)
            batch_results.append({
                'Machine': r['m'],
                'Order_ID': r['row'].get('productionOrderNumber'),
                'Product': r['row'].get('productGroupName'),
                'Qty': r['row'].get('totalQty'),
                'Start_Time': real_start.strftime("%Y-%m-%d %H:%M"),
                'Duration_Mins': r['dur']
            })
            
    return batch_results

def solve_for_site(site_name, df_site, writer):
    print(f"   ... Optimizing Site: {site_name} ({len(df_site)} orders)")
    if len(df_site) == 0: return False

    # Initialize machine availability (Time 0)
    machines = df_site['Machine_Name'].unique()
    machine_finish_times = {m: 0 for m in machines}
    final_schedule = []

    # --- BATCH PROCESSOR ---
    # Chunk size of 50 is safe for any laptop
    BATCH_SIZE = 50
    
    # Sort slightly to group similar items (optional optimization)
    df_site = df_site.sort_values(by=['Machine_Name']).reset_index(drop=True)

    total = len(df_site)
    for i in range(0, total, BATCH_SIZE):
        batch_df = df_site.iloc[i : i + BATCH_SIZE]
        print(f"      Processing Batch {int(i/BATCH_SIZE)+1} (Orders {i} to {min(i+BATCH_SIZE, total)})...")
        
        # Solve chunk
        batch_res = solve_batch(batch_df, machine_finish_times, horizon_mins=20000)
        final_schedule.extend(batch_res)

    if final_schedule:
        safe_sheet_name = str(site_name)[:30].replace("/", "-")
        pd.DataFrame(final_schedule).to_excel(writer, sheet_name=safe_sheet_name, index=False)
        print(f"      ✅ {site_name}: Completed {len(final_schedule)}/{total} orders!")
        return True
    return False

def run_optimizer():
    print("--- STEP 2: MULTI-SITE OPTIMIZATION (BATCH MODE) ---")
    try:
        if not os.path.exists(INPUT_FILE): return

        df = pd.read_csv(INPUT_FILE)
        
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            unique_sites = df['Plant'].unique()
            sheets_created = 0
            
            for site in unique_sites:
                if str(site) != "nan" and str(site) != "Unknown":
                    if solve_for_site(str(site), df[df['Plant'] == site].copy(), writer):
                        sheets_created += 1
            
            if sheets_created == 0:
                pd.DataFrame({'Error': ['No Valid Schedules']}).to_excel(writer, sheet_name="Report")

        print(f"✅ FINAL SUCCESS: {OUTPUT_FILE} is ready.")

    except Exception as e:
        print(f"❌ Error in Step 2: {e}")

if __name__ == "__main__":
    run_optimizer()