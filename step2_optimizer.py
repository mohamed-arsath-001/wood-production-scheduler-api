import pandas as pd
from datetime import datetime, timedelta
import math

def resolve_machine_code(name):
    """Translates plan codes to database names."""
    name = str(name).upper().strip()
    mapping = {
        'BXBF01': 'BXB_MFB_1', 'BXBF02': 'BXB_MFB_2',
        'BXBE01': 'BXB_MFB_2', 'BXB321': 'BXB_MFB_1',
        'PRFF01': 'MKD_MFB_1', 'PRFF02': 'MKD_MFB_2',
        'PRFE01': 'MKD_MFB_2', 'PRF218': 'MKD_MFB_1',
        'UGIF01': 'UGI_CONTI_1', 'UGI005': 'UGI_CONTI_1'
    }
    return mapping.get(name, name)

def train_performance_model(history_df):
    """Ranks workers by speed."""
    kb = {'performance_rank': {}, 'fallback_rank': {}}
    if history_df is None or history_df.empty: return kb

    if all(x in history_df.columns for x in ['stationName', 'productName', 'operatorTeam', 'cycleTimePerOneUnit(sec)']):
        history_df['cycleTimePerOneUnit(sec)'] = pd.to_numeric(history_df['cycleTimePerOneUnit(sec)'], errors='coerce')
        perf = history_df.groupby(['stationName', 'productName', 'operatorTeam'])['cycleTimePerOneUnit(sec)'].mean().reset_index()
        perf = perf.sort_values('cycleTimePerOneUnit(sec)')
        
        for _, row in perf.iterrows():
            key = (row['stationName'], row['productName'])
            if key not in kb['performance_rank']: kb['performance_rank'][key] = []
            kb['performance_rank'][key].append({'name': row['operatorTeam'], 'speed': row['cycleTimePerOneUnit(sec)']})

        fallback = history_df.groupby(['stationName', 'operatorTeam'])['cycleTimePerOneUnit(sec)'].mean().reset_index()
        fallback = fallback.sort_values('cycleTimePerOneUnit(sec)')
        for _, row in fallback.iterrows():
            if row['stationName'] not in kb['fallback_rank']: kb['fallback_rank'][row['stationName']] = []
            kb['fallback_rank'][row['stationName']].append({'name': row['operatorTeam'], 'speed': row['cycleTimePerOneUnit(sec)']})
    return kb

def run_optimizer(df, history_df=None):
    brain = train_performance_model(history_df)
    machine_clocks = {}
    worker_clocks = {} # Track when workers are free
    schedule = []
    
    # Weekly Start: Next Monday 06:00
    today = datetime.now()
    days_ahead = 0 - today.weekday()
    if days_ahead <= 0: days_ahead += 7 
    start_base = (today + timedelta(days=days_ahead)).replace(hour=6, minute=0, second=0, microsecond=0)

    # Site Detection
    def detect_site(row):
        m, s = str(row.get('Machine', '')).upper(), str(row.get('Source_File', '')).upper()
        if 'BXB' in s or 'BXB' in m: return 'Boksburg'
        if 'PRF' in s or 'PRF' in m: return 'Piet Retief'
        if 'UGI' in s or 'UGI' in m: return 'Ugie'
        return 'Boksburg'

    df['Site'] = df.apply(detect_site, axis=1)
    df = df.sort_values(by=['Site', 'Machine']).reset_index(drop=True)

    for index, row in df.iterrows():
        machine = str(row['Machine'])
        product = str(row['Product'])
        total_qty = row['Qty']

        # 1. Resolve Machine
        db_machine_name = resolve_machine_code(machine)

        # 2. Get Standard Speed (needed for calculation)
        calibrated_speed = 25
        experts = brain['performance_rank'].get((db_machine_name, product), [])
        if not experts: experts = brain['fallback_rank'].get(db_machine_name, [])
        if experts: calibrated_speed = experts[0]['speed']

        # --- FAULT 2 FIX: BATCH SPLITTING ---
        # If duration > 16 hours, split into smaller chunks
        MAX_BATCH_HRS = 16
        units_per_hr = (3600 / calibrated_speed)
        max_qty_per_batch = int(units_per_hr * MAX_BATCH_HRS)
        
        # Calculate how many splits we need
        num_splits = math.ceil(total_qty / max_qty_per_batch)
        
        for i in range(num_splits):
            # Calculate Qty for this sub-batch
            qty_this_batch = min(max_qty_per_batch, total_qty - (i * max_qty_per_batch))
            
            # 3. Schedule Time
            if machine not in machine_clocks: machine_clocks[machine] = start_base
            start_time = machine_clocks[machine]
            
            # Setup Time (Only on first sub-batch if product changed)
            if i == 0 and index > 0:
                prev_row = df.iloc[index-1]
                if prev_row['Machine'] == machine and prev_row['Product'] != product:
                    start_time += timedelta(minutes=45)

            # 4. Assign Worker (With Fatigue Buffer)
            assigned_worker = "Standard Team"
            found_worker = False
            
            if experts:
                for expert in experts:
                    w_name = expert['name']
                    # Check if worker is free AND has rested (Fatigue Buffer)
                    # We assume they need 8 hours rest after a shift, but for simplicity here,
                    # we just check if they are free at the start time.
                    if worker_clocks.get(w_name, start_base) <= start_time:
                        assigned_worker = w_name
                        calibrated_speed = expert['speed'] # Update speed to expert speed
                        found_worker = True
                        break
            
            if not found_worker:
                assigned_worker = f"Relief Team {df.iloc[index]['Site']}"
                calibrated_speed = 30 # Slower

            # Calculate Duration
            duration_mins = max(15, int((qty_this_batch * calibrated_speed) / 60))
            end_time = start_time + timedelta(minutes=duration_mins)

            # Update Clocks
            machine_clocks[machine] = end_time
            if found_worker:
                # Add 8 hours rest time to worker's clock so they aren't rebooked immediately
                worker_clocks[assigned_worker] = end_time + timedelta(hours=8)

            # Save Row
            new_row = row.copy()
            new_row['Qty'] = qty_this_batch
            new_row['Assigned_Team'] = assigned_worker
            new_row['Planned_Day'] = start_time.strftime("%A (%b %d)") 
            new_row['Start_Time'] = start_time.strftime("%Y-%m-%d %H:%M")
            new_row['End_Time'] = end_time.strftime("%Y-%m-%d %H:%M")
            new_row['Time_Only'] = start_time.strftime("%H:%M") 
            new_row['Duration_Mins'] = duration_mins
            new_row['Shift'] = "Morning" if 6 <= start_time.hour < 14 else "Afternoon" if 14 <= start_time.hour < 22 else "Night"
            
            # Add suffix if split
            if num_splits > 1:
                new_row['Order_ID'] = f"{row.get('Order_ID', 'UNK')}-{i+1}"
            
            schedule.append(new_row)

    return pd.DataFrame(schedule)
