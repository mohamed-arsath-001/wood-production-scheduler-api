import pandas as pd
from datetime import datetime, timedelta

def resolve_machine_code(name):
    """
    Translates plan codes to database names.
    """
    name = str(name).upper().strip()
    mapping = {
        'BXBF01': 'BXB_MFB_1', 'BXBF02': 'BXB_MFB_2',
        'BXBE01': 'BXB_MFB_2', 'BXB321': 'BXB_MFB_1',
        'BXBRF1': 'BXB_MFB_1', 'BXBSX1': 'BXB_MFB_1',
        'PRFF01': 'MKD_MFB_1', 'PRFF02': 'MKD_MFB_2',
        'PRFE01': 'MKD_MFB_2', 'PRF218': 'MKD_MFB_1',
        'PRFRF1': 'MKD_MFB_1', 'UGIF01': 'UGI_CONTI_1',
        'UGI005': 'UGI_CONTI_1', 'UGI003': 'UGI_CONTI_1'
    }
    return mapping.get(name, name)

def train_performance_model(history_df):
    """
    Ranks workers by speed for every Machine + Product.
    """
    kb = {'performance_rank': {}, 'fallback_rank': {}}
    if history_df is None or history_df.empty:
        return kb

    if all(x in history_df.columns for x in ['stationName', 'productName', 'operatorTeam', 'cycleTimePerOneUnit(sec)']):
        history_df['cycleTimePerOneUnit(sec)'] = pd.to_numeric(history_df['cycleTimePerOneUnit(sec)'], errors='coerce')
        
        # Rank by speed (fastest first)
        perf = history_df.groupby(['stationName', 'productName', 'operatorTeam'])['cycleTimePerOneUnit(sec)'].mean().reset_index()
        perf = perf.sort_values('cycleTimePerOneUnit(sec)')
        
        for _, row in perf.iterrows():
            key = (row['stationName'], row['productName'])
            if key not in kb['performance_rank']: kb['performance_rank'][key] = []
            kb['performance_rank'][key].append({'name': row['operatorTeam'], 'speed': row['cycleTimePerOneUnit(sec)']})

        # Fallback ranking
        fallback = history_df.groupby(['stationName', 'operatorTeam'])['cycleTimePerOneUnit(sec)'].mean().reset_index()
        fallback = fallback.sort_values('cycleTimePerOneUnit(sec)')
        for _, row in fallback.iterrows():
            if row['stationName'] not in kb['fallback_rank']: kb['fallback_rank'][row['stationName']] = []
            kb['fallback_rank'][row['stationName']].append({'name': row['operatorTeam'], 'speed': row['cycleTimePerOneUnit(sec)']})
    return kb

def run_optimizer(df, history_df=None):
    brain = train_performance_model(history_df)
    machine_clocks = {}
    
    # --- NEW: WORKER AVAILABILITY TRACKER ---
    # Stores the datetime when each worker will be free next
    worker_clocks = {} 
    
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
    
    # Sort by Machine to keep production continuous
    df = df.sort_values(by=['Site', 'Machine']).reset_index(drop=True)

    for index, row in df.iterrows():
        machine = str(row['Machine'])
        product = str(row['Product'])
        qty = row['Qty']

        # 1. Resolve Machine Name
        db_machine_name = resolve_machine_code(machine)
        
        # 2. Determine Start Time for this Order
        if machine not in machine_clocks:
            machine_clocks[machine] = start_base
        
        start_time = machine_clocks[machine]
        
        # Setup Time Logic
        if index > 0:
            prev_row = df.iloc[index-1]
            if prev_row['Machine'] == machine and prev_row['Product'] != product:
                start_time += timedelta(minutes=45)

        # 3. AI WORKER ASSIGNMENT (WITH CONFLICT CHECK)
        experts = brain['performance_rank'].get((db_machine_name, product), [])
        if not experts:
            experts = brain['fallback_rank'].get(db_machine_name, [])
        
        assigned_worker = "Standard Team"
        calibrated_speed = 25 
        
        # Find the best available expert
        found_worker = False
        if experts:
            for expert in experts:
                worker_name = expert['name']
                worker_speed = expert['speed']
                
                # Check when this worker is free
                next_free_time = worker_clocks.get(worker_name, start_base)
                
                # If worker is free BEFORE or AT the start time of this job, assign them
                if next_free_time <= start_time:
                    assigned_worker = worker_name
                    calibrated_speed = worker_speed
                    found_worker = True
                    break
        
        # If all experts are busy, we must assign a Relief Team
        if not found_worker and experts:
            # Fallback: Just take the standard speed, assign generic team to avoid double booking
            assigned_worker = f"Relief Team ({df.iloc[index]['Site']})"
            calibrated_speed = 30 # Slower speed for relief team

        duration_mins = max(15, int((qty * calibrated_speed) / 60))
        end_time = start_time + timedelta(minutes=duration_mins)
        
        # Update Clocks
        machine_clocks[machine] = end_time
        if found_worker:
            worker_clocks[assigned_worker] = end_time

        # Save results
        row['Assigned_Team'] = assigned_worker
        row['Planned_Day'] = start_time.strftime("%A (%b %d)") 
        row['Start_Time'] = start_time.strftime("%Y-%m-%d %H:%M")
        row['End_Time'] = end_time.strftime("%Y-%m-%d %H:%M")
        row['Time_Only'] = start_time.strftime("%H:%M") 
        row['Duration_Mins'] = duration_mins
        row['Shift'] = "Morning" if 6 <= start_time.hour < 14 else "Afternoon" if 14 <= start_time.hour < 22 else "Night"
        
        schedule.append(row)

    return pd.DataFrame(schedule)
