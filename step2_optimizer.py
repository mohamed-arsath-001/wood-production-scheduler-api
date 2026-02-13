import pandas as pd
from datetime import datetime, timedelta

def train_performance_model(history_df):
    """
    Ranks workers by speed for every Machine + Product combination 
    based on exact names in the database.
    """
    kb = {'performance_rank': {}, 'fallback_rank': {}}
    if history_df is None or history_df.empty:
        return kb

    if all(x in history_df.columns for x in ['stationName', 'productName', 'operatorTeam', 'cycleTimePerOneUnit(sec)']):
        history_df['cycleTimePerOneUnit(sec)'] = pd.to_numeric(history_df['cycleTimePerOneUnit(sec)'], errors='coerce')
        
        # Rank by lowest cycle time (fastest first)
        perf = history_df.groupby(['stationName', 'productName', 'operatorTeam'])['cycleTimePerOneUnit(sec)'].mean().reset_index()
        perf = perf.sort_values('cycleTimePerOneUnit(sec)')
        
        for _, row in perf.iterrows():
            key = (row['stationName'], row['productName'])
            if key not in kb['performance_rank']: kb['performance_rank'][key] = []
            kb['performance_rank'][key].append({'name': row['operatorTeam'], 'speed': row['cycleTimePerOneUnit(sec)']})

        # Fallback: Best workers per Machine in general
        fallback = history_df.groupby(['stationName', 'operatorTeam'])['cycleTimePerOneUnit(sec)'].mean().reset_index()
        fallback = fallback.sort_values('cycleTimePerOneUnit(sec)')
        for _, row in fallback.iterrows():
            if row['stationName'] not in kb['fallback_rank']: kb['fallback_rank'][row['stationName']] = []
            kb['fallback_rank'][row['stationName']].append({'name': row['operatorTeam'], 'speed': row['cycleTimePerOneUnit(sec)']})
    return kb

def run_optimizer(df, history_df=None):
    """
    Weekly AI Planner:
    - Starts the schedule on the next Monday.
    - Sorts by Machine for contiguous processing.
    - Assigns workers based on direct machine name matches in the DB.
    """
    brain = train_performance_model(history_df)
    machine_clocks = {}
    schedule = []
    
    # --- WEEKLY START LOGIC ---
    # Find the next Monday at 06:00 AM
    today = datetime.now()
    days_ahead = 0 - today.weekday()
    if days_ahead <= 0: days_ahead += 7 
    start_base = (today + timedelta(days=days_ahead)).replace(hour=6, minute=0, second=0, microsecond=0)

    # Site Detection for sheet organization
    def detect_site(row):
        m, s = str(row.get('Machine', '')).upper(), str(row.get('Source_File', '')).upper()
        if 'BXB' in s or 'BXB' in m: return 'Boksburg'
        if 'PRF' in s or 'PRF' in m: return 'Piet Retief'
        if 'UGI' in s or 'UGI' in m: return 'Ugie'
        return 'Boksburg'

    df['Site'] = df.apply(detect_site, axis=1)
    
    # --- CRITICAL FIX: RESET INDEX AFTER SORTING ---
    # Sorting ensures contiguous machine orders. Resetting index ensures the loop works correctly.
    df = df.sort_values(by=['Site', 'Machine']).reset_index(drop=True)

    for index, row in df.iterrows():
        machine = str(row['Machine'])
        product = str(row['Product'])
        qty = row['Qty']

        # AI PERFORMANCE ASSIGNMENT (Direct Match)
        experts = brain['performance_rank'].get((machine, product), [])
        if not experts:
            experts = brain['fallback_rank'].get(machine, [])
        
        assigned_worker = "Standard Team"
        calibrated_speed = 25 # Default seconds per unit

        if experts:
            # Assign the #1 performer
            assigned_worker = experts[0]['name']
            calibrated_speed = experts[0]['speed']

        duration_mins = max(15, int((qty * calibrated_speed) / 60))

        # Scheduling
        if machine not in machine_clocks:
            machine_clocks[machine] = start_base
        
        start_time = machine_clocks[machine]
        
        # Add 45-min setup time if product changes on the same machine
        # Because we reset_index, 'index-1' now correctly refers to the previous row in the sorted list
        if index > 0:
            prev_row = df.iloc[index-1]
            if prev_row['Machine'] == machine and prev_row['Product'] != product:
                start_time += timedelta(minutes=45)

        end_time = start_time + timedelta(minutes=duration_mins)
        machine_clocks[machine] = end_time

        # Save results
        row['Assigned_Team'] = assigned_worker
        row['Planned_Day'] = start_time.strftime("%A (%b %d)") 
        row['Start_Time'] = start_time.strftime("%Y-%m-%d %H:%M")
        row['End_Time'] = end_time.strftime("%Y-%m-%d %H:%M")
        row['Duration_Mins'] = duration_mins
        
        # Shift Logic
        hour = start_time.hour
        if 6 <= hour < 14: shift = "Morning"
        elif 14 <= hour < 22: shift = "Afternoon"
        else: shift = "Night"
        row['Shift'] = shift
        
        schedule.append(row)

    return pd.DataFrame(schedule)
