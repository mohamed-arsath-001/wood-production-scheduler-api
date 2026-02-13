import pandas as pd
from datetime import datetime, timedelta

# --- A. THE LEARNING LAYER (Reads your Master DB) ---
def train_model_from_history(history_df):
    """
    Learns Machine Speeds and Worker Rosters from the 'API Data.xlsx' file.
    """
    knowledge_base = {
        'machine_speeds': {},
        'worker_roster': {}
    }

    if history_df is None or history_df.empty:
        return knowledge_base

    # 1. LEARN SPEEDS
    if 'stationName' in history_df.columns and 'cycleTimePerOneUnit(sec)' in history_df.columns:
        valid_rows = history_df[history_df['cycleTimePerOneUnit(sec)'] > 0]
        avg_speed_machine = valid_rows.groupby('stationName')['cycleTimePerOneUnit(sec)'].mean().to_dict()
        knowledge_base['machine_speeds'] = avg_speed_machine

    # 2. LEARN WORKERS
    if 'stationName' in history_df.columns and 'operatorTeam' in history_df.columns:
        roster_groups = history_df.groupby('stationName')['operatorTeam'].unique().to_dict()
        clean_roster = {k: [x for x in v if str(x) != 'nan'] for k, v in roster_groups.items()}
        knowledge_base['worker_roster'] = clean_roster

    return knowledge_base

# --- B. THE OPTIMIZER (Uses the Knowledge) ---
def run_optimizer(df, history_df=None):
    
    brain = train_model_from_history(history_df)
    
    # --- SITE DETECTION LOGIC (The Fix) ---
    def detect_site(row):
        machine = str(row.get('Machine', '')).upper()
        source = str(row.get('Source_File', '')).upper()
        
        # Priority: Check Filename first, then Machine Code
        if 'BXB' in source or 'BOKSBURG' in source or 'BXB' in machine:
            return 'Boksburg'
        elif 'PRF' in source or 'PIET' in source or 'PRF' in machine:
            return 'Piet Retief'
        elif 'UGI' in source or 'UGIE' in source or 'UGI' in machine:
            return 'Ugie'
        elif 'MKD' in source or 'MKHONDO' in source or 'MKD' in machine:
            return 'Mkhondo'
        else:
            return 'Unknown_Site'

    df['Site'] = df.apply(detect_site, axis=1)
    
    # Sort
    df = df.sort_values(by=['Site', 'Machine', 'Product'])
    
    schedule = []
    machine_clocks = {}
    
    # Start: Tomorrow 06:00
    start_base = datetime.now().replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(days=1)

    for index, row in df.iterrows():
        machine = row.get('Machine', 'Unknown')
        product = row.get('Product', 'Unknown')
        qty = pd.to_numeric(row.get('Qty', 0), errors='coerce')
        
        if machine not in machine_clocks:
            machine_clocks[machine] = {'time': start_base, 'last_product': None}
            
        current_clock = machine_clocks[machine]['time']
        last_product = machine_clocks[machine]['last_product']
        
        # Intelligent Speed
        real_cycle_time_sec = brain['machine_speeds'].get(machine, 30) 
        duration_mins = (qty * real_cycle_time_sec) / 60
        duration_mins = int(duration_mins * 1.1) 
        if duration_mins < 5: duration_mins = 5

        # Setup Time (45 mins)
        if last_product and product != last_product:
            current_clock += timedelta(minutes=45)

        start_time = current_clock
        end_time = start_time + timedelta(minutes=duration_mins)

        # Worker Assignment
        possible_workers = brain['worker_roster'].get(machine, ['Unassigned Team'])
        if not possible_workers: possible_workers = ['Standard Team']
        
        worker_idx = start_time.hour % len(possible_workers)
        assigned_worker = possible_workers[worker_idx]

        # Shift
        hour = start_time.hour
        if 6 <= hour < 14: shift = "Morning"
        elif 14 <= hour < 18: shift = "Afternoon"
        elif 18 <= hour < 22: shift = "Evening"
        else: shift = "Night"

        row['Start_Time'] = start_time.strftime("%Y-%m-%d %H:%M")
        row['End_Time'] = end_time.strftime("%Y-%m-%d %H:%M")
        row['Duration_Mins'] = duration_mins
        row['Assigned_Team'] = assigned_worker
        row['Shift'] = shift
        
        schedule.append(row)
        machine_clocks[machine]['time'] = end_time
        machine_clocks[machine]['last_product'] = product

    return pd.DataFrame(schedule)
