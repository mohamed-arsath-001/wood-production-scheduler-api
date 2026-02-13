import pandas as pd
from datetime import datetime, timedelta
import re

def train_model_from_history(history_df):
    """
    Learns Machine Speeds and Worker Rosters from the Master Database.
    """
    knowledge_base = {
        'machine_speeds': {},  # Avg cycle time per machine
        'worker_roster': {},   # List of real workers per machine
        'site_defaults': {}    # Default speeds per factory site
    }

    if history_df is None or history_df.empty:
        return knowledge_base

    # 1. LEARN SPEEDS AND WORKERS
    # Mapping columns: stationName -> Machine, cycleTimePerOneUnit(sec) -> Speed, operatorTeam -> Worker
    if 'stationName' in history_df.columns:
        # Clean the speeds
        if 'cycleTimePerOneUnit(sec)' in history_df.columns:
            valid_speeds = history_df[history_df['cycleTimePerOneUnit(sec)'] > 0]
            knowledge_base['machine_speeds'] = valid_speeds.groupby('stationName')['cycleTimePerOneUnit(sec)'].mean().to_dict()
        
        # Clean the roster
        if 'operatorTeam' in history_df.columns:
            roster = history_df.groupby('stationName')['operatorTeam'].unique().to_dict()
            knowledge_base['worker_roster'] = {k: [str(x) for x in v if str(x) != 'nan'] for k, v in roster.items()}

    # 2. LEARN SITE DEFAULTS (Fallback if specific machine speed is missing)
    if 'factoryName' in history_df.columns and 'cycleTimePerOneUnit(sec)' in history_df.columns:
        site_avg = history_df.groupby('factoryName')['cycleTimePerOneUnit(sec)'].mean().to_dict()
        knowledge_base['site_defaults'] = site_avg

    return knowledge_base

def run_optimizer(df, history_df=None):
    """
    AI-Powered Optimizer:
    - Calibrates using historical data for real speeds and workers.
    - Site detection for Boksburg, Piet Retief, and Ugie.
    - Generates realistic Start/End times.
    """
    brain = train_model_from_history(history_df)
    
    # --- A. SITE DETECTION ---
    def detect_site(row):
        machine = str(row.get('Machine', '')).upper()
        source = str(row.get('Source_File', '')).upper()
        if 'BXB' in source or 'BXB' in machine: return 'Boksburg'
        if 'PRF' in source or 'PRF' in machine: return 'Piet Retief'
        if 'UGI' in source or 'UGI' in machine: return 'Ugie'
        return 'Unknown'

    df['Site'] = df.apply(detect_site, axis=1)
    df = df.sort_values(by=['Site', 'Machine', 'Product'])
    
    schedule = []
    machine_clocks = {}
    start_base = datetime.now().replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(days=1)

    for index, row in df.iterrows():
        machine = str(row.get('Machine', 'Unknown'))
        qty = pd.to_numeric(row.get('Qty', 0), errors='coerce')
        site = row.get('Site', 'Unknown')
        product = row.get('Product', 'Unknown')

        # --- B. AI DURATION CALCULATION ---
        # Look for machine speed in DB. 
        # Note: Plan names (BXBF01) might differ from DB names (BXB_MFB_1).
        # We use fuzzy matching to find the closest match in the DB.
        cycle_time = 25 # Default: 25 seconds per unit
        
        # Try to find a speed match in the brain
        for db_machine, speed in brain['machine_speeds'].items():
            if str(db_machine).replace("_","") in machine.replace("_",""):
                cycle_time = speed
                break
        
        # If no machine match, try Site default
        if cycle_time == 25:
            cycle_time = brain['site_defaults'].get(site, 25)

        # Calc duration in minutes (Qty * Seconds / 60)
        duration_mins = max(15, int((qty * cycle_time) / 60))

        # --- C. SHIFT & TIME LOGIC ---
        if machine not in machine_clocks:
            machine_clocks[machine] = {'time': start_base, 'last_product': None}
        
        current_time = machine_clocks[machine]['time']
        
        # Add 45-min setup if product changes
        if machine_clocks[machine]['last_product'] and machine_clocks[machine]['last_product'] != product:
            current_time += timedelta(minutes=45)

        start_time = current_time
        end_time = start_time + timedelta(minutes=duration_mins)

        # --- D. AI WORKER ASSIGNMENT ---
        # Look for real names from the DB
        real_workers = []
        for db_machine, workers in brain['worker_roster'].items():
            if str(db_machine).replace("_","") in machine.replace("_",""):
                real_workers = workers
                break
        
        if real_workers:
            # Cycle through real names
            assigned_team = real_workers[index % len(real_workers)]
        else:
            # Fallback Team Names
            assigned_team = f"Team {site} { (index % 3) + 1 }"

        # Build final row
        row['Start_Time'] = start_time.strftime("%Y-%m-%d %H:%M")
        row['End_Time'] = end_time.strftime("%Y-%m-%d %H:%M")
        row['Duration_Mins'] = duration_mins
        row['Assigned_Team'] = assigned_team
        row['Shift'] = "Morning" if 6 <= start_time.hour < 14 else "Afternoon" if 14 <= start_time.hour < 22 else "Night"

        schedule.append(row)
        machine_clocks[machine]['time'] = end_time
        machine_clocks[machine]['last_product'] = product

    return pd.DataFrame(schedule)
