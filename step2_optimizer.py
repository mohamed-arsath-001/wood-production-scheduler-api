import pandas as pd
from datetime import datetime, timedelta

# --- A. THE LEARNING LAYER (Reads your Master DB) ---
def train_model_from_history(history_df):
    """
    Learns Machine Speeds and Worker Rosters from the 'API Data.xlsx' file.
    """
    knowledge_base = {
        'machine_speeds': {},  # Stores avg cycle time per machine
        'worker_roster': {}    # Stores list of workers per machine
    }

    if history_df is None or history_df.empty:
        return knowledge_base

    # 1. LEARN SPEEDS (Cycle Time)
    # Group by Machine + Product to get precise speeds
    # Column map based on your file: 'stationName' = Machine, 'cycleTimePerOneUnit(sec)' = Speed
    if 'stationName' in history_df.columns and 'cycleTimePerOneUnit(sec)' in history_df.columns:
        # Clean data
        valid_rows = history_df[history_df['cycleTimePerOneUnit(sec)'] > 0]
        
        # Avg speed per machine (General)
        avg_speed_machine = valid_rows.groupby('stationName')['cycleTimePerOneUnit(sec)'].mean().to_dict()
        knowledge_base['machine_speeds'] = avg_speed_machine

    # 2. LEARN WORKERS
    # Column map: 'operatorTeam' = Worker Name
    if 'stationName' in history_df.columns and 'operatorTeam' in history_df.columns:
        roster_groups = history_df.groupby('stationName')['operatorTeam'].unique().to_dict()
        # Convert numpy arrays to clean lists
        clean_roster = {k: [x for x in v if str(x) != 'nan'] for k, v in roster_groups.items()}
        knowledge_base['worker_roster'] = clean_roster

    return knowledge_base

# --- B. THE OPTIMIZER (Uses the Knowledge) ---
def run_optimizer(df, history_df=None):
    """
    1. Trains on history (if provided).
    2. Schedules orders using REAL speeds.
    3. Assigns REAL workers.
    """
    
    # Train the model
    brain = train_model_from_history(history_df)
    
    # Prepare Data
    df = df.sort_values(by=['Site', 'Machine', 'Product'])
    schedule = []
    machine_clocks = {}
    
    # Start Time: Tomorrow 06:00
    start_base = datetime.now().replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(days=1)

    for index, row in df.iterrows():
        machine = row.get('Machine', 'Unknown')
        product = row.get('Product', 'Unknown')
        qty = pd.to_numeric(row.get('Qty', 0), errors='coerce')
        
        # Init Clock
        if machine not in machine_clocks:
            machine_clocks[machine] = {'time': start_base, 'last_product': None}
            
        current_clock = machine_clocks[machine]['time']
        last_product = machine_clocks[machine]['last_product']
        
        # 1. INTELLIGENT SPEED CALCULATION
        # Look up real speed from history. If not found, default to 30 seconds (0.5 mins)
        real_cycle_time_sec = brain['machine_speeds'].get(machine, 30) 
        duration_mins = (qty * real_cycle_time_sec) / 60
        # Add buffer (10%)
        duration_mins = int(duration_mins * 1.1) 
        if duration_mins < 5: duration_mins = 5 # Minimum run time

        # 2. SETUP TIME (45 mins rule)
        setup_min = 0
        if last_product and product != last_product:
            setup_min = 45
            current_clock += timedelta(minutes=45)

        start_time = current_clock
        end_time = start_time + timedelta(minutes=duration_mins)

        # 3. INTELLIGENT WORKER ASSIGNMENT
        # Get list of workers who actually work on THIS machine
        possible_workers = brain['worker_roster'].get(machine, ['Unassigned Team'])
        if not possible_workers: possible_workers = ['Standard Team']
        
        # Rotate workers based on time (Simple Shift Simulation)
        worker_idx = start_time.hour % len(possible_workers)
        assigned_worker = possible_workers[worker_idx]

        # Shift Name
        hour = start_time.hour
        if 6 <= hour < 14: shift = "Morning"
        elif 14 <= hour < 18: shift = "Afternoon"
        elif 18 <= hour < 22: shift = "Evening"
        else: shift = "Night"

        # Save Row
        row['Start_Time'] = start_time.strftime("%Y-%m-%d %H:%M")
        row['End_Time'] = end_time.strftime("%Y-%m-%d %H:%M")
        row['Duration_Mins'] = duration_mins
        row['Assigned_Team'] = assigned_worker
        row['Shift'] = shift
        
        schedule.append(row)
        
        # Update Clock
        machine_clocks[machine]['time'] = end_time
        machine_clocks[machine]['last_product'] = product

    # Return the full schedule
    return pd.DataFrame(schedule)
