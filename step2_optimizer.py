import pandas as pd
from datetime import datetime, timedelta

def run_optimizer(df, history_df=None):
    schedule = []
    
    # 1. Identify original columns dynamically
    machine_col = next((c for c in df.columns if 'w/h' in str(c).lower() or 'machine' in str(c).lower()), None)
    qty_col = next((c for c in df.columns if 'qty' in str(c).lower()), None)
    desc_col = next((c for c in df.columns if 'description' in str(c).lower() or 'item' in str(c).lower()), None)
    
    if not all([machine_col, qty_col, desc_col]):
        print("Warning: Could not detect standard columns.")
        return df

    # --- SMART GROUPING ---
    df[desc_col] = df[desc_col].astype(str).str.strip()
    df = df.sort_values(by=[machine_col, desc_col]).reset_index(drop=True)

    # Site & Production Line Translator
    def get_line_info(row):
        m_code = str(row[machine_col]).upper()
        d_text = str(row[desc_col]).upper()
        
        site = "Unknown"
        line = m_code
        
        if 'BXB' in m_code:
            site = 'Boksburg'
            if 'FOIL' in d_text or 'AEB' in m_code: line = "FOIL LINE"
            elif '321' in m_code: line = "MFB 2"
            else: line = "MFB 1"
        elif 'PRF' in m_code:
            site = 'Piet Retief'
            if 'CHIP' in d_text or '218' in m_code: line = "CHIP LINE"
            else: line = "MFB LINE"
        elif 'UGI' in m_code:
            site = 'Ugie'
            if 'CONTI' in d_text or '005' in m_code or '003' in m_code: line = "CONTI LINE"
            else: line = "MFB LINE"
            
        return site, line

    df[['Site', 'Production_Line']] = df.apply(get_line_info, axis=1, result_type="expand")

    # --- TIMING: START FROM TODAY ---
    # Gets the exact date the file is generated and starts at the 06:00 AM shift
    start_base = datetime.now().replace(hour=6, minute=0, second=0, microsecond=0)

    line_clocks = {} 
    current_batch_ids = {} # Tracks the Batch Numbers for easy viewing
    
    for index, row in df.iterrows():
        line = row['Production_Line']
        qty = float(row[qty_col])
        desc = row[desc_col]
        
        if line not in line_clocks:
            line_clocks[line] = start_base
            current_batch_ids[line] = 1 # Start at Batch 1
            
        start_time = line_clocks[line]
        setup_mins = 0
        
        # Setup Penalty & Batch Tracking
        if index > 0:
            prev_row = df.iloc[index-1]
            if prev_row['Production_Line'] == line:
                if prev_row[desc_col] != desc:
                    setup_mins = 30 # Product changed! Add 30 mins.
                    start_time += timedelta(minutes=setup_mins)
                    current_batch_ids[line] += 1 # Increment to the next Batch ID
                
        # Calculate run time (120 units per hour)
        run_time_mins = max(15, int((qty / 120) * 60))
        
        end_time = start_time + timedelta(minutes=run_time_mins)
        
        line_clocks[line] = end_time
        
        new_row = row.copy()
        new_row['Batch_ID'] = f"{line} - B{current_batch_ids[line]:02d}" # Creates tags like "MFB 1 - B01"
        new_row['Setup_Time_Mins'] = setup_mins
        new_row['Run_Time_Mins'] = run_time_mins
        new_row['Start_Time'] = start_time.strftime("%Y-%m-%d %H:%M")
        new_row['End_Time'] = end_time.strftime("%Y-%m-%d %H:%M")
        new_row['Planned_Day'] = start_time.strftime("%A (%b %d)")
        
        schedule.append(new_row)

    return pd.DataFrame(schedule)
