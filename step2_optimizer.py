import pandas as pd
from datetime import datetime, timedelta

def run_optimizer(df, history_df=None):
    """
    Phase 2: Machine-Centric Optimizer
    Focuses on smart grouping, setup reduction, and predictable machine speeds.
    """
    schedule = []
    
    # 1. Identify their original columns dynamically
    machine_col = next((c for c in df.columns if 'w/h' in str(c).lower() or 'machine' in str(c).lower()), None)
    qty_col = next((c for c in df.columns if 'qty' in str(c).lower()), None)
    desc_col = next((c for c in df.columns if 'description' in str(c).lower() or 'item' in str(c).lower()), None)
    
    # If we can't find standard columns, just return the dataframe safely
    if not all([machine_col, qty_col, desc_col]):
        print("Warning: Could not detect standard columns.")
        return df

    # --- THE MAGIC: SMART GROUPING ---
    # Sorting by Machine, then by Description mathematically groups identical colors/finishes together
    df[desc_col] = df[desc_col].astype(str).str.strip()
    df = df.sort_values(by=[machine_col, desc_col]).reset_index(drop=True)

    # Site & Production Line Translator
    def get_line_info(row):
        m_code = str(row[machine_col]).upper()
        d_text = str(row[desc_col]).upper()
        
        site = "Unknown"
        line = m_code
        
        # Boksburg
        if 'BXB' in m_code:
            site = 'Boksburg'
            if 'FOIL' in d_text or 'AEB' in m_code: line = "FOIL LINE"
            elif '321' in m_code: line = "MFB 2"
            else: line = "MFB 1"
        # Piet Retief
        elif 'PRF' in m_code:
            site = 'Piet Retief'
            if 'CHIP' in d_text or '218' in m_code: line = "CHIP LINE"
            else: line = "MFB LINE"
        # Ugie
        elif 'UGI' in m_code:
            site = 'Ugie'
            if 'CONTI' in d_text or '005' in m_code or '003' in m_code: line = "CONTI LINE"
            else: line = "MFB LINE"
            
        return site, line

    # Apply Site and Line logic
    df[['Site', 'Production_Line']] = df.apply(get_line_info, axis=1, result_type="expand")

    # --- TIMING & SETUP PENALTIES ---
    # Start schedule Next Monday at 06:00 AM
    today = datetime.now()
    days_ahead = 0 - today.weekday()
    if days_ahead <= 0: days_ahead += 7 
    start_base = (today + timedelta(days=days_ahead)).replace(hour=6, minute=0, second=0, microsecond=0)

    line_clocks = {} # Tracks the current time on each physical machine
    
    for index, row in df.iterrows():
        line = row['Production_Line']
        qty = float(row[qty_col])
        desc = row[desc_col]
        
        if line not in line_clocks:
            line_clocks[line] = start_base
            
        start_time = line_clocks[line]
        setup_mins = 0
        
        # Apply Setup Penalty: If the product description changes, add 30 mins for plate/paper change!
        if index > 0:
            prev_row = df.iloc[index-1]
            if prev_row['Production_Line'] == line and prev_row[desc_col] != desc:
                setup_mins = 30
                start_time += timedelta(minutes=setup_mins)
                
        # Calculate predictable run time (Assuming 120 units per hour baseline)
        # 120 units/hr = 2 units per minute
        run_time_mins = max(15, int((qty / 120) * 60))
        
        end_time = start_time + timedelta(minutes=run_time_mins)
        
        # Update the machine's clock
        line_clocks[line] = end_time
        
        # Build the final row (preserves their original columns, appends ours)
        new_row = row.copy()
        new_row['Setup_Time_Mins'] = setup_mins
        new_row['Run_Time_Mins'] = run_time_mins
        new_row['Start_Time'] = start_time.strftime("%Y-%m-%d %H:%M")
        new_row['End_Time'] = end_time.strftime("%Y-%m-%d %H:%M")
        new_row['Planned_Day'] = start_time.strftime("%A (%b %d)")
        
        schedule.append(new_row)

    return pd.DataFrame(schedule)
