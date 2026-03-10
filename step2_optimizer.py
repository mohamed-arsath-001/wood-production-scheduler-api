import pandas as pd
from datetime import datetime, timedelta

def run_optimizer(df, history_df=None):
    schedule = []
    
    machine_col = next((c for c in df.columns if 'w/h' in str(c).lower() or 'machine' in str(c).lower()), None)
    qty_col = next((c for c in df.columns if 'qty' in str(c).lower()), None)
    desc_col = next((c for c in df.columns if 'description' in str(c).lower() or 'item' in str(c).lower()), None)
    
    if not all([machine_col, qty_col, desc_col]):
        print("Warning: Could not detect standard columns.")
        return df

    df[desc_col] = df[desc_col].astype(str).str.strip()
    
    # 1. Classify by "Pools" for Load Balancing
    def get_category(row):
        m_code = str(row[machine_col]).upper()
        d_text = str(row[desc_col]).upper()
        
        if 'BXB' in m_code:
            if 'FOIL' in d_text or 'AEB' in m_code: return 'Boksburg', 'FOIL LINE'
            else: return 'Boksburg', 'BXB_MFB_POOL' # Groups MFB1 & MFB2 into a shared pool
        elif 'PRF' in m_code:
            if 'CHIP' in d_text or '218' in m_code: return 'Piet Retief', 'CHIP LINE'
            else: return 'Piet Retief', 'MFB LINE'
        elif 'UGI' in m_code:
            if 'CONTI' in d_text or '005' in m_code or '003' in m_code: return 'Ugie', 'CONTI LINE'
            else: return 'Ugie', 'MFB LINE'
        return "Unknown", m_code

    df[['Site', 'Category']] = df.apply(get_category, axis=1, result_type="expand")
    
    # 2. Sort to group identical items together within their pool
    df = df.sort_values(by=['Category', desc_col]).reset_index(drop=True)

    start_base = datetime.now().replace(hour=6, minute=0, second=0, microsecond=0)

    line_clocks = {} 
    current_batch_ids = {} 
    last_desc_on_line = {} 
    last_pool_assignment = {} 
    
    for index, row in df.iterrows():
        category = row['Category']
        qty = float(row[qty_col])
        desc = row[desc_col]
        
        # --- DYNAMIC LOAD BALANCING ---
        specific_line = category
        
        if category == 'BXB_MFB_POOL':
            prod_key = (category, desc)
            if prod_key in last_pool_assignment:
                # Keep identical products on the same line to form a continuous batch
                specific_line = last_pool_assignment[prod_key]
            else:
                # New product! Assign to whichever MFB line has the EARLIEST available time
                t1 = line_clocks.get('MFB 1', start_base)
                t2 = line_clocks.get('MFB 2', start_base)
                
                specific_line = 'MFB 1' if t1 <= t2 else 'MFB 2'
                last_pool_assignment[prod_key] = specific_line
        # --- END BALANCING ---
        
        if specific_line not in line_clocks:
            line_clocks[specific_line] = start_base
            current_batch_ids[specific_line] = 1
            last_desc_on_line[specific_line] = None
            
        start_time = line_clocks[specific_line]
        setup_mins = 0
        
        # Setup Penalty & Batch Tracking
        if last_desc_on_line[specific_line] is not None:
            if last_desc_on_line[specific_line] != desc:
                setup_mins = 30 # Plate/Paper change! Add 30 mins
                start_time += timedelta(minutes=setup_mins)
                current_batch_ids[specific_line] += 1
                
        run_time_mins = max(15, int((qty / 120) * 60))
        end_time = start_time + timedelta(minutes=run_time_mins)
        
        # Update trackers
        line_clocks[specific_line] = end_time
        last_desc_on_line[specific_line] = desc
        
        new_row = row.copy()
        new_row['Production_Line'] = specific_line
        new_row['Batch_ID'] = f"{specific_line} - B{current_batch_ids[specific_line]:02d}"
        new_row['Setup_Time_Mins'] = setup_mins
        new_row['Run_Time_Mins'] = run_time_mins
        new_row['Start_Time'] = start_time.strftime("%Y-%m-%d %H:%M")
        new_row['End_Time'] = end_time.strftime("%Y-%m-%d %H:%M")
        new_row['Planned_Day'] = start_time.strftime("%A (%b %d)")
        
        schedule.append(new_row)

    # Build final dataframe
    final_df = pd.DataFrame(schedule)
    if not final_df.empty:
        final_df = final_df.drop(columns=['Category'])
        
        # --- THE NEW SORTING LOGIC ---
        # 1. Create a temporary column that only holds the YYYY-MM-DD string
        final_df['Sort_Date'] = final_df['Start_Time'].str[:10]
        
        # 2. Sort by the Day first, then by the Machine, then by the exact Time
        final_df = final_df.sort_values(by=['Sort_Date', 'Production_Line', 'Start_Time']).reset_index(drop=True)
        
        # 3. Drop the temporary sorting column to keep the output clean
        final_df = final_df.drop(columns=['Sort_Date'])
    
    return final_df
