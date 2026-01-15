import pandas as pd
import numpy as np
import logging

# Configuration
INPUT_FILE = "lotto_activo_raw.csv" # Will switch to complete file later
OUTPUT_FILE = "lotto_activo_clean.csv"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(filepath):
    try:
        df = pd.read_csv(filepath)
        logging.info(f"Loaded {len(df)} rows from {filepath}")
        return df
    except Exception as e:
        logging.error(f"Error loading {filepath}: {e}")
        return None

def clean_data(df):
    # 1. Handle "00" vs "0" (The Whale Trap)
    # The scraper returns "00" as a string. We need to map it.
    # We will convert Animal_Number to integer.
    # Map "00" -> 37.
    
    # Ensure Animal_Number is string first to check for "00"
    df['Animal_Number'] = df['Animal_Number'].astype(str)
    
    def map_number(val):
        val = val.strip()
        if val == "00":
            return 37
        try:
            return int(val)
        except:
            return -999 # Error code

    df['Number_Int'] = df['Animal_Number'].apply(map_number)
    
    # FIX: Explicitly map 'Ballena' to 37, regardless of what the number says (often '0')
    # This fixes the data integrity issue where 0 (Delfin) and 00 (Ballena) were mixed.
    df.loc[df['Animal_Name'].str.strip().str.lower() == 'ballena', 'Number_Int'] = 37
    
    # Drop errors
    errors = df[df['Number_Int'] == -999]
    if not errors.empty:
        logging.warning(f"Dropped {len(errors)} rows with invalid numbers.")
        df = df[df['Number_Int'] != -999]

    # 2. Date and Time parsing
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Time format is "08:00 AM". Convert to datetime or time object.
    # We also need to sort by Date and Time to assign Draw Index.
    df['DateTime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'])
    
    # Sort
    df = df.sort_values(by='DateTime')
    
    return df

def normalize_schedules(df):
    # 3. Normalization (Draw Index)
    # Group by Date and rank the draws by time
    df['Draw_Index'] = df.groupby('Date')['DateTime'].rank(method='first').astype(int)
    
    # Verify we don't have duplicates per index per day (sanity check)
    # If there are duplicates, our scraper might have fetched twice.
    # Let's deduplicate first.
    original_count = len(df)
    df = df.drop_duplicates(subset=['Date', 'Time'])
    if len(df) < original_count:
        logging.info(f"Removed {original_count - len(df)} duplicate rows.")
        # Re-calculate rank after dedup
        df['Draw_Index'] = df.groupby('Date')['DateTime'].rank(method='first').astype(int)

    return df

def main():
    df = load_data(INPUT_FILE)
    if df is not None and not df.empty:
        df = clean_data(df)
        df = normalize_schedules(df)
        
        logging.info(f"Final dataset: {len(df)} rows.")
        logging.info(df.head())
        
        df.to_csv(OUTPUT_FILE, index=False)
        logging.info(f"Saved cleaned data to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
