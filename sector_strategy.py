import pandas as pd
import numpy as np
import logging

# Configuration
INPUT_FILE = "lotto_activo_clean.csv"
BACKTEST_SIZE = 5000
BET_SIZE = 4 # Betting on 4 animals
PAYOUT = 30 # 30:1

logging.basicConfig(level=logging.INFO, format='%(message)s')

def load_data():
    df = pd.read_csv(INPUT_FILE)
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    df = df.sort_values('DateTime').reset_index(drop=True)
    return df

def get_sector(number):
    # Median is 18.5
    # Low: 0-18 (19 nums)
    # High: 19-37 (19 nums) including 00 as 37
    if number <= 18: return "Low"
    return "High"

def run_sector_strategy():
    df = load_data()
    total_len = len(df)
    
    # We need enough history for Markov
    start_idx = total_len - BACKTEST_SIZE
    if start_idx < 1000:
        start_idx = 1000 # Minimum history
    
    logging.info(f"--- SECTOR CROSSING STRATEGY (Last {total_len - start_idx} draws) ---")
    logging.info("Logic: If Last is Low -> Bet Top 4 High. If Last is High -> Bet Top 4 Low.")
    
    balance = 0
    wins = 0
    losses = 0
    
    # Pre-calculate counts for Markov (approximate using full history up to start for speed base, then updating)
    # Actually, let's just use the 'tail' method for correctness again.
    # To speed up, we will just use a sliding window of last 1000 for stats.
    
    history_window_size = 1000
    
    for i in range(start_idx, total_len):
        # 1. Context
        prev_draw_row = df.iloc[i-1]
        last_val = prev_draw_row['Number_Int']
        actual_val = df.iloc[i]['Number_Int']
        
        # 2. Identify Sectors
        last_sector = get_sector(last_val)
        target_sector = "High" if last_sector == "Low" else "Low"
        
        # 3. Select Candidates
        # We look at historical behavior of 'last_val'
        # What does 'last_val' usually transition to?
        
        current_history = df.iloc[i-history_window_size : i]
        
        # Filter: shifts
        # We want P(Next | Prev = last_val)
        # Fast pandas way:
        followers = current_history[current_history['Number_Int'].shift(1) == last_val]['Number_Int']
        
        candidates = []
        
        if len(followers) > 0:
            # Transitions exist
            counts = followers.value_counts()
            # Filter for TARGET SECTOR only
            for num, count in counts.items():
                if get_sector(num) == target_sector:
                    candidates.append(num)
                    
        # If not enough candidates from Markov, fill with "Hot" numbers from target sector
        if len(candidates) < BET_SIZE:
            # General frequency in recent window
            general_counts = current_history['Number_Int'].value_counts()
            for num, count in general_counts.items():
                if num not in candidates and get_sector(num) == target_sector:
                    candidates.append(num)
                    if len(candidates) >= BET_SIZE:
                        break
        
        # Top 4
        picks = candidates[:BET_SIZE]
        
        # 4. Bet
        cost = BET_SIZE * 1 # 1 unit each
        if actual_val in picks:
            profit = (1 * PAYOUT) - cost
            balance += profit
            wins += 1
            res = "WIN"
        else:
            balance -= cost
            losses += 1
            res = "LOSS"
            
        if i % 500 == 0:
            logging.info(f"Draw {i}: Last={last_val}({last_sector}) -> Target={target_sector} | Picks={picks} | Act={actual_val} | {res} | Bal={balance}")

    # Results
    roi = (balance / ((total_len - start_idx) * BET_SIZE)) * 100
    logging.info("\n--- FINAL RESULTS ---")
    logging.info(f"Total Draws: {total_len - start_idx}")
    logging.info(f"Wins: {wins} | Losses: {losses} | Win Rate: {wins/(total_len - start_idx)*100:.2f}%")
    logging.info(f"Final Balance: {balance} units")
    logging.info(f"ROI: {roi:.2f}%")
    
    # Check if Sector Logic itself was correct (did it actually cross?)
    # Simple check: calculate how often the sector actually crossed vs stayed
    cross_hits = 0
    stay_hits = 0
    for i in range(start_idx, total_len):
        prev = df.iloc[i-1]['Number_Int']
        act = df.iloc[i]['Number_Int']
        if get_sector(prev) != get_sector(act):
            cross_hits += 1
        else:
            stay_hits += 1
            
    logging.info(f"\nSector Crossing Rate: {cross_hits/(cross_hits+stay_hits)*100:.2f}% (Neutral is ~50%)")

if __name__ == "__main__":
    run_sector_strategy()
