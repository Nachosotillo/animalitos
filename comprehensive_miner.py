import pandas as pd
import numpy as np
import logging
from collections import defaultdict, Counter
from itertools import combinations
import datetime

# Configuration
INPUT_FILE = "lotto_activo_clean.csv"
LOG_FILE = "comprehensive_miner.log"
PAYOUT = 30
MIN_OCCURRENCES = 100 # Minimum times the pattern must have triggered historically
MIN_ROI = 15.0 # Minimum total ROI %
MIN_STABILITY = 0.60 # Must be profitable in 60% of years active

logging.basicConfig(level=logging.INFO, format='%(message)s', handlers=[
    logging.FileHandler(LOG_FILE),
    logging.StreamHandler()
])

def load_data():
    df = pd.read_csv(INPUT_FILE)
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    df = df.sort_values('DateTime').reset_index(drop=True)
    return df

def scan_combinations(df):
    windows = [3, 5, 8]
    numbers = df['Number_Int'].values
    dates = df['DateTime'].values
    n = len(numbers)
    
    # Store Stats
    # Key: (WindowSize, PatternTuple, Target)
    # Value: {TotalTriggers, TotalWins, ProfitableYearsCount, ActiveYearsCount}
    # We need to track year-by-year profit efficiently.
    # Structure: rule_stats[key] = {year: profit}
    
    rule_yearly_profit = defaultdict(lambda: defaultdict(int))
    rule_triggers = defaultdict(int)
    
    logging.info(f"Scanning {n} draws across windows {windows} for Pairs & Triplets...")
    
    # Optimization: Iterate data once? No, iterate for each window size is clearer.
    for W in windows:
        logging.info(f"Scanning Window Size: {W}...")
        
        for i in range(W, n):
            current_year = pd.to_datetime(dates[i]).year
            target = numbers[i]
            
            # Context Window
            # Use set to avoid duplicates in window (presence is binary)
            window_nums = sorted(list(set(numbers[i-W : i])))
            
            # Combinations: Pairs (2) and Triplets (3)
            # If window has < 2 items, skip pairs. < 3 skip triplets.
            
            combos = []
            if len(window_nums) >= 2:
                combos.extend(combinations(window_nums, 2))
            if len(window_nums) >= 3:
                combos.extend(combinations(window_nums, 3))
                
            for pattern in combos:
                key = (W, pattern, target)
                rule_triggers[(W, pattern)] += 1 # Trigger count for the PATTERN (denominator)
                
                # Check win/loss later? 
                # No, we need to associate specific target.
                # Problem: Association Rules usually target ANY outcome.
                # If we track (Pattern, Target), we are splitting the support.
                # Wait. "If {A,B} -> C". We need P(C|{A,B}).
                # So we count Triggers of {A,B}. And Wins of ({A,B}->C).
                
                pass

    # RE-THINKING FOR EFFICIENCY:
    # 30,000 draws.
    # ~1000 combos per draw? No, window 5 has 5C2=10 pairs, 5C3=10 triplets. Total 20.
    # Window 8 has 8C2=28, 8C3=56. Total ~84.
    # Total ops = 30,000 * 84 = 2.5 million. Very Fast.
    # The bottleneck is updating dictionaries.
    
    # New Structure:
    # pattern_triggers[(W, pattern)] -> count
    # pattern_outcomes[(W, pattern)] -> Counter(target)
    # pattern_yearly_outcomes[(W, pattern, target)] -> {year: profit}
    
    pattern_triggers = defaultdict(int)
    pattern_yearly_profit = defaultdict(lambda: defaultdict(int))
    
    for W in windows:
        logging.info(f"  Window {W}...")
        for i in range(W, n):
            current_year = pd.to_datetime(dates[i]).year
            target = numbers[i]
            window_nums = sorted(list(set(numbers[i-W : i])))
            
            combos = []
            if len(window_nums) >= 2:
                combos.extend(combinations(window_nums, 2))
            if len(window_nums) >= 3:
                combos.extend(combinations(window_nums, 3))
            
            for pat in combos:
                pattern_triggers[(W, pat)] += 1
                
                # Calculate Profit for THIS specific target outcome
                # But wait, we don't know if we 'bet' on this target yet.
                # In mining, we just record "This target happened".
                # Profit calcs happen post-mining.
                # We update profit for the specific rule (Pat -> Target).
                # Profit = PAYOUT - 1 (Win)
                # But for every OTHER target that didn't happen, we lost 1 unit?
                # No. A rule is specific: "If A,B -> Bet C".
                # So if A,B happened, did C happen?
                # If yes -> +29. If no -> -1.
                
                # So we need to update "Loss" for ALL potential targets? No that's infinite.
                # We update "Trigger" count for the pattern.
                # And "Win" count for (Pattern, Target).
                # Only "Win" adds +30 revenue.
                # Cost is determined by Triggers.
                
                # So: 
                # pattern_triggers[(W, pat)] -> Triggers (Total Cost)
                # pattern_wins[(W, pat, target)] -> {year: wins}
                # pattern_triggers_yearly[(W, pat)] -> {year: count}
                
                # This way we can reconstruct Yearly ROI for any (Pat->Target).
                
                # However, updating dicts inside inner loop is slow?
                # Let's try.
                
                # Optimization for Profit:
                # We only care about (Pat->Target) if it actually WINS often.
                # So let's just count wins.
                
                pass

    # Implementation of Loop
    pattern_triggers_yearly = defaultdict(lambda: defaultdict(int))
    pattern_wins_yearly = defaultdict(lambda: defaultdict(int))
    
    for W in windows:
        for i in range(W, n):
            year = pd.to_datetime(dates[i]).year
            target = numbers[i]
            window_nums = sorted(list(set(numbers[i-W : i])))
            
            combos = []
            if len(window_nums) >= 2: combos.extend(combinations(window_nums, 2))
            if len(window_nums) >= 3: combos.extend(combinations(window_nums, 3))
            
            for pat in combos:
                pattern_triggers_yearly[(W, pat)][year] += 1
                pattern_wins_yearly[(W, pat, target)][year] += 1
                
    # Analysis & Formatting
    logging.info("Mining Complete. Analyzing profit stability...")
    
    results = []
    
    # Iterate over all specific rules that had at least one win
    # We iterate pattern_wins_yearly keys: (W, pat, target)
    
    for rule_key, wins_by_year in pattern_wins_yearly.items():
        W, pat, target = rule_key
        
        # Get total triggers by year from the parent pattern
        triggers_by_year = pattern_triggers_yearly[(W, pat)]
        
        total_triggers = sum(triggers_by_year.values())
        if total_triggers < MIN_OCCURRENCES:
            continue
            
        total_wins = sum(wins_by_year.values())
        
        # Calculate Global ROI
        global_balance = (total_wins * PAYOUT) - total_triggers
        global_roi = (global_balance / total_triggers) * 100
        
        if global_roi < MIN_ROI:
            continue
            
        # Check Stability
        years_active = 0
        years_profitable = 0
        
        # Check years 2017-2025
        all_years = sorted(list(triggers_by_year.keys()))
        
        for y in all_years:
            t = triggers_by_year[y]
            w = wins_by_year.get(y, 0)
            bal = (w * PAYOUT) - t
            
            if t > 0:
                years_active += 1
                if bal > 0:
                    years_profitable += 1
        
        stability = years_profitable / years_active if years_active > 0 else 0
        
        if stability >= MIN_STABILITY:
            results.append({
                "Window": W,
                "Pattern": list(pat),
                "Target": target,
                "Triggers": total_triggers,
                "Wins": total_wins,
                "Balance": global_balance,
                "ROI": global_roi,
                "Stability": f"{years_profitable}/{years_active} ({stability:.0%})"
            })
            
    # Sort by ROI
    results.sort(key=lambda x: x['ROI'], reverse=True)
    
    # Save to CSV
    pd.DataFrame(results).to_csv("master_patterns.csv", index=False)
    
    # Log Top 20
    logging.info(f"\n--- TOP 20 STABLE PATTERNS ---")
    headers = f"{'Win(W)':<6} | {'Pattern':<20} | {'Tgt':<3} | {'Trig':<5} | {'ROI':<8} | {'Stability'}"
    logging.info(headers)
    logging.info("-" * len(headers))
    for r in results[:20]:
        pat_str = str(r['Pattern'])
        logging.info(f"{r['Window']:<6} | {pat_str:<20} | {r['Target']:<3} | {r['Triggers']:<5} | {r['ROI']:.1f}%   | {r['Stability']}")

    return results

if __name__ == "__main__":
    df = load_data()
    scan_combinations(df)
