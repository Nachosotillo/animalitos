import pandas as pd
import numpy as np
import logging
import ast
import os
import requests
import time
import datetime
from bs4 import BeautifulSoup
import asyncio

# Configuration
PATTERNS_FILE = "master_patterns.csv"
INPUT_FILE = "lotto_activo_clean.csv"
CHECK_INTERVAL_SECONDS = 300 # 5 minutes

# Telegram Keys (User must set these)
TELEGRAM_BOT_TOKEN = "8459641995:AAEmBN3igwrkkRlVFVBUI6dTF0glxY6-a-E" 
TELEGRAM_CHAT_ID = "6700742710"

# URL Config
BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo/resultados/{date}/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Pre-defined "Iron Chains" (Topology)
IRON_CHAINS = {
    31: 0,   # Lapa -> Delfin
    29: 6,   # Elefante -> Rana
    17: 15,  # Pavo -> Zorro
    3: 34,   # Ciempies -> Venado
}

# Animal Names Mapping
ANIMAL_MAP = {
    0: "Delfin", 1: "Carnero", 2: "Toro", 3: "Ciempies", 4: "Alacran", 5: "Leon", 
    6: "Rana", 7: "Perico", 8: "Raton", 9: "Aguila", 10: "Tigre", 11: "Gato", 
    12: "Caballo", 13: "Mono", 14: "Paloma", 15: "Zorro", 16: "Oso", 17: "Pavo", 
    18: "Burro", 19: "Chivo", 20: "Cochino", 21: "Gallo", 22: "Camello", 23: "Cebra", 
    24: "Iguana", 25: "Gallina", 26: "Vaca", 27: "Perro", 28: "Zamuro", 29: "Elefante", 
    30: "Caiman", 31: "Lapa", 32: "Ardilla", 33: "Pescado", 34: "Venado", 35: "Jirafa", 
    36: "Culebra", 37: "Ballena" # 00 mapping
}

def send_telegram(message):
    if TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("[TELEGRAM SIMULATION] " + message)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Failed to send Telegram: {e}")

def load_patterns():
    if not os.path.exists(PATTERNS_FILE):
        return []
    df = pd.read_csv(PATTERNS_FILE)
    patterns = []
    df = df[df['ROI'] > 150]
    for idx, row in df.iterrows():
        try:
            pat_str = row['Pattern'].replace("np.int64(", "").replace(")", "")
            pat_list = ast.literal_eval(pat_str)
            patterns.append({
                "Window": int(row['Window']),
                "Context": set(pat_list),
                "Target": int(row['Target']),
                "ROI": float(row['ROI']),
                "Stability": row['Stability']
            })
        except:
            continue
    return patterns

def get_latest_draws_from_web():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    url = BASE_URL.format(date=today)
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return []
            
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []
        
        cards = soup.find_all("div", class_="circle-legend")
        for card in cards:
            h4 = card.find("h4")
            h5 = card.find("h5")
            if h4 and h5:
                text_parts = h4.get_text(strip=True).split(" ")
                time_val = h5.get_text(strip=True).replace("Lotto Activo", "").strip()
                
                if len(text_parts) >= 2:
                    raw_num = text_parts[0]
                    # Map to int
                    if raw_num == "00": num_int = 37
                    else: num_int = int(raw_num)
                    
                    results.append((time_val, num_int))
                    
        # Sort by time? Usually they are ordered visually.
        # But we just return the list.
        # Actually we need them ordered by DRAW ORDER.
        # The site orders them usually. We assume site order is chronologic?
        # Usually site shows latest first or grid.
        # Let's assume list order is chronological (Morning -> Night).
        # To be safe, we just return the list and rely on the fact that we append new ones.
        return results
    except Exception as e:
        print(f"Scrape error: {e}")
        return []

def get_initial_history():
    if os.path.exists(INPUT_FILE):
        df = pd.read_csv(INPUT_FILE)
        return df['Number_Int'].tail(30).astype(int).tolist()
    else:
        return []

# AHORA (Lo optimizado para GitHub Actions):
def run_once():
    print("--- RADAR BOT (MODO SNIPER - ONE SHOT) ---")
    
    # 1. Cargar datos
    patterns = load_patterns()
    history = get_initial_history()
    
    # 2. Descargar sorteos de hoy
    today_draws = get_latest_draws_from_web()
    
    # 3. Analizar SOLO el último sorteo disponible
    if today_draws:
        last_time, last_num = today_draws[-1]
        print(f"Analizando sorteo: {last_time} -> {last_num}")
        
        alerts = []
        # ... (Aquí va tu misma lógica de IRON CHAINS y PATTERNS) ...
        # ... (Copia y pega tus bloques 'if num in IRON_CHAINS' etc.) ...
        
        # 4. Enviar y salir
        if alerts:
            for alert in alerts:
                send_telegram(alert)
        else:
            print("Sin patrones detectados en este turno.")
            
    else:
        print("Aún no hay sorteos hoy o error de conexión.")

if __name__ == "__main__":
    run_once() # Ejecuta una vez y termina.


