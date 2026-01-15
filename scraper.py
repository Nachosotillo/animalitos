import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import random
from datetime import datetime, timedelta
import os
import csv
import logging
import time

# Configuration
BASE_URL = "https://loteriadehoy.com/animalito/lottoactivo/resultados/{date}/"
START_DATE = datetime(2017, 1, 1)
END_DATE = datetime.now()
OUTPUT_FILE = "lotto_activo_raw.csv"
LOG_FILE = "scraper.log"
MAX_CONCURRENT_REQUESTS = 10 # Reduced from 20 to avoid blocks

# Parsing Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler(LOG_FILE),
    logging.StreamHandler()
])

# Random User Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Connection": "keep-alive"
    }

async def fetch_data(session, date, semaphore):
    date_str = date.strftime("%Y-%m-%d")
    url = BASE_URL.format(date=date_str)
    
    async with semaphore:
        for attempt in range(2): # Reduced retries to 2 to speed up if server is just missing data
            try:
                async with session.get(url, headers=get_headers(), timeout=10) as response:
                    if response.status == 200:
                        return await response.text(), date_str
                    elif response.status == 404:
                        return None, date_str
                    else:
                        # logging.warning(f"Status {response.status} for {date_str}. Retrying...")
                        await asyncio.sleep(0.5)
            except Exception:
                await asyncio.sleep(1)
        
        # Final attempt failed
        # logging.error(f"Failed {date_str}")
        return None, date_str

def parse_html(html, date_str):
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    results = []
    
    cards = soup.find_all("div", class_="circle-legend")
    for card in cards:
        try:
            h4 = card.find("h4")
            if h4:
                text_parts = h4.get_text(strip=True).split(" ")
                if len(text_parts) >= 2:
                    # e.g. "36 Culebra"
                    animal_number = text_parts[0]
                    animal_name = " ".join(text_parts[1:])
                else: continue
            else: continue

            h5 = card.find("h5")
            if h5:
                time_val = h5.get_text(strip=True).replace("Lotto Activo", "").strip()
            else: continue

            results.append({
                "Date": date_str,
                "Time": time_val,
                "Animal_Number": animal_number,
                "Animal_Name": animal_name
            })
        except:
            continue
    return results

async def main_async():
    # Generate complete date list
    dates = []
    curr = START_DATE
    while curr <= END_DATE:
        dates.append(curr)
        curr += timedelta(days=1)
    
    logging.info(f"Starting crawl for {len(dates)} days (2017 -> Now).")

    # Initialize CSV
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Date", "Time", "Animal_Number", "Animal_Name"])
        writer.writeheader()

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession() as session:
        # Process in chunks of 50
        CHUNK_SIZE = 50
        total_records = 0
        
        for i in range(0, len(dates), CHUNK_SIZE):
            chunk_dates = dates[i:i+CHUNK_SIZE]
            tasks = [fetch_data(session, d, semaphore) for d in chunk_dates]
            
            # logging.info(f"Processing chunk {i//CHUNK_SIZE + 1}...")
            if i % 1000 == 0:
                logging.info(f"Progress: {i}/{len(dates)} days...")

            responses = await asyncio.gather(*tasks)
            
            chunk_results = []
            for html, date_str in responses:
                if html:
                    draws = parse_html(html, date_str)
                    chunk_results.extend(draws)
            
            if chunk_results:
                total_records += len(chunk_results)
                # Append to CSV immediately
                with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["Date", "Time", "Animal_Number", "Animal_Name"])
                    writer.writerows(chunk_results)
            
            await asyncio.sleep(0.2) # Micro-rest
            
    logging.info(f"Completed. Scraped {total_records} records.")

def main():
    try:
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logging.info("Scraper interrupted by user.")

if __name__ == "__main__":
    main()
