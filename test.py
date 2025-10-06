#!/usr/bin/env python3
"""
Gaming Zones Scraper - Simple Background Service
Usage:
    python scraper.py              # Run normally (foreground)
    python scraper.py --background # Run as background service
    python scraper.py --status     # Check if running
    python scraper.py --stop       # Stop background service
"""

import asyncio
import csv
import json
import os
import re
import sys
import time
import subprocess
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from playwright.async_api import async_playwright

# Configuration
QUERIES = ["gaming zones", "game arcades", "VR gaming centers"]
MAX_WORKERS = 25
OUTPUT_CSV = "gaming_zones_usa.csv"
OUTPUT_JSON = "gaming_zones_usa.json"
LOG_FILE = "scraper.log"
PID_FILE = "scraper.pid"

def log(message):
    """Simple logging to file and console"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    
    # Write to log file
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
    except:
        pass
    
    # Print to console if not in background mode
    if '--background' not in sys.argv:
        print(log_entry)

def is_running():
    """Check if background service is running"""
    if not os.path.exists(PID_FILE):
        return False
    
    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        
        # Check if process exists
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        # Process doesn't exist, clean up stale PID file
        try:
            os.remove(PID_FILE)
        except:
            pass
        return False

def get_pid():
    """Get PID of running service"""
    if not os.path.exists(PID_FILE):
        return None
    try:
        with open(PID_FILE, 'r') as f:
            return int(f.read().strip())
    except:
        return None

def stop_service():
    """Stop the background service"""
    if not is_running():
        print("Service is not running")
        return False
    
    pid = get_pid()
    if not pid:
        print("Could not read PID file")
        return False
    
    try:
        os.kill(pid, 9)  # SIGKILL
        time.sleep(1)
        
        # Clean up PID file
        try:
            os.remove(PID_FILE)
        except:
            pass
        
        print(f"Service stopped (PID: {pid})")
        return True
    except OSError as e:
        print(f"Error stopping service: {e}")
        return False

def show_status():
    """Show service status"""
    if is_running():
        pid = get_pid()
        print(f"Service is RUNNING (PID: {pid})")
        
        # Show last few log lines
        if os.path.exists(LOG_FILE):
            try:
                with open(LOG_FILE, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    print("\nRecent logs:")
                    for line in lines[-5:]:
                        print(line.rstrip())
            except:
                pass
    else:
        print("Service is NOT running")

def read_pincodes():
    """Read pincodes from CSV"""
    try:
        with open("pincode.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            pincodes = [row['pincode'].strip() for row in reader if row.get('pincode', '').strip()]
            log(f"Loaded {len(pincodes)} pincodes")
            return pincodes
    except Exception as e:
        log(f"Error reading pincodes: {e}")
        return []

def save_results(data):
    """Save results to CSV and JSON"""
    if not data:
        return
    
    # CSV headers
    headers = [
        "Business_Name", "Contact_Person", "Phone", "Email", "All_Emails_Found", 
        "Website", "All_Websites_Found", "City", "State", "Postal_Code", 
        "Full_Address", "Rating", "Review_Count", "Category", "Hours", 
        "Plus_Code", "Located_In", "Price_Level", "Amenities", 
        "Google_Maps_URL", "Query", "Timestamp"
    ]
    
    # Save CSV
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers)
        for row in data:
            writer.writerow(row)
    
    # Save JSON
    existing = []
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
                existing = json.load(f)
        except:
            existing = []
    
    for row in data:
        record = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
        existing.append(record)
    
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

def extract_emails(text):
    """Extract all emails from text"""
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    emails = re.findall(pattern, text, re.IGNORECASE)
    return list(set(emails))  # Remove duplicates

def parse_address(address):
    """Parse address into city, state, postal code"""
    city = state = postal = ""
    
    # Extract postal code
    postal_match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', address)
    if postal_match:
        postal = postal_match.group(1)
        
        # Extract state (2 letters before postal code)
        state_match = re.search(rf'\b([A-Z]{{2}})\s+{re.escape(postal)}', address)
        if state_match:
            state = state_match.group(1)
            
            # Extract city (before state)
            city_match = re.search(rf'([^,\n]+),\s*{re.escape(state)}', address)
            if city_match:
                city = city_match.group(1).strip()
    
    return city, state, postal

async def scrape_location(pincode, query, worker_id):
    """Scrape Google Maps for a location and query"""
    search_query = f"{query} {pincode} USA"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    log(f"[W{worker_id}] Scraping: {search_query}")
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            page.set_default_timeout(30000)
            
            # Navigate to Google Maps
            url = f"https://www.google.com/maps/search/{search_query}/"
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)
            
            # Check for results
            try:
                await page.wait_for_selector('.Nv2PK', timeout=20000)
            except:
                log(f"[W{worker_id}] No results found")
                await browser.close()
                return 0
            
            # Scroll to load all results
            for _ in range(10):
                await page.evaluate('document.querySelector("[role=feed]").scrollTop = document.querySelector("[role=feed]").scrollHeight')
                await page.wait_for_timeout(2000)
            
            # Get all businesses
            businesses = await page.locator('.Nv2PK').all()
            log(f"[W{worker_id}] Found {len(businesses)} businesses")
            
            results = []
            
            for idx, business in enumerate(businesses):
                try:
                    await business.click()
                    await page.wait_for_timeout(3000)
                    
                    # Extract data
                    name = await page.locator('.DUwDvf').first.inner_text() if await page.locator('.DUwDvf').first.count() > 0 else ""
                    
                    rating = ""
                    reviews = ""
                    try:
                        rating = await page.locator('.F7nice span[aria-hidden="true"]').first.inner_text()
                        review_text = await page.locator('.F7nice span[aria-label*="reviews"]').first.inner_text()
                        reviews = re.search(r'\(([0-9,]+)\)', review_text).group(1) if re.search(r'\(([0-9,]+)\)', review_text) else ""
                    except:
                        pass
                    
                    category = ""
                    try:
                        category = await page.locator('button[jsaction*="category"]').first.inner_text()
                    except:
                        pass
                    
                    address = ""
                    try:
                        address = await page.locator('[data-item-id="address"] .Io6YTe').first.inner_text()
                    except:
                        pass
                    
                    city, state, postal = parse_address(address)
                    
                    phone = ""
                    try:
                        phone = await page.locator('[data-item-id*="phone"] .Io6YTe').first.inner_text()
                    except:
                        pass
                    
                    website = ""
                    try:
                        website = await page.locator('.rogA2c.ITvuef .Io6YTe').first.inner_text()
                        if website and not website.startswith('http'):
                            website = f"https://{website}"
                    except:
                        pass
                    
                    # Extract emails from page
                    page_content = await page.content()
                    emails = extract_emails(page_content)
                    primary_email = emails[0] if emails else ""
                    all_emails = "; ".join(emails)
                    
                    hours = ""
                    try:
                        hours = await page.locator('.ZDu9vd').first.inner_text()
                    except:
                        pass
                    
                    maps_url = page.url
                    
                    if name:
                        result = [
                            name, "", phone, primary_email, all_emails,
                            website, website, city, state, postal,
                            address, rating, reviews, category, hours,
                            "", "", "", "", maps_url, query, timestamp
                        ]
                        results.append(result)
                        log(f"[W{worker_id}] Extracted: {name}")
                
                except Exception as e:
                    log(f"[W{worker_id}] Error on business {idx}: {e}")
                    continue
            
            await browser.close()
            
            # Save results
            if results:
                save_results(results)
            
            log(f"[W{worker_id}] Completed: {len(results)} businesses")
            return len(results)
    
    except Exception as e:
        log(f"[W{worker_id}] Error: {e}")
        return 0

def scrape_task(args):
    """Wrapper for multiprocessing"""
    pincode, query, worker_id = args
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(scrape_location(pincode, query, worker_id))
    finally:
        loop.close()

def run_scraping():
    """Main scraping logic"""
    log("Starting scraping cycle...")
    
    pincodes = read_pincodes()
    if not pincodes:
        log("No pincodes found. Exiting.")
        return
    
    # Create tasks
    tasks = []
    for pincode in pincodes:
        for query in QUERIES:
            tasks.append((pincode, query, len(tasks) % MAX_WORKERS + 1))
    
    log(f"Total tasks: {len(tasks)}")
    
    # Execute with multiprocessing
    start_time = time.time()
    total_results = 0
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(scrape_task, tasks)
        total_results = sum(results)
    
    duration = time.time() - start_time
    log(f"Completed! Total: {total_results} businesses in {duration:.1f}s")

def run_as_background():
    """Run as background service"""
    if is_running():
        print("Service is already running. Use --stop to stop it first.")
        sys.exit(1)
    
    # Fork to background
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process
            print(f"Service started in background (PID: {pid})")
            sys.exit(0)
    except AttributeError:
        # Windows doesn't support fork
        print("Background mode requires Unix/Linux. Use subprocess or run in terminal.")
        sys.exit(1)
    
    # Child process - detach from terminal
    os.setsid()
    os.chdir('/')
    
    # Redirect stdout/stderr
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    
    # Write PID file
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    
    # Run continuous scraping
    try:
        while True:
            run_scraping()
            log("Waiting 1 hour before next cycle...")
            time.sleep(3600)  # 1 hour
    except KeyboardInterrupt:
        pass
    finally:
        try:
            os.remove(PID_FILE)
        except:
            pass

def main():
    """Main entry point"""
    if len(sys.argv) > 1:
        if sys.argv[1] == '--background':
            run_as_background()
        elif sys.argv[1] == '--status':
            show_status()
        elif sys.argv[1] == '--stop':
            stop_service()
        else:
            print("Usage:")
            print("  python scraper.py              # Run normally")
            print("  python scraper.py --background # Run in background")
            print("  python scraper.py --status     # Check status")
            print("  python scraper.py --stop       # Stop background service")
    else:
        # Run normally
        run_scraping()

if __name__ == "__main__":
    main()