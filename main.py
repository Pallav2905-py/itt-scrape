import asyncio
import csv
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from playwright.async_api import async_playwright
import time
from datetime import datetime
import os

# Configuration
QUERIES = [
    "toys shops"
    "gift shops", 
    "toy stores",
    "children toys",
    "kids toys"
]
MAX_WORKERS = 30
OUTPUT_CSV = "toy_shops_pune_detailed.csv"
OUTPUT_JSON = "toy_shops_pune_detailed.json"

# Thread-safe file writing
file_lock = threading.Lock()

def read_pincodes_from_csv(filename="pincode.csv"):
    """Read pincodes from CSV file"""
    pincodes = []
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'pincode' in row and row['pincode'].strip():
                    pincodes.append(row['pincode'].strip())
        print(f"Loaded {len(pincodes)} pincodes from {filename}")
        return pincodes
    except FileNotFoundError:
        print(f"Error: {filename} not found!")
        return []
    except Exception as e:
        print(f"Error reading pincodes: {e}")
        return []

def read_cities_from_csv(filename="city_list.csv"):
    """Read cities from CSV file"""
    cities = []
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Try different possible column names for city
                city_value = None
                for col in ['city', 'City', 'CITY', 'city_name', 'City_Name']:
                    if col in row and row[col].strip():
                        city_value = row[col].strip()
                        break
                
                if city_value:
                    cities.append(city_value)
        print(f"Loaded {len(cities)} cities from {filename}")
        return cities
    except FileNotFoundError:
        print(f"Error: {filename} not found!")
        return []
    except Exception as e:
        print(f"Error reading cities: {e}")
        return []

def append_to_csv(data, filename=OUTPUT_CSV):
    """Thread-safe append to CSV file"""
    if not data:
        return
    
    with file_lock:
        # Check if file exists to write header
        file_exists = os.path.exists(filename)
        
        with open(filename, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            
            # Write header if file is new
            if not file_exists:
                writer.writerow(["Name", "Address", "Rating", "Phone", "Hours", "Category", "Website", "Location", "Location_Type", "Query", "Timestamp"])
            
            # Write data rows
            for row in data:
                writer.writerow(row)

def append_to_json(data, filename=OUTPUT_JSON):
    """Thread-safe append to JSON file"""
    if not data:
        return
    
    with file_lock:
        existing_data = []
        
        # Read existing data if file exists
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                existing_data = []
        
        # Convert data rows to dictionaries
        headers = ["Name", "Address", "Rating", "Phone", "Hours", "Category", "Website", "Location", "Location_Type", "Query", "Timestamp"]
        new_records = []
        for row in data:
            record = {}
            for i, header in enumerate(headers):
                record[header] = row[i] if i < len(row) else ""
            new_records.append(record)
        
        # Append new data
        existing_data.extend(new_records)
        
        # Write back to file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)

async def scrape_location_query(location, location_type, query, worker_id):
    """Scrape Google Maps for a specific location (pincode or city) and query"""
    if location_type == "Pincode":
        search_query = f"{query}+{location}"
    else:  # City
        search_query = f"{query}+{location}+india"
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"[Worker {worker_id}] Starting: {query} in {location} ({location_type})")
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            # Set timeout and disable images
            page.set_default_timeout(30000)
            await context.route("/*.{png,jpg,jpeg,gif,svg,ico,webp}", lambda route: route.abort())

            # Navigate to Google Maps search
            search_url = f"https://www.google.com/maps/search/{search_query}/"
            print(f"[Worker {worker_id}] Navigating to: {search_url}")
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            
            await page.wait_for_timeout(5000)
            
            # Wait for results
            try:
                await page.wait_for_selector('.Nv2PK', timeout=20000)
            except:
                print(f"[Worker {worker_id}] No results found for {query} in {location} ({location_type})")
                await browser.close()
                return []

            # Scroll to load results
            scroll_attempts = 0
            max_scrolls = 15
            prev_count = 0
            
            while scroll_attempts < max_scrolls:
                await page.evaluate('''
                    const feed = document.querySelector('[role="feed"]');
                    if (feed) feed.scrollTop = feed.scrollHeight;
                    
                    const main = document.querySelector('[role="main"]');
                    if (main) main.scrollTop = main.scrollHeight;
                    
                    window.scrollTo(0, document.body.scrollHeight);
                ''')
                
                await page.wait_for_timeout(2000)
                
                current_results = await page.locator('.Nv2PK').count()
                
                if current_results == prev_count and scroll_attempts > 5:
                    break
                
                prev_count = current_results
                scroll_attempts += 1

            # Extract shop data
            results = []
            shop_containers = await page.locator('.Nv2PK').all()
            
            print(f"[Worker {worker_id}] Extracting data from {len(shop_containers)} shops for {query} in {location} ({location_type})")
            
            for i, shop in enumerate(shop_containers):
                try:
                    # Extract name
                    name = ""
                    try:
                        name_elem = shop.locator('.qBF1Pd.fontHeadlineSmall')
                        if await name_elem.count() > 0:
                            name = await name_elem.inner_text()
                    except:
                        pass
                    
                    # Extract rating
                    rating = ""
                    try:
                        rating_elem = shop.locator('span[role="img"]')
                        if await rating_elem.count() > 0:
                            rating = await rating_elem.get_attribute('aria-label')
                    except:
                        pass
                    
                    # Extract category
                    category = ""
                    try:
                        category_elem = shop.locator('.W4Efsd span').first
                        if await category_elem.count() > 0:
                            category = await category_elem.inner_text()
                    except:
                        pass
                    
                    # Extract address
                    address = ""
                    try:
                        address_divs = await shop.locator('.W4Efsd').all()
                        for div in address_divs:
                            div_text = await div.inner_text()
                            div_text = div_text.replace('â‹…', '·').replace('â€¯', ' ').strip()
                            
                            if any(word in div_text for word in ['Road', 'Complex', 'Street', 'Nagar', 'Colony', 'Shop no', 'Warehouse', 'chowk', 'opposite']):
                                lines = div_text.split('·')
                                for line in lines:
                                    line = line.strip()
                                    if any(word in line.lower() for word in ['open', 'closed', 'closes', 'pm', 'am']):
                                        continue
                                    if line.lower() in ['toy store', 'store']:
                                        continue
                                    if any(word in line for word in ['Road', 'Complex', 'Street', 'Nagar', 'Colony', 'Shop no', 'Warehouse', 'chowk', 'opposite']) and len(line) > 10:
                                        address = line
                                        break
                                if address:
                                    break
                    except:
                        pass
                    
                    # Extract phone
                    phone = ""
                    try:
                        phone_elem = shop.locator('.UsdlK')
                        if await phone_elem.count() > 0:
                            phone = await phone_elem.inner_text()
                    except:
                        pass
                    
                    # Extract hours
                    hours = ""
                    try:
                        hours_divs = await shop.locator('.W4Efsd').all()
                        for div in hours_divs:
                            div_text = await div.inner_text()
                            div_text = div_text.replace('â‹…', '·').replace('â€¯', ' ').strip()
                            
                            if any(word in div_text for word in ['Open', 'Closed', 'Closes', 'pm', 'am']):
                                lines = div_text.split('·')
                                for line in lines:
                                    line = line.strip()
                                    if any(word in line for word in ['Open', 'Closed', 'Closes', 'pm', 'am']):
                                        hours = line
                                        break
                                if hours:
                                    break
                    except:
                        pass
                    
                    # Extract website
                    website = ""
                    try:
                        link_elem = shop.locator('a.hfpxzc')
                        if await link_elem.count() > 0:
                            href = await link_elem.get_attribute('href')
                            if href and 'google.com/maps' in href:
                                website = href
                    except:
                        pass

                    # Add to results if we have at least a name
                    if name and name.strip() and len(name.strip()) > 2:
                        results.append([
                            name.strip(),
                            address.strip(),
                            rating.strip(),
                            phone.strip(),
                            hours.strip(),
                            category.strip(),
                            website.strip(),
                            location,
                            location_type,
                            query,
                            timestamp
                        ])
                
                except Exception as e:
                    print(f"[Worker {worker_id}] Error extracting shop {i}: {e}")
                    continue

            await browser.close()
            
            # Save results immediately
            if results:
                append_to_csv(results)
                append_to_json(results)
                print(f"[Worker {worker_id}] Completed: {len(results)} shops found for {query} in {location} ({location_type})")
            else:
                print(f"[Worker {worker_id}] No results for {query} in {location} ({location_type})")
            
            return results

    except Exception as e:
        print(f"[Worker {worker_id}] Error scraping {query} in {location} ({location_type}): {e}")
        return []

def run_scraping_task(location, location_type, query, worker_id):
    """Wrapper to run async scraping in thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(scrape_location_query(location, location_type, query, worker_id))
    finally:
        loop.close()

def main():
    """Main function to orchestrate multithreaded scraping"""
    print("Starting multithreaded Google Maps scraper...")
    print(f"Configuration: {MAX_WORKERS} workers, {len(QUERIES)} queries")
    
    # Read pincodes and cities
    pincodes = read_pincodes_from_csv()
    cities = read_cities_from_csv()
    
    if not pincodes and not cities:
        print("No pincodes or cities found. Exiting...")
        return
    
    # Create tasks for all combinations of locations and queries
    tasks = []
    
    # Add pincode tasks
    for pincode in pincodes:
        for query in QUERIES:
            tasks.append((pincode, "Pincode", query))
    
    # Add city tasks
    for city in cities:
        for query in QUERIES:
            tasks.append((city, "City", query))
    
    print(f"Total tasks to process: {len(tasks)} ({len(pincodes)} pincodes, {len(cities)} cities)")
    
    # Clear output files
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)
    if os.path.exists(OUTPUT_JSON):
        os.remove(OUTPUT_JSON)
    
    start_time = time.time()
    total_results = 0
    
    # Process tasks with thread pool
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        for i, (location, location_type, query) in enumerate(tasks):
            worker_id = (i % MAX_WORKERS) + 1
            future = executor.submit(run_scraping_task, location, location_type, query, worker_id)
            futures.append(future)
            
            # Add small delay between task submissions
            time.sleep(1)
        
        # Wait for all tasks to complete
        for i, future in enumerate(futures):
            try:
                results = future.result(timeout=300)  # 5 minute timeout per task
                total_results += len(results)
                print(f"Task {i+1}/{len(tasks)} completed")
            except Exception as e:
                print(f"Task {i+1} failed: {e}")
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n{'='*50}")
    print(f"Scraping completed!")
    print(f"Total results: {total_results}")
    print(f"Total time: {duration:.2f} seconds")
    print(f"Results saved to: {OUTPUT_CSV} and {OUTPUT_JSON}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()