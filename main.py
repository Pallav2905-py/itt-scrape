import asyncio
import csv
import json
import threading
from concurrent.futures import ProcessPoolExecutor
from playwright.async_api import async_playwright
import time
from datetime import datetime
import os

# Configuration - Updated for Video Conferencing and IT Hardware Channel Partners
QUERIES = [
    "Video Conferencing Resellers",
    "Video Conferencing Distributors", 
    "VC Distributors",
    "Conferencing Solutions Resellers",
    "IT System Integrators",  # Updated from "System Integrators"
    "Video Conferencing Systems Distributors",
    "IT Hardware Distributors",
    "Technology Resellers",
    "IT Hardware Channel Partners",
    "Logitech Video Conferencing Distributors",
    "Logitech VC Systems Distributors",
]
MAX_WORKERS = 30  # Increased to 500
OUTPUT_CSV = "vc_channel_partners_detailed.csv"
OUTPUT_JSON = "vc_channel_partners_detailed.json"

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
            
            # Write header if file is new - Updated headers for channel partners
            if not file_exists:
                writer.writerow([
                    "Company_Name", "Address", "Rating", "Phone", "Hours", 
                    "Business_Category", "Website", "Location", "Location_Type", 
                    "Search_Query", "Partner_Type", "Services_Offered", "Timestamp"
                ])
            
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
        
        # Convert data rows to dictionaries - Updated headers
        headers = [
            "Company_Name", "Address", "Rating", "Phone", "Hours", 
            "Business_Category", "Website", "Location", "Location_Type", 
            "Search_Query", "Partner_Type", "Services_Offered", "Timestamp"
        ]
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

def classify_partner_type(query, category, name):
    """Classify the type of channel partner based on query and business info"""
    query_lower = query.lower()
    category_lower = category.lower() if category else ""
    name_lower = name.lower() if name else ""
    
    combined_text = f"{query_lower} {category_lower} {name_lower}"
    
    if any(word in combined_text for word in ['distributor', 'distribution']):
        return "Distributor"
    elif any(word in combined_text for word in ['reseller', 'dealer']):
        return "Reseller"
    elif any(word in combined_text for word in ['system integrator', 'integration']):
        return "System Integrator"
    elif any(word in combined_text for word in ['av ', 'audio visual', 'audiovisual']):
        return "AV Solutions Provider"
    elif any(word in combined_text for word in ['channel partner', 'partner']):
        return "Channel Partner"
    else:
        return "Technology Solutions Provider"

def extract_services_offered(category, name, address):
    """Extract potential services offered based on available information"""
    services = []
    combined_text = f"{category} {name} {address}".lower() if all([category, name, address]) else ""
    
    service_keywords = {
        "Video Conferencing": ['video conferencing', 'vc', 'conferencing', 'meeting solutions'],
        "IT Hardware": ['it hardware', 'hardware', 'computer', 'technology'],
        "Audio Visual": ['audio visual', 'av', 'audiovisual', 'sound', 'display'],
        "System Integration": ['system integration', 'integration', 'solutions'],
        "Enterprise Solutions": ['enterprise', 'business solutions', 'corporate'],
        "Logitech Products": ['logitech', 'logi']
    }
    
    for service, keywords in service_keywords.items():
        if any(keyword in combined_text for keyword in keywords):
            services.append(service)
    
    return "; ".join(services) if services else "IT/Technology Solutions"

async def scrape_location_query(location, location_type, query, worker_id):
    """Scrape Google Maps for Video Conferencing and IT Hardware channel partners"""
    if location_type == "Pincode":
        search_query = f"{query}+{location}+india"
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
            max_scrolls = 20  # Increased for business listings
            prev_count = 0
            
            while scroll_attempts < max_scrolls:
                await page.evaluate('''
                    const feed = document.querySelector('[role="feed"]');
                    if (feed) feed.scrollTop = feed.scrollHeight;
                    
                    const main = document.querySelector('[role="main"]');
                    if (main) main.scrollTop = main.scrollHeight;
                    
                    window.scrollTo(0, document.body.scrollHeight);
                ''')
                
                await page.wait_for_timeout(3000)  # Increased wait time
                
                current_results = await page.locator('.Nv2PK').count()
                
                if current_results == prev_count and scroll_attempts > 8:
                    break
                
                prev_count = current_results
                scroll_attempts += 1

            # Extract business data
            results = []
            business_containers = await page.locator('.Nv2PK').all()
            
            print(f"[Worker {worker_id}] Extracting data from {len(business_containers)} businesses for {query} in {location} ({location_type})")
            
            for i, business in enumerate(business_containers):
                try:
                    # Extract company name
                    company_name = ""
                    try:
                        name_elem = business.locator('.qBF1Pd.fontHeadlineSmall')
                        if await name_elem.count() > 0:
                            company_name = await name_elem.inner_text()
                    except:
                        pass
                    
                    # Extract rating
                    rating = ""
                    try:
                        rating_elem = business.locator('span[role="img"]')
                        if await rating_elem.count() > 0:
                            rating = await rating_elem.get_attribute('aria-label')
                    except:
                        pass
                    
                    # Extract business category
                    category = ""
                    try:
                        category_elem = business.locator('.W4Efsd span').first
                        if await category_elem.count() > 0:
                            category = await category_elem.inner_text()
                    except:
                        pass
                    
                    # Extract address - Enhanced for business addresses
                    address = ""
                    try:
                        address_divs = await business.locator('.W4Efsd').all()
                        for div in address_divs:
                            div_text = await div.inner_text()
                            div_text = div_text.replace('â‹…', '·').replace('â€¯', ' ').strip()
                            
                            # Look for business address indicators
                            if any(word in div_text for word in [
                                'Road', 'Complex', 'Street', 'Nagar', 'Colony', 'Office', 
                                'Building', 'Floor', 'Block', 'Sector', 'Phase', 'Industrial'
                            ]):
                                lines = div_text.split('·')
                                for line in lines:
                                    line = line.strip()
                                    if any(word in line.lower() for word in ['open', 'closed', 'closes', 'pm', 'am']):
                                        continue
                                    if any(word in line for word in [
                                        'Road', 'Complex', 'Street', 'Nagar', 'Colony', 
                                        'Office', 'Building', 'Floor', 'Block', 'Sector'
                                    ]) and len(line) > 10:
                                        address = line
                                        break
                                if address:
                                    break
                    except:
                        pass
                    
                    # Extract phone
                    phone = ""
                    try:
                        phone_elem = business.locator('.UsdlK')
                        if await phone_elem.count() > 0:
                            phone = await phone_elem.inner_text()
                    except:
                        pass
                    
                    # Extract hours
                    hours = ""
                    try:
                        hours_divs = await business.locator('.W4Efsd').all()
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
                        link_elem = business.locator('a.hfpxzc')
                        if await link_elem.count() > 0:
                            href = await link_elem.get_attribute('href')
                            if href and 'google.com/maps' in href:
                                website = href
                    except:
                        pass

                    # Classify partner type and services
                    partner_type = classify_partner_type(query, category, company_name)
                    services_offered = extract_services_offered(category, company_name, address)

                    # Add to results if we have at least a company name and it's relevant
                    if company_name and company_name.strip() and len(company_name.strip()) > 2:
                        # Filter out irrelevant results
                        exclude_keywords = [
                            'restaurant', 'food', 'hotel', 'hospital', 'medical', 
                            'clinic', 'pharmacy', 'bank', 'atm', 'petrol', 'gas station'
                        ]
                        
                        combined_check = f"{company_name} {category}".lower()
                        if not any(exclude in combined_check for exclude in exclude_keywords):
                            results.append([
                                company_name.strip(),
                                address.strip(),
                                rating.strip(),
                                phone.strip(),
                                hours.strip(),
                                category.strip(),
                                website.strip(),
                                location,
                                location_type,
                                query,
                                partner_type,
                                services_offered,
                                timestamp
                            ])
                
                except Exception as e:
                    print(f"[Worker {worker_id}] Error extracting business {i}: {e}")
                    continue

            await browser.close()
            
            # Save results immediately
            if results:
                append_to_csv(results)
                append_to_json(results)
                print(f"[Worker {worker_id}] Completed: {len(results)} channel partners found for {query} in {location} ({location_type})")
            else:
                print(f"[Worker {worker_id}] No results for {query} in {location} ({location_type})")
            
            return results

    except Exception as e:
        print(f"[Worker {worker_id}] Error scraping {query} in {location} ({location_type}): {e}")
        return []

def run_scraping_task(location, location_type, query, worker_id):
    """Wrapper to run async scraping in process"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(scrape_location_query(location, location_type, query, worker_id))
    finally:
        loop.close()

def main():
    """Main function to orchestrate multiprocess scraping for Video Conferencing Channel Partners"""
    print("Starting Video Conferencing & IT Hardware Channel Partners scraper...")
    print(f"Configuration: {MAX_WORKERS} workers, {len(QUERIES)} search queries")
    print("Target: Video Conferencing Resellers, Distributors, and System Integrators")
    
    # Read pincodes and cities
    pincodes = read_pincodes_from_csv()
    
    if not pincodes:
        print("No pincodes or cities found. Exiting...")
        return
    
    # Create tasks for all combinations of locations and queries
    tasks = []
    
    # Add pincode tasks
    for pincode in pincodes:
        for query in QUERIES:
            tasks.append((pincode, "Pincode", query))
    

    
    # print(f"Total tasks to process: {len(tasks)} ({len(pincodes)} pincodes, {len(cities)} cities)")
    print(f"Estimated completion time: {len(tasks) * 2 / MAX_WORKERS:.1f} minutes")
    
    # Clear output files
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)
    if os.path.exists(OUTPUT_JSON):
        os.remove(OUTPUT_JSON)
    
    start_time = time.time()
    total_results = 0
    
    # Process tasks with process pool
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        for i, (location, location_type, query) in enumerate(tasks):
            worker_id = (i % MAX_WORKERS) + 1
            future = executor.submit(run_scraping_task, location, location_type, query, worker_id)
            futures.append(future)
        
        # Wait for all tasks to complete
        completed = 0
        for i, future in enumerate(futures):
            try:
                results = future.result(timeout=300)  # 5 minute timeout per task
                total_results += len(results)
                completed += 1
                if completed % 50 == 0:  # Progress update every 50 tasks
                    print(f"Progress: {completed}/{len(tasks)} tasks completed ({completed/len(tasks)*100:.1f}%)")
            except Exception as e:
                print(f"Task {i+1} failed: {e}")
                completed += 1

    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n{'='*60}")
    print(f"Video Conferencing Channel Partners Scraping Completed!")
    print(f"Total channel partners found: {total_results}")
    print(f"Total execution time: {duration:.2f} seconds ({duration/60:.1f} minutes)")
    print(f"Average results per minute: {total_results/(duration/60):.1f}")
    print(f"Results saved to:")
    print(f"  - CSV: {OUTPUT_CSV}")
    print(f"  - JSON: {OUTPUT_JSON}")
    print(f"{'='*60}")
    
    # Print feasibility summary
    print(f"\nFEASIBILITY SUMMARY:")
    print(f"- Target Keywords: {len(QUERIES)} search terms")
    print(f"- Geographic Coverage: {len(pincodes)} pincodes + {len(cities)} cities")
    print(f"- Total Search Combinations: {len(tasks)}")
    print(f"- Estimated Timeline: {duration/3600:.1f} hours for full execution")
    print(f"- Data Quality: Structured with partner classification and services mapping")

if __name__ == "__main__":
    main()