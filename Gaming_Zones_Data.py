import asyncio
import csv
import json
import threading
from concurrent.futures import ProcessPoolExecutor
from playwright.async_api import async_playwright
import time
from datetime import datetime
import os
import re

# Configuration - Updated for Gaming Zones
QUERIES = ["gaming zones", "game arcades", "VR gaming centers"]
MAX_WORKERS = 5
OUTPUT_CSV = "gaming_zones_usa_canada.csv"
OUTPUT_JSON = "gaming_zones_usa_canada.json"

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
                writer.writerow([
                    "Business_Name", "Contact_Person", "Phone", "Email", "Website",
                    "City", "State", "Postal_Code", "Full_Address", "Rating", 
                    "Review_Count", "Category", "Hours", "Plus_Code", "Located_In",
                    "Price_Level", "Amenities", "Google_Maps_URL", "Query", "Timestamp"
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
        
        # Convert data rows to dictionaries
        headers = [
            "Business_Name", "Contact_Person", "Phone", "Email", "Website",
            "City", "State", "Postal_Code", "Full_Address", "Rating", 
            "Review_Count", "Category", "Hours", "Plus_Code", "Located_In",
            "Price_Level", "Amenities", "Google_Maps_URL", "Query", "Timestamp"
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

def extract_contact_info(text):
    """Extract phone numbers and emails from text"""
    # US/Canada phone patterns
    phone_patterns = [
        r'\+?1[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})',
        r'\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})'
    ]
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    
    phone_number = ""
    for pattern in phone_patterns:
        match = re.search(pattern, text)
        if match:
            if len(match.groups()) == 3:
                phone_number = f"+1 {match.group(1)}-{match.group(2)}-{match.group(3)}"
            break
    
    emails = re.findall(email_pattern, text)
    email = emails[0] if emails else ""
    
    return phone_number, email

def parse_address(address_text):
    """Parse address into components"""
    city = ""
    state = ""
    postal_code = ""
    
    # Extract postal code (US: 5 digits or ZIP+4, Canada: A1A 1A1 format)
    postal_patterns = [
        r'\b(\d{5}(?:-\d{4})?)\b',  # US ZIP
        r'\b([A-Z]\d[A-Z]\s?\d[A-Z]\d)\b'  # Canada postal code
    ]
    
    for pattern in postal_patterns:
        match = re.search(pattern, address_text, re.IGNORECASE)
        if match:
            postal_code = match.group(1).upper()
            break
    
    # Extract state/province (2-3 letter abbreviation before postal code)
    if postal_code:
        state_pattern = rf'\b([A-Z]{{2,3}})\s+{re.escape(postal_code)}'
        match = re.search(state_pattern, address_text, re.IGNORECASE)
        if match:
            state = match.group(1).upper()
    
    # Extract city (text before state)
    if state:
        city_pattern = rf'([^,\n]+),\s*{re.escape(state)}'
        match = re.search(city_pattern, address_text, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
    
    return city, state, postal_code

async def scrape_location_query(pincode, country, query, worker_id):
    """Scrape Google Maps for gaming zones"""
    search_query = f"{query} {pincode} {country}"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"[Worker {worker_id}] Starting: {query} in {pincode}, {country}")
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            
            page.set_default_timeout(30000)
            await context.route("/*.{png,jpg,jpeg,gif,svg,ico,webp}", lambda route: route.abort())

            search_url = f"https://www.google.com/maps/search/{search_query}/"
            print(f"[Worker {worker_id}] Navigating to: {search_url}")
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            
            await page.wait_for_timeout(5000)
            
            try:
                await page.wait_for_selector('.Nv2PK', timeout=20000)
            except:
                print(f"[Worker {worker_id}] No results found")
                await browser.close()
                return []

            # Scroll to load results
            scroll_attempts = 0
            max_scrolls = 10
            
            while scroll_attempts < max_scrolls:
                await page.evaluate('''
                    const feed = document.querySelector('[role="feed"]');
                    if (feed) feed.scrollTop = feed.scrollHeight;
                ''')
                await page.wait_for_timeout(2000)
                scroll_attempts += 1

            # Get all business listings
            business_containers = await page.locator('.Nv2PK').all()
            print(f"[Worker {worker_id}] Found {len(business_containers)} businesses")
            
            results = []
            
            for i, business in enumerate(business_containers):
                try:
                    # Click on business to open details
                    await business.click()
                    await page.wait_for_timeout(3000)
                    
                    # Get the current URL after clicking on the business
                    google_maps_url = page.url
                    
                    # Extract business name
                    business_name = ""
                    name_selectors = ['.DUwDvf', '.qBF1Pd.fontHeadlineSmall', 'h1']
                    for selector in name_selectors:
                        try:
                            name_elem = page.locator(selector).first
                            if await name_elem.count() > 0:
                                business_name = await name_elem.inner_text()
                                break
                        except:
                            continue
                    
                    # Extract rating and review count
                    rating = ""
                    review_count = ""
                    try:
                        rating_elem = page.locator('.F7nice span[aria-hidden="true"]').first
                        if await rating_elem.count() > 0:
                            rating = await rating_elem.inner_text()
                        
                        review_elem = page.locator('.F7nice span[aria-label*="reviews"]').first
                        if await review_elem.count() > 0:
                            review_text = await review_elem.inner_text()
                            review_count = re.search(r'\(([0-9,]+)\)', review_text)
                            review_count = review_count.group(1) if review_count else ""
                    except:
                        pass
                    
                    # Extract category
                    category = ""
                    try:
                        category_elem = page.locator('button[jsaction*="category"]').first
                        if await category_elem.count() > 0:
                            category = await category_elem.inner_text()
                    except:
                        pass
                    
                    # Extract full address
                    full_address = ""
                    address_selectors = [
                        '[data-item-id="address"] .Io6YTe',
                        '.Io6YTe.fontBodyMedium.kR99db.fdkmkc'
                    ]
                    for selector in address_selectors:
                        try:
                            addr_elem = page.locator(selector).first
                            if await addr_elem.count() > 0:
                                full_address = await addr_elem.inner_text()
                                break
                        except:
                            continue
                    
                    # Parse address components
                    city, state, postal_code = parse_address(full_address)
                    
                    # Extract "Located in" information
                    located_in = ""
                    try:
                        located_elem = page.locator('[data-item-id="locatedin"] .Io6YTe').first
                        if await located_elem.count() > 0:
                            located_in = await located_elem.inner_text()
                            located_in = located_in.replace("Located in: ", "")
                    except:
                        pass
                    
                    # Extract hours
                    hours = ""
                    try:
                        hours_elem = page.locator('.ZDu9vd').first
                        if await hours_elem.count() > 0:
                            hours = await hours_elem.inner_text()
                    except:
                        pass
                    
                    # Extract Plus Code
                    plus_code = ""
                    try:
                        plus_elem = page.locator('[data-item-id="oloc"] .Io6YTe').first
                        if await plus_elem.count() > 0:
                            plus_code = await plus_elem.inner_text()
                    except:
                        pass
                    
                    # Extract website
                    website = ""
                    try:
                        website_elem = page.locator('[data-item-id="authority"] a').first
                        if await website_elem.count() > 0:
                            website = await website_elem.get_attribute('href')
                    except:
                        pass
                    
                    # Extract phone number
                    contact_number = ""
                    phone_selectors = [
                        '[data-item-id*="phone"] .Io6YTe',
                        'button[aria-label*="Phone:"] .Io6YTe'
                    ]
                    for selector in phone_selectors:
                        try:
                            phone_elem = page.locator(selector).first
                            if await phone_elem.count() > 0:
                                phone_text = await phone_elem.inner_text()
                                contact_number, _ = extract_contact_info(phone_text)
                                if contact_number:
                                    break
                        except:
                            continue
                    
                    # Extract amenities (wheelchair accessible, etc.)
                    amenities = []
                    try:
                        amenity_elems = page.locator('.wmQCje[data-tooltip]')
                        count = await amenity_elems.count()
                        for j in range(count):
                            amenity = await amenity_elems.nth(j).get_attribute('data-tooltip')
                            if amenity:
                                amenities.append(amenity)
                    except:
                        pass
                    amenities_str = "; ".join(amenities) if amenities else ""
                    
                    # Extract price level (if available)
                    price_level = ""
                    try:
                        # Look for price indicators like $, $$, $$$, $$$$
                        price_elem = page.locator('.mgr77e').first
                        if await price_elem.count() > 0:
                            price_level = await price_elem.inner_text()
                    except:
                        pass
                    
                    # Extract email from website or reviews
                    email_address = ""
                    try:
                        # Look for website link first
                        if website and '@' in website:
                            _, email_address = extract_contact_info(website)
                        
                        # If no email found, search in reviews and content
                        if not email_address:
                            page_content = await page.content()
                            _, email_address = extract_contact_info(page_content)
                    except:
                        pass
                    
                    # Extract contact person name from reviews or owner responses
                    contact_person_name = ""
                    try:
                        # Look for owner responses
                        owner_responses = page.locator('.CDe7pd .wiI7pd')
                        if await owner_responses.count() > 0:
                            response_text = await owner_responses.first.inner_text()
                            # Look for patterns like "Hi, this is John" or "Thank you, - Mike"
                            name_patterns = [
                                r'(?:Hi|Hello|Thanks?),?\s+(?:this\s+is\s+)?([A-Z][a-z]+)\b',
                                r'(?:Sincerely|Best|Regards),?\s+([A-Z][a-z]+)',
                                r'-\s*([A-Z][a-z]+)\s*$'
                            ]
                            for pattern in name_patterns:
                                match = re.search(pattern, response_text, re.MULTILINE | re.IGNORECASE)
                                if match:
                                    contact_person_name = match.group(1)
                                    break
                        
                        # Also check business description for owner mentions
                        if not contact_person_name:
                            desc_elem = page.locator('.PYvSYb').first
                            if await desc_elem.count() > 0:
                                desc_text = await desc_elem.inner_text()
                                owner_patterns = [
                                    r'owner\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
                                    r'founded\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
                                    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:opened|started|founded)'
                                ]
                                for pattern in owner_patterns:
                                    match = re.search(pattern, desc_text, re.IGNORECASE)
                                    if match:
                                        contact_person_name = match.group(1)
                                        break
                    except:
                        pass

                    # Only add if we have essential data
                    if business_name and business_name.strip():
                        results.append([
                            business_name.strip(),
                            contact_person_name.strip(),
                            contact_number.strip(),
                            email_address.strip(),
                            website.strip() if website else "",
                            city.strip(),
                            state.strip(),
                            postal_code.strip(),
                            full_address.strip(),
                            rating.strip(),
                            review_count.strip(),
                            category.strip(),
                            hours.strip(),
                            plus_code.strip(),
                            located_in.strip(),
                            price_level.strip(),
                            amenities_str.strip(),
                            google_maps_url.strip(),
                            query,
                            timestamp
                        ])
                        print(f"[Worker {worker_id}] Extracted: {business_name}")
                
                except Exception as e:
                    print(f"[Worker {worker_id}] Error extracting business {i}: {e}")
                    continue

            await browser.close()
            
            # Save results
            if results:
                append_to_csv(results)
                append_to_json(results)
                print(f"[Worker {worker_id}] Completed: {len(results)} gaming zones found")
            else:
                print(f"[Worker {worker_id}] No results found")
            
            return results

    except Exception as e:
        print(f"[Worker {worker_id}] Error scraping: {e}")
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
    """Main function to orchestrate scraping"""
    print("Starting Google Maps scraper for Gaming Zones in USA/Canada...")
    
    # Read pincodes
    pincodes = read_pincodes_from_csv()
    
    if not pincodes:
        print("No pincodes found. Exiting...")
        return
    
    # Create tasks for both countries
    tasks = []
    countries = ["USA", "Canada"]
    
    for pincode in pincodes:
        for country in countries:
            for query in QUERIES:
                tasks.append((pincode, country, query))
    
    print(f"Total tasks: {len(tasks)}")
    
    # Clear output files
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)
    if os.path.exists(OUTPUT_JSON):
        os.remove(OUTPUT_JSON)
    
    start_time = time.time()
    total_results = 0
    
    # Process tasks
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        for i, (pincode, country, query) in enumerate(tasks):
            worker_id = (i % MAX_WORKERS) + 1
            future = executor.submit(run_scraping_task, pincode, country, query, worker_id)
            futures.append(future)
        
        for i, future in enumerate(futures):
            try:
                results = future.result(timeout=300)
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