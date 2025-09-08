import asyncio
import csv
import json
import threading
import random
import signal
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from playwright.async_api import async_playwright
import time
from datetime import datetime
import os
from asyncio import Semaphore
import logging

# Enhanced Configuration
QUERIES = [
    "Video Conferencing Resellers",
    "Video Conferencing Distributors", 
    "VC Distributors",
    "Conferencing Solutions Resellers",
    "IT System Integrators",
    "Video Conferencing Systems Distributors",
    "IT Hardware Distributors",
    "Technology Resellers",
    "IT Hardware Channel Partners",
    "Logitech Video Conferencing Distributors",
    "Logitech VC Systems Distributors",
]

# Optimized settings for high concurrency
MAX_WORKERS = 500  # Reduced from 500 to prevent resource exhaustion
MAX_BROWSER_INSTANCES = 250  # Maximum concurrent browser instances
OUTPUT_CSV = "vc_partner_new.csv"
OUTPUT_JSON = "vc_partner_new.json"

# Thread-safe file writing and browser management
file_lock = threading.Lock()
browser_semaphore = None  # Will be initialized in main()
failed_tasks = []

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BrowserPool:
    """Manages a pool of browser instances to prevent resource exhaustion"""
    def __init__(self, max_browsers=10):
        self.max_browsers = max_browsers
        self.browsers = []
        self.available_browsers = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(max_browsers)
        self._initialized = False
    
    async def initialize(self):
        """Initialize browser pool"""
        if self._initialized:
            return
        
        playwright = await async_playwright().start()
        for i in range(self.max_browsers):
            try:
                browser = await playwright.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-renderer-backgrounding',
                        '--disable-features=TranslateUI',
                        '--disable-extensions',
                        '--disable-default-apps',
                        '--disable-sync',
                        '--memory-pressure-off',
                        '--max_old_space_size=4096',
                    ]
                )
                await self.available_browsers.put(browser)
                self.browsers.append(browser)
                logger.info(f"Initialized browser {i+1}/{self.max_browsers}")
            except Exception as e:
                logger.error(f"Failed to initialize browser {i+1}: {e}")
        
        self._initialized = True
    
    async def get_browser(self):
        """Get an available browser from the pool"""
        await self.semaphore.acquire()
        return await self.available_browsers.get()
    
    async def return_browser(self, browser):
        """Return browser to the pool"""
        await self.available_browsers.put(browser)
        self.semaphore.release()
    
    async def close_all(self):
        """Close all browsers in the pool"""
        for browser in self.browsers:
            try:
                await browser.close()
            except:
                pass

# Global browser pool
browser_pool = BrowserPool(MAX_BROWSER_INSTANCES)

def read_pincodes_from_csv(filename="pincode.csv"):
    """Read pincodes from CSV file"""
    pincodes = []
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'pincode' in row and row['pincode'].strip():
                    pincodes.append(row['pincode'].strip())
        logger.info(f"Loaded {len(pincodes)} pincodes from {filename}")
        return pincodes
    except FileNotFoundError:
        logger.error(f"Error: {filename} not found!")
        return []
    except Exception as e:
        logger.error(f"Error reading pincodes: {e}")
        return []

def append_to_csv(data, filename=OUTPUT_CSV):
    """Thread-safe append to CSV file"""
    if not data:
        return
    
    with file_lock:
        file_exists = os.path.exists(filename)
        
        with open(filename, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            
            if not file_exists:
                writer.writerow([
                    "Company_Name", "Address", "Rating", "Phone", "Hours", 
                    "Business_Category", "Website", "Location", "Location_Type", 
                    "Search_Query", "Partner_Type", "Services_Offered", "Timestamp"
                ])
            
            for row in data:
                writer.writerow(row)

def append_to_json(data, filename=OUTPUT_JSON):
    """Thread-safe append to JSON file"""
    if not data:
        return
    
    with file_lock:
        existing_data = []
        
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                existing_data = []
        
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
        
        existing_data.extend(new_records)
        
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

async def scrape_location_query_with_retry(location, location_type, query, worker_id, max_retries=3):
    """Scrape with retry mechanism and better error handling"""
    
    for attempt in range(max_retries):
        try:
            # Add random delay to prevent overwhelming
            await asyncio.sleep(random.uniform(0.5, 2.0))
            
            browser = await browser_pool.get_browser()
            
            try:
                result = await scrape_with_browser(browser, location, location_type, query, worker_id, attempt)
                await browser_pool.return_browser(browser)
                return result
            except Exception as e:
                await browser_pool.return_browser(browser)
                if attempt == max_retries - 1:
                    logger.error(f"[Worker {worker_id}] Final attempt failed for {query} in {location}: {e}")
                    failed_tasks.append((location, location_type, query, worker_id))
                    return []
                else:
                    logger.warning(f"[Worker {worker_id}] Attempt {attempt + 1} failed for {query} in {location}: {e}")
                    await asyncio.sleep(random.uniform(2, 5))
                    continue
                    
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Critical error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                return []
            await asyncio.sleep(random.uniform(3, 7))
    
    return []

async def scrape_with_browser(browser, location, location_type, query, worker_id, attempt):
    """Actual scraping logic using provided browser"""
    if location_type == "Pincode":
        search_query = f"{query}+{location}+india"
    else:
        search_query = f"{query}+{location}+india"
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logger.info(f"[Worker {worker_id}] Attempt {attempt + 1}: {query} in {location} ({location_type})")
    
    try:
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        )
        page = await context.new_page()
        
        # Enhanced page settings
        page.set_default_timeout(45000)  # Increased timeout
        await page.set_extra_http_headers({
            'Accept-Language': 'en-US,en;q=0.9',
        })
        
        # Block unnecessary resources
        await context.route("**/*.{png,jpg,jpeg,gif,svg,ico,webp,css,font,woff,woff2}", lambda route: route.abort())
        
        # Navigate with better error handling
        search_url = f"https://www.google.com/maps/search/{search_query}/"
        
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
        except Exception as e:
            logger.warning(f"[Worker {worker_id}] Navigation timeout, trying alternative approach: {e}")
            await page.goto("https://www.google.com/maps", wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            search_box = await page.query_selector('input[id="searchboxinput"]')
            if search_box:
                await search_box.fill(search_query.replace('+', ' '))
                await page.keyboard.press('Enter')
            await page.wait_for_timeout(5000)
        
        # Wait for results with multiple selectors
        result_loaded = False
        selectors_to_try = ['.Nv2PK', '[data-value="Search results"]', '.section-result', '[role="main"]']
        
        for selector in selectors_to_try:
            try:
                await page.wait_for_selector(selector, timeout=15000)
                result_loaded = True
                break
            except:
                continue
        
        if not result_loaded:
            logger.warning(f"[Worker {worker_id}] No results found for {query} in {location}")
            await context.close()
            return []

        # Enhanced scrolling logic
        scroll_attempts = 0
        max_scrolls = 15
        prev_count = 0
        stable_count = 0
        
        while scroll_attempts < max_scrolls:
            # Multiple scroll strategies
            await page.evaluate('''
                const scrollableElements = [
                    document.querySelector('[role="feed"]'),
                    document.querySelector('[role="main"]'),
                    document.querySelector('.m6QErb'),
                    document.body
                ];
                
                scrollableElements.forEach(element => {
                    if (element) {
                        element.scrollTop = element.scrollHeight;
                    }
                });
                
                window.scrollTo(0, document.body.scrollHeight);
            ''')
            
            await page.wait_for_timeout(random.randint(2000, 4000))
            
            current_results = await page.locator('.Nv2PK').count()
            
            if current_results == prev_count:
                stable_count += 1
                if stable_count >= 3:  # Stop if count is stable for 3 attempts
                    break
            else:
                stable_count = 0
            
            prev_count = current_results
            scroll_attempts += 1

        # Extract business data with enhanced selectors
        results = []
        business_containers = await page.locator('.Nv2PK').all()
        
        logger.info(f"[Worker {worker_id}] Extracting data from {len(business_containers)} businesses")
        
        for i, business in enumerate(business_containers):
            try:
                # Enhanced data extraction with multiple fallback selectors
                company_name = await extract_text_with_fallbacks(business, [
                    '.qBF1Pd.fontHeadlineSmall',
                    '.qBF1Pd',
                    '[data-value="Name"]',
                    '.section-result-title'
                ])
                
                rating = await extract_attribute_with_fallbacks(business, 'aria-label', [
                    'span[role="img"]',
                    '.MW4etd'
                ])
                
                category = await extract_text_with_fallbacks(business, [
                    '.W4Efsd span:first-child',
                    '.W4Efsd:first-child span',
                    '.section-result-details span'
                ])
                
                # Enhanced address extraction
                address = await extract_address(business)
                
                phone = await extract_text_with_fallbacks(business, [
                    '.UsdlK',
                    '[data-value="Phone number"]'
                ])
                
                hours = await extract_hours(business)
                
                website = await extract_attribute_with_fallbacks(business, 'href', [
                    'a.hfpxzc',
                    'a[data-value="Website"]'
                ])

                partner_type = classify_partner_type(query, category, company_name)
                services_offered = extract_services_offered(category, company_name, address)

                if company_name and len(company_name.strip()) > 2:
                    # Enhanced filtering
                    if is_relevant_business(company_name, category):
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
                logger.warning(f"[Worker {worker_id}] Error extracting business {i}: {e}")
                continue

        await context.close()
        
        # Save results immediately
        if results:
            append_to_csv(results)
            append_to_json(results)
            logger.info(f"[Worker {worker_id}] Success: {len(results)} partners found for {query} in {location}")
        
        return results

    except Exception as e:
        logger.error(f"[Worker {worker_id}] Scraping error: {e}")
        raise e

async def extract_text_with_fallbacks(element, selectors):
    """Extract text using multiple fallback selectors"""
    for selector in selectors:
        try:
            locator = element.locator(selector)
            if await locator.count() > 0:
                return await locator.inner_text()
        except:
            continue
    return ""

async def extract_attribute_with_fallbacks(element, attribute, selectors):
    """Extract attribute using multiple fallback selectors"""
    for selector in selectors:
        try:
            locator = element.locator(selector)
            if await locator.count() > 0:
                return await locator.get_attribute(attribute) or ""
        except:
            continue
    return ""

async def extract_address(business):
    """Enhanced address extraction"""
    address = ""
    try:
        address_divs = await business.locator('.W4Efsd').all()
        for div in address_divs:
            div_text = await div.inner_text()
            div_text = div_text.replace('â‹…', '·').replace('â€¯', ' ').strip()
            
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
    return address

async def extract_hours(business):
    """Enhanced hours extraction"""
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
    return hours

def is_relevant_business(company_name, category):
    """Enhanced relevance filtering"""
    exclude_keywords = [
        'restaurant', 'food', 'hotel', 'hospital', 'medical', 
        'clinic', 'pharmacy', 'bank', 'atm', 'petrol', 'gas station',
        'school', 'college', 'temple', 'church', 'mosque'
    ]
    
    combined_check = f"{company_name} {category}".lower()
    return not any(exclude in combined_check for exclude in exclude_keywords)

async def process_tasks_async(tasks):
    """Process all tasks asynchronously"""
    await browser_pool.initialize()
    
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    
    async def bounded_task(task):
        async with semaphore:
            location, location_type, query, worker_id = task
            return await scrape_location_query_with_retry(location, location_type, query, worker_id)
    
    # Process all tasks
    results = await asyncio.gather(*[bounded_task(task) for task in tasks], return_exceptions=True)
    
    await browser_pool.close_all()
    
    return results

def run_async_scraping(tasks):
    """Run the async scraping process"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(process_tasks_async(tasks))
    finally:
        loop.close()

def main():
    """Main function with improved error handling and resource management"""
    logger.info("Starting Enhanced Video Conferencing & IT Hardware Channel Partners scraper...")
    logger.info(f"Configuration: {MAX_WORKERS} workers, {MAX_BROWSER_INSTANCES} browser instances")
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info("Received interrupt signal. Shutting down gracefully...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Read pincodes
    pincodes = read_pincodes_from_csv()
    
    if not pincodes:
        logger.error("No pincodes found. Exiting...")
        return
    
    # Create tasks
    tasks = []
    for i, pincode in enumerate(pincodes):
        for j, query in enumerate(QUERIES):
            worker_id = (i * len(QUERIES) + j) + 1
            tasks.append((pincode, "Pincode", query, worker_id))
    
    logger.info(f"Total tasks to process: {len(tasks)}")
    logger.info(f"Estimated completion time: {len(tasks) * 1.5 / MAX_WORKERS:.1f} minutes")
    
    # Clear output files
    for file in [OUTPUT_CSV, OUTPUT_JSON]:
        if os.path.exists(file):
            os.remove(file)
    
    start_time = time.time()
    
    try:
        # Run scraping
        results = run_async_scraping(tasks)
        
        # Count successful results
        total_results = sum(len(r) for r in results if isinstance(r, list))
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Enhanced Scraping Completed!")
        logger.info(f"Total channel partners found: {total_results}")
        logger.info(f"Total execution time: {duration:.2f} seconds ({duration/60:.1f} minutes)")
        logger.info(f"Failed tasks: {len(failed_tasks)}")
        logger.info(f"Success rate: {((len(tasks) - len(failed_tasks)) / len(tasks) * 100):.1f}%")
        logger.info(f"Results saved to: {OUTPUT_CSV} and {OUTPUT_JSON}")
        logger.info(f"{'='*60}")
        
        if failed_tasks:
            logger.info(f"\nFailed tasks summary:")
            for task in failed_tasks[:10]:  # Show first 10 failed tasks
                logger.info(f"  - {task[2]} in {task[0]} ({task[1]})")
    
    except Exception as e:
        logger.error(f"Fatal error in main process: {e}")
        return

if __name__ == "__main__":
    main()