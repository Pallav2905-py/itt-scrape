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
import logging
from collections import deque
import signal
import sys
import subprocess
import psutil
import platform

# Configuration - Updated for Gaming Zones
QUERIES = ["gaming zones", "game arcades", "VR gaming centers"]
MAX_WORKERS = 500
OUTPUT_CSV = "gaming_zones_usa.csv"
OUTPUT_JSON = "gaming_zones_usa.json"
LOG_FILE = "gaming_zones_scraper.log"
PID_FILE = "gaming_zones_scraper.pid"
MAX_LOG_LINES = 15

# Thread-safe file writing and logging
file_lock = threading.Lock()
log_lock = threading.Lock()

class RotatingLogger:
    def __init__(self, log_file, max_lines=15):
        self.log_file = log_file
        self.max_lines = max_lines
        self.buffer = deque(maxlen=max_lines)
        
    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        
        with log_lock:
            self.buffer.append(log_entry)
            self._write_to_file()
        
        # Don't print to console in daemon mode
        if not getattr(self, 'daemon_mode', False):
            print(log_entry)
    
    def _write_to_file(self):
        try:
            with open(self.log_file, 'w', encoding='utf-8') as f:
                for line in self.buffer:
                    f.write(line + '\n')
        except Exception as e:
            if not getattr(self, 'daemon_mode', False):
                print(f"Error writing to log file: {e}")

# Global logger instance
logger = RotatingLogger(LOG_FILE, MAX_LOG_LINES)

def create_pid_file():
    """Create PID file for daemon management"""
    try:
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))
        logger.log(f"PID file created: {PID_FILE} with PID: {os.getpid()}")
    except Exception as e:
        logger.log(f"Error creating PID file: {e}")

def remove_pid_file():
    """Remove PID file"""
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        logger.log("PID file removed")
    except Exception as e:
        logger.log(f"Error removing PID file: {e}")

def is_running():
    """Check if daemon is already running"""
    if not os.path.exists(PID_FILE):
        return False
    
    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        
        # Check if process is actually running
        return psutil.pid_exists(pid)
    except:
        return False

def stop_daemon():
    """Stop the running daemon - Windows compatible"""
    if not os.path.exists(PID_FILE):
        print("No PID file found. Daemon may not be running.")
        return False
    
    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        
        if psutil.pid_exists(pid):
            process = psutil.Process(pid)
            
            # Try graceful termination first
            process.terminate()
            
            # Wait for graceful shutdown
            try:
                process.wait(timeout=10)
                print(f"Daemon with PID {pid} stopped successfully")
            except psutil.TimeoutExpired:
                # Force kill if graceful shutdown fails
                process.kill()
                print(f"Daemon with PID {pid} force killed")
            
            remove_pid_file()
            return True
        else:
            print(f"Process with PID {pid} not found. Removing stale PID file.")
            remove_pid_file()
            return False
            
    except Exception as e:
        print(f"Error stopping daemon: {e}")
        return False

def daemonize():
    """Daemonize the process to run in background - Windows compatible"""
    system = platform.system().lower()
    
    if system == "windows":
        # Windows doesn't support fork, so we'll use subprocess to detach
        daemonize_windows()
    else:
        # Unix/Linux daemonization
        daemonize_unix()

def daemonize_windows():
    """Windows-specific daemonization using subprocess"""
    try:
        # Get current script path and arguments
        script_path = os.path.abspath(__file__)
        
        # Use a simpler approach for Windows
        if hasattr(subprocess, 'CREATE_NEW_PROCESS_GROUP'):
            creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            creation_flags = 0x00000200  # CREATE_NEW_PROCESS_GROUP value
        
        # Start the service process
        process = subprocess.Popen([
            sys.executable, script_path, '--service'
        ], 
        creationflags=creation_flags,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        close_fds=True
        )
        
        # Give it a moment to start
        time.sleep(2)
        
        # Check if process is still running
        if process.poll() is None:
            print(f"Daemon started successfully with PID: {process.pid}")
        else:
            print("Failed to start daemon process")
            
        # Parent process exits
        sys.exit(0)
        
    except Exception as e:
        print(f"Windows daemonization failed: {e}")
        print("Trying alternative method...")
        
        # Alternative method - just run as service directly
        try:
            script_path = os.path.abspath(__file__)
            os.system(f'start /B python "{script_path}" --service')
            print("Daemon started using alternative method")
            sys.exit(0)
        except Exception as e2:
            print(f"Alternative method also failed: {e2}")
            sys.exit(1)

def daemonize_unix():
    """Unix/Linux daemonization using fork"""
    try:
        # First fork
        pid = os.fork()
        if pid > 0:
            # Parent process exits
            print(f"Daemon starting with PID: {pid}")
            sys.exit(0)
    except OSError as e:
        logger.log(f"Fork #1 failed: {e}")
        sys.exit(1)
    
    # Decouple from parent environment
    os.chdir(os.path.expanduser("~"))  # Use home directory instead of root
    os.setsid()
    os.umask(0o022)  # More permissive umask
    
    try:
        # Second fork
        pid = os.fork()
        if pid > 0:
            # Second parent exits
            sys.exit(0)
    except OSError as e:
        logger.log(f"Fork #2 failed: {e}")
        sys.exit(1)
    
    # Redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    
    # Use /dev/null or NUL depending on platform
    null_device = '/dev/null' if os.name != 'nt' else 'NUL'
    
    try:
        # Redirect stdin, stdout, stderr
        with open(null_device, 'r') as f:
            os.dup2(f.fileno(), sys.stdin.fileno())
        with open(null_device, 'a+') as f:
            os.dup2(f.fileno(), sys.stdout.fileno())
        with open(null_device, 'a+') as f:
            os.dup2(f.fileno(), sys.stderr.fileno())
    except Exception as e:
        # If redirection fails, continue anyway
        logger.log(f"Warning: Could not redirect file descriptors: {e}")
    
    # Set daemon mode flag
    logger.daemon_mode = True
    
    # Create PID file
    create_pid_file()
    
    # Register cleanup on exit
    import atexit
    atexit.register(remove_pid_file)

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.log(f"Received signal {signum}. Stopping scraper...")
    remove_pid_file()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def read_pincodes_from_csv(filename="pincode_usa.csv"):
    """Read pincodes from CSV file"""
    pincodes = []
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'pincode' in row and row['pincode'].strip():
                    pincodes.append(row['pincode'].strip())
        logger.log(f"Loaded {len(pincodes)} pincodes from {filename}")
        return pincodes
    except FileNotFoundError:
        logger.log(f"Error: {filename} not found!")
        return []
    except Exception as e:
        logger.log(f"Error reading pincodes: {e}")
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
        logger.log(f"Loaded {len(cities)} cities from {filename}")
        return cities
    except FileNotFoundError:
        logger.log(f"Error: {filename} not found!")
        return []
    except Exception as e:
        logger.log(f"Error reading cities: {e}")
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
                    "Business_Name", "Contact_Person", "Phone", "Email", "All_Emails_Found", "Website",
                    "All_Websites_Found", "City", "State", "Postal_Code", "Full_Address", "Rating", 
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
            "Business_Name", "Contact_Person", "Phone", "Email", "All_Emails_Found", "Website",
            "All_Websites_Found", "City", "State", "Postal_Code", "Full_Address", "Rating", 
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

def extract_all_emails(text):
    """Extract all emails from text with business priority"""
    email_patterns = [
        r'\b[A-Za-z0-9._%+-]+@gmail\.com\b',  # Gmail addresses
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]*(?:business|company|corp|inc)\.[A-Za-z]{2,}\b',  # Business domains
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'  # General email pattern
    ]
    
    all_emails = []
    for pattern in email_patterns:
        emails = re.findall(pattern, text, re.IGNORECASE)
        all_emails.extend(emails)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_emails = []
    for email in all_emails:
        email_lower = email.lower()
        if email_lower not in seen:
            seen.add(email_lower)
            unique_emails.append(email)
    
    return unique_emails

def extract_emails_from_website(html_content):
    """Extract emails from website HTML content, prioritizing mailto links"""
    emails = []
    
    # Look for mailto: links first (highest priority)
    mailto_patterns = [
        r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'href=["\']mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})["\']'
    ]
    
    for pattern in mailto_patterns:
        mailto_emails = re.findall(pattern, html_content, re.IGNORECASE)
        emails.extend(mailto_emails)
    
    # If no mailto found, look for regular email patterns
    if not emails:
        emails = extract_all_emails(html_content)
    
    return emails

async def scrape_location_query(pincode, country, query, worker_id):
    """Scrape Google Maps for gaming zones"""
    search_query = f"{query} {pincode} {country}"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logger.log(f"[Worker {worker_id}] Starting: {query} in {pincode}, {country}")
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Changed to headless for service
            context = await browser.new_context()
            page = await context.new_page()
            
            page.set_default_timeout(30000)
            await context.route("/*.{png,jpg,jpeg,gif,svg,ico,webp}", lambda route: route.abort())

            search_url = f"https://www.google.com/maps/search/{search_query}/"
            logger.log(f"[Worker {worker_id}] Navigating to: {search_url}")
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            
            await page.wait_for_timeout(5000)
            
            try:
                await page.wait_for_selector('.Nv2PK', timeout=20000)
            except:
                logger.log(f"[Worker {worker_id}] No results found")
                await browser.close()
                return 0

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
            logger.log(f"[Worker {worker_id}] Found {len(business_containers)} businesses")
            
            total_results = 0
            
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
                    
                    # Initialize collections for all found data
                    all_emails_found = []
                    all_websites_found = []
                    
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
                    
                    # Enhanced website extraction
                    website = ""
                    try:
                        # Extract website from the specific element
                        website_text_elem = page.locator('.rogA2c.ITvuef .Io6YTe.fontBodyMedium.kR99db.fdkmkc').first
                        if await website_text_elem.count() > 0:
                            website_text = await website_text_elem.inner_text()
                            # Clean up the website text and ensure it has proper protocol
                            if website_text and not website_text.startswith(('http://', 'https://')):
                                website = f"https://{website_text.strip()}"
                            else:
                                website = website_text.strip()
                            if website:
                                all_websites_found.append(website)
                        
                        # Enhanced email extraction from website
                        if website:
                            try:
                                logger.log(f"[Worker {worker_id}] Visiting website: {website}")
                                website_page = await context.new_page()
                                await website_page.goto(website, timeout=20000)
                                await website_page.wait_for_timeout(3000)
                                
                                # Get website content
                                website_content = await website_page.content()
                                
                                # Look for mailto links and emails
                                website_emails = extract_emails_from_website(website_content)
                                all_emails_found.extend(website_emails)
                                
                                # Contact page extraction logic...
                                contact_selectors = [
                                    'a[href*="contact"]', 'a[href*="Contact"]', 
                                    'a:has-text("Contact")', 'a:has-text("contact")',
                                    'a:has-text("Contact Us")', 'a:has-text("CONTACT")',
                                    'a[href*="about"]', 'a[href*="About"]'
                                ]
                                
                                contact_links = []
                                for selector in contact_selectors:
                                    try:
                                        links = await website_page.locator(selector).all()
                                        contact_links.extend(links[:1])  # Take first from each selector
                                        if len(contact_links) >= 2:  # Stop after finding 2 contact links
                                            break
                                    except:
                                        continue
                                
                                for contact_link in contact_links[:2]:  # Check first 2 contact links
                                    try:
                                        contact_href = await contact_link.get_attribute('href')
                                        if contact_href and not contact_href.startswith('mailto:'):
                                            # Handle relative URLs
                                            if contact_href.startswith('/'):
                                                contact_href = f"{website.rstrip('/')}{contact_href}"
                                            elif not contact_href.startswith('http'):
                                                contact_href = f"{website.rstrip('/')}/{contact_href.lstrip('/')}"
                                            
                                            if contact_href not in all_websites_found:
                                                all_websites_found.append(contact_href)
                                            
                                            contact_page = await context.new_page()
                                            await contact_page.goto(contact_href, timeout=15000)
                                            await contact_page.wait_for_timeout(2000)
                                            
                                            contact_content = await contact_page.content()
                                            contact_emails = extract_emails_from_website(contact_content)
                                            all_emails_found.extend(contact_emails)
                                            
                                            await contact_page.close()
                                            break  # Found contact page, no need to check more
                                    except Exception as e:
                                        logger.log(f"[Worker {worker_id}] Error visiting contact page: {e}")
                                        continue
                                
                                await website_page.close()
                                
                            except Exception as e:
                                logger.log(f"[Worker {worker_id}] Error visiting website {website}: {e}")
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
                    
                    # Extract email from page content and reviews
                    email_address = ""
                    try:
                        # Get page content to search for emails
                        page_content = await page.content()
                        page_emails = extract_all_emails(page_content)
                        all_emails_found.extend(page_emails)
                        
                        # Look for website link first
                        if website and '@' in website:
                            _, email_address = extract_contact_info(website)
                        
                        # If no email found, use first email from page
                        if not email_address and page_emails:
                            email_address = page_emails[0]
                        
                        # Remove duplicates from all_emails_found
                        unique_emails = []
                        seen_emails = set()
                        for email in all_emails_found:
                            if email.lower() not in seen_emails:
                                unique_emails.append(email)
                                seen_emails.add(email.lower())
                        all_emails_found = unique_emails
                        
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
                        # Prepare the result data
                        result_data = [[
                            business_name.strip(),
                            contact_person_name.strip(),
                            contact_number.strip(),
                            email_address.strip(),
                            "; ".join(all_emails_found) if all_emails_found else "",
                            website.strip() if website else "",
                            "; ".join(all_websites_found) if all_websites_found else "",
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
                        ]]
                        
                        # Save immediately after each business
                        append_to_csv(result_data)
                        append_to_json(result_data)
                        total_results += 1
                        
                        logger.log(f"[Worker {worker_id}] Extracted & Saved: {business_name}")
                        if all_emails_found:
                            logger.log(f"[Worker {worker_id}] All emails: {', '.join(all_emails_found)}")
                        if all_websites_found:
                            logger.log(f"[Worker {worker_id} All websites: {', '.join(all_websites_found)}")
                
                except Exception as e:
                    logger.log(f"[Worker {worker_id}] Error extracting business {i}: {e}")
                    continue

            await browser.close()
            
            logger.log(f"[Worker {worker_id}] Completed: {total_results} businesses processed and saved")
            return total_results

    except Exception as e:
        logger.log(f"[Worker {worker_id}] Error scraping: {e}")
        return 0

def run_scraping_task(location, location_type, query, worker_id):
    """Wrapper to run async scraping in process"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(scrape_location_query(location, location_type, query, worker_id))
    finally:
        loop.close()

def run_continuous_scraping():
    """Run scraping continuously as a service"""
    logger.log("Starting Gaming Zones Scraper Service...")
    
    # Create PID file when running as service
    create_pid_file()
    
    # Register cleanup on exit
    import atexit
    atexit.register(remove_pid_file)
    
    # Set daemon mode flag for service mode
    logger.daemon_mode = True
    
    # Set environment for headless browser (Linux/Unix only)
    if platform.system().lower() != "windows":
        # Don't set DISPLAY if we're in a headless environment
        if 'DISPLAY' not in os.environ:
            os.environ['DISPLAY'] = ':0'  # Try default display first
    
    cycle_count = 0
    
    while True:
        try:
            cycle_count += 1
            logger.log(f"Starting scraping cycle #{cycle_count}")
            
            main_scraping_cycle()
            
            # Wait before next cycle (configurable)
            wait_time = int(os.environ.get('SCRAPER_CYCLE_INTERVAL', 3600))  # Default 1 hour
            logger.log(f"Scraping cycle #{cycle_count} completed. Waiting {wait_time} seconds before next cycle...")
            
            # Sleep in smaller chunks to allow signal handling
            sleep_chunks = max(1, wait_time // 60)  # At least 1 chunk
            chunk_size = wait_time // sleep_chunks
            
            for i in range(sleep_chunks):
                time.sleep(chunk_size)
                if i % 10 == 0 and i > 0:  # Log every 10 chunks
                    remaining = wait_time - (i * chunk_size)
                    logger.log(f"Next cycle in {remaining} seconds...")
            
            # Sleep remaining time
            remaining_sleep = wait_time % chunk_size
            if remaining_sleep > 0:
                time.sleep(remaining_sleep)
            
        except KeyboardInterrupt:
            logger.log("Service interrupted by user")
            break
        except Exception as e:
            logger.log(f"Error in scraping cycle: {e}")
            # Wait 5 minutes before retrying
            logger.log("Waiting 5 minutes before retry...")
            time.sleep(300)
        finally:
            # Ensure we clean up resources
            import gc
            gc.collect()

def main_scraping_cycle():
    """Single scraping cycle"""
    logger.log("Starting scraping cycle...")
    
    # Read pincodes
    pincodes = read_pincodes_from_csv()
    
    if not pincodes:
        logger.log("No pincodes found. Skipping cycle...")
        return
    
    # Create tasks for both countries
    tasks = []
    countries = ["USA"]
    
    for pincode in pincodes:
        for country in countries:
            for query in QUERIES:
                tasks.append((pincode, country, query))
    
    logger.log(f"Total tasks: {len(tasks)}")
    
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
                total_results += results
                logger.log(f"Task {i+1}/{len(tasks)} completed")
            except Exception as e:
                logger.log(f"Task {i+1} failed: {e}")

    end_time = time.time()
    duration = end_time - start_time
    
    logger.log(f"Scraping cycle completed! Total results: {total_results}, Time: {duration:.2f} seconds")

def main():
    """Main function to orchestrate scraping"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Gaming Zones Scraper')
    parser.add_argument('--daemon', action='store_true', help='Run as daemon (survives SSH disconnect)')
    parser.add_argument('--service', action='store_true', help='Run as background service')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--stop', action='store_true', help='Stop running daemon')
    parser.add_argument('--status', action='store_true', help='Check daemon status')
    parser.add_argument('--logs', action='store_true', help='Show current logs')
    parser.add_argument('--kill', action='store_true', help='Force kill all instances')
    
    args = parser.parse_args()
    
    if args.kill:
        # Force kill all Python processes running this script
        import psutil
        current_script = os.path.basename(__file__)
        killed = 0
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['name'] == 'python.exe' or proc.info['name'] == 'python':
                    cmdline = proc.info['cmdline']
                    if cmdline and any(current_script in arg for arg in cmdline):
                        if proc.pid != os.getpid():  # Don't kill ourselves
                            proc.kill()
                            killed += 1
                            print(f"Killed process {proc.pid}")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        print(f"Killed {killed} processes")
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
            print("Removed PID file")
        return
    
    if args.stop:
        if stop_daemon():
            print("Daemon stopped successfully")
        else:
            print("Failed to stop daemon or daemon not running")
        return
    
    if args.status:
        if is_running():
            print("Gaming Zones Scraper daemon is running")
            try:
                with open(PID_FILE, 'r') as f:
                    pid = f.read().strip()
                print(f"PID: {pid}")
                
                # Show additional process info
                if psutil.pid_exists(int(pid)):
                    process = psutil.Process(int(pid))
                    print(f"Status: {process.status()}")
                    try:
                        print(f"CPU: {process.cpu_percent()}%")
                        print(f"Memory: {process.memory_info().rss / 1024 / 1024:.1f} MB")
                        print(f"Running since: {datetime.fromtimestamp(process.create_time()).strftime('%Y-%m-%d %H:%M:%S')}")
                    except:
                        pass
            except Exception as e:
                print(f"Error getting process info: {e}")
        else:
            print("Gaming Zones Scraper daemon is not running")
        return
    
    if args.logs:
        try:
            if os.path.exists(LOG_FILE):
                with open(LOG_FILE, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if content:
                        print(content)
                    else:
                        print("Log file is empty")
            else:
                print("No log file found")
        except Exception as e:
            print(f"Error reading log file: {e}")
        return
    
    if args.daemon:
        if is_running():
            print("Daemon is already running. Use --stop to stop it first.")
            return
        
        print("Starting daemon...")
        try:
            daemonize()
        except Exception as e:
            print(f"Failed to start daemon: {e}")
            print("Try using --service instead for testing")
            return
            
        logger.log("Daemon started successfully")
        run_continuous_scraping()
        
    elif args.service:
        if is_running():
            print("Service is already running. Use --stop to stop it first.")
            return
            
        logger.log("Starting as background service...")
        run_continuous_scraping()
        
    else:
        logger.log("Running single scraping cycle...")
        main_scraping_cycle()

if __name__ == "__main__":
    main()