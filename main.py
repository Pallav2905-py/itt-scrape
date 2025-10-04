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

# Configuration - Updated for Medical and Pharma Colleges
QUERIES = [
    "medical colleges", 
    "pharmacy colleges", 
    "medical universities",
    "pharmaceutical universities",
    "medical schools"
]
MAX_WORKERS = 5
OUTPUT_CSV = "medical_pharma_colleges.csv"
OUTPUT_JSON = "medical_pharma_colleges.json"

# Target designations to look for
TARGET_DESIGNATIONS = [
    "Director", "Dean", "Head of Department", "HOD", "Vice Chancellor", 
    "President", "Principal", "Registrar", "Chairman", "Head of Anatomy",
    "Department Head", "Academic Director"
]

# Countries to process
COUNTRIES = ["Europe", "USA", "Canada", "India", "Middle East"]

# Thread-safe file writing
file_lock = threading.Lock()

def read_pincodes_from_csv(country):
    """Read pincodes from country-specific CSV file"""
    filename = f"pincode_{country}.csv"
    pincodes = []
    
    if not os.path.exists(filename):
        print(f"Warning: {filename} not found! Skipping {country}")
        return []
    
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
        print(f"Error reading pincodes from {filename}: {e}")
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
                    "Institute_University_Name", "Contact_Person_Name", "Designation", 
                    "Contact_Number", "Email_Address", "All_Emails_Found", "City", "State", "Postal_Code", 
                    "Full_Address", "Website", "All_Websites_Found", "Rating", "Review_Count", "Category", 
                    "Google_Maps_URL", "Country", "Query", "Timestamp"
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
            "Institute_University_Name", "Contact_Person_Name", "Designation", 
            "Contact_Number", "Email_Address", "All_Emails_Found", "City", "State", "Postal_Code", 
            "Full_Address", "Website", "All_Websites_Found", "Rating", "Review_Count", "Category", 
            "Google_Maps_URL", "Country", "Query", "Timestamp"
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
    # International phone patterns
    phone_patterns = [
        r'\+?(\d{1,4})[-.\s]?\(?(\d{1,4})\)?[-.\s]?(\d{1,4})[-.\s]?(\d{1,4})[-.\s]?(\d{0,4})',
        r'\+?1[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})',  # North America
        r'\+?44[-.\s]?\(?([0-9]{3,4})\)?[-.\s]?([0-9]{3,4})[-.\s]?([0-9]{3,4})',  # UK
        r'\+?91[-.\s]?\(?([0-9]{3,4})\)?[-.\s]?([0-9]{3,4})[-.\s]?([0-9]{3,4})',  # India
        r'\(?([0-9]{3,4})\)?[-.\s]?([0-9]{3,4})[-.\s]?([0-9]{3,4})'  # General
    ]
    
    # Enhanced email patterns with priority for institutional emails
    email_patterns = [
        r'\b[A-Za-z0-9._%+-]+@gmail\.com\b',  # Gmail addresses (high priority for institutes)
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]*(?:edu|ac|university|college|medical|pharma)\.[A-Za-z]{2,}\b',  # Educational domains
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.edu\b',  # .edu domains
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.ac\.[A-Za-z]{2,}\b',  # .ac domains
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'  # General email pattern
    ]
    
    phone_number = ""
    for pattern in phone_patterns:
        match = re.search(pattern, text)
        if match:
            groups = match.groups()
            if len(groups) >= 2:
                phone_number = "-".join([g for g in groups if g])
                break
    
    # Find all emails and prioritize institutional ones
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
    
    # Return the first email found (prioritized by pattern order)
    email = unique_emails[0] if unique_emails else ""
    
    return phone_number, email

def extract_all_emails(text):
    """Extract all emails from text with institutional priority"""
    email_patterns = [
        r'\b[A-Za-z0-9._%+-]+@gmail\.com\b',  # Gmail addresses
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]*(?:edu|ac|university|college|medical|pharma)\.[A-Za-z]{2,}\b',  # Educational domains
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.edu\b',  # .edu domains
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.ac\.[A-Za-z]{2,}\b',  # .ac domains
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

def parse_address(address_text):
    """Parse address into components"""
    city = ""
    state = ""
    postal_code = ""
    
    # Extract postal code (various international formats)
    postal_patterns = [
        r'\b(\d{5}(?:-\d{4})?)\b',  # US ZIP
        r'\b([A-Z]\d[A-Z]\s?\d[A-Z]\d)\b',  # Canada postal code
        r'\b([A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\b',  # UK postcode
        r'\b(\d{6})\b',  # India pincode
        r'\b(\d{5})\b',  # General 5-digit
    ]
    
    for pattern in postal_patterns:
        match = re.search(pattern, address_text, re.IGNORECASE)
        if match:
            postal_code = match.group(1).upper()
            break
    
    # Extract state/province/region
    state_patterns = [
        rf'\b([A-Z]{{2,3}})\s+{re.escape(postal_code)}' if postal_code else r'',
        r',\s*([A-Z]{2,20})\s*\d{5,6}',  # State before postal code
        r',\s*([A-Za-z\s]{2,20})(?:,|\s*$)'  # Last component before country
    ]
    
    for pattern in state_patterns:
        if pattern:
            match = re.search(pattern, address_text, re.IGNORECASE)
            if match:
                state = match.group(1).strip().title()
                break
    
    # Extract city
    city_patterns = [
        rf'([^,\n]+),\s*{re.escape(state)}' if state else r'',
        r'^([^,\n]+),',  # First component
        r',\s*([^,\n]+),\s*[A-Z]{2,20}'  # Middle component
    ]
    
    for pattern in city_patterns:
        if pattern:
            match = re.search(pattern, address_text, re.IGNORECASE)
            if match:
                city = match.group(1).strip().title()
                break
    
    return city, state, postal_code

def extract_designation_and_contact(text):
    """Extract designation and contact person from text"""
    designation = ""
    contact_person = ""
    
    # Look for designation patterns
    for target_designation in TARGET_DESIGNATIONS:
        patterns = [
            rf'{target_designation}[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
            rf'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)[,\s]+{target_designation}',
            rf'{target_designation}[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                designation = target_designation
                contact_person = match.group(1).strip()
                break
        
        if designation:
            break
    
    return designation, contact_person

async def scrape_location_query(pincode, country, query, worker_id):
    """Scrape Google Maps for medical and pharma colleges"""
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
            print(f"[Worker {worker_id}] Found {len(business_containers)} institutions")
            
            total_results = 0
            
            for i, business in enumerate(business_containers):
                try:
                    # Click on business to open details
                    await business.click()
                    await page.wait_for_timeout(3000)
                    
                    # Get the current URL after clicking on the business
                    google_maps_url = page.url
                    
                    # Extract institution name
                    institution_name = ""
                    name_selectors = ['.DUwDvf', '.qBF1Pd.fontHeadlineSmall', 'h1']
                    for selector in name_selectors:
                        try:
                            name_elem = page.locator(selector).first
                            if await name_elem.count() > 0:
                                institution_name = await name_elem.inner_text()
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
                    
                    # Initialize collections for all found data
                    all_emails_found = []
                    all_websites_found = []
                    
                    # Extract website and enhanced email search
                    website = ""
                    additional_emails = []
                    try:
                        # Extract website from the specific element you mentioned
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
                        
                        # Fallback: Extract website from Google Maps authority link
                        if not website:
                            website_elem = page.locator('[data-item-id="authority"] a').first
                            if await website_elem.count() > 0:
                                website_href = await website_elem.get_attribute('href')
                                # Skip schema.org and other unwanted URLs
                                if website_href and not any(skip_url in website_href.lower() for skip_url in ['schema.org', 'google.com', 'maps.google']):
                                    website = website_href
                                    all_websites_found.append(website)
                        
                        # Additional fallback: look for website in page content
                        if not website:
                            website_patterns = [
                                r'https?://(?:www\.)?([A-Za-z0-9.-]+\.(?:com|edu|org|net|gov|ac\.uk|ac\.in))',
                                r'www\.([A-Za-z0-9.-]+\.(?:com|edu|org|net|gov|ac\.uk|ac\.in))',
                                r'([A-Za-z0-9.-]+\.(?:edu|ac\.uk|ac\.in|com|org|net|gov))'
                            ]
                            page_content = await page.content()
                            for pattern in website_patterns:
                                matches = re.findall(pattern, page_content, re.IGNORECASE)
                                for match in matches:
                                    potential_url = match if match.startswith('http') else f"https://{match}"
                                    # Skip unwanted domains
                                    if not any(skip_domain in potential_url.lower() for skip_domain in ['schema.org', 'google.com', 'gstatic.com', 'googleapis.com']):
                                        if not website:
                                            website = potential_url
                                        if potential_url not in all_websites_found:
                                            all_websites_found.append(potential_url)
                        
                        # Enhanced email extraction from website
                        if website:
                            try:
                                print(f"[Worker {worker_id}] Visiting website: {website}")
                                website_page = await context.new_page()
                                await website_page.goto(website, timeout=20000)
                                await website_page.wait_for_timeout(3000)
                                
                                # Get website content
                                website_content = await website_page.content()
                                
                                # Look for mailto links and emails
                                website_emails = extract_emails_from_website(website_content)
                                additional_emails.extend(website_emails)
                                
                                # Also look for contact page links
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
                                            additional_emails.extend(contact_emails)
                                            
                                            await contact_page.close()
                                            break  # Found contact page, no need to check more
                                    except Exception as e:
                                        print(f"[Worker {worker_id}] Error visiting contact page: {e}")
                                        continue
                                
                                await website_page.close()
                                
                            except Exception as e:
                                print(f"[Worker {worker_id}] Error visiting website {website}: {e}")
                    except Exception as e:
                        print(f"[Worker {worker_id}] Error extracting website: {e}")
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
                    
                    # Extract email and look for designation/contact person
                    email_address = ""
                    gmail_address = ""
                    designation = ""
                    contact_person_name = ""
                    
                    try:
                        # Get page content to search for emails and designations
                        page_content = await page.content()
                        _, email_address = extract_contact_info(page_content)
                        
                        # Extract all emails to find Gmail addresses
                        page_emails = extract_all_emails(page_content)
                        all_emails_found.extend(page_emails)
                        
                        # Prioritize Gmail addresses
                        gmail_addresses = [email for email in page_emails if 'gmail.com' in email.lower()]
                        if gmail_addresses:
                            gmail_address = gmail_addresses[0]  # Take the first Gmail address
                        
                        # If we have additional emails from website, merge them
                        if additional_emails:
                            all_emails_found.extend(additional_emails)
                            # Update Gmail address if found on website
                            website_gmail = [email for email in additional_emails if 'gmail.com' in email.lower()]
                            if website_gmail and not gmail_address:
                                gmail_address = website_gmail[0]
                            
                            # Prioritize institutional emails from website
                            institutional_emails = [email for email in additional_emails 
                                                  if any(domain in email.lower() for domain in ['.edu', '.ac.', 'university', 'college', 'medical', 'pharma'])]
                            if institutional_emails and not email_address:
                                email_address = institutional_emails[0]
                        
                        # Use Gmail address as primary email if no other institutional email found
                        if gmail_address and not email_address:
                            email_address = gmail_address
                        
                        # Remove duplicates from all_emails_found
                        unique_emails = []
                        seen_emails = set()
                        for email in all_emails_found:
                            if email.lower() not in seen_emails:
                                unique_emails.append(email)
                                seen_emails.add(email.lower())
                        all_emails_found = unique_emails
                        
                        # Look for designation and contact person in reviews, descriptions, etc.
                        desc_elem = page.locator('.PYvSYb').first
                        if await desc_elem.count() > 0:
                            desc_text = await desc_elem.inner_text()
                            designation, contact_person_name = extract_designation_and_contact(desc_text)
                        
                        # Also check owner responses for contact information
                        if not contact_person_name or not designation:
                            owner_responses = page.locator('.CDe7pd .wiI7pd')
                            if await owner_responses.count() > 0:
                                response_text = await owner_responses.first.inner_text()
                                temp_designation, temp_contact = extract_designation_and_contact(response_text)
                                if not designation:
                                    designation = temp_designation
                                if not contact_person_name:
                                    contact_person_name = temp_contact
                        
                        # Look in reviews for staff mentions
                        if not contact_person_name or not designation:
                            review_elems = page.locator('.wiI7pd')
                            count = await review_elems.count()
                            for j in range(min(3, count)):  # Check first 3 reviews
                                review_text = await review_elems.nth(j).inner_text()
                                temp_designation, temp_contact = extract_designation_and_contact(review_text)
                                if not designation and temp_designation:
                                    designation = temp_designation
                                if not contact_person_name and temp_contact:
                                    contact_person_name = temp_contact
                                if designation and contact_person_name:
                                    break
                        
                    except Exception as e:
                        print(f"[Worker {worker_id}] Error extracting additional info: {e}")
                        pass

                    # Only add if we have essential data (institution name)
                    if institution_name and institution_name.strip():
                        # Prepare the result data
                        result_data = [[
                            institution_name.strip(),
                            contact_person_name.strip(),
                            designation.strip(),
                            contact_number.strip(),
                            email_address.strip(),
                            "; ".join(all_emails_found) if all_emails_found else "",
                            city.strip(),
                            state.strip(),
                            postal_code.strip(),
                            full_address.strip(),
                            website.strip() if website else "",
                            "; ".join(all_websites_found) if all_websites_found else "",
                            rating.strip(),
                            review_count.strip(),
                            category.strip(),
                            google_maps_url.strip(),
                            country,
                            query,
                            timestamp
                        ]]
                        
                        # Save immediately after each institute
                        append_to_csv(result_data)
                        append_to_json(result_data)
                        total_results += 1
                        
                        print(f"[Worker {worker_id}] Extracted & Saved: {institution_name}")
                        if gmail_address:
                            print(f"[Worker {worker_id}] Found Gmail: {gmail_address}")
                        if email_address and email_address != gmail_address:
                            print(f"[Worker {worker_id}] Found Email: {email_address}")
                        if all_emails_found:
                            print(f"[Worker {worker_id}] All emails: {', '.join(all_emails_found)}")
                        if website:
                            print(f"[Worker {worker_id}] Found Website: {website}")
                        if all_websites_found:
                            print(f"[Worker {worker_id}] All websites: {', '.join(all_websites_found)}")

                except Exception as e:
                    print(f"[Worker {worker_id}] Error extracting institution {i}: {e}")
                    continue

            await browser.close()
            
            print(f"[Worker {worker_id}] Completed: {total_results} institutions processed and saved")
            return total_results

    except Exception as e:
        print(f"[Worker {worker_id}] Error scraping: {e}")
        return 0

def run_scraping_task(location, country, query, worker_id):
    """Wrapper to run async scraping in process"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(scrape_location_query(location, country, query, worker_id))
    finally:
        loop.close()

def main():
    """Main function to orchestrate scraping"""
    print("Starting Google Maps scraper for Medical and Pharma Colleges...")
    
    # Create tasks for all countries and queries
    tasks = []
    total_pincodes = 0
    
    for country in COUNTRIES:
        pincodes = read_pincodes_from_csv(country)
        if not pincodes:
            continue
            
        total_pincodes += len(pincodes)
        
        for pincode in pincodes:
            for query in QUERIES:
                tasks.append((pincode, country, query))
    
    if not tasks:
        print("No pincodes found for any country. Exiting...")
        return
    
    print(f"Total pincodes loaded: {total_pincodes}")
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