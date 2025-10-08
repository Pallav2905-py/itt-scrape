import pandas as pd
import asyncio
import re
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse
import logging
from typing import Set, List
import threading
import concurrent.futures
from queue import Queue
import time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmailScraper:
    def __init__(self, max_concurrent_browsers=100, max_threads=100):
        self.max_concurrent_browsers = max_concurrent_browsers
        self.max_threads = max_threads
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        self.results_queue = Queue()
        self.processed_count = 0
        
    async def extract_emails_from_text(self, text: str) -> Set[str]:
        """Extract email addresses from text using regex"""
        if not text:
            return set()
            
        emails = set(self.email_pattern.findall(text))
        # Filter out common false positives but be less restrictive
        filtered_emails = {email for email in emails 
                          if not any(exclude in email.lower() 
                                   for exclude in ['example.com', 'test.com', 'localhost', 'domain.com', 'yoursite.com'])
                          and len(email) > 5  # Minimum email length
                          and '.' in email.split('@')[1] if '@' in email}  # Ensure domain has dot
        return filtered_emails
    
    async def scrape_website_emails(self, browser, website_url: str) -> Set[str]:
        """Scrape emails from a single website"""
        emails = set()
        context = None
        
        try:
            # Clean up URL
            if not website_url.startswith(('http://', 'https://')):
                website_url = 'https://' + website_url
                
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                ignore_https_errors=True
            )
            page = await context.new_page()
            
            # Debug for first few websites
            if self.processed_count < 5:
                logger.info(f"DEBUG: Processing {website_url}")
            
            # Set timeout and navigate
            page.set_default_timeout(15000)
            response = await page.goto(website_url, wait_until='domcontentloaded', timeout=15000)
            
            if self.processed_count < 5:
                logger.info(f"DEBUG: Response status: {response.status if response else 'No response'}")
            
            # Get both HTML content and visible text
            content = await page.content()
            text_content = await page.inner_text('body') if await page.query_selector('body') else ""
            
            if self.processed_count < 5:
                logger.info(f"DEBUG: Content lengths - HTML: {len(content) if content else 0}, Text: {len(text_content) if text_content else 0}")
            
            # Extract emails from both HTML and text
            if content:
                html_emails = await self.extract_emails_from_text(content)
                emails.update(html_emails)
                if self.processed_count < 5 and html_emails:
                    logger.info(f"DEBUG: Found emails in HTML: {html_emails}")
            
            if text_content:
                text_emails = await self.extract_emails_from_text(text_content)
                emails.update(text_emails)
                if self.processed_count < 5 and text_emails:
                    logger.info(f"DEBUG: Found emails in text: {text_emails}")
            
            # Look for mailto links
            mailto_links = await page.query_selector_all('a[href^="mailto:"]')
            for link in mailto_links:
                href = await link.get_attribute('href')
                if href:
                    email = href.replace('mailto:', '').split('?')[0]
                    if '@' in email and '.' in email:
                        emails.add(email.lower())
                        if self.processed_count < 5:
                            logger.info(f"DEBUG: Found mailto email: {email}")
            
            # Try to find contact page with multiple selectors
            contact_selectors = [
                'a[href*="contact" i]',
                'a[href*="/contact"]', 
                'a:has-text("Contact")',
                'a:has-text("contact")'
            ]
            
            contact_link = None
            for selector in contact_selectors:
                try:
                    contact_links = await page.query_selector_all(selector)
                    if contact_links:
                        contact_link = contact_links[0]
                        break
                except:
                    continue
            
            if contact_link:
                try:
                    href = await contact_link.get_attribute('href')
                    if href and not href.startswith('mailto:') and not href.startswith('#'):
                        contact_url = urljoin(website_url, href)
                        if self.processed_count < 5:
                            logger.info(f"DEBUG: Checking contact page: {contact_url}")
                        
                        await page.goto(contact_url, wait_until='domcontentloaded', timeout=10000)
                        contact_content = await page.content()
                        contact_text = await page.inner_text('body') if await page.query_selector('body') else ""
                        
                        if contact_content:
                            contact_emails = await self.extract_emails_from_text(contact_content)
                            emails.update(contact_emails)
                        if contact_text:
                            contact_text_emails = await self.extract_emails_from_text(contact_text)
                            emails.update(contact_text_emails)
                        
                        # Check mailto on contact page
                        contact_mailto = await page.query_selector_all('a[href^="mailto:"]')
                        for link in contact_mailto:
                            href = await link.get_attribute('href')
                            if href:
                                email = href.replace('mailto:', '').split('?')[0]
                                if '@' in email and '.' in email:
                                    emails.add(email.lower())
                except Exception as e:
                    if self.processed_count < 5:
                        logger.info(f"DEBUG: Contact page error: {e}")
                        
        except Exception as e:
            if self.processed_count < 10:
                logger.warning(f"Error scraping {website_url}: {e}")
        finally:
            # Ensure proper cleanup
            try:
                if context:
                    await context.close()
            except Exception as e:
                if self.processed_count < 5:
                    logger.warning(f"Error closing context for {website_url}: {e}")
        
        self.processed_count += 1
        
        # Log results
        if emails:
            logger.info(f"SUCCESS: Found {len(emails)} emails from {website_url}: {list(emails)}")
        elif self.processed_count <= 10:
            logger.info(f"DEBUG: No emails found for {website_url}")
            
        return emails

    async def process_websites_in_thread(self, websites_chunk: List[str], thread_id: int):
        """Process a chunk of websites in a single thread"""
        thread_results = {}
        
        async with async_playwright() as p:
            # Launch browsers for this thread
            browsers_per_thread = max(1, self.max_concurrent_browsers // self.max_threads)
            browsers = []
            
            for _ in range(min(browsers_per_thread, len(websites_chunk))):
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-dev-shm-usage']
                )
                browsers.append(browser)
            
            logger.info(f"Thread {thread_id}: Launched {len(browsers)} browsers for {len(websites_chunk)} websites")
            
            # Create semaphore to limit concurrent operations per thread
            semaphore = asyncio.Semaphore(len(browsers))
            
            async def scrape_with_semaphore(website_url: str):
                async with semaphore:
                    # Get an available browser (round-robin)
                    browser_index = hash(website_url) % len(browsers)
                    browser = browsers[browser_index]
                    emails = await self.scrape_website_emails(browser, website_url)
                    return website_url, emails
            
            # Process all websites in this thread
            tasks = [scrape_with_semaphore(website) for website in websites_chunk]
            completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect results
            for result in completed_tasks:
                if isinstance(result, tuple):
                    website, emails = result
                    thread_results[website] = emails
                    if emails:
                        logger.info(f"Thread {thread_id}: Found {len(emails)} emails from {website}")
                else:
                    logger.error(f"Thread {thread_id}: Task failed with exception: {result}")
            
            # Close browsers
            for browser in browsers:
                try:
                    await browser.close()
                except:
                    pass
        
        # Put results in queue
        self.results_queue.put((thread_id, thread_results))
        logger.info(f"Thread {thread_id} completed: processed {len(websites_chunk)} websites")
        return thread_results

    def run_async_in_thread(self, websites_chunk: List[str], thread_id: int):
        """Wrapper to run async function in thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.process_websites_in_thread(websites_chunk, thread_id))
        finally:
            loop.close()

    def process_websites_multithreaded(self, websites: List[str]) -> dict:
        """Process websites using multiple threads"""
        all_results = {}
        
        # Split websites into chunks for each thread
        chunk_size = max(1, len(websites) // self.max_threads)
        website_chunks = [websites[i:i + chunk_size] for i in range(0, len(websites), chunk_size)]
        
        logger.info(f"Processing {len(websites)} websites across {len(website_chunks)} threads")
        
        # Use ThreadPoolExecutor to run async functions in separate threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            # Submit tasks to thread pool
            future_to_thread = {
                executor.submit(self.run_async_in_thread, chunk, i): i 
                for i, chunk in enumerate(website_chunks)
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_thread):
                thread_id = future_to_thread[future]
                try:
                    thread_results = future.result()
                    all_results.update(thread_results)
                    logger.info(f"Thread {thread_id} results collected")
                except Exception as exc:
                    logger.error(f"Thread {thread_id} generated an exception: {exc}")
        
        return all_results

async def main():
    # Read CSV file - adjust path as needed
    excel_file = 'gaming_zones_usa.csv'
    
    try:
        df = pd.read_csv(excel_file)
        logger.info(f"Loaded {len(df)} rows from CSV file")
    except FileNotFoundError:
        logger.error(f"CSV file not found: {excel_file}")
        return
    
    # Get unique websites that are not empty
    websites = df['Website'].dropna().unique().tolist()
    websites = [w for w in websites if w and str(w).strip()]
    
    logger.info(f"Found {len(websites)} unique websites to process")
    logger.info(f"Sample websites: {websites[:5] if websites else 'None'}")
    
    # Test first website manually
    if websites:
        logger.info(f"Testing first website: {websites[0]}")
        test_scraper = EmailScraper(max_concurrent_browsers=1, max_threads=1)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)  # Run with GUI for testing
            test_emails = await test_scraper.scrape_website_emails(browser, websites[0])
            await browser.close()
            logger.info(f"Manual test result: {len(test_emails)} emails - {list(test_emails)}")
    
    # Initialize scraper with reduced settings for debugging
    scraper = EmailScraper(max_concurrent_browsers=2, max_threads=2)
    
    # Process websites in batches using multithreading
    batch_size = 250
    all_results = {}
    
    for i in range(0, len(websites), batch_size):
        batch = websites[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} websites with multithreading")
        
        start_time = time.time()
        batch_results = scraper.process_websites_multithreaded(batch)
        end_time = time.time()
        
        all_results.update(batch_results)
        logger.info(f"Batch {i//batch_size + 1} completed in {end_time - start_time:.2f} seconds")
        
        # Update DataFrame with found emails immediately after each batch
        def update_emails(row):
            website = row['Website']
            if pd.isna(website) or not website:
                return row['All_Emails_Found']
            
            if website in all_results:
                found_emails = all_results[website]
                if found_emails:
                    return ', '.join(sorted(found_emails))
            
            return row['All_Emails_Found'] if pd.notna(row['All_Emails_Found']) else ''
        
        df['All_Emails_Found'] = df.apply(update_emails, axis=1)
        
        # Save updated file immediately after each batch
        output_file = 'updated_data_with_emails.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Batch {i//batch_size + 1} completed and data saved to: {output_file}")
        
        # Small delay between batches
        await asyncio.sleep(1)
    
    # Print summary
    total_emails_found = sum(len(emails) for emails in all_results.values())
    websites_with_emails = sum(1 for emails in all_results.values() if emails)
    
    print(f"\nSummary:")
    print(f"Total websites processed: {len(websites)}")
    print(f"Websites with emails found: {websites_with_emails}")
    print(f"Total emails found: {total_emails_found}")
    logger.info(f"Final data saved to: updated_data_with_emails.csv")

if __name__ == "__main__":
    asyncio.run(main())
