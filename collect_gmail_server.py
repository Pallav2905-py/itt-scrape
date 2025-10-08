import pandas as pd
import asyncio
import re
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse
import logging
from typing import Set, List
import os
import signal
import sys

# Setup logging for server
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('collect_gmail.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ServerEmailScraper:
    def __init__(self, max_concurrent_browsers=250):
        self.max_concurrent_browsers = max_concurrent_browsers
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        self.processed_count = 0
        self.total_emails_found = 0
        
    async def extract_emails_from_text(self, text: str) -> Set[str]:
        """Extract email addresses from text using regex"""
        emails = set(self.email_pattern.findall(text))
        # Filter out common false positives
        filtered_emails = {email for email in emails 
                          if not any(exclude in email.lower() 
                                   for exclude in ['example.com', 'test.com', 'localhost', 'domain.com'])}
        return filtered_emails
    
    async def scrape_website_emails(self, browser, website_url: str) -> Set[str]:
        """Scrape emails from a single website"""
        emails = set()
        context = None
        
        try:
            if not website_url.startswith(('http://', 'https://')):
                website_url = 'https://' + website_url
                
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                viewport={'width': 1280, 'height': 720}
            )
            page = await context.new_page()
            
            # Faster timeout for server processing
            await page.goto(website_url, wait_until='domcontentloaded', timeout=15000)
            
            # Get page content
            content = await page.content()
            if content:
                emails.update(await self.extract_emails_from_text(content))
            
            # Quick contact page check
            try:
                contact_links = await page.query_selector_all('a[href*="contact" i]')
                if contact_links and len(contact_links) > 0:
                    link = contact_links[0]
                    if link:
                        href = await link.get_attribute('href')
                        if href and not href.startswith('mailto:'):
                            contact_url = urljoin(website_url, href)
                            await page.goto(contact_url, wait_until='domcontentloaded', timeout=10000)
                            contact_content = await page.content()
                            if contact_content:
                                emails.update(await self.extract_emails_from_text(contact_content))
            except:
                pass  # Skip contact page if any error
                        
        except Exception as e:
            logger.debug(f"Error scraping {website_url}: {e}")
        finally:
            if context:
                try:
                    await context.close()
                except:
                    pass
            
        self.processed_count += 1
        if emails:
            self.total_emails_found += len(emails)
            logger.info(f"Found {len(emails)} emails from {website_url}")
        
        if self.processed_count % 50 == 0:
            logger.info(f"Processed {self.processed_count} websites, found {self.total_emails_found} total emails")
            
        return emails

    async def process_all_websites(self, websites: List[str], df: pd.DataFrame) -> None:
        """Process all websites with maximum parallelism"""
        async with async_playwright() as p:
            # Launch browsers with optimized settings for server
            browsers = []
            browser_count = min(self.max_concurrent_browsers, len(websites))
            
            logger.info(f"Launching {browser_count} browser instances...")
            
            for i in range(browser_count):
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor',
                        '--memory-pressure-off'
                    ]
                )
                browsers.append(browser)
                if (i + 1) % 10 == 0:
                    logger.info(f"Launched {i + 1} browsers...")
            
            # Create website-browser mapping
            website_results = {}
            
            async def scrape_single_website(website_url: str, browser_index: int):
                browser = browsers[browser_index]
                emails = await self.scrape_website_emails(browser, website_url)
                website_results[website_url] = emails
                
                # Update DataFrame immediately for this website
                mask = df['Website'] == website_url
                if emails and mask.any():
                    email_str = ', '.join(sorted(emails))
                    df.loc[mask, 'All_Emails_Found'] = email_str
                    
                    # Save progress every 25 websites
                    if len(website_results) % 25 == 0:
                        df.to_csv('updated_data_with_emails.csv', index=False)
                        logger.info(f"Progress saved: {len(website_results)}/{len(websites)} websites processed")
            
            # Create tasks with browser assignment
            tasks = []
            for i, website in enumerate(websites):
                browser_index = i % len(browsers)
                task = scrape_single_website(website, browser_index)
                tasks.append(task)
            
            logger.info(f"Starting to process {len(websites)} websites with {len(browsers)} browsers...")
            
            # Process all websites concurrently
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Final save
            df.to_csv('updated_data_with_emails.csv', index=False)
            logger.info("Final data saved")
            
            # Close all browsers
            logger.info("Closing browsers...")
            for browser in browsers:
                try:
                    await browser.close()
                except:
                    pass

def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

async def main():
    # Setup signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    excel_file = 'gaming_zones_usa.csv'
    
    try:
        df = pd.read_csv(excel_file)
        logger.info(f"Loaded {len(df)} rows from CSV file")
    except FileNotFoundError:
        logger.error(f"CSV file not found: {excel_file}")
        return
    
    # Get unique websites
    websites = df['Website'].dropna().unique().tolist()
    websites = [w for w in websites if w and str(w).strip()]
    
    logger.info(f"Found {len(websites)} unique websites to process")
    
    # Initialize scraper with 250 concurrent browsers
    scraper = ServerEmailScraper(max_concurrent_browsers=250)
    
    try:
        await scraper.process_all_websites(websites, df)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    
    logger.info(f"Processing complete. Total emails found: {scraper.total_emails_found}")

if __name__ == "__main__":
    asyncio.run(main())
