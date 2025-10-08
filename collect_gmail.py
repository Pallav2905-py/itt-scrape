import pandas as pd
import asyncio
import re
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse
import logging
from typing import Set, List

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmailScraper:
    def __init__(self, max_concurrent_browsers=250):
        self.max_concurrent_browsers = max_concurrent_browsers
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
    async def extract_emails_from_text(self, text: str) -> Set[str]:
        """Extract email addresses from text using regex"""
        emails = set(self.email_pattern.findall(text))
        # Filter out common false positives
        filtered_emails = {email for email in emails 
                          if not any(exclude in email.lower() 
                                   for exclude in ['example.com', 'test.com', 'localhost'])}
        return filtered_emails
    
    async def scrape_website_emails(self, browser, website_url: str) -> Set[str]:
        """Scrape emails from a single website"""
        emails = set()
        context = None
        page = None
        
        try:
            # Clean up URL
            if not website_url.startswith(('http://', 'https://')):
                website_url = 'https://' + website_url
                
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()
            
            # Set timeout and navigate
            page.set_default_timeout(30000)
            await page.goto(website_url, wait_until='domcontentloaded', timeout=30000)
            
            # Get page content
            content = await page.content()
            if content:
                emails.update(await self.extract_emails_from_text(content))
            
            # Try to find contact page
            contact_links = await page.query_selector_all('a[href*="contact"], a[href*="Contact"], a[href*="CONTACT"]')
            
            if contact_links:
                for link in contact_links[:2]:  # Check first 2 contact links
                    try:
                        if link is None:
                            continue
                        href = await link.get_attribute('href')
                        if href:
                            contact_url = urljoin(website_url, href)
                            await page.goto(contact_url, wait_until='domcontentloaded', timeout=30000)
                            contact_content = await page.content()
                            if contact_content:
                                emails.update(await self.extract_emails_from_text(contact_content))
                            break  # Found contact page, no need to check more
                    except Exception as e:
                        logger.warning(f"Error accessing contact page for {website_url}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error scraping {website_url}: {e}")
        finally:
            # Ensure proper cleanup
            try:
                if context:
                    await context.close()
            except Exception as e:
                logger.warning(f"Error closing context for {website_url}: {e}")
            
        return emails
    
    async def process_websites_batch(self, websites: List[str]) -> dict:
        """Process a batch of websites with parallel browsers"""
        results = {}
        
        async with async_playwright() as p:
            # Launch browsers
            browsers = []
            for _ in range(min(self.max_concurrent_browsers, len(websites))):
                browser = await p.chromium.launch(headless=False)
                browsers.append(browser)
            
            # Create semaphore to limit concurrent operations
            semaphore = asyncio.Semaphore(len(browsers))
            
            async def scrape_with_semaphore(website_url: str):
                async with semaphore:
                    # Get an available browser (round-robin)
                    browser_index = hash(website_url) % len(browsers)
                    browser = browsers[browser_index]
                    emails = await self.scrape_website_emails(browser, website_url)
                    return website_url, emails
            
            # Process all websites
            tasks = [scrape_with_semaphore(website) for website in websites]
            completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect results
            for result in completed_tasks:
                if isinstance(result, tuple):
                    website, emails = result
                    results[website] = emails
                else:
                    logger.error(f"Task failed with exception: {result}")
            
            # Close browsers
            for browser in browsers:
                await browser.close()
                
        return results

async def main():
    # Read Excel file - adjust path as needed
    excel_file = 'gaming_zones_usa.csv'  # Change this to your actual file path
    
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
    
    # Initialize scraper
    scraper = EmailScraper(max_concurrent_browsers=2)
    
    # Process websites in batches
    batch_size = 250  # Process 50 websites at a time
    all_results = {}
    
    for i in range(0, len(websites), batch_size):
        batch = websites[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} websites")
        
        batch_results = await scraper.process_websites_batch(batch)
        all_results.update(batch_results)
        
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
        await asyncio.sleep(2)
    
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
