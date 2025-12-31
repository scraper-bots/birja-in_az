import asyncio
import aiohttp
import csv
import logging
from typing import List, Dict, Optional
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
import json
import re
from aiohttp import ClientTimeout, TCPConnector
from asyncio import Semaphore
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BirjaScraper:
    def __init__(self, base_url: str = "https://birja-in.az", max_concurrent: int = 5):
        self.base_url = base_url
        self.max_concurrent = max_concurrent
        self.semaphore = Semaphore(max_concurrent)
        self.session: Optional[aiohttp.ClientSession] = None
        self.scraped_ids = set()
        self.failed_urls = []
        self.output_file = Path('scraped_data.csv')
        self.progress_file = Path('scraper_progress.json')
        self.failed_file = Path('failed_urls.json')

        # CSV headers
        self.csv_headers = [
            'elan_id', 'title', 'url', 'price', 'currency', 'location', 'region',
            'metro', 'category', 'subcategory', 'elan_type', 'property_type',
            'rental_period', 'room_count', 'floor', 'total_floors', 'area_sqm',
            'repair_status', 'land_area_sot', 'house_area_sqm', 'advertiser_type',
            'description', 'contact_name', 'phone', 'date_posted', 'view_count',
            'images', 'all_properties', 'scraped_at'
        ]

        # Load progress
        self.load_progress()

    def load_progress(self):
        """Load previously scraped IDs to avoid duplicates"""
        try:
            if self.progress_file.exists():
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                    self.scraped_ids = set(progress.get('scraped_ids', []))
                logger.info(f"Loaded {len(self.scraped_ids)} previously scraped IDs")
        except Exception as e:
            logger.error(f"Error loading progress: {e}")

    def save_progress(self):
        """Save progress to resume if interrupted"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'scraped_ids': list(self.scraped_ids),
                    'last_update': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress: {e}")

    def save_failed_urls(self):
        """Save failed URLs for retry"""
        try:
            with open(self.failed_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'failed_urls': self.failed_urls,
                    'count': len(self.failed_urls)
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving failed URLs: {e}")

    async def create_session(self):
        """Create aiohttp session with proper configuration"""
        timeout = ClientTimeout(total=60, connect=10, sock_read=30)
        connector = TCPConnector(limit=20, limit_per_host=5, ttl_dns_cache=300)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'az,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=headers
        )

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()

    async def fetch_with_retry(self, url: str, max_retries: int = 5) -> Optional[str]:
        """Fetch URL with exponential backoff retry"""
        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 404:
                            logger.warning(f"404 Not Found: {url}")
                            return None
                        else:
                            logger.warning(f"Status {response.status} for {url}")
            except asyncio.TimeoutError:
                wait_time = 2 ** attempt
                logger.warning(f"Timeout on {url}, retry {attempt + 1}/{max_retries} after {wait_time}s")
                await asyncio.sleep(wait_time)
            except aiohttp.ClientError as e:
                wait_time = 2 ** attempt
                logger.warning(f"Client error on {url}: {e}, retry {attempt + 1}/{max_retries} after {wait_time}s")
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {e}")
                await asyncio.sleep(2 ** attempt)

        # All retries failed
        self.failed_urls.append({'url': url, 'time': datetime.now().isoformat()})
        return None

    def extract_listing_info(self, listing_html: BeautifulSoup) -> Optional[Dict]:
        """Extract basic info from listing card"""
        try:
            data = {}

            # Extract elan ID
            elan_num = listing_html.find('span', string=re.compile(r'Elan №'))
            if elan_num:
                data['elan_id'] = re.search(r'\d+', elan_num.text).group()
            else:
                return None

            # Skip if already scraped
            if data['elan_id'] in self.scraped_ids:
                logger.debug(f"Skipping already scraped ID: {data['elan_id']}")
                return None

            # Extract title and URL
            title_elem = listing_html.find('h2')
            if title_elem:
                link = title_elem.find('a')
                if link:
                    data['title'] = link.find('span', itemprop='name').text.strip() if link.find('span', itemprop='name') else link.text.strip()
                    data['url'] = self.base_url + link.get('href', '')

            # Extract price
            price_elem = listing_html.find('span', class_='value_cost_adv')
            if price_elem:
                data['price'] = price_elem.text.strip().replace(' ', '')

            currency_elem = listing_html.find('span', class_='value_currency')
            if currency_elem:
                data['currency'] = currency_elem.text.strip()

            # Extract location
            location_elem = listing_html.find('div', class_='block_name_region_adv')
            if location_elem:
                data['location'] = location_elem.text.strip()

            # Extract category
            category_elem = listing_html.find('div', class_='block_name_category_adv')
            if category_elem:
                category_span = category_elem.find('span', style=re.compile(r'color.*#ea6f24'))
                if category_span:
                    data['category'] = category_span.text.strip()

            # Extract short description
            desc_elem = listing_html.find('div', class_='short-text-ads')
            if desc_elem:
                data['short_description'] = desc_elem.text.strip()

            # Extract date
            date_elem = listing_html.find('span', itemprop='datePosted')
            if date_elem:
                data['date_posted'] = date_elem.text.strip()

            return data
        except Exception as e:
            logger.error(f"Error extracting listing info: {e}")
            return None

    def extract_detail_info(self, html: str, basic_info: Dict) -> Dict:
        """Extract detailed information from detail page"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            data = basic_info.copy()

            # Extract full description
            desc_elem = soup.find('td', itemprop='description')
            if desc_elem:
                data['description'] = desc_elem.text.strip()

            # Extract all properties from the table
            properties = {}
            property_rows = soup.find_all('tr')
            for row in property_rows:
                cells = row.find_all('td')
                if len(cells) == 2:
                    key = cells[0].text.strip()
                    value = cells[1].text.strip()
                    if key and value:
                        properties[key] = value

            # Map specific properties
            data['region'] = properties.get('Şəhər/ərazi', '')
            data['elan_type'] = properties.get('Elan növü', '')
            data['property_type'] = properties.get('Əmlak növü', '')
            data['rental_period'] = properties.get('Kirayə müddəti', '')
            data['room_count'] = properties.get('Otaq sayı', '')
            data['floor'] = properties.get('Mərtəbə', '')
            data['total_floors'] = properties.get('Mərtəbəli bina', '')
            data['area_sqm'] = properties.get('Sahəsi (m²)', '')
            data['repair_status'] = properties.get('Təmiri', '')
            data['land_area_sot'] = properties.get('Ümumi-sahə (sot)', '')
            data['house_area_sqm'] = properties.get('Evin-sahəsi (m²)', '')

            # Extract advertiser type
            advertiser_elem = soup.find('span', string=re.compile(r'ƏMLAK|Vasitəçi'))
            if advertiser_elem:
                data['advertiser_type'] = advertiser_elem.text.strip()

            # Extract contact info
            contact_name = soup.find('td', class_='name_adder')
            if contact_name:
                data['contact_name'] = contact_name.text.strip().split('\n')[0].strip()

            # Extract phone
            phone_row = soup.find('td', class_='td_name_param_phone')
            if phone_row and phone_row.find_next('td'):
                data['phone'] = phone_row.find_next('td').text.strip()

            # Extract view count
            view_elem = soup.find('td', class_='history', string=re.compile(r'Baxış sayı'))
            if view_elem:
                match = re.search(r'\d+', view_elem.text)
                if match:
                    data['view_count'] = match.group()

            # Extract images
            images = []
            img_links = soup.find_all('a', class_='fancybox-buttons')
            for img_link in img_links:
                img_url = img_link.get('href')
                if img_url:
                    images.append(self.base_url + img_url if not img_url.startswith('http') else img_url)
            data['images'] = '|'.join(images)

            # Store all properties as JSON
            data['all_properties'] = json.dumps(properties, ensure_ascii=False)

            # Add scrape timestamp
            data['scraped_at'] = datetime.now().isoformat()

            return data
        except Exception as e:
            logger.error(f"Error extracting detail info: {e}")
            return basic_info

    def write_to_csv(self, data: Dict):
        """Append data to CSV file (thread-safe)"""
        try:
            file_exists = self.output_file.exists()

            with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_headers, extrasaction='ignore')

                if not file_exists:
                    writer.writeheader()

                writer.writerow(data)

            logger.info(f"Saved listing {data.get('elan_id')} to CSV")
        except Exception as e:
            logger.error(f"Error writing to CSV: {e}")
            # Save to backup file
            backup_file = Path(f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(backup_file, 'a', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
                f.write('\n')

    async def scrape_listing(self, listing_html: BeautifulSoup):
        """Scrape a single listing"""
        try:
            # Extract basic info
            basic_info = self.extract_listing_info(listing_html)
            if not basic_info or not basic_info.get('url'):
                return

            elan_id = basic_info.get('elan_id')
            logger.info(f"Scraping listing {elan_id}: {basic_info.get('title', 'Unknown')}")

            # Fetch detail page
            detail_html = await self.fetch_with_retry(basic_info['url'])
            if not detail_html:
                logger.warning(f"Failed to fetch detail page for {elan_id}")
                return

            # Extract detailed info
            full_data = self.extract_detail_info(detail_html, basic_info)

            # Save to CSV
            self.write_to_csv(full_data)

            # Mark as scraped
            self.scraped_ids.add(elan_id)

            # Save progress periodically
            if len(self.scraped_ids) % 10 == 0:
                self.save_progress()

        except Exception as e:
            logger.error(f"Error scraping listing: {e}")

    async def scrape_page(self, page_num: int):
        """Scrape a single page of listings"""
        try:
            url = f"{self.base_url}/elanlar/ev-alqi-satqisi/num{page_num}.html"
            logger.info(f"Scraping page {page_num}: {url}")

            html = await self.fetch_with_retry(url)
            if not html:
                logger.warning(f"Failed to fetch page {page_num}")
                return

            soup = BeautifulSoup(html, 'html.parser')

            # Find all listing blocks
            listings = soup.find_all('div', class_='block_one_synopsis_advert')
            logger.info(f"Found {len(listings)} listings on page {page_num}")

            # Scrape each listing
            tasks = [self.scrape_listing(listing) for listing in listings]
            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}")

    async def scrape_pages(self, start_page: int = 52, end_page: int = 52):
        """Scrape multiple pages"""
        try:
            await self.create_session()

            # Scrape pages sequentially to avoid overwhelming the server
            for page_num in range(start_page, end_page + 1):
                await self.scrape_page(page_num)
                # Small delay between pages
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in scrape_pages: {e}")
        finally:
            # Save final progress
            self.save_progress()
            self.save_failed_urls()
            await self.close_session()

            logger.info(f"Scraping completed. Total scraped: {len(self.scraped_ids)}")
            logger.info(f"Failed URLs: {len(self.failed_urls)}")

    async def retry_failed(self):
        """Retry previously failed URLs"""
        if not self.failed_file.exists():
            logger.info("No failed URLs to retry")
            return

        try:
            with open(self.failed_file, 'r', encoding='utf-8') as f:
                failed_data = json.load(f)
                failed_urls = failed_data.get('failed_urls', [])

            if not failed_urls:
                logger.info("No failed URLs to retry")
                return

            logger.info(f"Retrying {len(failed_urls)} failed URLs")

            await self.create_session()

            # Clear failed list for new attempt
            self.failed_urls = []

            # Retry each URL
            for failed_item in failed_urls:
                url = failed_item['url']
                html = await self.fetch_with_retry(url)
                # Process based on URL type
                # ... (implementation depends on whether it's a listing or detail page)

            self.save_failed_urls()
            await self.close_session()

        except Exception as e:
            logger.error(f"Error retrying failed URLs: {e}")


async def main():
    """Main entry point"""
    scraper = BirjaScraper(max_concurrent=5)

    try:
        # Scrape page 52 (can adjust range as needed)
        await scraper.scrape_pages(start_page=1, end_page=55)

        # Optionally retry failed URLs
        if scraper.failed_urls:
            logger.info("Retrying failed URLs...")
            await scraper.retry_failed()

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        scraper.save_progress()
        scraper.save_failed_urls()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        scraper.save_progress()
        scraper.save_failed_urls()
    finally:
        if scraper.session:
            await scraper.close_session()


if __name__ == "__main__":
    asyncio.run(main())
