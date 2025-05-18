"""
Resource-efficient Facebook Marketplace car listing scraper
Uses minimal-footprint Selenium configuration with memory optimizations
"""

import time
import random
import re
import json
import logging
import psutil
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, Generator
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException,
    StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
import requests
from bs4 import BeautifulSoup

# Local imports
from src.utils.config import Config
from src.database.db_manager import DatabaseManager


class ResourceEfficientScraper:
    """A memory-efficient scraper for Facebook Marketplace car listings"""
    
    def __init__(self, config: Config, db_manager: DatabaseManager):
        """Initialize the scraper with configuration"""
        self.config = config
        self.db_manager = db_manager
        self.driver = None
        self.headless = config.get("scraper", "headless", True)
        self.disable_images = config.get("scraper", "disable_images", True)
        self.batch_size = config.get("scraper", "batch_size", 50)
        self.scroll_pause_time = config.get("scraper", "scroll_pause_time", 1.5)
        self.max_listings = config.get("scraper", "max_listings", 500)
        self.use_simplified_parser = config.get("scraper", "use_simplified_parser", False)
        self.retry_attempts = config.get("scraper", "retry_attempts", 3)
        self.backoff_factor = config.get("scraper", "backoff_factor", 2.0)
        self.memory_limit_mb = config.get("system", "memory_limit_mb", 512)
        self.cpu_usage_limit = config.get("system", "cpu_usage_limit", 50)
        
        # Configure logger
        self.logger = logging.getLogger(__name__)
        
        # Session ID for the current scraping session
        self.session_id = None
        
        # State for pause/resume functionality
        self.last_processed_url = None
        self.processed_urls = set()
        
        # Stats for reporting
        self.stats = {
            "listings_found": 0,
            "new_listings": 0,
            "updated_listings": 0,
            "errors": 0,
            "memory_usage_mb": 0,
            "cpu_usage": 0
        }
    
    def _setup_driver(self):
        """
        Configure and initialize the Selenium WebDriver with memory optimizations
        
        Returns:
            webdriver: Configured WebDriver instance
        """
        options = Options()
        
        if self.headless:
            options.add_argument("--headless=new")
        
        # Memory optimization options
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-browser-side-navigation")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-features=IsolateOrigins,site-per-process")
        options.add_argument("--disable-site-isolation-trials")
        
        # Set low-memory settings
        options.add_argument("--js-flags=--lite-mode")
        options.add_argument("--renderer-process-limit=1")
        options.add_argument("--single-process")
        options.add_argument("--disk-cache-size=1")
        
        # Set window size (smaller = less memory)
        options.add_argument("--window-size=1280,720")
        
        # Disable image loading if configured
        if self.disable_images:
            options.add_argument("--blink-settings=imagesEnabled=false")
            prefs = {"profile.managed_default_content_settings.images": 2}
            options.add_experimental_option("prefs", prefs)
        
        # User agent to avoid detection
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36")
        
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            
            # Set page load timeout
            driver.set_page_load_timeout(30)
            
            return driver
        except Exception as e:
            self.logger.error(f"Error setting up WebDriver: {e}")
            
            # Fallback to system ChromeDriver if available
            try:
                driver = webdriver.Chrome(options=options)
                driver.set_page_load_timeout(30)
                return driver
            except Exception as e2:
                self.logger.error(f"Fallback WebDriver setup failed: {e2}")
                raise
    
    def _check_resource_usage(self) -> Tuple[float, float]:
        """
        Check current memory and CPU usage
        
        Returns:
            tuple: (memory_usage_mb, cpu_usage_percent)
        """
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_usage_mb = memory_info.rss / (1024 * 1024)
        
        cpu_usage = psutil.cpu_percent(interval=0.1)
        
        # Update stats
        self.stats["memory_usage_mb"] = memory_usage_mb
        self.stats["cpu_usage"] = cpu_usage
        
        return memory_usage_mb, cpu_usage
    
    def _should_throttle(self) -> bool:
        """
        Check if scraping should be throttled based on resource usage
        
        Returns:
            bool: True if scraping should be throttled
        """
        memory_usage_mb, cpu_usage = self._check_resource_usage()
        
        # Check if we're exceeding limits
        if memory_usage_mb > self.memory_limit_mb:
            self.logger.warning(f"Memory usage ({memory_usage_mb:.1f} MB) exceeding limit ({self.memory_limit_mb} MB). Throttling.")
            return True
            
        if cpu_usage > self.cpu_usage_limit:
            self.logger.warning(f"CPU usage ({cpu_usage:.1f}%) exceeding limit ({self.cpu_usage_limit}%). Throttling.")
            return True
            
        return False
    
    def _throttle(self):
        """Implement throttling when resource usage is high"""
        memory_usage_mb, cpu_usage = self._check_resource_usage()
        
        # Adaptive sleep time based on how far over the limit we are
        if memory_usage_mb > self.memory_limit_mb:
            ratio = memory_usage_mb / self.memory_limit_mb
            sleep_time = min(30, max(1, ratio * 5))
            self.logger.info(f"Memory throttling: sleeping for {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
            
            # Force garbage collection if memory is very high
            if memory_usage_mb > self.memory_limit_mb * 1.5:
                import gc
                gc.collect()
                
        if cpu_usage > self.cpu_usage_limit:
            ratio = cpu_usage / self.cpu_usage_limit
            sleep_time = min(15, max(0.5, ratio * 3))
            self.logger.info(f"CPU throttling: sleeping for {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
    
    def _scroll_to_load_listings(self, max_listings: int) -> List[str]:
        """
        Incrementally scroll to load listings
        
        Args:
            max_listings (int): Maximum number of listings to load
            
        Returns:
            list: List of listing URLs
        """
        listing_urls = []
        scroll_attempts = 0
        max_scroll_attempts = max(20, max_listings // 10)
        last_count = 0
        
        self.logger.info(f"Scrolling to load up to {max_listings} listings...")
        
        while len(listing_urls) < max_listings and scroll_attempts < max_scroll_attempts:
            # Throttle if needed
            if self._should_throttle():
                self._throttle()
            
            # Execute scroll
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(self.scroll_pause_time)
            
            # Find all listing elements with links
            try:
                listing_elements = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/marketplace/item/']")
                current_urls = [elem.get_attribute("href") for elem in listing_elements if elem.get_attribute("href")]
                
                # Filter out duplicates and add to our list
                new_urls = [url for url in current_urls if url not in listing_urls]
                listing_urls.extend(new_urls)
                
                # Deduplicate
                listing_urls = list(dict.fromkeys(listing_urls))
                
                # Print progress
                if len(listing_urls) > last_count:
                    self.logger.info(f"Found {len(listing_urls)} listings...")
                    last_count = len(listing_urls)
                
                # If we haven't found new listings in a while, break
                if len(listing_urls) == last_count:
                    scroll_attempts += 1
                else:
                    scroll_attempts = 0
                    
            except Exception as e:
                self.logger.error(f"Error during scrolling: {e}")
                scroll_attempts += 1
        
        return listing_urls[:max_listings]
    
    def _parse_listing_page(self, url: str) -> Optional[Dict]:
        """
        Parse a single listing page
        
        Args:
            url (str): Listing URL
            
        Returns:
            dict: Parsed listing data or None if failed
        """
        # Retry logic with exponential backoff
        for attempt in range(self.retry_attempts):
            try:
                # Navigate to the listing
                self.driver.get(url)
                
                # Wait for the main listing container to load
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='main']"))
                    )
                except TimeoutException:
                    self.logger.warning(f"Timeout waiting for listing page: {url}")
                    
                    # Check if this is a login page
                    if "login" in self.driver.current_url or "Login" in self.driver.page_source:
                        self.logger.error("Facebook login required - cannot proceed")
                        return None
                        
                # Use a simpler parsing method if configured or if memory usage is high
                memory_usage_mb, _ = self._check_resource_usage()
                use_simple_parser = self.use_simplified_parser or memory_usage_mb > (self.memory_limit_mb * 0.8)
                
                if use_simple_parser:
                    return self._simple_parse_listing()
                else:
                    return self._full_parse_listing(url)
                    
            except WebDriverException as e:
                self.logger.error(f"WebDriver error on attempt {attempt+1}/{self.retry_attempts}: {e}")
                
                # Backoff before retry
                if attempt < self.retry_attempts - 1:
                    sleep_time = self.backoff_factor ** attempt
                    time.sleep(sleep_time)
                    
                    # Restart WebDriver if it crashed
                    if "crashed" in str(e).lower():
                        self.logger.warning("WebDriver crashed, restarting...")
                        self._restart_driver()
            
            except Exception as e:
                self.logger.error(f"Error parsing listing on attempt {attempt+1}/{self.retry_attempts}: {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.backoff_factor ** attempt)
        
        # All attempts failed
        self.stats["errors"] += 1
        return None
    
    def _restart_driver(self):
        """Safely restart the WebDriver if it crashed"""
        try:
            if self.driver:
                self.driver.quit()
        except:
            pass
            
        try:
            self.driver = self._setup_driver()
        except Exception as e:
            self.logger.error(f"Failed to restart WebDriver: {e}")
    
    def _simple_parse_listing(self) -> Dict:
        """
        Simple and lightweight parser for car listings
        
        Returns:
            dict: Basic listing data
        """
        try:
            # Listing ID from URL
            listing_id = self._extract_listing_id(self.driver.current_url)
            
            # Basic listing info
            title_element = self.driver.find_element(By.CSS_SELECTOR, "h1[dir='auto']")
            title = title_element.text.strip() if title_element else "Unknown Vehicle"
            
            # Price element
            price_element = self.driver.find_element(By.CSS_SELECTOR, "span[dir='auto'] > span:first-child")
            price_text = price_element.text.strip() if price_element else "$0"
            price = int(re.sub(r'[^\d]', '', price_text) or 0)
            
            # Get basic details
            details_elements = self.driver.find_elements(By.CSS_SELECTOR, "span[dir='auto']")
            details_text = [elem.text.strip() for elem in details_elements if elem.text.strip()]
            
            # Try to extract year, make, model
            car_info = {}
            if title:
                # Try to extract year from title
                year_match = re.search(r'\b(19|20)\d{2}\b', title)
                if year_match:
                    car_info['year'] = int(year_match.group())
                
                # Try to extract make and model
                # This is a simplified heuristic
                makes = ['honda', 'toyota', 'ford', 'chevrolet', 'chevy', 'nissan', 'bmw', 
                        'mercedes', 'audi', 'kia', 'hyundai', 'subaru', 'mazda', 'lexus', 
                        'acura', 'jeep', 'dodge', 'ram', 'chrysler', 'volvo', 'volkswagen', 
                        'vw', 'tesla', 'cadillac', 'buick', 'gmc']
                
                title_lower = title.lower()
                for make in makes:
                    if make in title_lower:
                        car_info['make'] = make.title()
                        break
            
            # Try to extract mileage
            mileage = 0
            for text in details_text:
                if 'mile' in text.lower():
                    mileage_match = re.search(r'([\d,]+)', text)
                    if mileage_match:
                        mileage = int(re.sub(r'[^\d]', '', mileage_match.group()))
                        break
            
            # Save HTML content for later parsing
            html_content = self.driver.page_source
            
            # Create listing data
            listing_data = {
                'id': listing_id,
                'title': title,
                'price': price,
                'mileage': mileage,
                'url': self.driver.current_url,
                'listing_date': datetime.now().isoformat(),
                'raw_html': html_content,
                **car_info
            }
            
            return listing_data
            
        except Exception as e:
            self.logger.error(f"Error in simple listing parser: {e}")
            return {
                'id': self._extract_listing_id(self.driver.current_url),
                'url': self.driver.current_url,
                'listing_date': datetime.now().isoformat(),
                'raw_html': self.driver.page_source
            }
    
    def _full_parse_listing(self, url: str) -> Dict:
        """
        Comprehensive parser for car listings
        
        Args:
            url (str): Listing URL
        
        Returns:
            dict: Detailed listing data
        """
        try:
            # Listing ID from URL
            listing_id = self._extract_listing_id(url)
            
            # Title
            title_element = self.driver.find_element(By.CSS_SELECTOR, "h1[dir='auto']")
            title = title_element.text.strip() if title_element else "Unknown Vehicle"
            
            # Price
            price_element = self.driver.find_element(By.CSS_SELECTOR, "span[dir='auto'] > span:first-child")
            price_text = price_element.text.strip() if price_element else "$0"
            price = int(re.sub(r'[^\d]', '', price_text) or 0)
            
            # Location
            location = ""
            location_elements = self.driver.find_elements(By.CSS_SELECTOR, "span[dir='auto'] a[href*='/marketplace/']")
            if location_elements:
                location = location_elements[0].text.strip()
            
            # Find all detail elements
            details = {}
            detail_labels = self.driver.find_elements(By.CSS_SELECTOR, "div[aria-label='Listing details'] span[dir='auto']")
            
            for i in range(0, len(detail_labels)-1, 2):
                try:
                    key = detail_labels[i].text.strip().lower()
                    value = detail_labels[i+1].text.strip()
                    
                    if key and value:
                        details[key] = value
                except (IndexError, StaleElementReferenceException):
                    continue
            
            # Process details to extract standard fields
            car_info = {}
            
            # Year, make, model from title
            if title:
                # Extract year
                year_match = re.search(r'\b(19|20)\d{2}\b', title)
                if year_match:
                    car_info['year'] = int(year_match.group())
                
                # Check if we can find make
                makes = ['honda', 'toyota', 'ford', 'chevrolet', 'chevy', 'nissan', 'bmw', 
                        'mercedes', 'audi', 'kia', 'hyundai', 'subaru', 'mazda', 'lexus', 
                        'acura', 'jeep', 'dodge', 'ram', 'chrysler', 'volvo', 'volkswagen', 
                        'vw', 'tesla', 'cadillac', 'buick', 'gmc']
                
                title_lower = title.lower()
                for make in makes:
                    if make in title_lower:
                        car_info['make'] = make.title()
                        # Try to extract model by getting text after make
                        make_pos = title_lower.find(make)
                        if make_pos >= 0:
                            rest_of_title = title[make_pos + len(make):].strip()
                            # Take the first word as the model
                            model_match = re.search(r'^[a-zA-Z0-9-]+', rest_of_title)
                            if model_match:
                                car_info['model'] = model_match.group().title()
                        break
            
            # Get standard fields from details
            for key, value in details.items():
                if 'condition' in key:
                    car_info['condition'] = value
                elif 'make' in key:
                    car_info['make'] = value
                elif 'model' in key:
                    car_info['model'] = value
                elif 'year' in key:
                    try:
                        car_info['year'] = int(value)
                    except ValueError:
                        pass
                elif 'mileage' in key or 'odometer' in key:
                    try:
                        car_info['mileage'] = int(re.sub(r'[^\d]', '', value))
                    except ValueError:
                        pass
                elif 'transmission' in key:
                    car_info['transmission'] = value
                elif 'fuel' in key or 'gas' in key:
                    car_info['fuel_type'] = value
                elif 'color' in key:
                    car_info['color'] = value
                elif 'drive' in key:
                    car_info['drive_type'] = value
                elif 'body' in key:
                    car_info['body_type'] = value
            
            # Get image URLs if enabled
            image_urls = []
            if not self.disable_images:
                try:
                    img_elements = self.driver.find_elements(By.CSS_SELECTOR, "img[data-visualcompletion='media-vc-image']")
                    image_urls = [img.get_attribute("src") for img in img_elements if img.get_attribute("src")]
                    image_urls = list(dict.fromkeys(image_urls))  # Remove duplicates
                except Exception as e:
                    self.logger.warning(f"Error extracting image URLs: {e}")
            
            # Get description
            description = ""
            try:
                desc_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[dir='auto'][style*='word-break']")
                if desc_elements:
                    description = desc_elements[0].text.strip()
            except Exception as e:
                self.logger.warning(f"Error extracting description: {e}")
            
            # Get seller info
            seller_name = ""
            try:
                seller_elements = self.driver.find_elements(By.CSS_SELECTOR, "a[aria-hidden='false'] > span")
                if seller_elements:
                    seller_name = seller_elements[0].text.strip()
            except Exception as e:
                self.logger.warning(f"Error extracting seller info: {e}")
            
            # Save raw HTML for fallback
            raw_html = self.driver.page_source if self.config.get("scraper", "save_raw_html", False) else None
            
            # Create listing data
            listing_data = {
                'id': listing_id,
                'title': title,
                'price': price,
                'url': url,
                'location': location,
                'listing_date': datetime.now().isoformat(),
                'description': description,
                'seller_name': seller_name,
                'image_urls': image_urls,
                'raw_html': raw_html,
                'details': details,
                **car_info
            }
            
            return listing_data
            
        except Exception as e:
            self.logger.error(f"Error in full listing parser: {e}")
            # Return minimal data with raw HTML for later parsing
            return {
                'id': self._extract_listing_id(url),
                'url': url,
                'listing_date': datetime.now().isoformat(),
                'raw_html': self.driver.page_source
            }
    
    def _extract_listing_id(self, url: str) -> str:
        """Extract the listing ID from the URL"""
        match = re.search(r'/item/(\d+)', url)
        if match:
            return match.group(1)
        else:
            # Generate a unique ID if we can't extract from URL
            return f"fb_{uuid.uuid4().hex[:16]}"
    
    def parse_offline(self, html_content: str) -> Dict:
        """
        Parse listing from stored HTML using BeautifulSoup
        For use when Selenium can't be used or for offline processing
        
        Args:
            html_content (str): Raw HTML content
            
        Returns:
            dict: Parsed listing data
        """
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Extract basic info
            title = ""
            title_element = soup.select_one("h1[dir='auto']")
            if title_element:
                title = title_element.text.strip()
            
            # Price
            price = 0
            price_element = soup.select_one("span[dir='auto'] > span")
            if price_element:
                price_text = price_element.text.strip()
                price = int(re.sub(r'[^\d]', '', price_text) or 0)
            
            # Extract vehicle details
            car_info = {}
            
            # Try to extract year, make, model from title
            if title:
                year_match = re.search(r'\b(19|20)\d{2}\b', title)
                if year_match:
                    car_info['year'] = int(year_match.group())
                
                # Check if we can find make
                makes = ['honda', 'toyota', 'ford', 'chevrolet', 'chevy', 'nissan', 'bmw', 
                        'mercedes', 'audi', 'kia', 'hyundai', 'subaru', 'mazda', 'lexus', 
                        'acura', 'jeep', 'dodge', 'ram', 'chrysler', 'volvo', 'volkswagen', 
                        'vw', 'tesla', 'cadillac', 'buick', 'gmc']
                
                title_lower = title.lower()
                for make in makes:
                    if make in title_lower:
                        car_info['make'] = make.title()
                        break
            
            # Mileage
            detail_texts = [elem.text for elem in soup.select("span[dir='auto']")]
            for text in detail_texts:
                if 'mile' in text.lower():
                    mileage_match = re.search(r'([\d,]+)', text)
                    if mileage_match:
                        car_info['mileage'] = int(re.sub(r'[^\d]', '', mileage_match.group()))
                        break
            
            # Get description
            description = ""
            desc_element = soup.select_one("div[dir='auto'][style*='word-break']")
            if desc_element:
                description = desc_element.text.strip()
            
            # Return parsed data
            listing_data = {
                'title': title,
                'price': price,
                'description': description,
                **car_info
            }
            
            return listing_data
            
        except Exception as e:
            self.logger.error(f"Error parsing HTML content: {e}")
            return {}
    
    def scrape_marketplace(self, search_url: str, max_listings: int = None) -> None:
        """
        Main method to scrape Facebook Marketplace
        
        Args:
            search_url (str): URL of Facebook Marketplace search results
            max_listings (int): Maximum number of listings to scrape (optional)
        """
        if max_listings is None:
            max_listings = self.max_listings
            
        # Start scraping session
        self.session_id = self.db_manager.start_scrape_session({"search_url": search_url})
        
        try:
            # Initialize WebDriver if not already done
            if not self.driver:
                self.driver = self._setup_driver()
            
            # Navigate to the search URL
            self.logger.info(f"Navigating to search URL: {search_url}")
            self.driver.get(search_url)
            
            # Wait for listings to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/marketplace/item/']"))
                )
            except TimeoutException:
                self.logger.warning("Timeout waiting for listing elements to load")
                
                # Check if this is a login page
                if "login" in self.driver.current_url or "Login" in self.driver.page_source:
                    self.logger.error("Facebook login required - cannot proceed")
                    self.db_manager.end_scrape_session(
                        self.session_id, 
                        status='failed',
                        error_message="Facebook login required"
                    )
                    return
            
            # Scroll to load listings
            listing_urls = self._scroll_to_load_listings(max_listings)
            self.stats["listings_found"] = len(listing_urls)
            
            self.logger.info(f"Found {len(listing_urls)} listings. Processing in batches of {self.batch_size}...")
            
            # Process listings in batches
            batch_counter = 0
            for i in range(0, len(listing_urls), self.batch_size):
                batch = listing_urls[i:i+self.batch_size]
                batch_counter += 1
                
                self.logger.info(f"Processing batch {batch_counter}/{(len(listing_urls) + self.batch_size - 1) // self.batch_size}")
                self._process_listing_batch(batch)
                
                # Check if we should pause due to resource constraints
                if self._should_throttle():
                    self.logger.info("Throttling due to resource constraints...")
                    self._throttle()
                    
                    # If memory usage is critically high, process the raw HTML later
                    memory_usage_mb, _ = self._check_resource_usage()
                    if memory_usage_mb > self.memory_limit_mb * 1.2:
                        self.logger.warning("Memory usage critically high, switching to simplified parser")
                        self.use_simplified_parser = True
            
            # Complete scraping session
            self.db_manager.end_scrape_session(
                self.session_id,
                status='completed',
                listings_found=self.stats["listings_found"],
                new_listings=self.stats["new_listings"],
                updated_listings=self.stats["updated_listings"]
            )
            
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
            self.db_manager.end_scrape_session(
                self.session_id,
                status='failed',
                listings_found=self.stats["listings_found"],
                new_listings=self.stats["new_listings"],
                updated_listings=self.stats["updated_listings"],
                error_message=str(e)
            )
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None
    
    def _process_listing_batch(self, urls: List[str]) -> None:
        """
        Process a batch of listing URLs
        
        Args:
            urls (list): List of listing URLs to process
        """
        # Filter out URLs that have already been processed in this session
        urls_to_process = [url for url in urls if url not in self.processed_urls]
        
        for url in urls_to_process:
            # Check resource usage before processing each listing
            if self._should_throttle():
                self._throttle()
            
            self.logger.info(f"Processing listing: {url}")
            listing_data = self._parse_listing_page(url)
            
            if listing_data:
                # Check if listing already exists
                existing_listing = self.db_manager.get_car_listing(listing_data['id'])
                
                if existing_listing:
                    self.stats["updated_listings"] += 1
                else:
                    self.stats["new_listings"] += 1
                
                # Save to database
                self.db_manager.save_car_listing(listing_data)
                
                # Mark as processed
                self.processed_urls.add(url)
                self.last_processed_url = url
                
                # Sleep a bit to avoid overloading resources
                time.sleep(random.uniform(0.5, 1.5))
    
    def pause_scraping(self) -> Dict:
        """
        Pause the current scraping session
        
        Returns:
            dict: Session state for resuming later
        """
        state = {
            "session_id": self.session_id,
            "last_processed_url": self.last_processed_url,
            "processed_urls": list(self.processed_urls),
            "stats": self.stats
        }
        
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
            
        return state
    
    def resume_scraping(self, state: Dict, search_url: str) -> None:
        """
        Resume scraping from a paused state
        
        Args:
            state (dict): Session state from pause_scraping
            search_url (str): Search URL to resume from
        """
        # Restore state
        self.session_id = state.get("session_id")
        self.last_processed_url = state.get("last_processed_url")
        self.processed_urls = set(state.get("processed_urls", []))
        self.stats = state.get("stats", self.stats)
        
        # Resume scraping
        self.scrape_marketplace(search_url)
    
    def cleanup(self) -> None:
        """Clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None