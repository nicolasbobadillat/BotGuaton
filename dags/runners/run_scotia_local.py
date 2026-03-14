import json
import logging
import time
import os

import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import scotia_scraper

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'json', 'scotiabank.json')

# Monkey Patch Driver to Local
def setup_local_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized") 
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    logger.info("Initializing local Chrome driver...")
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

scotia_scraper.setup_driver = setup_local_driver



# Monkey Patch the Main Scraping Logic to be more Robust
# Using the real scraper logic imported from scotia_scraper
# scotia_scraper.scrape_scotia = scrape_scotia_robust

if __name__ == "__main__":
    logger.info("Starting LOCAL SCOTIABANK Scraper...")
    items = scotia_scraper.scrape_scotia()
    if items:
        target = os.path.abspath(OUTPUT_FILE)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ Saved {len(items)} items to {target}")
    else:
        logger.error("No items found!")

