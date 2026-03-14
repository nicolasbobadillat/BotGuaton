import json
import logging
import time
import os

import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import bice_scraper

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'json', 'bice.json')

# Monkey Patch Driver to Local
def setup_local_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # Headful for monitoring (user request logic)
    options.add_argument("--start-maximized") 
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    logger.info("Initializing local Chrome driver...")
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

bice_scraper.setup_driver = setup_local_driver



if __name__ == "__main__":
    logger.info("Starting LOCAL BICE Scraper...")
    try:
        items = bice_scraper.scrape_bice()
        if items:
            logger.info(f"✅ Scraped {len(items)} items (saved by scraper to json/bice.json)")
        else:
            logger.error("No items found!")
    except Exception as e:
        logger.critical(f"FATAL ERROR: {e}")

