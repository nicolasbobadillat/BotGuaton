import json
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import cencosud_scraper

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MOCK setup_driver to use Local Chrome
def setup_local_driver():
    options = Options()
    options.add_argument("--start-maximized")
    # options.add_argument("--headless") 
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

# Monkey patch
cencosud_scraper.setup_driver = setup_local_driver



if __name__ == "__main__":
    import os
    logger.info("Starting Local Cencosud Scraper Run...")
    items = cencosud_scraper.scrape_cencosud()
    if items:
        out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'json', 'cencosud.json')
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ Saved {len(items)} items to {out_path}")
    else:
        logger.warning("No items scraped.")

