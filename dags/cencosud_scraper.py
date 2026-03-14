import logging
import json
import time
import re
import os

from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
URL = "https://www.tarjetacencosud.cl/publico/beneficios/landing/la-ruta-del-sabor"



def setup_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    grid_url = os.getenv('SELENIUM_GRID_URL', 'http://localhost:4444/wd/hub')
    logger.info(f"Connecting to Selenium Grid at {grid_url}...")
    
    driver = webdriver.Remote(
        command_executor=grid_url,
        options=options
    )
    return driver



def scrape_cencosud():
    driver = setup_driver()
    all_items = []
    
    # Define targets: (URL, Category)
    TARGETS = [
        ("https://www.tarjetacencosud.cl/publico/beneficios/landing/la-ruta-del-sabor", "Restaurante"),
        ("https://www.tarjetacencosud.cl/publico/beneficios/landing/descuentos-comida", "Antojo")
    ]
    
    try:
        for url, category in TARGETS:
            try:
                logger.info(f"--- Scraping Category: {category} ---")
                logger.info(f"Navigating to {url}...")
                driver.get(url)
                time.sleep(5) # Allow full render

                # --- Extract Cards based on Category/Page Structure ---
                
                # RESTAURANTE LOGIC (Existing)
                if category == "Restaurante":
                    cards = driver.find_elements(By.CSS_SELECTOR, ".grilla_item")
                    logger.info(f"Found {len(cards)} cards for {category}")
            
                    for card in cards:
                        try:
                            # 1. Title
                            title = "Unknown"
                            try:
                                title_el = card.find_element(By.CSS_SELECTOR, ".tit")
                                title = title_el.text.strip()
                            except: pass
            
                            # 2. Discount
                            discount_text = "N/A"
                            try:
                                desc_el = card.find_element(By.CSS_SELECTOR, ".desc")
                                discount_text = desc_el.text.strip().replace("\n", " ")
                            except: pass
            
                            # 3. Location & Recurrence (from UL > LI)
                            location = "Varios"
                            recurrence = "Consultar condiciones"
                            try:
                                lis = card.find_elements(By.CSS_SELECTOR, "ul li")
                                if len(lis) >= 1:
                                    location = lis[0].text.strip()
                                if len(lis) >= 2:
                                    recurrence = lis[1].text.strip()
                            except: pass
            
                            # 4. Conditions (Hidden legal text)
                            conditions = ""
                            try:
                                legal_el = card.find_element(By.CSS_SELECTOR, ".legal")
                                conditions = legal_el.get_attribute("textContent").strip()
                            except: pass
            
                            # 5. Link
                            link = ""
                            try:
                                a_tag = card.find_element(By.TAG_NAME, "a")
                                link = a_tag.get_attribute("href")
                            except: pass

                            # Image
                            img_url = ""
                            try:
                                img = card.find_element(By.TAG_NAME, "img")
                                img_url = img.get_attribute("src")
                            except: pass
            
                            item = {
                                "bank": "cencosud",
                                "title": title,
                                "discount_text": discount_text,
                                "recurrence": recurrence,
                                "location": location,
                                "conditions": conditions,
                                "description": f"{title}\n{discount_text}\n{location}\n{recurrence} | {conditions[:50]}...",
                                "image_url": img_url,
                                "url": link,
                                "scraped_at": datetime.now().isoformat(),
                                "category": category
                            }
                            
                            if title != "Unknown":
                                all_items.append(item)
            
                        except Exception as e:
                            logger.error(f"Error parsing Restaurante item: {e}")
                            continue

                if category == "Antojo":
                    # Target specifically the FOOD container
                    cards = driver.find_elements(By.CSS_SELECTOR, "#benefitsContainerComida div.benefit-card")
                    logger.info(f"Found {len(cards)} cards for {category}")

                    for card in cards:
                        try:
                            # Link
                            link = ""
                            try:
                                a_tag = card.find_element(By.CSS_SELECTOR, "a.card-ben")
                                link = a_tag.get_attribute("href")
                            except: pass

                            # Image
                            img_url = ""
                            try:
                                img = card.find_element(By.CLASS_NAME, "card_foto").find_element(By.TAG_NAME, "img")
                                img_url = img.get_attribute("src")
                            except: pass

                            # Text Content
                            title = "Unknown"
                            discount_text = "N/A"
                            recurrence = ""
                            
                            try:
                                txt_div = card.find_element(By.CLASS_NAME, "card_txt")
                                # Use textContent to get text even if hidden/hover-only
                                h4_text = txt_div.find_element(By.TAG_NAME, "h4").get_attribute("textContent").strip()
                                
                                # Try to split discount from title
                                if "%" in h4_text:
                                    parts = h4_text.split(" en ", 1)
                                    if len(parts) > 1:
                                        discount_text = parts[0].strip() # "40% dcto."
                                        title = parts[1].strip()         # "Burger King"
                                    else:
                                        title = h4_text
                                        discount_text = h4_text
                                else:
                                    title = h4_text

                                # Recurrence/Conditions in <p>
                                p_text = txt_div.find_element(By.TAG_NAME, "p").get_attribute("textContent").strip()
                                recurrence = p_text
                            except: pass
                            
                            # Fallback for discount if visible in hidden div? (User snippet shows visible=false)
                            # Checking for explicit hidden discount div just in case it's useful
                            if discount_text == "N/A":
                                try:
                                    dcto_div = card.find_element(By.CLASS_NAME, "card_dcto")
                                    discount_text = dcto_div.get_attribute("textContent").strip()
                                except: pass

                            item = {
                                "bank": "cencosud",
                                "title": title,
                                "discount_text": discount_text,
                                "recurrence": recurrence,
                                "location": "Varios", # Usually chains
                                "conditions": recurrence, # Mapping description to conditions for now
                                "description": f"{title}\n{discount_text}\n{recurrence}",
                                "image_url": img_url,
                                "url": link or url,
                                "scraped_at": datetime.now().isoformat(),
                                "category": category
                            }

                            if title != "Unknown":
                                all_items.append(item)

                        except Exception as e:
                            logger.error(f"Error parsing Antojo item: {e}")
                            continue

            except Exception as cat_e:
                logger.error(f"Error processing category {category}: {cat_e}")
                continue

    finally:
        driver.quit()
        
    return all_items

if __name__ == "__main__":
    items = scrape_cencosud()
    if items:
        # Save locally
        with open(os.path.join(os.path.dirname(__file__), "json", "cencosud.json"), "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ Scraped {len(items)} items -> json/cencosud.json")

