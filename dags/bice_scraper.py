import logging
import json
import time
import os

import random
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import requests
from bs4 import BeautifulSoup

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def setup_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    logger.info("Setting up Chrome driver...")
    from selenium import webdriver
    
    grid_url = os.getenv('SELENIUM_GRID_URL')
    if grid_url:
        logger.info(f"Connecting to Selenium Grid at {grid_url}...")
        driver = webdriver.Remote(command_executor=grid_url, options=options)
    else:
        driver = webdriver.Chrome(options=options)
        
    driver.set_window_size(1920, 1080)
    
    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        logger.info("CDP/JS Stealth command injected.")
    except Exception as e:
        logger.warning(f"Stealth injection failed: {e}")

    return driver

def scrape_bice():
    all_items = []
    
    # Define targets: (URL, Category)
    TARGETS = [
        ("https://banco.bice.cl/personas/beneficios?categoriaSeleccionada=restaurante", "Restaurante"),
        ("https://banco.bice.cl/personas/beneficios?categoriaSeleccionada=gourmet", "Antojo")
    ]
    
    for url, category in TARGETS:
        logger.info(f"--- Starting Scrape for Category: {category} ---")
        driver = None
        try:
            driver = setup_driver()
            
            logger.info(f"Navigating to {url}...")
            driver.get(url)
            logger.info("Page loaded. Waiting 15s for stability...")
            time.sleep(15) 
            
            # --- SCROLL & LOAD MORE ---
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            
            page_clicks = 0
            max_clicks = 20
            
            while page_clicks < max_clicks:
                try:
                    load_more_btn = None
                    # Priority 1: User specified button "Conocer más beneficios"
                    potential_btns = driver.find_elements(By.XPATH, "//button[contains(text(), 'Conocer más beneficios')]")
                    
                    if not potential_btns:
                        # Priority 2: Generic "Cargar"
                        potential_btns = driver.find_elements(By.XPATH, "//button[contains(@class, 'btn-primary') and contains(text(), 'Cargar')]")
                    
                    if not potential_btns:
                        # Priority 3: Broad "Ver más"
                        potential_btns = driver.find_elements(By.XPATH, "//button[contains(text(), 'Cargar') or contains(text(), 'Ver más')]")
    
                    found_active = False
                    for btn in potential_btns:
                        if btn.is_displayed():
                            load_more_btn = btn
                            found_active = True
                            break
                    
                    if found_active and load_more_btn:
                        logger.info(f"Found Load More button: '{load_more_btn.text}'")
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more_btn)
                        time.sleep(1)
                        try:
                            load_more_btn.click()
                        except:
                            driver.execute_script("arguments[0].click();", load_more_btn)
                            
                        time.sleep(4) 
                        page_clicks += 1
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(2)
                    else:
                        break
                        
                except Exception as e:
                    logger.warning(f"Pagination loop warnings: {e}")
                    break
            
            # --- EXTRACTION ---
            logger.info(f"Extracting data for {category}...")
            
            # Use specific class 'div.beneficio'
            candidates = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
            if not candidates:
                logger.info("No 'div.beneficio' found (fallback to broad)...")
                candidates = driver.find_elements(By.XPATH, "//div[.//img and string-length(normalize-space(.)) > 20]")
            
            logger.info(f"Found {len(candidates)} potential elements.")
            
            seen_urls = set()
            
            for i, card in enumerate(candidates):
                try:
                    text = card.text.strip()
                    if len(text) < 10 or len(text) > 800: continue
                    
                    # Link
                    link_url = ""
                    try:
                        if card.tag_name == 'a': link_url = card.get_attribute("href")
                        else:
                            links = card.find_elements(By.TAG_NAME, "a")
                            if links: link_url = links[0].get_attribute("href")
                            else:
                                parent = card.find_element(By.XPATH, "./..")
                                if parent.tag_name == 'a': link_url = parent.get_attribute("href")
                    except: pass
                    
                    if link_url:
                        clean_link = link_url.split('#')[0]
                        if clean_link in seen_urls: continue
                        if "/campana/" in clean_link: continue
                        if "banco.bice.cl" in clean_link and "beneficios" not in clean_link: continue
                    
                    # Image
                    img_url = ""
                    try:
                        imgs = card.find_elements(By.TAG_NAME, "img")
                        if imgs: img_url = imgs[0].get_attribute("src")
                    except: pass
                    if "arrow" in img_url or "search.svg" in img_url: continue

                    # --- TITLE PARSING ---
                    lines = [l.strip() for l in text.split('\n') if l.strip()]
                    if not lines: continue
                    
                    title = ""
                    try:
                         title_el = card.find_element(By.CLASS_NAME, "card-link__title")
                         title = title_el.text.strip()
                    except: pass

                    if not title:
                        title_idx = 0
                        while title_idx < len(lines):
                            candidate = lines[title_idx]
                            if re.match(r'^\d+%$', candidate) or candidate.lower() in ["restaurante", "gourmet", "beneficios", "nuevo", "exclusivo"]:
                                title_idx += 1
                            else:
                                break
                        if title_idx < len(lines): title = lines[title_idx]
                        else: title = lines[0]

                    if title in ["Gourmet", "Beneficios", "Restaurante", "Vigencia", "Conócelas todas", "Ver más"]: continue
                    if "conocelas todas" in title.lower(): continue

                    # Discount
                    match = re.search(r'(\d{1,2})%', text)
                    if not match and "dcto" not in text.lower() and "descuento" not in text.lower():
                         continue
                    discount_text = f"{match.group(1)}%" if match else "Ver detalle"

                    # --- LOCATION PARSING ---
                    locations = ["Varios"]
                    try:
                        if category == "Antojo":
                            locations = ["Todas las sucursales"]
                        else:
                             start_idx = 0
                             for idx, val in enumerate(lines):
                                  if val == title: 
                                       start_idx = idx + 1
                                       break
                             for p_line in lines[start_idx:]:
                                 if len(p_line) < 4: continue
                                 if "valid" in p_line.lower() or "hasta" in p_line.lower(): continue
                                 if "%" in p_line: continue
                                 if p_line.lower() in [title.lower(), "restaurante", "gourmet"]: continue
                                 
                                 if not re.search(r'\d', p_line) and len(p_line) < 50:
                                    locations = [x.strip() for x in re.split(r',| y ', p_line) if x.strip()]
                                    if len(locations) > 0: break
                    except: pass

                    item_template = {
                        "bank": "bice",
                        "title": title[:100],
                        "discount_text": discount_text,
                        "description": text[:500],
                        "image_url": img_url,
                        "url": link_url or url,
                        "scraped_at": datetime.now().isoformat(),
                        "category": category
                    }
    
                    for loc in locations:
                        final_item = item_template.copy()
                        final_item["location"] = loc
                        all_items.append(final_item)
                        
                    if link_url: seen_urls.add(clean_link)
                except Exception as e:
                    logger.warning(f"Error parsing card: {e}")
                    continue

        except Exception as cat_e:
            logger.error(f"Error scraping category {category}: {cat_e}")
        
        finally:
            if driver:
                try: driver.quit()
                except: pass
                
    # --- FINAL CLEANUP ---
    logger.info("Performing final cleanup...")
    final_items = []
    seen = set()
    for item in all_items:
        # Deduplicate
        uid = f"{item['title']}_{item['location']}"
        if uid in seen: continue
        seen.add(uid)

        # Fix duplicate Tearapy 
        if "Mi??rcoles" in item['description']:
            item['description'] = item['description'].replace("Mi??rcoles", "Miércoles")
        
        if "tearapy" in item['title'].lower() and "Miércoles" not in item['description']:
             item['description'] += " Miércoles"
             
        final_items.append(item)
        
    # --- DETAIL PAGE SCRAPING (Caps & Expirations) ---
    logger.info("Extracting details (Tope & Fecha) from individual pages...")
    detail_driver = None
    try:
        detail_driver = setup_driver()
        for item in final_items:
            link = item.get("url")
            if not link or "banco.bice.cl" not in link:
                continue
                
            logger.info(f"Visiting detail: {link}")
            detail_driver.get(link)
            time.sleep(3) # Wait for page to load
            
            try:
                page_text = detail_driver.find_element(By.TAG_NAME, "body").text
                
                # 1. Tope de descuento
                tope_match = re.search(r'Tope\s*(?:de\s*descuento)?\s*:\s*\$([\d\.]+)', page_text, re.IGNORECASE)
                if not tope_match:
                    tope_match = re.search(r'Tope\s*\$([\d\.]+)', page_text, re.IGNORECASE)
                    
                if tope_match:
                    cap_str = tope_match.group(1).replace(".", "")
                    try:
                        item["discount_cap"] = int(cap_str)
                        logger.info(f"Found discount cap: {item['discount_cap']}")
                    except ValueError:
                        pass
                
                # 2. Fecha límite
                fecha_match = re.search(r'Fecha límite:\s*Hasta\s+([\d]{1,2})\s+de\s+([a-zA-Z]+)\s+de\s+(\d{4})', page_text, re.IGNORECASE)
                if fecha_match:
                    day_str = fecha_match.group(1).zfill(2)
                    month_str = fecha_match.group(2).lower()
                    year_str = fecha_match.group(3)
                    
                    meses = {
                        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
                        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
                        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
                        "sept": "09"
                    }
                    
                    if month_str in meses:
                        item["expiration_date"] = f"{year_str}-{meses[month_str]}-{day_str}"
                        logger.info(f"Found expiration date: {item['expiration_date']}")
                    else:
                        item["expiration_date_str"] = fecha_match.group(0).strip()
            except Exception as e:
                logger.warning(f"Error extracting details for {link}: {e}")
                
    except Exception as e:
        logger.error(f"Error during detail scraping: {e}")
    finally:
        if detail_driver:
            try: detail_driver.quit()
            except: pass

    logger.info(f"Total VALID items after detail scraping: {len(final_items)}")
    
    # Save locally
    local_path = os.path.join(os.path.dirname(__file__), "json", "bice.json")
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(final_items, f, ensure_ascii=False, indent=4)
    logger.info(f"Saved to {local_path}")
    
    return final_items

if __name__ == "__main__":
    scrape_bice()
