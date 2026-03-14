import logging
import json
import time
import os

import re
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
START_URL = "https://www.scotiarewards.cl/scclubfront/categoria/platosycomida/rutagourmet"



MONTH_MAP = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
}

def normalize_date(date_str):
    if not date_str: return None
    try:
        # Format: "28 de febrero de 2026"
        parts = date_str.lower().split()
        if len(parts) >= 5:
            day = parts[0].zfill(2)
            month = MONTH_MAP.get(parts[2], "01")
            year = parts[4]
            return f"{year}-{month}-{day}"
    except:
        pass
    return None

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
    
    grid_url = os.getenv('SELENIUM_GRID_URL')
    if grid_url:
        logger.info(f"Connecting to Selenium Grid at {grid_url}...")
        driver = webdriver.Remote(command_executor=grid_url, options=options)
    else:
        driver = webdriver.Chrome(options=options)
        
    driver.set_window_size(1920, 1080)
    
    # CDP Stealth Injection
    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        logger.info("CDP/JS Stealth command injected.")
    except Exception as e:
        logger.warning(f"Stealth injection failed: {e}")

    return driver



def scrape_scotia():
    driver = setup_driver()
    all_items = []
    seen_urls = set()

    try:
        logger.info(f"Navigating to {START_URL}...")
        driver.get(START_URL)
        logger.info("Page loaded. Waiting 20s for stability...")
        time.sleep(20) 
        
        # 1. Pagination Loop (Click 'Load More')
        # We assume there might be a "Cargar más" or similar button
        page_clicks = 0
        max_clicks = 50 
        
        while page_clicks < max_clicks:
            try:
                # Scotiabank specific: Look for button with text "Cargar más" or "Ver más"
                btns = driver.find_elements(By.XPATH, "//button[contains(text(), 'Cargar') or contains(text(), 'Ver más') or contains(text(), 'Load')]")
                # Also check 'a' tags that look like buttons
                if not btns:
                    btns = driver.find_elements(By.XPATH, "//a[contains(@class, 'btn')][contains(text(), 'Cargar') or contains(text(), 'Ver más')]")
                
                load_btn = None
                for btn in btns:
                    if btn.is_displayed():
                        load_btn = btn
                        break
                
                if load_btn:
                    logger.info(f"Found Load More button: '{load_btn.text}'")
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_btn)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", load_btn)
                    time.sleep(4)
                    page_clicks += 1
                else:
                    logger.info("No active 'Load More' button found.")
                    break
            except Exception as e:
                 logger.warning(f"Pagination error: {e}")
                 break
        
        # --- SCRAPE ANTOJOS (RutaExpress) FIRST ---
        try:
            antojos_items = scrape_antojos(driver)
            all_items.extend(antojos_items)
            logger.info(f"Added {len(antojos_items)} Antojos items.")
            for item in antojos_items:
                seen_urls.add(f"{item['title']}_{item['location']}")
        except Exception as e:
            logger.error(f"Error scraping Antojos: {e}")

        # --- SCRAPE GOURMET (RutaGourmet) SECOND ---
        logger.info("Starting Click & Scrape Gourmet extraction...")
        
        # Already loaded Gourmet initially, but let's check if we are still on the right page
        # Actually, scrape_antojos leaves us on the Antojos page. Need to go back to Gourmet.
        driver.get(START_URL)
        time.sleep(10)
        
        candidates = driver.find_elements(By.CLASS_NAME, "marketing-card")
        if not candidates:
            candidates = driver.find_elements(By.XPATH, "//div[contains(@class, 'marketing-card') or (contains(@class, 'card') and .//img)]")
        
        total_items = len(candidates)
        logger.info(f"Found {total_items} Gourmet cards.")
        
        # Apply LIMIT_ITEMS if present
        limit_env = os.environ.get("LIMIT_ITEMS")
        limit = int(limit_env) if limit_env else total_items 
        
        for i in range(min(total_items, limit)):
            try:
                # Re-find candidates to ensure freshness
                current_cards = driver.find_elements(By.CLASS_NAME, "marketing-card")
                if not current_cards:
                     current_cards = driver.find_elements(By.XPATH, "//div[contains(@class, 'marketing-card') or (contains(@class, 'card') and .//img)]")
                
                if i >= len(current_cards): break 
                
                card = current_cards[i]
                
                # Robust Title Extraction via textContent
                pre_click_title = ""
                try:
                    # Target the H3 confirmed by inspection
                    h3_el = card.find_element(By.TAG_NAME, "h3")
                    pre_click_title = h3_el.get_attribute("textContent").strip()
                except:
                    # Fallback to general textContent scan if H3 fails
                    pre_click_title = driver.execute_script("""
                        var card = arguments[0];
                        var h3 = card.querySelector('h3');
                        if (h3) return h3.textContent.trim();
                        var h4 = card.querySelector('h4');
                        if (h4) return h4.textContent.trim();
                        return '';
                    """, card)
                
                if not pre_click_title:
                    # Final attempt: try to find any text that isn't the discount
                    card_text = driver.execute_script("return arguments[0].textContent", card).strip()
                    lines = [l.strip() for l in card_text.split('\n') if l.strip()]
                    for line in lines:
                        if len(line) > 3 and '%' not in line and 'Tope' not in line:
                            pre_click_title = line
                            break
                
                card_text = driver.execute_script("return arguments[0].textContent", card).strip()
                # Antojos specific: look for 'headline-small' inside the card for discount
                try:
                     discount_small = card.find_element(By.CLASS_NAME, "headline-small").text.strip()
                     if "%" in discount_small:
                         card_text += " " + discount_small # Append to refine regex search
                except: pass

                match = re.search(r'(\d{1,2})%', card_text)
                discount_text = f"{match.group(1)}%" if match else "Ver detalle"
                
                # Scroll and Click to open Detail Overlay
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
                time.sleep(2)
                
                # Try clicking nicely, then JS
                clicked = False
                try:
                    # element must be clickable
                    WebDriverWait(driver, 5).until(EC.element_to_be_clickable(card))
                    card.click()
                    clicked = True
                except:
                    try:
                        driver.execute_script("arguments[0].click();", card)
                        clicked = True
                    except:
                        logger.warning(f"Could not click card {i}")
                
                if clicked:
                     time.sleep(4) # Force wait for animation/modal load
                                 
                # Wait for Detail View (Check for specific ID from user snippet)
                
                # Wait for Detail View (Check for specific ID from user snippet)
                try:
                    # Wait for address container to be present in DOM (even if hidden)
                    WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "direcciones-container"))
                    )
                except:
                    # If wait fails, maybe it didn't open or is different format
                    logger.warning(f"Detail view didn't load for item {i}. Skipping.")
                    continue

                # --- SCRAPE DETAIL OVERLAY ---
                try:
                    # Title
                    try:
                        title_el = driver.find_element(By.ID, "detalle-banner-nombre")
                        title = title_el.text.strip()
                    except: 
                        title = pre_click_title or card_text.split('\n')[0] # Fallback
                    
                    # Description (Legal Terms + More)
                    try:
                        # Get main legal terms
                        desc_text = ""
                        try:
                            desc_el = driver.find_element(By.ID, "condiciones-legales")
                            desc_text = desc_el.text.strip()
                        except: pass
                        
                        # Get "Detalle del Beneficio" and Header snippets
                        extra_text = ""
                        try:
                            # Capture any 'p' with caption or body classes inside the detail view
                            captions = driver.find_elements(By.CSS_SELECTOR, "#sitio-detalle p.caption, #sitio-detalle p.body-2, #sitio-detalle h4")
                            extra_text = "\n".join([c.text.strip() for c in captions if c.text.strip()])
                        except: pass
                        
                        description = f"{extra_text}\n\n{desc_text}".strip()
                        if not description:
                             description = card_text 
                    except:
                        description = card_text # Fallback

                    # Extraction Logic for Cap and Exp
                    discount_cap = None
                    # Regex for Cap: looks for numbers after "tope", usually with dot separators (e.g. 15.000)
                    match_cap = re.search(r'tope\s*.*?\$?\s*(\d{1,3}(?:\.\d{3})+|\d{4,6})', description, re.IGNORECASE)
                    if match_cap:
                        discount_cap = match_cap.group(1).replace('.', '')
                        # Sanity check: if cap is too small (e.g. < 1000) it might be a percentage or wrong match
                        if int(discount_cap) < 1000:
                            discount_cap = None
                    
                    expiration_date = None
                    # Improved Regex to handle "hasta el 31 de marzo..." and other variations
                    match_exp = re.search(r'hasta el (\d{1,2} de \w+ de \d{4})', description, re.IGNORECASE)
                    if not match_exp:
                        match_exp = re.search(r'válido hasta el (\d{1,2} de \w+ de \d{4})', description, re.IGNORECASE)
                    
                    if match_exp:
                        expiration_date = normalize_date(match_exp.group(1))
                    else:
                        # Fallback for "marzo de 2026" without day
                        match_month_year = re.search(r'de (\w+) de (\d{4})', description, re.IGNORECASE)
                        if match_month_year:
                             # Default to last day? Or just leave it for SQL to handle?
                             # Let's keep it null if no day, but capture month info for SQL
                             pass

                    # Location (Address)
                    try:
                        # Capture ALL address containers, not just the first one. Use presence to get hidden ones.
                        addr_container = driver.find_elements(By.CLASS_NAME, "direcciones-container")
                        if addr_container:
                            # Try to get text, fallback to innerHTML parsing/textContent
                            container_text = addr_container[0].get_attribute("textContent").strip()
                            
                            # Look for <a> tags inside
                            links = addr_container[0].find_elements(By.TAG_NAME, "a")
                            if links:
                                locations_list = [link.get_attribute("textContent").strip() for link in links if link.get_attribute("textContent").strip()]
                                location = " | ".join(locations_list)
                            elif container_text:
                                location = container_text
                            else:
                                location = "Varios"
                        else:
                            location = "Varios"
                    except:
                        location = "Varios"
                    
                    # Image
                    img_url = ""
                    try:
                        # Main hero image in detail view
                        hero_img = driver.find_element(By.XPATH, "//div[@id='sitio-detalle']//img[contains(@class, 'sc-max-height')]")
                        img_url = hero_img.get_attribute("src")
                    except: pass

                    item = {
                        "bank": "scotiabank",
                        "title": title[:150],
                        "discount_text": discount_text,
                        "location": location,
                        "description": description[:2000],
                        "discount_cap": discount_cap,
                        "expiration_date": expiration_date,
                        "image_url": img_url,
                        "url": START_URL, # They don't have unique URLs
                        "scraped_at": datetime.now().isoformat()
                    }
                    
                    # Deduplication check (Content Key)
                    content_key = f"{title}_{location}"
                    if content_key not in seen_urls:
                         all_items.append(item)
                         seen_urls.add(content_key)
                         logger.info(f"[{i+1}/{total_items}] Scraped: {title} (Cap: {discount_cap})")
                    
                except Exception as e:
                    logger.error(f"Error extracting details for {i}: {e}")

                # --- GO BACK (Critical) ---
                try:
                    # Button "Volver"
                    back_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'Volver')]")
                    driver.execute_script("arguments[0].click();", back_btn)
                    
                    # Wait for list to reappear
                    WebDriverWait(driver, 5).until(
                        EC.invisibility_of_element_located((By.ID, "sitio-detalle"))
                    )
                    time.sleep(1) # Extra buffer for list re-render
                except Exception as e:
                    logger.error(f"Failed to go back: {e}. Trying to refresh.")
                    driver.get(START_URL)
                    time.sleep(5)
            
            except Exception as e:
                logger.error(f"Item loop error: {e}")
                continue
            
        logger.info(f"Total VALID items collected: {len(all_items)}")

    finally:
        driver.quit()
        
    return all_items

def scrape_antojos(driver):
    """
    Scrapes the 'RutaExpress' (Antojos) category using click-to-extract.
    URL: https://www.scotiarewards.cl/scclubfront/categoria/platosycomida/RutaExpress
    """
    url = "https://www.scotiarewards.cl/scclubfront/categoria/platosycomida/RutaExpress"
    items = []
    seen_titles = set()
    
    logger.info(f"Navigating to Antojos URL: {url}")
    driver.get(url)
    time.sleep(10) # Wait for load
    
    # Pagination
    page_clicks = 0
    max_clicks = 20
    while page_clicks < max_clicks:
        try:
            btns = driver.find_elements(By.XPATH, "//button[contains(text(), 'Cargar')]")
            if not btns:
                btns = driver.find_elements(By.XPATH, "//a[contains(@class, 'btn')][contains(text(), 'Cargar')]")
            
            load_btn = None
            for btn in btns:
                if btn.is_displayed():
                    load_btn = btn
                    break
            
            if load_btn:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", load_btn)
                time.sleep(3)
                page_clicks += 1
            else:
                break
        except:
            break

    # Extract Cards with clicks
    cards = driver.find_elements(By.CLASS_NAME, "marketing-card")
    total_antojos = len(cards)
    
    limit_env = os.environ.get("LIMIT_ITEMS")
    limit = int(limit_env) if limit_env else total_antojos
    
    logger.info(f"Found {total_antojos} Antojos cards to process. Using limit: {limit}")
    
    for i in range(min(total_antojos, limit)):
        try:
            current_cards = driver.find_elements(By.CLASS_NAME, "marketing-card")
            if i >= len(current_cards): break
            card = current_cards[i]
            
            # Basic info before click - capture it as fallback
            card_text = card.text.strip()
            if not card_text:
                card_text = card.get_attribute("innerText") or ""
            
            try:
                title = card.find_element(By.CSS_SELECTOR, "h3.subtitle-1, h3").text.strip()
            except:
                title = card.get_attribute("data-titulo") or f"Antojo_{i}"
            
            # Days usually in subtitle
            pre_click_days = ""
            try:
                pre_click_days = card.find_element(By.CSS_SELECTOR, ".subtitle-2, .body-2, h6").text.strip()
            except: pass

            # Click to get details
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
            time.sleep(2)
            
            try:
                driver.execute_script("arguments[0].click();", card)
                time.sleep(5)
            except:
                logger.warning(f"Could not click Antojo card {i}")

            # Scrape Detail (modal)
            discount_text_final = "Ver detalle"
            try:
                description = ""
                # Try multiple common selectors for legal text
                for selector in ["condiciones-legales", "condicion-legal", "legal-text", "marketing-legal"]:
                    try:
                        desc_el = driver.find_element(By.ID, selector)
                        description = desc_el.text.strip()
                        if description: break
                    except: pass
                
                # Extract simple legal text from body if it looks like the open modal
                if not description:
                     body_text = driver.find_element(By.TAG_NAME, "body").text
                     if "Tope de Dcto" in body_text or "Valid" in body_text:
                         match = re.search(r'(Promoción válida.*?Tope.*?titular\.)', body_text, re.DOTALL | re.IGNORECASE)
                         if match:
                             description = match.group(1).replace('\n', ' ')

                if not description or len(description) < 20:
                    description = pre_click_days if pre_click_days else (card_text if card_text else "Promoción Scotiabank")
                
                # Set discount text natively
                discount_match = re.search(r'(\d{1,2})%', card_text)
                if discount_match:
                    discount_text_final = f"{discount_match.group(1)}%"
                else:
                    try:
                         discount_small = card.find_element(By.CLASS_NAME, "headline-small").text.strip()
                         if "%" in discount_small:
                             discount_text_final = discount_small
                    except: pass

                # Title Fallback using lines
                lines = [l.strip() for l in card_text.split('\n') if l.strip()]
                if title.startswith("Antojo_"):
                    for l in lines:
                        if len(l) > 3 and "viernes" not in l.lower() and "lunes" not in l.lower() and "%" not in l:
                            title = l[:100]
                            break

                # Cap and Exp
                discount_cap = None
                match_cap = re.search(r'tope\s*.*?\$?\s*(\d{1,3}(?:\.\d{3})+|\d{4,6})', description, re.IGNORECASE)
                if match_cap:
                    discount_cap = match_cap.group(1).replace('.', '')
                    if int(discount_cap) < 1000:
                        discount_cap = None
                
                expiration_date = None
                match_exp = re.search(r'hasta el (\d{1,2} de \w+ de \d{4})', description, re.IGNORECASE)
                if match_exp:
                    expiration_date = normalize_date(match_exp.group(1))

                img_url = ""
                try:
                    hero_img = driver.find_element(By.XPATH, "//div[@id='sitio-detalle']//img[contains(@class, 'sc-max-height')]")
                    img_url = hero_img.get_attribute("src")
                except: pass

                item = {
                    "bank": "scotiabank",
                    "title": title,
                    "discount_text": discount_text_final, 
                    "location": "Varios",
                    "description": description,
                    "discount_cap": discount_cap,
                    "expiration_date": expiration_date,
                    "image_url": img_url,
                    "url": url,
                    "scraped_at": datetime.now().isoformat(),
                    "category": "Antojo"
                }

                if title not in seen_titles:
                    items.append(item)
                    seen_titles.add(title)
                    logger.info(f"Scraped Antojo: {title} (Cap: {discount_cap})")

            except Exception as e:
                logger.warning(f"Error in Antojo detail extraction {i}: {e}")

            # Go Back
            try:
                back_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'Volver')]")
                driver.execute_script("arguments[0].click();", back_btn)
                time.sleep(1)
            except:
                try:
                    from selenium.webdriver.common.action_chains import ActionChains
                    from selenium.webdriver.common.keys import Keys
                    ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                    time.sleep(1)
                except:
                    driver.get(url)
                    time.sleep(5)

        except Exception as e:
            logger.warning(f"Error iterating Antojo card {i}: {e}")
            
    return items

if __name__ == "__main__":
    data = scrape_scotia()
    if data:
        local_path = os.path.join(os.path.dirname(__file__), "json", "scotiabank.json")
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ Scraped {len(data)} items -> {local_path}")
    else:
        logger.error("No items found.")

