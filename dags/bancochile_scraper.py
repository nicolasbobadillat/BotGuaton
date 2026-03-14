import logging
import json
import time
import os

import random
import re
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configure Logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bancochile_debug.log", mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
URLS_CONFIG = [
    {
        "url": "https://sitiospublicos.bancochile.cl/personas/beneficios/sabores/restaurantes-y-bares",
        "card_segment": "General",
    },
    {
        "url": "https://sitiospublicos.bancochile.cl/personas/beneficios/sabores/40-de-descuento-visa",
        "card_segment": "Visa Signature/Platinum", 
    },
    {
        "url": "https://sitiospublicos.bancochile.cl/personas/beneficios/sabores/50-de-descuento-visa-infinite",
        "card_segment": "Visa Infinite",
    },
    {
        "url": "https://sitiospublicos.bancochile.cl/personas/beneficios/sabores/comida-rapida",
        "card_segment": "Comida Rápida",
    },
    {
        "url": "https://sitiospublicos.bancochile.cl/personas/beneficios/sabores/cafeterias",
        "card_segment": "Cafeterías",
    },
    {
        "url": "https://sitiospublicos.bancochile.cl/personas/beneficios/categoria#sabores",
        "card_segment": "Sabores",
    }

]

# Output Config
JSON_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'json')
JSON_OUTPUT_FILE = os.path.join(JSON_OUTPUT_DIR, 'bancochile.json')

def parse_days_from_text(text):
    """
    Parses natural language day ranges/lists into a list of day codes or names.
    Examples:
    - "Lunes a Viernes" -> ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes']
    - "Sábados y Domingos" -> ['Sábado', 'Domingo']
    - "Todos los días" -> ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    - "Lunes y jueves a domingo" -> ['Lunes', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    """
    if not text: return []
    DAYS_REAL = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    DAYS_NORM = ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado', 'Domingo']
    
    # 1. Normalize text EARLY
    text = text.lower()
    text = text.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
    text = text.replace('miercoles', 'miercoles').replace('sabado', 'sabado') 
    
    # 2. Check for "all days" safely
    if 'todos los dias' in text or 'diariamente' in text:
        return DAYS_REAL
    
    found_days = set()
    
    # Strategy: Split by 'y' or ',' to separate distinct blocks, then parse each block
    parts = re.split(r'\s+y\s+|,\s*', text)
    
    for part in parts:
        part = part.strip()
        # Look for range "day 'a' day"
        range_match = re.search(r'(lunes|martes|miercoles|jueves|viernes|sabado|domingo)\s+a\s+(lunes|martes|miercoles|jueves|viernes|sabado|domingo)', part)
        
        if range_match:
            start_day = range_match.group(1).capitalize()
            end_day = range_match.group(2).capitalize()
            try:
                start_idx = DAYS_NORM.index(start_day)
                end_idx = DAYS_NORM.index(end_day)
                if start_idx <= end_idx:
                    found_days.update(DAYS_REAL[start_idx:end_idx+1])
                else: # Wrap around
                    found_days.update(DAYS_REAL[start_idx:] + DAYS_REAL[:end_idx+1])
            except: pass
        else:
            # Single days in this part
            for idx, day in enumerate(DAYS_NORM):
                if day.lower() in part:
                    found_days.update([DAYS_REAL[idx]])

    # Sort days according to week order
    return sorted(list(found_days), key=lambda d: DAYS_REAL.index(d))



def setup_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    grid_url = os.getenv('SELENIUM_GRID_URL', 'http://localhost:4444/wd/hub')
    logger.info(f"Connecting to Selenium Grid at {grid_url}...")
    
    try:
        driver = webdriver.Remote(
            command_executor=grid_url,
            options=options
        )
        # Mask webdriver property
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver
    except Exception as e:
        logger.error(f"Failed to connect to Selenium Grid: {e}")
        raise e

def save_json(data):
    """Save scraped data to local JSON file."""
    os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)
    with open(JSON_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logger.info(f"Saved {len(data)} items to {JSON_OUTPUT_FILE}")
 
def parse_expiration_date(soup, fallback_text=""):
    """
    Extracts and parses the expiration date from the page.
    Prioritizes the p#validez element, then fallbacks to regex on provided text.
    """
    expiration_date = None
    try:
        # 1. Primary Source: p#validez
        validez_p = soup.select_one("p#validez")
        search_text = validez_p.get_text(strip=True) if validez_p else fallback_text
        
        # 2. Regex Search
        # Handles "hasta el 30 de junio de 2026" and "desde el 01 al 28 de febrero de 2026"
        date_match = re.search(r'(?:hasta el|al)\s+(\d{1,2}) de ([a-z]+) de (\d{4})', search_text, re.IGNORECASE)
        if date_match:
            day, month_str, year = date_match.groups()
            months = {
                'nero': '01', 'ebrero': '02', 'arzo': '03', 'bril': '04', 'ayo': '05', 'unio': '06',
                'ulio': '07', 'gosto': '08', 'eptiembre': '09', 'ctubre': '10', 'oviembre': '11', 'iciembre': '12'
            }
            month_key = next((m for m in months if m in month_str.lower()), None)
            if month_key:
                expiration_date = f"{year}-{months[month_key]}-{int(day):02d}"
    except Exception as e:
        logger.debug(f"Error parsing expiration date: {e}")
        
    return expiration_date

def extract_detail_data(page_source, segment):
    soup = BeautifulSoup(page_source, 'html.parser')
    
    # --- ANTOJOS SPECIAL HANDLING ---
    # Detect based on segment OR title (chains usually are antojos)
    CHAINS_PATTERN = r'(MCDONALD|KFC|JUAN MAESTRO|CHINA WOK|WENDY|DOMINO|DUNKIN|SUSHI BLUES|VOLKA|YOGEN FRUZ|MELT|BURGER KING|DOGGIS|PAPA JOHN|PIZZA HUT|SUBWAY|TARRAGONA|LITTLE CAESAR|LE VICE|STARBUCKS|CARL|TOMMY|PAPA CONEJO|TACO BELL|VELVET BAKERY|BURGERBEEF)'

    
    # Try to get title from soup
    h1 = soup.select_one("h1.beneficio-compania")
    page_title = h1.get_text(strip=True) if h1 else ""
    
    is_antojo = (segment in ["Comida Rápida", "Cafeterías", "Sabores"]) or \
                (re.search(CHAINS_PATTERN, page_title, re.IGNORECASE))
    
    location = "Varios"
    extracted_title = page_title


    description = ""
    rules = []
    discount_pct_val = None
    discount_days_text = ""
    
    # --- GLOBAL EXTRACTION (Common to all detail pages) ---
    try:
        h2 = soup.select_one("h2.beneficio-title")
        if h2: 
            h2_text = h2.get_text(strip=True)
            # This is often the best source for days and description
            description = h2_text
            discount_days_text = h2_text 
            # Extract discount % (e.g., "50% dto.")
            disc_match = re.search(r'(\d+)%', h2_text)
            if disc_match:
                discount_pct_val = int(disc_match.group(1))
    except: pass

    if is_antojo:
        # Title already extracted as page_title

        
        # 2. Locations (Skip individual)
        location = "Todas las sucursales"
        full_locations = [location]
        
        # 3. Rules from div.text-gray
        try:
            info_div = soup.select_one("div.text-gray.mb-5")
            if info_div:
                u = info_div.find("ul")
                if u: rules = [li.get_text(strip=True) for li in u.find_all("li")]
                # Also append paragraph text to description if needed
                p = info_div.find("p")
                if p: description += "\n" + p.get_text("\n", strip=True)
        except: pass

        # 5. Expiration
        expiration_date = parse_expiration_date(soup, f"{description} {' '.join(rules)}")
        
        # 6. Discount Cap from Rules
        discount_cap = None
        full_rules_text = " ".join(rules)
        # Pattern: "Tope de dcto. $40.000 por mesa"
        try:
            tope_match = re.search(r'tope(?:[\s\.]*(?:dto\.?|de\s*descuento))?[\s\:]*\$([\d\.]+)', full_rules_text, re.IGNORECASE)
            if tope_match:
                raw_cap = tope_match.group(1).replace('.', '')
                discount_cap = int(raw_cap)
        except: pass

        phone = "" # Usually no phone for antojos/chains
        
    else:
        # --- STANDARD RESTAURANT HANDLING ---
        
        # 1. Location & Address from 'div.sucursal'
        try:
            sucursales = soup.select("div.sucursal")
            full_locations = []
            if sucursales:
                for suc in sucursales:
                    try:
                        # Get the first h4 which contains the street address
                        address_h4 = suc.select_one("div.title h4")
                        address = address_h4.get_text(strip=True) if address_h4 else ""
                        region_p = suc.select_one("p.text-1.text-gray-light")
                        region_txt = region_p.get_text(strip=True) if region_p else ""
                        full_loc = f"{address}, {region_txt}".strip(", ")
                        if full_loc: full_locations.append(full_loc)
                    except: pass
                if full_locations: location = " | ".join(full_locations)
        except: pass

        # 2. Description & Rules
        try:
            info_div = soup.select_one("div.text-gray.mb-5")
            if info_div:
                p = info_div.find("p")
                if p: 
                    p_text = p.get_text("\n", strip=True)
                    # If we don't have a description from h2, use this
                    if not description: description = p_text
                    # Append it anyway to have full info
                    else: description += " | " + p_text
                
                u = info_div.find("ul")
                if u: rules = [li.get_text(strip=True) for li in u.find_all("li")]
        except: pass
        
        # If h2 wasn't found, try parsing days from description/rules
        if not discount_days_text:
            discount_days_text = f"{description} {' '.join(rules)}"

        # 3. Phone (from JSON-LD usually)
        try:
            ld_scripts = soup.find_all("script", type="application/ld+json")
            for script in ld_scripts:
                try:
                    data = json.loads(script.string)
                    if "@type" in data and data["@type"] == "BankOrCreditUnion":
                        phone = data.get("telephone", "")
                except: pass
        except: pass

        # 4. Title Fallback
        try:
            h1 = soup.select_one("h1.beneficio-compania")
            if h1: extracted_title = h1.get_text(strip=True)
        except: pass
        
        # 5. Advanced Parsing: Tope & Expiration (Same logic as before but applied to standard)
        discount_cap = None
        expiration_date = parse_expiration_date(soup, f"{description} {' '.join(rules)}")

    # --- COMMON RETURN ---
    
    # Combined description for recurrence parsing if needed
    recurrence_text = discount_days_text if is_antojo else description
    
    return {
        "location": location,
        "description": description,
        "phone": phone if 'phone' in locals() else "",
        "conditions": f"{segment} - {' '.join(rules)}" if rules else segment,
        "extracted_title": extracted_title,
        "locations_list": full_locations if 'full_locations' in locals() else [location],
        "discount_cap": discount_cap,
        "expiration_date": expiration_date,
        "recurrence_text": recurrence_text, # For day parsing
        "discount_pct": discount_pct_val # NEW: extracted from banner
    }


def scrape_bancochile(target_segments=None):
    driver = setup_driver()
    all_items = []
    seen_hashes = set()
    processed_urls = set()
    
    try:
        for config in URLS_CONFIG:
            url = config["url"]
            segment = config["card_segment"]
            
            if target_segments and segment not in target_segments:
                continue
                
            logger.info(f"--- Processing {segment} ---")
            
            driver.get(url)
            time.sleep(10) # Initial load

            wait = WebDriverWait(driver, 20)
            grid_selector = "div.grid"
            next_btn_id = "ppp_landing-link-next-page"
            
            page_num = 1
            while True:
                logger.info(f"Processing Page {page_num} ({segment})")
                
                # 1. Wait for Grid
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, grid_selector)))
                    time.sleep(3)
                except:
                    logger.warning(f"Timeout waiting for grid on page {page_num}. Skipping segment.")
                    break

                # 2. Get Cards
                cards = driver.find_elements(By.CSS_SELECTOR, "div.grid > *")
                count = len(cards)
                
                limit_env = os.environ.get("LIMIT_ITEMS")
                limit = int(limit_env) if limit_env else count
                cards_to_process = cards[:limit]
                
                logger.info(f"Found {count} cards on page {page_num}. Processing {len(cards_to_process)} due to LIMIT_ITEMS.")

                if count == 0: break

                for idx, card in enumerate(cards_to_process):
                    try:
                        # Re-locate cards after navigation
                        grid = driver.find_element(By.CSS_SELECTOR, grid_selector)
                        current_cards = grid.find_elements(By.XPATH, "./*")
                        if idx >= len(current_cards): break
                        card = current_cards[idx]

                        # Basic Info from Card
                        title = "Unknown"
                        try:
                            title = card.find_element(By.CSS_SELECTOR, "h3.title-beneficio-filtro").text.strip()
                        except: pass

                        discount_text = "N/A"
                        try:
                            discount_text = card.find_element(By.CSS_SELECTOR, "div.descuento-filtro b").text.strip()
                        except: pass

                        # Get Detail URL - ROBUST
                        card_url = ""
                        try:
                            # Debug element
                            tag_type = card.tag_name
                            
                            if tag_type.lower() == 'a':
                                card_url = card.get_attribute("href")
                            else:
                                # Look for 'a' inside
                                links = card.find_elements(By.CSS_SELECTOR, "a")
                                if links:
                                    card_url = links[0].get_attribute("href")
                                else:
                                    # Log HTML for analysis
                                    continue
                        except Exception as e:
                            logger.warning(f"Could not find URL for card {idx}: {e}")
                            continue

                        # SKIP IF PROCESSED
                        if not card_url:
                            logger.warning(f"Empty URL for card {idx}")
                            continue
                            
                        if card_url in processed_urls:
                            logger.info(f"Skipping already processed URL: {card_url}")
                            continue
                        processed_urls.add(card_url)
                        
                        logger.info(f"Processing Card: {title} ({discount_text}) -> {card_url}")

                        # Deduplication check before clicking
                        item_hash = f"{title}_{discount_text}_{segment}"
                        if title != "Unknown" and item_hash in seen_hashes:
                            logger.info(f"[{idx+1}/{count}] {title} | SKIP (Duplicate)")
                            continue
                        
                        # Open in New Tab to preserve list state
                        driver.execute_script("window.open('');")
                        driver.switch_to.window(driver.window_handles[-1])
                        driver.get(card_url)

                        # Wait for Detail Page
                        try:
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.sucursal, h1.beneficio-compania, h2.beneficio-title")))
                            time.sleep(2)
                            
                            # Extract Advanced Data
                            details = extract_detail_data(driver.page_source, segment)
                            
                            final_title = title
                            if (final_title == "Unknown" or not final_title) and details.get("extracted_title"):
                                final_title = details["extracted_title"]

                            # FIX: Use banner discount if card grid has N/A
                            final_discount = discount_text
                            if (not final_discount or final_discount == "N/A") and details.get("discount_pct"):
                                final_discount = f"{details['discount_pct']}%"

                            # FIX: Consolidate locations into a single field instead of looping
                            location_val = details.get("location", "Varios")

                            item = {
                                "bank": "bancochile",
                                "title": final_title,
                                "discount_text": final_discount,
                                "recurrence": details["recurrence_text"],
                                "location": location_val,
                                "phone": details["phone"],
                                "conditions": details["conditions"],
                                "description": f"{final_title} - {final_discount} - {details['description']}",
                                "image_url": "", # Optional
                                "active_days": parse_days_from_text(details["recurrence_text"]), # Standardized day list
                                "discount_cap": details["discount_cap"],
                                "expiration_date": details["expiration_date"],
                                "url": driver.current_url,
                                "scraped_at": datetime.now().isoformat()
                            }
                            all_items.append(item)


                            
                            seen_hashes.add(item_hash)
                            logger.info(f"[{idx+1}/{count}] {title} | EXTRACTED")

                            # Limit check for testing
                            limit = os.getenv("LIMIT_ITEMS")
                            if limit and len(all_items) >= int(limit):
                                logger.info(f"Limit of {limit} items reached. Stopping.")
                                driver.close()
                                driver.switch_to.window(driver.window_handles[0])
                                return all_items

                        except Exception as e:
                            logger.warning(f"Error extracting details for {title}: {e}")
                        
                        # Close Tab and Return
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                        time.sleep(0.5)

                    except Exception as e:
                        logger.error(f"Error processing card index {idx}: {e}")
                        # Ensure we are on the main window
                        if len(driver.window_handles) > 1:
                            driver.close()
                            driver.switch_to.window(driver.window_handles[0])
                
                # 3. Next Page Logic
                try:
                    next_btns = driver.find_elements(By.ID, next_btn_id)
                    if not next_btns: break
                    
                    next_btn = next_btns[0]
                    parent_li = next_btn.find_element(By.XPATH, "./..")
                    if "disabled" in parent_li.get_attribute("class"): break
                    
                    driver.execute_script("arguments[0].click();", next_btn)
                    logger.info(f"Moving to page {page_num + 1}")
                    page_num += 1
                    time.sleep(5)
                except:
                    break
                    
    finally:
        driver.quit()
        
    return items if 'items' in locals() else all_items

if __name__ == "__main__":
    try:
        items = scrape_bancochile()
        if items:
            save_json(items)
            print(f"✅ Scraped {len(items)} items -> {JSON_OUTPUT_FILE}")
        else:
            logger.error("No items collected!")
    except Exception as e:
        logger.critical(f"FATAL ERROR: {e}")
