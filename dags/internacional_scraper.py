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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
START_URL = "https://beneficios.internacional.cl/categoria/seccioneshome/restaurantes"



def setup_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    grid_url = os.getenv('SELENIUM_GRID_URL', 'http://selenium_chrome:4444/wd/hub')
    logger.info(f"Connecting to Selenium Grid at {grid_url}...")
    
    try:
        driver = webdriver.Remote(command_executor=grid_url, options=options)
        driver.set_window_size(1920, 1080)
        return driver
    except Exception as e:
        logger.error(f"Failed to connect to Selenium Grid: {e}")
        raise



def scrape_internacional():
    driver = setup_driver()
    all_items = []
    seen_ids = set()
    wait = WebDriverWait(driver, 10)

    try:
        logger.info(f"Navigating to {START_URL}...")
        driver.get(START_URL)
        time.sleep(5) 

        # 1. Find all cards first
        # Snippet: <div class="product-item ...">
        # We need to re-find them in loop, but let's count first
        card_xpath = "//div[contains(@class, 'product-item')]"
        total_cards_initial = len(driver.find_elements(By.XPATH, card_xpath))
        logger.info(f"Found approx {total_cards_initial} cards.")

        for i in range(total_cards_initial):
            try:
                # Re-find to avoid stale
                cards = driver.find_elements(By.XPATH, card_xpath)
                if i >= len(cards): break
                card = cards[i]

                # Extract basic info from card list view (Image, maybe ID from onclick)
                # Snippet: onclick="verDetalles('66', 'Ramblas...')"
                # Cloudflare wrapper: if (!window.__cfRLUnblockHandlers) return false; verDetalles(...)
                link_btn = card.find_element(By.XPATH, ".//a[contains(@onclick, 'verDetalles')]")
                onclick_text = link_btn.get_attribute("onclick")
                logger.info(f"Raw onclick: {onclick_text}")
                
                # Parse ID/Title from onclick
                # Regex needs to be robust against newlines and spacing
                match = re.search(r"verDetalles\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", onclick_text)
                
                card_id = "N/A"
                card_title_raw = "N/A"
                if match:
                    card_id = match.group(1)
                    card_title_raw = match.group(2)
                else:
                    # Fallback ID to ensure we don't skip unique items just because parsing failed
                    card_id = f"fallback_{i}"
                    card_title_raw = f"Item {i}"

                logger.info(f"Processing ID: {card_id} | {card_title_raw}...")

                # Dedupe
                if card_id in seen_ids:
                    continue
                seen_ids.add(card_id)

                # Strategy: Force Direct JS Call to bypass Cloudflare 'if (!window...)' check
                # standard .click() fails silently because the onclick text returns false.
                
                logger.info(f"Invoking properties: ID={card_id}, Title={card_title_raw}")
                js_success = False

                if match:
                    try:
                        # Direct call: verDetalles('66', 'Ramblas...')
                        # Escaping quotes just in case
                        safe_title = card_title_raw.replace("'", "\\'") 
                        driver.execute_script(f"verDetalles('{card_id}', '{safe_title}')")
                        js_success = True
                        logger.info("Executed verDetalles() via JS.")
                    except Exception as e:
                        logger.warning(f"Direct JS execution failed: {e}")

                if not js_success:
                    # Fallback to standard click if regex failed or JS crashed
                    logger.info("Falling back to standard click...")
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link_btn)
                    time.sleep(1)
                    try:
                        link_btn.click()
                    except:
                        driver.execute_script("arguments[0].click();", link_btn)
                
                # WAIT for modal or content update
                time.sleep(5)

                try:
                    # Capture FULL DOM text (including hidden elements) using textContent
                    # Selenium .text ignores hidden/collapsed elements. .get_attribute("textContent") gets everything.
                    body_el = driver.find_element(By.TAG_NAME, "body")
                    full_content = body_el.get_attribute("textContent")
                    
                    # Also capture innerHTML for debugging if needed, but textContent is cleaner for regex
                    # full_html = body_el.get_attribute("innerHTML")

                    # Log to confirm we have data
                    if "Condiciones" in full_content:
                        logger.info("Found 'Condiciones' in content.")

                    # Regex Extraction
                    discount = "N/A"
                    dct_match = re.search(r'(\d{1,2})\s?%\s*(de\s*)?(dcto|descuento|off)', full_content, re.IGNORECASE)
                    if dct_match:
                        discount = f"{dct_match.group(1)}%"
                    
                    # 1. Expiration Date
                    # Patterns: "al 25 de febrero de 2026", "a 25 de febrero de 2026", "28 de febrero del año 2026", "25 de febrero 2026"
                    expiration_date = None
                    exp_match = re.search(r'(?:al|a|desde el|del)\s+(\d{1,2})\s+de\s+([a-zA-Z]+)(?:\s+de\s+|\s+del\s+año\s+|\s+)?(202\d)', full_content, re.IGNORECASE)
                    if exp_match:
                        day = exp_match.group(1).zfill(2)
                        month_name = exp_match.group(2).lower()
                        year = exp_match.group(3)
                        months = {
                            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
                            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
                            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
                        }
                        month = months.get(month_name)
                        if month:
                            expiration_date = f"{year}-{month}-{day}"

                    # Conditions
                    conditions = "N/A"
                    cond_match = re.search(r'(Términos y condiciones)([\s\S]{10,2000})', full_content, re.IGNORECASE)
                    if cond_match:
                        conditions = cond_match.group(2)[:1000].strip()

                    # 2. Recurrence (Active Days)
                    # Patterns: "lunes, martes y miércoles", "de lunes a sábado"
                    recurrence = ""
                    rec_match = re.search(r'(lunes a sábado|lunes a viernes|lunes a domingo|todos los días(?!\s+(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bados?|domingos?))|lunes, martes y mi[eé]rcoles|lunes, martes, mi[eé]rcoles|lunes y martes)', conditions, re.IGNORECASE)
                    if rec_match:
                        recurrence = rec_match.group(1).capitalize()
                    else:
                        # Fallback: look for specific days in conditions
                        days_found = re.findall(r'(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bados?|domingos?)', conditions, re.IGNORECASE)
                        if days_found:
                            recurrence = ", ".join(sorted(list(set(d.capitalize() for d in days_found))))
                        
                        if not recurrence:
                            # Fallback to full_content
                            fb_match = re.search(r'(lunes a sábado|lunes a viernes|lunes a domingo|todos los días(?!\s+(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bados?|domingos?))|lunes, martes y mi[eé]rcoles|lunes, martes, mi[eé]rcoles|lunes y martes)', full_content, re.IGNORECASE)
                            if fb_match: recurrence = fb_match.group(1).capitalize()

                    # 3. Discount Cap
                    # Pattern: "Tope máximo de descuento de $40.000"
                    discount_cap = None
                    cap_match = re.search(r'Tope\s+máximo\s+de\s+descuento\s+de\s+\$?\s*([\d\.]+)', full_content, re.IGNORECASE)
                    if cap_match:
                        discount_cap = int(cap_match.group(1).replace(".", ""))

                    # 4. Address
                    # Pattern: "en [Restaurante], [Dirección], [Comuna]"
                    address = ""
                    # The SQL already does a good job, but let's try Python too
                    addr_match = re.search(rf'en\s+{re.escape(card_title_raw)}[,\s]+([^.]+)', full_content, re.IGNORECASE)
                    if addr_match:
                        address = addr_match.group(1).strip()

                    item = {
                        "bank": "internacional",
                        "title": card_title_raw,
                        "discount_text": discount,
                        "expiration_date": expiration_date,
                        "recurrence": recurrence,
                        "discount_cap": discount_cap,
                        "location": address,
                        "conditions": conditions,
                        "debug_dump": full_content[:1500],
                        "scraped_at": datetime.now().isoformat()
                    }
                    all_items.append(item)
                    logger.info(f"Extracted: {card_title_raw} | Discount={discount} | Exp={expiration_date}")

                except Exception as e:
                    logger.error(f"Extraction error: {e}")

                # CLOSE MODAL (Try ESC)
                try:
                    webdriver.ActionChains(driver).send_keys('\ue00c').perform() 
                    time.sleep(1)
                except: 
                    pass

            except Exception as e:
                logger.error(f"Error on card {i}: {e}")
                # Try to recover navigation
                try: webdriver.ActionChains(driver).send_keys('\ue00c').perform()
                except: pass

        logger.info(f"Total items extracted: {len(all_items)}")
        return all_items

    finally:
        driver.quit()

if __name__ == "__main__":
    data = scrape_internacional()
    if data:
        out_path = os.path.join(os.path.dirname(__file__), "json", "internacional.json")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ Scraped {len(data)} items -> {out_path}")

