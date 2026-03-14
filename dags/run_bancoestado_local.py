import logging
import json
import time
import os
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Setup Logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), 'json', 'bancoestado.json')

def get_local_driver():
    options = Options()
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)

def scrape_bancoestado_local(limit_items: int = 0):
    driver = None
    all_items = []
    
    # Allow limiting for debug

    
    try:
        driver = get_local_driver()
        url = "https://www.bancoestado.cl/content/bancoestado-public/cl/es/home/home/todosuma---bancoestado-personas/un-mes-de-sabores---bancoestado-personas.html#/"
        logger.info(f"Navegando a {url}...")
        driver.get(url)
        
        wait = WebDriverWait(driver, 20)
        
        logger.info("Esperando carga de tarjetas...")
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'card-beneficios')]")))
            time.sleep(5) 
        except:
            logger.warning("Timeout esperando tarjetas.")
            return []

        cards = driver.find_elements(By.XPATH, "//div[contains(@class, 'card-beneficios')]")
        logger.info(f"Encontradas {len(cards)} tarjetas.")
        
        seen_ids = set()
        
        for index, card in enumerate(cards):
            if limit_items > 0 and len(all_items) >= limit_items:
                logger.info(f"Limit alcanzado ({limit_items}).")
                break
                
            try:
                # 1. Attributes
                title = card.get_attribute("data-name")
                discount_raw = card.get_attribute("data-tarjeta")
                validity_raw = card.get_attribute("data-oferta")
                
                if not title:
                    try:
                        title = card.find_element(By.CLASS_NAME, "title").text
                    except: pass
                
                if not title: continue
                if "priceless" in title.lower(): continue

                # 2. Location from subfiltros
                subfiltros = card.get_attribute("data-subfiltros")
                location = "Regiones" 
                if subfiltros:
                    try:
                        sf = json.loads(subfiltros)
                        if "zona" in sf and sf["zona"]:
                            location = ", ".join(sf["zona"])
                    except: pass
                
                # 3. Clean basic fields
                discount_val = discount_raw
                validity_val = validity_raw
                if (not discount_val or not re.search(r'\d', discount_val)) and (validity_val and '%' in validity_val):
                    discount_val = validity_val
                    validity_val = "Todos los días"
                
                # 4. Click for details (Tope / Expiration)
                discount_cap = None
                expiration_date = None
                
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
                    time.sleep(0.5)
                    
                    driver.execute_script("arguments[0].click();", card)
                    
                    # Simple wait for modal animation/render
                    # Explicit waits fail because modal elements are often hidden/duplicated in DOM
                    time.sleep(2)
                    
                    # Use page_source directly as it proved reliable for finding text even if element finding fails
                    modal_text = ""
                    try:
                        # Try to get text from visible modal if possible (best effort)
                        for m in driver.find_elements(By.CLASS_NAME, "modal-body"):
                            if m.is_displayed():
                                modal_text = m.text
                                break
                    except: pass
                    
                    # Regex Tope
                    # Try modal_text first, then generic page_source
                    
                    # Regex Tope
                    # Try modal_text first, then generic page_source
                    source_to_check = modal_text if len(modal_text) > 20 else driver.page_source

                    tope_match = re.search(r'Tope(?:\s+de\s+descuento)?\s*\$([\d.]+)', source_to_check, re.IGNORECASE)
                    if not tope_match and source_to_check != driver.page_source:
                         tope_match = re.search(r'Tope(?:\s+de\s+descuento)?\s*\$([\d.]+)', driver.page_source, re.IGNORECASE)

                    if tope_match:
                         discount_cap = int(tope_match.group(1).replace('.', ''))
                    
                    # Regex Expiration
                    exp_match = re.search(r'Válid[ao] hasta (?:el\s+)?(\d{1,2}\s+de\s+[a-zA-Z]+\s+(?:de\s+)?\d{4})', source_to_check, re.IGNORECASE)
                    if not exp_match and source_to_check != driver.page_source:
                         exp_match = re.search(r'Válid[ao] hasta (?:el\s+)?(\d{1,2}\s+de\s+[a-zA-Z]+\s+(?:de\s+)?\d{4})', driver.page_source, re.IGNORECASE)

                    if exp_match:
                        raw_date = exp_match.group(1)
                        mon_map = {
                            "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
                            "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
                        }
                        try:
                            parts = raw_date.lower().replace(' de ', ' ').split()
                            day = int(parts[0])
                            year = int(parts[-1])
                            month_str = next((m for m in mon_map if m in raw_date.lower()), None)
                            if month_str:
                                month = mon_map[month_str]
                                expiration_date = f"{year}-{month:02d}-{day:02d}"
                        except Exception as e:
                             pass

                    # Close Modal
                    ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                    time.sleep(1) 
                    
                except Exception as e:
                    logger.warning(f"Warning extracting details for {title}: {e}")
                    try:
                        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                    except: pass

                
                item = {
                    "bank": "bancoestado",
                    "title": title.strip(),
                    "discount_text": discount_val,
                    "validity": validity_val,
                    "location": location,
                    "discount_cap": discount_cap,
                    "expiration_date": expiration_date,
                    "scraped_at": datetime.now().isoformat()
                }
                
                if title not in seen_ids:
                    all_items.append(item)
                    seen_ids.add(title)
                    logger.info(f"Scraped: {title} | Cap: {discount_cap} | Exp: {expiration_date}")

            except Exception as e:
                logger.error(f"Error procesando card {index}: {e}")

        logger.info(f"Total items extraídos: {len(all_items)}")
        
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_items, f, indent=4, ensure_ascii=False)
            logger.info(f"Guardado en {OUTPUT_FILE}")
            
        return all_items

    except Exception as e:
        logger.error(f"Error global: {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    scrape_bancoestado_local()
