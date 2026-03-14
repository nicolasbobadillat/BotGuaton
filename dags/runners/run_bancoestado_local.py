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

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), '..', 'json', 'bancoestado.json')

def get_local_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    return webdriver.Chrome(options=options)

def scrape_bancoestado_local():
    driver = None
    all_items = []
    
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
        except Exception as e:
            logger.warning(f"Timeout esperando tarjetas: {e}")
            return []

        cards = driver.find_elements(By.XPATH, "//div[contains(@class, 'card-beneficios')]")
        logger.info(f"Encontradas {len(cards)} tarjetas.")
        
        seen_ids = set()
        
        for index, card in enumerate(cards):
            try:
                # Basic Extract
                title = card.get_attribute("data-name")
                if not title:
                    try: title = card.find_element(By.CLASS_NAME, "title").text
                    except: pass
                
                if not title: continue
                if "priceless" in title.lower(): continue
                
                logger.info(f"Procesando: {title}")
                
                discount_val = card.get_attribute("data-tarjeta")
                validity_val = card.get_attribute("data-oferta")
                
                if (not discount_val or not re.search(r'\d', discount_val)) and (validity_val and '%' in validity_val):
                    discount_val = validity_val
                    validity_val = "Todos los días"
                
                # Default Location
                location = "Regiones"
                subfiltros = card.get_attribute("data-subfiltros")
                if subfiltros:
                    try:
                        sf = json.loads(subfiltros)
                        if "zona" in sf and sf["zona"]:
                            location = " / ".join(sf["zona"])
                    except: pass

                # Button / Modal Extractor
                caps_val = None
                exp_val = None
                
                try:
                    # Scroll into view
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
                    time.sleep(0.1)
                    
                    btn = card.find_element(By.CLASS_NAME, "button-card-ubicacion")
                    modal_id = btn.get_attribute("data-modal-id")
                    driver.execute_script("arguments[0].click();", btn)
                    
                    if modal_id:
                        wait.until(EC.visibility_of_element_located((By.ID, modal_id)))
                        time.sleep(0.1)
                        modal = driver.find_element(By.ID, modal_id)
                    else:
                        wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "modal-content")))
                        time.sleep(0.1)
                        modal = driver.find_element(By.CLASS_NAME, "modal-content")
                    
                    # Wait for location content to load inside modal
                    try:
                        WebDriverWait(modal, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul li")))
                        time.sleep(0.1)
                    except: pass

                    
                    # Parsers
                    ps = modal.find_elements(By.TAG_NAME, "p")
                    for p in ps:
                        txt = p.text.lower()
                        if "tope" in txt or "máximo" in txt:
                            try:
                                m = re.search(r'\$([\d\.]+)', txt)
                                if m: caps_val = int(m.group(1).replace(".", ""))
                            except: pass
                        if "hasta el" in txt:
                            exp_val = txt
                            
                    # Locaciones (ul/li)
                    lis = modal.find_elements(By.CSS_SELECTOR, "ul li")
                    if lis:
                        locs = [li.text.strip() for li in lis if li.text.strip()]
                        if locs:
                            location = " | ".join(locs)
                            
                    # Cerrar modal
                    close_btn = modal.find_element(By.CLASS_NAME, "modal-close")
                    driver.execute_script("arguments[0].click();", close_btn)
                    time.sleep(0.5)
                except Exception as e:
                    logger.debug(f"Sin modal o error extra en {title}: {e}")
                    # Recovery
                    try:
                        driver.execute_script("document.querySelectorAll('.modal-close').forEach(b => b.click())")
                        time.sleep(0.5)
                    except: pass

                item = {
                    "bank": "bancoestado",
                    "title": title.strip(),
                    "discount_text": discount_val,
                    "validity": validity_val,
                    "location": location,
                    "discount_cap": caps_val,
                    "expiration_date": exp_val,
                    "scraped_at": datetime.now().isoformat()
                }
                
                if title not in seen_ids:
                    all_items.append(item)
                    seen_ids.add(title)

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
