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
from selenium.webdriver import ActionChains

# Setup Logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), '..', 'json', 'internacional.json')

def get_local_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)

def normalize_date(text: str):
    if not text: return None
    months = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
    }
    exp_match = re.search(r'(?:al|a|desde el|del)\s+(\d{1,2})\s+de\s+([a-zA-Z]+)(?:\s+de\s+|\s+del\s+año\s+|\s+)?(202\d)', text, re.IGNORECASE)
    if exp_match:
        day = exp_match.group(1).zfill(2)
        month_name = exp_match.group(2).lower()
        year = exp_match.group(3)
        month = months.get(month_name)
        if month:
            return f"{year}-{month}-{day}"
    return None

def scrape_internacional_local():
    driver = None
    all_items = []
    
    try:
        driver = get_local_driver()
        url = "https://beneficios.internacional.cl/categoria/seccioneshome/restaurantes"
        logger.info(f"Navegando a {url}...")
        driver.get(url)
        
        wait = WebDriverWait(driver, 20)
        time.sleep(5)
        
        card_xpath = "//div[contains(@class, 'product-item')]"
        cards = driver.find_elements(By.XPATH, card_xpath)
        total_cards = len(cards)
        logger.info(f"Encontradas {total_cards} tarjetas.")
        
        seen_ids = set()
        
        for i in range(total_cards):
            try:
                cards = driver.find_elements(By.XPATH, card_xpath)
                if i >= len(cards): break
                card = cards[i]
                
                link_btn = card.find_element(By.XPATH, ".//a[contains(@onclick, 'verDetalles')]")
                onclick_text = link_btn.get_attribute("onclick")
                match = re.search(r"verDetalles\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", onclick_text)
                
                if match:
                    card_id = match.group(1)
                    title = match.group(2)
                else:
                    card_id = f"fallback_{i}"; title = f"Item {i}"
                
                if card_id in seen_ids: continue
                seen_ids.add(card_id)

                logger.info(f"Procesando: {title}...")
                safe_title = title.replace("'", "\\'")
                driver.execute_script(f"verDetalles('{card_id}', '{safe_title}')")
                time.sleep(4)
                
                body_el = driver.find_element(By.TAG_NAME, "body")
                full_content = body_el.get_attribute("textContent")
                
                # 1. Discount
                discount = "N/A"
                dct_match = re.search(r'(\d{1,2})\s?%\s*(de\s*)?(dcto|descuento|off)', full_content, re.IGNORECASE)
                if dct_match: discount = f"{dct_match.group(1)}%"
                
                # 2. Expiration Date
                expiration_date = normalize_date(full_content)
                
                # Conditions
                conditions = ""
                cond_match = re.search(r'(Términos y condiciones)([\s\S]{10,2000})', full_content, re.IGNORECASE)
                if cond_match: conditions = cond_match.group(2)[:1000].strip()

                # 3. Recurrence
                recurrence = ""
                # First try to find recurrence in the specific conditions block to avoid background card cross-contamination
                rec_match = re.search(r'(lunes a sábado|lunes a viernes|lunes a domingo|todos los días(?!\s+(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bados?|domingos?))|lunes, martes y mi[eé]rcoles|lunes, martes, mi[eé]rcoles|lunes y martes)', conditions, re.IGNORECASE)
                if rec_match: recurrence = rec_match.group(1).capitalize()
                else:
                    days_found = re.findall(r'(lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bados?|domingos?)', conditions, re.IGNORECASE)
                    if days_found:
                        recurrence = ", ".join(sorted(list(set(d.capitalize() for d in days_found))))
                    
                    # Fallback to full_content only if no explicit days found in conditions
                    if not recurrence:
                        fb_match = re.search(r'(lunes a sábado|lunes a viernes|lunes a domingo|todos los días(?!\s+(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bados?|domingos?))|lunes, martes y mi[eé]rcoles|lunes, martes, mi[eé]rcoles|lunes y martes)', full_content, re.IGNORECASE)
                        if fb_match: recurrence = fb_match.group(1).capitalize()

                # 4. Cap
                discount_cap = None
                cap_match = re.search(r'Tope\s+máximo\s+de\s+descuento\s+de\s+\$?\s*([\d\.]+)', full_content, re.IGNORECASE)
                if cap_match: discount_cap = int(cap_match.group(1).replace(".", ""))

                # 5. Address
                address = ""
                addr_match = re.search(rf'en\s+{re.escape(title)}[,\s]+([^.]+)', full_content, re.IGNORECASE)
                if addr_match: address = addr_match.group(1).strip()
                
                item = {
                    "bank": "internacional",
                    "title": title,
                    "discount_text": discount,
                    "expiration_date": expiration_date,
                    "recurrence": recurrence,
                    "discount_cap": discount_cap,
                    "location": address,
                    "conditions": conditions,
                    "scraped_at": datetime.now().isoformat()
                }
                all_items.append(item)
                logger.info(f"   -> Exp: {expiration_date} | Cap: {discount_cap}")

                ActionChains(driver).send_keys('\ue00c').perform()
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error en card {i}: {e}")
                try: ActionChains(driver).send_keys('\ue00c').perform()
                except: pass

        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_items, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ Guardados {len(all_items)} productos.")
    except Exception as e:
        logger.error(f"Error global: {e}")
    finally:
        if driver: driver.quit()

if __name__ == "__main__":
    scrape_internacional_local()
