import os
import json
import time
import logging
import re
from datetime import datetime
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, Page, Locator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RipleyScraper:
    """
    Playwright scraper for Banco Ripley.
    Targets 'Comida & Delivery' and 'Restofans' (Antojos).
    """
    
    BASE_URL = "https://www.bancoripley.cl/beneficios-y-promociones"
    BANK_NAME = "ripley"
    
    def _get_playwright_args(self) -> List[str]:
        return [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--disable-gpu"
        ]

    def _normalize_date(self, text: str) -> Optional[str]:
        """Converts strings like 'Hasta el 28 de febrero' to '2026-02-28'."""
        if not text: return None
        
        months = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
        }
        
        try:
            # Handle '31 de marzo' or 'Hasta el 31 de marzo' or 'DD de mes'
            match = re.search(r"(\d{1,2})\s+de\s+([a-z]+)", text.lower())
            if match:
                day = match.group(1).zfill(2)
                month_name = match.group(2)
                month = months.get(month_name)
                if month:
                    year = "2026"
                    return f"{year}-{month}-{day}"
            
            # Simplified match for '31 marzo' or '31/03' if needed, but let's stick to 'de' first as it's the most common
        except:
            pass
        return None

    def _parse_cap(self, text: str) -> Optional[int]:
        """Extracts numeric value from strings like 'Tope de descuento $40.000'."""
        if not text: return None
        # Look for $ followed by numbers and dots
        match = re.search(r"Tope de descuento[:\s]*\$?([\d\.]+)", text, re.IGNORECASE)
        if match:
            clean_val = match.group(1).replace(".", "")
            try:
                return int(clean_val)
            except:
                return None
        return None

    def _extract_detail(self, page: Page, title: str) -> Dict:
        """Extracts data from the detail view/modal."""
        detail = {
            "expiration_date": None,
            "discount_cap": None,
            "conditions": ""
        }
        try:
            # Wait for any of the potential detail elements
            try:
                page.wait_for_selector(".boxConsumo, .legalDetalle, .purpleDark", timeout=8000)
            except:
                logger.warning(f"  Timeout waiting for detail content for {title}")

            # 1. Expiration Date
            # Try specific and generic selectors
            vigencia_selectors = [".boxConsumo p.purpleDark.s1", "p.purpleDark.s1", "p:has-text('Hasta')", "p:has-text('marzo')"]
            for sel in vigencia_selectors:
                if page.locator(sel).count() > 0:
                    for item in page.locator(sel).all():
                        try:
                            text = item.inner_text().strip()
                            if not text: continue
                            
                            normalized = self._normalize_date(text)
                            if normalized:
                                detail["expiration_date"] = normalized
                                break
                            
                            # Fallback if it contains 'Hasta'
                            if "Hasta" in text and not detail["expiration_date"]:
                                detail["expiration_date"] = text
                        except:
                            continue
                    
                if detail["expiration_date"] and "-" in str(detail["expiration_date"]): 
                    break 
            
            # 2. Clicking accordion for cap and conditions
            accordion_btn = "button.btn-legalDetalle"
            if page.locator(accordion_btn).count() > 0:
                try:
                    btn = page.locator(accordion_btn).first
                    btn.scroll_into_view_if_needed()
                    btn.click(force=True)
                    time.sleep(1.5) # Wait for expansion
                except Exception as e:
                    logger.warning(f"Could not click legal accordion for {title}: {e}")
            
            legal_selector = "p.legalDetalle"
            if page.locator(legal_selector).count() > 0:
                legal_texts = []
                for p in page.locator(legal_selector).all():
                    txt = p.inner_text().strip()
                    if txt: legal_texts.append(txt)
                
                if legal_texts:
                    full_legal = " / ".join(legal_texts)
                    detail["conditions"] = full_legal
                    detail["discount_cap"] = self._parse_cap(full_legal)

        except Exception as e:
            logger.warning(f"Error extracting detail for {title}: {e}")
        
        return detail

    def _extract_card(self, card_locator: Locator, page: Page, category: str) -> Optional[Dict]:
        """Extracts data from a single .new-card_beneficios element, opening detail view."""
        title = "Unknown Restaurant"
        try:
            # 1. Basic Info (from card)
            title_el = card_locator.locator(".title").first
            title_text = title_el.inner_text().strip()
            if not title_text or "Aprovecha" in title_text: return None
            title = title_text

            discount_text = ""
            if card_locator.locator(".dcto").count() > 0:
                discount_text = card_locator.locator(".dcto").first.inner_text().strip()

            active_days = []
            if card_locator.locator(".textInfo").count() > 0:
                day_text = card_locator.locator(".textInfo").first.inner_text().strip()
                if day_text: active_days.append(day_text)

            description = ""
            if card_locator.locator(".description").count() > 0:
                description = card_locator.locator(".description").first.inner_text().strip()
            
            image_url = ""
            if card_locator.locator("img.imgLogo").count() > 0:
                image_url = card_locator.locator("img.imgLogo").first.get_attribute("src")

            card_types_found = []
            for img in card_locator.locator(".imgTarjetas img").all():
                src = img.get_attribute("src") or ""
                if "mastercard-black" in src: card_types_found.append("Black")
                elif "debit" in src: card_types_found.append("Debito")
                elif "mastercard" in src: card_types_found.append("Mastercard")

            # 2. CLICK TO OPEN DETAIL
            logger.info(f"  Opening detail for {title}...")
            card_locator.scroll_into_view_if_needed()
            
            # Click specifically on 'Más información' link
            try:
                info_btn = card_locator.locator("text='Más información'").first
                if info_btn.count() > 0:
                    info_btn.click(force=True)
                else:
                    card_locator.click(force=True)
                
                # Wait for modal specifically by looking for a modal element
                page.wait_for_selector(".boxConsumo, .modal-content, p.purpleDark.s1", timeout=10000)
                time.sleep(2) 
                
                detail_info = self._extract_detail(page, title)
                
                # Take a debug screenshot for the first valid item
                if not os.path.exists("debug_detail.png"):
                    page.screenshot(path="debug_detail.png")
                    logger.info("  Saved debug_detail.png")

                # Close detail
                for _ in range(3):
                    page.keyboard.press("Escape")
                    time.sleep(0.5)
                
                # Try common close selectors including common Angular/Material ones
                close_selectors = [
                    ".close-modal", ".btn-close", "[aria-label='Close']", 
                    ".modal-header .close", "button:has-text('X')", 
                    ".mat-dialog-close-button", ".modal-dismiss"
                ]
                for sel in close_selectors:
                    try:
                        if page.locator(sel).count() > 0:
                            page.locator(sel).first.click()
                            time.sleep(0.5)
                    except: pass
                
                # Final attempt: click the backdrop (very common in SPAs)
                if page.locator(".btn-legalDetalle").count() > 0:
                    # Click far corner
                    page.mouse.click(10, 10)
                    time.sleep(0.5)
                    # Swipe down or something? No, let's just hope one of these worked
            except Exception as e:
                logger.warning(f"  Could not open detail for {title}: {e}")
                detail_info = {"expiration_date": None, "discount_cap": None, "conditions": " / ".join(card_types_found)}

            location = description
            if category == "Antojo":
                location = "Todas las sucursales"

            return {
                "bank": self.BANK_NAME,
                "title": title,
                "discount_text": discount_text,
                "recurrence": " / ".join(active_days),
                "location": location,
                "conditions": detail_info["conditions"] or " / ".join(card_types_found),
                "discount_cap": detail_info["discount_cap"],
                "expiration_date": detail_info["expiration_date"],
                "image_url": image_url,
                "url": self.BASE_URL,
                "scraped_at": datetime.utcnow().isoformat(),
                "category": category
            }

        except Exception as e:
            logger.warning(f"Error extracting card {title}: {e}")
            try: page.keyboard.press("Escape")
            except: pass
            return None

    def scrape(self) -> List[Dict]:
        all_data = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=self._get_playwright_args()
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            targets = [
                {"name": "Restofans", "category": "Restaurante", "selector": "#nav-restofans-tab"},
                {"name": "Comida & Delivery", "category": "Antojo", "selector": "#nav-beneficio12-tab"}
            ]

            try:
                for target in targets:
                    logger.info(f"Navigating to {self.BASE_URL} for {target['name']}...")
                    page.goto(self.BASE_URL, timeout=60000, wait_until="domcontentloaded")
                    time.sleep(6)
                    
                    # Find and click tab using direct selector
                    tab_selector = target["selector"]
                    tab_locator = page.locator(tab_selector).first
                    
                    if tab_locator.count() > 0:
                        logger.info(f"Clicking tab: {target['name']} ({tab_selector})")
                        tab_locator.click(force=True)
                        time.sleep(5)
                    else:
                        logger.error(f"Could not find tab for {target['name']} with selector {tab_selector}")
                        continue
                    
                    # Accept cookies if visible (can block clicks)
                    cookie_btn = page.locator("button:has-text('Aceptar')").first
                    if cookie_btn.count() > 0:
                        logger.info("Accepting cookies...")
                        cookie_btn.click(force=True)
                        time.sleep(1)

                    # Scroll to load all
                    logger.info("Scrolling to load all cards...")
                    for _ in range(8):
                        page.evaluate("window.scrollBy(0, 1000)")
                        time.sleep(1.5)

                    card_selector = ".new-card_beneficios"
                    cards_count = page.locator(card_selector).count()
                    logger.info(f"Found {cards_count} items in {target['name']}.")

                    processed_titles = set()
                    
                    # We iterate by title/index, but re-locate each time
                    for i in range(cards_count):
                        # Re-ensure we are on the main page/tab if needed
                        # (If a previous card left us on a detail page)
                        if page.url != self.BASE_URL or page.locator(card_selector).count() == 0:
                            logger.info("  Re-navigating to main page...")
                            page.goto(self.BASE_URL, timeout=60000, wait_until="domcontentloaded")
                            time.sleep(5)
                            # Re-click tab
                            # Re-click tab
                            try:
                                tab_sel = target["selector"]
                                page.locator(tab_sel).click(force=True)
                                time.sleep(4)
                            except:
                                logger.warning(f"Could not re-click tab {target['name']}")
                            # Re-scroll
                            page.evaluate("window.scrollTo(0, 0)")
                            for _ in range(3): 
                                page.evaluate("window.scrollBy(0, 1000)")
                                time.sleep(1)

                        # Re-locate the i-th card
                        cards = page.locator(card_selector)
                        if i >= cards.count(): break
                        
                        card = cards.nth(i)
                        item = self._extract_card(card, page, target["category"])
                        
                        if item:
                            unique_key = f"{item['title']}_{item.get('location', '')}"
                            if unique_key not in processed_titles:
                                all_data.append(item)
                                processed_titles.add(unique_key)
                                logger.info(f"  [{len(processed_titles)}/{cards_count}] Extracted: {item['title']}")
                        
                        # After extraction, ensure we go back to the list
                        # The _extract_card tries to press Escape, but if it fails:
                        if page.url != self.BASE_URL or page.locator(card_selector).count() == 0:
                            # Try to find a back button
                            back_selectors = ["text='< '", "text='Volver'", ".back-button"]
                            found_back = False
                            for b in back_selectors:
                                if page.locator(b).count() > 0:
                                    page.locator(b).first.click()
                                    time.sleep(2)
                                    found_back = True
                                    break
                            
                            if not found_back:
                                logger.warning("  Could not find back button, re-navigating...")
                                # The next loop iteration will handle re-navigation
                
            except Exception as e:
                logger.error(f"Scraping failed: {e}")
                try: page.screenshot(path="ripley_error.png")
                except: pass
            finally:
                try:
                    browser.close()
                except Exception as e:
                    logger.warning(f"Error closing browser (ignoring): {e}")
        
        return all_data

def main():
    scraper = RipleyScraper()
    data = scraper.scrape()
    
    # Save even if scrape partially fails or close fails
    output_path = os.path.join(os.path.dirname(__file__), "json", "ripley.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    logger.info(f"✅ Saved {len(data)} items to {output_path}")

if __name__ == "__main__":
    main()
