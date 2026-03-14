import os
import re
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional, Set
from bs4 import BeautifulSoup


from playwright.sync_api import sync_playwright, Page, Locator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BancoScraper:
    """
    Scraper for Banco Falabella discounts using Playwright.
    Targets specific categories (Restaurantes, Antojos) and extracts deep details.
    Uses Click-and-Back SPA navigation for interactive cards.

    Version: 2.1.0 (Feb 2026)
    - Robustness: Added Smart Slug Generation (Priority candidates).
    - Robustness: Recursive Modal Fallback for hidden summaries.
    - Specific Fixes: Hardcoded Cabrera override & Dual Ampersand strategy.
    """
    
    # Updated targets
    TARGET_CATEGORIES = [
        {"url": "https://www.bancofalabella.cl/descuentos/restaurantes", "category": "Restaurante"},
        {"url": "https://www.bancofalabella.cl/descuentos/antojos", "category": "Antojo"},
        {"url": "https://www.bancofalabella.cl/descuentos/sabores", "category": "Antojo"},
        {"url": "https://www.bancofalabella.cl/descuentos/comida-y-delivery", "category": "Antojo"},
        {"url": "https://www.bancofalabella.cl/descuentos/comida-delivery", "category": "Antojo"},
        {"url": "https://www.bancofalabella.cl/descuentos/delivery", "category": "Antojo"},
        {"url": "https://www.bancofalabella.cl/descuentos/panoramas", "category": "Restaurante"}
    ]
    
    # Names to always check (even if not found in categories)
    EXTRA_NAMES = [
        "Vapiano",
        "Santa Brasa", 
        "Pistacho",
        "Pistacho Osorno",
        "Comedor Pelícano",
        "Muu Grill",
        "La Caperucita y el Lobo",
        "Tigre Bravo",
        "Tanta",
        "Badass Parque Arauco",
        "Da Salvatore Trattoria & Pizzeria",
        "La Fabrica"
    ]
    
    # Specific URLs provided by the user that might be missed by discovery
    MANUAL_URLS = [
        "https://www.bancofalabella.cl/descuentos/detalle/chilis",
        "https://www.bancofalabella.cl/descuentos/detalle/martes-de-borderio-krossbar",
        "https://www.bancofalabella.cl/descuentos/detalle/la-tabla-borderio",
        "https://www.bancofalabella.cl/descuentos/detalle/mccombodelmes",
        "https://www.bancofalabella.cl/descuentos/detalle/tommybeans",
        "https://www.bancofalabella.cl/descuentos/detalle/barrio-chicken",
        "https://www.bancofalabella.cl/descuentos/detalle/papa-johns",
        "https://www.bancofalabella.cl/descuentos/detalle/biancolatte",
        "https://www.bancofalabella.cl/descuentos/detalle/le-vice",
        "https://www.bancofalabella.cl/descuentos/detalle/mr-pretzel",
        "https://www.bancofalabella.cl/descuentos/detalle/dominos-pizza",
        "https://www.bancofalabella.cl/descuentos/detalle/dulceria-apoquindo",
        "https://www.bancofalabella.cl/descuentos/detalle/leonidas",
        "https://www.bancofalabella.cl/descuentos/detalle/volka"
    ]
    
    def __init__(self):
        self.bank_name = "banco_falabella"
    
    def _get_playwright_args(self) -> List[str]:
        return [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--disable-gpu"
        ]

    def _scroll_to_bottom(self, page: Page):
        """Scrolls the page to load all lazy-loaded items with more patience."""
        logger.info("Scrolling to load dynamic content (Relaxed pace)...")
        previous_height = page.evaluate("document.body.scrollHeight")
        for i in range(100): # Increased scroll attempts
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(3.5) # Increased wait for network/rendering
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == previous_height:
                # Double check: sometimes it takes a bit more for the loader to trigger
                time.sleep(2)
                if page.evaluate("document.body.scrollHeight") == new_height:
                    break
            previous_height = new_height
            if i % 10 == 0:
                logger.info(f"  Scroll iteration {i}...")

    def _extract_common_fields(self, page: Page, url: str) -> Dict:
        """Extract fields common to both Restaurante and Antojo."""
        data = {
            "url": url,
            "extraction_timestamp": datetime.utcnow().isoformat(),
            "title": "",
            "discount_pct": "",
            "discount_type": "",
            "active_days": [],
            "validity": "",
            "modality": "",
            "location": "",
            "card_types": [],
            "discount_details": "",
            "conditions": "",
            "address": "",
            "steps": "",
            "raw_text": ""
        }
        
        try:
            # --- TITLE ---
            try:
                title_el = page.locator("h1")
                if title_el.count() > 0:
                    data["title"] = title_el.first.inner_text().strip()
            except:
                pass
            
            # --- DISCOUNT PERCENTAGE ---
            try:
                pct_el = page.locator("text=/\\d+%/").first
                if pct_el.count() > 0:
                    data["discount_pct"] = pct_el.inner_text().strip()
            except:
                pass
            
            # --- DISCOUNT TYPE ---
            try:
                type_el = page.locator("[class*='CardImage_text-bottom'], [class*='text-red']").first
                if type_el.count() > 0:
                    data["discount_type"] = type_el.inner_text().strip()
            except:
                pass
                
            # --- ACTIVE DAYS ---
            try:
                days_locator = page.locator("[class*='DiscountDays_wrapper-day'][class*='isActive']")
                if days_locator.count() > 0:
                    for wrapper in days_locator.all():
                        day_text = wrapper.inner_text().strip()
                        if day_text:
                            data["active_days"].append(day_text)
            except:
                pass
            
            # --- VALIDITY ---
            try:
                val_el = page.locator("text=/Válido hasta/").first
                if val_el.count() > 0:
                    validity_text = val_el.inner_text()
                    data["validity"] = validity_text.replace("Válido hasta", "").strip()
            except:
                pass
            
            # --- MODALITY ---
            try:
                modality_img = page.locator("img[alt*='modalidad']").first
                if modality_img.count() > 0:
                    data["modality"] = modality_img.get_attribute("alt").replace("modalidad", "").strip()
            except:
                pass
            
            # --- LOCATION ---
            try:
                loc_el = page.locator("[class*='mode-tag--region'] span").first
                if loc_el.count() > 0:
                    data["location"] = loc_el.inner_text().strip()
            except:
                pass
            
            # --- CARD TYPES ---
            try:
                card_wrapper = page.locator("[class*='CardInfo_wrapper-images'] img")
                if card_wrapper.count() > 0:
                    for img in card_wrapper.all():
                        alt = img.get_attribute("alt")
                        if alt:
                            data["card_types"].append(alt.strip())
            except:
                pass
            
            # --- DETAIL BANNER TEXT ---
            try:
                detail_banner = page.locator("[class*='DetailBanner_wrapper-content']")
                if detail_banner.count() > 0:
                    data["steps"] = detail_banner.first.inner_text()
                    
                    paragraphs = page.locator("[class*='DetailBanner_wrapper-content'] [class*='ParagraphMarkdown_container']").all()
                    for p in paragraphs:
                        p_text = p.inner_text().strip()
                        if "%" in p_text and "dcto" in p_text.lower():
                            data["discount_details"] = p_text
                        elif "Condiciones:" in p_text:
                            data["conditions"] = p_text
                        elif "Dirección:" in p_text or "*Dirección" in p_text:
                            data["address"] = p_text.replace("*Dirección:", "").replace("Dirección:", "").strip()
            except:
                pass
                
        except Exception as e:
            logger.warning(f"Error extracting common fields for {url}: {e}")
            data["error"] = str(e)
            
        return data

    def _extract_restaurante_locations(self, page: Page, data: Dict):
        """
        RESTAURANTE-ONLY: Deep location/address extraction.
        Handles 4 formats of location data from the detail banner.
        This logic is isolated so Antojo changes never affect it.
        """
        try:
            detail_banner = page.locator("[class*='DetailBanner_wrapper-content']")
            if detail_banner.count() == 0:
                return
            
            soup = BeautifulSoup(detail_banner.first.inner_html(), 'html.parser')
            uls = soup.find_all('ul', class_=lambda x: x and 'ListItemsMarkdown' in x)
            
            best_ul = None
            max_score = -5
            
            INSTRUCTION_KEYWORDS = ['menciona', 'dicta', 'indica', 'código', 'cupón', 'rut', 'caja', 'pagar', 'solicita', 'reserva', 'paso', 'sigue', 'usa ', 'listo!', 'disfruta']
            LOCATION_KEYWORDS = ['mall', 'av.', 'avenida', 'calle', 'piso', 'local', 'boulevard', 'plaza', 'esquina', 'region', 'región', 'km', 'parque', 'paseo', 'dirección', 'santiago', 'providencia', 'las condes', 'vitacura', 'viña', 'concepción', 'temuco', 'antofagasta']

            for candidate_ul in uls:
                items_text = []
                for li in candidate_ul.find_all('li'):
                    p_tag = li.find('p')
                    text = p_tag.get_text(strip=True) if p_tag else li.get_text(strip=True)
                    items_text.append(text)
                
                if not items_text:
                    continue
                
                loc_score = 0
                instr_score = 0
                for item in items_text:
                    text_lower = item.lower()
                    if any(k in text_lower for k in INSTRUCTION_KEYWORDS):
                        instr_score += 1
                    if any(k in text_lower for k in LOCATION_KEYWORDS):
                        loc_score += 1
                
                score = loc_score - instr_score
                if score > max_score:
                    max_score = score
                    best_ul = candidate_ul

            ul = best_ul if max_score >= -1 else None
            
            if ul:
                raw_items = [li.get_text(strip=True) for li in ul.find_all('li') if li.get_text(strip=True)]
                has_colon_format = any(':' in item for item in raw_items)
                
                loc_details = []
                if has_colon_format:
                    for text in raw_items:
                        parts = text.split(':', 1)
                        if len(parts) == 2:
                            loc_details.append({"location": parts[0].strip(), "days": parts[1].strip()})
                        else:
                            loc_details.append({"location": text.strip(), "days": None})
                else:
                    DAY_KEYWORDS = ['lunes', 'martes', 'miércoles', 'miercoles', 'jueves',
                                    'viernes', 'sábado', 'sabado', 'domingo', 'todos los']
                    locations = []
                    shared_days = None
                    for text in raw_items:
                        if any(kw in text.lower() for kw in DAY_KEYWORDS):
                            shared_days = text.strip()
                        else:
                            locations.append(text.strip())
                    for loc_name in locations:
                        loc_details.append({"location": loc_name, "days": shared_days})
                
                if loc_details:
                    data["location_details"] = loc_details
                    logger.info(f"    Extracted {len(loc_details)} detailed locations")
            
            # Format 3: "*Dirección: addr1 y addr2"
            elif not data.get("location_details") and data.get("address"):
                addr = data["address"]
                if re.search(r'\by\b', addr) and re.search(r'\d', addr):
                    parts = re.split(r'\s+y\s+', addr)
                    if len(parts) >= 2:
                        data["location_details"] = [{"location": p.strip(), "days": None} for p in parts if p.strip()]
                        logger.info(f"    Extracted {len(data['location_details'])} locations from address text")
                        
            # Format 4: Regex fallback
            if not data.get("location_details") and not data.get("address"):
                full_text = f"{data.get('steps','')} {data.get('conditions','')} {data.get('raw_text','')[:1000]}"
                address_pattern = r'(?i)\b(Av\.?|Avenida|Calle|Paseo|Camino|Ruta|Pasaje|Boulevard|Autopista|Del)\s+[^#\n]{1,50}?\d{3,6}'
                matches = list(re.finditer(address_pattern, full_text))
                if matches:
                    found = [m.group(0).strip() for m in matches if not any(y in m.group(0) for y in ['2024','2025','2026'])]
                    if found:
                        if len(found) > 1:
                            data["location_details"] = [{"location": a, "days": None} for a in found]
                            logger.info(f"    Extracted {len(found)} locations via regex")
                        else:
                            data["address"] = found[0]
                            logger.info(f"    Extracted address via regex: {found[0]}")
        except Exception as e:
            logger.warning(f"Error extracting Restaurante locations: {e}")

    def _extract_detail_page(self, page: Page, url: str, category: str = "Restaurante") -> Dict:
        """
        Dispatches to the correct extraction path based on category.
        Common fields are always extracted. Location logic only runs for Restaurantes.
        """
        time.sleep(3)
        data = self._extract_common_fields(page, url)
        
        if category == "Restaurante":
            self._extract_restaurante_locations(page, data)
        # Antojo: no location extraction needed (SQL forces location_id='all')
        
        # Fallback: raw text
        try:
            data["raw_text"] = page.inner_text("body")
        except:
            pass
        
        return data



    def scrape(self) -> List[Dict]:
        """
        Refactored scraping logic:
        1. Visit Category List
        2. Scroll once
        3. Iterate through cards and extract
        """
        all_data = []
        seen_offers = set() # To avoid duplicates between categories

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=self._get_playwright_args()
            )
            # Use a desktop user agent
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                viewport={'width': 1280, 'height': 800}
            )
            page = context.new_page()
            
            for cat_info in self.TARGET_CATEGORIES:
                category_url = cat_info["url"]
                category_label = cat_info["category"]
                
                try:
                    logger.info(f"--- Processing Category: {category_label} ({category_url}) ---")
                    page.goto(category_url, timeout=60000, wait_until="domcontentloaded")
                    time.sleep(8) 
                    self._scroll_to_bottom(page)
                    
                    # 1. Collect all names/titles first to define the work queue
                    time.sleep(5)
                    self._scroll_to_bottom(page)
                    
                    potential_names = page.evaluate("""
                        () => {
                            const cards = document.querySelectorAll("div[class*='BenefitsCard_card']");
                            return Array.from(cards).map(c => {
                                const h = c.querySelector('h1, h2, h3, h4');
                                if (h) return h.innerText.trim();
                                const img = c.querySelector('img[alt]');
                                if (img) return img.getAttribute('alt');
                                return "Unknown";
                            });
                        }
                    """)
                    
                    # Clean and dedup names — keep raw for modal matching
                    queue = []  # list of (cleaned_name, raw_name)
                    for raw_name in potential_names:
                        # Strip common discount prefixes to get the pure restaurant name
                        name = re.sub(r'^(Descuentos? en restaurante[s]? |Descuentos? en restuarante[s]? |Descuentos? en |Experiencia |Beneficio |Disfruta de tu beneficio en |\d+% (de )?dcto en )', '', raw_name, flags=re.IGNORECASE).strip()
                            
                        if name and name != "Unknown" and name not in seen_offers:
                            queue.append((name, raw_name))
                    
                    limit_str = os.getenv("LIMIT_ITEMS")
                    if limit_str and limit_str.strip():
                        try:
                            lim = int(limit_str)
                            if lim > 0:
                                queue = queue[:lim]
                        except ValueError:
                            pass
                            
                    logger.info(f"Queue for {category_label}: {len(queue)} items")
                    
                    for name, raw_name in list(queue):
                        if name in seen_offers: continue
                        
                        # Phase 1: Robust Candidate Search
                        slug_candidates = self._get_slug_candidates(name)
                        
                        extracted = False
                        for slug in slug_candidates:
                            url = f"https://www.bancofalabella.cl/descuentos/detalle/{slug}"
                            
                            try:
                                logger.info(f"  [{category_label}] Phase 1: Trial for {name} ({slug[:30]}...)")
                                page.goto(url, timeout=15000, wait_until="domcontentloaded")
                                time.sleep(2)
                                
                                # STRICT validation: DetailBanner must exist
                                if page.locator("[class*='DetailBanner_wrapper-content']").count() > 0:
                                    item_data = self._extract_detail_page(page, url, category=category_label)
                                    if item_data:
                                        item_data["category"] = category_label
                                        all_data.append(item_data)
                                        seen_offers.add(name)
                                        logger.info(f"    ✓ Extracted via Candidate: {slug}")
                                        extracted = True
                                        break
                            except Exception:
                                pass
                                
                        if extracted:
                            continue
                        
                        # Phase 2: If Phase 1 failed, we stay on the detail page or landing.
                        # We'll handle modal fallback in a separate pass or just go back now.
                        # Let's go back specifically for this one if we really want it.
                        logger.warning(f"    ? Direct URL failed for {name}. Will try Modal later.")

                    # Phase 3: Modal Fallback Pass (only for what's left)
                    remaining = [(n, r) for n, r in queue if n not in seen_offers]
                    if remaining:
                        logger.info(f"--- Modal Fallback Pass for {len(remaining)} items in {category_label} ---")
                        page.goto(category_url, timeout=60000, wait_until="domcontentloaded")
                        time.sleep(8)
                        self._scroll_to_bottom(page)
                        
                        for name, raw_name in remaining:
                            if name in seen_offers: continue
                            try:
                                logger.info(f"  [{category_label}] Phase 2: Modal for {name} (raw: {raw_name[:40]})...")
                                
                                # More robust matching: iterate through cards and check substring
                                # Since Falabella truncates text on cards (e.g. "..."), exact has_text fails
                                all_cards = page.locator("div[class*='NewCardBenefits_top-content__']").all()
                                card_target = None
                                
                                # Build fuzzy search term: first 2 words of cleaned name
                                name_words = name.split()
                                fuzzy_term = " ".join(name_words[:2]).lower() if len(name_words) >= 2 else name.lower()
                                
                                for c in all_cards:
                                    try:
                                        c_text = c.inner_text().lower()
                                        if name.lower() in c_text or raw_name.lower() in c_text:
                                            card_target = c
                                            break
                                    except:
                                        pass
                                
                                # Fuzzy fallback: try first 2 words if exact match failed
                                if not card_target and len(fuzzy_term) >= 4:
                                    for c in all_cards:
                                        try:
                                            c_text = c.inner_text().lower()
                                            if fuzzy_term in c_text:
                                                card_target = c
                                                logger.info(f"    🔍 Fuzzy match on '{fuzzy_term}'")
                                                break
                                        except:
                                            pass
                                
                                if card_target:
                                    card_target.scroll_into_view_if_needed(timeout=5000)
                                    time.sleep(1)
                                    try:
                                        if card_target.locator("h2, h3").count() > 0:
                                            card_target.locator("h2, h3").first.click(force=True)
                                        else:
                                            card_target.click(force=True)
                                    except:
                                        # Fallback if standard click fails
                                        card_target.click(force=True)
                                    
                                    time.sleep(4)
                                    if page.locator("[class*='DetailBanner_wrapper-content']").count() > 0:
                                        item_data = self._extract_detail_page(page, page.url, category=category_label)
                                        if item_data:
                                            item_data["category"] = category_label
                                            all_data.append(item_data)
                                            seen_offers.add(name)
                                            logger.info(f"    ✓ Extracted via Modal")
                                        
                                        if "/detalle/" in page.url:
                                            page.go_back()
                                        else:
                                            page.keyboard.press("Escape")
                                        time.sleep(1.5)
                                    else:
                                        logger.error(f"    ❌ Modal failed to open for {name}")
                                        if "/detalle/" in page.url:
                                            page.go_back()
                                        else:
                                            page.keyboard.press("Escape")
                            except Exception as e:
                                logger.error(f"    ❌ Error in modal fallback for {name}: {e}")
                                if "/detalle/" in page.url:
                                    page.go_back()
                                else:
                                    page.keyboard.press("Escape")
                                time.sleep(1.5)
                
                except Exception as e:
                    logger.error(f"Error processing category {category_url}: {e}")
            
            # Phase 3: Check Extra Names (Force visit)
            logger.info("--- Checking Extra Names ---")
            for name in self.EXTRA_NAMES:
                if name in seen_offers: continue
                
                slug_candidates = self._get_slug_candidates(name)
                for slug in slug_candidates:
                    url = f"https://www.bancofalabella.cl/descuentos/detalle/{slug}"
                    try:
                        page.goto(url, timeout=20000, wait_until="domcontentloaded")
                        time.sleep(4)
                        # STRICT validation
                        if page.locator("[class*='DetailBanner_wrapper-content']").count() > 0:
                            item_data = self._extract_detail_page(page, page.url, category="Restaurante")
                            if item_data and (item_data.get("title") or item_data.get("discount_pct")):
                                item_data["category"] = "Restaurante"
                                all_data.append(item_data)
                                seen_offers.add(name)
                                logger.info(f"  ✓ Extracted Extra via Candidate: {name} ({slug})")
                                break
                    except:
                        pass
            
            # Phase 4: Handle Manual URLs
            logger.info("--- Processing Manual URLs ---")
            for url in self.MANUAL_URLS:
                if any(d.get("url") == url for d in all_data):
                    continue
                
                try:
                    logger.info(f"  [Manual] Processing: {url}")
                    page.goto(url, timeout=30000, wait_until="domcontentloaded")
                    time.sleep(3)
                    
                    if page.locator("[class*='DetailBanner_wrapper-content']").count() > 0:
                        # Infer category for manual URLs
                        title_text = page.locator("h1").first.inner_text().lower() if page.locator("h1").count() > 0 else ""
                        category = "Antojo"
                        # Simple heuristic: if it mentions 'restaurante' or is a known restaurant without being a quick-chain
                        if "restaurante" in title_text or "chili's" in title_text or "la tabla" in title_text:
                            category = "Restaurante"
                            
                        item_data = self._extract_detail_page(page, url, category=category)
                        if item_data:
                            item_data["category"] = category
                            all_data.append(item_data)
                            logger.info(f"    ✓ Extracted Manual: {url}")
                except Exception as e:
                    logger.error(f"    ❌ Error processing manual URL {url}: {e}")

            browser.close()
        
        return all_data

    def _name_to_slug(self, name: str) -> str:
        """Helper to convert names to URL slugs with minimal cleaning."""
        import unicodedata
        # 1. Replace & with 'y' before cleaning symbols
        name = name.replace('&', ' y ')
        
        # 2. Standard normalization
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
        name = name.lower()
        name = re.sub(r'[^a-z0-9]+', '-', name)
        return name.strip('-')

    def _get_slug_candidates(self, name: str) -> List[str]:
        """
        Generates a prioritization list of possible slugs for a restaurant.
        Tries longer/specific names first, then cuts back to generic names.
        """
        # A. Clean name from obvious noise first
        name_clean = re.sub(r'^(Descuentos? en restaurante[s]? |Descuentos? en |Experiencia |Beneficio |Disfruta de tu beneficio en |\s*\d+% dcto en )', '', name, flags=re.IGNORECASE).strip()
        
        # 1. Try RAW slug first (exactly as it appears on the card)
        raw_slug = self._name_to_slug(name_clean)
        candidates = [raw_slug]

        # 1b. Try variations of ampersands (Falabella sometimes removes & instead of using 'y')
        if '&' in name_clean:
            # self._name_to_slug handles '&' -> ' y '
            # Let's try one where we just remove it
            name_no_amp = name_clean.replace('&', ' ')
            candidates.append(self._name_to_slug(name_no_amp))

        # 1c. Force specific missing restaurants
        if "cabrera" in name_clean.lower():
            candidates.append("la-cabrera-al-paso")
        
        # 2. Try WITHOUT mall names (as a secondary option)
        mall_patterns = r'(?i)\s+(Parque Arauco|Costanera Center|Mall Plaza|Alto Las Condes|Open Kennedy|Vivo Imperio|Plaza Egaña|Plaza Vespucio|Plaza Oeste|Plaza Tobalaba|Plaza Los Dominicos|Plaza Norte|Mirador del Alto)$'
        no_mall_name = re.sub(mall_patterns, '', name_clean).strip()
        base_slug = self._name_to_slug(no_mall_name)
        
        if base_slug != raw_slug:
            candidates.append(base_slug)
        
        # 3. Known Falabella URL Patterns
        candidates.append(f"40-{base_slug}")
        candidates.append(f"sog-{base_slug}")
        candidates.append(f"40-de-dcto-en-restaurantes-mallplaza-{base_slug}")
        candidates.append(f"40-de-dcto-en-restaurantes-vivo-imperio-{base_slug}")
        candidates.append(f"borderio-{base_slug}")
        
        # 4. Regional Suffixes
        regional_cities = ["talca", "concepcion", "antofagasta", "iquique", "arica", "la-serena", "serena", "vina", "vina-del-mar", "temuco", "valdivia", "puerto-varas", "osorno", "valp", "valparaiso"]
        for city in regional_cities:
            candidates.append(f"{base_slug}-{city}")

        # 5. Recursive reduction (removing last words)
        words = base_slug.split('-')
        while len(words) > 1:
            words.pop()
            shorter = "-".join(words)
            if shorter:
                candidates.append(shorter)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_candidates = []
        for c in candidates:
            if c not in seen:
                unique_candidates.append(c)
                seen.add(c)
                
        return unique_candidates




def main():
    logger.info("Starting Banco Falabella Daily Scraper (SPA Click Mode - Robust Version)")
    scraper = BancoScraper()
    data = scraper.scrape()
    
    if data:
        # Calculate Unique Stats
        unique_restaurants = {d.get("title") for d in data if d.get("category") == "Restaurante"}
        unique_antojos = {d.get("title") for d in data if d.get("category") == "Antojo"}
        
        logger.info("--- SCRAPING SUMMARY ---")
        logger.info(f"Total entries scraped: {len(data)}")
        logger.info(f"Unique Restaurants: {len(unique_restaurants)}")
        logger.info(f"Unique Antojos: {len(unique_antojos)}")
        logger.info("------------------------")

        out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json", "falabella.json")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ Scraped {len(data)} items -> {out_path}")
    else:
        logger.warning("No data scraped.")

if __name__ == "__main__":
    main()

