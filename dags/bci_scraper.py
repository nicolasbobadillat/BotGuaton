
import requests
import json
import time
import re
import os
from datetime import datetime
from playwright.sync_api import sync_playwright

import unicodedata

def clean_text(text):
    if not text:
        return ""
    return text.strip().replace('\n', ' ').replace('\t', ' ')

def normalize_slug(text):
    """Normalize text for URL construction (remove accents, lowercase)."""
    if not text: return "restaurantes"
    text = str(text).lower()
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    return re.sub(r'[^a-z0-9]+', '-', text).strip('-')

class BciScraper:
    BASE_URL = "https://api.bciplus.cl/bff-loyalty-beneficios/v1/offers"
    
    DIAS_TRADUCCION = {
        "MONDAY": "Lunes", "TUESDAY": "Martes", "WEDNESDAY": "Miércoles",
        "THURSDAY": "Jueves", "FRIDAY": "Viernes", "SATURDAY": "Sábado", "SUNDAY": "Domingo"
    }

    def __init__(self):
        self.session = requests.Session()
        self.api_key = None
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": "https://www.bci.cl/"
        })
        # Default fallback key (proven to work)
        self.fallback_key = "fa981752762743668413b68821a43840"

    def obtener_api_key(self):
        """
        Intenta capturar la key con Playwright. Si falla, usa fallback.
        """
        print("Iniciando navegador para capturar API Key de BCI (Headless Off)...")
        found_key = None
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                page = browser.new_page()
                
                def handle_request(request):
                    nonlocal found_key
                    if "api.bciplus.cl" in request.url and "ocp-apim-subscription-key" in request.headers:
                        key = request.headers["ocp-apim-subscription-key"]
                        if key: found_key = key
                
                page.on("request", handle_request)
                
                try:
                    page.goto("https://www.bci.cl/beneficios/beneficios-bci/restaurantes", timeout=60000)
                    # Esperar hasta capturar la key o timeout corto
                    for _ in range(15):
                        if found_key:
                            break
                        time.sleep(1)
                except Exception as e:
                    print(f"Advertencia capturando key: {e}")
                
                browser.close()
        except Exception as e:
            print(f"Error launching playwright for key: {e}")

        if found_key:
            print(f"API Key capturada: {found_key[:5]}...")
            self.api_key = found_key
        else:
            print("No se pudo capturar key dinámica. Usando fallback.")
            self.api_key = self.fallback_key
            
        self.session.headers.update({"ocp-apim-subscription-key": self.api_key})

    def obtener_ofertas(self):
        if not self.session.headers.get("ocp-apim-subscription-key"):
            self.obtener_api_key()
            
        todas_ofertas = []
        pagina = 1
        items_por_pagina = 100
        
        print("Descargando lista de ofertas...")
        while True:
            try:
                params = {"itemsPorPagina": items_por_pagina, "pagina": pagina}
                response = self.session.get(self.BASE_URL, params=params, timeout=15)
                
                if response.status_code != 200:
                    print(f"Error página {pagina}: {response.status_code}")
                    break
                
                data = response.json()
                ofertas = data.get("ofertas", [])
                
                if not ofertas:
                    break
                
                todas_ofertas.extend(ofertas)
                print(f"Página {pagina}: {len(ofertas)} items.")
                
                paginado = data.get("paginado", {})
                if pagina >= paginado.get("totalPaginas", pagina):
                    break
                    
                pagina += 1
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error en loop: {e}")
                break
                
        return todas_ofertas

    def enrich_with_details(self, ofertas):
        """
        Visita el detalle de cada oferta (filtrada) para extraer tipo de tarjeta y ubicacion.
        """
        print(f"Enriqueciendo {len(ofertas)} ofertas con detalle (Headless Off)...")
        
        # Primero filtrar
        to_enrich = []
        for o in ofertas:
            cats = [c.get("titulo", "") for c in o.get("categorias", [])]
            tags = [t.get("nombre", "") for t in o.get("tags", [])]
            combined = " ".join(cats + tags).lower()
            if any(x in combined for x in ["restaurante", "sabor", "comida", "bar", "cafe", "gastronomia", "antojo", "dulce", "helado", "pasteleria", "chocolate"]):
                to_enrich.append(o)
        
        print(f"Filtrado: {len(to_enrich)} ofertas relevantes para restaurantes.")
        
        limit = int(os.environ.get("LIMIT_ITEMS", 0))
        if limit > 0:
            print(f"Limiting enrichment to first {limit} items.")
            to_enrich = to_enrich[:limit]
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            for idx, item in enumerate(to_enrich):
                try:
                    # BCI detail pages work more consistently under the /detalle/ path
                    slug = item.get("slug")
                    url = f"https://www.bci.cl/beneficios/beneficios-bci/detalle/{slug}"
                    
                    print(f"[{idx+1}/{len(to_enrich)}] Visitando: {item.get('titulo')[:30]}...")
                    
                    try:
                        page.goto(url, timeout=30000)
                        # Wait for the main content or specifically the date icon
                        try:
                            page.wait_for_selector(".icon-bci-date", timeout=3000)
                        except: pass
                        
                        content = page.inner_text("body")
                        content_lower = content.lower()
                        
                        # Extract Cards
                        tarjetas = []
                        if "visa infinite" in content_lower or "infinite" in content_lower: tarjetas.append("Visa Infinite")
                        if "visa signature" in content_lower or "signature" in content_lower: tarjetas.append("Visa Signature")
                        if "mastercard black" in content_lower or "black" in content_lower: tarjetas.append("Mastercard Black")
                        if "débito" in content_lower or "debito" in content_lower or "redcompra" in content_lower:
                             tarjetas.append("Débito Bci")
                        if "tarjeta de credito" in content_lower or "tarjeta de crédito" in content_lower:
                             tarjetas.append("Crédito Bci")
                        item["extracted_cards"] = list(set(tarjetas))
                        
                        # Extract Location
                        loc = "Ver sitio"
                        match = re.search(r'(?:Dirección|Ubicación|Lugar):\s*([^\n\r]+)', content, re.IGNORECASE)
                        if match:
                            loc = clean_text(match.group(1))
                        else:
                            # Strategy 2: Infer from 'titulo'
                            api_title = item.get("titulo", "")
                            if " en " in api_title:
                                parts = api_title.split(" en ")
                                if len(parts) > 1:
                                    possible_loc = parts[-1].strip()
                                    if len(possible_loc) < 30 and not any(x in possible_loc.lower() for x in ["descuento", "bci", "tarjeta"]):
                                        loc = possible_loc
                        item["extracted_location"] = loc
                        item["detail_url"] = url
                        
                        # Extract Cap and Exp
                        cap = None
                        match_cap = re.search(r'tope.*\$?(\d[\d\.]+)', content, re.IGNORECASE)
                        if match_cap:
                            cap = match_cap.group(1).replace('.', '')
                        item["extracted_cap"] = cap

                        exp = None
                        # Priority 1: Specific element with "Vencimiento"
                        try:
                            # Wait up to 3s for the icon to appear (dynamic content)
                            vencimiento_element = page.wait_for_selector(".icon-bci-date", timeout=3000)
                            if vencimiento_element:
                                # Get the parent 'p' element's text
                                parent_text = page.evaluate("el => el.parentElement.innerText", vencimiento_element)
                                # Capture "Vencimiento: 31/03/2026" or similar
                                match_venc = re.search(r'Vencimiento:?\s*(\d{2}/\d{2}/\d{4})', parent_text, re.IGNORECASE)
                                if match_venc:
                                    dd, mm, yyyy = match_venc.group(1).split('/')
                                    exp = f"{yyyy}-{mm}-{dd}"
                        except Exception as e:
                            # If it fails or times out, we move to fallback
                            pass

                        # Priority 2: Regex on body content (fallback)
                        if not exp:
                            # Expand to catch "Vencimiento" or "Válido hasta" in the body
                            match_exp = re.search(r'(?:Válid[oa].*hasta|Vencimiento:).*?(\d{1,2}.*?20\d{2})|Vencimiento:\s*(\d{2}/\d{2}/\d{4})', content, re.IGNORECASE)
                            if match_exp:
                                if match_exp.group(2): # Case DD/MM/YYYY
                                    dd, mm, yyyy = match_exp.group(2).split('/')
                                    exp = f"{yyyy}-{mm}-{dd}"
                                else:
                                    exp = match_exp.group(1) # Case "28 de febrero de 2026"
                        
                        item["extracted_exp"] = exp
                        
                    except Exception as e:
                        print(f"Error visitando {url}: {e}")
                        item["extracted_cards"] = []
                        item["extracted_location"] = "Ver sitio"
                        item["extracted_exp"] = None

                except Exception as e:
                    print(f"Error procesando item: {e}")
                    print(f"Error processing item: {e}")
            
            browser.close()
        return ofertas

    def process_data(self, ofertas):
        print("Procesando datos finales...")
        final_data = []
        
        for oferta in ofertas:
            cats = [c.get("titulo", "") for c in oferta.get("categorias", [])]
            tags = [t.get("nombre", "") for t in oferta.get("tags", [])]
            combined = " ".join(cats + tags).lower()
            
            if not any(x in combined for x in ["restaurante", "sabor", "comida", "bar", "cafe", "gastronomia", "antojo", "dulce", "helado", "pasteleria", "pastelería", "dulcería", "chocolate", "bombones"]):
                continue

            # USE COMERCIO NOMBRE FOR REAL RESTAURANT NAME
            comercio_name = oferta.get("comercio", {}).get("nombre", "N/A")
            comercio_name = re.sub(r'\s*-\s*Descuento.*$', '', comercio_name, flags=re.IGNORECASE).strip()
            
            # Use original title as description/text source
            api_desc_title = oferta.get("titulo", "")
            
            # Discount
            descuento = "Ver detalle"
            match = re.search(r'(\d+)%', api_desc_title)
            if match:
                descuento = match.group(1)
            else:
                val = oferta.get("beneficio", {}).get("discount", {}).get("porcentajeDescuento")
                if val and val > 0: 
                    descuento = str(val)
                else:
                    # Fallback to legal text (e.g. cashback or general description)
                    legal_for_desc = oferta.get("legal", "") or oferta.get("descripcion", "")
                    match_legal = re.search(r'(\d+)%\s*(?:de\s+)?(?:descuento|dcto|cashback)', legal_for_desc, re.IGNORECASE)
                    if match_legal:
                        descuento = match_legal.group(1)

            # Days
            dias_raw = oferta.get("scheduling", {}).get("dayRecurrence", [])
            dias = ", ".join([self.DIAS_TRADUCCION.get(d, d) for d in dias_raw]) if dias_raw else "Todos los días"

            # Cards
            extracted = oferta.get("extracted_cards", [])
            
            card_type_key = "bci_credito" # Default "All other Credit"
            
            if extracted:
                extracted = list(set(extracted))
                
                has_premium = any(x in ["Visa Infinite", "Visa Signature", "Mastercard Black"] for x in extracted)
                has_debito = "Débito Bci" in extracted
                
                if has_premium:
                     # Any premium card implies the full Premium suite
                     card_type_key = "bci_premium"
                elif has_debito:
                    card_type_key = "bci_credito_debito"
                # Heuristic: 50% discount -> Premium
                elif oferta.get("discount_text", "0") == "50" or "50%" in oferta.get("discount_text", ""):
                     card_type_key = "bci_premium"
                else:
                    card_type_key = "bci_credito"
            else:
                # Fallback text search
                texto_full = f"{api_desc_title} {oferta.get('legal','')} {oferta.get('descripcion','')}"
                if "Visa Infinite" in texto_full or "Visa Signature" in texto_full or "Mastercard Black" in texto_full: 
                    card_type_key = "bci_premium"
                elif "Débito" in texto_full or "Debito" in texto_full: 
                    card_type_key = "bci_credito_debito"
                elif oferta.get("discount_text", "0") == "50" or "50%" in oferta.get("discount_text", ""): 
                    card_type_key = "bci_premium"
                else: 
                    card_type_key = "bci_credito"

            # Location from enrichment
            location = oferta.get("extracted_location", "Ver sitio")

            # Image
            image_url = ""
            imgs = oferta.get("imagenes", {})
            if imgs:
                image_url = imgs.get("imagen1", "") or list(imgs.values())[0]

            # Conditions/Legal
            legal_text = oferta.get("legal", "")
            
            # Cap and Exp Extraction (Fallback to legal text)
            cap = oferta.get("extracted_cap")
            if not cap:
                # Improved regex: non-greedy, matches $ optionally, handles dots
                match_cap = re.search(r'tope.*?\$\s?(\d[\d\.]+)|\btope\b.*?\b(\d[\d\.]+)', legal_text, re.IGNORECASE)
                if match_cap:
                    # Use the non-empty group
                    val_str = match_cap.group(1) or match_cap.group(2)
                    cap = val_str.replace('.', '').replace(',', '')

            exp = oferta.get("extracted_exp")
            if not exp:
                # More flexible exp regex for legal text (Spanish dates)
                match_exp = re.search(r'(?:Válid[oa]|hasta el|Vencimiento:)\s*.*?(\d{1,2}.*?20\d{2})', legal_text, re.IGNORECASE)
                if match_exp:
                    exp = match_exp.group(1)
                
                # Check for DD/MM/YYYY in legal text as well
                if not exp:
                    match_venc_legal = re.search(r'Vencimiento:\s*(\d{2}/\d{2}/\d{4})', legal_text, re.IGNORECASE)
                    if match_venc_legal:
                        dd, mm, yyyy = match_venc_legal.group(1).split('/')
                        exp = f"{yyyy}-{mm}-{dd}"

            row = {
                "title": comercio_name,
                "discount_text": descuento,
                "location": location,
                "recurrence": dias,
                "image_url": image_url,
                "url": f"https://www.bci.cl/beneficios/beneficios-bci/detalle/{oferta.get('slug','')}",
                "bank": "bci",
                "card_type": card_type_key,
                "conditions": legal_text[:500],
                "discount_cap": cap,
                "expiration_date": exp,
                "scraped_at": datetime.now().isoformat()
            }

            # -------------------------------------------------------------------------
            # HARDCODED LOCATION FIXES (User Request)
            # -------------------------------------------------------------------------
            comercio_lower = comercio_name.lower()
            
            # 1. Cabrera al paso -> Split into Las Condes and La Reina
            if "cabrera al paso" in comercio_lower:
                # Create Record 1: Las Condes
                row1 = row.copy()
                row1["location"] = "Las Condes"
                final_data.append(row1)
                
                # Create Record 2: La Reina
                row2 = row.copy()
                row2["location"] = "La Reina"
                final_data.append(row2)
                continue # Skip default append
            
            # 2. Specific Overrides
            if "coya" in comercio_lower:
                row["location"] = "Arica"
            elif "la mamba" in comercio_lower or "bar la mamba" in comercio_lower:
                row["location"] = "Valparaíso"
            elif "manhatan" in comercio_lower or "manhattan" in comercio_lower:
                row["location"] = "Osorno"

            final_data.append(row)
            
        return final_data

def get_bci_discounts_final():
    scraper = BciScraper()
    raw_ofertas = scraper.obtener_ofertas()
    if raw_ofertas:
        scraper.enrich_with_details(raw_ofertas)
        
    final_items = scraper.process_data(raw_ofertas)
    
    with open(os.path.join(os.path.dirname(__file__), 'json', 'bci.json'), 'w', encoding='utf-8') as f:
        json.dump(final_items, f, ensure_ascii=False, indent=4)
        
    return final_items



if __name__ == "__main__":
    items = get_bci_discounts_final()
    if items:
        print(f"✅ Scraped {len(items)} items -> json/bci.json")

