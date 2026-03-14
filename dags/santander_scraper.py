"""
Santander Scraper v2 — API-based (promociones.json)
Fetches from the public Santander API instead of scraping HTML.
Uses Selenium only to bypass WAF (fetches JSON endpoints via browser).

Endpoints:
  List:   /beneficios/promociones.json?per_page=500
  Detail: /beneficios/promociones/{slug}.json  (has custom_fields)

Field sources:
  discount_pct      ← regex on custom_fields["Bajada externa"], fallback tags
  valid_days        ← tags (lunes, martes, ..., todos-los-dias), fallback Bajada
  expiration_date   ← custom_fields["Vigencia"]
  discount_cap      ← regex on description HTML ("tope" / "máximo")
  card_type         ← tags (exclusivo-amex, wm-limited, ...) + description fallback
  location          ← custom_fields["Comuna cobertura"] + description address

Multi-% handling (parse_multi_discount):
  Texts like "40% dcto. lunes y 30% dcto. otros días" are split into
  separate (pct, [days]) pairs. Channel-based splits ("40% web y 20%
  tiendas") are detected by keywords (tienda/web/.cl) and ignored.
"""
import os
import re
import json
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LIST_URL = "https://banco.santander.cl/beneficios/promociones.json?per_page=500"
DETAIL_URL_TPL = "https://banco.santander.cl/beneficios/promociones/{slug}.json"

# Tags that indicate restaurant/gastronomy promos
INCLUDE_TAGS = {"cat-sabores", "miercoles-de-sabores"}
# Tags that indicate non-restaurant promos
EXCLUDE_TAGS = {"cat-cuotas-sin-interes", "empresas"}

# Day tags → display names
DAY_MAP = {
    "lunes": "Lunes",
    "martes": "Martes",
    "miercoles": "Miércoles",
    "jueves": "Jueves",
    "viernes": "Viernes",
    "sabado": "Sábado",
    "domingo": "Domingo",
    "todos-los-dias": "Todos los días",
}

# Discount tags → percentages (fallback only)
DISCOUNT_TAG_MAP = {
    "cincuenta": 50,
    "cuarenta": 40,
    "treinta": 30,
    "veinticinco": 25,
    "veinte": 20,
    "quince": 15,
    "diez": 10,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_chrome_driver():
    options = Options()
    # NOTE: NOT headless — Santander WAF blocks headless browsers
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def warmup_session(driver):
    """Visit the main page to acquire WAF cookies before hitting JSON endpoints."""
    print("  Warming up session (visiting main page)...")
    driver.get("https://banco.santander.cl/beneficios")
    time.sleep(3)
    print("  [OK] Session ready")


def fetch_json(driver, url, retries=2):
    """Navigate to a JSON endpoint and parse the response."""
    for attempt in range(retries + 1):
        try:
            driver.get(url)
            time.sleep(0.5)
            # Get the page source (browser renders JSON as text in <pre> or <body>)
            try:
                body = driver.find_element("tag name", "pre").text
            except Exception:
                body = driver.find_element("tag name", "body").text
            if not body.strip():
                raise ValueError("Empty response body")
            return json.loads(body)
        except Exception as e:
            if attempt < retries:
                print(f"  ⚠ Retry {attempt+1} for {url[:80]}...")
                time.sleep(2)
            else:
                print(f"  ⚠ Failed to fetch {url[:80]}: {e}")
                return None


def strip_html(html_str):
    """Remove HTML tags, decode entities, normalize whitespace."""
    if not html_str:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', html_str)
    text = re.sub(r'</li>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&ordm;', 'º')
    return '\n'.join(line.strip() for line in text.split('\n') if line.strip())


def extract_discount_pct(bajada_text, tags):
    """Extract discount % — priority: Bajada externa regex, fallback: tags."""
    if bajada_text:
        m = re.search(r'(\d+)\s*%', bajada_text)
        if m:
            return int(m.group(1))
    # Fallback to tags
    for tag, pct in DISCOUNT_TAG_MAP.items():
        if tag in tags:
            return pct
    return None


def extract_valid_days(bajada_text, tags):
    """Extract valid days — priority: Bajada externa (more reliable), fallback: tags."""
    # 1. From Bajada externa (user-facing promo text, most reliable)
    if bajada_text:
        bajada_lower = bajada_text.lower()
        if "todos los días" in bajada_lower or "toda la semana" in bajada_lower:
            return ["Todos los días"]
        found = []
        for tag_key, display in DAY_MAP.items():
            if tag_key.replace("-", " ") in bajada_lower or display.lower() in bajada_lower:
                found.append(display)
        if found:
            return found

    # 2. Fallback: from tags
    day_tags = [DAY_MAP[t] for t in tags if t in DAY_MAP]
    if day_tags:
        return day_tags

    return ["Todos los días"]


def extract_discount_cap(description_text):
    """Extract discount cap from description. Returns (cap_int, is_unlimited)."""
    if not description_text:
        return None, None

    desc_clean = re.sub(r'[áàä]', 'a', description_text.lower())
    desc_clean = re.sub(r'[éèë]', 'e', desc_clean)
    desc_clean = re.sub(r'[íìï]', 'i', desc_clean)
    desc_clean = re.sub(r'[óòö]', 'o', desc_clean)
    desc_clean = re.sub(r'[úùü]', 'u', desc_clean)

    # Check for "sin tope"
    if re.search(r'sin\s+tope', desc_clean):
        return None, True

    # Extract numeric cap
    m = re.search(r'(?:tope|maximo|descuento maximo)[^$]*?\$([\d\.]+)', desc_clean)
    if m:
        cap_str = m.group(1).replace('.', '')
        try:
            return int(cap_str), False
        except ValueError:
            pass

    return None, None


def extract_expiration(custom_fields, description_text):
    """Extract expiration date — priority: custom_fields['Vigencia'], fallback: description."""
    vigencia = None
    if custom_fields and 'Vigencia' in custom_fields:
        vigencia = custom_fields['Vigencia'].get('value', '')

    if not vigencia and description_text:
        m = re.search(r'(?:vigencia|válido|valido).*?hasta[^.]*', description_text, re.IGNORECASE)
        if m:
            vigencia = m.group(0)

    if not vigencia:
        return ""
    return vigencia.strip()


def extract_card_type(tags, description_text):
    """Extract card type from tags, with fallback to description."""
    tag_set = set(tags)

    if 'exclusivo-amex' in tag_set or 'amex' in tag_set:
        return 'amex'
    if 'exclusivo-limited' in tag_set or 'wm-limited' in tag_set:
        return 'limited'
    if 'life-y-debito' in tag_set:
        return 'credito_debito'

    # Fallback: check description
    if description_text:
        desc_lower = description_text.lower()
        if 'american express' in desc_lower or 'amex' in desc_lower:
            return 'amex'
        if 'worldmember limited' in desc_lower or 'wm limited' in desc_lower:
            return 'limited'
        if 'débito' in desc_lower or 'debito' in desc_lower:
            return 'credito_debito'

    return 'general'


def extract_location(custom_fields, description_text):
    """Extract location from custom_fields and description."""
    commune = ""
    region = ""
    address = ""

    if custom_fields:
        if 'Comuna cobertura' in custom_fields:
            commune = custom_fields['Comuna cobertura'].get('value', '')
        if 'Región cobertura' in custom_fields:
            region = custom_fields['Región cobertura'].get('value', '')

    # Extract address from description <li> items
    if description_text:
        lines = description_text.split('\n')
        for line in lines:
            line = line.strip()
            # Heuristic: line has digits and looks like an address (comma-separated, commune name)
            if (re.search(r'\d', line) and ',' in line and len(line) > 5
                    and not re.search(r'(?:tope|descuento|acumulable|exclu|válido|propina|pedido)', line, re.IGNORECASE)):
                address = line
                break
            # Also match "Av." / "Calle" / "Ruta" patterns
            if re.search(r'(?:^|\s)(Av\.?|Calle|Ruta|Camino|Strip|Mall|Portal)\s', line, re.IGNORECASE) and len(line) > 5:
                if not re.search(r'(?:tope|descuento|acumulable)', line, re.IGNORECASE):
                    address = line
                    break

    # Build location string
    if address:
        return address
    if commune:
        return commune
    if region:
        return region
    return "Varios"


def parse_multi_discount(bajada_text, tags):
    """
    Detect multi-% Bajada texts and return (pct, [days]) pairs.
    E.g. "40% dcto. todos los lunes y 30% dcto. los otros días de la semana"
         → [(40, ['Lunes']), (30, ['Martes','Miércoles','Jueves','Viernes','Sábado','Domingo'])]
    Returns empty list if single-% or channel-based split (e.g. "40% web y 20% tiendas").
    """
    if not bajada_text:
        return []

    # Find all "N% ... day-reference" segments
    # Pattern: capture blocks separated by " y " that each contain a %
    pct_matches = list(re.finditer(r'(\d+)\s*%\s*(?:dcto\.?)?\s*(.*?)(?=\d+\s*%|$)', bajada_text, re.IGNORECASE))
    if len(pct_matches) < 2:
        return []  # Single-% text, use normal path

    ALL_DAYS = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    day_keywords = {
        'lunes': 'Lunes', 'martes': 'Martes', 'miércoles': 'Miércoles', 'miercoles': 'Miércoles',
        'jueves': 'Jueves', 'viernes': 'Viernes', 'sábado': 'Sábado', 'sabado': 'Sábado',
        'domingo': 'Domingo',
    }

    results = []
    assigned_days = set()

    for m in pct_matches:
        pct = int(m.group(1))
        context = m.group(2).lower().strip()

        # Check if this segment is channel-based (web, tiendas, app) not day-based
        if re.search(r'(tienda|web|app|online|delivery|\.cl)', context):
            continue  # Skip channel-based segments

        # Check for "otros días" / "resto" / "demás días"
        if re.search(r'otros?\s+d[ií]as|resto|dem[aá]s', context):
            # Will be resolved after all specific days are assigned
            results.append((pct, 'OTHER'))
            continue

        # Check for "todos los días" / "toda la semana"
        if re.search(r'todos\s+los\s+d[ií]as|toda\s+la\s+semana', context):
            results.append((pct, ALL_DAYS[:]))
            assigned_days.update(ALL_DAYS)
            continue

        # Extract specific day names
        found_days = []
        for key, display in day_keywords.items():
            if key in context:
                found_days.append(display)
                assigned_days.add(display)
        if found_days:
            results.append((pct, found_days))

    # If no valid day-based segments were found, return empty (channel-only split)
    if not results:
        return []

    # Resolve 'OTHER' entries
    resolved = []
    for pct, days in results:
        if days == 'OTHER':
            other_days = [d for d in ALL_DAYS if d not in assigned_days]
            if other_days:
                resolved.append((pct, other_days))
        else:
            resolved.append((pct, days))

    return resolved if len(resolved) >= 2 else []


# ---------------------------------------------------------------------------
# Main Scraper
# ---------------------------------------------------------------------------
def scrape_santander():
    driver = get_chrome_driver()
    items = []

    try:
        # Step 0: Warm up session to acquire WAF cookies
        warmup_session(driver)

        # Step 1: Fetch the list
        print(f"Fetching list: {LIST_URL}")
        list_data = fetch_json(driver, LIST_URL)
        if not list_data:
            print("❌ FATAL: Could not fetch promotion list.")
            return []

        promos = list_data.get("promociones", [])
        meta = list_data.get("meta", {})
        total = meta.get("total_entries", len(promos))
        total_pages = meta.get("total_pages", 1)
        current_page = meta.get("current_page", 1)
        print(f"  ✓ Page {current_page}/{total_pages} — {len(promos)} promos (total: {total})")

        # Handle pagination (dynamic, not hardcoded)
        while current_page < total_pages:
            next_page = current_page + 1
            url = f"https://banco.santander.cl/beneficios/promociones.json?per_page=500&page={next_page}"
            print(f"  Fetching page {next_page}...")
            page_data = fetch_json(driver, url)
            if page_data:
                promos.extend(page_data.get("promociones", []))
                meta = page_data.get("meta", {})
                current_page = meta.get("current_page", next_page)
            else:
                break

        print(f"Total promos from API: {len(promos)}")

        # Step 2: Filter by category tags
        filtered = []
        for p in promos:
            tags = set(p.get("tags", []))
            if tags & EXCLUDE_TAGS:
                continue
            if tags & INCLUDE_TAGS:
                filtered.append(p)
        print(f"After category filter: {len(filtered)} promos (included cat-sabores/miercoles-de-sabores)")

        # Step 3: Fetch details for each promo (to get custom_fields)
        for i, promo in enumerate(filtered):
            slug = promo.get("slug", "")
            title = promo.get("title", "Unknown").strip()
            tags = promo.get("tags", [])

            print(f"  [{i+1}/{len(filtered)}] {title} ({slug})...", end=" ")

            # Fetch detail for custom_fields
            detail = fetch_json(driver, DETAIL_URL_TPL.format(slug=slug))
            custom_fields = {}
            if detail:
                custom_fields = detail.get("custom_fields", {})
                # Merge detail fields if available
                if detail.get("description"):
                    promo["description"] = detail["description"]
                if detail.get("conditions"):
                    promo["conditions"] = detail["conditions"]

            # Extract all fields
            description_text = strip_html(promo.get("description", ""))
            bajada = custom_fields.get("Bajada externa", {}).get("value", "")

            discount_pct = extract_discount_pct(bajada, tags)
            valid_days = extract_valid_days(bajada, tags)
            cap, is_unlimited = extract_discount_cap(description_text)
            expiration = extract_expiration(custom_fields, description_text)
            card_type = extract_card_type(tags, description_text)
            location = extract_location(custom_fields, description_text)

            # Handle multi-% Bajada texts like "40% lunes y 30% otros días"
            discount_day_pairs = parse_multi_discount(bajada, tags)

            if discount_day_pairs:
                # Multi-% case: create items per (pct, day) pair
                for pct, days_list in discount_day_pairs:
                    for day in days_list:
                        item = {
                            "bank": "santander",
                            "title": title,
                            "discount_text": bajada,
                            "raw_discount_text": bajada,
                            "discount_pct": pct,
                            "recurrence": day,
                            "expiration_date": expiration,
                            "location": location,
                            "card_type": card_type,
                            "discount_cap": cap,
                            "discount_cap_is_unlimited": is_unlimited,
                            "conditions": promo.get("conditions", ""),
                            "description": description_text,
                            "image_url": (promo.get("covers") or [""])[0],
                            "url": promo.get("url", ""),
                            "tags": tags,
                            "scraped_at": datetime.now().isoformat(),
                        }
                        items.append(item)
                all_days = [d for _, days in discount_day_pairs for d in days]
                all_pcts = list(set(p for p, _ in discount_day_pairs))
                print(f"[MULTI] {'/'.join(str(p) for p in all_pcts)}% | {', '.join(all_days)} | cap={'sin tope' if is_unlimited else cap}")
            else:
                # Standard single-% case: one item per day
                for day in valid_days:
                    item = {
                        "bank": "santander",
                        "title": title,
                        "discount_text": bajada or (f"{discount_pct}% dcto." if discount_pct else ""),
                        "raw_discount_text": bajada,
                        "discount_pct": discount_pct,
                        "recurrence": day,
                        "expiration_date": expiration,
                        "location": location,
                        "card_type": card_type,
                        "discount_cap": cap,
                        "discount_cap_is_unlimited": is_unlimited,
                        "conditions": promo.get("conditions", ""),
                        "description": description_text,
                        "image_url": (promo.get("covers") or [""])[0],
                        "url": promo.get("url", ""),
                        "tags": tags,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    items.append(item)

                print(f"{discount_pct}% | {', '.join(valid_days)} | cap={'sin tope' if is_unlimited else cap}")
            time.sleep(0.2)

    finally:
        # Save
        output_path = os.path.join(os.path.dirname(__file__), 'json', 'santander.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"\nSaved {len(items)} items to {output_path}")
        driver.quit()

    return items


if __name__ == "__main__":
    data = scrape_santander()
    if data:
        print(f"✅ Scraped {len(data)} items → json/santander.json")
    else:
        print("No items scraped.")
