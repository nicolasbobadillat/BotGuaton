import logging
import json
import time
import re
import os

from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TARGETS = [
    ("https://itaubeneficios.cl/beneficios/beneficios-y-descuentos/ruta-gourmet/", "Restaurante"),
    ("https://itaubeneficios.cl/restaurantes/",                                    "Restaurante"),
    ("https://itaubeneficios.cl/beneficios/beneficios-legend/",                    "Restaurante"),
    ("https://itaubeneficios.cl/promociones-del-mes/ruta-gourmet-tarjeta-blue/",   "Antojo"),
    ("https://itaubeneficios.cl/beneficios/beneficios-y-descuentos/de-compras/",   "Antojo"),
]

EXTRA_URLS = [
    ("https://itaubeneficios.cl/restaurantes/bar-la-santoria-2/",   "Restaurante"),
    ("https://itaubeneficios.cl/miercoles-gourmet/mamut/",          "Restaurante"),
    ("https://itaubeneficios.cl/miercoles-gourmet/chicken-factory/","Restaurante"),
]

# ---------------------------------------------------------------------------
# Stealth init script — patches the most common Cloudflare/bot-detection checks
# ---------------------------------------------------------------------------
STEALTH_SCRIPT = """
// 1. Hide webdriver flag
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

// 2. Fake plugins
Object.defineProperty(navigator, 'plugins', {
    get: () => [1, 2, 3, 4, 5],
});

// 3. Fake languages
Object.defineProperty(navigator, 'languages', {
    get: () => ['es-CL', 'es', 'en-US', 'en'],
});

// 4. Fake hardware concurrency
Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });

// 5. Fix chrome object (missing in headless)
window.chrome = {
    runtime: {},
    loadTimes: function() {},
    csi: function() {},
    app: {}
};

// 6. Fix permissions query
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : originalQuery(parameters)
);

// 7. Remove headless from userAgent hints
Object.defineProperty(navigator, 'userAgentData', {
    get: () => ({
        brands: [
            { brand: 'Google Chrome', version: '120' },
            { brand: 'Chromium',      version: '120' },
            { brand: 'Not-A.Brand',   version: '99'  },
        ],
        mobile: false,
        platform: 'Windows',
    }),
});
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
FOOD_KEYWORDS = [
    "gourmet", "restaurante", "bar", "café", "cafe", "té", "coffee", "pizka", "fruit",
    "chocolate", "vino", "cerveza", "panadería", "panaderia", "sushi", "pizza", "burger",
    "sandwich", "helado", "dulce", "salado", "comida", "alimento", "bebida", "trago",
    "mercado", "supermercado", "botillería", "botilleria", "congelados", "carnes",
    "cecinas", "quesos", "fiambres", "pastelería", "pasteleria", "torta", "cupcake",
    "muffin", "donut", "pretzel", "tacos", "burritos", "grill", "bistró", "bistro",
    "cocina", "sabor", "delicia", "postre", "snack", "yogurt", "fruz", "tea", "latte",
    "smoothie", "batido", "jugo", "tostaduría", "viña", "vina", "yoyo", "boost", "bacon",
]

EXCLUDE_KEYWORDS = [
    "vestuario", "ropa", "zapatos", "calzado", "maletas", "equipaje", "tecnología",
    "tecnologia", "celulares", "computación", "hogar", "muebles", "decoración",
    "juguetes", "infantil", "optica", "salud", "belleza", "farmacia", "librería",
    "mascotas", "camillas", "joyas", "accesorios", "relojes", "lentes", "anteojos",
    "audífonos", "cuidado", "piel", "dental", "perfume", "crema", "shampoo", "jabón",
    "jabon", "masaje", "fitness", "deporte", "pet", "veterinaria",
    "skinfit", "perry ellis", "penguin", "trial",
]


def is_food_item(text: str) -> bool:
    text = text.lower()
    for k in EXCLUDE_KEYWORDS:
        if len(k) < 5:
            if re.search(r'\b' + re.escape(k) + r'\b', text):
                return False
        else:
            if k in text:
                return False
    for k in FOOD_KEYWORDS:
        if len(k) < 4:
            if re.search(r'\b' + re.escape(k) + r'\b', text):
                return True
        else:
            if k in text:
                return True
    return False


def parse_recurrence(text: str) -> str:
    t = text.upper()
    if "LUNES"    in t: return "LUNES"
    if "MARTES"   in t: return "MARTES"
    if "MIERCOLES" in t or "MIÉRCOLES" in t: return "MIERCOLES"
    if "JUEVES"   in t: return "JUEVES"
    if "VIERNES"  in t: return "VIERNES"
    if "SABADO"   in t or "SÁBADO"   in t: return "SABADO"
    if "DOMINGO"  in t: return "DOMINGO"
    return "TODOS LOS DÍAS"


def parse_itau_date(date_str: str):
    if not date_str:
        return None
    m = re.search(r'(\d{2})-(\d{2})-(\d{4})', date_str)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo}-{d}"
    return None


def get_text(page, selector: str) -> str:
    try:
        el = page.query_selector(selector)
        if el:
            return el.inner_text().strip()
    except Exception:
        pass
    return ""


def get_image_from_style(page, selector: str) -> str:
    try:
        el = page.query_selector(selector)
        if el:
            style = el.get_attribute("style") or ""
            m = re.search(r'url\(["\']?(.*?)["\']?\)', style)
            if m:
                return m.group(1)
    except Exception:
        pass
    return ""


def wait_past_cloudflare(page, timeout_ms: int = 25000):
    """Wait until Cloudflare challenge page disappears."""
    deadline = time.time() + timeout_ms / 1000
    while time.time() < deadline:
        title = page.title()
        if "Just a moment" not in title and "Attention Required" not in title:
            return True
        logger.info("  ⏳ Waiting for Cloudflare challenge…")
        time.sleep(3)
    return False


def make_browser_context(playwright):
    """Launch a stealth Chromium context."""
    browser = playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--window-size=1280,800",
            "--disable-extensions",
            "--disable-gpu",
            "--ignore-certificate-errors",
        ],
    )
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 800},
        locale="es-CL",
        timezone_id="America/Santiago",
        extra_http_headers={
            "Accept-Language": "es-CL,es;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )
    # Inject stealth patches on every new page
    context.add_init_script(STEALTH_SCRIPT)
    return browser, context


# ---------------------------------------------------------------------------
# Phase 1 — collect detail URLs from listing pages
# ---------------------------------------------------------------------------
def collect_detail_urls(context) -> dict:
    """Returns {url: category}"""
    detail_urls: dict[str, str] = {}
    page = context.new_page()

    for listing_url, category in TARGETS:
        logger.info(f"📋 Collecting URLs from: {listing_url}")
        try:
            page.goto(listing_url, wait_until="domcontentloaded", timeout=30000)
        except PlaywrightTimeout:
            logger.warning(f"  Timeout navigating to {listing_url}, skipping.")
            continue

        if not wait_past_cloudflare(page):
            logger.warning(f"  Cloudflare did not clear for {listing_url}, skipping.")
            continue

        # Wait for cards
        try:
            page.wait_for_selector(".page-beneficios-list-default__grid__item", timeout=20000)
        except PlaywrightTimeout:
            logger.warning(f"  No grid found on {listing_url}")
            continue

        # Scroll to bottom to trigger lazy-load
        prev_height = 0
        for _ in range(20):  # max 20 scroll attempts
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            height = page.evaluate("document.body.scrollHeight")
            if height == prev_height:
                break
            prev_height = height

        # Extract hrefs
        hrefs = page.eval_on_selector_all(
            ".page-beneficios-list-default__grid__item",
            "els => els.map(e => e.getAttribute('href')).filter(Boolean)"
        )

        new_count = 0
        for href in hrefs:
            if href not in detail_urls:
                detail_urls[href] = category
                new_count += 1

        logger.info(f"  Found {new_count} new URLs (total so far: {len(detail_urls)})")

    page.close()

    # Add hardcoded extras
    for url, category in EXTRA_URLS:
        if url not in detail_urls:
            detail_urls[url] = category

    logger.info(f"✅ Total URLs to scrape: {len(detail_urls)}")
    return detail_urls


# ---------------------------------------------------------------------------
# Phase 2 — scrape each detail page
# ---------------------------------------------------------------------------
def scrape_detail(page, link: str, category: str) -> list[dict]:
    """
    Scrapes one detail page. Returns a list of dicts (multiple if dropdown present).
    Returns empty list on failure.
    """
    try:
        page.goto(link, wait_until="domcontentloaded", timeout=25000)
    except PlaywrightTimeout:
        logger.warning(f"  Timeout navigating to {link}")
        return []

    if not wait_past_cloudflare(page, timeout_ms=20000):
        logger.warning(f"  Cloudflare persists on {link}")
        return []

    # Wait for meaningful content
    try:
        page.wait_for_selector(
            ".beneficio__sidebar__caption, .page-title h2, h1",
            timeout=15000,
        )
    except PlaywrightTimeout:
        logger.warning(f"  Content selector timeout on {link}")
        # Log what we got for debugging
        logger.debug(f"  Page title: {page.title()}")
        logger.debug(f"  Content snippet: {page.content()[:200]}")
        return []

    # Small buffer for any JS hydration
    page.wait_for_timeout(1000)

    # --- Common fields ---
    title = get_text(page, ".page-title h2") or get_text(page, "h1")
    if not title:
        logger.warning(f"  No title found for {link}")

    discount_text = get_text(page, ".beneficio__sidebar__caption p")
    description   = get_text(page, ".beneficio__information__texto__block-1")
    conditions    = get_text(page, ".beneficio__information__texto__block-2")
    commune       = (
        get_text(page, ".beneficio__sidebar__highlight__location__value")
        or get_text(page, ".beneficio__sidebar__highlight__info__ubicacion__value")
    )
    image_url     = get_image_from_style(page, ".beneficio__header-image-full")

    # Conditions fallback: find h2 "Restricciones" parent
    if not conditions:
        try:
            h2s = page.query_selector_all("h2")
            for h2 in h2s:
                if "Restricciones" in (h2.inner_text() or ""):
                    parent = h2.evaluate_handle("el => el.parentElement")
                    conditions = parent.as_element().inner_text().replace("Restricciones", "").strip()
                    break
        except Exception:
            pass

    # Expiration date
    expiration_date = None
    sidebar_text = get_text(page, ".beneficio__sidebar__date")
    m = re.search(r'hasta el (\d{2}-\d{2}-\d{4})', sidebar_text, re.IGNORECASE)
    if m:
        expiration_date = parse_itau_date(m.group(1))

    # Antojo filter
    if category == "Antojo":
        full_text = f"{title} {description} {conditions}"
        if not is_food_item(full_text):
            logger.info(f"  Skipping non-food Antojo: {title}")
            return []

    base_record = {
        "bank":            "itau",
        "title":           title,
        "discount_text":   discount_text,
        "recurrence":      parse_recurrence(description),
        "conditions":      conditions,
        "description":     description,
        "image_url":       image_url,
        "expiration_date": expiration_date,
        "url":             link,
        "scraped_at":      datetime.now().isoformat(),
        "category":        category,
    }

    items: list[dict] = []

    # --- Multiple locations dropdown? ---
    select_el = page.query_selector("#standard-select")
    if select_el:
        options = page.eval_on_selector_all(
            "#standard-select option",
            "els => els.map(e => e.textContent.trim()).filter(t => t && !t.includes('Selecciona'))"
        )
        for opt_text in options:
            try:
                page.select_option("#standard-select", label=opt_text)
                page.wait_for_timeout(1500)
                addr = get_text(page, ".beneficio__sidebar__contact__location__value") or opt_text
                record = {**base_record, "location": addr, "commune": opt_text}
                items.append(record)
            except Exception as e:
                logger.warning(f"  Error selecting option '{opt_text}': {e}")
    else:
        addr = get_text(page, ".beneficio__sidebar__contact__location__value") or commune
        items.append({**base_record, "location": addr, "commune": commune})

    return items


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def scrape_itau() -> list[dict]:
    all_items:   list[dict] = []
    stats = {"total": 0, "success": 0, "failed": 0}
    start = time.time()

    with sync_playwright() as p:
        browser, context = make_browser_context(p)

        try:
            # Phase 1
            detail_urls = collect_detail_urls(context)
            stats["total"] = len(detail_urls)

            # Phase 2 — one persistent page, no driver restarts
            page = context.new_page()

            for index, (link, category) in enumerate(detail_urls.items()):
                logger.info(f"[{index+1}/{stats['total']}] {link}")

                items = scrape_detail(page, link, category)

                if items:
                    all_items.extend(items)
                    stats["success"] += 1
                    logger.info(f"  ✅ {len(items)} record(s) extracted")
                else:
                    stats["failed"] += 1
                    logger.warning(f"  ⚠️  No records for {link}")

                # Polite delay — mimics human browsing
                time.sleep(1.5)

            page.close()

        finally:
            browser.close()

    # --- Deduplication ---
    seen     = set()
    unique   = []
    for item in all_items:
        key = (item.get("title"), item.get("location"), item.get("discount_text"))
        if key not in seen:
            seen.add(key)
            unique.append(item)

    dupes = len(all_items) - len(unique)
    elapsed = time.time() - start

    logger.info("=" * 55)
    logger.info("ITAU SCRAPER SUMMARY")
    logger.info(f"  Total URLs   : {stats['total']}")
    logger.info(f"  Successful   : {stats['success']}")
    logger.info(f"  Failed       : {stats['failed']}")
    logger.info(f"  Duplicates   : {dupes}")
    logger.info(f"  Final records: {len(unique)}")
    logger.info(f"  Elapsed      : {elapsed:.1f}s  ({elapsed/max(stats['total'],1):.1f}s/url)")
    logger.info("=" * 55)

    return unique


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    items = scrape_itau()
    if items:
        out_path = os.path.join(os.path.dirname(__file__), "json", "itau.json")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ Saved {len(items)} items → {out_path}")
    else:
        logger.warning("No items scraped.")