import os
import sys
import json
import logging
from datetime import datetime

# Add dags to path to allow importing ripley_scraper
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import ripley_scraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("RipleyRunner")

def run_local():
    logger.info("Iniciando scraper LOCAL de Ripley (Usando Playwright)...")
    
    scraper = ripley_scraper.RipleyScraper()
    data = scraper.scrape()
    
    if not data:
        logger.error("No se extrajeron datos!")
        return

    output_path = os.path.join(os.path.dirname(__file__), "..", "json", "ripley.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    logger.info(f"✅ Guardados {len(data)} items en {output_path}")
    
    # Análisis rápido de Meses para feedback al usuario
    marzo_count = sum(1 for i in data if i.get("expiration_date") and "2026-03" in str(i["expiration_date"]))
    
    print("\n--- Análisis de Vigencia ---")
    print(f"Items con vencimiento en Marzo: {marzo_count}")
    
    if marzo_count > 0:
        print(f"✅ Se detectaron {marzo_count} items con vigencia en Marzo.")
    else:
        print("⚠️ No se detectaron vigencias normalizadas a Marzo. Verificar selectores.")

if __name__ == "__main__":
    run_local()
