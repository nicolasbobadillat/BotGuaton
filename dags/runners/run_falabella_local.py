"""
Runner local para Falabella (Versión Playwright / SPA Click Mode)
Usa el scraper visual que captura Vapiano y filtra zombies.
"""
import json
import os
import sys

# Add dags to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from banco_falabella_scraper import BancoScraper

def main():
    print("--- Ejecutando Scraper Falabella (Playwright) ---")
    scraper = BancoScraper()
    
    # Run Scrape
    data = scraper.scrape()
    
    # Save to Local JSON for Loader
    output_file = os.path.join(os.path.dirname(__file__), '..', 'json', 'falabella.json')
    
    print(f"--- FInalizado: {len(data)} items ---")
    print(f"Guardando en {output_file}...")
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    print("Guadado exitoso.")

if __name__ == "__main__":
    main()
