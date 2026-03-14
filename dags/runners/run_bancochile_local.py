import os
import sys
import json
import logging

# Add parent dir to path to allow importing from dags
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from dags.bancochile_scraper import scrape_bancochile
except ImportError:
    try:
        from bancochile_scraper import scrape_bancochile
    except ImportError:
        # If running from dags/runners/ directly
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from bancochile_scraper import scrape_bancochile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    print("--- Running Banco de Chile Scraper Locally ---")
    items = scrape_bancochile()
    
    if items:
        # Save to dags/json/bancochile.json
        output_dir = os.path.join(os.path.dirname(__file__), '..', 'json')
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'bancochile.json')
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=4)
            
        print(f"✅ Successfully scraped {len(items)} items and saved to {output_path}")
        
        # Print sample of new fields
        print("\nSample of new fields (Top 5 with constraints):")
        count = 0
        for item in items:
            if item.get('discount_cap') or item.get('expiration_date'):
                print(f"- {item['title']}: Cap=${item.get('discount_cap')}, Exp={item.get('expiration_date')}")
                count += 1
                if count >= 5: break
    else:
        print("❌ No items scraped!")
