import os
import sys

# Add dags to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from bci_scraper import get_bci_discounts_final

if __name__ == "__main__":
    print("--- Running BCI Scraper (Local) ---")
    data = get_bci_discounts_final()
    print(f"--- Finished. Total items: {len(data)} ---")
    
    # Check for keywords
    antojos = [x for x in data if 'antojo' in str(x).lower()]
    print(f"Items with 'antojo': {len(antojos)}")
    
    # Check caps/exps
    caps = [x for x in data if x.get('discount_cap')]
    exps = [x for x in data if x.get('expiration_date')]
    print(f"Items with Cap: {len(caps)}")
    print(f"Items with Exp: {len(exps)}")
