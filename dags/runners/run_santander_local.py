"""
Run Santander Scraper locally and save to JSON file.
Santander scraper already saves to dags/json/santander.json in its finally block.
"""
import santander_scraper

if __name__ == "__main__":
    print("Starting Santander Scraper (Local)...")
    data = santander_scraper.scrape_santander()
    if data:
        print(f"✅ Scraped {len(data)} items -> json/santander.json")
    else:
        print("No items collected!")

