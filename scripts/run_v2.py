"""
Run V2 Schema and Transform
Executes the new normalized schema and ETL logic

Configurable via environment variables:
  DUCKDB_PATH     – path to the DuckDB database file
  JSON_BASE_PATH  – directory containing bank JSON files
  SQL_BASE_PATH   – directory containing sql/ folder (schema, ref_locations, transformers)

Defaults resolve to local repo paths so existing usage is unchanged.
"""
import duckdb
import pandas as pd
import os

# ---------------------------------------------------------------------------
# Paths – configurable by env, with local-repo defaults
# ---------------------------------------------------------------------------
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
SQL_DIR         = os.environ.get('SQL_BASE_PATH',  os.path.join(BASE_DIR, 'sql'))
DB_PATH         = os.environ.get('DUCKDB_PATH',    os.path.join(os.path.dirname(BASE_DIR), 'datitos_nam.duckdb'))
JSON_BASE_PATH  = os.environ.get('JSON_BASE_PATH', os.path.join(BASE_DIR, 'json'))

# Normalise to forward-slash (DuckDB on Windows needs this)
JSON_BASE_PATH = JSON_BASE_PATH.replace('\\', '/')


def read_sql(path: str) -> str:
    """Read a .sql file and substitute the {{JSON_BASE_PATH}} placeholder."""
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    return sql.replace('{{JSON_BASE_PATH}}', JSON_BASE_PATH)


# ---------------------------------------------------------------------------
print(f"DB_PATH        = {DB_PATH}")
print(f"SQL_DIR        = {SQL_DIR}")
print(f"JSON_BASE_PATH = {JSON_BASE_PATH}")

# Connect to existing database (do NOT delete!)
print(f"\nConnecting to database at: {DB_PATH}")
con = duckdb.connect(DB_PATH)

# Execute schema
print("\n--- Executing Schema V2 ---")
schema_sql = read_sql(os.path.join(SQL_DIR, 'schema_v2.sql'))

try:
    con.execute(schema_sql)
    print("Schema created successfully!")
except Exception as e:
    print(f"Schema Error: {e}")
    exit(1)

# Load ref_locations (reference table for region filtering)
print("\n--- Loading Reference Locations ---")
ref_sql = read_sql(os.path.join(SQL_DIR, 'ref_locations.sql'))

try:
    con.execute(ref_sql)
    print("Reference locations loaded successfully!")
except Exception as e:
    print(f"Ref Locations Error: {e}")
    exit(1)

# Execute transformers
print("\n--- Executing Modular Transformers ---")
print("Starting transaction...")
con.execute("BEGIN TRANSACTION;")

TRANSFORMERS_DIR = os.path.join(SQL_DIR, 'transformers')
errors = []
for sql_file in sorted(os.listdir(TRANSFORMERS_DIR)):
    if sql_file.endswith('.sql'):
        print(f"Running transformer: {sql_file}")
        sql = read_sql(os.path.join(TRANSFORMERS_DIR, sql_file))
        try:
            con.execute(sql)
        except Exception as e:
            print(f"❌ Error in {sql_file}: {e}")
            errors.append(sql_file)

if errors:
    print(f"\n⚠️ {len(errors)} transformer(s) failed: {errors}")
    print("Rolling back transaction. Database remains unmodified.")
    con.execute("ROLLBACK;")
    exit(1)
else:
    print("\nAll transformers executed successfully. Committing transaction.")
    con.execute("COMMIT;")

# Show stats
print("\n--- Database Stats ---")
print("\nRestaurants:")
print(con.query("SELECT COUNT(*) as total, SUM(CASE WHEN is_chain THEN 1 ELSE 0 END) as chains FROM dim_restaurants").df())

print("\nOffers by Bank:")
print(con.query("""
    SELECT b.bank_name, COUNT(*) as offers 
    FROM fact_offers f 
    JOIN dim_banks b ON f.bank_id = b.bank_id 
    GROUP BY b.bank_name 
    ORDER BY offers DESC
""").df())

print("\nOffers by Card Type:")
print(con.query("""
    SELECT c.bank_id, c.card_name, COUNT(*) as offers 
    FROM fact_offers f 
    JOIN dim_card_types c ON f.card_type_id = c.card_type_id 
    GROUP BY c.bank_id, c.card_name 
    ORDER BY c.bank_id, offers DESC
""").df())

print("\nTotal Offers:")
print(con.query("SELECT COUNT(*) as total FROM fact_offers").df())

# Sample query: Find restaurant with multiple offers
print("\n--- Sample: Restaurant with Multiple Offers ---")
print(con.query("""
    SELECT r.name, b.bank_name, c.card_name, f.valid_days, f.discount_pct, l.commune as location
    FROM fact_offers f
    JOIN dim_restaurants r ON f.restaurant_id = r.restaurant_id
    JOIN dim_banks b ON f.bank_id = b.bank_id
    JOIN dim_card_types c ON f.card_type_id = c.card_type_id
    LEFT JOIN dim_locations l ON f.location_id = l.location_id
    WHERE r.name ILIKE '%Muu%' OR r.name ILIKE '%Kechua%'
    ORDER BY r.name, b.bank_name
    LIMIT 20
""").df())


con.close()

# NOTE: Manual inserts and overrides are now handled by SQL transformers:
#   - zz_01_user_knowledge.sql  (scraper patches)
#   - zz_02_overrides.sql       (corrections)
# No need for external manual_inserts.py anymore.

print("\n✓ Done!")
