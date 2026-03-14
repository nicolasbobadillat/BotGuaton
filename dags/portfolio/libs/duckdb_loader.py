"""
DuckDB loader for portfolio.
Builds a local DuckDB file from SQL schema + transformers using JSON inputs.
"""
import os
import duckdb


def _read_sql(path: str, json_base_path: str) -> str:
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    return sql.replace('{{JSON_BASE_PATH}}', json_base_path)


def build_duckdb(
    db_path: str,
    sql_dir: str,
    json_base_path: str,
    snapshot_date: str | None = None,
    run_id: str | None = None,
) -> None:
    """
    Build DuckDB using schema_v2.sql + ref_locations.sql + transformers/*.sql.

    If snapshot_date is provided, snapshots current fact_offers into
    offers_snapshot BEFORE running the transformers (persistent, for diff).
    Raises RuntimeError if any transformer fails.
    """
    json_base_path = json_base_path.replace('\\', '/')
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    con = duckdb.connect(db_path)
    try:
        # 1. Schema + ref data
        schema_sql = _read_sql(os.path.join(sql_dir, 'schema_v2.sql'), json_base_path)
        con.execute(schema_sql)

        ref_sql = _read_sql(os.path.join(sql_dir, 'ref_locations.sql'), json_base_path)
        con.execute(ref_sql)

        # 2. Snapshot current fact_offers BEFORE rebuild
        if snapshot_date and run_id:
            row_count = con.execute("SELECT count(*) FROM fact_offers").fetchone()[0]
            if row_count > 0:
                # Rerun replaces: delete existing snapshot for this date
                con.execute(
                    "DELETE FROM offers_snapshot WHERE snapshot_date = CAST(? AS DATE)",
                    [snapshot_date],
                )
                con.execute("""
                    INSERT INTO offers_snapshot
                        (snapshot_date, run_id, offer_id, restaurant_id, bank_id,
                         card_type_id, location_id, valid_days, discount_pct, discount_cap)
                    SELECT
                        CAST(? AS DATE), ?,
                        offer_id, restaurant_id, bank_id, card_type_id,
                        location_id, valid_days, discount_pct, discount_cap
                    FROM fact_offers
                """, [snapshot_date, run_id])
                print(f"📸 Snapshot: {row_count} offers saved for {snapshot_date}")
            else:
                print(f"📸 Snapshot: fact_offers empty, nothing to snapshot for {snapshot_date}")

        # 2b. Clear fact_offers AFTER snapshot to avoid FK violations during rebuild
        con.execute("DELETE FROM fact_offers")

        # 3. Run transformers
        transformers_dir = os.path.join(sql_dir, 'transformers')
        errors = []
        con.execute('BEGIN TRANSACTION;')
        import json
        for sql_file in sorted(os.listdir(transformers_dir)):
            if not sql_file.endswith('.sql'):
                continue
                
            # Skip if the corresponding JSON is explicitly an empty list
            bank_name = sql_file.replace('.sql', '')
            json_file = os.path.join(json_base_path, f"{bank_name}.json")
            if os.path.exists(json_file):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if not data:
                        print(f"⏩ Skipping {sql_file} because {bank_name}.json is empty (scraper failed/no data).")
                        continue
                except Exception:
                    pass

            sql = _read_sql(os.path.join(transformers_dir, sql_file), json_base_path)
            try:
                con.execute(sql)
            except Exception as e:
                errors.append(f"{sql_file}: {e}")

        if errors:
            con.execute('ROLLBACK;')
            raise RuntimeError('Transformer failures: ' + ' | '.join(errors))
        con.execute('COMMIT;')
    finally:
        con.close()

