"""
DuckDB diff engine.
Compares current fact_offers vs previous offers_snapshot to detect
added/removed/changed offers. Writes results to diff_offers table.
"""
import json
import os
from datetime import datetime, timezone

import duckdb


DEFAULT_DUCKDB_PATH = "/opt/airflow/dags/datitos_nam.duckdb"


def compute_duckdb_diff(
    diff_date: str,
    run_id: str,
    db_path: str | None = None,
) -> dict:
    """
    Compute diff between current fact_offers and previous snapshot.

    1. Finds latest snapshot_date < diff_date in offers_snapshot.
    2. Guardrail: skips diff if current fact_offers is empty.
    3. Computes added/removed/changed.
    4. Inserts into diff_offers (idempotent per diff_date + run_id).
    5. Returns summary dict.
    """
    db_path = db_path or os.environ.get("PF_DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
    con = duckdb.connect(db_path)
    try:
        # Find previous snapshot date
        prev = con.execute("""
            SELECT MAX(snapshot_date) FROM offers_snapshot
            WHERE snapshot_date < CAST(? AS DATE)
        """, [diff_date]).fetchone()[0]

        current_count = con.execute("SELECT count(*) FROM fact_offers").fetchone()[0]

        # Guardrail: empty current → skip diff to avoid false mass-removals
        if current_count == 0:
            print(f"⚠️ fact_offers is empty for {diff_date} — skipping diff")
            return {
                "diff_date": diff_date,
                "run_id": run_id,
                "prev_snapshot_date": str(prev) if prev else None,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "counts": {
                    "added": 0, "removed": 0, "changed": 0,
                    "current_total": 0,
                    "previous_total": 0,
                },
                "status": "empty_current",
            }

        if prev is None:
            print(f"💡 No previous snapshot found before {diff_date} — first run fallback")
            return {
                "diff_date": diff_date,
                "run_id": run_id,
                "prev_snapshot_date": None,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "counts": {
                    "added": 0, "removed": 0, "changed": 0,
                    "current_total": current_count,
                    "previous_total": 0,
                },
                "status": "no_previous_snapshot",
            }

        prev_str = str(prev)
        prev_count = con.execute("""
            SELECT count(*) FROM offers_snapshot WHERE snapshot_date = CAST(? AS DATE)
        """, [prev_str]).fetchone()[0]

        print(f"📊 Diff: current fact_offers ({current_count}) vs snapshot {prev_str} ({prev_count})")

        # Clear previous diff for this date — ensures reruns overwrite instead of appending
        con.execute(
            "DELETE FROM diff_offers WHERE diff_date = CAST(? AS DATE)",
            [diff_date],
        )

        # ADDED: in fact_offers but NOT in previous snapshot
        con.execute("""
            INSERT INTO diff_offers
                (diff_date, run_id, diff_type, offer_id,
                 restaurant_id, bank_id, card_type_id, location_id,
                 valid_days, discount_pct, discount_cap)
            SELECT
                CAST(? AS DATE), ?, 'added', f.offer_id,
                f.restaurant_id, f.bank_id, f.card_type_id, f.location_id,
                f.valid_days, f.discount_pct, f.discount_cap
            FROM fact_offers f
            WHERE f.offer_id NOT IN (
                SELECT offer_id FROM offers_snapshot WHERE snapshot_date = CAST(? AS DATE)
            )
        """, [diff_date, run_id, prev_str])

        added = con.execute("""
            SELECT count(*) FROM diff_offers
            WHERE diff_date = CAST(? AS DATE) AND run_id = ? AND diff_type = 'added'
        """, [diff_date, run_id]).fetchone()[0]

        # REMOVED: in previous snapshot but NOT in fact_offers
        con.execute("""
            INSERT INTO diff_offers
                (diff_date, run_id, diff_type, offer_id,
                 restaurant_id, bank_id, card_type_id, location_id,
                 valid_days, discount_pct, discount_cap)
            SELECT
                CAST(? AS DATE), ?, 'removed', s.offer_id,
                s.restaurant_id, s.bank_id, s.card_type_id, s.location_id,
                s.valid_days, s.discount_pct, s.discount_cap
            FROM offers_snapshot s
            WHERE s.snapshot_date = CAST(? AS DATE)
              AND s.offer_id NOT IN (SELECT offer_id FROM fact_offers)
        """, [diff_date, run_id, prev_str])

        removed = con.execute("""
            SELECT count(*) FROM diff_offers
            WHERE diff_date = CAST(? AS DATE) AND run_id = ? AND diff_type = 'removed'
        """, [diff_date, run_id]).fetchone()[0]

        # CHANGED: same offer_id in both, but discount_pct/valid_days/discount_cap differ
        con.execute("""
            INSERT INTO diff_offers
                (diff_date, run_id, diff_type, offer_id,
                 restaurant_id, bank_id, card_type_id, location_id,
                 valid_days, discount_pct, discount_cap,
                 prev_valid_days, prev_discount_pct, prev_discount_cap)
            SELECT
                CAST(? AS DATE), ?, 'changed', f.offer_id,
                f.restaurant_id, f.bank_id, f.card_type_id, f.location_id,
                f.valid_days, f.discount_pct, f.discount_cap,
                s.valid_days, s.discount_pct, s.discount_cap
            FROM fact_offers f
            JOIN offers_snapshot s
              ON f.offer_id = s.offer_id AND s.snapshot_date = CAST(? AS DATE)
            WHERE f.discount_pct IS DISTINCT FROM s.discount_pct
               OR f.valid_days IS DISTINCT FROM s.valid_days
               OR f.discount_cap IS DISTINCT FROM s.discount_cap
        """, [diff_date, run_id, prev_str])

        changed = con.execute("""
            SELECT count(*) FROM diff_offers
            WHERE diff_date = CAST(? AS DATE) AND run_id = ? AND diff_type = 'changed'
        """, [diff_date, run_id]).fetchone()[0]

        summary = {
            "diff_date": diff_date,
            "run_id": run_id,
            "prev_snapshot_date": prev_str,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "counts": {
                "added": added,
                "removed": removed,
                "changed": changed,
                "current_total": current_count,
                "previous_total": prev_count,
            },
            "status": "ok",
        }

        print(f"📊 Diff result: +{added} / -{removed} / ~{changed}")
        return summary

    finally:
        con.close()
