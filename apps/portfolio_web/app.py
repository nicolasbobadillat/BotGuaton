import io
import json
import os

import pandas as pd
import streamlit as st
import duckdb
from minio import Minio

# ---------------------------------------------------------------------------
# DuckDB Connection Manager
# ---------------------------------------------------------------------------
def get_duckdb_connection():
    db_path = "/workspace/dags/datitos_nam.duckdb"
    if not os.path.exists(db_path):
        db_path = "dags/datitos_nam.duckdb"
    if not os.path.exists(db_path):
        db_path = "../dags/datitos_nam.duckdb"
    try:
        con = duckdb.connect(db_path, read_only=True)
        return con, db_path
    except Exception as e:
        return None, str(e)

st.set_page_config(
    page_title="Datitos NAM Portfolio",
    page_icon="🍔",
    layout="wide",
)

st.title("🍔 Datitos NAM Portfolio (MVP)")

# ---------------------------------------------------------------------------
# Config & MinIO Connection
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
PUBLIC_BUCKET = os.environ.get("PF_BUCKET_PUBLIC", "nam-pf-public")

@st.cache_resource
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

@st.cache_data(ttl=300)
def get_available_banks():
    """List available banks by exploring the 'latest/' prefix in MinIO."""
    try:
        client = get_minio_client()
        objects = client.list_objects(PUBLIC_BUCKET, prefix="latest/", recursive=False)
        banks = []
        for obj in objects:
            if obj.is_dir or obj.object_name.endswith("/"):
                # Extract 'bank' from 'latest/bank/'
                parts = obj.object_name.strip("/").split("/")
                if len(parts) >= 2:
                    banks.append(parts[1])
        return sorted(list(set(banks)))
    except Exception as e:
        st.sidebar.error(f"Error discovering banks: {e}")
        return []

@st.cache_data(ttl=120)
def fetch_json(key):
    try:
        client = get_minio_client()
        response = client.get_object(PUBLIC_BUCKET, key)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()
        return data
    except Exception:
        return None



# ---------------------------------------------------------------------------
# Sidebar & Discovery
# ---------------------------------------------------------------------------
st.sidebar.header("Configuration")
available_banks = get_available_banks()

if not available_banks:
    st.error("❌ No banks found in 'latest/' prefix of MinIO. Ensure the pipeline has published at least one bank.")
    st.stop()

bank_options = ["All Banks"] + available_banks
bank = st.sidebar.selectbox("Select Bank", bank_options)
prefix = f"latest/{bank}" if bank != "All Banks" else "latest"

# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------
with st.spinner(f"Loading data for {bank}..."):
    con, db_status = get_duckdb_connection()
    if con is None:
        st.error(f"❌ No se pudo conectar a DuckDB.")
        st.code(db_status)
        st.stop()
        
    # Build query base
    base_query = """
        SELECT f.bank_id, r.name as title, f.discount_pct, f.valid_days, 
               l.commune as location, c.card_name as card_type, 
               f.discount_cap, f.expiration_date
        FROM fact_offers f
        JOIN dim_restaurants r ON f.restaurant_id = r.restaurant_id
        JOIN dim_locations l ON f.location_id = l.location_id
        JOIN dim_card_types c ON f.card_type_id = c.card_type_id
    """
    
    if bank == "All Banks":
        total_offers = 0
        all_qa = True
        for b in available_banks:
            b_prefix = f"latest/{b}"
            meta = fetch_json(f"{b_prefix}/metadata.json")
            if meta:
                all_qa = all_qa and meta.get("qa_passed", False)
                
        df = con.execute(base_query).df()
        
        metadata = {
            "record_count": len(df),
            "qa_passed": all_qa,
            "published_at": "Mixed (Multiple Banks)"
        }
        qa_report = None
        diff_summary = None
        
    else:
        query = base_query + f" WHERE f.bank_id = '{bank}'"
        df = con.execute(query).df()
        
        # Mandatory
        metadata = fetch_json(f"{prefix}/metadata.json")
        if metadata:
            metadata["record_count"] = len(df)
            
        # Optional
        qa_report = fetch_json(f"{prefix}/qa_report.json")
        diff_summary = fetch_json(f"{prefix}/diff_summary.json")

if df is None:
    st.error(f"⚠️ Database query failed.")
    st.stop()
if metadata is None:
    st.error(f"⚠️ Metadata missing from MinIO.")
    st.stop()

# ---------------------------------------------------------------------------
# Header KPIs
# ---------------------------------------------------------------------------
st.subheader(f"📊 {bank.capitalize()} Key Metrics")
col1, col2, col3, col4 = st.columns(4)

total_offers = metadata.get("record_count", len(df))
qa_passed = metadata.get("qa_passed", False)
published_at = metadata.get("published_at", "N/A")

# Format date nicely
try:
    published_fmt = pd.to_datetime(published_at).strftime("%Y-%m-%d %H:%M")
except:
    published_fmt = published_at

col1.metric("Total Offers", total_offers)
col2.metric("QA Passed", "✅ Yes" if qa_passed else "❌ No")
col3.metric("Last Published", published_fmt)

if diff_summary and "counts" in diff_summary:
    counts = diff_summary["counts"]
    diff_str = f"+{counts.get('added',0)} / -{counts.get('removed',0)} / ~{counts.get('changed',0)}"
    col4.metric("Daily Changes", diff_str)
else:
    col4.metric("Daily Changes", "N/A", help="Diff summary not available for this run.")

# ---------------------------------------------------------------------------
# Filterable Data Table
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("🛍️ Offers Catalog")

# Filters
f_col1, f_col2, f_col3, f_col4 = st.columns(4)
title_q = f_col1.text_input("Search Title", "")
loc_q = f_col2.text_input("Search Location", "")

card_types = sorted(df["card_type"].dropna().unique().tolist())
card_q = f_col3.multiselect("Card Type", options=card_types, default=[])

days_column = "valid_days" if "valid_days" in df.columns else None
if days_column:
    days_types = sorted([d for d in df[days_column].unique().tolist() if pd.notna(d) and d != ""])
    days_q = f_col4.multiselect("Valid Days", options=days_types, default=[])
else:
    days_q = []

filtered_df = df.copy()
if title_q:
    filtered_df = filtered_df[filtered_df["title"].str.contains(title_q, case=False, na=False)]
if loc_q:
    filtered_df = filtered_df[filtered_df["location"].str.contains(loc_q, case=False, na=False)]
if card_q:
    filtered_df = filtered_df[filtered_df["card_type"].isin(card_q)]
if days_q:
    filtered_df = filtered_df[filtered_df[days_column].isin(days_q)]

if bank == "All Banks":
    # Show bank_id explicitly when viewing all banks
    display_cols = ["bank_id", "title", "discount_pct", "valid_days", "location", "card_type", "discount_cap", "expiration_date"] if days_column else ["bank_id", "title", "discount_pct", "location", "card_type", "discount_cap", "expiration_date"]
else:
    display_cols = ["title", "discount_pct", "valid_days", "location", "card_type", "discount_cap", "expiration_date"] if days_column else ["title", "discount_pct", "location", "card_type", "discount_cap", "expiration_date"]

# Ensure columns actually exist before trying to display them
display_cols = [c for c in display_cols if c in filtered_df.columns]

st.dataframe(
    filtered_df[display_cols],
    use_container_width=True,
    hide_index=True
)
st.markdown(f"**Showing {len(filtered_df)} of {len(df)} offers.**")


# ---------------------------------------------------------------------------
# QA details & Diff details
# ---------------------------------------------------------------------------
st.markdown("---")
b_col1, b_col2 = st.columns(2)

with b_col1:
    st.subheader("✅ Data Quality Details")
    if qa_report and "checks" in qa_report:
        for m_name, check in qa_report["checks"].items():
            status_icon = "🟢" if check["status"] == "PASS" else ("🟡" if check["status"] == "WARN" else "🔴")
            st.markdown(f"{status_icon} **{m_name}**: {check['detail']}")
    else:
        st.info("No QA report details found for this bank.")

with b_col2:
    st.subheader("🔄 Daily Changes Breakdown")
    if diff_summary:
        if "counts" in diff_summary:
            st.json(diff_summary["counts"])
        if diff_summary.get("status") == "fallback":
            st.caption("ℹ️ First run or no previous snapshot to compare against (fallback mode).")
    else:
        st.warning("No daily diff summary found.")
