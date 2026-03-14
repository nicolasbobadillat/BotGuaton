# 🍔 Datitos NAM Portfolio — Discounts Analytics Pipeline

**Datitos NAM** is an automated data pipeline that scrapes, normalizes, and analyzes retail/bank discounts and promotions across Chile (e.g., Santander, Itaú, Banco de Chile). 

This repository highlights a **modern analytics architecture** designed to replace legacy flat-file systems, featuring a fully relational analytics data warehouse and an interactive Audit Dashboard.

---

## 🏗️ Architecture & Core Features

### 1. High-Performance Analytics (DuckDB)
Migrated descriptive flat-file loads (Parquets) to a **relational, in-memory OLAP table model via DuckDB**.
*   **Multi-Location Support**: Enabled advanced SQL setups to accurately map individual mall branches and recursive chains, solving high-density query anomalies for complex queries (e.g., chains like *Tanta*).
*   **Normalised Schema**: Separates facts (`fact_offers`) from descriptive dimension tables (`dim_restaurants`, `dim_locations`, `dim_card_types`).

### 2. Persistent Diffing Engine (Daily Audits)
Tracks day-over-day changes to detect anomalies or changes in promotions.
*   **Logic**: Uses snapshot buckets (`offers_snapshot`) to benchmark current records vs historical rows.
*   **Categories**: Categorizes changes into `Added`, `Removed`, and `Modified` (using SQL `IS DISTINCT FROM` logic to safeguard against Null exceptions).

### 3. Streamlit Analytics & Audit Portal
*   **Catalog Viewer**: Multiselect queries, dynamic joins on locations.
*   **Control Panel**: View daily diff stats directly from the live DuckDB reader instance.
*   **Outlier Alarms**: Automatically flags row exceptions (e.g., discounts > 50%) directly in front-end pages before updates take full execution.

---

## 🛠️ Tech Stack

*   **Database / OLAP**: DuckDB (Persistent Storage & Vector Processing)
*   **Scrapers / Scraping framework**: Playwright with recursive SPA Modals (SPA Navigation fallback setup), and BeautifulSoup.
*   **Frontend UI**: Streamlit (Native SQL reader layout).
*   **Orchestration / DAG Management**: Apache Airflow.
*   **Backing / Archive logic**: MinIO object storage (Runtime log diagnostics & backups).

---

## 📂 Key Files & Workspace Setup

*   `dags/portfolio/libs/duckdb_diff.py`: Central Diffing Engine core loop.
*   `apps/portfolio_web/pages/1_Auditoria_Diffs.py`: Audit Dashboard Streamlit interface.
*   `apps/portfolio_web/app.py`: High-density joining Catalog browser.
*   `dags/sql/schema_v2.sql`: Logical data definitions supporting constraints.
*   `docker-compose.yml`: Container descriptions for quick stack isolations.

---

## 🚀 How to Run (Local Quickstart)

1. **Deploy Containers**:
   ```bash
   docker-compose up -d
   ```
2. **Setup Sub-Paths**:
   If dealing with absolute string mappings in legacy templates:
   ```powershell
   powershell scripts/patch_sql_paths.ps1
   ```
3. **Execute Setup**:
   Trigger `pf_orchestrator_duckdb` in the Airflow dashboard and inspect the analytical outputs inside the Steamlit setup.

