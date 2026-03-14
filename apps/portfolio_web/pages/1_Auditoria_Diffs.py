import streamlit as st
import duckdb
import pandas as pd
import os
from datetime import datetime

st.set_page_config(
    page_title="Auditoría Diffs | Datitos NAM",
    page_icon="🔍",
    layout="wide",
)

st.title("🔍 Auditoría de Diferencias (Diffs y Outliers)")
st.markdown("Esta vista se conecta directamente a la base de datos DuckDB para monitorear qué está cambiando exactamente día a día en el pipeline.")

# ---------------------------------------------------------------------------
# DuckDB Connection Manager
# ---------------------------------------------------------------------------
@st.cache_resource
def get_duckdb_connection():
    db_path = "dags/datitos_nam.duckdb"

    try:
        # read_only=True is CRITICAL to avoid locking Airflow's write processes
        con = duckdb.connect(db_path, read_only=True)
        return con, db_path
    except Exception as e:
        return None, str(e)

con, db_status = get_duckdb_connection()

if con is None:
    st.error(f"❌ No se pudo conectar a DuckDB.")
    st.code(db_status)
    st.info("Asegúrate de que la ruta 'dags/datitos_nam.duckdb' exista y sea accesible.")
    st.stop()

# ---------------------------------------------------------------------------
# Data Fetching
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def fetch_latest_diff_date():
    try:
        res = con.execute("SELECT MAX(diff_date) FROM diff_offers").fetchone()
        return res[0] if res else None
    except:
        return None

@st.cache_data(ttl=60)
def fetch_diff_summary(date):
    if not date: return pd.DataFrame()
    query = """
        SELECT diff_type, count(*) as cantidad
        FROM diff_offers
        WHERE diff_date = CAST(? AS DATE)
        GROUP BY diff_type
    """
    return con.execute(query, [date]).df()

@st.cache_data(ttl=60)
def fetch_diff_details(date, diff_type):
    if not date: return pd.DataFrame()
    
    if diff_type == 'added':
        query = """
            SELECT 
                d.bank_id,
                COALESCE(r.name, d.restaurant_id) as restaurante,
                d.discount_pct as descuento,
                d.valid_days as dias_validos,
                f.conditions as detalle_oferta
            FROM diff_offers d
            LEFT JOIN dim_restaurants r ON d.restaurant_id = r.restaurant_id
            LEFT JOIN fact_offers f ON d.offer_id = f.offer_id
            WHERE d.diff_date = CAST(? AS DATE) AND d.diff_type = 'added'
            ORDER BY d.bank_id, d.discount_pct DESC
        """
    elif diff_type == 'removed':
        query = """
            SELECT 
                d.bank_id,
                COALESCE(r.name, d.restaurant_id) as restaurante_eliminado,
                d.discount_pct as descuento_previo,
                d.valid_days as dias_previos
            FROM diff_offers d
            LEFT JOIN dim_restaurants r ON d.restaurant_id = r.restaurant_id
            WHERE d.diff_date = CAST(? AS DATE) AND d.diff_type = 'removed'
            ORDER BY d.bank_id, d.discount_pct DESC
        """
    elif diff_type == 'changed':
        query = """
            SELECT 
                d.bank_id,
                COALESCE(r.name, d.restaurant_id) as restaurante,
                f.conditions as detalle_oferta,
                CASE 
                    WHEN d.prev_discount_pct IS DISTINCT FROM d.discount_pct AND d.prev_valid_days IS DISTINCT FROM d.valid_days 
                        THEN CONCAT('Descuento: ', COALESCE(CAST(d.prev_discount_pct AS VARCHAR), 'N/A'), '% -> ', COALESCE(CAST(d.discount_pct AS VARCHAR), 'N/A'), '%  ||  Días: ', COALESCE(d.prev_valid_days, 'N/A'), ' -> ', COALESCE(d.valid_days, 'N/A'))
                    WHEN d.prev_discount_pct IS DISTINCT FROM d.discount_pct 
                        THEN CONCAT('Descuento: ', COALESCE(CAST(d.prev_discount_pct AS VARCHAR), 'N/A'), '% -> ', COALESCE(CAST(d.discount_pct AS VARCHAR), 'N/A'), '%')
                    WHEN d.prev_valid_days IS DISTINCT FROM d.valid_days 
                        THEN CONCAT('Días: ', COALESCE(d.prev_valid_days, 'N/A'), ' -> ', COALESCE(d.valid_days, 'N/A'))
                    ELSE 'Otra condición'
                END as que_cambio
            FROM diff_offers d
            LEFT JOIN dim_restaurants r ON d.restaurant_id = r.restaurant_id
            LEFT JOIN fact_offers f ON d.offer_id = f.offer_id
            WHERE d.diff_date = CAST(? AS DATE) AND d.diff_type = 'changed'
            ORDER BY d.bank_id
        """
    else:
        return pd.DataFrame()
        
    return con.execute(query, [date]).df()

@st.cache_data(ttl=60)
def fetch_outliers():
    # Detect anomalous data in current fact_offers
    query = """
        SELECT 
            bank_id,
            conditions as oferta,
            discount_pct as descuento_anomalo,
            'Descuento Inusualmente Alto (>50%)' as tipo_alerta
        FROM fact_offers
        WHERE discount_pct > 50 AND discount_pct <= 100
        
        UNION ALL
        
        SELECT 
            bank_id,
            conditions as oferta,
            NULL as descuento_anomalo,
            'Locación Inválida (n/a)' as tipo_alerta
        FROM fact_offers
        WHERE location_id = 'n/a'
        
        UNION ALL
        
        SELECT
            bank_id,
            conditions as oferta,
            NULL as descuento_anomalo,
            'Restaurante No Mapeado (Extracción Fallida)' as tipo_alerta
        FROM fact_offers
        WHERE restaurant_id IS NULL OR restaurant_id = ''
        
        ORDER BY bank_id, tipo_alerta
    """
    try:
        return con.execute(query).df()
    except:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# UI Construction
# ---------------------------------------------------------------------------
latest_date = fetch_latest_diff_date()

if not latest_date:
    st.warning("⚠️ No se encontraron registros en la tabla `diff_offers`. Asegúrate de haber corrido orquestador tras haber guardado el snapshot previo.")
    st.stop()

st.header(f"📅 Resumen del Día: {latest_date}")
summary_df = fetch_diff_summary(latest_date)

# Create KPIs
added_count = int(summary_df[summary_df['diff_type'] == 'added']['cantidad'].sum()) if not summary_df.empty else 0
removed_count = int(summary_df[summary_df['diff_type'] == 'removed']['cantidad'].sum()) if not summary_df.empty else 0
changed_count = int(summary_df[summary_df['diff_type'] == 'changed']['cantidad'].sum()) if not summary_df.empty else 0

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("🟢 Ofertas Añadidas", added_count)
kpi2.metric("🔴 Ofertas Eliminadas", removed_count)
kpi3.metric("🟡 Ofertas Modificadas", changed_count)

st.markdown("---")
st.subheader("📋 Detalle de Cambios")

tab1, tab2, tab3, tab4 = st.tabs(["🟢 Añadidas", "🔴 Eliminadas", "🟡 Modificadas", "🚨 Outliers & Alertas"])

with tab1:
    df_added = fetch_diff_details(latest_date, 'added')
    if not df_added.empty:
        # Optional filters
        sel_bank = st.multiselect("Filtrar por Banco", options=df_added['bank_id'].unique(), key="add_bank")
        if sel_bank:
            df_added = df_added[df_added['bank_id'].isin(sel_bank)]
        st.dataframe(df_added, use_container_width=True, hide_index=True)
    else:
        st.info("No hay nuevas ofertas añadidas en esta fecha.")

with tab2:
    df_removed = fetch_diff_details(latest_date, 'removed')
    if not df_removed.empty:
        sel_bank_rm = st.multiselect("Filtrar por Banco", options=df_removed['bank_id'].unique(), key="rm_bank")
        if sel_bank_rm:
            df_removed = df_removed[df_removed['bank_id'].isin(sel_bank_rm)]
        st.dataframe(df_removed, use_container_width=True, hide_index=True)
    else:
        st.info("Ninguna oferta fue eliminada en esta fecha.")

with tab3:
    df_changed = fetch_diff_details(latest_date, 'changed')
    if not df_changed.empty:
        st.dataframe(df_changed, use_container_width=True, hide_index=True)
    else:
        st.info("Ninguna oferta cambió sus condiciones matemáticas hoy.")

with tab4:
    st.markdown("Revisa posibles errores de extracción o promociones inusuales.")
    df_outliers = fetch_outliers()
    if not df_outliers.empty:
        st.dataframe(df_outliers, use_container_width=True, hide_index=True)
    else:
        st.success("¡Todo limpio! No se detectaron anomalías.")

# Footer info
st.caption(f"Base de datos conectada: `{db_status}`")
