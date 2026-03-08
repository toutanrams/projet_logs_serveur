import os
import streamlit as st
import pandas as pd
import requests
import plotly.express as px

# Configuration de l'API
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Dashboard Logs Serveur (via API)", layout="wide")
st.title("📊 Analyse des logs serveur (via API)")

# Fonction pour appeler l'API avec mise en cache
@st.cache_data(ttl=60)
def call_api(endpoint):
    try:
        response = requests.get(f"{API_URL}{endpoint}")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Erreur API {endpoint} : {e}")
        return None

# --- Métriques principales ---
st.subheader("📈 Vue d'ensemble")

total_data = call_api("/total")
if total_data:
    total = total_data["total"]
else:
    total = 0

# Période (via les données de trafic)
traffic_data = call_api("/traffic")
if traffic_data and len(traffic_data) > 0:
    df_traffic = pd.DataFrame(traffic_data)
    df_traffic['hour'] = pd.to_datetime(df_traffic['hour'])
    debut = df_traffic['hour'].min()
    fin = df_traffic['hour'].max()
else:
    debut = fin = None

col1, col2, col3 = st.columns(3)
col1.metric("Requêtes totales", f"{total:,}")
col2.metric("Début", debut.strftime("%d/%m/%Y %H:%M") if debut else "N/A")
col3.metric("Fin", fin.strftime("%d/%m/%Y %H:%M") if fin else "N/A")

# --- Graphiques ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Répartition par code HTTP")
    status_data = call_api("/status_codes")
    if status_data:
        df_status = pd.DataFrame(status_data)
        fig = px.pie(df_status, values='count', names='status', title='Codes HTTP')
        st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Trafic par heure")
    if traffic_data:
        fig = px.line(df_traffic, x='hour', y='count', title='Requêtes par heure')
        st.plotly_chart(fig, use_container_width=True)

# --- Top URLs ---
st.subheader("🔗 Top 10 des URLs les plus demandées")
top_urls_data = call_api("/top_urls?limit=10")
if top_urls_data:
    df_top = pd.DataFrame(top_urls_data)
    st.dataframe(df_top, use_container_width=True)

# --- Dernières requêtes ---
st.subheader("🕒 Dernières requêtes")
recent_data = call_api("/recent?limit=20")
if recent_data:
    df_recent = pd.DataFrame(recent_data)
    # Conversion du timestamp pour l'affichage
    if 'timestamp' in df_recent.columns:
        df_recent['timestamp'] = pd.to_datetime(df_recent['timestamp'])
    st.dataframe(df_recent, use_container_width=True)

st.markdown("---")
st.caption("Dashboard alimenté par l'API FastAPI.")