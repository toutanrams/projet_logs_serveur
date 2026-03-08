from fastapi import FastAPI, HTTPException
import psycopg2
import pandas as pd
from typing import List, Dict
from datetime import datetime

app = FastAPI(
    title="Logs Server API",
    description="API pour accéder aux métriques des logs",
    version="1.0.0"
)

# Configuration de la base de données (à adapter si besoin)
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "logs"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres")
}

def get_db_connection():
    """Retourne une connexion à PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)

@app.get("/")
def root():
    return {"message": "Bienvenue sur l'API des logs serveur", "docs": "/docs"}

@app.get("/health")
def health():
    """Vérifie la connexion à la base."""
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/total")
def total_requests():
    """Retourne le nombre total de requêtes."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM logs")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()
    return {"total": total}

@app.get("/top_urls")
def top_urls(limit: int = 10):
    """Retourne les URLs les plus demandées (limit par défaut 10)."""
    conn = get_db_connection()
    query = """
        SELECT url, COUNT(*) as count
        FROM logs
        GROUP BY url
        ORDER BY count DESC
        LIMIT %s
    """
    df = pd.read_sql(query, conn, params=[limit])
    conn.close()
    return df.to_dict(orient='records')

@app.get("/status_codes")
def status_codes():
    """Retourne la répartition des codes HTTP."""
    conn = get_db_connection()
    query = """
        SELECT status, COUNT(*) as count
        FROM logs
        GROUP BY status
        ORDER BY status
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict(orient='records')

@app.get("/traffic")
def traffic_by_hour():
    """Retourne le nombre de requêtes par heure."""
    conn = get_db_connection()
    query = """
        SELECT date_trunc('hour', timestamp) as hour, COUNT(*) as count
        FROM logs
        GROUP BY hour
        ORDER BY hour
    """
    df = pd.read_sql(query, conn)
    # Convertir les datetime en chaînes pour JSON
    df['hour'] = df['hour'].astype(str)
    conn.close()
    return df.to_dict(orient='records')

@app.get("/recent")
def recent_logs(limit: int = 20):
    """Retourne les dernières lignes de logs."""
    conn = get_db_connection()
    query = """
        SELECT timestamp, ip, method, url, status, size
        FROM logs
        ORDER BY timestamp DESC
        LIMIT %s
    """
    df = pd.read_sql(query, conn, params=[limit])
    df['timestamp'] = df['timestamp'].astype(str)
    conn.close()
    return df.to_dict(orient='records')