"""
DAG Airflow pour traiter les logs serveur.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import re
import psycopg2
from botocore.client import Config
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def list_files(**context):
    """Liste les objets dans le bucket logs-bruts."""
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    response = s3.list_objects_v2(Bucket='logs-bruts')
    files = [obj['Key'] for obj in response.get('Contents', [])]
    context['task_instance'].xcom_push(key='files', value=files)
    return files

def parse_and_load(**context):
    """Télécharge chaque fichier, le parse et charge dans PostgreSQL."""
    files = context['task_instance'].xcom_pull(key='files', task_ids='list_files')
    if not files:
        print("Aucun fichier trouvé.")
        return

    conn = psycopg2.connect(
        host='postgres_logs',
        database='logs',
        user='postgres',
        password='postgres'
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            ip VARCHAR(15),
            timestamp TIMESTAMP,
            method VARCHAR(10),
            url TEXT,
            status INT,
            size INT,
            ingestion_time TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()

    pattern = re.compile(r'(\S+) - - \[(.*?)\] "(\S+) (\S+) HTTP/\d\.\d" (\d+) (\d+)')

    for file_key in files:
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        local_path = f'/tmp/{file_key}'
        s3.download_file('logs-bruts', file_key, local_path)

        data = []
        with open(local_path, 'r') as f:
            for line in f:
                match = pattern.match(line)
                if match:
                    ip, timestamp_str, method, url, status, size = match.groups()
                    try:
                        # On ignore le fuseau horaire pour éviter les problèmes de parsing
                        date_part = timestamp_str.split(' ')[0]  # partie avant le fuseau
                        dt = datetime.strptime(date_part, "%d/%b/%Y:%H:%M:%S")
                        timestamp = dt
                    except Exception as e:
                        print(f"Erreur parsing timestamp: {timestamp_str} -> {e}")
                        timestamp = None
                    data.append((ip, timestamp, method, url, int(status), int(size)))

        if data:
            insert_sql = """
                INSERT INTO logs (ip, timestamp, method, url, status, size)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cur.executemany(insert_sql, data)
            conn.commit()
            print(f"{len(data)} lignes insérées depuis {file_key}")

    cur.close()
    conn.close()

with DAG(
    'logs_pipeline',
    default_args=default_args,
    description='Traite les logs depuis MinIO vers PostgreSQL',
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='list_files',
        python_callable=list_files,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id='parse_and_load',
        python_callable=parse_and_load,
        provide_context=True,
    )

    t1 >> t2