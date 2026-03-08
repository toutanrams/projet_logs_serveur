"""
Upload un fichier de logs vers MinIO (bucket logs-bruts).
"""

import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Configuration MinIO
endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
secret_key = os.getenv('MINIO_SECRET_KEY', 'password')

# Créer un client S3 pointant vers MinIO
s3 = boto3.client(
    's3',
    endpoint_url=f'http://{endpoint}',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'  # n'importe quelle région
)

bucket_name = 'logs-bruts'

# Créer le bucket s'il n'existe pas
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"✅ Le bucket '{bucket_name}' existe déjà.")
except:
    s3.create_bucket(Bucket=bucket_name)
    print(f"✅ Bucket '{bucket_name}' créé.")

# Upload du fichier
file_path = 'data/logs_20250308_1.log'  # chemin relatif depuis la racine du projet
object_name = 'logs_20250308_1.log'      # nom sous lequel il sera stocké dans MinIO

try:
    s3.upload_file(file_path, bucket_name, object_name)
    print(f"✅ Fichier '{file_path}' uploadé vers '{bucket_name}/{object_name}'")
except Exception as e:
    print(f"❌ Erreur lors de l'upload : {e}")