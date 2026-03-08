"""
Génération de logs serveur synthétiques au format Common Log Format.
Permet de créer des fichiers de logs pour tester le pipeline.
"""

import random
import argparse
from datetime import datetime, timedelta

# Paramètres configurables pour simuler un trafic varié
URLS = ['/index.html', '/about', '/contact', '/products', '/images/logo.png', '/api/v1/users']
METHODS = ['GET', 'POST', 'PUT', 'DELETE']
STATUS_CODES = [200, 201, 301, 302, 400, 401, 403, 404, 500]
IPS = [f'192.168.1.{i}' for i in range(1, 51)]  # 50 IPs différentes

def generate_log_line(timestamp):
    """
    Génère une ligne de log à un instant donné.
    Format : IP - - [date] "METHOD url HTTP/1.1" status size
    """
    ip = random.choice(IPS)
    method = random.choice(METHODS)
    url = random.choice(URLS)
    status = random.choice(STATUS_CODES)
    size = random.randint(100, 10000)
    date_str = timestamp.strftime("%d/%b/%Y:%H:%M:%S %z")
    return f'{ip} - - [{date_str}] "{method} {url} HTTP/1.1" {status} {size}'

def generate_logs(num_lines, output_file):
    """
    Génère un fichier contenant `num_lines` lignes de logs.
    Les timestamps sont répartis aléatoirement sur une période d'une heure.
    """
    start_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    with open(output_file, 'w', encoding='utf-8') as f:
        for _ in range(num_lines):
            ts = start_time + timedelta(seconds=random.randint(0, 3599))
            f.write(generate_log_line(ts) + '\n')
    print(f"✅ {num_lines} lignes écrites dans {output_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Génère des logs synthétiques")
    parser.add_argument('--num', type=int, default=1000,
                        help='Nombre de lignes à générer (défaut: 1000)')
    parser.add_argument('--output', type=str, default='data/logs_sample.log',
                        help='Fichier de sortie (défaut: data/logs_sample.log)')
    args = parser.parse_args()
    generate_logs(args.num, args.output)