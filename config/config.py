import psycopg2
import oracledb
import os

def get_db_connection_data():
    # Obtén la ruta absoluta de db_config.txt usando la ubicación de config.py
    base_dir = os.path.dirname(__file__)
    config_path = os.path.join(base_dir, 'db_config.txt')

    connection_data = {}
    with open(config_path, 'r') as f:
        for line in f:
            key, value = line.strip().split('=')
            connection_data[key] = value
    return connection_data


def conectar_db():
    connection_data = get_db_connection_data()
    
    # Detecta si la base de datos es Oracle o PostgreSQL
    if 'Postgres' in connection_data['db_type']:
        return psycopg2.connect(
            host=connection_data['host'],
            database=connection_data['database'],
            user=connection_data['user'],
            password=connection_data['password']
        )
    elif 'Oracle' in connection_data['db_type']:
        return oracledb.connect(
            user=connection_data['user'],
            password=connection_data['password'],
            dsn=f"{connection_data['host']}:1521/{connection_data['database']}"
        )