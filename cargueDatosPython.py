import os
import pandas as pd
import shutil
from sqlalchemy import create_engine
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta


# Configuración de conexión a PostgreSQL
POSTGRES_CONN_STR = 'postgresql://postgres:admin@172.26.224.1:5432/rcdatos'

# Función que carga el archivo a PostgreSQL
def cargar_datos_postgres():
    bad_rows = []

    def processBadLines(badLine):
        bad_rows.append(badLine)
        return None
    
    # Ruta del archivo detectado
    archivos = [f for f in os.listdir('/mnt/c/Lake/data/') if f.endswith('.CSV')]
    if not archivos:
        raise FileNotFoundError("No se encontraron archivos en la carpeta.")
    
    for i in range(len(archivos)):
        bad_rows = []
        archivo_errores = '/mnt/c/Lake/errors/errores_'
        
        archivo = os.path.join('/mnt/c/Lake/data/', archivos[i])
        print("++++++++++++++++++++++++++", archivo,"++++++++++++++++++++++++++")

        # Leer el archivo CSV con pandas
        try:
            df = pd.read_csv(archivo, quotechar='"', on_bad_lines=processBadLines, engine='python', header=None)
        except UnicodeDecodeError:
            # Si falla, intentar con latin1
            print(f"Error de codificación con utf-8 en {archivo}, intentando con latin1.")
            df = pd.read_csv(archivo, encoding='latin1', quotechar='"', on_bad_lines=processBadLines, engine='python', header=None)

        # Asignacion de cabecera de los archivos
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)


        if bad_rows:
            with open(archivo_errores, 'w', encoding='latin1') as f:
                f.write(','.join(df.columns) + '\n')
                for row in bad_rows:
                    f.write(','.join(row) + '\n')
            print(f'Se encontraron {len(bad_rows)} filas problemáticas. Guardadas en C/Lake/erros/errores_{os.path.basename(archivo)}.', 'error')

        print(df)
        # Conectarse a PostgreSQL
        engine = create_engine(POSTGRES_CONN_STR)

        # Obtener nombre de tabla a partir del nombre del archivo
        nombre_archivo = os.path.basename(archivo).replace('.CSV', '')
        
        if len(nombre_archivo) > 32:
            nombre_archivo = nombre_archivo[0:55].replace('_', '')

        # Cargar datos en PostgreSQL, creando la tabla si no existe
        df.to_sql(nombre_archivo, engine, if_exists='replace', index=False, schema='staging')

        shutil.move(archivo, os.path.join('/mnt/c/Lake/procesados/', os.path.basename(archivo)))


# Definir el DAG de Airflow
default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 9, 17),
    'retries': 2,
}

with DAG(dag_id='cargar_datos_Postgres_py',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # FileSensor que detecta el archivo en la carpeta
    archivo_sensor = FileSensor(
        task_id='esperar_archivo',
        filepath='/mnt/c/Lake/data/',  # Ruta de la carpeta a monitorear
        fs_conn_id='fs_default',
        poke_interval=timedelta(seconds=5),
        timeout=timedelta(seconds=60),
        mode='poke',
    )

    # Operador Python para cargar los datos a PostgreSQL
    cargar_datos = PythonOperator(
        task_id='cargar_datos_postgres',
        python_callable=cargar_datos_postgres,
        provide_context=True,
    )

    # Definir la secuencia de tareas
    archivo_sensor >> cargar_datos
