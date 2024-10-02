import pandas as pd
import os
from sqlalchemy import create_engine
import prueba2

POSTGRES_CONN_STR = 'postgresql://postgres:admin@localhost:5432/rcdatos'


def insertTable(name, file_type, separator=None):
    archivo_errores = '../../../../Lake/errors/errores_' + name
    bad_rows = []
    total_filas = 0  # Contador de filas totales procesadas
    filas_procesadas = 0  # Contador de filas que se insertaron correctamente

    def processBadLines(badLine):
        bad_rows.append(badLine)
        return None

    try:
        if file_type == 'csv':
            df = pd.read_csv('../../../../Lake/data/' + name, sep=separator, quotechar='"', on_bad_lines=processBadLines, engine='python', header=None)
        elif file_type == 'excel':
            df = pd.read_excel('../../../../Lake/data/' + name)
        elif file_type == 'json':
            df = pd.read_json('../../../../Lake/data/' + name)
        elif file_type == 'metadata':
            # Procesar metadata aquí
            pass
    except UnicodeDecodeError:
        # Si falla, intentar con latin1 para CSV
        print(f"Error de codificación con utf-8 en {name}, intentando con latin1.")
        if file_type == 'csv':
            df = pd.read_csv('../../../../Lake/data/' + name, sep=separator, quotechar='"', encoding='latin1', on_bad_lines=processBadLines, engine='python', header=None)

    print(df)
    # Asignacion de cabecera de los archivos
    if file_type == 'csv':
        df.columns = df.iloc[0]  # Usar la primera fila como nombres de columnas
        df = df[1:].reset_index(drop=True)  # Eliminar la fila de cabecera y resetear el índice
    
    print(df)
    # # Contar filas totales
    # total_filas = len(df)

    # if bad_rows:
    #     with open(archivo_errores, 'w', encoding='latin1') as f:
    #         f.write(','.join(df.columns) + '\n')
    #         for row in bad_rows:
    #             f.write(','.join(row) + '\n')
    #     flash(f'Se encontraron {len(bad_rows)} filas problemáticas. Guardadas en C/Lake/errors/errores_{name}.', 'error')

    # # Inserción a PostgreSQL
    engine = create_engine(POSTGRES_CONN_STR)
    
    nombre_archivo = os.path.basename('../../../../Lake/data/' + name).replace('.CSV', '')
    
    if len(nombre_archivo) > 32:
        nombre_archivo = nombre_archivo[0:55].replace('_', '')
    print(nombre_archivo)
    print(df)
    try:
        df.to_sql(nombre_archivo, engine, if_exists='replace', schema='staging')
        filas_procesadas = len(df)  # Las filas insertadas exitosamente son las del dataframe final
    except Exception as e:
        print(f'Error al insertar datos en la tabla: {str(e)}', 'error')
    finally:
        engine.dispose()

    # # Mover el archivo al directorio de procesados
    # move('../../../../Lake/data/' + name, '../../../../Lake/procesados/' + name)

    # # Mensaje de éxito con filas procesadas y problemáticas
    print(f'{name} subido correctamente. Se insertaron {filas_procesadas} filas correctamente.', 'success')

    # if bad_rows:
    #     flash(f'{len(bad_rows)} filas problemáticas encontradas y guardadas.', 'error')


def main():
    pepe = 20
    prueba2.callFuntion(pepe)

    # insertTable('caratula.CSV', 'csv', ',')

main()