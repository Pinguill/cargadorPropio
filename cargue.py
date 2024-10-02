import os
import pandas as pd
import getDescData
import xlrd
from shutil import move
from flask import Blueprint, render_template, request, redirect, url_for, flash
from werkzeug.utils import secure_filename
from sqlalchemy import create_engine

# Cargue rápido de la tabla a PostgreSQL
POSTGRES_CONN_STR = 'postgresql://postgres:admin@localhost:5432/rcdatos'

def insertTable(name, file_type, separator):
    archivo_errores = '../../../../Lake/errors/errores_' + name
    bad_rows = []
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
            getDescData.getDDIData(name)
            return
    except UnicodeDecodeError:
        # Si falla, intentar con latin1 para CSV
        print(f"Error de codificación con utf-8 en {name}, intentando con latin1.")
        if file_type == 'csv':
            df = pd.read_csv('../../../../Lake/data/' + name, sep=separator, quotechar='"', encoding='latin1', on_bad_lines=processBadLines, engine='python', header=None)

    # Asignacion de cabecera de los archivos
    if file_type == 'csv':
        df.columns = df.iloc[0]  # Usar la primera fila como nombres de columnas
        df = df[1:].reset_index(drop=True)  # Eliminar la fila de cabecera y resetear el índice

    if bad_rows:
        with open(archivo_errores, 'w', encoding='latin1') as f:
            f.write(','.join(df.columns) + '\n')
            for row in bad_rows:
                f.write(','.join(row) + '\n')
        flash(f'Se encontraron {len(bad_rows)} filas problemáticas. Guardadas en C/Lake/errors/errores_{name}.', 'error')

    # Inserción a PostgreSQL
    engine = create_engine(POSTGRES_CONN_STR)
    
    nombre_archivo = os.path.basename('../../../../Lake/data/' + name).replace('.CSV', '')
    
    if len(nombre_archivo) > 32:
        nombre_archivo = nombre_archivo[0:55].replace('_', '')

    try:
        df.to_sql(nombre_archivo, engine, if_exists='replace', schema='staging', index=False)
        filas_procesadas = len(df)  # Las filas insertadas exitosamente son las del dataframe final
    except Exception as e:
        flash(f'Error al insertar datos en la tabla: {str(e)}', 'error')
    finally:
        engine.dispose()

    # Mover el archivo al directorio de procesados
    move('../../../../Lake/data/' + name, '../../../../Lake/procesados/' + name)

    # Mensaje de éxito con filas procesadas y problemáticas
    flash(f'{name} subido correctamente. Se insertaron {filas_procesadas} filas correctamente.', 'success')

cargue_bp = Blueprint('cargue', __name__)

# Directorio donde se guardarán los archivos subidos
UPLOAD_FOLDER = '../../../../Lake/data/'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Funcion encargada de tomar los datos de la pagina y subir el archivo al Data Lake
@cargue_bp.route('/cargue', methods=['GET', 'POST'])
def cargue():
    if request.method == 'POST':
        new_filename = request.form.get('new_filename')
        quick_create = 'quick_create' in request.form
        file_type = request.form.get('file_type')
        separator = request.form.get('separator', ',')

        if not separator:
            separator = ','

        if 'file' not in request.files:
            
            flash('No se ha seleccionado ningún archivo')
            return redirect(request.url)
        
        file = request.files['file']
        
        if file.filename == '':
            flash('No se ha seleccionado ningún archivo')
            return redirect(request.url)

        if file:
            original_filename = secure_filename(file.filename)
            file_extension = os.path.splitext(original_filename)[1]

            if new_filename:
                filename = secure_filename(new_filename) + file_extension
            else:
                filename = original_filename

            if quick_create:
                flash(f'Archivo "{filename}" subido y se creará la tabla rápidamente.')

                # Llamado de la funcion para insertar la tabla
                file.save(os.path.join(UPLOAD_FOLDER, filename))
                insertTable(filename, file_type, separator)
                file.save(os.path.join('../../../../Lake/procesados', filename))
                flash(f'Tabla {filename} creada exitosamente')
                return redirect(url_for('cargue.cargue'))
            
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            flash(f'Archivo {filename} cargado exitosamente')
            return redirect(url_for('cargue.cargue'))

    return render_template('cargue.html')