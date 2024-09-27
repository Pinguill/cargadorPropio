import os
import pandas as pd
from flask import Blueprint, render_template, request, redirect, url_for, flash
from werkzeug.utils import secure_filename
from sqlalchemy import create_engine

# Cargue rápido de la tabla a PostgreSQL
POSTGRES_CONN_STR = 'postgresql://postgres:admin@localhost:5432/rcdatos'

def insertTable(name):
    #df = pd.read_csv('../../../../Lake/data/' + name)
    archivo_errores = '../../../../Lake/errors/errores_' + name

    bad_rows = []

    def processBadLines(badLine):
        bad_rows.append(badLine)
        return None

    try:
        df = pd.read_csv('../../../../Lake/data/' + name, quotechar='"', on_bad_lines=processBadLines, engine='python')
    except UnicodeDecodeError:
        # Si falla, intentar con latin1
        print(f"Error de codificación con utf-8 en {name}, intentando con latin1.")
        df = pd.read_csv('../../../../Lake/data/' + name, quotechar='"', encoding='latin1', on_bad_lines=processBadLines, engine='python')

    if bad_rows:
        with open(archivo_errores, 'w', encoding='latin1') as f:
            f.write(','.join(df.columns) + '\n')
            for row in bad_rows:
                f.write(','.join(row) + '\n')
        flash(f'Se encontraron {len(bad_rows)} filas problemáticas. Guardadas en C/Lake/errors/errores_{name}.', 'error')

    engine = create_engine(POSTGRES_CONN_STR)
    
    nombre_archivo = os.path.basename('../../../../Lake/data/' + name).replace('.CSV', '')
    
    if len(nombre_archivo) > 32:
        nombre_archivo = nombre_archivo[0:55].replace('_', '')

    # Cargar datos en PostgreSQL, creando la tabla si no existe
    df.to_sql(nombre_archivo, engine, if_exists='replace', index=False, schema='public')

# Configura el blueprint para el módulo de cargue
cargue_bp = Blueprint('cargue', __name__)

# Directorio donde se guardarán los archivos subidos
UPLOAD_FOLDER = '../../../../Lake/data/'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@cargue_bp.route('/cargue', methods=['GET', 'POST'])
def cargue():
    if request.method == 'POST':
        new_filename = request.form.get('new_filename')
        quick_create = 'quick_create' in request.form

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
                insertTable(filename)
                file.save(os.path.join('../../../../Lake/procesados', filename))
                flash(f'Tabla {filename} creada exitosamente')
                return redirect(url_for('cargue.cargue'))
            
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            flash(f'Archivo {filename} cargado exitosamente')
            return redirect(url_for('cargue.cargue'))

    return render_template('cargue.html')
