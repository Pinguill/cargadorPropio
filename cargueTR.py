import os
import pandas as pd
from flask import Blueprint, render_template, request, redirect, flash
from werkzeug.utils import secure_filename
from flask_wtf import FlaskForm
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


UPLOAD_FOLDER = '../../../../Lake/data/'

# Cambiar esta cadena de conexión según tu configuración
DB_CONNECTION = 'postgresql://postgres:admin@localhost:5432/rcdatos'

class SimpleForm(FlaskForm):
    pass  # Esto generará el csrf_token automáticamente


def clean_string(value):
    # Intentar codificar y decodificar con 'utf-8'
    return str(value).encode('utf-8', 'ignore').decode('utf-8', 'ignore')

# Función para insertar las hojas del Excel como tablas en PostgreSQL
def insertTablasReferencia(file_path):
    # Conectar a la base de datos
    engine = create_engine(DB_CONNECTION)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Cargar el archivo Excel con todas las hojas utilizando openpyxl
    xls = pd.ExcelFile(file_path, engine='openpyxl')

    # Iterar por cada hoja del Excel
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)

        # Limpiar todas las columnas de tipo string
        df = df.applymap(lambda x: clean_string(x) if isinstance(x, str) else x)

        # Nombrar la tabla como el nombre de la hoja (asegurarse de que sea un nombre válido)
        table_name = sheet_name.lower().replace(' ', '_')  # Evitar espacios y usar minúsculas

        # Crear la tabla con columnas 'codigo' y 'descripcion'
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS staging.{table_name} (
            codigo character varying(12),
            descripcion character varying(120)
        );
        """
        
        # Ejecutar el CREATE TABLE
        session.execute(create_table_query)
        session.commit()

        # Insertar los datos del DataFrame en la tabla recién creada
        insert_query = f"INSERT INTO staging.{table_name} (codigo, descripcion) VALUES (:codigo, :descripcion)"
        
        # Iterar sobre el DataFrame y ejecutar el INSERT
        for _, row in df.iterrows():
            session.execute(insert_query, {'codigo': row['codigo'], 'descripcion': row['descripcion']})
        
        session.commit()
        print(f"Datos insertados en la tabla: {table_name}")

    # Cerrar la sesión y la conexión a la base de datos
    session.close()
    engine.dispose()


cargueTR_bp = Blueprint('cargueTR', __name__)

@cargueTR_bp.route('/cargueTR', methods=['GET', 'POST'])
def cargueTR():
    form = SimpleForm()  # Instancia el formulario
    
    if request.method == 'POST':
        # Verifica si el archivo fue enviado en la petición
        if 'excel-File' not in request.files:
            flash('No se ha seleccionado ningún archivo', 'error')
            return redirect(request.url)

        # Obtener el archivo
        file = request.files['excel-File']
        if file.filename == '':
            flash('No se ha seleccionado ningún archivo', 'error')
            return redirect(request.url)

        # Guardar el archivo en el servidor
        filename = secure_filename(file.filename)
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(file_path)

        # Llamada a la función para insertar en la base de datos
        insertTablasReferencia(file_path)

        flash('Archivo subido y procesado correctamente.', 'success')
        return redirect(request.url)

    # Si es GET, simplemente renderiza el formulario
    return render_template('cargueTR.html', form=form)  # Pasa el formulario a la plantilla
