import os
import pandas as pd
from flask import Blueprint, render_template, request, redirect, flash
from werkzeug.utils import secure_filename
from flask_wtf import FlaskForm
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from config.config import conectar_db, get_db_connection_data  # Importar funciones de conexión

UPLOAD_FOLDER = '../../../../Lake/data/'

class SimpleForm(FlaskForm):
    pass  # Esto generará el csrf_token automáticamente


def clean_string(value):
    return str(value).encode('utf-8', 'ignore').decode('utf-8', 'ignore')


def insertTablasReferencia(file_path):
    # Obtener los datos de conexión y detectar la base de datos
    connection_data = get_db_connection_data()
    db_type = connection_data['db_type']
    
    try:
        if 'Postgres' in db_type:
            # Conexión a PostgreSQL
            engine = create_engine(
                f"postgresql://{connection_data['user']}:{connection_data['password']}@{connection_data['host']}/{connection_data['database']}"
            )
            Session = sessionmaker(bind=engine)
            session = Session()
        elif 'Oracle' in db_type:
            # Conexión a Oracle
            conn = conectar_db()  # Usar función personalizada para conexión Oracle
            cursor = conn.cursor()
        else:
            flash('Tipo de base de datos no soportado.', 'error')
            return

        # Cargar el archivo Excel con todas las hojas
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            df = df.applymap(lambda x: clean_string(x) if isinstance(x, str) else x)
            table_name = sheet_name.lower().replace(' ', '_')

            if 'Postgres' in db_type:
                # Crear tabla en PostgreSQL
                create_table_query = text(f"""
                    CREATE TABLE IF NOT EXISTS structured.{table_name} (
                        codigo VARCHAR(12),
                        descripcion VARCHAR(120)
                    );
                """)
                session.execute(create_table_query)
                session.commit()

                # Insertar datos en PostgreSQL
                insert_query = text(f"INSERT INTO structured.{table_name} (codigo, descripcion) VALUES (:codigo, :descripcion)")
                for _, row in df.iterrows():
                    session.execute(insert_query, {'codigo': row['codigo'], 'descripcion': row['descripcion']})
                session.commit()

            elif 'Oracle' in db_type:
                # Crear tabla en Oracle
                cursor.execute(f"""
                    BEGIN
                        EXECUTE IMMEDIATE 'DROP TABLE {table_name}';
                    EXCEPTION
                        WHEN OTHERS THEN NULL;
                    END;
                """)
                cursor.execute(f"CREATE TABLE {table_name} ({', '.join([f'{col} VARCHAR2(100)' for col in df.columns])})")

                # Insertar datos en Oracle
                for _, row in df.iterrows():
                    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join([f':{i+1}' for i in range(len(df.columns))])})"
                    cursor.execute(insert_query, tuple(row))
                conn.commit()

        # Cerrar la sesión o la conexión
        if 'Postgres' in db_type:
            session.close()
            engine.dispose()
        elif 'Oracle' in db_type:
            cursor.close()
            conn.close()

        flash(f'Archivo {file_path} procesado e insertado correctamente.', 'success')
    
    except SQLAlchemyError as e:
        flash(f'Error de SQLAlchemy en PostgreSQL: {str(e)}', 'error')
    except Exception as e:
        flash(f'Error al procesar archivo en Oracle: {str(e)}', 'error')


cargueTR_bp = Blueprint('cargueTR', __name__)

@cargueTR_bp.route('/cargueTR', methods=['GET', 'POST'])
def cargueTR():
    form = SimpleForm()
    
    if request.method == 'POST':
        if 'excel-File' not in request.files:
            flash('No se ha seleccionado ningún archivo', 'error')
            return redirect(request.url)

        file = request.files['excel-File']
        if file.filename == '':
            flash('No se ha seleccionado ningún archivo', 'error')
            return redirect(request.url)

        filename = secure_filename(file.filename)
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(file_path)

        insertTablasReferencia(file_path)

        flash('Archivo subido y procesado correctamente.', 'success')
        return redirect(request.url)

    return render_template('cargueTR.html', form=form)
