from flask import Blueprint, flash, redirect, render_template, request, jsonify, Response, url_for
from io import BytesIO
from flask_wtf import FlaskForm
import psycopg2
import oracledb
import pandas as pd
import os

consulta_bp = Blueprint('consulta', __name__)

class SimpleForm(FlaskForm):
    pass  # No es necesario definir campos, pero esto generará el csrf_token automáticamente

def get_db_connection_data():
    connection_data = {}
    with open('config/db_config.txt', 'r') as f:
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

def obtener_tags():
    query = "SELECT nombre_tag, descripcion, id_tag FROM metadata.tags"
    conn = conectar_db()
    cursor = conn.cursor()
    cursor.execute(query)
    tags = cursor.fetchall()
    cursor.close()
    conn.close()
    return tags

def obtener_conjuntos():
    query = "SELECT DISTINCT nombre_tabla FROM metadata.descripcion_columnas"
    conn = conectar_db()
    cursor = conn.cursor()
    cursor.execute(query)
    conjuntos = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return conjuntos


def obtener_datos(page, per_page, selected_tags=None, conjunto=None, descripcion=None):
    offset = (page - 1) * per_page  # Calcula el desplazamiento
    
    # Consulta básica sin filtros, agregando dc.id_columna
    query = """
        SELECT dc.id_columna, dc.nombre_tabla, dc.nombre_columna, dc.descripcion, dc.tipo_columna, ARRAY_AGG(t.nombre_tag), dc.es_tabla, dc.check_temp
        FROM metadata.descripcion_columnas dc
        LEFT JOIN metadata.tag_columna tc ON dc.id_columna = tc.id_column
        LEFT JOIN metadata.tags t ON tc.id_tag = t.id_tag
        WHERE 1=1
    """
    
    params = []
    
    # Filtros opcionales
    if conjunto:
        query += " AND dc.nombre_tabla ILIKE %s"
        params.append(f'%{conjunto}%')
    
    if descripcion:
        query += " AND dc.descripcion ILIKE %s"
        params.append(f'%{descripcion}%')
    
    if selected_tags:
        query += """
            AND dc.id_columna IN (
                SELECT tc.id_column
                FROM metadata.tag_columna tc
                WHERE tc.id_tag = ANY(%s)
            )
        """
        params.append(selected_tags)
    
    # Añade la cláusula GROUP BY
    query += " GROUP BY dc.id_columna, dc.nombre_tabla, dc.nombre_columna, dc.descripcion, dc.tipo_columna, dc.es_tabla, dc.check_temp LIMIT %s OFFSET %s"
    params.extend([per_page, offset])
    
    # Ejecuta la consulta
    conn = conectar_db()
    cursor = conn.cursor()
    cursor.execute(query, params)
    data = cursor.fetchall()
    
    # Consulta para contar el total de registros
    total_records_query = """
        SELECT COUNT(DISTINCT dc.id_columna)
        FROM metadata.descripcion_columnas dc
        LEFT JOIN metadata.tag_columna tc ON dc.id_columna = tc.id_column
        WHERE 1=1
    """
    
    total_params = []
    if conjunto:
        total_records_query += " AND dc.nombre_tabla ILIKE %s"
        total_params.append(f'%{conjunto}%')
    
    if descripcion:
        total_records_query += " AND dc.descripcion ILIKE %s"
        total_params.append(f'%{descripcion}%')
    
    if selected_tags:
        total_records_query += """
            AND dc.id_columna IN (
                SELECT tc.id_column
                FROM metadata.tag_columna tc
                WHERE tc.id_tag = ANY(%s)
            )
        """
        total_params.append(selected_tags)
    
    cursor.execute(total_records_query, total_params)
    total_records = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    return data, total_records

@consulta_bp.route('/consulta', methods=['GET', 'POST'])
def consulta():
    # Parámetro de la página (por defecto es 1)
    form = SimpleForm()
    page = request.args.get('page', 1, type=int)
    per_page = 10  # Número de registros por página

    # Obtener los filtros de descripción y conjunto de datos
    conjunto = request.args.get('filtrar_conjunto', type=str)
    descripcion = request.args.get('filtrar_desc', type=str)

    # Obtener los tags seleccionados (si los hay)
    selected_tags = request.args.getlist('tags', type=int)

    # Obtener los datos de la base de datos con los filtros aplicados
    data, total_records = obtener_datos(page, per_page, selected_tags, conjunto, descripcion)

    # Obtener todos los tags para mostrar en el formulario
    tags = obtener_tags()

    # Obtener todos los conjuntos de datos para mostrar en el dropdown
    conjuntos = obtener_conjuntos()

    # Calcular el número total de páginas
    total_pages = (total_records + per_page - 1) // per_page

    # Renderizar la plantilla y pasar los datos, los tags, los conjuntos, la página actual y el total de páginas
    return render_template('consulta.html', data=data, tags=tags, conjuntos=conjuntos, page=page, total_pages=total_pages, form=form)


@consulta_bp.route('/actualizar_estado', methods=['GET', 'POST'])
def actualizar_estado():
    # Obtén los datos enviados desde el cliente (AJAX)
    data = request.get_json()
    
    # Extrae los valores de columna y el estado del checkbox
    columna = data.get('columna')
    es_checkbox = data.get('esCheckbox')
    
    # Conecta a la base de datos
    conn = conectar_db()
    cursor = conn.cursor()
    
    try:
        # Actualiza el campo "esCheckbox" en la tabla según el nombre de la columna
        print(type(es_checkbox))
        if es_checkbox == 0:
            query = """
                UPDATE metadata.descripcion_columnas
                SET check_temp = false
                WHERE id_columna = %s
            """
        elif es_checkbox == 1:
            query = """
                UPDATE metadata.descripcion_columnas
                SET check_temp = true
                WHERE id_columna = %s
            """
        cursor.execute(query, (columna,))
        conn.commit()
        cursor.close()
        conn.close()
        
        # Responde con un JSON de éxito
        return jsonify(success=True)
    except Exception as e:
        print(f"Error al actualizar el estado: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        # En caso de error, responde con un JSON de error
        return jsonify(success=False, error=str(e)), 500

@consulta_bp.route('/exportar_datos', methods=['POST'])
def exportar_datos():
    try:
        # Obtener la conexión dinámica a la base de datos
        conn = conectar_db()

        # Obtener las filas seleccionadas de la tabla `descripcion_columnas`
        query_selected_rows = """
            SELECT id_columna, nombre_columna AS nombre_variable
            FROM metadata.descripcion_columnas
            WHERE check_temp = TRUE
        """
        selected_ids = pd.read_sql(query_selected_rows, conn)

        if selected_ids.empty:
            return Response("No hay filas seleccionadas para exportar.", status=400)

        # Obtener los datos de la tabla `categorias` para las columnas seleccionadas
        query_categorias = f"""
            SELECT c.id_columna, c.codigo_categoria, c.descripcion_categoria, d.nombre_columna AS nombre_variable
            FROM metadata.categorias c
            JOIN metadata.descripcion_columnas d ON c.id_columna = d.id_columna
            WHERE c.id_columna IN ({', '.join(map(str, selected_ids['id_columna'].tolist()))})
        """
        categorias_data = pd.read_sql(query_categorias, conn)

        if categorias_data.empty:
            return Response("No hay datos en la tabla 'categorias' para los valores seleccionados.", status=400)

        # Crear el archivo Excel con una hoja por cada `id_columna`, usando `nombre_variable` como nombre de la pestaña
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for id_columna, nombre_variable in selected_ids.itertuples(index=False):
                data = categorias_data[categorias_data['id_columna'] == id_columna]
                data[['codigo_categoria', 'descripcion_categoria']].to_excel(
                    writer, sheet_name=nombre_variable[:31], index=False  # Limitar el nombre a 31 caracteres
                )

        # Actualizar los valores de `check_temp` a FALSE después de exportar
        update_check_temp = """
            UPDATE metadata.descripcion_columnas
            SET check_temp = FALSE
            WHERE check_temp = TRUE
        """
        with conn.cursor() as cursor:
            cursor.execute(update_check_temp)
            conn.commit()

        # Guardar el archivo en memoria
        output.seek(0)

        # Enviar el archivo como respuesta
        return Response(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": "attachment;filename=datos_exportados.xlsx"}
        )
    except Exception as e:
        # Si ocurre un error, responder con un estado 500
        return Response(f"Error al exportar los datos: {str(e)}", status=500)
    finally:
        # Cerrar la conexión
        if 'conn' in locals() and conn:
            conn.close()

@consulta_bp.route('/consulta/crearTag', methods=['GET'])
def crearTag():
    # Obtener datos de la fila seleccionada
    form = SimpleForm()
    id_columna = request.args.get('id_columna')
    nombre_tabla =  request.args.get('nombre_tabla')
    nombre_columna = request.args.get('nombre_columna')
    descripcion = request.args.get('descripcion')
    if not id_columna:
        flash('No se ha seleccionado una columna.', 'error')
        return redirect(url_for('consulta.consulta'))
    tags = obtener_tags()

    return render_template('crearTag.html', id_columna=id_columna, form=form, tags=tags, nombre_tabla=nombre_tabla, nombre_columna=nombre_columna, descripcion=descripcion)


@consulta_bp.route('/consulta/guardarTag', methods=['POST'])
def guardarTag():
    nombre_tag = request.form.get('nombre_tag')
    descripcion = request.form.get('descripcion')
    id_columna = request.form.get('id_columna')
    
    # Lógica para guardar el tag en la base de datos
    query = "INSERT INTO metadata.tags (nombre_tag, descripcion) VALUES (%s, %s) RETURNING id_tag"
    conn = conectar_db()
    cursor = conn.cursor()
    cursor.execute(query, (nombre_tag, descripcion))
    id_tag = cursor.fetchone()[0]
    
    # Asociar el tag con la columna seleccionada
    query_assoc = "INSERT INTO metadata.tag_columna (id_tag, id_column) VALUES (%s, %s)"
    cursor.execute(query_assoc, (id_tag, id_columna))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    # Redirigir al usuario a la página original
    flash('Tag creado y asociado a exitosamente', 'success')
    return redirect(url_for('consulta.consulta'))


@consulta_bp.route('/consulta/agregarTagExistente', methods=['POST'])
def agregarTagExistente():
    id_tag = request.form.get('tag_existente')
    id_columna = request.form.get('id_columna')

    if not id_tag or not id_columna:
        flash('Tag o columna no seleccionados.', 'error')
        return redirect(url_for('consulta.creacionTag', id_columna=id_columna))

    conn = conectar_db()
    cursor = conn.cursor()
    try:
        # Verificar si ya existe la asociación
        query_verificar = "SELECT 1 FROM metadata.tag_columna WHERE id_tag = %s AND id_column = %s"
        cursor.execute(query_verificar, (id_tag, id_columna))
        if cursor.fetchone():
            flash('El tag ya está asociado con esta columna.', 'error')
            return redirect(url_for('consulta.consulta'))

        # Insertar la asociación
        query_assoc = "INSERT INTO metadata.tag_columna (id_tag, id_column) VALUES (%s, %s)"
        cursor.execute(query_assoc, (id_tag, id_columna))
        conn.commit()

        flash('Tag asociado exitosamente.', 'success')
        return redirect(url_for('consulta.consulta'))

    except Exception as e:
        conn.rollback()
        flash(f'Ocurrió un error: {str(e)}', 'error')
        return redirect(url_for('consulta.creacionTag', id_columna=id_columna))
    finally:
        cursor.close()
        conn.close()
