from flask import Blueprint, render_template, request, jsonify
import psycopg2
import oracledb
import os

consulta_bp = Blueprint('consulta', __name__)

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

def obtener_datos(page, per_page, selected_tags=None, conjunto=None, descripcion=None):
    offset = (page - 1) * per_page  # Calcula el desplazamiento
    
    # Consulta básica sin filtros
    query = """
        SELECT dc.nombre_tabla, dc.nombre_columna, dc.descripcion, dc.tipo_columna, ARRAY_AGG(t.nombre_tag)
        FROM metadata.descripcion_columnas dc
        LEFT JOIN metadata.tag_columna tc ON dc.id_columna = tc.id_column
        LEFT JOIN metadata.tags t ON tc.id_tag = t.id_tag
        WHERE 1=1
    """
    
    params = []
    
    # Filtro por conjunto de datos
    if conjunto:
        query += " AND dc.nombre_tabla ILIKE %s"
        params.append(f'%{conjunto}%')
    
    # Filtro por descripción
    if descripcion:
        query += " AND dc.descripcion ILIKE %s"
        params.append(f'%{descripcion}%')
    
    # Filtro por tags seleccionados
    if selected_tags:
        query += """
            AND dc.id_columna IN (
                SELECT tc.id_column
                FROM metadata.tag_columna tc
                WHERE tc.id_tag = ANY(%s)
            )
        """
        params.append(selected_tags)
    
    # Añade la cláusula GROUP BY y paginación
    query += " GROUP BY dc.id_columna LIMIT %s OFFSET %s"
    params.extend([per_page, offset])
    
    conn = conectar_db()
    cursor = conn.cursor()
    cursor.execute(query, params)
    data = cursor.fetchall()
    
    # Consulta para contar el total de registros (con o sin filtro)
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

@consulta_bp.route('/consulta')
def consulta():
    # Parámetro de la página (por defecto es 1)
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

    # Calcular el número total de páginas
    total_pages = (total_records + per_page - 1) // per_page
    
    # Renderizar la plantilla y pasar los datos, los tags, la página actual y el total de páginas
    return render_template('consulta.html', data=data, tags=tags, page=page, total_pages=total_pages)


@consulta_bp.route('/consulta/crear_tablas', methods=['POST'])
def crear_tablas():
    # Obtener los nombres de las columnas seleccionadas
    columnas_seleccionadas = request.form.getlist('es_tabla')
    
    if columnas_seleccionadas:
        # Ruta donde se guardará el archivo .txt
        ruta_txt = os.path.join(os.getcwd(), 'scripts_crear_tablas.txt')

        with open(ruta_txt, 'w') as file:
            for columna in columnas_seleccionadas:
                # Crear el script SQL para la tabla
                script_sql = f"""
                CREATE TABLE {columna} (
                    codigo character varying(32),
                    descripcion TEXT
                );
                """
                file.write(script_sql + '\n')

        # Devolver el mensaje en formato JSON
        return jsonify({'message': f"Archivo creado: {ruta_txt}"})

    # Si no se seleccionaron columnas, devolver un mensaje de error
    return jsonify({'message': "No se seleccionaron columnas para crear tablas."})