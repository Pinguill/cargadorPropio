from flask import Blueprint, render_template, request, jsonify
import psycopg2

consulta_bp = Blueprint('consulta', __name__)

def conectar_db():
    return psycopg2.connect(
        host="localhost",
        database="rcdatos",
        user="postgres",
        password="admin"
    )

def obtener_datos(page, per_page, search_tag=None):
    offset = (page - 1) * per_page
    
    # Consulta base
    query = """
        SELECT dc.nombre_tabla, dc.nombre_columna, dc.descripcion, dc.tipo_columna, 
               STRING_AGG(t.nombre_tag, ', ') AS tags
        FROM metadata.descripcion_columnas dc
        LEFT JOIN metadata.tag_columna tc ON dc.id_columna = tc.id_column
        LEFT JOIN metadata.tags t ON tc.id_tag = t.id_tag
    """
    
    count_query = """
        SELECT COUNT(DISTINCT dc.id_columna)
        FROM metadata.descripcion_columnas dc
        LEFT JOIN metadata.tag_columna tc ON dc.id_columna = tc.id_column
        LEFT JOIN metadata.tags t ON tc.id_tag = t.id_tag
    """

    params = []  # Lista para almacenar los parámetros de la consulta

    # Si hay un término de búsqueda, se añade una cláusula WHERE
    if search_tag:
        query += " WHERE t.nombre_tag ILIKE %s"
        count_query += " WHERE t.nombre_tag ILIKE %s"
        params.append(f"%{search_tag}%")  # Agregamos el valor de búsqueda a los parámetros

    query += " GROUP BY dc.nombre_tabla, dc.nombre_columna, dc.descripcion, dc.tipo_columna"
    query += " LIMIT %s OFFSET %s"
    
    params.extend([per_page, offset])  # Agregamos los parámetros de paginación

    conn = conectar_db()
    cursor = conn.cursor()

    # Ejecutar la consulta para obtener los datos filtrados
    cursor.execute(query, tuple(params))
    data = cursor.fetchall()

    # Ejecutar la consulta para contar el número total de registros filtrados
    if search_tag:
        cursor.execute(count_query, (f"%{search_tag}%",))
    else:
        cursor.execute(count_query)
    
    total_records = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return data, total_records


@consulta_bp.route('/consulta')
def consulta():
    page = request.args.get('page', 1, type=int)
    per_page = 10
    search_tag = request.args.get('search_tag', None)
    data, total_records = obtener_datos(page, per_page, search_tag)
    total_pages = (total_records + per_page - 1) // per_page
    return render_template('consulta.html', data=data, page=page, total_pages=total_pages, search_tag=search_tag)

@consulta_bp.route('/update_tag', methods=['POST'])
def update_tag():
    data = request.get_json()
    table_name = data.get('table_name')
    column_name = data.get('column_name')
    new_tag = data.get('new_tag')

    update_query = """
        UPDATE metadata.descripcion_columnas
        SET tag = %s
        WHERE nombre_tabla = %s AND nombre_columna = %s
    """

    try:
        conn = conectar_db()
        cursor = conn.cursor()
        cursor.execute(update_query, (new_tag, table_name, column_name))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        print(e)
        return jsonify({"success": False, "error": str(e)})
