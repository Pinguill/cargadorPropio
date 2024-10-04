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

def obtener_datos(page, per_page):
    offset = (page - 1) * per_page
    query = """
        SELECT nombre_tabla, nombre_columna, descripcion, tipo_columna
        FROM metadata.descripcion_columnas
        LIMIT %s OFFSET %s
    """
    total_records_query = "SELECT COUNT(*) FROM metadata.descripcion_columnas"

    conn = conectar_db()
    cursor = conn.cursor()

    cursor.execute(query, (per_page, offset))
    data = cursor.fetchall()

    cursor.execute(total_records_query)
    total_records = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return data, total_records

@consulta_bp.route('/consulta')
def consulta():
    page = request.args.get('page', 1, type=int)
    per_page = 10
    data, total_records = obtener_datos(page, per_page)
    total_pages = (total_records + per_page - 1) // per_page
    return render_template('consulta.html', data=data, page=page, total_pages=total_pages)

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
