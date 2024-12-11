import xml.etree.ElementTree as ET
import unicodedata
import psycopg2

POSTGRES_CONN_STR = 'postgresql://postgres:admin@localhost:5432/rcdatos'

def createDescription(tableName, column, desc, tipo):
    conn = psycopg2.connect(POSTGRES_CONN_STR)
    conn.autocommit = True
    cursor = conn.cursor()

    # Insertar o actualizar la descripción de la columna
    query = '''
        INSERT INTO metadata.descripcion_columnas (nombre_tabla, nombre_columna, descripcion, tipo_columna, es_tabla)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (nombre_tabla, nombre_columna)
        DO UPDATE SET descripcion = EXCLUDED.descripcion, tipo_columna = EXCLUDED.tipo_columna
        RETURNING id_columna
    '''
    cursor.execute(query, (tableName, column, desc, tipo, False))  
    id_columna = cursor.fetchone()[0]

    print(id_columna)

    cursor.close()
    conn.close()
    return id_columna

def createCategory(id_columna, var_name, catgry_code, label):
    conn = psycopg2.connect(POSTGRES_CONN_STR)
    conn.autocommit = True
    cursor = conn.cursor()

    # Insertar la categoría
    query = '''
        INSERT INTO metadata.categorias (id_columna, nombre_variable, codigo_categoria, descripcion_categoria)
        VALUES (%s, %s, %s, %s)
    '''
    cursor.execute(query, (id_columna, var_name, catgry_code, label))

    cursor.close()
    conn.close()

def updateEsTabla(id_columna):
    conn = psycopg2.connect(POSTGRES_CONN_STR)
    conn.autocommit = True
    cursor = conn.cursor()

    # Actualizar es_tabla a True
    query = '''
        UPDATE metadata.descripcion_columnas
        SET es_tabla = TRUE
        WHERE id_columna = %s
    '''
    cursor.execute(query, (id_columna,))

    cursor.close()
    conn.close()

def remove_accents(text):
    nfkd_form = unicodedata.normalize('NFD', text)
    return ''.join([char for char in nfkd_form if not unicodedata.combining(char)])

def getDDIData(fileName):
    tree = ET.parse('../../../../Lake/data/' + fileName)
    root = tree.getroot()
    namespaces = {'ddi': 'http://www.icpsr.umich.edu/DDI'}

    datasets = {}

    for file_dscr in root.findall(".//ddi:fileDscr", namespaces):
        file_id = file_dscr.attrib.get("ID")
        file_name_element = file_dscr.find(".//ddi:fileTxt//ddi:fileName", namespaces)
        if file_name_element is not None:
            file_name = remove_accents(file_name_element.text.strip())
            file_name = file_name.replace('.NSDstat', '')
            datasets[file_id] = {"file_name": file_name, "variables": []}

    for var in root.findall(".//ddi:var", namespaces):
        var_id = var.attrib.get("ID")
        var_name = var.attrib.get("name")
        file_ref = var.attrib.get("files")

        label_element = var.find(".//ddi:labl", namespaces)
        label = label_element.text.strip() if label_element is not None else "Sin descripción"

        var_format_element = var.find(".//ddi:varFormat", namespaces)
        var_type = var_format_element.attrib.get("type") if var_format_element is not None else "Desconocido"

        if file_ref in datasets:
            variable_info = {
                "var_id": var_id,
                "var_name": var_name,
                "description": label,
                "var_type": var_type,
                "categories": []
            }

            for catgry in var.findall(".//ddi:catgry", namespaces):
                catgry_code = catgry.find(".//ddi:catValu", namespaces)
                catgry_label = catgry.find(".//ddi:labl", namespaces)

                if catgry_code is not None and catgry_label is not None:
                    catgry_code_text = catgry_code.text.strip()
                    catgry_label_text = catgry_label.text.strip()

                    variable_info["categories"].append({
                        "code": catgry_code_text,
                        "label": catgry_label_text
                    })

            datasets[file_ref]["variables"].append(variable_info)
    print(datasets)
    for dataset_id, dataset_info in datasets.items():
        for var in dataset_info["variables"]:
            id_columna = createDescription(dataset_info['file_name'], var['var_name'], var['description'], var['var_type'])

            if var["categories"]:
                for category in var["categories"]:
                    createCategory(id_columna, var['var_name'], category['code'], category['label'])
                updateEsTabla(id_columna)