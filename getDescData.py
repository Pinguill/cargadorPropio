import xml.etree.ElementTree as ET
import unicodedata
import psycopg2

POSTGRES_CONN_STR = 'postgresql://postgres:admin@localhost:5432/rcdatos'

def createDescription(tableName, column, desc, tipo):
    conn = psycopg2.connect(POSTGRES_CONN_STR)
    conn.autocommit = True
    cursor = conn.cursor()

    query = ''' INSERT INTO metadata.descripcion_columnas (nombre_tabla, nombre_columna, descripcion, tipo_columna) 
            VALUES('{}', '{}', '{}', '{}')
             '''.format(tableName, column, desc, tipo)    
    cursor.execute(query)
    
    cursor.close()
    conn.close()


# Función para eliminar tildes y caracteres especiales
def remove_accents(text):
    # Normalizamos la cadena a la forma "NFD" que descompone los caracteres acentuados
    nfkd_form = unicodedata.normalize('NFD', text)
    # Filtramos los caracteres que no son letras base (como tildes)
    return ''.join([char for char in nfkd_form if not unicodedata.combining(char)])

def getDDIData(fileName):
    # Parsear el archivo XML
    tree = ET.parse('../../../../Lake/data/' + fileName)
    root = tree.getroot()

    # Definir el espacio de nombres que se está utilizando en el archivo XML
    namespaces = {'ddi': 'http://www.icpsr.umich.edu/DDI'}

    datasets = {}

    # Buscar los elementos <fileDscr> usando el espacio de nombres
    for file_dscr in root.findall(".//ddi:fileDscr", namespaces):
        file_id = file_dscr.attrib.get("ID")  # Obtener el ID del dataset

        # Encontrar el nombre del archivo en <fileTxt> -> <fileName>
        file_name_element = file_dscr.find(".//ddi:fileTxt//ddi:fileName", namespaces)
        if file_name_element is not None:
            # Limpiar el nombre del archivo eliminando \n, espacios extra, y tildes
            file_name = file_name_element.text.strip()
            # print("=========================" + file_name + "=================================")

            file_name = remove_accents(file_name)  # Eliminar tildes
            # file_name = file_name.replace(' ', '_')
            print("=========================" + file_name + "=================================")

            file_name = file_name.replace('.NSDstat', '')
            # if len(file_name) > 32:
            #     file_name = file_name[0:55].replace('_', '')
            # print("=========================" + file_name + "=================================")
            

            # Inicializar una lista para las variables de este dataset
            datasets[file_id] = {"file_name": file_name, "variables": []}

    # Ahora buscamos las variables dentro del archivo XML
    for var in root.findall(".//ddi:var", namespaces):
        var_id = var.attrib.get("ID")  # Obtener el ID de la variable
        var_name = var.attrib.get("name")  # Obtener el nombre de la variable
        file_ref = var.attrib.get("files")  # A qué archivo pertenece

        # Obtener la descripción de la variable en <labl>
        label_element = var.find(".//ddi:labl", namespaces)
        label = label_element.text.strip() if label_element is not None else "Sin descripción"

        # Obtener el tipo de dato en <varFormat>
        var_format_element = var.find(".//ddi:varFormat", namespaces)
        var_type = var_format_element.attrib.get("type") if var_format_element is not None else "Desconocido"

        # Si el archivo tiene un ID y pertenece a uno de los datasets encontrados
        if file_ref in datasets:
            datasets[file_ref]["variables"].append({
                "var_id": var_id,
                "var_name": var_name,
                "description": label,
                "var_type": var_type
            })

    # Mostrar los datasets y sus variables
    for dataset_id, dataset_info in datasets.items():
        print(f"Dataset ID: {dataset_id}")
        print(f"Nombre del Dataset: {dataset_info['file_name']}")
        print("Variables:")
        for var in dataset_info["variables"]:
            print(f"  ID: {var['var_id']}, Nombre: {var['var_name']}, Descripción: {var['description']}")
            createDescription(dataset_info['file_name'], var['var_name'], var['description'], var['var_type'])
        print()

