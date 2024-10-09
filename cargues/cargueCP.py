import shapefile
import psycopg2
from shapely.geometry import shape
from shapely.wkt import dumps

# Conectar a PostgreSQL
conn = psycopg2.connect(
    host="localhost",  # Cambia esto si tu DB no está en localhost
    database="rcdatos",  # Tu base de datos
    user="postgres",  # Usuario de PostgreSQL
    password="admin"  # Contraseña de PostgreSQL
    )

def getDBdata():
    cursor = conn.cursor()
    query = "SELECT codigocentropoblado FROM divipola.rc_divipola_centros_poblados"
    cursor.execute(query)
    
    data = cursor.fetchall()

    return data

def getShapeData():
    cursor = conn.cursor()

    # Leer el shapefile
    reader = shapefile.Reader("../../../Downloads/MGN2023_URB_ZONA_URBANA/MGN_URB_ZONA_URBANA.shp")
    fields = reader.fields[1:13]
    field_names = [field[0] for field in fields]
    
    data = getDBdata()

    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        if atr["dpto_ccdgo"] == "76":
            # print(atr)
            flag = False
            for i in range(len(data)):
                if atr['zu_cdivi'] == data[i][0]:
                    flag = True
            if flag == False:
                print(atr['mpio_cdpmp'], atr['zu_cdivi'], atr['zu_cnmbre'], atr['clas_ccdgo'])

                # Preparar el SQL para insertar los datos
                insert_query = """
                    INSERT INTO divipola.rc_divipola_centros_poblados(codigomunicipio, codigocentropoblado, nombrecentropoblado, tipo)
                    VALUES (%s, %s, %s, %s);
                """
                cursor.execute(insert_query, (atr['mpio_cdpmp'], atr['zu_cdivi'], atr['zu_cnmbre'], '''CP''' ))
    
    # 76001100000
    # 76147200202

    # Confirmar los cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

getShapeData()

# setu_ccnct