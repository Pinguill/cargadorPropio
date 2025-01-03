import shapefile
import psycopg2
from shapely.geometry import shape
from shapely.wkt import dumps


conn = psycopg2.connect(
    host="localhost",  # Cambia esto si tu DB no está en localhost
    database="rcdatos",  # Tu base de datos
    user="postgres",  # Usuario de PostgreSQL
    password="admin"  # Contraseña de PostgreSQL
)

def getDBdata():
    cursor = conn.cursor()
    query = "SELECT codigomanzana FROM divipola.rc_divipola_manzanas_shape"
    cursor.execute(query)
    
    data = cursor.fetchall()

    return data


def getShapeData():
    # Conectar a PostgreSQL
    cursor = conn.cursor()

    # Leer el shapefile
    reader = shapefile.Reader("../../../Downloads/MGN2023_MANZANA/MGN_URB_MANZANA.shp")
    fields = reader.fields[1:17]
    field_names = [field[0] for field in fields]

    data = getDBdata()
    flag = False
    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        if atr['dpto_ccdgo'] == '76':
            # print(atr["manz_ccnct"])
            flag = False
            for i in range(len(data)):
                    if atr['manz_ccnct'] == data[i][0]:
                        # print(atr["manz_ccnct"])
                        flag = True
            if flag == False:
                print(atr["manz_ccnct"], atr['zu_cdivi'], atr['secu_ccnct'])
                geom = sr.shape.__geo_interface__
                shapely_geom = shape(geom)  # Convertir a objeto Shapely
                wkt_geom = dumps(shapely_geom)  # Convertir a WKT

                # Preparar el SQL para insertar los datos
                insert_query = """
                    INSERT INTO divipola.rc_divipola_manzanas_shape(codigomanzana, codigocentropoblado, codigoseccionurbano, geometry)
                    VALUES (%s, %s, %s, ST_GeomFromText(%s, 4686));
                """
                cursor.execute(insert_query, (atr["manz_ccnct"], atr['zu_cdivi'], atr['secu_ccnct'], wkt_geom))
                 
    # 5071120020500100010113
    # 76001100000000010101

    # Confirmar los cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

getShapeData()