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
    query = "SELECT codigosectorurbano FROM divipola.rc_divipola_sector_urbano_shape"
    cursor.execute(query)
    
    data = cursor.fetchall()

    return data

def getShapeData():
    cursor = conn.cursor()

    # Leer el shapefile
    reader = shapefile.Reader("../../../Downloads/MGN2023_URB_SECTOR/MGN_URB_SECTOR.shp")
    fields = reader.fields[1:13]
    field_names = [field[0] for field in fields]
    
    data = getDBdata()

    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        if atr["dpto_ccdgo"] == "76":
            # print(atr)
            flag = False
            for i in range(len(data)):
                if atr['setu_ccnct'] == data[i][0]:
                    flag = True
            if flag == False:
                print(atr['setu_ccnct'], atr['zu_cdivi'])
                geom = sr.shape.__geo_interface__
                shapely_geom = shape(geom)  # Convertir a objeto Shapely
                wkt_geom = dumps(shapely_geom)  # Convertir a WKT

                # Preparar el SQL para insertar los datos
                insert_query = """
                    INSERT INTO divipola.rc_divipola_sector_urbano_shape(codigosectorurbano, codigocentropoblado, geometry)
                    VALUES (%s, %s, ST_GeomFromText(%s, 4686));
                """
                cursor.execute(insert_query, (atr['setu_ccnct'], atr['zu_cdivi'], wkt_geom))
    
    
    
    
    

    # Confirmar los cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

getShapeData()

# setu_ccnct