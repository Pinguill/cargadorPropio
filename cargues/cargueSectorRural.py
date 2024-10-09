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
    query = "SELECT codigosectorrural FROM divipola.rc_divipola_sector_rural_shape"
    cursor.execute(query)
    
    data = cursor.fetchall()

    return data

def getShapeData():
    cursor = conn.cursor()

    # Leer el shapefile
    reader = shapefile.Reader("../../../Downloads/MGN2023_RUR_SECTOR/MGN_RUR_SECTOR.shp")
    fields = reader.fields[1:9]
    field_names = [field[0] for field in fields]
    
    data = getDBdata()

    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        if atr["dpto_ccdgo"] == "76":
            print(atr)
            flag = False
            for i in range(len(data)):
                if atr['setr_ccnct'] == data[i][0]:
                    flag = True
            if flag == False:
                print(atr['setr_ccnct'], atr['mpio_cdpmp'])
                geom = sr.shape.__geo_interface__
                shapely_geom = shape(geom)  # Convertir a objeto Shapely
                wkt_geom = dumps(shapely_geom)  # Convertir a WKT

                # Preparar el SQL para insertar los datos
                insert_query = """
                    INSERT INTO divipola.rc_divipola_sector_rural_shape(codigosectorrural, codigomunicipio, geometry)
                    VALUES (%s, %s, ST_GeomFromText(%s, 4686));
                """
                cursor.execute(insert_query, (atr['setr_ccnct'], atr['mpio_cdpmp'], wkt_geom))
    
    
    
    # 760011000
    # 
    

    # Confirmar los cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

getShapeData()

# setu_ccnct