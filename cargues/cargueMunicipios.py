import shapefile
import psycopg2
from shapely.geometry import shape
from shapely.wkt import dumps

def getShapeData():
    # Conectar a PostgreSQL
    conn = psycopg2.connect(
        host="localhost",  # Cambia esto si tu DB no está en localhost
        database="rcdatos",  # Tu base de datos
        user="postgres",  # Usuario de PostgreSQL
        password="admin"  # Contraseña de PostgreSQL
    )
    cursor = conn.cursor()

    # Leer el shapefile
    reader = shapefile.Reader("../../../Downloads/MGN2023_MPIO_POLITICO/MGN_ADM_MPIO_GRAFICO.shp")

    for sr in reader.shapeRecords():
        atr = dict(zip(["dpto_ccdgo", "mpio_ccdgo", "mpio_cdpmp"], sr.record))

        # Filtrar para insertar solo si no es el municipio 76001
        if atr["dpto_ccdgo"] != "76":
            # Convertir la geometría a formato WKT
            geom = sr.shape.__geo_interface__
            shapely_geom = shape(geom)  # Convertir a objeto Shapely
            wkt_geom = dumps(shapely_geom)  # Convertir a WKT

            # Preparar el SQL para insertar los datos
            insert_query = """
                INSERT INTO divipola.rc_divipola_municipios_shape (codigomunicipio, geometry)
                VALUES (%s, ST_GeomFromText(%s, 4686));
            """
            cursor.execute(insert_query, (atr["mpio_cdpmp"], wkt_geom))

    # Confirmar los cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

# Llamar la función
getShapeData()
