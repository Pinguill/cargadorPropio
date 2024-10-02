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
    reader = shapefile.Reader("../../../Downloads/MGN2023_MANZANA/MGN_URB_MANZANA.shp")
    fields = reader.fields[1:16]
    field_names = [field[0] for field in fields]
    field_names.append("manz_ccdgo")
    
    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        print(atr)
    
    #5071120020500100010113

    # Confirmar los cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

getShapeData()