import pandas as pd
import psycopg2

xls = pd.ExcelFile("C:/ingenieriadatos/datos/CENSOExcel/TR_CENSO_FALLECIDOS.xlsx", engine='openpyxl')

conn = psycopg2.connect(
                host= 'localhost',
                database= 'rcdatos',
                user= 'postgres',
                password= 'admin'
            )

cursor = conn.cursor()


table = "rc_censo_fallecidos"
consulta = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '{table}'
"""

schema = 'CENSO'
query = 'SELECT '
tableNames = xls.sheet_names

cursor.execute(consulta)

columnas = cursor.fetchall()

for columna in columnas:
    query += columna[0] + ', '
    for name in tableNames:
        # print(columna[0] + ',', name.lower() + ',')
        if columna[0] == name.lower():
            query += '{}.descripcion, '.format(name)
        # print(columna[0])

query = query[:-2]
query +=  '\nFROM {}.{}\n'.format(schema, table)

cursor.close()
conn.close()


for name in tableNames:
    query += 'INNER JOIN structured.{} ON {}.codigo = {}.{}\n'.format(name, name, table, name)
print(query + ';')