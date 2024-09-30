import pandas as pd

def main():
    df = pd.read_csv ("../../../../Lake/data/prueba3.CSV", encoding='latin1',  header=None, on_bad_lines='warn', )
    df.columns = df.iloc[0]

    # Eliminar la fila 0 que ahora es el encabezado
    df = df[1:].reset_index(drop=True)

    print(df.head())

    print(df['SEDE_CODIGO'])

main()