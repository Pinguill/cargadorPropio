import pandas as pd

def getdf():
    xls = pd.ExcelFile('../../../../Lake/data/TR_CENSO_FALLECIDOS.xlsx')
    for sheet_name in xls.sheet_names:
        print(sheet_name.lower())
        df = pd.read_excel(xls, sheet_name=sheet_name)

        print(df)


def callFuntion(x):
    print("otra funcion jaja", x)
    getdf()

callFuntion(1)