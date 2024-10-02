import pandas as pd

def getdf():
    df = pd.read_csv('../../../../Lake/data/caratula.CSV', encoding='latin1')
    print(df)


def callFuntion(x):
    print("otra funcion jaja", x)
    getdf()