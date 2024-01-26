from ftplib import FTP
import pandas as pd
import numpy as np


def ScaricaDatiPartitore():

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/San_Teodoro')
    gFile = open("201833_Partitore.csv", "wb")
    ftp.retrbinary('RETR 201833_Partitore.csv', gFile.write)
    gFile.close()
    ftp.close()

    df = pd.read_csv("201833_Partitore.csv", on_bad_lines='skip', header='infer', delimiter=';')

    # Selezione delle variabili
    Tempi = df['TimeString']
    Variabili = df['VarName']
    Valori = df['VarValue']

    # Organizza valori
    indQ = [i for i, d in enumerate(Variabili) if d == 'Portata_FTP']
    TimeQ = Tempi[indQ]
    TimeQ = pd.to_datetime(TimeQ, format='%d/%m/%Y %H:%M:%S')
    TimeQ = TimeQ[1:len(TimeQ)]
    ValQ = Valori[indQ]
    ValQ = pd.Series(ValQ).str.replace(',', '.')
    ValQ = ValQ[1:len(ValQ)]
    ValQ = ValQ.astype(float)

    indP = [i for i, d in enumerate(Variabili) if d == 'Potenza_FTP']
    ValP = Valori[indP]
    ValP = pd.Series(ValP).str.replace(',', '.')
    ValP = ValP.astype(float)
    ValP = ValP[1:len(ValP)]

    indH = [i for i, d in enumerate(Variabili) if d == 'Pressione_FTP']
    ValH = Valori[indH]
    ValH = pd.Series(ValH).str.replace(',', '.')
    ValH = ValH.astype(float)
    ValH = ValH*10.1974
    ValH = ValH[1:len(ValH)]

    data = {"Time": np.array(TimeQ), "Charge": ValQ, "Power": ValP, "Jump": ValH}

    return data


def ScaricaDatiSA3():

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/SA3')
    gFile = open("201905_SA3.csv", "wb")
    ftp.retrbinary('RETR 201905_SA3.csv', gFile.write)
    gFile.close()
    ftp.close()
    df = pd.read_csv("201905_SA3.csv", on_bad_lines='skip', header='infer', delimiter=';', encoding='UTF-16')

    # Selezione delle variabili
    Tempi = df['LocalCol']
    ValQ = df['Durchfluss']
    ValH = df['Druck Eingang']
    ValP = df['Leistung']

    Tempi = pd.to_datetime(Tempi)

    ValQ = pd.Series(ValQ).str.replace(',', '.')
    ValQ = ValQ.astype(float)

    ValP = pd.Series(ValP).str.replace(',', '.')
    ValP = ValP.astype(float)

    ValH = pd.Series(ValH).str.replace(',', '.')
    ValH = ValH.astype(float)
    ValH = ValH*10.1974

    data = {"Time": np.array(Tempi), "Charge": ValQ, "Power": ValP, "Jump": ValH}

    return data
