from ftplib import FTP
import pandas as pd
import numpy as np


def ScaricaDatiPartitore():

    # connetto a FTP e prelievo il file
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/San_Teodoro')
    gFile = open("201833_Partitore.csv", "wb")
    ftp.retrbinary('RETR 201833_Partitore.csv', gFile.write)
    gFile.close()
    df = pd.read_csv("201833_Partitore.csv", on_bad_lines='skip', header='infer', delimiter=';')
    ftp.close()

    # Selezione delle variabili
    Tempi = df['TimeString']
    Variabili = df['VarName']
    Valori = df['VarValue']

    # Organizza valori
    indQ = [i for i, d in enumerate(Variabili) if d == 'Portata_FTP']
    TimeQ = Tempi[indQ]
    TimeQ = TimeQ[1:len(TimeQ)]
    TimeQ = pd.to_datetime(TimeQ, format='%d/%m/%Y %H:%M:%S')

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

    data = {"Time": np.array(TimeQ), "Charge": np.array(ValQ), "Power": np.array(ValP), "Jump": np.array(ValH)}

    return data


def ScaricaDatiST():

    # connetto a FTP e prelievo il file
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/San_Teodoro')
    gFile = open("201832_SanTeodoro.csv", "wb")
    ftp.retrbinary('RETR 201832_SanTeodoro.csv', gFile.write)
    gFile.close()

    gFile = open("DBST.csv", "wb")
    ftp.retrbinary('RETR DBST.csv', gFile.write)
    gFile.close()

    ftp.close()

    df = pd.read_csv("201832_SanTeodoro.csv", on_bad_lines='skip', header='infer', delimiter=';')

    # Selezione delle variabili
    Tempi = df['TimeString']
    Variabili = df['VarName']
    Valori = df['VarValue']

    # Organizza valori
    indQ = [i for i, d in enumerate(Variabili) if d == 'Portata_FTP']
    TimeQ = Tempi[indQ]
    TimeQ = pd.to_datetime(TimeQ, format='%d/%m/%Y %H:%M:%S')
    ValQ = Valori[indQ]
    ValQ = pd.Series(ValQ).str.replace(',', '.')
    ValQ = ValQ.astype(float)

    indP = [i for i, d in enumerate(Variabili) if d == 'Potenza_FTP']
    ValP = Valori[indP]
    ValP = pd.Series(ValP).str.replace(',', '.')
    ValP = ValP.astype(float)

    indH = [i for i, d in enumerate(Variabili) if d == 'Pressione_FTP']
    ValH = Valori[indH]
    ValH = pd.Series(ValH).str.replace(',', '.')
    ValH = ValH.astype(float)
    ValH = ValH*10.1974

    TimeQ = TimeQ[1:]
    ValQ = ValQ[1:]

    data = {"Time": np.array(TimeQ), "Charge": np.array(ValQ), "Power": np.array(ValP), "Jump": np.array(ValH)}

    # aggiungo i nuovi dati al database
    # dfOld = pd.read_csv("DBST.csv", on_bad_lines='skip', header='infer', delimiter=';')
    #
    # TempiOld = dfOld['TimeString']
    # TempiOld = pd.to_datetime(TempiOld, format='%d/%m/%Y %H:%M:%S')
    # last_tOld = TempiOld.iloc[-1]
    #
    # last_New = TimeQ.iloc[-1]
    #
    # NewIndexes = pd.DatetimeIndex(TimeQ)
    # OldIndexes = pd.DatetimeIndex(TempiOld)
    # toAdd = TimeQ[last_New > last_tOld]
    # C = 5

    return data
