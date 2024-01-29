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

    # aggiungo i nuovi dati al database
    df = pd.read_csv("201832_SanTeodoro.csv", on_bad_lines='skip', header='infer', delimiter=';')

    dfDB = pd.read_csv("DBST.csv", on_bad_lines='skip', header='infer', delimiter=',')
    tDB = dfDB["timestamp"]

    tDB = pd.to_datetime(tDB)
    # dfDB["timestamp"] = tDB
    last_t_stored = tDB.iloc[-1]

    Tempi = df['TimeString']
    Tempi = pd.to_datetime(Tempi, format='%d/%m/%Y %H:%M:%S')

    lastFromPLC = Tempi.iloc[-1]

    if lastFromPLC > last_t_stored:

        col = list(dfDB.columns)
        dataToStore = df[Tempi > last_t_stored]

        # Selezione delle variabili
        VariabiliToStore = dataToStore['VarName']
        ValoriToStore = dataToStore['VarValue']
        TempiToStore = dataToStore['TimeString']
        TempiToStore = pd.to_datetime(TempiToStore, format='%d/%m/%Y %H:%M:%S')

        QToStore = ValoriToStore[VariabiliToStore == "Portata_FTP"]
        QToStore = pd.Series(QToStore).str.replace(',', '.')
        QToStore = QToStore.astype(float)
        QToStore.name = col[1]

        BarToStore = ValoriToStore[VariabiliToStore == "Pressione_FTP"]
        BarToStore = pd.Series(BarToStore).str.replace(',', '.')
        BarToStore = BarToStore.astype(float)
        BarToStore.name = col[2]

        PToStore = ValoriToStore[VariabiliToStore == "Potenza_FTP"]
        PToStore = pd.Series(PToStore).str.replace(',', '.')
        PToStore = PToStore.astype(float)
        PToStore.name = col[3]

        CosPhiToStore = ValoriToStore[VariabiliToStore == "CosPhi_FTP"]
        CosPhiToStore = pd.Series(CosPhiToStore).str.replace(',', '.')
        CosPhiToStore = CosPhiToStore.astype(float)
        CosPhiToStore.name = col[4]

        tToStore = TempiToStore[VariabiliToStore == "Portata_FTP"]
        tToStore = pd.to_datetime(tToStore, format='%d/%m/%Y %H:%M:%S')
        tToStore.name = col[0]

        dfToConcat = pd.concat([tToStore.reset_index(drop=True), QToStore.reset_index(drop=True),
                                BarToStore.reset_index(drop=True), PToStore.reset_index(drop=True),
                                CosPhiToStore.reset_index(drop=True)], axis=1)
        dfDBNew = pd.concat([dfDB, dfToConcat], ignore_index=True)
        dfDBNew.to_csv('DBST.csv', index=False)

    else:
        dfDBNew = dfDB

    return dfDBNew
