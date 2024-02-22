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


def ScaricaDatiSA3New():

    # connetto a FTP e prelievo il file
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/SA3')
    gFile = open("201905_SA3.csv", "wb")
    ftp.retrbinary('RETR 201905_SA3.csv', gFile.write)
    gFile.close()

    gFile = open("DBSA3.csv", "wb")
    ftp.retrbinary('RETR DBSA3.csv', gFile.write)
    gFile.close()

    # aggiungo i nuovi dati al database
    df = pd.read_csv("201905_SA3.csv", header='infer', delimiter=';', encoding='UTF-16')

    dfDB = pd.read_csv("DBSA3.csv", on_bad_lines='skip', header='infer', delimiter=',')
    tDB = dfDB["t"]

    tDB = pd.to_datetime(tDB)
    # dfDB["timestamp"] = tDB
    last_t_stored = tDB.iloc[-1]

    Tempi = df['LocalCol']
    Tempi = pd.to_datetime(Tempi)

    lastFromPLC = Tempi.iloc[-1]

    if lastFromPLC > last_t_stored:

        DBName = "DBSA3.csv"

        col = list(dfDB.columns)
        dataToStore = df[Tempi > last_t_stored]

        # Selezione delle variabili
        TempiToStore = dataToStore['TimeString']
        TempiToStore = pd.to_datetime(TempiToStore, format='%Y-%m-%d %H:%M:%S')

        QToStore = df["Durchfluss"]
        QToStore = pd.Series(QToStore).str.replace(',', '.')
        QToStore[QToStore == "1.#QNAN"] = float("nan")
        QToStore = QToStore.astype(float)
        QToStore.name = col[1]

        BarToStore = ValoriToStore[VariabiliToStore == "Pressione_FTP"]
        BarToStore = pd.Series(BarToStore).str.replace(',', '.')
        BarToStore[BarToStore == "1.#QNAN"] = float("nan")
        BarToStore = BarToStore.astype(float)
        BarToStore.name = col[2]

        PToStore = ValoriToStore[VariabiliToStore == "Potenza_FTP"]
        PToStore = pd.Series(PToStore).str.replace(',', '.')
        PToStore[PToStore == "1.#QNAN"] = float("nan")
        PToStore = PToStore.astype(float)
        PToStore.name = col[3]

        CosPhiToStore = ValoriToStore[VariabiliToStore == "CosPhi_FTP"]
        CosPhiToStore = pd.Series(CosPhiToStore).str.replace(',', '.')
        CosPhiToStore[CosPhiToStore == "1.#QNAN"] = float("nan")
        CosPhiToStore = CosPhiToStore.astype(float)
        CosPhiToStore.name = col[4]

        tToStore = TempiToStore[VariabiliToStore == "Portata_FTP"]
        tToStore = pd.to_datetime(tToStore, format='%d/%m/%Y %H:%M:%S')
        tToStore.name = col[0]

        dfToConcat = pd.concat([tToStore.reset_index(drop=True), QToStore.reset_index(drop=True),
                                BarToStore.reset_index(drop=True), PToStore.reset_index(drop=True),
                                CosPhiToStore.reset_index(drop=True)], axis=1)
        dfDBNew = pd.concat([dfDB, dfToConcat], ignore_index=True)
        dfDBNew.to_csv('DBSA3.csv', index=False)

        File = open(DBName, "rb")
        ftp.storbinary(f"STOR " + DBName, File)

    else:
        dfDBNew = dfDB

    ftp.close()

    return dfDBNew
