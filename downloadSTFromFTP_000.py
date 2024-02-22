from ftplib import FTP
import pandas as pd


def ScaricaDatiPAR():

    # connetto a FTP e prelievo il file
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/San_Teodoro')
    gFile = open("201833_Partitore.csv", "wb")
    ftp.retrbinary('RETR 201833_Partitore.csv', gFile.write)
    gFile.close()

    gFile = open("DBPAR.csv", "wb")
    ftp.retrbinary('RETR DBPAR.csv', gFile.write)
    gFile.close()

    # aggiungo i nuovi dati al database
    df = pd.read_csv("201833_Partitore.csv", on_bad_lines='skip', header='infer', delimiter=';')

    dfDB = pd.read_csv("DBPAR.csv", on_bad_lines='skip', header='infer', delimiter=',')
    tDB = dfDB["tQ"]

    tDB = pd.to_datetime(tDB)
    # dfDB["timestamp"] = tDB
    last_t_stored = tDB.iloc[-1]

    Tempi = df['TimeString']
    Tempi = pd.to_datetime(Tempi, format='%d/%m/%Y %H:%M:%S')

    lastFromPLC = Tempi.iloc[-1]
    DBName = "DBPAR.csv"

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
        dfDBNew.to_csv('DBPAR.csv', index=False)

        File = open(DBName, "rb")
        ftp.storbinary(f"STOR " + DBName, File)

    else:
        dfDBNew = dfDB

    ftp.close()

    return dfDBNew


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

        DBName = "DBST.csv"

        col = list(dfDB.columns)
        dataToStore = df[Tempi > last_t_stored]

        # Selezione delle variabili
        VariabiliToStore = dataToStore['VarName']
        ValoriToStore = dataToStore['VarValue']
        TempiToStore = dataToStore['TimeString']
        TempiToStore = pd.to_datetime(TempiToStore, format='%d/%m/%Y %H:%M:%S')

        QToStore = ValoriToStore[VariabiliToStore == "Portata_FTP"]
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
        dfDBNew.to_csv('DBST.csv', index=False)

        File = open(DBName, "rb")
        ftp.storbinary(f"STOR " + DBName, File)

    else:
        dfDBNew = dfDB

    ftp.close()

    return dfDBNew
