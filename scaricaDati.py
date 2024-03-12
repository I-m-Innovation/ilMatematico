from ftplib import FTP
import pandas as pd
from datetime import datetime, timedelta
from quest2HigecoDefs import call2Higeco
import numpy as np
import pytz
from dateutil import tz
import os
import shutil


def ScaricaDatiSA3():

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

    dfDB = pd.read_csv("DBSA3.csv", on_bad_lines='skip', header='infer', delimiter=',')
    tDB = dfDB["t"]

    tDB = pd.to_datetime(tDB, format='mixed')
    # dfDB["timestamp"] = tDB
    last_t_stored = tDB.iloc[-1]

    try:
        df = pd.read_csv("201905_SA3.csv", header='infer', delimiter=';', encoding='UTF-16')
        Tempi = df['LocalCol']
        Tempi = pd.to_datetime(Tempi, format='mixed')
        lastFromPLC = Tempi.iloc[-1]

    except Exception as err:
        df = []
        print(err)
        if str(err) == "No columns to parse from file":
            print("File FTP vuoto")
        else:
            print(err)
        lastFromPLC = last_t_stored

    if lastFromPLC > last_t_stored:

        DBName = "DBSA3.csv"

        col = list(dfDB.columns)

        # Selezione delle variabili

        QToStore = df["Durchfluss"]
        QToStore = pd.Series(QToStore).str.replace(',', '.')
        QToStore[QToStore == "1.#QNAN"] = float("nan")
        QToStore = QToStore.astype(float)
        QToStore.name = col[1]

        BarToStore = df["Druck Eingang"]
        BarToStore = pd.Series(BarToStore).str.replace(',', '.')
        BarToStore[BarToStore == "1.#QNAN"] = float("nan")
        BarToStore = BarToStore.astype(float)
        BarToStore.name = col[2]

        PToStore = df["Leistung"]
        PToStore = pd.Series(PToStore).str.replace(',', '.')
        PToStore[PToStore == "1.#QNAN"] = float("nan")
        PToStore = PToStore.astype(float)
        PToStore.name = col[3]

        CosPhiToStore = df["CosPhi"]
        CosPhiToStore = pd.Series(CosPhiToStore).str.replace(',', '.')
        CosPhiToStore[CosPhiToStore == "1.#QNAN"] = float("nan")
        CosPhiToStore = CosPhiToStore.astype(float)
        CosPhiToStore.name = col[4]

        tToStore = df["LocalCol"]
        tToStore = pd.to_datetime(tToStore, format='mixed')
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


def sistemaCartellaFTP_TF(Plant):
    if Plant == "TF":
        DBileName = "DBTFNEW4.csv"

        # Entro nella cartella FTP
        ftp = FTP("192.168.10.211", timeout=120)
        ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
        ftp.cwd('/dati/Torrino_Foresta')

        Files = ftp.nlst()

        for File in range(len(Files)):

            try:
                currFile = Files[File]

                if currFile[0:4] == "DATI":
                    #   apro i file
                    gFile = open(DBileName, "wb")
                    ftp.retrbinary("RETR " + DBileName, gFile.write)
                    gFile.close()

                    DB = pd.read_csv(DBileName, on_bad_lines='skip', header='infer', delimiter=',')
                    gFile = open(currFile, "wb")
                    ftp.retrbinary('RETR ' + currFile, gFile.write)
                    gFile.close()

                    newLine = pd.read_csv(currFile, on_bad_lines='skip', header='infer', delimiter=';')
                    DataNewLine = newLine.iloc[1, :]

                    newTime = DataNewLine.iloc[0]
                    test = datetime.strptime(newTime, '%d/%m/%Y %H:%M:%S')
                    test2 = datetime(test.year, test.month, test.day, test.hour, test.minute, test.second,
                                     tzinfo=pytz.utc)
                    to_zone = tz.tzlocal()
                    central = test2.astimezone(to_zone)
                    newTimeLoc = datetime(central.year, central.month, central.day, central.hour,
                                          central.minute, central.second)
                    newTimeLoc = datetime.strftime(newTimeLoc, format="%Y-%m-%d %H:%M:%S")

                    newPT_Linea = float(DataNewLine.iloc[1].replace(",", "."))
                    newPT_Turbina = float(DataNewLine.iloc[2].replace(",", "."))
                    newPotAtt = float(DataNewLine.iloc[3].replace(",", "."))
                    newPort = float(DataNewLine.iloc[4].replace(",", "."))
                    newCosPhi = float(DataNewLine.iloc[5].replace(",", "."))
                    newLevStram = float(DataNewLine.iloc[6].replace(",", "."))
                    newPortata1600 = float(DataNewLine.iloc[7].replace(",", "."))
                    newPressioneUscita = float(DataNewLine.iloc[8].replace(",", "."))
                    newLevScarico = float(DataNewLine.iloc[9].replace(",", "."))

                    NewLine = pd.DataFrame({'x__TimeStamp': [newTime], 'Local': [newTimeLoc],
                                            'PLC1_AI_PT_LINEA': [newPT_Linea],
                                            'PLC1_AI_PT_TURBINA': [newPT_Turbina], 'PLC1_AI_POT_ATTIVA': [newPotAtt],
                                            'PLC1_AI_FT_PORT_IST': [newPort], 'PLC1_AI_COSPHI': [newCosPhi],
                                            'PLC1_AI_LT_STRAMAZZO': [newLevStram],
                                            'PLC1_AI_FT_PORT_COND': [newPortata1600],
                                            'PLC1_AI_PT_TURBINA_OUT': [newPressioneUscita],
                                            'PLC1_AI_LT_SCAR_C': [newLevScarico]})

                    newDB = pd.concat([DB, NewLine], ignore_index=True)
                    newDB.to_csv(DBileName, index=False)

                    #   Salvo il nuovo database
                    File = open(DBileName, "rb")
                    ftp.storbinary(f"STOR " + DBileName, File)
                    ftp.delete(currFile)

                    thisFolder = os.getcwd()
                    source = thisFolder + "\\" + currFile
                    destination = thisFolder + "\\Database impianti\\Torrino Foresta\\Dati raw\\" + currFile
                    # destination = destFolder + currFile
                    shutil.move(source, destination)

            except Exception as err:
                print(err)

        ftp.close()


def ScaricaDatiTF():
    # print("Sono entrato in scaricaDatiPG")
    # connetto a FTP e prelievo il file
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/Torrino_Foresta')
    gFile = open("DBTFNEW4.csv", "wb")
    ftp.retrbinary('RETR DBTFNEW4.csv', gFile.write)
    gFile.close()
    ftp.close()

    df = pd.read_csv("DBTFNEW4.csv", on_bad_lines='skip', header='infer', delimiter=',', low_memory=False)

    # Selezione delle variabili
    TempiString = df['Local']
    Tempi = TempiString
    Tempi = np.array(Tempi)  # in UTC
    TimeQ = pd.to_datetime(Tempi, format='mixed')

    ValQ = df['PLC1_AI_FT_PORT_IST']

    ValP = df['PLC1_AI_POT_ATTIVA']

    ValHMonte = df['PLC1_AI_PT_TURBINA']

    ValHValle = df['PLC1_AI_PT_TURBINA_OUT']
    ValHValle = ValHValle.fillna(ValHValle.mean())
    ValH = ValHMonte - ValHValle

    OutVal = np.empty([len(ValH), 1])

    for i in range(len(ValH)):
        try:
            OutVal[i] = float(ValH[i])  # *10.1974
        except Exception as err:
            print(err)
            OutVal[i] = float('nan')

    ValH = ValH * 10.1974

    data = {"Time": TimeQ, "Charge": ValQ, "Power": ValP, "Jump": ValH}

    data = pd.DataFrame.from_dict(data)

    return data


def sistemaCartellaFTPPG():

    #   Entro nella cartella FTP
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/ponte_giurino')

    Files = ftp.nlst()

    for File in range(len(Files)):
        currFile = Files[File]

        if currFile[0:4] == "DATI":
            #   apro i file
            gFile = open('DBPGNEW.csv', "wb")
            ftp.retrbinary('RETR DBPGNEW.csv', gFile.write)
            gFile.close()

            DB = pd.read_csv("DBPGNEW.csv", on_bad_lines='skip', header='infer', delimiter=',',
                             low_memory=False)
            gFile = open(currFile, "wb")
            ftp.retrbinary('RETR '+currFile, gFile.write)
            gFile.close()

            newLine = pd.read_csv(currFile, on_bad_lines='skip', header='infer', delimiter=';')
            DataNewLine = newLine.iloc[1, :]

            newTime = DataNewLine.iloc[0]
            test = datetime.strptime(newTime, '%d/%m/%Y %H:%M:%S')
            test2 = datetime(test.year, test.month, test.day, test.hour, test.minute, test.second,
                             tzinfo=pytz.utc)
            to_zone = tz.tzlocal()
            central = test2.astimezone(to_zone)

            newTimeLoc = datetime(central.year, central.month, central.day, central.hour,
                                  central.minute, central.second)
            newTimeLoc = datetime.strftime(newTimeLoc, format="%d/%m/%Y %H:%M:%S")

            newPT_Linea = float(DataNewLine.iloc[1].replace(",", "."))
            newPT_Turbina = float(DataNewLine.iloc[2].replace(",", "."))
            newPotAtt = float(DataNewLine.iloc[3].replace(",", "."))
            newPort = float(DataNewLine.iloc[4].replace(",", "."))
            newCosPhi = float(DataNewLine.iloc[5].replace(",", "."))
            newLevStram = float(DataNewLine.iloc[6].replace(",", "."))

            NewLine = pd.DataFrame({'x__TimeStamp': [newTime], 'Local': [newTimeLoc],
                                    'PLC1_AI_PT_LINEA': [newPT_Linea], 'PLC1_AI_PT_TURBINA': [newPT_Turbina],
                                    'PLC1_AI_POT_ATTIVA': [newPotAtt], 'PLC1_AI_FT_PORT_IST': [newPort],
                                    'PLC1_AI_COSPHI': [newCosPhi], 'PLC1_AI_LT_STRAMAZZO': [newLevStram]})

            newDB = pd.concat([DB, NewLine], ignore_index=True)
            newDB.to_csv("DBPGNEW.csv", index=False)

            #   Salvo il nuovo database
            File = open("DBPGNEW.csv", "rb")
            ftp.storbinary(f"STOR DBPGNEW.csv", File)
            ftp.delete(currFile)

            thisFolder = os.getcwd()
            source = thisFolder + "\\" + currFile
            destination = thisFolder + "\\Database impianti\\Ponte Giurino\\Dati raw\\" + currFile
            shutil.move(source, destination)

    ftp.close()


def ScaricaDatiPG():

    # connetto a FTP e prelievo il file

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/ponte_giurino')
    gFile = open("DBPGNEW.csv", "wb")
    ftp.retrbinary('RETR DBPGNEW.csv', gFile.write)
    gFile.close()
    df = pd.read_csv("DBPGNEW.csv", on_bad_lines='skip', header='infer', delimiter=',', low_memory=False)
    ftp.close()

    return df


def ScaricaDatiCST(PlantData):

    rho = 1000
    g = 9.81
    Now = datetime.now()

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/San_Teodoro')
    gFile = open("DBCST.csv", "wb")
    ftp.retrbinary('RETR DBCST.csv', gFile.write)
    gFile.close()

    DBCST = pd.read_csv("DBCST.csv")

    t = DBCST["t"]
    t = pd.to_datetime(t)
    QPAR = DBCST["QPAR"]
    QST = DBCST["QST"]
    Q = DBCST["Q"]
    PPAR = DBCST["PPAR"]
    PST = DBCST["PST"]
    P = DBCST["P"]
    Bar = DBCST["Bar"]
    etaPAR = DBCST["etaPAR"]
    etaST = DBCST["etaST"]
    eta = DBCST["eta"]

    tPARNew = PlantData["PAR"]["DB"]["timestamp"]
    tPARNew = pd.to_datetime(tPARNew)
    tSTNew = PlantData["ST"]["DB"]["timestamp"]
    tSTNew = pd.to_datetime(tSTNew)
    QPARNew = PlantData["PAR"]["DB"]["Portata [l/s]"]
    QSTNew = PlantData["ST"]["DB"]["Portata [l/s]"]
    PPARNew = PlantData["PAR"]["DB"]["Potenza [kW]"]
    PSTNew = PlantData["ST"]["DB"]["Potenza [kW]"]
    BARPARNew = PlantData["PAR"]["DB"]["Pressione [bar]"]
    BARSTNew = PlantData["ST"]["DB"]["Pressione [bar]"]

    etaPARNew = np.divide(1000 * PPARNew, rho * g * np.multiply(QPARNew / 1000, BARPARNew) * 10.1974)
    etaSTNew = np.divide(1000 * PSTNew, rho * g * np.multiply(QSTNew / 1000, BARSTNew) * 10.1974)

    tCurr = t.iloc[-1]

    while tCurr <= Now-timedelta(hours=1):

        t = pd.concat([t, pd.Series(tCurr + timedelta(minutes=30))], ignore_index=True, axis=0)

        QMeanPAR = np.mean(QPARNew[(tPARNew >= tCurr) & (tPARNew < tCurr + timedelta(hours=1))])
        QMeanST = np.mean(QSTNew[(tSTNew >= tCurr) & (tSTNew < tCurr + timedelta(hours=1))])
        QPAR = pd.concat([QPAR, pd.Series(QMeanPAR)], ignore_index=True, axis=0)
        QST = pd.concat([QST, pd.Series(QMeanST)], ignore_index=True, axis=0)
        QS = QMeanPAR + QMeanST
        Q = pd.concat([Q, pd.Series(QS)], ignore_index=True)

        PMeanPAR = np.mean(PPARNew[(tPARNew >= tCurr) & (tPARNew < tCurr + timedelta(hours=1))])
        PMeanST = np.mean(PSTNew[(tSTNew >= tCurr) & (tSTNew < tCurr + timedelta(hours=1))])
        PPAR = pd.concat([PPAR, pd.Series(PMeanPAR)], ignore_index=True, axis=0)
        PST = pd.concat([PST, pd.Series(PMeanST)], ignore_index=True, axis=0)
        PS = PMeanPAR + PMeanST
        P = pd.concat([P, pd.Series(PS)], ignore_index=True, axis=0)

        BARMeanPAR = np.mean(BARPARNew[(tPARNew >= tCurr) & (tPARNew < tCurr + timedelta(hours=1))])
        BARMeanST = np.mean(BARSTNew[(tSTNew >= tCurr) & (tSTNew < tCurr + timedelta(hours=1))])
        BARS = np.mean([BARMeanPAR, BARMeanST])
        Bar = pd.concat([Bar, pd.Series(BARS)], ignore_index=True, axis=0)

        etaMeanPAR = np.mean(etaPARNew[(tPARNew >= tCurr) & (tPARNew < tCurr + timedelta(hours=1))])
        etaMeanST = np.mean(etaSTNew[(tSTNew >= tCurr) & (tSTNew < tCurr + timedelta(hours=1))])
        etaPAR = pd.concat([etaPAR, pd.Series(etaMeanPAR)], ignore_index=True, axis=0)
        etaST = pd.concat([etaST, pd.Series(etaMeanST)], ignore_index=True, axis=0)
        etaS = (70 * etaMeanST + 25 * etaMeanST) / 95

        eta = pd.concat([eta, pd.Series(etaS)], ignore_index=True, axis=0)
        tCurr = tCurr + timedelta(hours=1)

    NewDict = {"t": t, "QPAR": QPAR, "QST": QST, "Q": Q, "PPAR": PPAR, "PST": PST, "P": P,
               "Bar": Bar, "etaPAR": etaPAR, "etaST": etaST, "eta": eta}

    NewDB = pd.DataFrame.from_dict(NewDict)
    NewDB.to_csv("DBCST.csv", index=False)

    File = open("DBCST.csv", "rb")
    ftp.storbinary(f"STOR " + "DBCST.csv", File)

    ftp.close()

    return DBCST


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
    tDB = dfDB["timestamp"]

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


def scaricaDatiRUB(token):

    # scarico il DB dalla FTP

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/Rubino')

    DBFileName = "DBRUB.csv"
    gFile = open(DBFileName, "wb")
    ftp.retrbinary("RETR " + DBFileName, gFile.write)
    gFile.close()

    # Carico il DB

    OldDB = pd.read_csv(DBFileName)
    t = OldDB["t"]
    t = pd.to_datetime(t)

    # prelevo l'informazione sull'ultimo timestamp
    lastTimeStamp = t.iloc[-1]

    # vado a scaricare i dati successivi al timestamp
    tOn = datetime.timestamp(lastTimeStamp)
    I = call2Higeco("RUB", "Irradiation", tOn, token)
    TModData = call2Higeco("RUB", "TMod", tOn, token)
    P1Data = call2Higeco("RUB", "Power1", tOn, token)
    P2Data = call2Higeco("RUB", "Power2", tOn, token)

    coeff = len(I) * len(TModData) * len(P1Data) * len(P2Data)

    if coeff > 0:
        new_t = I['t']
        new_t = new_t.apply(lambda x: x.replace(tzinfo=None))

        newI = I['Val']
        newI[newI == '#E2'] = float('nan')
        newI[newI == '#E3'] = float('nan')

        newT = TModData['Val']
        newT[newT == '#E2'] = float('nan')
        newT[newT == '#E3'] = float('nan')

        newP1 = P1Data['Val']
        newP1[newP1 == '#E2'] = float('nan')
        newP1[newP1 == '#E3'] = float('nan')

        newP2 = P2Data['Val']
        newP2[newP2 == '#E2'] = float('nan')
        newP2[newP2 == '#E3'] = float('nan')

        MergedDataDict = {"t": new_t, "I": newI, "TMod": newT, "P1": P1Data['Val'], "P2": P2Data['Val']}
        MergedData = pd.DataFrame.from_dict(MergedDataDict)

        newDB = pd.concat([OldDB, MergedData], ignore_index=True)
        newDB.to_csv("DBSCN.csv", index=False)
    else:
        newDB = OldDB

    File = open(DBFileName, "rb")
    ftp.storbinary(f"STOR " + DBFileName, File)
    ftp.close()

    return newDB


def scaricaDatiSCN(token):

    # scarico il DB dalla FTP

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/SCN')

    DBFileName = "DBSCN.csv"
    gFile = open(DBFileName, "wb")
    ftp.retrbinary("RETR " + DBFileName, gFile.write)
    gFile.close()

    # Carico il DB

    OldDB = pd.read_csv(DBFileName)
    t = OldDB["t"]
    t = pd.to_datetime(t)

    # prelevo l'informazione sull'ultimo timestamp
    lastTimeStamp = t.iloc[-1]+timedelta(minutes=5)
    if pd.isnull(lastTimeStamp):
        lastTimeStamp = t.iloc[-1]+timedelta(minutes=5)

    # vado a scaricare i dati successivi al timestamp
    tOn = datetime.timestamp(lastTimeStamp)
    I = call2Higeco("SCN", "Irradiation", tOn, token)
    TModData = call2Higeco("SCN", "TMod", tOn, token)
    P1Data = call2Higeco("SCN", "Power1", tOn, token)
    P2Data = call2Higeco("SCN", "Power2", tOn, token)

    coeff = len(I) * len(TModData) * len(P1Data) * len(P2Data)

    if coeff > 0:
        new_t = I['t']
        new_t = new_t.apply(lambda x: x.replace(tzinfo=None))

        newI = I['Val']
        newI[newI == '#E2'] = float('nan')
        newI[newI == '#E3'] = float('nan')

        newT = TModData['Val']
        newT[newT == '#E2'] = float('nan')
        newT[newT == '#E3'] = float('nan')

        newP1 = P1Data['Val']
        newP1[newP1 == '#E2'] = float('nan')
        newP1[newP1 == '#E3'] = float('nan')

        newP2 = P2Data['Val']
        newP2[newP2 == '#E2'] = float('nan')
        newP2[newP2 == '#E3'] = float('nan')

        MergedDataDict = {"t": new_t, "I": newI, "TMod": newT, "P1": P1Data['Val'], "P2": P2Data['Val']}
        MergedData = pd.DataFrame.from_dict(MergedDataDict)

        newDB = pd.concat([OldDB, MergedData], ignore_index=True)
        newDB.to_csv("DBSCN.csv", index=False)
    else:
        newDB = OldDB

    File = open(DBFileName, "rb")
    ftp.storbinary(f"STOR " + DBFileName, File)
    ftp.close()

    return newDB


def scaricaDati(Plant, token, Data):

    if Plant == "SCN":
        newDB = scaricaDatiSCN(token)
        newDB["PN"] = 926.64
        newDB["Tariffa"] = 0.225

    elif Plant == "RUB":
        newDB = scaricaDatiRUB(token)
        newDB["PN"] = 997.44
        newDB["Tariffa"] = 0.315
    elif Plant == "ST":
        newDB = ScaricaDatiST()
    elif Plant == "PAR":
        newDB = ScaricaDatiPAR()
    elif Plant == "PG":
        sistemaCartellaFTPPG()
        newDB = ScaricaDatiPG()
    elif Plant == "TF":
        sistemaCartellaFTP_TF("TF")
        newDB = ScaricaDatiTF()
    elif Plant == "CST":
        newDB = ScaricaDatiCST(Data)
    elif Plant == "SA3":
        newDB = ScaricaDatiSA3()
    else:
        newDB = []

    return newDB
