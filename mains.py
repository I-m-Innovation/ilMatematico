import numpy as np
import pandas as pd
from alertSystem import controllaFotovoltaico, controllaCentraleTG, sendTelegram, controllaCST
from colorama import Fore, Style
from quest2HigecoDefs import call2lastValue, authenticateHigeco, call2Higeco
from downloadSTFromFTP_000 import ScaricaDatiPAR
from downloadSA3FromFTP_000 import ScaricaDatiSA3New
from downloadPGFromFTP_000 import ScaricaDatiPG, sistemaCartellaFTPPG
from downloadTFFromFTP import ScaricaDatiTF, sistemaCartellaFTP_TF
from datetime import datetime, timedelta
import telebot
from ftplib import FTP


def mainSCN(PlantData, TGMode):

    token = authenticateHigeco("SCN")

    # controllo se l'impianto funziona e in caso mando l'allarme
    NewState = checkSCNProduction(token, PlantData)

    # mando gli allarmi se serve
    sendTelegram(PlantData["Plant state"]["SCN1"], NewState["SCN1"], TGMode, "SCN Pilota - Inverter 1")
    sendTelegram(PlantData["Plant state"]["SCN2"], NewState["SCN2"], TGMode, "SCN Pilota - Inverter 2")

    PlantData["Plant state"]["SCN1"] = NewState["SCN1"]
    displayState(NewState["SCN1"])

    PlantData["Plant state"]["SCN2"] = NewState["SCN2"]
    displayState(NewState["SCN2"])

    data = scaricaDatiSCN(token)
    data["PN"] = 926.64
    data["Tariffa"] = 0.225

    calcolaAggregatiPV(data, "SCN")

    return PlantData


def checkTFProduction(PlantData):

    DB = PlantData["DB"]
    N = len(DB["Time"])
    DatiIst = {"t": DB["Time"][N-1], "Q": DB["Charge"][N-1], "P": DB["Power"][N-1], "Pressure": DB["Jump"][N-1]}

    try:
        TFNewState = controllaCentraleTG(DatiIst, "TF", PlantData["Plant state"])

    except Exception as err:
        print(err)
        TFNewState = "U"

    return TFNewState


def mainTF(PlantData, TGMode):

    print("- Aggiornamento del database...")
    sistemaCartellaFTP_TF("TF")
    data = ScaricaDatiTF()
    data = pd.DataFrame.from_dict(data)
    print("-- Database aggiornato!")

    PlantData["DB"] = data

    NewState = checkTFProduction(PlantData)
    sendTelegram(PlantData["Plant state"], NewState, TGMode, "Torrino Foresta")
    PlantData["Plant state"] = NewState
    displayState(NewState)

    calcolaAggregatiHydro(data, "TF")

    return PlantData


def checkPARProduction(PlantData):

    DB = PlantData["DB"]
    N = len(DB["tQ"])
    DatiIst = {"t": DB["tQ"][N-1], "Q": DB["Q"][N-1], "P": DB["P"][N-1], "Pressure": DB["Bar"][N-1]}

    try:
        NewState = controllaCentraleTG(DatiIst, "PAR", PlantData["Plant state"])

    except Exception as err:
        print(err)
        NewState = "U"

    return NewState


def mainPartitore(PlantData, TGMode):

    print("- Download dei dati...")
    data = ScaricaDatiPAR()
    PlantData["DB"] = data
    NewState = checkPARProduction(PlantData)
    sendTelegram(PlantData["Plant state"], NewState, TGMode, "Partitore")
    PlantData["Plant state"] = NewState
    displayState(NewState)

    calcolaAggregatiHydro(data, "PAR")

    return PlantData


def calcolaPeriodiHydro(data, Period):

    Now = datetime.now()
    Tariffa = 0.21

    t = data["t"]
    t = pd.to_datetime(t)
    Q = data["Q"]
    P = data["P"]
    Bar = data["Bar"]
    eta = data["eta"]
    Q4Eta = data["Q4Eta"]

    if Period == "Annuale":
        tStart = datetime(Now.year, 1, 1, 0, 0, 0)
    elif Period == "Mensile":
        tStart = datetime(Now.year, Now.month, 1, 0, 0, 0)
    else:
        dt = timedelta(hours=24)
        tStart = Now - dt

    dt = Now - tStart

    QSel = Q[t >= tStart]
    PSel = P[t >= tStart]
    BarSel = Bar[t >= tStart]
    tSel = t[t >= tStart]
    etaSel = eta[t >= tStart]

    QMean = np.mean(QSel)
    QDev = np.std(QSel)

    PMean = np.mean(PSel)
    PDev = np.std(PSel)

    BarMean = np.mean(BarSel)
    BarDev = np.std(BarSel)

    etaMean = np.mean(etaSel[QSel >= Q4Eta])
    etaDev = np.std(etaSel[QSel >= Q4Eta])

    ESel = PMean * (dt.days * 24 + dt.seconds / 3600)
    FERSel = ESel * Tariffa

    NSamples = len(tSel)
    NOn = len(PSel[PSel > 0])

    if NSamples != 0:
        Av = NOn / NSamples
    else:
        Av = 0

    TLDict = {"t": tSel, "Q": QSel, "P": PSel, "Bar": BarSel, "Eta": etaSel}
    TLdf = pd.DataFrame.from_dict(TLDict)

    StatDict = {"QMean": [QMean], "QDev": [QDev], "PMean": [PMean], "PDev": [PDev], "BarMean": [BarMean],
                "BarDev": [BarDev], "etaMean": [etaMean], "etaDev": [etaDev], "Energy": [ESel], "Resa": [FERSel],
                "Availability": [Av]}

    Statdf = pd.DataFrame.from_dict(StatDict)

    return TLdf, Statdf


def calcolaAggregatiHydro(data, PlantTag):

    g = 9.81
    rho = 1000

    if PlantTag == "ST":

        t = data["timestamp"]
        t = pd.to_datetime(t)
        Q = data["Portata [l/s]"] / 1000
        P = data["Potenza [kW]"]
        Bar = data["Pressione [bar]"]
        Q4Eta = 10
        Q4Eta = Q4Eta / 1000
        FTPFolder = '/dati/San_Teodoro'

    elif PlantTag == "PG":

        t = data["Local"]
        t = pd.to_datetime(t)
        Q = data["PLC1_AI_FT_PORT_IST"] / 1000
        P = data["PLC1_AI_POT_ATTIVA"]
        Bar = data["PLC1_AI_PT_TURBINA"]
        Q4Eta = 10
        Q4Eta = Q4Eta / 1000
        FTPFolder = '/dati/ponte_giurino'

    elif PlantTag == "PAR":

        t = data["tQ"]
        t = pd.to_datetime(t)
        Q = data["Q"] / 1000
        P = data["P"]
        Bar = data["Bar"]
        Q4Eta = 5
        Q4Eta = Q4Eta / 1000

        FTPFolder = '/dati/San_Teodoro'

    elif PlantTag == "TF":

        t = data["Time"]
        t = pd.to_datetime(t)
        Q = data["Charge"]
        P = data["Power"]
        Bar = data["Jump"] / 10.1974
        Q4Eta = 0.6
        Q4Eta = Q4Eta
        FTPFolder = '/dati/Torrino_Foresta'

    elif PlantTag == "SA3":

        t = data["Time"]
        t = pd.to_datetime(t)
        Q = data["Charge"]
        P = data["Power"]
        Bar = data["Jump"] / 10.1974
        Q4Eta = 0.6
        Q4Eta = Q4Eta
        FTPFolder = '/dati/SA3'

    elif PlantTag == "CST":

        t = data["DB"]["t"]
        Q = data["DB"]["Q"]
        PPAR = data["DB"]["PPAR"]
        PST = data["DB"]["PST"]
        P = data["DB"]["P"]
        Bar = data["DB"]["Bar"]
        eta = data["DB"]["eta"]
        Q4Eta = 0
        FTPFolder = '/dati/San_Teodoro'

    else:
        Q4Eta = 0
        t = data["DB"]["t"]
        Q = data["DB"]["Q"]
        P = data["DB"]["P"]
        Bar = data["DB"]["Bar"]

        eta = data["DB"]["eta"]

        FTPFolder = '/dati/San_Teodoro'

    if PlantTag != "CST":
        eta = np.divide(1000*P, rho * g * np.multiply(Q, Bar) * 10.1974)
        dataPeriodi = {"t": t, "Q": Q, "P": P, "Bar": Bar, "Q4Eta": Q4Eta, "eta": eta}
    else:
        dataPeriodi = {"t": t, "Q": Q, "PST": PST, "PPAR": PPAR, "P": P,"Bar": Bar, "Q4Eta": Q4Eta, "eta": eta}

    YearTL, YearStat = calcolaPeriodiHydro(dataPeriodi, "Annuale")
    YearTLFileName = PlantTag+"YearTL.csv"
    YearStatFileName = PlantTag+"YearStat.csv"
    YearTL.to_csv(YearTLFileName, index=False)
    YearStat.to_csv(YearStatFileName, index=False)

    # calcolo i dati mensili
    MonthTL, MonthStat = calcolaPeriodiHydro(dataPeriodi, "Mensile")
    MonthTLFileName = PlantTag+"MonthTL.csv"
    MonthStatFileName = PlantTag+"MonthStat.csv"
    MonthTL.to_csv(MonthTLFileName, index=False)
    MonthStat.to_csv(MonthStatFileName, index=False)

    # calcolo i dati giornalieri
    last24TL, last24Stat = calcolaPeriodiHydro(dataPeriodi, "24h")
    last24TLFileName = PlantTag+"last24hTL.csv"
    last24StatFileName = PlantTag+"last24hStat.csv"
    last24TL.to_csv(last24TLFileName, index=False)
    last24Stat.to_csv(last24StatFileName, index=False)

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd(FTPFolder)

    File = open(YearTLFileName, "rb")
    ftp.storbinary(f"STOR "+YearTLFileName, File)
    File = open(YearStatFileName, "rb")
    ftp.storbinary(f"STOR "+YearStatFileName, File)

    File = open(MonthTLFileName, "rb")
    ftp.storbinary(f"STOR "+MonthTLFileName, File)
    File = open(MonthStatFileName, "rb")
    ftp.storbinary(f"STOR "+MonthStatFileName, File)

    File = open(last24TLFileName, "rb")
    ftp.storbinary(f"STOR "+last24TLFileName, File)
    File = open(last24StatFileName, "rb")
    ftp.storbinary(f"STOR "+last24StatFileName, File)

    ftp.close()


def checkSTProduction(PlantData):

    DB = PlantData["DB"]
    N = len(DB["timestamp"])

    DatiIst = {"t": DB["timestamp"][N-1], "Q": DB["Portata [l/s]"][N-1], "P": DB["Potenza [kW]"][N-1],
               "Pressure": DB["Pressione [bar]"][N-1]}

    try:
        STNewState = controllaCentraleTG(DatiIst, "ST", PlantData["Plant state"])

    except Exception as err:
        print(err)
        STNewState = "U"

    return STNewState


def mainST(PlantData, TGMode):

    # DatiMeteo = []
    data = ScaricaDatiST()
    PlantData["DB"] = data
    NewState = checkSTProduction(PlantData)

    sendTelegram(PlantData["Plant state"], NewState, TGMode, "San Teodoro")
    PlantData["Plant state"] = NewState
    displayState(NewState)

    calcolaAggregatiHydro(data, "ST")

    return PlantData


def checkCSTCharge(PlantData):

    Now = datetime.now()

    t = PlantData["DB"]["t"]
    t = pd.to_datetime(t)
    Q = PlantData["DB"]["Q"]

    tStart = Now - timedelta(hours=12)
    lastQs = Q[t >= tStart]

    lastQCST = np.mean(lastQs)

    try:
        CSTNewState = controllaCST(lastQCST)

    except Exception as err:
        print(err)
        CSTNewState = "U"

    return CSTNewState


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

    tPARNew = PlantData["PAR"]["DB"]["tQ"]
    tPARNew = pd.to_datetime(tPARNew)
    tSTNew = PlantData["ST"]["DB"]["timestamp"]
    tSTNew = pd.to_datetime(tSTNew)
    QPARNew = PlantData["PAR"]["DB"]["Q"]
    QSTNew = PlantData["ST"]["DB"]["Portata [l/s]"]
    PPARNew = PlantData["PAR"]["DB"]["P"]
    PSTNew = PlantData["ST"]["DB"]["Potenza [kW]"]
    BARPARNew = PlantData["PAR"]["DB"]["P"]
    BARSTNew = PlantData["ST"]["DB"]["Potenza [kW]"]
    etaPARNew = np.divide(1000 * PPARNew, rho * g * np.multiply(QPARNew, BARPARNew) * 10.1974)
    etaSTNew = np.divide(1000 * PSTNew, rho * g * np.multiply(QSTNew, BARSTNew) * 10.1974)

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
        BARS = np.mean([BARMeanPAR,BARMeanST])
        Bar = pd.concat([Bar, pd.Series(BARS)], ignore_index=True, axis=0)

        etaMeanPAR = np.mean(etaPARNew[(tPARNew >= tCurr) & (tPARNew < tCurr + timedelta(hours=1))])
        etaMeanST = np.mean(etaSTNew[(tSTNew >= tCurr) & (tSTNew < tCurr + timedelta(hours=1))])
        etaPAR = pd.concat([etaPAR, pd.Series(etaMeanPAR)], ignore_index=True, axis=0)
        etaST = pd.concat([etaST, pd.Series(etaMeanST)], ignore_index=True, axis=0)
        etaSTOld = etaST
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


def mainCST(PlantData, TGMode):

    PlantData["DB"] = ScaricaDatiCST(PlantData)
    NewState = checkCSTCharge(PlantData)
    sendTelegram(PlantData["Plant state"], NewState, TGMode, "San Teodoro")
    PlantData["Plant state"] = NewState

    displayState(NewState)

    calcolaAggregatiHydro(PlantData, "CST")

    return PlantData


def checkPGProduction(PlantData):

    DB = PlantData["DB"]
    N = len(DB["x__TimeStamp"])

    t = DB["Local"][N-1]
    t = datetime.strptime(t, "%d/%m/%Y %H:%M:%S")

    DatiIst = {"t": t, "Q": DB["PLC1_AI_FT_PORT_IST"][N-1], "P": DB["PLC1_AI_POT_ATTIVA"][N-1],
               "Pressure": DB["PLC1_AI_PT_TURBINA"][N-1]}

    try:
        NewState = controllaCentraleTG(DatiIst, "PG", PlantData["Plant state"])

    except Exception as err:
        print(err)
        NewState = "U"

    return NewState


def mainPG(PlantData, TGMode):

    print("- Aggiornamento del database...")
    sistemaCartellaFTPPG()
    print("-- Database aggiornato!")

    print("- Download dei dati...")
    data = ScaricaDatiPG()
    PlantData["DB"] = data

    print("- Controllo del funzionamento della centrale")
    NewState = checkPGProduction(PlantData)
    sendTelegram(PlantData["Plant state"], NewState, TGMode, "Ponte Giurino")
    displayState(NewState)

    calcolaAggregatiHydro(data, "PG")

    return PlantData






def mainRUB(PlantData, TGMode):

    token = authenticateHigeco("Rubino")

    # controllo se l'impianto funziona e in caso mando l'allarme
    NewState = checkRUBProduction(token, PlantData)

    # mando gli allarmi se serve
    sendTelegram(PlantData["Plant state"], NewState, TGMode, "Rubino")

    PlantData["Plant state"] = NewState
    displayState(NewState)

    data = scaricaDatiRUB(token)
    data["PN"] = 997.44
    data["Tariffa"] = 0.315

    calcolaAggregatiPV(data, "RUB")


    return PlantData


def checkSA3Production(PlantData):

    if PlantData["Error"] == "Empty file":
        STNewState = "W"

    else:
        DB = PlantData["DB"]
        N = len(DB["Time"])
        DatiIst = {"t": DB["Time"][N-1], "Q": DB["Charge"][N-1], "P": DB["Power"][N-1], "Pressure": DB["Jump"][N-1]}

        try:
            STNewState = controllaCentraleTG(DatiIst, "ST", PlantData["Plant state"])

        except Exception as err:
            print(err)
            STNewState = "U"

    return STNewState


def mainSA3(PlantData, TGMode):

    try:
        data = ScaricaDatiSA3New()
        PlantData["DB"] = data
        PlantData["Error"] = "None"

    except Exception as err:
        print(err)
        data = []
        PlantData["Error"] = "Empty file"
        if str(err) != "No columns to parse from file":
            token = "6007635672:AAF_kA2nV4mrscssVRHW0Fgzsx0DjeZQIHU"
            bot = telebot.TeleBot(token)
            TestId = "-672088289"
            Text = "MONITORAGGIO STRAORDINARIO SA3:" + str(err)
            bot.send_message(TestId, text=Text)

    NewState = checkSA3Production(PlantData)

    sendTelegram(PlantData["Plant state"], NewState, TGMode, "SA3")
    PlantData["Plant state"] = NewState
    displayState(NewState)

    if data != []:
        calcolaAggregatiHydro(data, "SA3")

    return PlantData
