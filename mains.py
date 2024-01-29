import numpy as np
import pandas as pd
from alertSystem import controllaFotovoltaico, controllaCentraleTG, sendTelegram
from colorama import Fore, Style
from quest2HigecoDefs import call2lastValue, authenticateHigeco
from downloadSTFromFTP_000 import ScaricaDatiST, ScaricaDatiPartitore
from downloadSA3FromFTP_000 import ScaricaDatiSA3
from downloadPGFromFTP_000 import ScaricaDatiPG, sistemaCartellaFTPPG
from downloadTFFromFTP import ScaricaDatiTF, sistemaCartellaFTP_TF
from datetime import datetime, timedelta
import telebot
from ftplib import FTP


def displayState(State):

    if State == "O":
        print(f'-- {Fore.GREEN}In produzione!{Style.RESET_ALL}')

    elif State == "W":
        print(f'-- {Fore.YELLOW}In no link!{Style.RESET_ALL}')

    elif State == "U":
        print(f'-- {Fore.YELLOW}Stato centrale non riconosciuto!{Style.RESET_ALL}')

    else:
        print(f'-- {Fore.RED}ANOMALIE CENTRALE RILEVATA!{Style.RESET_ALL}')


def checkSCNProduction(token, PlantData):

    # leggo gli ultimi dati da Higeco
    Data1 = call2lastValue(token, "SCN1")
    Data2 = call2lastValue(token, "SCN2")
    lastI = Data2["lastI"]

    TMY = PlantData["TMY"]

    if lastI == "#E2":
        tCfr = Data1['lastT']
        tCfr = datetime(2020, tCfr.month, tCfr.day, tCfr.hour, tCfr.minute)
        ICfr = TMY["Imean"]
        lastI = ICfr[TMY["date"] == tCfr]
        lastI = float(lastI)

    try:
        SCN1NewState = controllaFotovoltaico(Data1, "SCN1", PlantData["Plant state"]["SCN1"], lastI)
    except Exception as err:
        print(err)
        SCN1NewState = "U"

    try:
        SCN2NewState = controllaFotovoltaico(Data2, "SCN1", PlantData["Plant state"]["SCN2"], lastI)
    except Exception as err:
        print(err)
        SCN2NewState = "U"

    NewState = {"SCN1": SCN1NewState, "SCN2": SCN2NewState}

    return NewState


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
    print("-- Database aggiornato!")

    PlantData["DB"] = data

    NewState = checkTFProduction(PlantData)
    sendTelegram(PlantData["Plant state"], NewState, TGMode, "Torrino Foresta")
    PlantData["Plant state"] = NewState
    displayState(NewState)

    return PlantData


def checkPARProduction(PlantData):

    DB = PlantData["DB"]
    N = len(DB["Time"])
    DatiIst = {"t": DB["Time"][N-1], "Q": DB["Charge"][N-1], "P": DB["Power"][N-1], "Pressure": DB["Jump"][N-1]}

    try:
        NewState = controllaCentraleTG(DatiIst, "PAR", PlantData["Plant state"])

    except Exception as err:
        print(err)
        NewState = "U"

    return NewState


def mainPartitore(PlantData, TGMode):

    print("- Download dei dati...")
    data = ScaricaDatiPartitore()
    PlantData["DB"] = data
    NewState = checkPARProduction(PlantData)
    sendTelegram(PlantData["Plant state"], NewState, TGMode, "Partitore")
    PlantData["Plant state"] = NewState
    displayState(NewState)

    calcolaAggregatiHydro(data, "PAR")

    return PlantData


def calcolaPeriodi(data, Period):

    Now = datetime.now()

    t = data["t"]
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

    TLDict = {"t": tSel, "Q": QSel, "P": PSel, "Bar": BarSel, "Eta": etaSel}
    TLdf = pd.DataFrame.from_dict(TLDict)

    StatDict = {"QMean": [QMean], "QDev": [QDev], "PMean": [PMean], "PDev": [PDev], "BarMean": [BarMean],
                "BarDev": [BarDev], "etaMean": [etaMean], "etaDev": [etaDev]}
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

    else:

        t = []
        Q = []
        P = []
        Bar = []
        Q4Eta = []
        FTPFolder = ''

    eta = np.divide(1000*P, rho * g * np.multiply(Q, Bar) * 10.1974)
    dataPeriodi = {"t": t, "Q": Q, "P": P, "Bar": Bar, "Q4Eta": Q4Eta, "eta": eta}

    YearTL, YearStat = calcolaPeriodi(dataPeriodi, "Annuale")
    YearTLFileName = PlantTag+"YearTL.csv"
    YearStatFileName = PlantTag+"YearStat.csv"
    YearTL.to_csv(YearTLFileName, index=False)
    YearStat.to_csv(YearStatFileName, index=False)

    # calcolo i dati mensili
    MonthTL, MonthStat = calcolaPeriodi(dataPeriodi, "Mensile")
    MonthTLFileName = PlantTag+"YearTL.csv"
    MonthStatFileName = PlantTag+"YearStat.csv"
    MonthTL.to_csv(MonthTLFileName, index=False)
    MonthStat.to_csv(MonthStatFileName, index=False)

    # calcolo i dati giornalieri
    last24TL, last24Stat = calcolaPeriodi(dataPeriodi, "24h")
    last24TLFileName = PlantTag+"YearTL.csv"
    last24StatFileName = PlantTag+"YearStat.csv"
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


def checkRUBProduction(token, PlantData):
    # leggo gli ultimi dati da Higeco
    Data = call2lastValue(token, "RUB")
    lastI = Data["lastI"]

    TMY = PlantData["TMY"]

    if lastI == "#E2":
        tCfr = Data['lastT']
        tCfr = datetime(2020, tCfr.month, tCfr.day, tCfr.hour, tCfr.minute)
        ICfr = TMY["Imean"]
        lastI = ICfr[TMY["date"] == tCfr]

        if len(lastI) == 0:
            lastI = "Not found"
        else:
            lastI = float(lastI)

    print("- Controllo dello stato della centrale...")
    try:
        NewState = controllaFotovoltaico(Data, "RUB", PlantData["Plant state"], lastI)
    except Exception as err:
        print(err)
        NewState = "U"

    return NewState
    # esco dalla funzione lo stato dell'impianto


def mainRUB(PlantData, TGMode):

    token = authenticateHigeco("Rubino")

    # controllo se l'impianto funziona e in caso mando l'allarme
    NewState = checkRUBProduction(token, PlantData)

    # mando gli allarmi se serve
    sendTelegram(PlantData["Plant state"], NewState, TGMode, "Rubino")

    PlantData["Plant state"] = NewState
    displayState(NewState)

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
        data = ScaricaDatiSA3()
        PlantData["DB"] = data
        PlantData["Error"] = "None"

    except Exception as err:
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

    return PlantData
