from alertSystem import controllaFotovoltaico, controllaCentraleTG, sendTelegram
from colorama import Fore, Style
from quest2HigecoDefs import call2lastValue, authenticateHigeco
from downloadSTFromFTP_000 import ScaricaDatiST, ScaricaDatiPartitore
from downloadSA3FromFTP_000 import ScaricaDatiSA3
from downloadPGFromFTP_000 import ScaricaDatiPG, sistemaCartellaFTPPG
from downloadTFFromFTP import ScaricaDatiTF, sistemaCartellaFTP_TF
from datetime import datetime
import telebot


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

    return PlantData


def checkSTProduction(PlantData):

    DB = PlantData["DB"]
    N = len(DB["Time"])
    DatiIst = {"t": DB["Time"][N-1], "Q": DB["Charge"][N-1], "P": DB["Power"][N-1], "Pressure": DB["Jump"][N-1]}

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

    return PlantData


def checkPGProduction(PlantData):

    DB = PlantData["DB"]
    N = len(DB["Time"])
    DatiIst = {"t": DB["Time"][N-1], "Q": DB["Charge"][N-1], "P": DB["Power"][N-1], "Pressure": DB["Jump"][N-1]}

    try:
        NewState = controllaCentraleTG(DatiIst, "PG", PlantData["Plant state"])

    except Exception as err:
        print(err)
        NewState = "U"

    return NewState


def mainPG(PlantData, TGMode):

    print("- Aggiornamento del database...")
    sistemaCartellaFTPPG("PG")
    print("-- Database aggiornato!")

    print("- Download dei dati...")
    data = ScaricaDatiPG()
    PlantData["DB"] = data

    NewState = checkPGProduction(PlantData)
    sendTelegram(PlantData["Plant state"], NewState, TGMode, "Ponte Giurino")
    displayState(NewState)

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

    sendTelegram(PlantData["Plant state"], NewState, TGMode, "San Teodoro")
    PlantData["Plant state"] = NewState
    displayState(NewState)

    return PlantData
