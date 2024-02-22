from quest2HigecoDefs import call2lastValue
from datetime import datetime, timedelta
from alertSystem import controllaFotovoltaico, controllaCentraleTG, controllaCST
import pandas as pd
import numpy as np


def checkSA3Production(PlantData):

    DB = PlantData["DB"]
    N = len(DB["t"])
    DatiIst = {"t": DB["t"][N-1], "Q": DB["Q"][N-1], "P": DB["P"][N-1], "Pressure": DB["Bar"][N-1]}

    try:
        NewState = controllaCentraleTG(DatiIst, "SA3", PlantData["Plant state"])

    except Exception as err:
        print(err)
        NewState = "U"

    return NewState


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


def checkSCNProduction(token, Data):

    # leggo gli ultimi dati da Higeco
    Data1 = call2lastValue(token, "SCN1")
    Data2 = call2lastValue(token, "SCN2")
    lastI = Data2["lastI"]

    TMY = Data["TMY"]

    if lastI == "#E2":
        tCfr = Data1['lastT']
        tCfr = datetime(2020, tCfr.month, tCfr.day, tCfr.hour, tCfr.minute)
        ICfr = TMY["Imean"]
        lastI = ICfr[TMY["date"] == tCfr]
        lastI = float(lastI.iloc[0])

    try:
        SCN1NewState = controllaFotovoltaico(Data1, "SCN1", Data["Plant state"]["SCN1"], lastI)
    except Exception as err:
        print(err)
        SCN1NewState = "U"

    try:
        SCN2NewState = controllaFotovoltaico(Data2, "SCN1", Data["Plant state"]["SCN2"], lastI)
    except Exception as err:
        print(err)
        SCN2NewState = "U"

    NewState = {"SCN1": SCN1NewState, "SCN2": SCN2NewState}

    return NewState


def checkProduction(Plant, token, Data):

    if Plant == 'SCN':
        NewState = checkSCNProduction(token, Data)
    elif Plant == "RUB":
        NewState = checkRUBProduction(token, Data)
    elif Plant == "ST" or Plant == "PAR":
        NewState = checkSTProduction(Data)
    elif Plant == "CST":
        NewState = checkCSTCharge(Data)
    elif Plant == "PG":
        NewState = checkPGProduction(Data)
    elif Plant == "TF":
        NewState = checkTFProduction(Data)
    elif Plant == "SA3":
        NewState = checkSA3Production(Data)
    else:
        NewState = "U"

    return NewState
