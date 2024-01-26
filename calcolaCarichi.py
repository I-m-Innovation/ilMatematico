import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from statistics import mean
from ftplib import FTP


def calcolaCarico(data, Plant, lastTime):
    np.seterr(divide='ignore')
    indicator = 0

    if Plant == "TF" or Plant == "Partitore" or Plant == "ST" or Plant == "PG" or Plant == "SA3":
        t = np.array(data["t"])
        Var2 = np.array(data["Q"])
        h = np.array(data["h"])
        hOut = []
        indicator = 1
        P = np.array(data["P"])

    elif Plant == "Cavarzan":
        t = np.array(data["t"])
        Var2 = np.array(data["G"])
        P = np.array(data["P"])

    elif Plant == "DI" or Plant == "ZG":
        t = np.array(data["t"])
        Var2 = np.array(data["Val"])
        P = np.array(data["Val"])

    else:
        t = np.array(data["tP"])
        Var2 = np.array(data["G"])
        P = np.array(data["P"])

    t = pd.to_datetime(t)

    firstTime = lastTime - timedelta(days=1)

    currTime = firstTime
    nextTime = firstTime + timedelta(hours=1)

    Var2Out = []
    EOut = []
    tOut = []

    while currTime < lastTime:
        
        PInvolved = P[(t >= currTime) & (t <= nextTime)]
        Var2Involved = Var2[(t >= currTime) & (t <= nextTime)]

        try:
            EOut.append(mean(PInvolved))
        except Exception as err:
            print(err)
            EOut.append(float('nan'))

        tOut.append(currTime + (nextTime - currTime) / 2)
        try:
            Var2Out.append(mean(Var2Involved))
        except Exception as err:
            print(err)
            Var2Out.append(float('nan'))
            
        if indicator == 1:
            hInvolved = h[(t >= currTime) & (t <= nextTime)]
            try:
                hOut.append(mean(hInvolved))
            except Exception as err:
                print(err)
                hOut.append(float('nan'))

        currTime = nextTime
        nextTime = currTime + timedelta(hours=1)

    if indicator == 1:
        Carico = {"t": tOut, "E": EOut, "Var2": Var2Out, "h":hOut}
    else:
        Carico = {"t": tOut, "E": EOut, "Var2": Var2Out}

    return Carico


def calcolaCaricoTotale(DatiRubino, TF, Partitore, ST, PG, DatiSCN, SA3, DI, ZG):

    try:
        CaricoDI = DI["E"]
        lastIDI = float('nan')
        lastPDI = DI["Var2"]
    except Exception as err:
        print(err)
        CaricoDI = []
        lastIDI = []
        lastPDI = np.ones(24)*float('nan')
    
    CaricoZG = ZG["E"]
    lastIZG = float('nan')
    lastPZG = ZG["Var2"]

    CaricoRubino = DatiRubino["Carico"]
    lastIRubino = DatiRubino["lastI"]
    lastPRubino = DatiRubino["lastP"]
    try:
        CaricoSCN = DatiSCN["Carico"]
        lastPSCN = DatiSCN["lastP"]
        lastISCN = DatiSCN["lastI"]
        ESCN = np.array(CaricoSCN["E"])

    except Exception as err:
        print(err)
        ESCN = 0
        lastPSCN = 0
        lastISCN = 0

    lastPPV = lastPSCN + lastPRubino + lastPDI +lastPZG
    lastIPV = lastIRubino + lastISCN

    PRPV = lastPPV / lastIPV / (997.44 + 926.64) * 1000 * 100

    t = CaricoRubino["t"]
    ERubino = np.array(CaricoRubino["E"])
    ERubino[np.isnan(ERubino) ==1 ]=0

    EDI = np.array(CaricoDI)
    EZG = np.array(CaricoZG)

    ETF = np.array(TF["E"])
    EPartitore = np.array(Partitore["E"])

    try:
        ESA3 = np.array(SA3["E"])
    except Exception as err:
        ESA3 = 0

    QPartitore = np.array(Partitore["Var2"])

    EST = np.array(ST["E"])
    QST = np.array(ST["Var2"])
    EPG = np.array((PG["E"]))

    if len(EDI) == 0:
        EDI = np.ones(24)*float('nan')

    EPVArray = np.array([ERubino, ESCN, EDI, EZG])

    EPV = np.nansum(EPVArray, axis=0)

    ERoofArray = np.array([EDI, EZG])
    ERoof = np.nansum(ERoofArray, axis=0)

    EH2O = ETF + EPartitore + EST + EPG + ESA3
    ECST = EST + EPartitore
    QCST = QST + QPartitore
    ETot = EPV + EH2O

    MPPV = 0

    hST = ST["h"]
    hPartitore = Partitore["h"]
    hCST = np.mean([np.array(hST), np.array(hPartitore)], axis=0)

    Carichi = {"t": t, "EPV": EPV, "PPV": lastPPV, "IPV": lastIPV, "PRPV": PRPV, "H2O": EH2O, "CST": ECST, "Tot": ETot,
               "Partitore": EPartitore, "ST": EST, "PG": EPG,
               "TF": ETF, "SCN": ESCN, "Rubino": ERubino, "MPPV": MPPV, "QCST": QCST, "hCST":hCST, "SA3": ESA3,
               "DI": EDI, "ZG": EZG}

    myDict = pd.DataFrame.from_dict(Carichi)
    myDict.to_csv("Carichi.csv")

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/Database_Produzione')
    File = open("Carichi.csv", "rb")
    ftp.storbinary(f"STOR Carichi.csv", File)
    ftp.close()

