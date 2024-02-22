import pandas as pd
import numpy as np
from ftplib import FTP
from calcolaPeriodi import calcolaPeriodiPV, calcolaPeriodiHydro


def calcolaAggregatiHydro(Plant, data):

    g = 9.81
    rho = 1000

    PST = []
    PPAR = []
    eta = []

    if Plant == "ST" or Plant == "PAR":

        t = data["timestamp"]
        t = pd.to_datetime(t)
        Q = data["Portata [l/s]"] / 1000
        P = data["Potenza [kW]"]
        Bar = data["Pressione [bar]"]
        Q4Eta = 10
        if Plant == "PAR":
            Q4Eta = 5
        Q4Eta = Q4Eta / 1000
        FTPFolder = '/dati/San_Teodoro'

    elif Plant == "PG":

        t = data["Local"]
        t = pd.to_datetime(t)
        Q = data["PLC1_AI_FT_PORT_IST"] / 1000
        P = data["PLC1_AI_POT_ATTIVA"]
        Bar = data["PLC1_AI_PT_TURBINA"]
        Q4Eta = 10
        Q4Eta = Q4Eta / 1000
        FTPFolder = '/dati/ponte_giurino'

    elif Plant == "TF":

        t = data["Time"]
        t = pd.to_datetime(t)
        Q = data["Charge"]
        P = data["Power"]
        Bar = data["Jump"] / 10.1974
        Q4Eta = 0.6
        Q4Eta = Q4Eta
        FTPFolder = '/dati/Torrino_Foresta'

    elif Plant == "SA3":

        t = data["t"]
        t = pd.to_datetime(t)
        Q = data["Q"]
        P = data["P"]
        Bar = data["Bar"] / 10.1974
        Q4Eta = 0.6
        Q4Eta = Q4Eta
        FTPFolder = '/dati/SA3'

    elif Plant == "CST":

        t = data["t"]
        Q = data["Q"] / 1000
        PPAR = data["PPAR"]
        PST = data["PST"]
        P = data["P"]
        Bar = data["Bar"]
        eta = data["eta"]
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

    if Plant != "CST":
        eta = np.divide(1000*P, rho * g * np.multiply(Q, Bar) * 10.1974)
        dataPeriodi = {"t": t, "Q": Q, "P": P, "Bar": Bar, "Q4Eta": Q4Eta, "eta": eta}
    else:
        dataPeriodi = {"t": t, "Q": Q, "PST": PST, "PPAR": PPAR, "P": P, "Bar": Bar, "Q4Eta": Q4Eta, "eta": eta}

    YearTL, YearStat = calcolaPeriodiHydro(Plant, dataPeriodi, "Annuale")
    YearTLFileName = Plant+"YearTL.csv"
    YearStatFileName = Plant+"YearStat.csv"
    YearTL.to_csv(YearTLFileName, index=False)
    YearStat.to_csv(YearStatFileName, index=False)

    # calcolo i dati mensili
    MonthTL, MonthStat = calcolaPeriodiHydro(Plant, dataPeriodi, "Mensile")
    MonthTLFileName = Plant+"MonthTL.csv"
    MonthStatFileName = Plant+"MonthStat.csv"
    MonthTL.to_csv(MonthTLFileName, index=False)
    MonthStat.to_csv(MonthStatFileName, index=False)

    # calcolo i dati giornalieri
    last24TL, last24Stat = calcolaPeriodiHydro(Plant, dataPeriodi, "24h")
    last24TLFileName = Plant+"last24hTL.csv"
    last24StatFileName = Plant+"last24hStat.csv"
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


def calcolaAggregatiPV(Plant, data):

    t = data["t"]
    t = pd.to_datetime(t)
    I = data["I"]
    TMod = data["TMod"]
    P1 = []
    P2 = []

    if Plant == "SCN":

        P1 = data["P1"]
        P2 = data["P2"]

        P = P1 + P2
        PN = 926.64

        FTPFolder = '/dati/SCN'

    else:

        P = data["P"]

        PN = 997.44

        FTPFolder = '/dati/Rubino'

    eta = np.divide(1000 * P, I) / PN
    if Plant == "SCN":
        dataPeriodi = {"t": t, "I": I, "P1": P1, "P2": P2, "TMod": TMod, "eta": eta, "P": P,
                       "Tariffa": data["Tariffa"], "PN": data["PN"]}
    else:
        dataPeriodi = {"t": t, "I": I, "TMod": TMod, "eta": eta, "P": P, "Tariffa": data["Tariffa"],
                       "PN": data["PN"]}

    YearTL, YearStat = calcolaPeriodiPV(dataPeriodi, "Annuale", Plant)
    YearTLFileName = Plant + "YearTL.csv"
    YearStatFileName = Plant + "YearStat.csv"
    YearTL.to_csv(YearTLFileName, index=False)
    YearStat.to_csv(YearStatFileName, index=False)

    # calcolo i dati mensili
    MonthTL, MonthStat = calcolaPeriodiPV(dataPeriodi, "Mensile", Plant)
    MonthTLFileName = Plant + "MonthTL.csv"
    MonthStatFileName = Plant + "MonthStat.csv"
    MonthTL.to_csv(MonthTLFileName, index=False)
    MonthStat.to_csv(MonthStatFileName, index=False)

    # calcolo i dati giornalieri
    last24TL, last24Stat = calcolaPeriodiPV(dataPeriodi, "24h", Plant)
    last24TLFileName = Plant + "last24hTL.csv"
    last24StatFileName = Plant + "last24hStat.csv"
    last24TL.to_csv(last24TLFileName, index=False)
    last24Stat.to_csv(last24StatFileName, index=False)

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd(FTPFolder)

    File = open(YearTLFileName, "rb")
    ftp.storbinary(f"STOR " + YearTLFileName, File)
    File = open(YearStatFileName, "rb")
    ftp.storbinary(f"STOR " + YearStatFileName, File)

    File = open(MonthTLFileName, "rb")
    ftp.storbinary(f"STOR " + MonthTLFileName, File)
    File = open(MonthStatFileName, "rb")
    ftp.storbinary(f"STOR " + MonthStatFileName, File)

    File = open(last24TLFileName, "rb")
    ftp.storbinary(f"STOR " + last24TLFileName, File)
    File = open(last24StatFileName, "rb")
    ftp.storbinary(f"STOR " + last24StatFileName, File)

    ftp.close()


def calcolaAggregati(Plant, data):

    if Plant == "SCN" or Plant == "RUB":
        calcolaAggregatiPV(Plant, data)

    else:
        calcolaAggregatiHydro(Plant, data)
