import pandas as pd
import numpy as np
from ftplib import FTP
from calcolaPeriodi import calcolaPeriodiPV, calcolaPeriodiHydro
import csv
import json

def ExpectedEta(lastVar2, Plant, lastVar3):

    if Plant == "SA3":

        FileName = "rendimentoReale" + Plant + ".csv"
        CurvaRendimento = pd.read_csv(FileName)

        QTeo = CurvaRendimento["QOut"]
        if Plant != "TF" and Plant != "SA3":
            QTeo = QTeo / 1000

        etaTeo = CurvaRendimento["etaOut"]
        etaDev = CurvaRendimento["devEta"]

        # cerco i valori più vicini last Q

        i = 0
        while lastVar2 > QTeo.iloc[i]:
            i = i + 1

        if i != 0:
            etaAspettato = (etaTeo[i - 1] + etaTeo[i]) / 2
            devAspettato = 0.5 * np.sqrt(etaDev[i - 1] ** 22 + etaDev[i] ** 2)
        else:
            etaAspettato = (etaTeo[i] + etaTeo[i]) / 2
            devAspettato = 0.5 * np.sqrt(etaDev[i] ** 2 + etaDev[i] ** 2)

        etaMin = etaAspettato - devAspettato
        etaMax = etaAspettato + devAspettato

    else:

        if Plant != "TF" and Plant != "SA3" and Plant != "SCN":
            lastVar2 = lastVar2 * 1000

        MeanFile = "MeanEta" + Plant + ".csv"
        DevFile = "DevEta" + Plant + ".csv"

        CurvaRendimento = pd.read_csv(MeanFile, header=None)
        devRendimento = pd.read_csv(DevFile, header=None)

        AsseVar2 = CurvaRendimento.iloc[0, 1:]
        AsseVar3 = CurvaRendimento.iloc[1:, 0]

        # cerco i valori più vicini last Q

        i = 0
        Var2Test = AsseVar2.iloc[i]
        # print(len(AsseVar2))

        while lastVar2 > Var2Test and i < len(AsseVar2) - 1:
            # print(str(i))
            i = i + 1
            Var2Test = AsseVar2.iloc[i]

        j = 0
        Var3Test = AsseVar3.iloc[j]

        if np.isnan(lastVar3):
            lastVar3 = 0
        FinalJ = 1
        while lastVar3 > Var3Test and j < len(AsseVar3):
            j = j + 1
            if j >= len(AsseVar3):
                FinalJ = j - 1
                Var3Test = AsseVar3.iloc[FinalJ]

            else:
                FinalJ = j
                Var3Test = AsseVar3.iloc[FinalJ]

        etaAspettato = CurvaRendimento.iloc[FinalJ, i]
        if np.isnan(etaAspettato):
            etaAspettato = np.mean([CurvaRendimento.iloc[FinalJ - 1, i], CurvaRendimento.iloc[FinalJ + 1, i],
                                    CurvaRendimento.iloc[FinalJ, i - 1], CurvaRendimento.iloc[FinalJ, i + 1]])

        devAspettato = devRendimento.iloc[FinalJ, i]

        etaMin = etaAspettato - devAspettato
        etaMax = etaAspettato + devAspettato

    return etaAspettato, etaMin, etaMax


def salvaUltimoTimeStamp(Data, Plant, DatiImpianti):

    last_t = Data["t"].iloc[-1]
    lastP = Data["P"].iloc[-1]
    lastVar2 = Data["Q"].iloc[-1]
    lastVar3 = Data["Bar"].iloc[-1]

    rho = 1000
    g = 9.81
    lastEta = lastP / (rho * g *lastVar2 * lastVar3 )

    if Plant == "SA3":
        etaExpected, etaMinExpected, etaMaxExpected = 0, 0, 0
        etaDev = etaExpected - etaMinExpected

    elif Plant == "CST":
        etaAspettatoST, etaMinusST, etaPlusST = ExpectedEta(Data["QST"].iloc[-1], "ST", lastVar3)
        etaAspettatoPAR, etaMinusPAR, etaPlusPAR = ExpectedEta(Data["QPAR"].iloc[-1], "PAR", lastVar3)
        etaExpected = (70*etaAspettatoST + 25*etaAspettatoPAR)/95
        devEtaST = etaAspettatoST - etaMinusST
        devEtaPAR = etaAspettatoPAR - etaMinusPAR
        etaDev = np.sqrt((70*devEtaST)**2 + (25*devEtaPAR)**2)/95
        etaMinExpected = etaExpected - etaDev
        etaMaxExpected = etaExpected + etaDev

    else:
        etaExpected, etaMinExpected, etaMaxExpected = ExpectedEta(lastVar2, Plant, lastVar3)
        etaDev = etaExpected - etaMinExpected

    PMinus = etaMinExpected * rho * g * lastVar2 * lastVar3 * 10.1974 / 1000
    PPlus = etaMaxExpected * rho * g * lastVar2 * lastVar3 * 10.1974 / 1000

    PExpected = etaExpected * rho * g * lastVar2 * lastVar3 * 10.1974 / 1000
    PDev = etaDev * rho * g * lastVar2 * lastVar3 * 10.1974 / 1000

    DatiImpianto = DatiImpianti[DatiImpianti["Tag"] == Plant]
    DatiImpianto = DatiImpianto.reset_index()

    PN = DatiImpianto["potenza_installata_kWp"][0]
    Var2Max = DatiImpianto["Var2_max"][0]
    Var2Media = DatiImpianto["Var2_media"][0]
    Var2Dev = DatiImpianto["Var2_dev"][0]

    Var3Max = DatiImpianto["Var3_max"][0]
    Var3Media = DatiImpianto["Var3_media"][0]
    Var3Dev = DatiImpianto["Var3_dev"][0]

    DatiGauge = {
        "Power": {"last_value": lastP, "MaxScala": PN, "Media": PExpected, "Dev": PDev},
         "Var2": {"last_value": lastVar2, "MaxScala": Var2Max, "Media": Var2Media, "Dev": Var2Dev},
         "Var3": {"last_value": lastVar3, "MaxScala": Var3Max, "Media": Var3Media, "Dev": Var3Dev},
         "Eta": {"last_value": lastEta, "MaxScala": 100, "Media": etaExpected, "Dev": etaDev}
    }

    # pd.DataFrame(DatiGauge).to_csv("dati gauge.csv", index=False)

    lastTSFileName = "dati gauge.csv"
    pd.DataFrame.from_dict(DatiGauge).to_csv(lastTSFileName)
    # with open("dati gauge.json", "w") as outfile:
    #     json.dump(DatiGauge, outfile)

    # with open(lastTSFileName, 'w') as csvfile:
    #     writer = csv.DictWriter(csvfile, DatiGauge.keys())
    #     writer.writeheader()
    #     writer.writerow(DatiGauge)

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')

    ftp.cwd('/dati/'+DatiImpianto["folder"][0])

    fileName = "dati gauge.csv"
    File = open(fileName, "rb")
    ftp.storbinary(f"STOR " + fileName, File)
    ftp.close()


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
        t = pd.to_datetime(t, format='mixed')
        Q = data["Q"]
        P = data["P"]
        Bar = data["Bar"] / 10.1974
        Q4Eta = 0.6
        Q4Eta = Q4Eta
        FTPFolder = '/dati/SA3'

    elif Plant == "CST":

        t = data["t"]
        Q = data["Q"] / 1000
        QPAR = data["QPAR"]
        QST = data["QST"]
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
        dataPeriodi = {"t": t, "QST": QST, "QPAR": QPAR, "Q": Q, "PST": PST, "PPAR": PPAR, "P": P, "Bar": Bar, "Q4Eta": Q4Eta, "eta": eta}

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

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')

    ftp.cwd('/dati/Database_Produzione')

    gFile = open("lista_impianti.xlsx", "wb")
    ftp.retrbinary('RETR lista_impianti.xlsx', gFile.write)
    gFile.close()

    DatiImpianti = pd.read_excel("lista_impianti.xlsx")

    salvaUltimoTimeStamp(dataPeriodi, Plant, DatiImpianti)


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

    ftp.cwd('/dati/Database_Produzione')

    gFile = open("lista_impianti.xlsx", "wb")
    ftp.retrbinary('RETR lista_impianti.xlsx', gFile.write)
    gFile.close()

    DatiImpianti = pd.read_excel("lista_impianti.xlsx")

    # calcolo i dati istantanei
    lastTS = salvaUltimoTimeStamp(dataPeriodi, Plant, DatiImpianti)
    lastTSFileName = Plant + "lastTimeStamp.csv"
    lastTS.to_csv(lastTSFileName, index=False)

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

    File = open(lastTSFileName, "rb")
    ftp.storbinary(f"STOR " + lastTSFileName, File)

    ftp.close()


def calcolaAggregati(Plant, data):

    if Plant == "SCN" or Plant == "RUB":
        calcolaAggregatiPV(Plant, data)

    else:
        calcolaAggregatiHydro(Plant, data)
