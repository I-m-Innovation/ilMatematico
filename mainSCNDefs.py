import numpy as np
from datetime import datetime, timedelta, timezone
from statistics import mean
from ricostruisciTLDefs import calcolaTL
from quest2HigecoDefs import call2HigecoNew, authenticateHigeco
import pandas as pd
import pytz
from num2string_001 import convertNumber

def prevediProduzione(TMY_I, TMY_Target, DatiMensili, datiTabella, lastt):

    Pn = 926.34
    # etaInv = 0.981
    MedieSCN = pd.read_excel("MedieSCN.xlsx")
    ITMY = TMY_I["Imean"]
    t_TMY = np.array(TMY_I["date"], dtype=datetime)

    PTarget = TMY_Target["PTarget"]
    # tPTarget = np.array(TMY_Target["t"], dtype=datetime)
    # tPtarget = pd.to_datetime(tPTarget)

    dt = timedelta(minutes=10)
    firstt = lastt + dt
    # firsttTMY = datetime(2020, firstt.month, firstt.day, firstt.hour, firstt.minute)

    IMensiliFuture = np.zeros(12, dtype=float)
    TargetMensiliFuturi = np.zeros(12, dtype=float)

    currMonth = firstt.month

    tOn = firstt
    tOnTMY = datetime(2020, tOn.month, tOn.day, tOn.hour, tOn.minute)

    for i in range(currMonth, 12):
        tOff = datetime(tOn.year, tOn.month + 1, 1, 1, 0)
        tOffTMY = datetime(2020, tOff.month, tOff.day, tOff.hour, tOff.minute)

        dt = tOff - tOn
        dt = dt.days * 24 + dt.seconds / 3600

        IMensiliFuture[i - 1] = mean(ITMY[(t_TMY >= tOnTMY) & (t_TMY < tOffTMY)]) * dt / 1000
        TargetMensiliFuturi[i - 1] = mean(PTarget[(t_TMY >= tOnTMY) & (t_TMY < tOffTMY)]) * dt

        tOn = tOff

    tOnTMY = datetime(2020, tOn.month, tOn.day, tOn.hour, tOn.minute)
    tOff = datetime(tOn.year + 1, 1, 1, 0, 0, 0)
    # tOffTMY = datetime(2020, tOff.month, tOff.day, tOff.hour, tOff.minute)

    dt = tOff - tOn
    dt = dt.days * 24 + dt.seconds / 3600
    IAttuale = DatiMensili["Irraggiamenti"]

    IMensiliFuture[11] = mean(ITMY[t_TMY >= tOnTMY]) * dt / 1000
    TargetMensiliFuturi[11] = mean(PTarget[t_TMY >= tOnTMY]) * dt
    usiPropri = MedieSCN["UsiPropri"]
    TargetMensiliFuturi = np.multiply(TargetMensiliFuturi, 1 - usiPropri)
    TargetAttuale = DatiMensili["ETargetMensile"]

    TargetPrevista = TargetAttuale + TargetMensiliFuturi
    HeqTargetPrevista = sum(TargetPrevista) / Pn
    # TargetPrevista = sum(TargetMensiliFuturi)

    PRMedi = MedieSCN["PR"]

    ProdottaPRMedio = np.multiply(IAttuale, PRMedi) * Pn
    CedutaPRMedio = np.multiply(ProdottaPRMedio, 1 - usiPropri)
    ProdottaFutura = np.multiply(IMensiliFuture, PRMedi) * Pn
    CedutaFutura = np.multiply(ProdottaFutura, 1 - usiPropri)
    CedutaAttuale = DatiMensili["CedutaMensile"]
    CedutaPrevista = CedutaAttuale + CedutaFutura

    HeqCedutaPrevista = sum(CedutaPrevista) / Pn
    CedutaPRMedioPrevista = CedutaPRMedio + CedutaFutura
    HeqPRMedioPrevista = sum(CedutaPRMedioPrevista) / Pn
    # CedutaPrevista = sum(CedutaPrevista)

    PRPVGIS = MedieSCN["PRPVGIS"]
    IAttuale = DatiMensili["Irraggiamenti"]
    IPrevista = IAttuale + IMensiliFuture
    ProdottaPRPVGIS = np.multiply(IAttuale, PRPVGIS) * Pn
    CedutaPRPVGIS = np.multiply(ProdottaPRPVGIS, 1 - usiPropri)

    # IPrevista = sum(IPrevista)
    ProdottaPrevistaPVGIS = np.multiply(IPrevista, PRPVGIS) * Pn
    CedutaPrevistaPVGIS = np.multiply(ProdottaPrevistaPVGIS, 1 - usiPropri)
    HeqCedutaPrevistaPVGIS = sum(CedutaPrevistaPVGIS) / Pn
    # CedutaPrevistaPVGIS = sum(CedutaPrevistaPVGIS)

    DatiMensili["CedutaPRMedio"] = CedutaPRMedio
    DatiMensili["CedutaPRPVGIS"] = CedutaPRPVGIS

    datiPrevisionali = {"IPrevista": IPrevista, "TargetPrevista": TargetPrevista, "CedutaPrevista": CedutaPrevista,
                        "CedutaPrevistaPVGIS": CedutaPrevistaPVGIS, "CedutaPRMedioPrevista": CedutaPRMedioPrevista}

    datiTabella["HeqPrevista"] = HeqCedutaPrevista
    datiTabella["HeqTargetPrevista"] = HeqTargetPrevista
    datiTabella["HeqPRMedioPrevista"] = HeqPRMedioPrevista
    datiTabella["HeqPVGISPrevista"] = HeqCedutaPrevistaPVGIS

    return datiPrevisionali, DatiMensili, datiTabella


def calcolaAggregati(DataBase, lastt, TMY_I):
    # global tFineMese, i
    Pn = 926.34

    tInYear = datetime(lastt.year, 1, 1, 0, 0, 0)
    token = authenticateHigeco("SCN")

    EMHData = call2HigecoNew("SCN", "EMH", tInYear, token)
    EMHEn = EMHData["Val"]
    EMHEn = np.array(EMHEn, dtype=float)
    EMHt = EMHData["t"]
    LandisData = call2HigecoNew("SCN", "Landis", tInYear, token)
    LandisEn = LandisData["Val"]
    LandisEn = np.array(LandisEn, dtype=float)
    Landist = LandisData["t"]

    P1 = np.array(DataBase["P1"])
    lastP1 = P1[len(P1) - 1]

    P2 = np.array(DataBase["P2"])
    lastP2 = P2[len(P2) - 1]

    I = np.array(DataBase["I"])
    lastI = I[len(I) - 1]

    PR = np.divide(P1 + P2, I) / Pn * 1000 * 100

    lastP = lastP1 + lastP2
    lastPR = lastP / lastI * 1000 / Pn
    tI = DataBase["t"]
    tI = np.array(tI, dtype=datetime)
    tI = pd.to_datetime(tI)

    I_TMY = TMY_I["Imean"]
    lasttTMY = datetime(2020, lastt.month, lastt.day, lastt.hour, lastt.minute)

    t_TMY = np.array(TMY_I["date"], dtype=datetime)
    lastTMY = I_TMY[t_TMY == lasttTMY]
    PTarget = DataBase["PTarget"]
    lastPTarget = PTarget[len(PTarget) - 1]
    lastPRTarget = lastPTarget / lastI * 1000 / Pn

    # for i in range(len(EMHEn)-1):
    #     EMHEn[i] = 240 * EMHEn[i]

    # EMHTotalYear = EMHEn[len(EMHEn) - 1] - EMHEn[0]
    # LandisTotalYear = LandisEn[len(LandisEn) - 1] - LandisEn[0]

    currMonth = lastt.month
    # CedutaMensile = np.zeros(12, dtype=float)
    ProdottaMensile = np.zeros(12, dtype=float)
    IrradiataMensile = np.zeros(12, dtype=float)
    ETargetMensile = np.zeros(12, dtype=float)
    TMYMensile = np.zeros(12, dtype=float)
    tInizioAnno = datetime(lastt.year, 1, 1, 0, 0, 0)
    tFineMese = []

    for i in range(1, currMonth):

        tInizioMese = datetime(lastt.year, i , 1, 0, 0, 0)
        tFineMese = datetime(lastt.year, i + 1 , 1, 0, 0, 0)
        dt = tFineMese - tInizioMese
        dt = dt.days * 24 + dt.seconds / 3600

        EMHPeriodo = EMHEn[(EMHt >= tInizioMese) & (EMHt < tFineMese)]
        EMHPeriodo = EMHPeriodo[len(EMHPeriodo) - 1] - EMHPeriodo[0]

        LandisPeriodo = LandisEn[(Landist >= tInizioMese) & (Landist < tFineMese)]
        LandisPeriodo = LandisPeriodo[len(LandisPeriodo) - 1] - LandisPeriodo[0]

        ProdottaMensile[i - 1] = EMHPeriodo + LandisPeriodo
        IrradiataMensile[i-1] = mean(I[(tI >= tInizioMese) & (tI < tFineMese)]) * dt / 1000
        ETargetMensile[i-1] = mean(PTarget[(tI >= tInizioMese) & (tI < tFineMese)]) * dt

        tInizioMeseTMY = datetime(2020, tInizioMese.month, tInizioMese.day, tInizioMese.hour, tInizioMese.minute)
        tFineMeseTMY = datetime(2020, tFineMese.month, tFineMese.day, tFineMese.hour, tFineMese.minute)

        TMYMensile[i-1] = mean(I_TMY[(t_TMY >= tInizioMeseTMY) & (t_TMY < tFineMeseTMY)]) * dt / 1000


    tFineMese = lastt

    tInizioMese = datetime(tFineMese.year, tFineMese.month, 1, 1, 1)




    tInizioMeseTMY = datetime(2020, tInizioMese.month, tInizioMese.day, tInizioMese.hour, tInizioMese.minute)

    # tFineMeseTMY = datetime(2020, tFineMese.month, tFineMese.day, tFineMese.hour, tFineMese.minute)

    EMHPeriodo = EMHEn[EMHt >= tInizioMese]
    EMHPeriodo = EMHPeriodo[len(EMHPeriodo) - 1] - EMHPeriodo[0]

    LandisPeriodo = LandisEn[Landist >= tInizioMese]
    LandisPeriodo = LandisPeriodo[len(LandisPeriodo) - 1] - LandisPeriodo[0]

    ProdottaMensile[currMonth-1] = EMHPeriodo + LandisPeriodo

    df = pd.read_excel("MedieSCN.xlsx")
    usiPropri = df["UsiPropri"]
    PRPVGIS = df["PRPVGIS"]
    PRMedi = df["PR"]

    dt = tFineMese - tInizioMese
    dt = dt.days * 24 + dt.seconds / 3600
    IrradiataMensile[currMonth-1] = mean(I[tI >= tInizioMese]) * dt / 1000
    IrradiataTotale = sum(IrradiataMensile)

    CedutaMensile = np.multiply(ProdottaMensile, 1 - usiPropri)
    ProdottaMensilePRMedio = np.multiply(IrradiataMensile, PRMedi) * Pn
    ProdottaMensilePRPVGIS = np.multiply(IrradiataMensile, PRPVGIS) * Pn

    CedutaMensilePRPVGIS = np.multiply(ProdottaMensilePRPVGIS, 1 - usiPropri)
    CedutaMensilePRMedio = np.multiply(ProdottaMensilePRMedio, 1 - usiPropri)
    CedutaTotale = sum(CedutaMensile)
    PRCedutaTotale = CedutaTotale / IrradiataTotale / Pn

    CedutaPRMedioTotale = sum(CedutaMensilePRMedio)
    PRMedioTotale = CedutaPRMedioTotale / IrradiataTotale / Pn

    CedutaPRPVGISTotale = sum(CedutaMensilePRPVGIS)
    PRPVGISTtotale = CedutaPRPVGISTotale / IrradiataTotale / Pn

    IrradiataUltimoMese = IrradiataMensile[currMonth-1]

    CedutaUltimoMese = CedutaMensile[currMonth-1]
    PRCedutaMese = CedutaUltimoMese / IrradiataUltimoMese / Pn

    CedutaUltimoMesePRMedio = CedutaMensilePRMedio[currMonth-1]
    PRMedioMese = CedutaUltimoMesePRMedio / IrradiataUltimoMese / Pn

    CedutaUltimoMesePRPVGIS = CedutaMensilePRPVGIS[currMonth-1]
    PRPVGISMese = CedutaUltimoMesePRPVGIS / IrradiataUltimoMese / Pn

    ETargetMensile[currMonth-1] = mean(PTarget[tI >= tInizioMese]) * dt
    ETargetMensile = np.multiply(ETargetMensile, 1 - usiPropri)
    ETargetTotale = sum(ETargetMensile)
    PRTargetTotale = ETargetTotale / IrradiataTotale / Pn
    ETargetUltimoMese = ETargetMensile[currMonth-1]

    PRTargetMese = ETargetUltimoMese / IrradiataUltimoMese / Pn

    TMYMensile[currMonth-1] = mean(I_TMY[t_TMY >= tInizioMeseTMY]) * dt / 1000
    I_TMYTotale = sum(TMYMensile)
    I_TMYUltimoMese = TMYMensile[currMonth-1]

    tInizioGiorno = datetime(lastt.year, lastt.month, lastt.day, 0, 0, 0)
    tInizioGiornoTMY = datetime(2020, tInizioGiorno.month, tInizioGiorno.day, 0, 0, 0)

    EMHOggi = EMHEn[EMHt >= tInizioGiorno]
    EMHOggi = EMHOggi[len(EMHOggi) - 1] - EMHOggi[0]
    LandisOggi = LandisEn[Landist >= tInizioGiorno]
    LandisOggi = LandisOggi[len(LandisOggi) - 1] - LandisOggi[0]

    ProdottaOggi = EMHOggi + LandisOggi
    CedutaOggi = ProdottaOggi * (1 - usiPropri[currMonth-1])

    dt = lastt - tInizioGiorno
    dt = dt.seconds / 3600
    IrradiataOggi = mean(I[tI >= tInizioGiorno]) * dt / 1000
    ETargetOggi = mean(PTarget[tI >= tInizioGiorno]) * (1 - usiPropri[currMonth-1]) * dt

    I_TMYOggi = mean(I_TMY[t_TMY >= tInizioGiornoTMY]) * dt / 1000

    PROggi = CedutaOggi / IrradiataOggi / Pn
    PRTargetOggi = ETargetOggi / IrradiataOggi / Pn
    ProdottaPRMedioOggi = IrradiataOggi * PRMedi[currMonth-1] * Pn
    CedutaPRMedioOggi = ProdottaPRMedioOggi * (1 - usiPropri[currMonth-1])
    ProdottaPRPVGISOggi = IrradiataOggi * PRPVGIS[currMonth-1] * Pn
    CedutaPRPVGISOggi = ProdottaPRPVGISOggi * (1 - usiPropri[currMonth-1])

    PRMedioOggi = PRMedi[currMonth-1]
    PRPVGISOggi = PRPVGIS[currMonth-1]

    ProdottaOraMedio = lastI * PRMedioOggi * Pn / 1000
    ProdottaOraPVGIS = lastI * PRPVGISOggi * Pn / 1000

    PRMese = str(round(100 * PRCedutaMese, 1))+ " %"
    PRAnno = str(round(100 * PRCedutaTotale, 1))+ " %"

    datiTabella = {

        "tInAnno": tInizioAnno, "tInizioMese": tInizioMese, "tInizioGiorno": tInizioGiorno, "tFinale": lastt,
        "CedutaTotale": CedutaTotale, "IrradiataTotale": IrradiataTotale, "ETargetTotale": ETargetTotale,
        "ITMYTotale": I_TMYTotale, "EPRMedioTotale": CedutaPRMedioTotale, "EPVGIS": CedutaPRPVGISTotale,
        "PRCedutaTotale": PRCedutaTotale, "PRTargetTotale": PRTargetTotale, "PRMedioTotale": PRMedioTotale,
        "PRPVGISTotale": PRPVGISTtotale,
        "CedutaMese": CedutaUltimoMese, "IrradiataMese": IrradiataUltimoMese, "TargetMese": ETargetUltimoMese,
        "ITMYMese": I_TMYUltimoMese, "EPRMedioMese": CedutaUltimoMesePRMedio, "EPVGISMese": CedutaUltimoMesePRPVGIS,
        "PRCedutaMese": PRCedutaMese, "PRTargetMese": PRTargetMese, "PRMedioMese": PRMedioMese,
        "PRPVGISMese": PRPVGISMese,
        "CedutaOggi": CedutaOggi, "IrradiataOggi": IrradiataOggi, "TargetOggi": ETargetOggi, "ITMYOggi": I_TMYOggi,
        "EPRMedioOggi": CedutaPRMedioOggi, "EPVGISOggi": CedutaPRPVGISOggi,
        "PRCedutaOggi": PROggi, "PRTargetOggi": PRTargetOggi, "PRMedioOggi": PRMedioOggi, "PRPVGISOggi": PRPVGISOggi,
        "ProdottaOra": lastP, "PR": lastPR, "IrradiataOra": lastI, "PTargetOra": lastPTarget, "PRTarget": lastPRTarget,
        "lastTMY": lastTMY, "PMedioOra": ProdottaOraMedio, "PRMedioOra": PRMedioOggi, "PPVGISOra": ProdottaOraPVGIS,
        "PRPVGISOra": PRPVGISOggi, "PRMese": PRMese, "PRAnno": PRAnno
    }

    DatiMensili = {
        "Irraggiamenti": IrradiataMensile, "CedutaMensile": CedutaMensile, "ETargetMensile": ETargetMensile,
        "ITMYMensile": TMYMensile}

    #   Preparazione dei dati giornalieri

    tDayP = tI[tI >= tInizioGiorno]
    tDayI = tDayP
    TargetDay = PTarget[tI >= tInizioGiorno]
    IDay = I[tI >= tInizioGiorno]
    P1Day = P1[tI >= tInizioGiorno]
    P2Day = P2[tI >= tInizioGiorno]
    PDay = P1Day + P2Day
    IMedioDay = TMY_I.Imean[(t_TMY >= tInizioGiornoTMY) & (t_TMY <= lasttTMY)]
    PRDay = PR[tI >= tInizioGiorno]

    datiDay = {"tP": tDayP.values, "tI": tDayI.values, "G": IDay, "P": PDay, "Target": TargetDay,
               "ITargetDay": IMedioDay.values, "PRDay": PRDay}
    # last 24
    
    Now = datetime.now()
    tIeri = Now - timedelta(hours=24)
    tlast24P = tI[tI >= tIeri]
    tlast24I = tlast24P
    Targetlast24 = PTarget[tI >= tIeri]
    Ilast24 = I[tI >= tIeri]
    P1last24 = P1[tI >= tIeri]
    P2last24 = P2[tI >= tIeri]
    Plast24 = P1last24 + P2last24
    # PRlast24 = []
    # for i in range(len(Plast24)):
    #     Ecum = sum(Plast24[0:i])
    #     EIrrcum = sum(Ilast24[0:i])
    #     if EIrrcum == 0:
    #         PRlast24 = float('nan')
    #     else:
    #         PRlast24.append(Ecum / EIrrcum / 926.64*1000)

    # IMedioDay = TMY_I.Imean[(t_TMY >= tInizioGiornoTMY) & (t_TMY <= lasttTMY)]
    PRlast24 = PR[tI >= tIeri]

    PMean = mean(P1+P2)
    tOnlast24 = tlast24P[Plast24 > 0]
    sampleOn = len(tOnlast24)
    sampleTot = 24*4 + 1

    sampleFermi = sampleTot - sampleOn

    MinutiFermi = sampleFermi * 15
    EnergiaPersa = PMean * MinutiFermi/60
    ProduzioneMancata = 0

    try:
        PInc = Plast24
        PInc[PInc < 0] = 0
        EInc = mean(PInc[tlast24P >= tIeri]) * 24

    except Exception as err:
        print(err)
        EInc = float('nan')

    Tariffa = 0.225
    EInc = EInc * Tariffa

    # Mensilmente
    tInMese = datetime(Now.year, Now.month, 1, 0, 0, 0)
    dtMese = Now - tInMese
    dtMese = 24*dtMese.days + dtMese.seconds / 3600
    PInc = P1 + P2
    try:
        EIncMese = mean(PInc[tI >= tInMese]) * dtMese
    except Exception as err:
        EIncMese = float("nan")
        
    EIncMeseString, dummy = convertNumber(EIncMese, "Energy", "HTML", "SCN")
    CorrMese, dummy = convertNumber(EIncMese*0.225, "Money", "HTML", "SCN")
    CorrMese = CorrMese

    datilast24 = {"tP": tlast24P.values, "tI": tlast24I.values, "G": Ilast24, "P": Plast24, "Target": Targetlast24,
                  "PRlast24": PRlast24, "EnergiaPersa": EnergiaPersa, "MinutiFermi": MinutiFermi,
                  "MancataProduzione": ProduzioneMancata, "Corrispettivo": EInc,
                  "EIncMese": EIncMeseString, "CorrMese": CorrMese}

    datiGrafico = pd.DataFrame.from_dict(datiDay)
    datiGrafico24 = pd.DataFrame.from_dict(datilast24)

    return datiTabella, DatiMensili, datiGrafico, datiGrafico24


def prevediTimeLines(lastt, TMY_I, TMY_T):
    Pn = 926.64
    beta = 0.0037

    dt = timedelta(minutes=10)
    tCurr = lastt + dt
    # lastt = datetime(2020, lastt.month, lastt.day, lastt.hour, lastt.minute,lastt.second)
    t = np.array(TMY_I["date"], dtype=datetime)
    I = np.array(TMY_I["Imean"])
    T = np.array(TMY_T["Tmean"])

    PTarget = []

    tOut = []
    IOut = []
    TOut = []

    tOff = datetime(tCurr.year + 1, 1, 1, 0, 0, 0)

    for i in range(len(t)):
        PTarget.append(Pn * I[i] * (1 - beta * (T[i] - 25)) * 0.981 / 1000)

    while tCurr < tOff:
        tOut.append(tCurr)

        tCompare = datetime(2020, tCurr.month, tCurr.day, tCurr.hour, tCurr.minute, tCurr.second)
        ICurr = I[t == tCompare]
        ICurr = ICurr[0]
        IOut.append(ICurr)

        TCurr = T[t == tCompare]
        TCurr = TCurr[0]
        TOut.append(TCurr)

        PTarget.append(Pn * ICurr * (1 - beta * (TCurr - 25)) * 0.981 / 1000)

        tCurr = tCurr + dt

    DatiPrevisionali = {"t": tOut, "I": IOut, "T": TOut, "PTarget": PTarget}

    return DatiPrevisionali


def costruisciTimeLines(DatiGrezzi, TMY_I, TMY_T):

    Pn = 926.64
    beta = 0.0037

    I = DatiGrezzi["IData"]["Val"]
    tI = DatiGrezzi["IData"]["t"]
    tIfirst = tI[0]
    tIlast = tI[len(tI) - 1]

    T = DatiGrezzi["TModData"]["Val"]
    tT = DatiGrezzi["TModData"]["t"]
    tTfirst = tT[0]
    tTlast = tT[len(tT) - 1]

    P1 = DatiGrezzi["P1Data"]["Val"]
    tP1 = DatiGrezzi["P1Data"]["t"]
    tP1first = tP1[0]
    tP1last = tP1[len(tP1) - 1]

    P2 = DatiGrezzi["P2Data"]["Val"]
    tP2 = DatiGrezzi["P2Data"]["t"]
    tP2first = tP2[0]
    tP2last = tP2[len(tP2) - 1]

    # PTarget = []

    I_TMY = np.array(TMY_I)
    tI_TMY = I_TMY[:, 0]
    I_TMY = I_TMY[:, 1]

    T_TMY = np.array(TMY_T)
    tT_TMY = T_TMY[:, 0]
    T_TMY = T_TMY[:, 1]

    # Now = datetime.now()

    # t = datetime(Now.year, 1, 1, 0, 0, 0)
    t = min(tIfirst, tTfirst, tP1first, tP2first)
    tMax = max(tIlast, tTlast, tP1last, tP2last)
    dt = timedelta(minutes=10)

    timeLinest = []
    timeLineI = []
    timeLineT = []
    timeLineP1 = []
    timeLineP2 = []
    timeLinePTarget = []

    try:
        while t <= tMax:

            timeLinest.append(t)

            timeLineI, lastI = calcolaTL(t, tI, I, I_TMY, tI_TMY, timeLineI, "I")
            timeLineT, lastT = calcolaTL(t, tT, T, T_TMY, tT_TMY, timeLineT, "T")
            try:
                timeLineP1, lastP1 = calcolaTL(t, tP1, P1, 0, 0, timeLineP1, "P1")
            except Exception as err:
                print(err)

            timeLineP2, lastP2 = calcolaTL(t, tP2, P2, 0, 0, timeLineP2, "P2")

            try:
                lastPTarget = Pn * lastI * (1 - beta * (lastT - 25)) * 0.981 / 1000
                timeLinePTarget.append(lastPTarget)
            except Exception as err:
                print(err)

            t = t + dt
    except Exception  as err:
        print(err)

    # timeLinest = np.array(timeLinest)
    # timeLineI = np.array(timeLineI)
    # timeLineP1 = np.array(timeLineP1)
    # timeLineP2 = np.array(timeLineP2)
    # timeLinePTarget = np.array(timeLinePTarget)

    timeLines = {"t": timeLinest, "I": timeLineI, "T": timeLineT, "P1": timeLineP1, "P2": timeLineP2,
                 "PTarget": timeLinePTarget}

    return timeLines


def aggiornaDataBase(DB, TMY_I, TMY_T):

    t = np.array(DB["t"], dtype=datetime)
    t = pd.to_datetime(t)
    lastt = t[len(t) - 1]

    dt = timedelta(minutes=10)
    firstt = lastt + dt

    token = authenticateHigeco("SCN")

    IData = call2HigecoNew("SCN", "Irradiation", firstt, token)
    # EIrrData = call2HigecoNew("SCN", "Eirr", firstt, token)
    # PData = call2HigecoNew("SCN", "Power", firstt, token)
    # ProdData = call2HigecoNew("SCN", "Energy", firstt, token)
    TModData = call2HigecoNew("SCN", "Temperature", firstt, token)
    P1Data = call2HigecoNew("SCN", "Power1", firstt, token)
    P2Data = call2HigecoNew("SCN", "Power2", firstt, token)

    data = {"IData": IData, "TModData": TModData, "P1Data": P1Data, "P2Data": P2Data}

    if len(P1Data["t"]) > 0:
        lastP1t = P1Data["t"][0]

    else:
        tempi = np.array(DB["t"])
        lastP1t = tempi[len(tempi) - 1]
        lastP1t = datetime.strptime(lastP1t, "%Y-%m-%d %H:%M:%S")
        # DB["lastP1t"] = tempi[len(tempi)-1]

    # lastP2t = P2Data["t"]
    if len(P2Data["t"]) > 0:

        lastP2t = P2Data["t"][0]

    else:

        tempi = np.array(DB["t"])
        lastP2t = tempi[len(tempi) - 1]
        lastP2t = datetime.strptime(lastP2t, "%Y-%m-%d %H:%M:%S")

    if len(data["IData"]["t"]) > 0 and len(data["TModData"]["t"]) > 0 and len(data["P1Data"]["t"]) > 0 and len(
            data["P2Data"]["t"]) > 0:

        timeLines = costruisciTimeLines(data, TMY_I, TMY_T)
        # newDB = {"IData":IData, "TData":TModData, "P1Data": P1Data, "P2Data":P2Data}
        tSeries = pd.Series(timeLines["t"])
        newt = pd.concat([DB["t"], tSeries])
        newt = np.array(newt)

        ISeries = pd.Series(timeLines["I"])
        newI = pd.concat([DB["I"], ISeries])
        newI = np.array(newI)

        TSeries = pd.Series(timeLines["T"])
        newT = pd.concat([DB["T"], TSeries])
        newT = np.array(newT)

        P1Series = pd.Series(timeLines["P1"])
        newP1 = pd.concat([DB["P1"], P1Series])
        newP1 = np.array(newP1)

        P2Series = pd.Series(timeLines["P2"])
        newP2 = pd.concat([DB["P2"], P2Series])
        newP2 = np.array(newP2)

        PTargetSeries = pd.Series(timeLines["PTarget"])
        newPTarget = pd.concat([DB["PTarget"], PTargetSeries])
        newPTarget = np.array(newPTarget)

        newDB = {"t": newt, "I": newI, "T": newT, "P1": newP1, "P2": newP2,
                 "PTarget": newPTarget}

        newDf = pd.DataFrame.from_dict(newDB)
        pd.DataFrame.to_csv(newDf, "newDBSCN3.csv")

        lastt = newt[len(newt) - 1]

    else:

        newDB = DB
        lastts = np.array(DB["t"], dtype=datetime)
        lastt = lastts[len(lastts) - 1]
        lastt = datetime.strptime(lastt, "%Y-%m-%d %H:%M:%S")

    lastI = newDB["I"]
    lastI = lastI[len(lastI) - 1]
    
    lastP1 = newDB["P1"]
    lastP1 = lastP1[len(lastP1)-1]

    lastP2 = newDB["P2"]
    lastP2 = lastP2[len(lastP2)-1]
    
    lastP = lastP1 + lastP2

    return newDB, lastt, lastI, lastP1t, lastP2t, lastP


def ricostruisciTimeLines(DatiGrezzi, TMY_I, TMY_T):
    
    Pn = 926.64
    beta = 0.0037

    I = DatiGrezzi["IData"]["Val"]
    tI = DatiGrezzi["IData"]["t"]
    tIlast = tI[len(tI) - 1]

    T = DatiGrezzi["TModData"]["Val"]
    tT = DatiGrezzi["TModData"]["t"]
    tTlast = tT[len(tT) - 1]

    P1 = DatiGrezzi["P1Data"]["Val"]
    tP1 = DatiGrezzi["P1Data"]["t"]
    tP1last = tP1[len(tP1) - 1]

    P2 = DatiGrezzi["P2Data"]["Val"]
    tP2 = DatiGrezzi["P2Data"]["t"]
    tP2last = tP2[len(tP2) - 1]

    # PTarget = []

    I_TMY = np.array(TMY_I)
    tI_TMY = I_TMY[:, 0]
    I_TMY = I_TMY[:, 1]

    T_TMY = np.array(TMY_T)
    tT_TMY = T_TMY[:, 0]
    T_TMY = T_TMY[:, 1]

    Now = datetime.now()
    t = datetime(Now.year, 1, 1, 0, 0, 0)
    tMax = max(tIlast, tTlast, tP1last, tP2last)
    dt = timedelta(minutes=10)

    timeLinest = []
    timeLineI = []
    timeLineT = []
    timeLineP1 = []
    timeLineP2 = []
    timeLinePTarget = []

    while t <= tMax:

        timeLinest.append(t)

        timeLineI, lastI = calcolaTL(t, tI, I, I_TMY, tI_TMY, timeLineI, "I")
        timeLineT, lastT = calcolaTL(t, tT, T, T_TMY, tT_TMY, timeLineT, "T")
        timeLineP1, lastP1 = calcolaTL(t, tP1, P1, 0, 0, timeLineP1, "P1")
        timeLineP2, lastP2 = calcolaTL(t, tP2, P2, 0, 0, timeLineP2, "P2")

        try:
            timeLinePTarget.append(Pn * lastI * (1 - beta * (lastT - 25)) * 0.981 / 1000)
        except Exception as err:
            print(err)
        t = t + dt

    # timeLinest = np.array(timeLinest)
    # timeLineI = np.array(timeLineI)
    # timeLineP1 = np.array(timeLineP1)
    # timeLineP2 = np.array(timeLineP2)
    # timeLinePTarget = np.array(timeLinePTarget)

    timeLines = {"t": timeLinest, "I": timeLineI, "T": timeLineT, "P1": timeLineP1, "P2": timeLineP2,
                 "PTarget": timeLinePTarget}

    return timeLines
