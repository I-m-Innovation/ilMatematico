from datetime import datetime, timedelta
import numpy as np
import pandas as pd


def calcolaPeriodiHydro(Plant, data, Period):

    Now = datetime.now()
    Tariffa = 0.21

    t = data["t"]
    t = pd.to_datetime(t)
    Q = data["Q"]
    P = data["P"]

    PST = []
    PPAR = []

    if Plant == "CST":
        PST = data["PST"]
        PPAR = data["PPAR"]

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
    PMean = np.mean(PSel)
    PDev = np.std(PSel)

    PSTSel = []
    PPARSel = []
    PSTMean = []
    PSTDev = []
    PPARMean = []
    PPARDev = []

    if Plant == "CST":
        PSTSel = PST[t >= tStart]
        PPARSel = PPAR[t >= tStart]
        PSTMean = np.mean(PSTSel)
        PSTDev = np.std(PSTSel)
        PPARMean = np.mean(PPARSel)
        PPARDev = np.std(PPARSel)

    BarSel = Bar[t >= tStart]
    tSel = t[t >= tStart]
    etaSel = eta[t >= tStart]

    QMean = np.mean(QSel)
    QDev = np.std(QSel)

    BarMean = np.mean(BarSel)
    BarDev = np.std(BarSel)

    etaMean = np.mean(etaSel[(QSel >= Q4Eta) & (etaSel < 1) & (np.isinf(etaSel)==0)])
    etaDev = np.std(etaSel[(QSel >= Q4Eta) & (etaSel < 1) & (np.isinf(etaSel)==0)])

    ESel = PMean * (dt.days * 24 + dt.seconds / 3600)
    FERSel = ESel * Tariffa

    NSamples = len(tSel)
    NOn = len(PSel[PSel > 0])

    if NSamples != 0:
        Av = NOn / NSamples
    else:
        Av = 0

    if Plant == "CST":
        TLDict = {"t": tSel, "Q": QSel, "P": PSel, "Bar": BarSel, "Eta": etaSel, "PST": PSTSel, "PPAR": PPARSel}
    else:
        TLDict = {"t": tSel, "Q": QSel, "P": PSel, "Bar": BarSel, "Eta": etaSel}

    TLdf = pd.DataFrame.from_dict(TLDict)

    StatDict = {"QMean": [QMean], "QDev": [QDev], "PMean": [PMean], "PDev": [PDev], "BarMean": [BarMean],
                "BarDev": [BarDev], "etaMean": [etaMean], "etaDev": [etaDev], "Energy": [ESel], "Resa": [FERSel],
                "Availability": [Av]}

    Statdf = pd.DataFrame.from_dict(StatDict)

    return TLdf, Statdf


def calcolaPeriodiPV(data, Period, PlantTag):

    Now = datetime.now()

    Tariffa = data["Tariffa"][0]
    PN = data["PN"][0]

    t = data["t"]
    I = data["I"]
    if PlantTag == "SCN":
        P1 = data["P1"]
        P2 = data["P2"]
        P = P1 + P2
    else:
        P = data["P"]

    PN = data["PN"]

    TMod = data["TMod"]
    eta = data["eta"]

    if Period == "Annuale":
        tStart = datetime(Now.year, 1, 1, 0, 0, 0)
    elif Period == "Mensile":
        tStart = datetime(Now.year, Now.month, 1, 0, 0, 0)
    else:
        dt = timedelta(hours=24)
        tStart = Now - dt

    dt = Now - tStart

    ISel = I[t >= tStart]
    TSel = TMod[t >= tStart]

    tSel = t[t >= tStart]
    etaSel = eta[t >= tStart]

    IMean = np.mean(ISel)
    IDev = np.std(ISel)

    if PlantTag == "SCN":
        P1Sel = P1[t >= tStart]
        P2Sel = P2[t >= tStart]
        P1Mean = np.mean(P1Sel)
        P1Dev = np.std(P1Sel)
        P2Mean = np.mean(P2Sel)
        P2Dev = np.std(P2Sel)

        PSel = P1Sel + P2Sel

    else:
        PSel = P[t >= tStart]

    PMean = np.mean(PSel)
    PDev = np.std(PSel)

    TModMean = np.mean(TSel)
    TModDev = np.std(TSel)

    ESel = PMean * (dt.days * 24 + dt.seconds / 3600)

    EISel = IMean * (dt.days * 24 + dt.seconds / 3600)

    if PlantTag == "SCN":

        E1Sel = P1Mean * (dt.days * 24 + dt.seconds / 3600)
        E2Sel = P2Mean * (dt.days * 24 + dt.seconds / 3600)

        eta1Mean = E1Sel / EISel / PN[0]
        eta2Mean = E2Sel / EISel / PN[0]

        # NSamples1 = len(t)
        NSunOn = len(ISel[ISel>=50])
        NOn1 = len(P1Sel[(P1Sel > 0) & (ISel>=50)])

        if NSunOn >0:

            Av1 = NOn1 / NSunOn

            NSamples2 = len(t)
            NOn1 = len(P2Sel[(P2Sel > 0) & (ISel>=50)])
            Av2 = NOn1 / NSunOn
        else:
            Av1 = float("nan")
            Av2 = float("nan")

    else:

        NSamples = len(t)
        NSunOn = len(ISel[ISel>=50])
        NOn = len(PSel[(PSel > 0) & (ISel>=50)])
        if NSunOn > 0:
            Av = NOn / NSunOn
        else:
            Av = float("nan")

    etaMean = ESel / EISel / PN[0] * 1000
    etaDev = np.std(etaSel)

    FTVSel = ESel * Tariffa

    TLDict = {"t": tSel, "I": ISel, "P": PSel, "TMod": TSel, "Eta": etaSel}
    TLdf = pd.DataFrame.from_dict(TLDict)

    if PlantTag == "SCN":

        StatDict = {"EI": [EISel], "PMean": [PMean], "PDev": [PDev], "TModMean": [TModMean],
                    "TModDev": [TModDev], "Energy": [ESel], "Energy 1": [E1Sel],
                    "Energy 2": [E2Sel], "etaMean": [etaMean], "etaDev": [etaDev], "etaMean 1": [eta1Mean],
                    "etaMean 2": [eta2Mean],
                    "Resa": [FTVSel],
                    "Availability Inv 1": [Av1], "Availability Inv 2": [Av2]}
    else:
        StatDict = {"EI": [EISel], "PMean": [PMean], "PDev": [PDev], "TModMean": [TModMean],
                    "TModDev": [TModDev], "Energy": [ESel], "etaMean": [etaMean], "etaDev": [etaDev],
                    "Resa": [FTVSel],
                    "Availability": [Av]}

    Statdf = pd.DataFrame.from_dict(StatDict)

    return TLdf, Statdf
