from datetime import datetime, timedelta
import numpy as np
from statistics import mean
import math


def calcolaTL(t, tVar, Var, Var_TMY, tVar_TMY, timeLineVar, VarType):
    try:
        dt = timedelta(minutes=10)
        VarCurr = Var[tVar == t]

        if np.size(VarCurr) == 0:

            VarPrev = Var[tVar == t - dt]
            VarNext = Var[tVar == t + dt]

            if np.size(VarPrev) + np.size(VarNext) > 1:

                try:

                    VarCurr = mean([VarPrev[0], VarNext[0]])

                except Exception as err:
                    print(err)
            else:
                if VarType == "P" or VarType == "P1" or VarType == "P2":
                    VarCurr = 0
                else:
                    tCompare = datetime(2020, t.month, t.day, t.hour, t.minute, t.second)
                    VarCurr = Var_TMY[tVar_TMY == tCompare]
                    VarCurr = VarCurr[0]
        else:
            VarCurr = VarCurr[0]

        if VarType == "I":
            if VarCurr >= 0:
                timeLineVar.append(VarCurr)
        else:
            timeLineVar.append(VarCurr)
    except Exception as err:
        print(err)

    return timeLineVar, VarCurr


def calcolaTLRubino(t, tVar, Var, Var_TMY, tVar_TMY, timeLineVar, VarType):
    dt = timedelta(minutes=15)
    VarCurr = Var[tVar == t]

    if np.size(VarCurr) == 0:

        VarPrev = Var[tVar == t - dt]
        VarNext = Var[tVar == t + dt]

        if np.size(VarPrev) + np.size(VarNext) > 1:

            try:
                VarCurr = mean([VarPrev[0], VarNext[0]])

            except Exception as err:
                print(err)
        else:
            if VarType == "P":
                VarCurr = 0
            else:
                tCompare = datetime(2020, t.month, t.day, t.hour, t.minute, t.second)
                VarCurr = VarPrev
                try:
                    VarCurr = Var_TMY[tVar_TMY == tCompare]
                    VarCurr = VarCurr[0]
                except Exception as err:
                    print(err)
    else:
        VarCurr = VarCurr[0]

    if VarType == "I":
        if VarCurr >= 0:
            timeLineVar.append(VarCurr)
    else:
        timeLineVar.append(VarCurr)

    return timeLineVar, VarCurr
