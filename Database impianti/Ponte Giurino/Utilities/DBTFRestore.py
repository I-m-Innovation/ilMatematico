import pandas as pd
import numpy as np
from dateutil import tz
from datetime import datetime
import pytz


OldDB = pd.read_csv("DBTFNEW4(19).csv")

Rest1 = pd.read_csv("DatiTrend3_20240612_111237.csv", sep=";")
# Rest2 = pd.read_csv("RestTF2.csv", sep=";")
# Rest3 = pd.read_csv("RestTF3.csv", sep=";")

# RestTot = pd.concat([Rest1, Rest2, Rest3])
RestTot = Rest1
newTime = RestTot["TimeCol"]
newLocal = pd.to_datetime(RestTot["LocalCol"], dayfirst=True, format= 'mixed')

newPT_Linea = RestTot["PLC1_AI_PT_TURBINAmt"][1:len(RestTot)].replace(",", ".")
newPT_Turbina = RestTot["PLC1_AI_PT_TURBINAmt"][1:len(RestTot)].replace(",", ".")
newPotAtt = RestTot["PLC1_AI_POT_ATTIVA"][1:len(RestTot)].replace(",", ".")
newPort = RestTot["PLC1_AI_FT_PORT_IST"][1:len(RestTot)].replace(",", ".")

newPT_LineaSave = []
newPT_TurbinaSave = []
newPotAttSave = []
newPortSave = []
newCosPhi = []
newLevStream = []

for i in range(len(newPT_Linea)):
    
    try:
        newPT_LineaSave.append(float(newPT_Linea.values[i].replace(",", ".")))
        newPT_TurbinaSave.append(float(newPT_Turbina.values[i].replace(",", ".")))
        newPotAttSave.append(float(newPotAtt.values[i].replace(",", ".")))
        newPortSave.append(float(newPort.values[i].replace(",", ".")))

    except Exception as err:
        newPT_LineaSave.append(float('nan'))
        newPT_TurbinaSave.append(float('nan'))
        newPotAttSave.append(float('nan'))
        newPortSave.append(float('nan'))

    newCosPhi.append(0)
    newLevStream.append(0)


NewLines = pd.DataFrame({'x__TimeStamp': newTime[1:len(newTime)], 'Local': newLocal[1:len(newLocal)],
                         'PLC1_AI_PT_LINEA': newPT_LineaSave, 'PLC1_AI_PT_TURBINA': newPT_TurbinaSave,
                         'PLC1_AI_POT_ATTIVA': newPotAttSave, 'PLC1_AI_FT_PORT_IST': newPortSave, 'PLC1_AI_COSPHI': newCosPhi,
                         'PLC1_AI_LT_STRAMAZZO': newLevStream})

LastTab = pd.concat([OldDB, NewLines[1:len(NewLines)-1]])
LastTab.to_csv("DBTFNEW3_TEST.csv", index=False)
