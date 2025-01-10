import pandas as pd
from datetime import datetime

t0 = datetime(2024, 3, 2, 0, 0, 0)
tF = datetime(2024, 12, 13, 0, 0, 0)

db_to_restore = pd.read_csv("DBPGOLD.csv")

db_to_insert_5 = pd.read_csv("DatiTrend5_20241216_094409.csv", on_bad_lines='skip', sep=';')
db_to_insert_7 = pd.read_csv("DatiTrend7_20241216_100902.csv", on_bad_lines='skip', sep=';')

# seleziono la fetta di dati che mi interessa

x__TimeStamp = []
for time in db_to_insert_5["TimeCol"]:
    try:
        test = datetime.strptime(time, "%d/%m/%Y %H:%M")
    except Exception as e:
        print(e)
        test = datetime.strptime(time, "%d/%m/%Y")
    x__TimeStamp.append(test.strftime("%d/%m/%Y %H:%M:%S"))
x__TimeStamp = pd.Series(x__TimeStamp)


Local = []
for time in db_to_insert_5["LocalCol"]:
    try:
        test = datetime.strptime(time, "%d/%m/%Y %H:%M")
    except Exception as e:
        print(e)
        test = datetime.strptime(time, "%d/%m/%Y")

    Local.append(test.strftime("%d/%m/%Y %H:%M:%S"))
Local = pd.Series(Local)

PLC_1_AI_PT_LINEA = db_to_insert_7["PRESSIONE ACQUA DI LINEA [EW166]"].str.replace(",", ".").astype(float)
PLC_1_AI_PT_TURBINA = db_to_insert_5["PRESSIONE ACQUA TURBINA [EW164]"].str.replace(",", ".").astype(float)
PLC_1_AI_POT_ATTIVA = db_to_insert_5["G1 : Potenza attiva prodotta Turbina [STRUMENTO]"].str.replace(",", ".").astype(float)
PLC1_AI_FT_PORT_IST = db_to_insert_5["MISURATORE DI PORTATA [EW168]"].str.replace(",", ".").astype(float)
PLC1_AI_COSPHI = pd.Series(['ND']*len(PLC1_AI_FT_PORT_IST))
PLC1_AI_LT_STRAMAZZO = pd.Series(['ND']*len(PLC1_AI_FT_PORT_IST))

df = pd.concat([x__TimeStamp, PLC_1_AI_PT_LINEA, PLC_1_AI_PT_TURBINA, PLC_1_AI_POT_ATTIVA,PLC1_AI_FT_PORT_IST, PLC1_AI_COSPHI, PLC1_AI_LT_STRAMAZZO, Local], axis=1)
df.to_csv("DBPG_to_insert.csv", index=False)
