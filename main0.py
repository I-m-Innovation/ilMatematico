import time
from datetime import datetime
import telebot
import os
import psutil
import pandas as pd
import csv
from ftplib import FTP
from scan import scan
from colorama import Fore, Style
from num2string_001 import convertNumber
from alertSystem import sendResume
import numpy as np

p = psutil.Process(os.getpid())


def main(MainData, bot_data):

    RUBDataNew = scan("RUB", MainData["RUB"], bot_data)
    SCNDataNew = scan("SCN", MainData["SCN"], bot_data)
    PARDataNew = scan("PAR", MainData["PAR"], bot_data)
    STDataNew = scan("ST", MainData["ST"], bot_data)
    PlantData = MainData["CST"]
    PlantData["ST"] = STDataNew
    PlantData["PAR"] = PARDataNew
    CSTDataNew = scan("CST", PlantData, bot_data)
    SA3DataNew = scan("SA3", MainData["SA3"], bot_data)
    TFDataNew = scan("TF", MainData["TF"], bot_data)
    PGDataNew = scan("PG", MainData["PG"], bot_data)

    DataNew = {"SCN": SCNDataNew, "TF": TFDataNew, "ST": STDataNew, "PG": PGDataNew, "PAR": PARDataNew,
               "RUB": RUBDataNew, "SA3": SA3DataNew, "CST": CSTDataNew}

    return DataNew

    # -----------------------------------------------------------------------------------------------------


def writeLastCycle():
    Now = datetime.now()
    ultimoCiclo = {"t": Now}

    with open("lastRun.csv", 'w') as csvfile:

        writer = csv.DictWriter(csvfile, ultimoCiclo.keys())
        writer.writeheader()
        writer.writerow(ultimoCiclo)


def salvaAllarmi(data):

    SCN1State = data["SCN"]["Plant state"]["SCN1"]
    SCN2State = data["SCN"]["Plant state"]["SCN2"]

    if SCN1State == "W" or SCN2State == "W":
        SCNState = "W"

    elif SCN1State == "A" or SCN2State == "A":
        SCNState = "A"

    else:
        SCNState = "O"

    StatoAllarmi = {
        "ST": data["ST"]["Plant state"], "PG": data["PG"]["Plant state"], "SCN": SCNState,
        "SCN1": data["SCN"]["Plant state"]["SCN1"],
        "SCN2": data["SCN"]["Plant state"]["SCN2"], "RUB": data["RUB"]["Plant state"],
        "PAR": data["PAR"]["Plant state"], "TF": data["TF"]["Plant state"], "SA3": data["SA3"]["Plant state"],
        "CST": data["CST"]["Plant state"]
    }

    with open("AlarmStatesBeta.csv", 'w') as csvfile:

        writer = csv.DictWriter(csvfile, StatoAllarmi.keys())
        writer.writeheader()
        writer.writerow(StatoAllarmi)

    fileName = "AlarmStatesBeta.csv"
    File = open(fileName, "rb")
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/Database_Produzione')
    ftp.storbinary(f"STOR " + fileName, File)
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.close()


TGmode = "TEST"
# mode = "RUN"


print("Inizializzazione del sistema.")
# inizializzo lo stato delle centrali in "O" in modo che vengano rinnovati tutti gli allarmi vecchi
STState = "O"
PGState = "O"
SCNState = "O"
SCN1State = "O"
SCN2State = "O"
RUBState = "O"
PARState = "O"
TFState = "O"
SA3State = "O"
CSTSTate = "OK"

token = "6007635672:AAF_kA2nV4mrscssVRHW0Fgzsx0DjeZQIHU"
bot = telebot.TeleBot(token)

print("Caricamento dei dati statici.")
TMYSCN = pd.read_excel("Database impianti/SCN Pilota/TMY SCN.xlsx")
TMYRUB = pd.read_excel("Database impianti/Rubino/TMY RUBINO.xlsx")

# Tabelle fisse SCN

SCNState = {"SCN": SCNState, "SCN1": SCN1State, "SCN2": SCN2State}
SCNData = {"TMY": TMYSCN, "Plant state": SCNState}
TFData = {"Plant state": TFState}
STData = {"Plant state": STState}
PGData = {"Plant state": PGState}
PARData = {"Plant state": PARState}
RUBData = {"TMY": TMYRUB, "Plant state": RUBState}
SA3Data = {"Plant state": SA3State}
CSTData = {"Plant state": CSTSTate}

cycleN = 1
Data = {"SCN": SCNData, "TF": TFData, "ST": STData, "PG": PGData, "PAR": PARData, "RUB": RUBData, "SA3": SA3Data,
        "CST": CSTData}


if TGmode == "TEST":
    print(f'{Fore.YELLOW}Warning: ilMatematico sta lavorando in modalità TEST{Style.RESET_ALL}')
    dt = 2  # minutes

else:
    print(f'{Fore.GREEN}Warning: ilMatematico sta lavorando in modalità RUN{Style.RESET_ALL}')
    dt = 10  # minutes

botData = {"bot": bot, "mode": TGmode}


def sendLast24Resa(mode):

    #---TF---#
    FileName = "TFlast24hStat.csv"
    df = pd.read_csv(FileName)
    ETF, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "TF")

    #---ST---#
    FileName = "STlast24hStat.csv"
    df = pd.read_csv(FileName)
    EST, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "ST")

    #---PAR---#
    FileName = "PARlast24hStat.csv"
    df = pd.read_csv(FileName)
    EPAR, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "PAR")

    #---PG---#
    FileName = "PGlast24hStat.csv"
    df = pd.read_csv(FileName)
    EPG, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "PG")

    #---SCN---#
    FileName = "SCNlast24hStat.csv"
    df = pd.read_csv(FileName)
    ESCN, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "SCN")

    #---RUB---#
    FileName = "RUBlast24hStat.csv"
    df = pd.read_csv(FileName)
    if np.isnan(df["Energy"][0]):
        ERUB = "?"
    else:
        ERUB, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "RUB")

    text = ("*PRODUZIONE DELLE ULTIME 24 ORE:\nTorrino Foresta*: "+ETF+"\n*San Teodoro*: "+ EST+ "\n*Partitore*: "+ EPAR
            +"\n*Ponte Giurino*; "+ EPG + "\n*SCN Pilota*: " + ESCN +"\n*Rubino*: " + ERUB)

    sendResume(text, mode)

isSent = 0

while True:
    print("========================================================================")
    print("CICLO DI CALCOLO NUMERO  "+str(cycleN)+":")
    print("CPU:" + str(p.cpu_percent()) + " %")
    Data = main(Data, botData)
    writeLastCycle()
    salvaAllarmi(Data)
    Now = datetime.now()

    print("Report inviato: "+isSent)
    if Now.minute >= 15 and isSent == 0:
        sendLast24Resa(botData["mode"])
        isSent = 1
    elif Now.minute < 15:
        isSent = 0

    print("CICLO DI CALCOLO NUMERO "+str(cycleN)+" TERMINATO.")
    print("========================================================================")

    time.sleep(60 * dt)
    cycleN = cycleN + 1
    os.system('cls||clear')
