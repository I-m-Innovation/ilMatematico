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
from alertSystem import send_resume
import numpy as np

p = psutil.Process(os.getpid())


def main(main_data, bot_data):

    # SCNDataNew = scan("SCN", MainData["SCN"], bot_data)
    sa3_data_new = scan("SA3", main_data["SA3"], bot_data)
    # RUBDataNew = scan("RUB", MainData["RUB"], bot_data)
    par_data_new = scan("PAR", main_data["PAR"], bot_data)
    st_data_new = scan("ST", main_data["ST"], bot_data)
    plant_data = main_data["CST"]
    plant_data["ST"] = st_data_new
    plant_data["PAR"] = par_data_new
    cst_data_new = scan("CST", plant_data, bot_data)
    tf_data_new = scan("TF", main_data["TF"], bot_data)
    pg_data_new = scan("PG", main_data["PG"], bot_data)

    data_new = {"TF": tf_data_new, "ST": st_data_new, "PG": pg_data_new, "PAR": par_data_new, "SA3": sa3_data_new,
                "CST": cst_data_new}

    return data_new

    # -----------------------------------------------------------------------------------------------------


def write_last_cycle():

    now = datetime.now()
    ultimo_ciclo = {"t": now}

    with open("lastRun.csv", 'w') as csvfile:

        writer = csv.DictWriter(csvfile, ultimo_ciclo.keys())
        writer.writeheader()
        writer.writerow(ultimo_ciclo)
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')

    ftp.cwd('/dati/Database_Produzione')

    file_name = "lastRun.csv"
    file = open(file_name, "rb")
    ftp.storbinary(f"STOR " + file_name, file)
    ftp.close()


def salva_allarmi(data):

    stato_allarmi = {
        "ST": data["ST"]["Plant state"], "PG": data["PG"]["Plant state"], "PAR": data["PAR"]["Plant state"],
        "TF": data["TF"]["Plant state"], "SA3": data["SA3"]["Plant state"], "CST": data["CST"]["Plant state"],
    }

    with open("AlarmStatesBeta.csv", 'w') as csvfile:

        writer = csv.DictWriter(csvfile, stato_allarmi.keys())
        writer.writeheader()
        writer.writerow(stato_allarmi)

    filename = "AlarmStatesBeta.csv"
    file = open(filename, "rb")
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/Database_Produzione')
    ftp.storbinary(f"STOR " + filename, file)
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.close()


TGmode = "TEST"
TGmode = "RUN"

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
    dt = 10  # minutes

else:
    print(f'{Fore.GREEN}Warning: ilMatematico sta lavorando in modalità RUN{Style.RESET_ALL}')
    dt = 10  # minutes

botData = {"bot": bot, "mode": TGmode}


def send_last24_resa(mode):

    # ---TF--- #
    filename = "TFlast24hStat.csv"
    df = pd.read_csv(filename)
    energy_tf, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "TF")

    # ---ST--- #
    filename = "STlast24hStat.csv"
    df = pd.read_csv(filename)
    energy_st, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "ST")

    # ---PAR--- #
    filename = "PARlast24hStat.csv"
    df = pd.read_csv(filename)
    energy_par, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "PAR")

    # ---PG--- #
    filename = "PGlast24hStat.csv"
    df = pd.read_csv(filename)
    energy_pg, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "PG")

    # ---SCN--- #
    filename = "SCNlast24hStat.csv"
    df = pd.read_csv(filename)
    energy_scn, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "SCN")

    # ---RUB--- #
    filename = "RUBlast24hStat.csv"
    df = pd.read_csv(filename)
    if np.isnan(df["Energy"][0]):
        energy_rub = "?"
    else:
        energy_rub, dummy = convertNumber(df["Energy"][0], "Energy", "HTML", "RUB")

    text = ("*PRODUZIONE DELLE ULTIME 24 ORE:\nTorrino Foresta*: " + energy_tf + "\n*San Teodoro*: " + energy_st +
            "\n*Partitore*: " + energy_par + "\n*Ponte Giurino*; " + energy_pg + "\n*SCN Pilota*: " + energy_scn +
            "\n*Rubino*: " + energy_rub)

    send_resume(text, mode)


isSent = 0

while True:
    print("========================================================================")
    print("CICLO DI CALCOLO NUMERO  "+str(cycleN)+":")
    print("CPU:" + str(p.cpu_percent()) + " %")
    Data = main(Data, botData)
    write_last_cycle()
    salva_allarmi(Data)
    Now = datetime.now()

    if Now.hour >= 21 and isSent == 0:
        send_last24_resa(botData["mode"])
        isSent = 1
    elif Now.hour < 21:
        isSent = 0

    print("CICLO DI CALCOLO NUMERO "+str(cycleN)+" TERMINATO.")
    print("========================================================================")

    time.sleep(60 * dt)
    cycleN = cycleN + 1
    os.system('cls||clear')
