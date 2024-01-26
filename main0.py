import time
from datetime import datetime
from colorama import Fore, Style
import telebot
from mains import mainSCN, mainTF, mainST, mainPG, mainPartitore, mainRUB, mainSA3
import os
import psutil
import pandas as pd
import csv

p = psutil.Process(os.getpid())


def main(mode, MainData):
    TestId = "-672088289"

    Sep = "------------------------------------------------------------------------"

    # ===================SCN PILOTA===================
    try:

        print(Sep)
        Now = datetime.now()
        print(str(Now) + f': inizio calcolo di {Fore.YELLOW}SCN PILOTA{Style.RESET_ALL}')

        PlantData = MainData["SCN"]
        SCNDataNew = mainSCN(PlantData, mode)

        message = "Dati di SCN PILOTA salvati alle " + str(Now)
        print(f'-- {Fore.GREEN}' + message + f'{Style.RESET_ALL}')

    except Exception as err:

        message = "ERRORE IN SCN PILOTA: " + str(err)
        bot.send_message(TestId, text=message)
        print(f'-- {Fore.RED}' + message + f'{Style.RESET_ALL}')
        SCNDataNew = MainData["SCN"]

    # ===================TORRINO FORESTA===================
    try:

        print(Sep)
        Now = datetime.now()
        print(str(Now) + f': inizio calcolo di {Fore.BLUE}TORRINO FORESTA{Style.RESET_ALL}')

        PlantData = MainData["TF"]
        TFDataNew = mainTF(PlantData, mode)

        message = "Dati di TORRINO FORESTA salvati alle " + str(Now)
        print(f'-- {Fore.GREEN}' + message + f'{Style.RESET_ALL}')

    except Exception as err:

        message = "ERRORE IN TORRINO FORESTA: " + str(err)
        print(f'-- {Fore.RED}' + message + f'{Style.RESET_ALL}')
        TFDataNew = MainData["TF"]

    # ===================SAN TEODORO===================
    try:

        print(Sep)
        Now = datetime.now()
        print(str(Now) + f': inizio calcolo di {Fore.BLUE}SAN TEODORO{Style.RESET_ALL}')

        PlantData = MainData["ST"]
        STDataNew = mainST(PlantData, mode)

        message = "Dati di San Teodoro salvati alle " + str(Now)
        print(f'-- {Fore.GREEN}' + message + f'{Style.RESET_ALL}')

    except Exception as err:

        message = "ERRORE IN SAN TEODORO: " + str(err)
        print(f'-- {Fore.RED}' + message + f'{Style.RESET_ALL}')
        STDataNew = MainData["ST"]

    # ===================PONTE GIURINO===================
    try:

        print(Sep)
        Now = datetime.now()
        print(str(Now) + f': inizio calcolo di {Fore.BLUE}PONTE GIURINO{Style.RESET_ALL}')

        PlantData = MainData["PG"]
        PGDataNew = mainPG(PlantData, mode)

        message = "Dati di Ponte Giurino salvati alle " + str(Now)
        print(f'-- {Fore.GREEN}' + message + f'{Style.RESET_ALL}')

    except Exception as err:

        message = "ERRORE IN PONTE GIURINO: " + str(err)
        print(f'-- {Fore.RED}' + message + f'{Style.RESET_ALL}')
        PGDataNew = MainData["PG"]

    # ===================PARTITORE===================
    try:

        print(Sep)
        Now = datetime.now()
        print(str(Now) + f': inizio calcolo di {Fore.BLUE}PARTITORE{Style.RESET_ALL}')

        PlantData = MainData["PAR"]
        PARDataNew = mainPartitore(PlantData, mode)

        message = "Dati di Partitore salvati alle " + str(Now)
        print(f'-- {Fore.GREEN}' + message + f'{Style.RESET_ALL}')

    except Exception as err:

        message = "ERRORE IN PARTITORE: " + str(err)
        print(f'-- {Fore.RED}' + message + f'{Style.RESET_ALL}')
        PARDataNew = MainData["PAR"]

    # ===================RUBINO===================
    try:

        print(Sep)
        Now = datetime.now()
        print(str(Now) + f': inizio calcolo di {Fore.YELLOW}RUBINO{Style.RESET_ALL}')

        PlantData = MainData["RUB"]
        RUBDataNew = mainRUB(PlantData, mode)

        message = "Dati di Rubino salvati alle " + str(Now)
        print(f'-- {Fore.GREEN}' + message + f'{Style.RESET_ALL}')

    except Exception as err:

        message = "ERRORE IN RUBINO: " + str(err)
        print(f'-- {Fore.RED}' + message + f'{Style.RESET_ALL}')
        RUBDataNew = MainData["RUB"]

    # ===================SA3===================
    try:

        print(Sep)
        Now = datetime.now()
        print(str(Now) + f': inizio calcolo di {Fore.YELLOW}SA3{Style.RESET_ALL}')

        PlantData = MainData["SA3"]
        SA3DataNew = mainSA3(PlantData, mode)

        message = "Dati di SA3 salvati alle " + str(Now)
        print(f'-- {Fore.GREEN}' + message + f'{Style.RESET_ALL}')

    except Exception as err:

        message = "ERRORE IN SA3: " + str(err)
        print(f'-- {Fore.RED}' + message + f'{Style.RESET_ALL}')
        SA3DataNew = MainData["SA3"]

    DataNew = {"SCN": SCNDataNew, "TF": TFDataNew, "ST": STDataNew, "PG": PGDataNew, "PAR": PARDataNew,
               "RUB": RUBDataNew, "SA3": SA3DataNew}

    return DataNew

    # -----------------------------------------------------------------------------------------------------


def writeLastCycle():
    Now = datetime.now()
    ultimoCiclo = {"t": Now}

    with open("lastRun.csv", 'w') as csvfile:

        writer = csv.DictWriter(csvfile, ultimoCiclo.keys())
        writer.writeheader()
        writer.writerow(ultimoCiclo)


TGmode = "TEST"
# mode = "RUN"


print("Inizializzazione del sistema.")
# inizializzo lo stato delle centrali in "O" in modo che vengano rinnovati tutti gli allarmi vecchi
STState = "O"
PGState = "O"
SCN1State = "O"
SCN2State = "O"
RUBState = "O"
PARState = "O"
TFState = "O"
SA3State = "O"

token = "6007635672:AAF_kA2nV4mrscssVRHW0Fgzsx0DjeZQIHU"
bot = telebot.TeleBot(token)

print("Caricamento dei dati statici.")
TMYSCN = pd.read_excel("Database impianti/SCN Pilota/TMY SCN.xlsx")
TMYRUB = pd.read_excel("Database impianti/Rubino/TMY RUBINO.xlsx")

# Tabelle fisse SCN

SCNState = {"SCN1": SCN1State, "SCN2": SCN2State}
SCNData = {"TMY": TMYSCN, "Plant state": SCNState}
TFData = {"Plant state": TFState}
STData = {"Plant state": STState}
PGData = {"Plant state": PGState}
PARData = {"Plant state": PARState}
RUBData = {"TMY": TMYRUB, "Plant state": RUBState}
SA3Data = {"Plant state": SA3State}

cycleN = 1
Data = {"SCN": SCNData, "TF": TFData, "ST": STData, "PG": PGData, "PAR": PARData, "RUB": RUBData, "SA3": SA3Data}

if TGmode == "TEST":
    print(f'{Fore.YELLOW}Warning: ilMatematico sta lavorando in modalità TEST{Style.RESET_ALL}')
    dt = 5  # minutes

else:
    print(f'{Fore.GREEN}Warning: ilMatematico sta lavorando in modalità RUN{Style.RESET_ALL}')
    dt = 10  # minutes


while True:
    print("========================================================================")
    print("CICLO DI CALCOLO NUMERO  "+str(cycleN)+":")
    print("CPU:" + str(p.cpu_percent()) + " %")
    Data = main(TGmode, Data)
    writeLastCycle()
    print("CICLO DI CALCOLO NUMERO "+str(cycleN)+" TERMINATO.")
    print("========================================================================")

    time.sleep(60 * dt)
    cycleN = cycleN + 1
    os.system('cls||clear')
