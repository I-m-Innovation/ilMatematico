from datetime import datetime
from colorama import Fore, Style
from quest2HigecoDefs import authenticateHigeco
from checkProduction import checkProduction
from alertSystem import sendTelegram
from displayState import displayState
from scaricaDati import scaricaDati
from calcolaAggregati import calcolaAggregati


def main(Plant, Data, TGMode):

    if Plant == "SCN" or Plant == "RUB":
        token = authenticateHigeco("SCN")
        data = scaricaDati(Plant, token, [])

    else:
        token = []
        data = scaricaDati(Plant, token, Data)
        Data["DB"] = data

    # controllo se l'impianto funziona e in caso mando l'allarme
    NewState = checkProduction(Plant, token, Data)

    if Plant == "SCN":
        # mando gli allarmi se serve
        sendTelegram(Data["Plant state"]["SCN1"], NewState["SCN1"], TGMode, "SCN Pilota - Inverter 1")
        sendTelegram(Data["Plant state"]["SCN2"], NewState["SCN2"], TGMode, "SCN Pilota - Inverter 2")

        Data["Plant state"]["SCN1"] = NewState["SCN1"]
        displayState(NewState["SCN1"])
        Data["Plant state"]["SCN2"] = NewState["SCN2"]
        displayState(NewState["SCN2"])
    else:

        sendTelegram(Data["Plant state"], NewState, TGMode, Data["PlantName"])
        Data["Plant state"] = NewState
        displayState(NewState)

    calcolaAggregati(Plant, data)

    return Data


def scan(Plant, Data, botData):

    if Plant == "SCN":
        PlantName = "SCN Pilota"
    elif Plant == "RUB":
        PlantName = "Rubino"
    elif Plant == "ST":
        PlantName = "San Teodoro"
    elif Plant == "CST":
        PlantName = "Condotta San Teodoro"
    elif Plant == "PAR":
        PlantName = "Partitore"
    elif Plant == "PG":
        PlantName = "Ponte Giurino"
    elif Plant == "TF":
        PlantName = "Torrino Foresta"
    elif Plant == "SA3":
        PlantName = "SA3"
    else:
        PlantName = "IMPIANTO IGNOTO"

    Data["PlantName"] = PlantName

    TestId = "-672088289"
    mode = botData["mode"]
    bot = botData["bot"]

    Sep = "------------------------------------------------------------------------"
    try:

        print(Sep)
        Now = datetime.now()
        print(str(Now) + f': inizio calcolo di {Fore.YELLOW}'+PlantName+f'{Style.RESET_ALL}')

        PlantData = Data
        DataNew = main(Plant, PlantData, mode)

        message = "Dati di " + PlantName + " salvati alle " + str(Now)
        print(f'-- {Fore.GREEN}' + message + f'{Style.RESET_ALL}')

    except Exception as err:

        message = "ERRORE IN "+PlantName+": " + str(err)
        bot.send_message(TestId, text=message)
        print(f'-- {Fore.RED}' + message + f'{Style.RESET_ALL}')
        DataNew = Data

    return DataNew
