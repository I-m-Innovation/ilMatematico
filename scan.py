from datetime import datetime
from colorama import Fore, Style
from quest2HigecoDefs import authenticateHigeco
from checkProduction import checkProduction
from alertSystem import send_telegram
from displayState import displayState
from scaricaDati import scaricaDati
from calcolaAggregati import calcola_aggregati


def main(plant, data_in, tg_mode):

    if plant == "SCN" or plant == "RUB":
        token = authenticateHigeco("SCN")
        database = scaricaDati(plant, token, [])

    else:
        token = []
        database = scaricaDati(plant, token, data_in)
        data_in["DB"] = database

    if plant == "ZG":
        database = {"DB": database, "TMY": data_in["TMY"]}

    # controllo se l'impianto funziona e in caso mando l'allarme
    new_state = checkProduction(plant, token, data_in)

    if plant == "SCN":
        # mando gli allarmi se serve
        send_telegram(data_in["Plant state"]["SCN1"], new_state["SCN1"], tg_mode, "SCN Pilota - Inverter 1")
        send_telegram(data_in["Plant state"]["SCN2"], new_state["SCN2"], tg_mode, "SCN Pilota - Inverter 2")

        database["Plant state"]["SCN1"] = new_state["SCN1"]
        displayState(new_state["SCN1"])
        database["Plant state"]["SCN2"] = new_state["SCN2"]
        displayState(new_state["SCN2"])

    else:
        send_telegram(data_in["Plant state"], new_state, tg_mode, data_in["PlantName"])
        data_in["Plant state"] = new_state
        displayState(new_state)

    calcola_aggregati(plant, database)

    return data_in


def scan(plant, data, bot_data):

    if plant == "SCN":
        plant_name = "SCN Pilota"
    elif plant == "RUB":
        plant_name = "Rubino"
    elif plant == "ST":
        plant_name = "San Teodoro"
    elif plant == "CST":
        plant_name = "Condotta San Teodoro"
    elif plant == "PAR":
        plant_name = "Partitore"
    elif plant == "PG":
        plant_name = "Ponte Giurino"
    elif plant == "TF":
        plant_name = "Torrino Foresta"
    elif plant == "SA3":
        plant_name = "SA3"
    elif plant == "ZG":
        plant_name = "ZG"
    else:
        plant_name = "IMPIANTO IGNOTO"

    data["PlantName"] = plant_name

    test_id = "-672088289"
    mode = bot_data["mode"]
    bot = bot_data["bot"]

    sep = "------------------------------------------------------------------------"
    try:

        print(sep)
        now = datetime.now()
        print(str(now) + f': inizio calcolo di {Fore.YELLOW}' + plant_name + f'{Style.RESET_ALL}')

        plant_data = data
        data_new = main(plant, plant_data, mode)

        message = "Dati di " + plant_name + " salvati alle " + str(now)
        print(f'-- {Fore.GREEN}' + message + f'{Style.RESET_ALL}')

    except Exception as err:

        message = "ERRORE IN " + plant_name + ": " + str(err)
        bot.send_message(test_id, text=message)
        print(f'-- {Fore.RED}' + message + f'{Style.RESET_ALL}')
        data_new = data

    return data_new
