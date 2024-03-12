from datetime import datetime
import numpy as np
import telebot


def send_resume(text, tg_mode):

    token = "6007635672:AAF_kA2nV4mrscssVRHW0Fgzsx0DjeZQIHU"
    bot = telebot.TeleBot(token)
    run_id = "-821960472"
    test_id = "-672088289"

    if tg_mode == "TEST":
        tg_id = test_id
    else:
        tg_id = run_id
    bot.send_message(tg_id, text=text, parse_mode='Markdown')


def controlla_centrale_tg(dati_ist, plant, old_state):

    now = np.datetime64(datetime.now(), 'm')

    dt_test = 30
    if plant == "SCN1":
        last_power = dati_ist["P1"]
        last_power = last_power[len(last_power) - 1]
        last_timestamp = dati_ist["lastP1t"]
        last_timestamp = np.datetime64(last_timestamp[len(last_timestamp) - 1], 'm')

    elif plant == "SCN2":
        last_power = dati_ist["P2"]
        last_power = last_power[len(last_power) - 1]
        last_timestamp = dati_ist["lastP2t"]
        last_timestamp = np.datetime64(last_timestamp[len(last_timestamp) - 1], 'm')

    elif plant == "Rubino":
        last_power = dati_ist["lastP"]

        if last_power < 1.5:
            last_power = 0

        last_timestamp = dati_ist["lastT"]
        last_timestamp = np.datetime64(last_timestamp, 'm')

    elif plant == "PAR":
        powers = np.array(dati_ist["P"])
        last_power = powers.item()
        times = np.array(dati_ist["t"])
        last_timestamp = times.item()
        last_timestamp = np.datetime64(last_timestamp, 'm')

    else:

        try:
            last_power = dati_ist["P"]
            last_timestamp = dati_ist["t"]
            last_timestamp = np.datetime64(last_timestamp, 'm')

        except Exception as err:
            print(err)
            last_timestamp = []
            last_power = []

    if plant == "TF":
        dt_test = 40

    dt = now - last_timestamp

    if dt > dt_test or last_power == []:

        if old_state == "W":
            new_state = "A"

        elif old_state == "A":
            new_state = "A"
        else:
            new_state = "W"

    elif last_power <= 0:
        new_state = "A"

    else:
        new_state = "O"

    return new_state


def controlla_fotovoltaico(dati_ist, plant, old_state, last_irr):

    now = np.datetime64(datetime.now(), 'm')

    if plant == "SCN1":
        last_power = dati_ist["lastP"]
        last_timestamp = dati_ist["lastT"]
        last_timestamp = np.datetime64(last_timestamp, 'm')

    elif plant == "SCN2":
        last_power = dati_ist["lastP"]
        last_timestamp = dati_ist["lastT"]
        last_timestamp = np.datetime64(last_timestamp, 'm')

    elif plant == "RUB":
        last_power = dati_ist["lastP"]
        last_timestamp = dati_ist["lastT"]
        last_timestamp = np.datetime64(last_timestamp, 'm')

    elif plant == "DI" or plant == "ZG":
        last_power = dati_ist["lastP"]
        last_timestamp = dati_ist["lastT"]
        last_timestamp = np.datetime64(last_timestamp, 'm')
        
    else:
        last_power = dati_ist["P"]
        last_timestamp = dati_ist["t"]
        last_timestamp = np.datetime64(last_timestamp, 'm')

    dt = now - last_timestamp

    if dt > 30 or last_power == [] or last_irr == "Not Found":

        if old_state == "W":
            new_state = "A"

        elif old_state == "A":
            new_state = "A"

        else:
            new_state = "W"

    else:

        if last_irr > 50:
            if last_power == 0:
                new_state = "A"

            else:
                new_state = "O"

        else:
            print("Irraggiamento sotto soglia!")
            new_state = old_state

    return new_state


def send_telegram(old, new, tg_mode, plant_name):

    token = "6007635672:AAF_kA2nV4mrscssVRHW0Fgzsx0DjeZQIHU"
    bot = telebot.TeleBot(token)
    run_id = "-821960472"
    test_id = "-672088289"

    if tg_mode == "TEST":
        tg_id = test_id
    else:
        tg_id = run_id

    if plant_name == "Condotta San Teodoro":

        if new == "OK":
            if old != "OK":
                text = "💦 *CONDOTTA SAN TEODORO*: Portata rientrata a regime!"
                bot.send_message(tg_id, text=text, parse_mode='Markdown')

            elif new == "A":
                if old != "A":
                    text = "🔴 *CONDOTTA SAN TEODORO*: Portata sotto soglia!"
                    bot.send_message(tg_id, text=text, parse_mode='Markdown')

            elif new == "U":

                if old != "U":
                    text = "❓ *CONDOTTA SAN TEODORO*: Stato condotta non riconosciuto!"
                    bot.send_message(tg_id, text=text, parse_mode='Markdown')

        elif new == "A":
            if old != "A":
                text = "🔴 *CONDOTTA SAN TEODORO*: Portata sotto soglia!"
                bot.send_message(tg_id, text=text, parse_mode='Markdown')

    else:

        if new == "O":
            if old != "O":
                text = "🟢 *" + plant_name + "*: IN PRODUZIONE!"
                bot.send_message(tg_id, text=text, parse_mode='Markdown')

        elif new == "W":
            if old != "W":
                text = "🟠 *" + plant_name + "*: FERMO o in NO LINK!"
                bot.send_message(tg_id, text=text, parse_mode='Markdown')

        elif new == "A":
            if old != "A":
                text = "🔴 *" + plant_name + "*: FERMO o in NO LINK!"
                bot.send_message(tg_id, text=text, parse_mode='Markdown')

        elif new == "U":

            if old != "U":
                text = "❓ *" + plant_name + "*: Stato centrale non riconosciuto!"
                bot.send_message(tg_id, text=text, parse_mode='Markdown')


def controlla_cst(last_q_cst):

    if last_q_cst < 91:
        new_state = "A"

    else:
        new_state = "OK"

    return new_state
