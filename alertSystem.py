from datetime import datetime
import numpy as np
import telebot


def sendResume(text, TGMode):

    token = "6007635672:AAF_kA2nV4mrscssVRHW0Fgzsx0DjeZQIHU"
    bot = telebot.TeleBot(token)
    RunId = "-821960472"
    TestId = "-672088289"

    if TGMode == "TEST":
        ID = TestId
    else:
        ID = RunId
    bot.send_message(ID, text=text, parse_mode='Markdown')


def controllaCentraleTG(DatiIst, Plant, oldState):

    Now = np.datetime64(datetime.now(), 'm')

    if Plant == "SCN1":
        lastP = DatiIst["P1"]
        lastP = lastP[len(lastP) - 1]
        lastT = DatiIst["lastP1t"]
        lastT = np.datetime64(lastT[len(lastT) - 1], 'm')

    elif Plant == "SCN2":
        lastP = DatiIst["P2"]
        lastP = lastP[len(lastP) - 1]
        lastT = DatiIst["lastP2t"]
        lastT = np.datetime64(lastT[len(lastT) - 1], 'm')

    elif Plant == "Rubino":
        lastP = DatiIst["lastP"]

        if lastP < 1.5:
            lastP = 0

        lastT = DatiIst["lastT"]
        lastT = np.datetime64(lastT, 'm')

    elif Plant == "PAR":
        Powers = np.array(DatiIst["P"])
        lastP = Powers.item()
        Times = np.array(DatiIst["t"])
        lastT = Times.item()
        lastT = np.datetime64(lastT, 'm')

    else:

        try:
            lastP = DatiIst["P"]
            lastT = DatiIst["t"]
            lastT = np.datetime64(lastT, 'm')

        except Exception as err:
            print(err)
            lastT = []
            lastP = []

    dt = Now - lastT

    if dt > 30 or lastP == []:

        if oldState == "W":
            newState = "A"

        elif oldState == "A":
            newState = "A"
        else:
            newState = "W"

    elif lastP <= 0:
        newState = "A"

    else:
        newState = "O"

    return newState


def controllaFotovoltaico(DatiIst, Plant, OldState, lastI):

    Now = np.datetime64(datetime.now(), 'm')

    if Plant == "SCN1":
        lastP = DatiIst["lastP"]
        lastT = DatiIst["lastT"]
        lastT = np.datetime64(lastT, 'm')

    elif Plant == "SCN2":
        lastP = DatiIst["lastP"]
        lastT = DatiIst["lastT"]
        lastT = np.datetime64(lastT, 'm')

    elif Plant == "RUB":
        lastP = DatiIst["lastP"]
        lastT = DatiIst["lastT"]
        lastT = np.datetime64(lastT, 'm')

    elif Plant == "DI" or Plant == "ZG":
        lastP = DatiIst["lastP"]
        lastT = DatiIst["lastT"]
        lastT = np.datetime64(lastT, 'm')
        
    else:
        lastP = DatiIst["P"]
        lastT = DatiIst["t"]
        lastT = np.datetime64(lastT, 'm')

    dt = Now - lastT

    if dt > 30 or lastP == [] or lastI == "Not Found":

        if OldState == "W":
            newState = "A"

        elif OldState == "A":
            newState = "A"

        else:
            newState = "W"

    else:

        if lastI > 50:
            if lastP == 0:
                newState = "A"

            else:
                newState = "O"

        else:
            print("Irraggiamento sotto soglia!")
            newState = OldState

    return newState


def sendTelegram(Old, New, TGMode, PlantName):

    token = "6007635672:AAF_kA2nV4mrscssVRHW0Fgzsx0DjeZQIHU"
    bot = telebot.TeleBot(token)
    RunId = "-821960472"
    TestId = "-672088289"

    if TGMode == "TEST":
        ID = TestId
    else:
        ID = RunId

    if PlantName == "CST":

        if New == "OK":
            if Old != "OK":
                Text = "💦 *CONDOTTA SAN TEODORO*: Portata rientrata a regime!"
                bot.send_message(ID, text=Text, parse_mode='Markdown')

            elif New == "A":
                if Old != "A":
                    Text = "🔴 *CONDOTTA SAN TEODORO*: Portata sotto soglia!"
                    bot.send_message(ID, text=Text, parse_mode='Markdown')

            elif New == "U":

                if Old != "U":
                    Text = "❓ *CONDOTTA SAN TEODORO*: Stato condotta non riconosciuto!"
                    bot.send_message(ID, text=Text, parse_mode='Markdown')

    else:

        if New == "O":
            if Old != "O":
                Text = "🟢 *" + PlantName + "*: IN PRODUZIONE!"
                bot.send_message(ID, text=Text, parse_mode='Markdown')

        elif New == "W":
            if Old != "W":
                Text = "🟠 *" + PlantName + "*: FERMO o in NO LINK!"
                bot.send_message(ID, text=Text, parse_mode='Markdown')

        elif New == "A":
            if Old != "A":
                Text = "🔴 *" + PlantName + "*: FERMO o in NO LINK!"
                bot.send_message(ID, text=Text, parse_mode='Markdown')

        elif New == "U":

            if Old != "U":
                Text = "❓ *" + PlantName + "*: Stato centrale non riconosciuto!"
                bot.send_message(ID, text=Text, parse_mode='Markdown')


def controllaCST(lastQCST):

    if lastQCST < 91:
        newState = "A"

    else:
        newState = "OK"

    return newState