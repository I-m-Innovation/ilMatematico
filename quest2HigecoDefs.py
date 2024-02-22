from datetime import datetime
import requests
import json
import pandas as pd
import pytz


def call2lastValue(token, Plant):

    headers = {
        "accept": "application/json",
        "Authorization": str(token),
        "Content-Type": "application/json"
    }   # QUESTA LINEA VA CAMBIATA PER MASCHERARE LE CREDENZIALI

    BaseURL = "https://higeco-monitoraggio.it/api/v1/getLastValue/"

    if Plant == "RUB":
        PlantId = "992"
        DevId = "3189OVTGIDEF"
        LogId = "2001087"  # Inverter
        PId = "2001087089"

    elif Plant == "SCN1":
        PlantId = "1032"
        DevId = "2589LJTGI818"
        LogId = "2001442"
        PId = '2001442447'

    elif Plant == "SCN2":
        PlantId = "1032"
        DevId = "2589LJTGI818"
        LogId = "2002672"
        PId = '2002672677'

    elif Plant == "DI":
        PlantId = "53"
        DevId = "2360PGKGI8E8"
        LogId = "1999813"
        PId = '1999813105'
        BaseURL = "https://hsi.higeco.com/api/v1/getLastValue/"

    else:
        PlantId = "54"
        DevId = "2360ZGKGI5E7"
        LogId = "2000629"
        PId = '2000629635'
        BaseURL = "https://hsi.higeco.com/api/v1/getLastValue/"

    URL = BaseURL + PlantId + "/" + DevId + "/" + LogId + "/" + PId

    resp = requests.get(URL, headers=headers)
    Dict = resp.json()
    data = Dict["items"]
    df = pd.DataFrame.from_dict(data)

    lastP = df["value"][0]

    utc = df["utc"][0]
    my_timezone = pytz.timezone('GMT')
    tNew = datetime.fromtimestamp(utc, tz=my_timezone)
    tLoc = tNew.strftime("%d/%m/%Y %H:%M")
    tLoc = datetime.strptime(tLoc, "%d/%m/%Y %H:%M")

    if Plant != "DI" and Plant != "ZG":

        if Plant == "Rubino":
            LogId = "1042331"
            IId = "1042331009"

        else:
            DevId = '2589LJTGI818'
            LogId = '1042242'
            IId = '1042242009'

        URL = "https://higeco-monitoraggio.it/api/v1/getLastValue/" + PlantId + "/" + DevId + "/" + LogId + "/" + IId

        resp = requests.get(URL, headers=headers)
        Dict = resp.json()
        data = Dict["items"]
        df = pd.DataFrame.from_dict(data)

        lastI = df["value"][0]

    else:
        lastI = float('nan')

    Data = {"lastT": tLoc, "lastP": lastP, "lastI": lastI}

    return Data


def authenticateHigeco(Plant):

    if Plant == "Rubino" or Plant == "SCN":
        params = {
            "username": "ziliogroup",
            "password": "Yeeph4ue"
        }

        URL = 'https://higeco-monitoraggio.it/api/v1/authenticate'

    else:
        params = {
            "username": "Zilio group",
            "password": "he9gieLi"
        }
        URL = 'https://hsi.higeco.com/api/v1/authenticate'

    responseApi = requests.post(URL, data=json.dumps(params))

    token = json.loads(responseApi.text)['token']

    return token


def call2Higeco(Plant, DataType, UnixOnSet, token):

    headers = {
        "accept": "application/json",
        "Authorization": str(token),
        "Content-Type": "application/json"
    }   # QUESTA LINEA VA CAMBIATA PER MASCHERARE LE CREDENZIALI

    if Plant == "SCN":
        PId = '1032'
        DevId = '2589LJTGI818'
        LogId = '1042242'

        if DataType == "Irradiation":
            VarId = '1042242009'

        elif DataType == "Eirr":
            VarId = '1042242003'

        elif DataType == "PowerEMH":
            LogId = '2003079'
            VarId = '2003079081'

        elif DataType == "Power":
            VarId = '1042242001'

        elif DataType == "PowerLandis":
            LogId = '2003079'
            VarId = '2003079081'

        elif DataType == "Energy":
            VarId = '1042242000'

        elif DataType == "TMod":
            VarId = '1042242134'

        elif DataType == "EMH":
            LogId = '2003079'
            VarId = '2005861865'

        elif DataType == "Landis":

            LogId = '2005861'
            VarId = '2003079083'

        elif DataType == "Power1":

            LogId = '2001442'
            VarId = '2001442447'

        elif DataType == "Power2":

            LogId = '2002672'
            VarId = '2002672677'

    else:
        PId = '992'
        DevId = '3189OVTGIDEF'
        LogId = '2001087'

        if DataType == "Irradiation":
            VarId = '1042331009'

        elif DataType == "Energy":
            VarId = '1042331000'

        elif DataType == "Power":
            VarId = '1042331001'

        else:
            VarId = '1042331008'

    URL = "https://higeco-monitoraggio.it/api/v1/getLogData/" + PId + "/" + DevId + "/" + LogId + "/" + VarId +\
          "?from=" + str(int(UnixOnSet))

    resp = requests.get(URL, headers=headers)

    Dict = resp.json()

    df = pd.DataFrame.from_dict(Dict['data'])

    if len(df) > 0:

        t = pd.to_datetime(df[0], unit='s', utc=True).map(lambda x: x.tz_convert('Europe/Rome'))
        Var = df[1]
        data = {"t": t, "Val": Var}

    else:
        data = []

    return data
