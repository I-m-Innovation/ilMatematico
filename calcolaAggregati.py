import pandas as pd
import numpy as np
from ftplib import FTP
from calcolaPeriodi import calcola_periodi


def expected_eta(last_var2, plant, last_var3):

    if plant == "SA3":

        file_name = "rendimentoReale" + plant + ".csv"
        curva_rendimento = pd.read_csv(file_name)

        q_teo = curva_rendimento["QOut"]
        if plant != "TF" and plant != "SA3":
            q_teo = q_teo / 1000

        eta_teo = curva_rendimento["etaOut"]
        eta_dev = curva_rendimento["devEta"]

        # cerco i valori più vicini last Q

        i = 0
        while last_var2 > q_teo.iloc[i]:
            i = i + 1

        if i != 0:
            eta_aspettato = (eta_teo[i - 1] + eta_teo[i]) / 2
            dev_aspettato = 0.5 * np.sqrt(eta_dev[i - 1] ** 22 + eta_dev[i] ** 2)
        else:
            eta_aspettato = (eta_teo[i] + eta_teo[i]) / 2
            dev_aspettato = 0.5 * np.sqrt(eta_dev[i] ** 2 + eta_dev[i] ** 2)

        eta_min = eta_aspettato - dev_aspettato
        eta_max = eta_aspettato + dev_aspettato

    else:

        if plant != "TF" and plant != "SA3" and plant != "SCN":
            last_var2 = last_var2 * 1000

        mean_file = "MeanEta" + plant + ".csv"
        dev_file = "DevEta" + plant + ".csv"

        curva_rendimento = pd.read_csv(mean_file, header=None)
        dev_rendimento = pd.read_csv(dev_file, header=None)

        asse_var2 = curva_rendimento.iloc[0, 1:]
        asse_var3 = curva_rendimento.iloc[1:, 0]

        # cerco i valori più vicini last Q

        i = 0
        var2_test = asse_var2.iloc[i]

        while last_var2 > var2_test and i < len(asse_var2) - 1:
            i = i + 1
            var2_test = asse_var2.iloc[i]

        j = 0
        var3_test = asse_var3.iloc[j]

        if np.isnan(last_var3):
            last_var3 = 0
        final_j = 1
        while last_var3 > var3_test and j < len(asse_var3):
            j = j + 1
            if j >= len(asse_var3):
                final_j = j - 1
                var3_test = asse_var3.iloc[final_j]

            else:
                final_j = j
                var3_test = asse_var3.iloc[final_j]

        eta_aspettato = curva_rendimento.iloc[final_j, i]
        if np.isnan(eta_aspettato):
            eta_aspettato = np.mean([curva_rendimento.iloc[final_j - 1, i], curva_rendimento.iloc[final_j + 1, i],
                                     curva_rendimento.iloc[final_j, i - 1], curva_rendimento.iloc[final_j, i + 1]])

        dev_aspettato = dev_rendimento.iloc[final_j, i]

        eta_min = eta_aspettato - dev_aspettato
        eta_max = eta_aspettato + dev_aspettato

    return eta_aspettato, eta_min, eta_max


def salva_ultimo_timestamp(data, plant):

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')

    ftp.cwd('/dati/Database_Produzione')

    g_file = open("lista_impianti.xlsx", "wb")
    ftp.retrbinary('RETR lista_impianti.xlsx', g_file.write)
    g_file.close()

    dati_impianti = pd.read_excel("lista_impianti.xlsx")

    last_power = data["P"].iloc[-1]

    rho = 1000
    g = 9.81

    if plant != "ZG":
        last_var2 = data["Q"].iloc[-1]
        last_var3 = data["Bar"].iloc[-1]
        last_eta = 1000 * last_power / (rho * g * last_var2 * last_var3 * 10.1974)
    else:
        last_var2 = data["I"].iloc[-1]
        last_var3 = data["TMod"].iloc[-1]

    if plant == "SA3":
        eta_expected, eta_min_expected, eta_max_expected = 0, 0, 0
        eta_dev = eta_expected - eta_min_expected

    elif plant == "CST":
        eta_aspettato_st, eta_minus_st, eta_plus_st = expected_eta(data["QST"].iloc[-1]/1000, "ST", last_var3)
        eta_aspettato_par, eta_minus_par, eta_plus_par = expected_eta(data["QPAR"].iloc[-1]/1000, "PAR", last_var3)
        eta_expected = (70 * eta_aspettato_st + 25 * eta_aspettato_par) / 95
        dev_eta_st = eta_aspettato_st - eta_minus_st
        dev_eta_par = eta_aspettato_par - eta_minus_par
        eta_dev = np.sqrt((70 * dev_eta_st) ** 2 + (25 * dev_eta_par) ** 2) / 95

    elif plant == "ZG":
        eta_aspettato_st, eta_minus_st, eta_plus_st = float('NaN'), float('NaN'), float('NaN')
        eta_aspettato_par, eta_minus_par, eta_plus_par = float('NaN'), float('NaN'), float('NaN')
        eta_expected = float('NaN')
        dev_eta_st = float('NaN')
        dev_eta_par = float('NaN')
        eta_dev = float('NaN')

    else:
        eta_expected, eta_min_expected, eta_max_expected = expected_eta(last_var2, plant, last_var3)
        eta_dev = eta_expected - eta_min_expected

    power_expected = eta_expected * rho * g * last_var2 * last_var3 * 10.1974 / 1000
    power_dev = eta_dev * rho * g * last_var2 * last_var3 * 10.1974 / 1000

    dati_impianto = dati_impianti[dati_impianti["Tag"] == plant]
    dati_impianto = dati_impianto.reset_index()

    pn = dati_impianto["potenza_installata_kWp"][0]
    var2_max = dati_impianto["Var2_max"][0]
    var2_media = dati_impianto["Var2_media"][0]
    var2_dev = dati_impianto["Var2_dev"][0]

    var3_max = dati_impianto["Var3_max"][0]
    var3_media = dati_impianto["Var3_media"][0]
    var3_dev = dati_impianto["Var3_dev"][0]

    print(f"Eta medio gauge: {eta_expected}; Deviazione standard: {eta_dev}")
    print(f"Potenza medio gauge: {power_expected}; Deviazione standard: {power_dev}")

    if plant != "ZG":

        dati_gauge = {
            "Power": {"last_value": last_power, "MaxScala": pn, "Media": power_expected, "Dev": power_dev},
            "Var2": {"last_value": last_var2, "MaxScala": var2_max, "Media": var2_media, "Dev": var2_dev},
            "Var3": {"last_value": last_var3, "MaxScala": var3_max, "Media": var3_media, "Dev": var3_dev},
            "Eta": {"last_value": last_eta, "MaxScala": 100, "Media": eta_expected, "Dev": eta_dev}
        }
    else:
        dati_gauge = {
            "Power": {"last_value": last_power, "MaxScala": pn, "Media": power_expected, "Dev": power_dev},
            "Var2": {"last_value": last_var2, "MaxScala": var2_max, "Media": var2_media, "Dev": var2_dev},
            "Var3": {"last_value": last_var3, "MaxScala": var3_max, "Media": var3_media, "Dev": var3_dev},
            "Eta": {"last_value": float('NaN'), "MaxScala": 100, "Media": float('NaN'), "Dev": float('NaN')}

        }

    last_ts_filename = plant+"_dati_gauge.csv"
    pd.DataFrame.from_dict(dati_gauge).to_csv(last_ts_filename)

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')

    ftp.cwd('/dati/' + dati_impianto["folder"][0])

    file = open(last_ts_filename, "rb")
    ftp.storbinary(f"STOR " + last_ts_filename, file)
    ftp.close()


def calcola_aggregati_hydro(plant, data):

    g = 9.81
    rho = 1000

    power_st = []
    power_par = []
    q_st = []
    q_par = []
    eta = []

    if plant == "ST" or plant == "PAR":

        t = data["timestamp"]
        t = pd.to_datetime(t, format='mixed')
        q = data["Portata [l/s]"] / 1000
        power = data["Potenza [kW]"]
        bar = data["Pressione [bar]"]
        q4eta = 10
        if plant == "PAR":
            q4eta = 5
        q4eta = q4eta / 1000
        ftp_folder = '/dati/San_Teodoro'

    elif plant == "PG":

        t = data["Local"]
        t = pd.to_datetime(t, dayfirst=True)
        q = data["PLC1_AI_FT_PORT_IST"] / 1000
        power = data["PLC1_AI_POT_ATTIVA"]
        bar = data["PLC1_AI_PT_TURBINA"]
        q4eta = 10
        q4eta = q4eta / 1000
        ftp_folder = '/dati/ponte_giurino'

    elif plant == "TF":

        t = data["Time"]
        t = pd.to_datetime(t)
        q = data["Charge"]
        power = data["Power"]
        bar = data["Jump"] / 10.1974
        q4eta = 0.6
        q4eta = q4eta
        ftp_folder = '/dati/Torrino_Foresta'

    elif plant == "SA3":

        t = data["t"]
        t = pd.to_datetime(t, format='mixed')
        q = data["Q"]
        power = data["P"]
        bar = data["Bar"] / 10.1974
        q4eta = 0.6
        q4eta = q4eta
        ftp_folder = '/dati/SA3'

    elif plant == "CST":

        t = data["t"]
        q = data["Q"] / 1000
        q_par = data["QPAR"]
        q_st = data["QST"]
        power_par = data["PPAR"]
        power_st = data["PST"]
        power = data["P"]
        bar = data["Bar"]
        eta = data["eta"]
        q4eta = 0
        ftp_folder = '/dati/San_Teodoro'

    else:
        q4eta = 0
        t = data["DB"]["t"]
        q = data["DB"]["Q"]
        power = data["DB"]["P"]
        bar = data["DB"]["Bar"]

        eta = data["DB"]["eta"]

        ftp_folder = '/dati/San_Teodoro'

    if plant != "CST":
        eta = np.divide(1000 * power, rho * g * np.multiply(q, bar) * 10.1974)
        data_periodi = {"t": t, "Q": q, "P": power, "Bar": bar, "Q4Eta": q4eta, "eta": eta}
    else:
        data_periodi = {"t": t, "QST": q_st, "QPAR": q_par, "Q": q, "PST": power_st, "PPAR": power_par, "P": power,
                        "Bar": bar, "Q4Eta": q4eta, "eta": eta}

    calcola_periodi(data_periodi, plant, ftp_folder)

    salva_ultimo_timestamp(data_periodi, plant)


def calcola_aggregati_pv(plant, data):

    t = data["DB"]["t"]
    t = pd.to_datetime(t)

    power_1 = []
    power_2 = []

    if plant == "SCN":

        power_1 = data["P1"]
        power_2 = data["P2"]

        power = power_1 + power_2
        pn = 926.64

        ftp_folder = '/dati/SCN'
        temp_mod = data["TMod"]
        irr = data["I"]
    elif plant == "ZG":

        power = data["DB"]["PAC_PV"]
        pn = 40
        ftp_folder = '/dati/Zilio_Roof/Zilio Group'
        irr = data["TMY"]["Irr"]
        temp_mod = data["TMY"]["T_Mod"]
    else:
        irr = data["I"]

        power = data["P"]

        pn = 997.44

        ftp_folder = '/dati/Rubino'

    eta = np.divide(1000 * power, irr) / pn

    if plant == "SCN":
        data_periodi = {"t": t, "I": irr, "P1": power_1, "P2": power_2, "TMod": temp_mod, "eta": eta, "P": power,
                        "Tariffa": data["Tariffa"], "PN": data["PN"]}

    elif plant == "ZG":
        data_periodi = {"t": t, "I": irr, "TMod": temp_mod, "eta": eta, "P": power, "Tariffa": 0,
                        "PN": pn}
    else:
        data_periodi = {"t": t, "I": irr, "TMod": temp_mod, "eta": eta, "P": power, "Tariffa": data["Tariffa"],
                        "PN": data["PN"]}

    calcola_periodi(data_periodi, plant, ftp_folder)

    salva_ultimo_timestamp(data_periodi, plant)


def calcola_aggregati(plant, data):

    if plant == "SCN" or plant == "RUB" or plant == "ZG":
        calcola_aggregati_pv(plant, data)

    else:
        calcola_aggregati_hydro(plant, data)
