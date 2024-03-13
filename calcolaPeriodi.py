from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from ftplib import FTP


def detect_t_start(period):

    now = datetime.now()

    if period == "Annuale":
        t_start = datetime(now.year, 1, 1, 0, 0, 0)
    elif period == "Mensile":
        t_start = datetime(now.year, now.month, 1, 0, 0, 0)
    else:
        dt = timedelta(hours=24)
        t_start = now - dt

    dt = now - t_start

    return t_start, dt


def calcola_periodi_hydro(plant, data, period):

    tariffa = 0.21

    t = data["t"]
    t = pd.to_datetime(t)
    q = data["Q"]
    power = data["P"]

    power_st = []
    power_par = []

    if plant == "CST":
        power_st = data["PST"]
        power_par = data["PPAR"]

    bar = data["Bar"]
    eta = data["eta"]
    q4eta = data["Q4Eta"]

    t_start, dt = detect_t_start(period)

    q_sel = q[t >= t_start]
    q_sel = q_sel[np.isnan(q_sel) == 0]

    power_sel = power[t >= t_start]
    power_sel = power_sel[np.isnan(power_sel) == 0]
    power_mean = np.mean(power_sel)
    power_dev = np.std(power_sel)

    power_st_sel = []
    power_par_sel = []

    if plant == "CST":
        power_st_sel = power_st[t >= t_start]
        power_par_sel = power_par[t >= t_start]

    bar_sel = bar[t >= t_start]
    bar_sel = bar_sel[np.isnan(bar_sel) == 0]

    t_sel = t[t >= t_start]
    eta_sel = eta[t >= t_start]

    q_mean = np.mean(q_sel)
    q_dev = np.std(q_sel)

    bar_mean = np.mean(bar_sel)
    bar_dev = np.std(bar_sel)

    eta_mean = np.mean(eta_sel[(q_sel >= q4eta) & (eta_sel < 1) & (np.isinf(eta_sel) == 0)])
    eta_dev = np.std(eta_sel[(q_sel >= q4eta) & (eta_sel < 1) & (np.isinf(eta_sel) == 0)])

    energy_sel = power_mean * (dt.days * 24 + dt.seconds / 3600)
    fer_sel = energy_sel * tariffa

    n_samples = len(t_sel)
    n_on = len(power_sel[power_sel > 0])

    if n_samples != 0:
        av = n_on / n_samples
    else:
        av = 0

    if plant == "CST":
        tl_dict = {"t": t_sel, "Q": q_sel, "P": power_sel, "Bar": bar_sel, "Eta": eta_sel, "PST": power_st_sel,
                   "PPAR": power_par_sel}
    else:
        tl_dict = {"t": t_sel, "Q": q_sel, "P": power_sel, "Bar": bar_sel, "Eta": eta_sel}

    tl_df = pd.DataFrame.from_dict(tl_dict)

    stat_dict = {"QMean": [q_mean], "QDev": [q_dev], "PMean": [power_mean], "PDev": [power_dev], "BarMean": [bar_mean],
                 "BarDev": [bar_dev], "etaMean": [eta_mean], "etaDev": [eta_dev], "Energy": [energy_sel],
                 "Resa": [fer_sel], "Availability": [av]}

    stat_df = pd.DataFrame.from_dict(stat_dict)

    return tl_df, stat_df


def calcola_periodi_pv(data, period, plant_tag):

    tariffa = data["Tariffa"][0]

    t = data["t"]
    irr = data["I"]
    if plant_tag == "SCN":
        power1 = data["P1"]
        power2 = data["P2"]
        power = power1 + power2
    else:
        power = data["P"]

    pn = data["PN"]
    temp_mod = data["TMod"]
    eta = data["eta"]

    t_start, dt = detect_t_start(period)

    irr_sel = irr[t >= t_start]
    temp_sel = temp_mod[t >= t_start]

    t_sel = t[t >= t_start]
    eta_sel = eta[t >= t_start]

    irr_mean = np.mean(irr_sel)
    power1 = []
    power2 = []

    if plant_tag == "SCN":
        power1_sel = power1[t >= t_start]
        power2_sel = power2[t >= t_start]
        power_sel = power1_sel + power2_sel

    else:
        power_sel = power[t >= t_start]

    power_mean = np.mean(power_sel)
    power_dev = np.std(power_sel)

    temp_mod_mean = np.mean(temp_sel)
    t_mod_dev = np.std(temp_sel)

    energy_sel = power_mean * (dt.days * 24 + dt.seconds / 3600)

    energy_irr_sel = irr_mean * (dt.days * 24 + dt.seconds / 3600)

    power1_mean = []
    power2_mean = []
    power1_sel = []
    power2_sel = []
    energy1_sel = []
    energy2_sel = []
    eta1_mean = []
    eta2_mean = []

    av = []
    av1 = []
    av2 = []

    if plant_tag == "SCN":

        energy1_sel = power1_mean * (dt.days * 24 + dt.seconds / 3600)
        energy2_sel = power2_mean * (dt.days * 24 + dt.seconds / 3600)

        eta1_mean = energy1_sel / energy_irr_sel / pn[0]
        eta2_mean = energy2_sel / energy_irr_sel / pn[0]

        # NSamples1 = len(t)
        n_sun_on = len(irr_sel[irr_sel >= 50])
        n_on_1 = len(power1_sel[(power1_sel > 0) & (irr_sel >= 50)])

        if n_sun_on > 0:

            av1 = n_on_1 / n_sun_on
            n_on_1 = len(power2_sel[(power2_sel > 0) & (irr_sel >= 50)])
            av2 = n_on_1 / n_sun_on
        else:
            av1 = float("nan")
            av2 = float("nan")

    else:

        n_sun_on = len(irr_sel[irr_sel >= 50])
        n_on = len(power_sel[(power_sel > 0) & (irr_sel >= 50)])
        if n_sun_on > 0:
            av = n_on / n_sun_on
        else:
            av = float("nan")

    eta_mean = energy_sel / energy_irr_sel / pn[0] * 1000
    eta_dev = np.std(eta_sel)

    ftv_sel = energy_sel * tariffa

    tl_dict = {"t": t_sel, "I": irr_sel, "P": power_sel, "TMod": temp_sel, "Eta": eta_sel}
    tl_df = pd.DataFrame.from_dict(tl_dict)

    if plant_tag == "SCN":

        stat_dict = {"EI": [energy_irr_sel], "PMean": [power_mean], "PDev": [power_dev], "TModMean": [temp_mod_mean],
                     "TModDev": [t_mod_dev], "Energy": [energy_sel], "Energy 1": [energy1_sel],
                     "Energy 2": [energy2_sel], "etaMean": [eta_mean], "etaDev": [eta_dev], "etaMean 1": [eta1_mean],
                     "etaMean 2": [eta2_mean], "Resa": [ftv_sel], "Availability Inv 1": [av1],
                     "Availability Inv 2": [av2]}
    else:

        stat_dict = {"EI": [energy_irr_sel], "PMean": [power_mean], "PDev": [power_dev], "TModMean": [temp_mod_mean],
                     "TModDev": [t_mod_dev], "Energy": [energy_sel], "etaMean": [eta_mean], "etaDev": [eta_dev],
                     "Resa": [ftv_sel], "Availability": [av]}

    stat_df = pd.DataFrame.from_dict(stat_dict)

    return tl_df, stat_df


def calcola_periodi(data_periodi, plant, ftp_folder):

    if plant == "RUB" or plant == "SCN":
        year_tl, year_stat = calcola_periodi_pv(plant, data_periodi, "Annuale")
        month_tl, month_stat = calcola_periodi_pv(plant, data_periodi, "Mensile")
        last24_tl, last24_stat = calcola_periodi_pv(plant, data_periodi, "24h")

    else:

        year_tl, year_stat = calcola_periodi_hydro(plant, data_periodi, "Annuale")
        month_tl, month_stat = calcola_periodi_hydro(plant, data_periodi, "Mensile")
        last24_tl, last24_stat = calcola_periodi_hydro(plant, data_periodi, "24h")

    year_tl_filename = plant + "YearTL.csv"
    year_stat_filename = plant + "YearStat.csv"
    year_tl.to_csv(year_tl_filename, index=False)
    year_stat.to_csv(year_stat_filename, index=False)

    # calcolo i dati mensili
    month_tl_filename = plant + "MonthTL.csv"
    month_stat_filename = plant + "MonthStat.csv"
    month_tl.to_csv(month_tl_filename, index=False)
    month_stat.to_csv(month_stat_filename, index=False)

    # calcolo i dati giornalieri
    last24_tl_filename = plant + "last24hTL.csv"
    last24_stat_filename = plant + "last24hStat.csv"
    last24_tl.to_csv(last24_tl_filename, index=False)
    last24_stat.to_csv(last24_stat_filename, index=False)

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd(ftp_folder)

    file = open(year_tl_filename, "rb")
    ftp.storbinary(f"STOR " + year_tl_filename, file)
    file = open(year_stat_filename, "rb")
    ftp.storbinary(f"STOR " + year_stat_filename, file)

    file = open(month_tl_filename, "rb")
    ftp.storbinary(f"STOR " + month_tl_filename, file)
    file = open(month_stat_filename, "rb")
    ftp.storbinary(f"STOR " + month_stat_filename, file)

    file = open(last24_tl_filename, "rb")
    ftp.storbinary(f"STOR " + last24_tl_filename, file)
    file = open(last24_stat_filename, "rb")
    ftp.storbinary(f"STOR " + last24_stat_filename, file)
