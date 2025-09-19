from ftplib import FTP_TLS
from datetime import datetime,timedelta
from gme_ftp import connect_to_ftp
import pandas as pd
import sqlite3
import numpy as np
import os
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import numpy as np


def scarica_file(conn, t_on):
    now = datetime.now()

    # cartella dove salvare i file
    download_dir = "gme_downloads"
    os.makedirs(download_dir, exist_ok=True)

    df_total = pd.DataFrame()
    date = pd.Series(dtype="datetime64[ns]")

    while t_on < now:
        t_on_plus_one = t_on + timedelta(hours=1)
        file_time_stamp = t_on_plus_one.strftime("%Y%m%d")
        filename = f"{file_time_stamp}MGPPrezzi.xml"
        filepath = os.path.join(download_dir, filename)

        # ✅ scarica solo se non presente
        if os.path.exists(filepath):
            print(f"File già presente, salto: {filename}")
        else:
            print(f"Scarico: {filename}")
            with open(filepath, "wb") as file:
                conn.retrbinary(f"RETR {filename}", file.write)

        # leggi comunque il file locale
        df = pd.read_xml(filepath)
        date = pd.concat([date, pd.Series([t_on] * (len(df) - 1))])
        df_total = pd.concat([df_total, df[1:]])

        t_on += timedelta(days=1)

    # pulizia e conversione dati
    df_new = df_total[["Data", "PUN", "CALA", "NORD"]].copy()
    df_new["PUN"] = df_new["PUN"].str.replace(",", ".").astype(float)
    df_new["CALA"] = df_new["CALA"].str.replace(",", ".").astype(float)
    df_new["NORD"] = df_new["NORD"].str.replace(",", ".").astype(float)
    df_new["Data"] = date.values

    # salva in SQLite (⚠️ replace sovrascrive: se vuoi append basta cambiare)
    conn_sql = sqlite3.connect(r"C:\Users\Sviluppo_Software_ZG\Desktop\PortaleMonitoraggio\Produzione\db.sqlite3")
    df_new.to_sql("Prezzi energia", conn_sql, if_exists="append", index=False)
    conn_sql.close()
    conn.close()

    return df_new



def scarica_file_00(conn, t_on):
    now = datetime.now()

    df_total = pd.DataFrame()

    date = pd.Series()

    while t_on < now:
        # t_on_plus_one = t_on + timedelta(hours=1)
        file_time_stamp = t_on.strftime("%Y%m%d")
        filename = f"{file_time_stamp}MGPPrezzi.xml"
        file_path = f'gme_downloads/{filename}'

        with open( file_path, 'wb' ) as file :
            conn.retrbinary('RETR %s' % filename, file.write)

        df = pd.read_xml(file_path)

        dates = df["Data"][1:]
        date_string = str(int(dates.iloc[0]))
        hours = df["Ora"][1:]

        dates = []

        for hour in hours:

            if hour == 25:
                hour = 24

            hour_string = str(int(hour-1))

            if hour < 10:
                hour_string = f'0{hour_string}'

            complete_string = f'{date_string} {hour_string}'
            dates.append(datetime.strptime(complete_string,'%Y%m%d %H'))

        date = pd.concat([date, pd.Series(dates)])

        if len(dates) != len(df[1:]):
            A = 2
        df_total = pd.concat([df_total, df[1:]])
        t_on += timedelta(days=1)

    df_new = df_total[["Data", "PUN", "CALA", "NORD"]]
    df_new["PUN"] = df_new["PUN"].str.replace(",",".").astype(float)
    df_new["CALA"] = df_new["CALA"].str.replace(",",".").astype(float)
    df_new["NORD"] = df_new["NORD"].str.replace(",",".").astype(float)
    df_new["Data"] = date.values
    conn_sql = sqlite3.connect(r"C:\Users\Sviluppo_Software_ZG\Desktop\PortaleMonitoraggio\Produzione\db.sqlite3")
    df_new.to_sql("Prezzi energia", conn_sql, if_exists="append", index=False)
    conn_sql.close()
    conn.close()

    return df_new

def calcola_medie_mensili():
    conn_sql = sqlite3.connect(r"C:\Users\Sviluppo_Software_ZG\Desktop\PortaleMonitoraggio\Produzione\db.sqlite3")
    df2 = pd.read_sql("SELECT * FROM 'Prezzi energia'", conn_sql)

    t = pd.to_datetime(df2["Data"])

    t_on = t[0]
    now = datetime.now()

    timestamp = []
    mean_puns = []

    t_out = datetime(now.year, now.month, 1)
    while t_on < t_out:
        if t_on.month == 12:
            t_off = datetime(t_on.year + 1, 1, 1)
        else:
            t_off = datetime(t_on.year, t_on.month + 1, 1) 
    
        timestamp.append(t_on)
        mean_pun = np.mean(df2["PUN"][(t>=t_on) & (t<t_off)])
        mean_puns.append(mean_pun)

        t_on = t_off

    df = pd.DataFrame({
        "timestamp": timestamp,
        "mean_puns": mean_puns
    })

    conn_sql = sqlite3.connect(r"C:\Users\Sviluppo_Software_ZG\Desktop\PortaleMonitoraggio\Produzione\db.sqlite3")
    df.to_sql("Prezzi medi mensili", conn_sql, if_exists="append", index=False)
    conn_sql.close()

def read_last_file():
    download_dir = "gme_downloads"
    if not os.path.exists(download_dir):
        print("Cartella downloads non trovata.")
        return []
    
    files = os.listdir(download_dir)
    files = [f for f in files if os.path.isfile(os.path.join(download_dir, f))]
    files.sort()  # ordine alfabetico crescente
    t_on_str = files[-1]

    return datetime.strptime(t_on_str[:8], "%Y%m%d")
    

def aggiorna_tabelle_pun():
    print("Aggiornando le tabelle del PUN")
    conn = connect_to_ftp()
    t_on = read_last_file()
    # t_on = datetime(2021, 1, 1)
    # t_on = datetime(2025, 1, 1)

    scarica_file_00(conn, t_on)
    calcola_medie_mensili()
    
    print("Tabelle del PUN aggiornate.")
