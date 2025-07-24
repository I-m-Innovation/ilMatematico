# prendo il DB
# seleziono i dati dalle mezzanotte del giorno corrente
# chiamo il file backup_yyyyMMDD.csv
# lo salvo nella sotto cartella di impianto backups
from datetime import datetime
import pandas as pd
import shutil
import os


def salva_backup(plant):

    if plant == "TF":
        DB_file_path = "DBTFNEW5.csv"
        DB = pd.read_csv(DB_file_path)
        t = pd.to_datetime(DB.Local, format='mixed')
        destinazione = "Database impianti/Torrino Foresta/backups"
    elif plant == "PG":
        DB_file_path = "DBPGNEW.csv"
        DB = pd.read_csv(DB_file_path)
        t = pd.to_datetime(DB.Local, format='%d/%m/%Y %H:%M:%S')
        destinazione = "Database impianti/Ponte Giurino/backups"
    elif plant == "SA3":
        DB_file_path = "DBSA3.csv"
        DB = pd.read_csv(DB_file_path)
        t = pd.to_datetime(DB.t, format='mixed')
        destinazione = "Database impianti/SA3/backups"

    Now = datetime.today()
    today = datetime(Now. year, Now.month, Now.day)
    sub_db = DB[t >= today]

    new_file_name = f"backup_{today.strftime('%Y%m%d')}"
    sub_db.to_csv(new_file_name, index=False)
    origine = os.path.join(os.getcwd(), new_file_name)
    shutil.move(origine, destinazione+"/"+new_file_name)

