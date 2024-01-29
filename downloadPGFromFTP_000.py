from ftplib import FTP
import pandas as pd
from datetime import datetime
import pytz
from dateutil import tz
import os
import shutil


def sistemaCartellaFTPPG():

    #   Entro nella cartella FTP
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/ponte_giurino')

    Files = ftp.nlst()

    for File in range(len(Files)):
        currFile = Files[File]

        if currFile[0:4] == "DATI":
            #   apro i file
            gFile = open('DBPGNEW.csv', "wb")
            ftp.retrbinary('RETR DBPGNEW.csv', gFile.write)
            gFile.close()

            DB = pd.read_csv("DBPGNEW.csv", on_bad_lines='skip', header='infer', delimiter=',')
            gFile = open(currFile, "wb")
            ftp.retrbinary('RETR '+currFile, gFile.write)
            gFile.close()

            newLine = pd.read_csv(currFile, on_bad_lines='skip', header='infer', delimiter=';')
            DataNewLine = newLine.iloc[1, :]

            newTime = DataNewLine.iloc[0]
            test = datetime.strptime(newTime, '%d/%m/%Y %H:%M:%S')
            test2 = datetime(test.year, test.month, test.day, test.hour, test.minute, test.second,
                             tzinfo=pytz.utc)
            to_zone = tz.tzlocal()
            central = test2.astimezone(to_zone)

            newTimeLoc = datetime(central.year, central.month, central.day, central.hour,
                                  central.minute, central.second)
            newTimeLoc = datetime.strftime(newTimeLoc, format="%d/%m/%Y %H:%M:%S")

            newPT_Linea = float(DataNewLine.iloc[1].replace(",", "."))
            newPT_Turbina = float(DataNewLine.iloc[2].replace(",", "."))
            newPotAtt = float(DataNewLine.iloc[3].replace(",", "."))
            newPort = float(DataNewLine.iloc[4].replace(",", "."))
            newCosPhi = float(DataNewLine.iloc[5].replace(",", "."))
            newLevStram = float(DataNewLine.iloc[6].replace(",", "."))

            NewLine = pd.DataFrame({'x__TimeStamp': [newTime], 'Local': [newTimeLoc],
                                    'PLC1_AI_PT_LINEA': [newPT_Linea], 'PLC1_AI_PT_TURBINA': [newPT_Turbina],
                                    'PLC1_AI_POT_ATTIVA': [newPotAtt], 'PLC1_AI_FT_PORT_IST': [newPort],
                                    'PLC1_AI_COSPHI': [newCosPhi], 'PLC1_AI_LT_STRAMAZZO': [newLevStram]})

            newDB = pd.concat([DB, NewLine], ignore_index=True)
            newDB.to_csv("DBPGNEW.csv", index=False)

            #   Salvo il nuovo database
            File = open("DBPGNEW.csv", "rb")
            ftp.storbinary(f"STOR DBPGNEW.csv", File)
            ftp.delete(currFile)

            thisFolder = os.getcwd()
            source = thisFolder + "\\" + currFile
            destination = thisFolder + "\\Database impianti\\Ponte Giurino\\Dati raw\\" + currFile
            shutil.move(source, destination)

    ftp.close()


def ScaricaDatiPG():

    # connetto a FTP e prelievo il file

    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/ponte_giurino')
    gFile = open("DBPGNEW.csv", "wb")
    ftp.retrbinary('RETR DBPGNEW.csv', gFile.write)
    gFile.close()
    df = pd.read_csv("DBPGNEW.csv", on_bad_lines='skip', header='infer', delimiter=',', low_memory=False)
    ftp.close()

    return df
