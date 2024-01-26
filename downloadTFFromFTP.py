from ftplib import FTP
import pandas as pd
from datetime import datetime
import numpy as np
import pytz
from dateutil import tz
import shutil
import os


def sistemaCartellaFTP_TF(Plant):

    if Plant == "TF":
        DBileName = "DBTFNEW4.csv"

        # Entro nella cartella FTP
        ftp = FTP("192.168.10.211", timeout=120)
        ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
        ftp.cwd('/dati/Torrino_Foresta')

        Files = ftp.nlst()

        for File in range(len(Files)):

            try:
                currFile = Files[File]

                if currFile[0:4] == "DATI":
                    #   apro i file
                    gFile = open(DBileName, "wb")
                    ftp.retrbinary("RETR " + DBileName, gFile.write)
                    gFile.close()

                    DB = pd.read_csv(DBileName, on_bad_lines='skip', header='infer', delimiter=',')
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
                    newPortata1600 = float(DataNewLine.iloc[7].replace(",", "."))
                    newPressioneUscita = float(DataNewLine.iloc[8].replace(",", "."))
                    newLevScarico = float(DataNewLine.iloc[9].replace(",", "."))

                    NewLine = pd.DataFrame({'x__TimeStamp': [newTime], 'Local': [newTimeLoc],
                                            'PLC1_AI_PT_LINEA': [newPT_Linea],
                                            'PLC1_AI_PT_TURBINA': [newPT_Turbina], 'PLC1_AI_POT_ATTIVA': [newPotAtt],
                                            'PLC1_AI_FT_PORT_IST': [newPort], 'PLC1_AI_COSPHI': [newCosPhi],
                                            'PLC1_AI_LT_STRAMAZZO': [newLevStram],
                                            'PLC1_AI_FT_PORT_COND': [newPortata1600],
                                            'PLC1_AI_PT_TURBINA_OUT': [newPressioneUscita],
                                            'PLC1_AI_LT_SCAR_C': [newLevScarico]})
                    newDB = pd.concat([DB, NewLine], ignore_index=True)
                    newDB.to_csv(DBileName, index=False)

                    #   Salvo il nuovo database
                    File = open(DBileName, "rb")
                    ftp.storbinary(f"STOR " + DBileName, File)
                    ftp.delete(currFile)

                    thisFolder = os.getcwd()
                    source = thisFolder + "\\" + currFile
                    destination = thisFolder + "\\Database impianti\\Torrino Foresta\\Dati raw\\" + currFile
                    # destination = destFolder + currFile
                    shutil.move(source, destination)

            except Exception as err:
                print(err)

        ftp.close()


def ScaricaDatiTF():

    # print("Sono entrato in scaricaDatiPG")
    # connetto a FTP e prelievo il file
    ftp = FTP("192.168.10.211", timeout=120)
    ftp.login('ftpdaticentzilio', 'Sd2PqAS.We8zBK')
    ftp.cwd('/dati/Torrino_Foresta')
    gFile = open("DBTFNEW4.csv", "wb")
    ftp.retrbinary('RETR DBTFNEW4.csv', gFile.write)
    gFile.close()
    ftp.close()

    df = pd.read_csv("DBTFNEW4.csv", on_bad_lines='skip', header='infer', delimiter=',', low_memory=False)

    # Selezione delle variabili
    TempiString = df['Local']
    Tempi = TempiString
    Tempi = np.array(Tempi)  # in UTC
    TimeQ = pd.to_datetime(Tempi, format='mixed')
    
    ValQ = df['PLC1_AI_FT_PORT_IST']

    ValP = df['PLC1_AI_POT_ATTIVA']

    ValHMonte = df['PLC1_AI_PT_TURBINA']

    ValHValle = df['PLC1_AI_PT_TURBINA_OUT']
    ValHValle = ValHValle.fillna(ValHValle.mean())
    ValH = ValHMonte - ValHValle

    OutVal = np.empty([len(ValH), 1])

    for i in range(len(ValH)):
        try:
            OutVal[i] = float(ValH[i])  # *10.1974
        except Exception as err:
            print(err)
            OutVal[i] = float('nan')

    ValH = ValH * 10.1974

    data = {"Time": TimeQ, "Charge": ValQ, "Power": ValP, "Jump": ValH}

    return data
