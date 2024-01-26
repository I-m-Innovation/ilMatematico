import time
import pandas as pd


Plants = pd.read_excel('Documentazione/Lista impianti.xlsx') # indicizzazione in 'Documentazione/Lista impianti.xlsx'

currPlantIndex = 1
NumberOfPlants = len(Plants)


def readFromHigeco(Tag):

    if Tag == "SCN":
        FilePath = "Documentazione/Chiavi API/API SCN.xlsx"

    keyTab = pd.read_excel(FilePath)




def check_and_refreshDB(Plant):

    # identifico il provider dei dati
    Provider = Plant["Provider monitoraggio"]
    Tag = Plant["Tag"]

    if Provider == "Higeco":
        lastVals = readFromHigeco(Tag, "lastValues")

    elif Provider == "ACTECH":
        readFromACT(Tag)

    else:
        readfromCAP(Tag)

    # vado a leggere i dati



    # vado a controllare il file di input
        # con quale metodo?

def scanPlant(Plant):

    check_and_refreshDB(Plant)

    # leggo se ci sono nuovi dati
        # 1. Ci sono -> aggiorno il database

    # controllo lo stato dell'impianto
    # preparo i file da leggere


def main(Plant):

    scanPlant(Plant)


while True:

    currPlantTag = Plants["Tag"]
    Plant = Plants.iloc[currPlantIndex-1]
    Sleep_dt = 5 # seconds
    main(Plant)
    currPlantIndex = currPlantIndex % NumberOfPlants + 1 # impianto succesivo
    time.sleep(Sleep_dt)    # codice in pausa

