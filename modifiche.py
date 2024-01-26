import csv
import json

def leggiDBSCN():
    
    input_file = csv.DictReader(open("DBSCN.csv"))
    for row in input_file:
        data = row
        # for column in row:
        #     data = column
    
    
    return data

def DBSCNInitialization(data):
    A=1
    #   Il codice ha lo scopo di ricostruire le timeseries relative a
    #   Timestamp, Irraggiamento, Potenza Inverter 1, Potenza Inverter 2, Temperatura dei moduli, Potenza Target
    