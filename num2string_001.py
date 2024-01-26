import math

# Funzione che converte i numeri in stringhe con separatori corretti


def convertNumber(Number, QuantityType, OutType, Plant):

    global OutNumber
    
    if QuantityType == "ERad":
        Number = Number

    if Number < 0:
        Number = -Number
        signChar = "-"
        signCol = "red"

    else:
        signChar = ""
        signCol = "black"

    flooredNumber = int(math.floor(Number))
    Ndigits = len(str(flooredNumber))

    if QuantityType != "Money" and QuantityType != "MoneySpeed" and QuantityType != "PUN":
        if Ndigits > 2:
            Number = int(round(Number, 0))

        elif Ndigits > 1:
            Number = int(round(Number, 1))

        else:
            Number = round(Number, 2)

    elif QuantityType == "PUN":
        Number = round(Number, 3)

    else:
        Number = round(Number, 2)

    decimalDigits = Number - flooredNumber

    if QuantityType == "Money" or QuantityType == "MoneySpeed":
        decimalDigits = str(round(decimalDigits, 2))

    elif QuantityType == "PUN":
        decimalDigits = str(round(decimalDigits, 3))

    else:
        if not isinstance(decimalDigits, int):
            isDec = decimalDigits.is_integer()

            if isDec:
                decimalDigits = str(round(decimalDigits, 1))

            else:
                isDec = (10*decimalDigits).is_integer()

                if isDec:
                    decimalDigits = str(round(decimalDigits, 1))

                else:
                    isDec = (100 * decimalDigits).is_integer()

                    if isDec:
                        decimalDigits = str(round(decimalDigits, 2))

                    else:
                        decimalDigits = str(round(decimalDigits, 3))

        else:
            decimalDigits = str(round(decimalDigits, 1))

    N = len(decimalDigits)
    decimalDigits = decimalDigits[2:N]
    flooredNumber = str(flooredNumber)

    Ndigits = len(flooredNumber)
    NumbOfThSep = int((Ndigits-1)/3)
    firstDigits = Ndigits-3*NumbOfThSep

    if firstDigits > 0:
        OutNumber = flooredNumber[0:firstDigits]

    start = firstDigits
    stop = firstDigits + 3
    
    for i in range(NumbOfThSep):
        toAdd = flooredNumber[start:stop]
        OutNumber = OutNumber+"."+toAdd
        start = start+3
        stop = stop + 3

    if decimalDigits != "":
        OutNumber = OutNumber+","+str(decimalDigits)

    if QuantityType == "Power":
        OutNumber = OutNumber+" kW"

    elif QuantityType == "Energy":
        OutNumber = OutNumber + " kWh"

    elif QuantityType == "Money":
        if OutType == "HTML":
            OutNumber = signChar+OutNumber + " €"

        else:
            OutNumber = signChar+OutNumber

    elif QuantityType == "MoneySpeed":
        if OutType == "HTML":
            OutNumber = signChar+OutNumber + " €/h"

        else:
            OutNumber = signChar+OutNumber
            
    elif QuantityType == "PUN":
        OutNumber = signChar + OutNumber

    elif QuantityType == "Perc":
        OutNumber = signChar + OutNumber + " %"

    elif QuantityType == "Charge":
        if Plant == "TF":
            if OutType == "HTML":
                OutNumber = OutNumber + " m"'<sup>'"3"'</sup>'"/s"

            else:
                OutNumber = OutNumber + " m\u00b3/s"

        else:
            OutNumber = OutNumber + " l/s"

    elif QuantityType == "Rad":
        if OutType == "HTML":
            OutNumber = OutNumber + " W/m"'<sup>'"2"'</sup>'

        else:
            OutNumber = OutNumber + " W/m\u00b2"

    elif QuantityType == "HEQ":
        OutNumber = OutNumber + " kWh/kW"

    elif QuantityType == "Temperature":
        OutNumber = OutNumber + " °C"
        
    else:
        if OutType == "HTML":
            OutNumber = OutNumber + " kWh/m"'<sup>'"2"'</sup>'

        else:
            OutNumber = OutNumber + " kWh/m\u00b2"

    return OutNumber, signCol
