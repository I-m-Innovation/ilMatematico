import shutil
import os


currFile = "DATI_202401231400.csv"

thisFolder = os.getcwd()
source = thisFolder + "\\" + currFile

destination = thisFolder + "\\Subfolder\\" + currFile
# destination = destFolder + currFile
shutil.move(source, destination)