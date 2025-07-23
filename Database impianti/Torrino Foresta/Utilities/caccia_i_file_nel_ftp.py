from ftplib import FTP
import os

# Configurazione
FTP_HOST = "192.168.10.211"
FTP_USER = "ftpdaticentzilio"
FTP_PASS = "Sd2PqAS.We8zBK"
LOCAL_DIR = r"C:\Users\Sviluppo_Software_ZG\Desktop\ilMatematico\GitHub\ilMatematico\Database impianti\Torrino Foresta\Utilities\Ripopola db\FIles"
REMOTE_DIR = "/dati/Torrino_Foresta"

# Connessione al server FTP
ftp = FTP(FTP_HOST)
ftp.login(FTP_USER, FTP_PASS)

# Cambia la directory remota
ftp.cwd(REMOTE_DIR)

# Carica tutti i file dalla cartella locale
for filename in os.listdir(LOCAL_DIR):
    local_path = os.path.join(LOCAL_DIR, filename)
    if os.path.isfile(local_path):
        with open(local_path, 'rb') as file:
            print(f"Caricamento di {filename}...")
            ftp.storbinary(f"STOR {filename}", file)

# Chiude la connessione
ftp.quit()
print("Tutti i file sono stati copiati con successo.")
