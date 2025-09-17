import sqlite3

# connessione al DB
conn = sqlite3.connect(r"C:\Users\Sviluppo_Software_ZG\Desktop\PortaleMonitoraggio\Produzione\db.sqlite3")
cur = conn.cursor()

# rimozione della tabella se esiste
cur.execute("DROP TABLE IF EXISTS 'Prezzi medi mensili'")
cur.execute("DROP TABLE IF EXISTS 'Prezzi energia'")

conn.commit()
conn.close()
