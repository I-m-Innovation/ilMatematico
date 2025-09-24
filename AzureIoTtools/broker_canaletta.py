import sqlite3
import json
from azure.eventhub import EventHubConsumerClient

# 🔹 CONFIGURAZIONE
EVENTHUB_CONNECTION_STR = "Endpoint=sb://ihsuprodparres005dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=CYDA25asU2InqmVB9TFIEDjeYQv25RPNdAIoTIQahoY=;EntityPath=iothub-ehub-canaletta-55484935-b4d61d764c"
EVENTHUB_CONSUMER_GROUP = "$Default"

DB_NAME = "messages_canaletta.db"

# 🔹 CREA IL DB SQLITE
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS measurements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT,
        group_name TEXT,
        parent_timestamp INTEGER,
        parent_timestampMsec INTEGER,
        measure_name TEXT,
        raw_data REAL,
        status INTEGER,
        measure_timestamp INTEGER,
        measure_timestampMsec INTEGER
    )
    """)
    # 🔹 Aggiungo un vincolo unico
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_measure
    ON measurements (device_id, measure_name, measure_timestampMsec)
    """)
    conn.commit()
    conn.close()

# 🔹 CALLBACK PER OGNI MESSAGGIO
def on_event(partition_context, event):
    try:
        payload = json.loads(event.body_as_str())
    except json.JSONDecodeError:
        print("Messaggio non in formato JSON valido")
        return

    parent_timestamp = payload.get("timestamp")
    parent_timestampMsec = payload.get("timestampMsec")
    group_name = payload.get("group_name")
    values = payload.get("values", {})

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    new_rows = 0

    for device_id, measures in values.items():
        for measure_name, measure_data in measures.items():
            raw_data = measure_data.get("raw_data")
            status = measure_data.get("status")
            measure_timestamp = measure_data.get("timestamp")
            measure_timestampMsec = measure_data.get("timestampMsec")

            cursor.execute("""
                INSERT OR IGNORE INTO measurements (
                    device_id, group_name, parent_timestamp, parent_timestampMsec,
                    measure_name, raw_data, status, measure_timestamp, measure_timestampMsec
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                device_id, group_name, parent_timestamp, parent_timestampMsec,
                measure_name, raw_data, status, measure_timestamp, measure_timestampMsec
            ))

            if cursor.rowcount > 0:  # se è stato inserito
                new_rows += 1

    conn.commit()
    conn.close()

    # checkpoint
    partition_context.update_checkpoint(event)
    print(f"✅ Inserite {new_rows} nuove righe (su {len(values)} device)")

# 🔹 MAIN
def main():
    init_db()
    client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STR,
        consumer_group=EVENTHUB_CONSUMER_GROUP
    )

    print("In ascolto dei messaggi IoT Hub... (CTRL+C per uscire)")
    try:
        with client:
            client.receive(
                on_event=on_event,
                # starting_position="-1"
            )
    except KeyboardInterrupt:
        print("Interrotto dall'utente.")

if __name__ == "__main__":
    main()
