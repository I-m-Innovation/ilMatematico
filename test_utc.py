from datetime import datetime

# Ottieni l'ora attuale in formato ISO 8601
current_time_iso = datetime.utcnow().isoformat()
print(current_time_iso)
