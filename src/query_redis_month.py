"""
Consulta en Redis:
Determina el mes con más ventas (YYYY-MM).
"""

import redis
import json
from config import REDIS_HOST, REDIS_PORT
from datetime import datetime

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

meses = {}

# Recorrer todas las ventas
for key in r.scan_iter("sale:*"):

    data = json.loads(r.get(key))

    event_time = data.get("event_time")
    if not event_time:
        continue

    # Convertir string a datetime
    fecha = datetime.strptime(event_time, "%Y-%m-%d %H:%M:%S %Z")

    month = fecha.strftime("%Y-%m")

    meses[month] = meses.get(month, 0) + 1

# Determinar el mes top
if meses:
    mes_top = max(meses, key=meses.get)
    total = meses[mes_top]
    print(f"El mes con más ventas fue {mes_top} con {total} ventas.")
else:
    print("No se encontraron meses.")
