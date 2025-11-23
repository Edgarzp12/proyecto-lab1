"""
Consulta en Redis:
Determina el mes con más ventas (YYYY-MM).
"""

import redis
from config import REDIS_HOST, REDIS_PORT
from datetime import datetime

# Conexión a Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

meses = {}

# Leer todas las claves
for key in r.scan_iter("transaction:*"):

    data = r.hgetall(key)

    event_time = data.get("event_time")
    if not event_time:
        continue

    # Formato ISO: "2020-08-15 05:46:01+00:00"
    try:
        fecha = datetime.fromisoformat(event_time)
    except ValueError:
        continue

    month = fecha.strftime("%Y-%m")

    meses[month] = meses.get(month, 0) + 1

# Resultado
if meses:
    mes_top = max(meses, key=meses.get)
    total = meses[mes_top]
    print(f"El mes con más ventas fue {mes_top} con {total} ventas.")
else:
    print("No se encontraron meses.")
