"""
Consulta en Redis:
Suma ingresos por marca y determina cuál generó más dinero.
"""

import redis
from config import REDIS_HOST, REDIS_PORT

# Conexión a Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

marcas = {}

# Recorrer todos los hashes con formato "transaction:*"
for key in r.scan_iter("transaction:*"):

    data = r.hgetall(key)

    marca = data.get("brand")
    price = data.get("price")

    # Validaciones básicas
    if not marca or marca == "unknown":
        continue

    try:
        price = float(price)
    except (TypeError, ValueError):
        continue

    # Acumular ingresos por marca
    marcas[marca] = marcas.get(marca, 0.0) + price

# Resultados
if marcas:
    marca_top = max(marcas, key=marcas.get)
    ingresos = round(marcas[marca_top], 2)
    print(f"La marca con más ingresos fue '{marca_top}' con ${ingresos}.")
else:
    print("No se encontraron marcas.")
