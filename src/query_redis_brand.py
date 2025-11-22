"""
Consulta en Redis:
Suma ingresos por marca y determina cuál generó más dinero.
"""

import redis
import json
from config import REDIS_HOST, REDIS_PORT

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

marcas = {}

# Recorrer las ventas almacenadas
for key in r.scan_iter("sale:*"):

    data = json.loads(r.get(key))
    marca = data.get("brand")
    price = data.get("price", 0)

    if marca is None:
        continue

    marcas[marca] = marcas.get(marca, 0) + float(price)

# Evaluar resultados
if marcas:
    marca_top = max(marcas, key=marcas.get)
    ingresos = round(marcas[marca_top], 2)
    print(f"La marca con más ingresos fue '{marca_top}' con ${ingresos}.")
else:
    print("No se encontraron marcas.")

