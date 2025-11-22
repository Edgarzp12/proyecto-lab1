"""
Consulta en Redis:
Cuenta todas las categorías y determina cuál aparece más veces.
"""

import redis
import json
from config import REDIS_HOST, REDIS_PORT

# 1. Conexión al servidor Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Diccionario para contar ventas por categoría
categorias = {}

# 2. Iterar sobre todas las claves (cada row)
for key in r.scan_iter("sale:*"):

    # Recuperar JSON de Redis
    data = json.loads(r.get(key))

    categoria = data.get("category_code")

    # Ignorar valores vacíos
    if categoria is None:
        continue

    # Contar categoría
    categorias[categoria] = categorias.get(categoria, 0) + 1

# 3. Determinar la categoría top
if categorias:
    categoria_top = max(categorias, key=categorias.get)
    total = categorias[categoria_top]
    print(f"La categoría más vendida fue '{categoria_top}' con {total} ventas.")
else:
    print("No se encontraron categorías.")

