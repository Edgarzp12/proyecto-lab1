"""
Consulta en Redis:
Cuenta todas las categorías y determina cuál aparece más veces.
"""

import redis
from config import REDIS_HOST, REDIS_PORT

# 1. Conexión a Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

categorias = {}

# 2. Iterar sobre todas las ventas en Redis
for key in r.scan_iter("transaction:*"):

    data = r.hgetall(key)

    categoria = data.get("category_code")

    # Ignorar valores vacíos o "unknown"
    if not categoria or categoria.lower() == "unknown":
        continue

    categorias[categoria] = categorias.get(categoria, 0) + 1

# 3. Mostrar resultado
if categorias:
    categoria_top = max(categorias, key=categorias.get)
    total = categorias[categoria_top]
    print(f"La categoría más vendida fue '{categoria_top}' con {total} ventas.")
else:
    print("No se encontraron categorías.")
