"""
Consulta en MongoDB:
Obtiene la categoría más vendida (mayor número de documentos).
"""

from pymongo import MongoClient
from config import MONGO_HOST, MONGO_PORT

# 1. Conexión a Mongo
client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client["ecommerce"]
collection = db["orders"]

# 2. Pipeline de agregación
#    - Agrupa por categoría
#    - Cuenta cuántas ventas hay por categoría
#    - Ordena de mayor a menor
#    - Devuelve solo la primera (la más vendida)
pipeline = [
    { "$group": { "_id": "$category_code", "count": { "$sum": 1 } }},
    { "$sort": { "count": -1 }},
    { "$limit": 1 }
]

# 3. Ejecutar consulta
res = list(collection.aggregate(pipeline))

# 4. Imprimir resultado en formato amigable
if res:
    categoria = res[0]["_id"]
    cantidad = res[0]["count"]
    print(f"La categoría más vendida fue '{categoria}' con {cantidad} ventas.")
else:
    print("No se encontraron resultados.")
