"""
Consulta en MongoDB:
Determina el mes con más ventas (event_time ya está en formato datetime).
"""

from pymongo import MongoClient
from config import MONGO_HOST, MONGO_PORT

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client["ecommerce"]
collection = db["orders"]

# Pipeline:
# 1. Proyectar un nuevo campo "month" con formato YYYY-MM
# 2. Agrupar por mes y sumar ventas
# 3. Ordenar descendentemente
# 4. Tomar el primer resultado
pipeline = [
    { "$project": { 
        "month": { "$dateToString": { "format": "%Y-%m", "date": "$event_time" }}
    }},
    { "$group": { "_id": "$month", "total": { "$sum": 1 } }},
    { "$sort": { "total": -1 }},
    { "$limit": 1 }
]

res = list(collection.aggregate(pipeline))

if res:
    mes = res[0]["_id"]
    total = res[0]["total"]
    print(f"El mes con más ventas fue {mes} con {total} ventas.")
else:
    print("No se encontraron meses.")


