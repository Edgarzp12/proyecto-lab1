"""
Consulta en MongoDB:
Obtiene la marca que generó más ingresos (suma total de price).
"""

from pymongo import MongoClient
from config import MONGO_HOST, MONGO_PORT

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client["ecommerce"]
collection = db["orders"]

# Pipeline:
# 1. Filtrar marcas válidas (brand != None)
# 2. Agrupar por marca y sumar el price (ingresos)
# 3. Ordenar descendentemente
# 4. Limitar a 1 marca
pipeline = [
    { "$match": { "brand": { "$ne": None } } },
    { "$group": { "_id": "$brand", "revenue": { "$sum": "$price" } }},
    { "$sort": { "revenue": -1 }},
    { "$limit": 1 }
]

res = list(collection.aggregate(pipeline))

if res:
    marca = res[0]["_id"]
    ingresos = round(res[0]["revenue"], 2)
    print(f"La marca con más ingresos fue '{marca}' con ${ingresos}.")
else:
    print("No se encontraron marcas.")

