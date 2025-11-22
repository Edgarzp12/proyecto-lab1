from pymongo import MongoClient
from config import MONGO_HOST, MONGO_PORT

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client["ecommerce"]

pipeline = [
    {"$group": {"_id": "$brand", "revenue": {"$sum": "$price"}}},
    {"$sort": {"revenue": -1}},
    {"$limit": 1}
]

res = list(db.transactions.aggregate(pipeline))
print("Marca con más ingresos:", res)

