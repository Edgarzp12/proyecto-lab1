from pymongo import MongoClient
from config import MONGO_HOST, MONGO_PORT

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client["ecommerce"]

pipeline = [
    {"$group": {"_id": "$category_code", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
]

res = list(db.transactions.aggregate(pipeline))
print("Categoría más vendida:", res)
