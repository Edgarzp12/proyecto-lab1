from pymongo import MongoClient
from config import MONGO_HOST, MONGO_PORT

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client["ecommerce"]

pipeline = [
    {"$project": {"month": {"$substr": ["$event_time", 0, 7]}}},
    {"$group": {"_id": "$month", "total": {"$sum": 1}}},
    {"$sort": {"total": -1}},
    {"$limit": 1}
]

res = list(db.transactions.aggregate(pipeline))
print("Mes con más ventas:", res)
