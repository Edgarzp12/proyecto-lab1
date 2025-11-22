import redis
from config import REDIS_HOST, REDIS_PORT
from collections import defaultdict

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

revenue = defaultdict(float)
keys = r.keys("transaction:*")

for key in keys:
    brand = r.hget(key, "brand")
    price = float(r.hget(key, "price"))
    revenue[brand] += price

top = max(revenue.items(), key=lambda x: x[1])
print("Marca con más ingresos:", top)
