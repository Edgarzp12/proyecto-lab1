import pandas as pd
import redis
from config import REDIS_HOST, REDIS_PORT

df = pd.read_csv("data/dataset.csv")

# Limpiar datos antes de enviar a Redis
df = df.fillna({
    "category_code": "unknown",
    "brand": "unknown",
    "price": 0,
    "event_time": "unknown",
    "user_id": "unknown"
})

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

count = 0
for _, row in df.iterrows():
    key = f"transaction:{row['order_id']}:{row['product_id']}"
    r.hset(key, mapping={
        "event_time": str(row["event_time"]),
        "category_code": str(row["category_code"]),
        "brand": str(row["brand"]),
        "price": float(row["price"]),
        "user_id": str(row["user_id"])
    })
    count += 1

print("Datos cargados en Redis:", count)
