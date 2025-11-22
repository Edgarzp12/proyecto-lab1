import pandas as pd
import redis
from config import REDIS_HOST, REDIS_PORT

df = pd.read_csv("data/dataset.csv")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

for _, row in df.iterrows():
    key = f"transaction:{row['order_id']}:{row['product_id']}"
    r.hset(key, mapping={
        "event_time": row["event_time"],
        "category_code": row["category_code"],
        "brand": row["brand"],
        "price": row["price"],
        "user_id": row["user_id"]
    })

print("Datos cargados en Redis:", len(df))
