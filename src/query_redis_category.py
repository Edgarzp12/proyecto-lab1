import redis
from config import REDIS_HOST, REDIS_PORT
from collections import Counter

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

keys = r.keys("transaction:*")
counter = Counter()

for key in keys:
    category = r.hget(key, "category_code")
    counter[category] += 1

print(counter.most_common(1))
