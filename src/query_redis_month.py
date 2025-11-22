import redis
from config import REDIS_HOST, REDIS_PORT
from collections import Counter

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

counter = Counter()
keys = r.keys("transaction:*")

for key in keys:
    date = r.hget(key, "event_time")
    month = date[:7]  # YYYY-MM
    counter[month] += 1

print(counter.most_common(1))
