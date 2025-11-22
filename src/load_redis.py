import pandas as pd
import redis
from config import REDIS_HOST, REDIS_PORT

# ------------------------------------
# Cargar dataset
# ------------------------------------
df = pd.read_csv("data/dataset.csv")

# Limpiar NaN en columnas importantes
df = df.dropna(subset=["category_code", "brand", "price"])

# Convertir fechas a formato ISO 8601
df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
df = df.dropna(subset=["event_time"])
df["event_time"] = df["event_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

# ------------------------------------
# Conexión a Redis
# ------------------------------------
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Usar pipeline para mejorar el rendimiento
pipe = r.pipeline()

# ------------------------------------
# Insertar registros
# ------------------------------------
for _, row in df.iterrows():

    # Redis key con estructura lógica
    key = f"transaction:{row['order_id']}:{row['product_id']}"

    # Insertar como hash
    pipe.hset(key, mapping={
        "event_time": row["event_time"],
        "category_code": row["category_code"],
        "brand": row["brand"],
        "price": float(row["price"]),
        "user_id": str(row["user_id"])
    })

# Ejecutar pipeline (inserta miles de registros por lote)
pipe.execute()

print("Datos cargados en Redis:", len(df))
