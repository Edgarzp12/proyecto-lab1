import pandas as pd
from pymongo import MongoClient
from config import MONGO_HOST, MONGO_PORT

# 1. Cargar el CSV
df = pd.read_csv("data/dataset.csv")

# 2. Limpieza de datos -------------------------------

# Reemplazar strings "nan" por None
df = df.replace("nan", None)

# Eliminar filas sin categoría
df = df[df["category_code"].notna()]

# Convertir event_time a datetime
df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

df = df[df["event_time"].notna()]

# 3. Convertir a diccionarios
data_dict = df.to_dict("records")

# 4. Conectar a Mongo
client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client["ecommerce"]

collection = db["orders"]
collection.drop()

# 5. Insertar datos limpios
result = collection.insert_many(data_dict)
print(f"Insertados {len(result.inserted_ids)} documentos limpios.")

# 6. Crear índices
collection.create_index({ "category_code": 1 })
collection.create_index({ "brand": 1 })
collection.create_index({ "event_time": 1 })

print("Carga + limpieza completada.")