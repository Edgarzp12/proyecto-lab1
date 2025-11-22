import pandas as pd
from pymongo import MongoClient
from config import MONGO_HOST, MONGO_PORT

# Cargar CSV
df = pd.read_csv("data/dataset.csv")

# Conexión a Mongo
client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client["ecommerce"]
collection = db["transactions"]

# Convertir a diccionarios
data_dict = df.to_dict("records")

# Insertar
result = collection.insert_many(data_dict)

print(f"Insertados {len(result.inserted_ids)} documentos.")
