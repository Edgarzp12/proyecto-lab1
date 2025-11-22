from pymongo import MongoClient

print("Intentando conectar a MongoDB...")

client = MongoClient("mongodb://localhost:27017/")
print("Conexión exitosa. Bases de datos existentes:")
print(client.list_database_names())
