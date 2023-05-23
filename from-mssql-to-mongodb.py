# Luis Contreras
# May 23rd, 2023

import pyodbc
from pymongo import MongoClient

# Configuration for Microsoft SQL Server connection
sql_server_connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=replicadb;UID=sa;PWD=Sql2022"
sql_server_conn = pyodbc.connect(sql_server_connection_string)
sql_server_cursor = sql_server_conn.cursor()

# Configuration for MongoDB connection
mongo_client = MongoClient("mongodb://192.168.68.91:27017/")
mongo_db = mongo_client["demo"]

# Replicate the database and specific tables
def replicate_database():
    # Replicate the database if it doesn't exist
    if not schema_exists("replicadb"):
        replicate_schema("replicadb")

    # Replicate specific tables if they don't exist
    if not collection_exists("datos1"):
        replicate_table("replicadb", "dbo.datos1")

    if not collection_exists("datos2"):
        replicate_table("replicadb", "dbo.datos2")

# Check if a specific schema (database) exists in MongoDB
def schema_exists(schema_name):
    return schema_name in mongo_db.list_collection_names()

# Check if a specific collection exists in MongoDB
def collection_exists(collection_name):
    return collection_name in mongo_db.list_collection_names()

# Replicate a specific schema (database)
def replicate_schema(schema_name):
    # No need to create a database in MongoDB as MongoDB doesn't use predefined schemas
    pass

# Replicate a specific table
def replicate_table(schema_name, table_name):
    # Get the data from the table in SQL Server
    sql_server_cursor.execute(f"SELECT * FROM {schema_name}.{table_name};")
    rows = sql_server_cursor.fetchall()

    # Insert the data into the MongoDB collection
    collection = mongo_db[table_name]
    for row in rows:
        document = {}
        for index, value in enumerate(row):
            column_name = sql_server_cursor.description[index][0]
            document[column_name] = value
        collection.insert_one(document)
        print("Replicated record:", document)

# Call the main function to replicate the database and specific tables
replicate_database()

# Close the connections
sql_server_cursor.close()
sql_server_conn.close()
mongo_client.close()
