from db import get_client

def list_databases():
    client = get_client()
    return client.list_database_names()

def list_collections(db_name: str):
    client = get_client()
    return client[db_name].list_collection_names()
