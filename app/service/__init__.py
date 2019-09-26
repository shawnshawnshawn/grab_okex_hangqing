from app import settings
from pymongo import MongoClient

mongodb = None
ma = None

# set MongoDB configuration
if settings.DATABASE_SERVER_ADDRESS and settings.DATABASE_SERVER_PORT:
    conn = MongoClient(settings.DATABASE_SERVER_ADDRESS, settings.DATABASE_SERVER_PORT)
    if settings.DATABASE_SERVER_USERNAME and settings.DATABASE_SERVER_PASSWORD:
        db_auth = conn.prophet
        db_auth.authenticate(settings.DATABASE_SERVER_USERNAME, settings.DATABASE_SERVER_PASSWORD)
    mongodb = conn.get_database(settings.DATABASE_NAME)
