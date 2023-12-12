# import pymongo
from pymongo import MongoClient

def get_mongodb_db():
    client = MongoClient('localhost', 27017)  # start connection to mongodb server
    db = client["chicago-neighborhoods-database"]  # create/access database
    neighborhoods = db["neighborhoods-collection"]  # create a collection in the database
    return db
