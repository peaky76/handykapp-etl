import ssl
from pymongo import MongoClient


# mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_client = MongoClient(host="172.17.0.1", port=27017)
