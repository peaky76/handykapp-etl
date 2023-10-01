from clients import mongo_client as client
from prefect import task
from pymongo import ASCENDING as ASC

db = client.handykapp


@task
def drop_database():
    client.drop_database("handykapp")


@task
def spec_database():
    db.horses.create_index(
        [("name", ASC), ("country", ASC), ("year", ASC)], unique=True
    )
    db.horses.create_index("name")
    db.people.create_index(
        [("last", ASC), ("first", ASC), ("middle", ASC)], unique=True
    )
    db.racecourses.create_index([("name", ASC), ("country", ASC)], unique=True)
    db.races.create_index([("course", ASC), ("date", ASC), ("time", ASC)], unique=True)
    db.races.create_index("result.horse")
