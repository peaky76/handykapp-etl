import csv
import json

import pendulum
from bson import ObjectId
from requests import get

from src.clients import mongo_client
from src.clients import spaces_client as client

db = mongo_client.handykapp

BUCKET_NAME = "peaky76"


def fetch_content(url, params=None, headers=None):
    response = get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.content


def get_files(dirname, modified_after=None):
    continuation_token = ""
    while True:
        response = client.list_objects_v2(
            Bucket=BUCKET_NAME, Prefix=dirname, ContinuationToken=continuation_token
        )
        files = response.get("Contents", [])
        continuation_token = response.get("NextContinuationToken")

        def within_date(x):
            return modified_after is None or x.get("LastModified") > modified_after

        yield from [
            key
            for file in files
            if "." in (key := file.get("Key")) and within_date(file)
        ]
        if not response.get("IsTruncated"):
            break


def read_file(file_path):
    file_type = file_path.split(".")[-1]
    output = {
        "csv": lambda x: list(csv.reader(x.splitlines())),
        "json": lambda x: json.loads(x),
    }
    stream = stream_file(file_path).decode("utf-8")
    return output[file_type](stream)


def stream_file(file_path):
    obj = client.get_object(Bucket=BUCKET_NAME, Key=file_path)
    return obj["Body"].read()


def write_file(content, filename):
    client.put_object(Bucket=BUCKET_NAME, Key=filename, Body=content, ACL="private")


def get_last_occurrence_of(weekday):
    return pendulum.now().add(days=1).previous(weekday).date()


def get_race(race_id):
    pipeline = [
        {"$match": {"_id": ObjectId(race_id)}},
        {"$lookup": {
            "from": "racecourses",
            "localField": "racecourse",
            "foreignField": "_id",
            "as": "racecourse"
        }},
        {"$unwind": "$racecourse"},
        {"$addFields": {"racecourse": "$racecourse.name"}},
        {"$unwind": "$runners"},
        {"$lookup": {
            "from": "horses",
            "localField": "runners.horse",
            "foreignField": "_id",
            "as": "runners.horse"
        }},
        {"$unwind": "$runners.horse"},
        {"$addFields": {"runners.horse": "$runners.horse.name"}},
        {"$group": {
            "_id": "$_id",
            "racecourse": {"$first": "$racecourse"},
            "runners": {"$push": "$runners.horse"}
        }}
    ]

    races = db.races.aggregate(pipeline)
    for race in races:
        print(race)


