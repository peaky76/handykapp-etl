import csv
import json
import pendulum
from clients import spaces_client as client
from requests import get

BUCKET_NAME = "peaky76"


def fetch_content(url, params=None, headers=None):
    response = get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.content


def get_files(dirname, modified_after=None):
    response = client.list_objects(Bucket=BUCKET_NAME, Prefix=dirname)
    files = response.get("Contents", [])

    if modified_after:
        files = [f for f in files if f.get("LastModified") > modified_after]

    return [key for file in files if "." in (key := file.get("Key"))]


def read_file(file_path):
    format = file_path.split(".")[-1]
    output = {
        "csv": lambda x: [row for row in csv.reader(x.splitlines())],
        "json": lambda x: json.loads(x),
    }

    return output[format](stream_file(file_path))


def stream_file(file_path):
    obj = client.get_object(Bucket=BUCKET_NAME, Key=file_path)
    return obj["Body"].read().decode("utf-8")


def write_file(content, filename):
    client.put_object(Bucket=BUCKET_NAME, Key=filename, Body=content, ACL="private")


def get_last_occurrence_of(weekday):
    return pendulum.now().add(days=1).previous(weekday)
