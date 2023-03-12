import csv
import json
import pendulum
from prefect import get_run_logger
from clients import spaces_client as client
from requests import get

BUCKET_NAME = "peaky76"


def fetch_content(url, params=None, headers=None):
    response = get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.content


def get_all_files(dirname):
    continuation_token = None
    while True:
        response = client.list_objects_v2(
            Bucket=BUCKET_NAME, Prefix=dirname, ContinuationToken=continuation_token
        )
        files = response.get("Contents", [])
        continuation_token = response.get("NextContinuationToken")
        yield from [key for file in files if "." in (key := file.get("Key"))]
        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")


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
    stream = stream_file(file_path).decode("utf-8")
    return output[format](stream)


def stream_file(file_path):
    obj = client.get_object(Bucket=BUCKET_NAME, Key=file_path)
    return obj["Body"].read()


def write_file(content, filename):
    client.put_object(Bucket=BUCKET_NAME, Key=filename, Body=content, ACL="private")


def get_last_occurrence_of(weekday):
    return pendulum.now().add(days=1).previous(weekday).date()


def log_validation_problem(problem):
    msg = f"{problem['error']} in row {problem['row']} for {problem['field']}: {problem['value']}"
    logger = get_run_logger()
    logger.error(msg)
