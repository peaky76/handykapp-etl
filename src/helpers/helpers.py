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
    logger.warning(msg)
