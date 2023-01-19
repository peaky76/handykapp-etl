from digital_ocean_client import client
from json import loads
from requests import get

BUCKET_NAME = 'peaky76'


def fetch_content(url, params=None, headers=None):
    response = get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.content


def get_files(dirname, modified_after=None):
    response = client.list_objects(Bucket=BUCKET_NAME,
                                   Prefix=dirname)
    files = response.get('Contents', [])

    if modified_after:
        files = [f for f in files if f.get('LastModified') > modified_after]

    return [key for file in files if '.' in (key := file.get('Key'))]


def read_json(file):
    obj = client.get_object(Bucket=BUCKET_NAME, Key=file)
    return loads(obj['Body'].read())


def write_file(content, filename):
    client.put_object(Bucket=BUCKET_NAME,
                      Key=filename,
                      Body=content,
                      ACL='private')
