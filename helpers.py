from do_client import client
from requests import get

BUCKET_NAME = 'peaky76'


def download_file(url, params=None, headers=None):
    response = get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.content


def get_files(dirname):
    response = client.list_objects(Bucket=BUCKET_NAME,
                                   Prefix=dirname)
    files = response.get('Contents', [])
    return [key for file in files if '.' in (key := file.get('Key'))]


def write_file(content, filename):
    client.put_object(Bucket=BUCKET_NAME,
                      Key=filename,
                      Body=content,
                      ACL='private')
