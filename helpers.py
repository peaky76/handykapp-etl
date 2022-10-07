from do_client import client
from requests import get


def download_file(url):
    response = get(url)
    response.raise_for_status()
    return response.content


def write_file(content, filename):
    client.put_object(Bucket='peaky76',
                      Key=filename,
                      Body=content,
                      ACL='private')

    # with open(filename, 'wb+') as f:
    #     f.write(content)
