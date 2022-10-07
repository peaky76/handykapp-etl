from requests import get

def download_file(url):
    response = get(url)
    response.raise_for_status()
    return response.content


def write_file(content, filename):
    with open(filename, 'wb+') as f:
        f.write(content)