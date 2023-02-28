import boto3
from prefect.blocks.system import Secret


session = boto3.session.Session()
spaces_client = session.client(
    "s3",
    region_name="ams3",
    endpoint_url="https://ams3.digitaloceanspaces.com",
    aws_access_key_id=Secret.load("spaces-key").get(),
    aws_secret_access_key=Secret.load("spaces-secret").get(),
)
