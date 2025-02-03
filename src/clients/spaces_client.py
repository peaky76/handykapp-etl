import asyncio

import boto3  # type: ignore
from prefect.blocks.system import Secret


async def create_spaces_client() -> boto3.client:
    session = boto3.session.Session()
    spaces_key = await Secret.load("spaces-key")
    spaces_secret = await Secret.load("spaces-secret")
    return session.client(
        "s3",
        region_name="ams3",
        endpoint_url="https://ams3.digitaloceanspaces.com",
        aws_access_key_id=spaces_key.get(),
        aws_secret_access_key=spaces_secret.get(),
    )


spaces_client = asyncio.run(create_spaces_client())