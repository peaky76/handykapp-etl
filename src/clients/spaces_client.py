import asyncio
from typing import Optional

import boto3  # type: ignore
from prefect.blocks.system import Secret


class SpacesClient:
    _client: Optional[boto3.client] = None

    @classmethod
    async def _create(cls) -> boto3.client:
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

    @classmethod
    def get(cls) -> boto3.client:
        if cls._client is None:
            cls._client = asyncio.run(cls._create())
        return cls._client

