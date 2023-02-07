import boto3
# from dotenv import load_dotenv
# from os import getenv
from prefect.blocks.system import Secret


# load_dotenv()
# Secret(value=getenv('SPACES_KEY')).save('spaces-key', overwrite=True)
# Secret(value=getenv('SPACES_SECRET')).save('spaces-secret', overwrite=True)

session = boto3.session.Session()
client = session.client('s3',
                        region_name='ams3',
                        endpoint_url='https://ams3.digitaloceanspaces.com',
                        aws_access_key_id=Secret.load("spaces-key").get(),
                        aws_secret_access_key=Secret.load("spaces-secret").get())
