import os
import boto3
from botocore.client import Config

"""
Establishes connection to object storage using boto3.client()
"""

def get_s3_client(): #s3_client_conn
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("s3_endpoint"),
        aws_access_key_id=os.getenv("access_key"),
        aws_secret_access_key=os.getenv("access_secret_key"),
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"}
        )
    )
    return s3