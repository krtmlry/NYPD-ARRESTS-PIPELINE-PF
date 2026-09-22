import os
from botocore.exceptions import ClientError

"""
To check if a certain file exists in the destination bucket.
"""

def file_exists(s3, key:str)-> bool:
    bucket = os.getenv('bucket')

    if key.startswith("s3://"):
        key = key.split(f"s3://{bucket}/", 1)[1]

    try:
        s3.head_object(
            Bucket=bucket,
            Key=key
        )
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
