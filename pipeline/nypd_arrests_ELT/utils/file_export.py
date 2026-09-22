import os
import io
import pandas as pd

"""
To export raw file as parquet in s3(RustFS).
"""

def export_file(s3, df:pd.DataFrame, key:str):
    bucket = os.getenv("bucket")
    if bucket is None:
        bucket = os.getenv('bucket')

    if df.empty:
        raise ValueError(
            f"Cannot save empty Dataframe."
        )

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', compression='zstd')
    buffer.seek(0)

    s3.put_object(
        Bucket=os.getenv('bucket'),
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    print(f"Uploaded s3://{bucket}/{key}")
