import boto3
import json
import time

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123"
)

try:
    s3.head_bucket(Bucket="bronze")
except:
    s3.create_bucket(Bucket="bronze")


def upload(data):

    s3.put_object(
        Bucket="bronze",
        Key=f"bronze_{int(time.time()*1000)}.json",
        Body=json.dumps(data)
    )