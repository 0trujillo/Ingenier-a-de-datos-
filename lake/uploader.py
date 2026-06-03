import boto3
import json
import time

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123"
)

def upload(data):
    s3.put_object(
        Bucket="bronze",
        Key=f"data_{int(time.time())}.json",
        Body=json.dumps(data)
    )