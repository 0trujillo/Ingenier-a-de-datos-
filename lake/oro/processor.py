import json
import time
import requests
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123"
)

try:
    s3.head_bucket(Bucket="oro")
except:
    s3.create_bucket(Bucket="oro")


def enrich(record):

    response = requests.post(
        "http://ml-api:8000/predict",
        json={
            "temp": record["temperature"],
            "humidity": record["humidity"],
            "wind": record["wind_speed"]
        }
    )

    prediction = response.json()

    final_record = {
        **record,
        "risk_score": prediction["risk_score"],
        "risk_level": prediction["risk_level"]
    }

    s3.put_object(
        Bucket="oro",
        Key=f"gold_{int(time.time()*1000)}.json",
        Body=json.dumps(final_record)
    )

    return final_record