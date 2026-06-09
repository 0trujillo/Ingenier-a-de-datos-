import json
import boto3

from processor import enrich

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123"
)

processed = set()

print("Gold Runner iniciado")

while True:

    response = s3.list_objects_v2(
        Bucket="plata"
    )

    if "Contents" not in response:
        continue

    for obj in response["Contents"]:

        key = obj["Key"]

        if key in processed:
            continue

        file = s3.get_object(
            Bucket="plata",
            Key=key
        )

        data = json.loads(
            file["Body"].read()
        )

        enrich(data)

        print(
            f"Enriquecido -> {key}"
        )

        processed.add(key)