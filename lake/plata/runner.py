import json
import boto3

from limpieza import (
    process_record,
    upload_to_silver
)

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123"
)

processed = set()

print("Silver Runner iniciado")

while True:

    response = s3.list_objects_v2(
        Bucket="bronze"
    )

    if "Contents" not in response:
        continue

    for obj in response["Contents"]:

        key = obj["Key"]

        if key in processed:
            continue

        file = s3.get_object(
            Bucket="bronze",
            Key=key
        )

        data = json.loads(
            file["Body"].read()
        )

        clean_data = process_record(
            data
        )

        if clean_data:

            upload_to_silver(
                clean_data
            )

            print(
                f"Procesado -> {key}"
            )

        processed.add(key)