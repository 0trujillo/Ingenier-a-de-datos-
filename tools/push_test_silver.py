import boto3, json, time, os

MINIO_ENDPOINT_URL = os.getenv('MINIO_ENDPOINT_URL', 'http://minio:9000')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'admin123')
PLATA_BUCKET = os.getenv('PLATA_BUCKET', 'plata')

s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT_URL, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

record = {
    "event_id": f"test-{int(time.time())}",
    "source": "manual-test",
    "temperature": 50.0,
    "humidity": 10.0,
    "wind_speed": 30.0,
    "timestamp": time.time()
}

key = f"manual_test_{int(time.time()*1000)}.json"
print('Uploading to', PLATA_BUCKET, 'key=', key)
s3.put_object(Bucket=PLATA_BUCKET, Key=key, Body=json.dumps(record))
print('Uploaded')
