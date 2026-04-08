import json, boto3, os
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

s3        = boto3.client("s3")
BUCKET    = os.getenv("S3_BUCKET_NAME")
TOPIC     = os.getenv("KAFKA_TOPIC", "air-quality-readings")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "s3-consumer-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

def upload(record: dict):
    dt  = datetime.now(timezone.utc)
    key = (
        f"raw/{record['country']}/{dt.strftime('%Y/%m/%d')}/"
        f"{record['parameter']}_{record['location_id']}_{dt.strftime('%H%M%S%f')}.json"
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"[S3] {key}")

print(f"S3 consumer started — bucket: {BUCKET}")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[ERROR] {msg.error()}")
            continue
        record = json.loads(msg.value().decode("utf-8"))
        upload(record)
finally:
    consumer.close()
