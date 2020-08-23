## USAGE : python send_to_process.py "TESSER_OCR" "images"

from Util.Azure import blobs
import sys
from kafka import KafkaProducer
import json

TOPIC_NAME = "EASY_OCR"
CONTAINER_NAME = "test"

TOPIC_NAME = sys.argv[1] if len(sys.argv) > 1 else TOPIC_NAME
CONTAINER_NAME = sys.argv[2] if len(sys.argv) > 2 else CONTAINER_NAME

print(f"topic name : {TOPIC_NAME}")

producer = KafkaProducer(
    bootstrap_servers=["40.88.35.171:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


for blob in blobs(CONTAINER_NAME):
    print(blob.name)
    message = {
        'container_name': CONTAINER_NAME,
        'blob_name': blob.name
    }
    producer.send(TOPIC_NAME, value=message)
    producer.flush()
