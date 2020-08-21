from pathlib import Path
from kafka import KafkaProducer
import json
import base64
import sys

from topics import topicsList

producer = KafkaProducer(
    bootstrap_servers=["40.88.35.171:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

TOPIC_NAME = "EASY_OCR"

topic = sys.argv[1]

if topic:
    TOPIC_NAME = topic

print(f"topic name : {TOPIC_NAME}")

images_path = list(Path('images').glob('*.*'))

images_name = [p.name for p in images_path]


for idx, image in enumerate(images_path):

    with open(image, "rb") as image_file:
        encoded_image = base64.b64encode(image_file.read()).decode('ascii')
    # send to consumer
    message = {
        'image_id': images_name[idx],
        'data': encoded_image
    }

    producer.send(TOPIC_NAME, value=message)
    producer.flush()
