from kafka import KafkaConsumer
from json import loads
import base64
from pathlib import Path

from topics import topicsList


# TOPIC = topicsList['EASY_OCR']['name']
TOPIC = "EASY_OCR"

consumer_easyocr = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["40.88.35.171:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)


for message in consumer_easyocr:
    message = message.value
    print("MESSAGE RECEIVED consumer_easyocr: ")
    image_id = message['image_id']

    folder_path = "images/EASY_OCR/"
    Path(folder_path).mkdir(parents=True, exist_ok=True)

    data = message['data']

    with open(folder_path+image_id, "wb") as fh:
        fh.write(base64.b64decode(data.encode("ascii")))

