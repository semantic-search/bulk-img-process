from Util.Azure import getData

import sys
from pathlib import Path

from kafka import KafkaConsumer
from json import loads

# Script to for consuming topic, default set, pass arg to change
# usage : python consumer.py "EASY_OCR"
TOPIC_NAME = "EASY_OCR"
CONTAINER_NAME = "test"

TOPIC_NAME = sys.argv[1] if len(sys.argv) > 1 else TOPIC_NAME
CONTAINER_NAME = sys.argv[2] if len(sys.argv) > 2 else TOPIC_NAME

print(f"topic name : {TOPIC_NAME}")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=["40.88.35.171:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)


def truncated(data):
    data = (data[:40] + '..') if len(data) > 40 else data
    return data


for message in consumer:
    message = message.value
    print(f"MESSAGE RECEIVED")
    print(f"{truncated(str(message))}")

    container_name = message['container_name']
    blob_name = message['blob_name']

    blob_data = getData(container_name, blob_name)

    folder_path = "data/" + container_name 
    Path(folder_path).mkdir(parents=True, exist_ok=True)


    with open(folder_path+blob_name, "wb") as fh:
        fh.write(blob_data)



