import sys

from kafka import KafkaConsumer
from json import loads

# Script to for consuming topic, default set, pass arg to change
# usage : python consumer.py "EASY_OCR"
TOPIC_NAME = "IMAGE_RESULTS"

topic = sys.argv[1]


if topic:
    TOPIC_NAME = topic

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
    print(f"MESSAGE RECEIVED : {truncated(str(message))}")
