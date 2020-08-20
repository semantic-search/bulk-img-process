from kafka import KafkaConsumer
from json import loads

TOPIC_NAME = "IMAGE_RESULTS"
# TOPIC_NAME = "EASY_OCR"
#TOPIC_NAME = "DENSECAP"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=["40.88.35.171:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)

for message in consumer:
    message = message.value
    print("MESSAGE RECEIVED : ")
