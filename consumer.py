from kafka import KafkaConsumer
from json import loads

from topics import topicsList


EASY_OCR_TOPIC = topicsList['EASY_OCR']['name']
DENSECAP_TOPIC = topicsList['DENSECAP']['name']

consumer_easyocr = KafkaConsumer(
    EASY_OCR_TOPIC,
    bootstrap_servers=["40.88.35.171:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)

consumer_densecap = KafkaConsumer(
    DENSECAP_TOPIC,
    bootstrap_servers=["40.88.35.171:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)

for message in consumer_easyocr:
    message = message.value
    print("MESSAGE RECEIVED consumer_easyocr: ", message)


for message in consumer_densecap:
    message = message.value
    print("MESSAGE RECEIVED consumer_densecap: ", message)
