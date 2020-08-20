from kafka import KafkaConsumer
from json import loads
from base64 import decodestring
import base64
from pathlib import Path

from topics import topicsList


# TOPIC = topicsList['DENSECAP']['name']
TOPIC = "DENSECAP"
print(TOPIC)
consumer_densecap = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["40.88.35.171:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: loads(x.decode("utf-8")),
)


for message in consumer_densecap:
    message = message.value
    print("MESSAGE RECEIVED DENSECAP: ")
    image_id = message['image_id']

    folder_path = "images/DENSECAP/"
    Path(folder_path).mkdir(parents=True, exist_ok=True)

    data = message['data']

    with open(folder_path+image_id, "wb") as fh:
        fh.write(base64.b64decode(data.encode("ascii")))

