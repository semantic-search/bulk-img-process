from pathlib import Path
from kafka import KafkaProducer
import json
import base64
import sys


producer = KafkaProducer(
    bootstrap_servers=["40.88.35.171:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

TOPIC_NAME = "MAX_AUDIO"

if len(sys.argv) > 1:
    TOPIC_NAME = sys.argv[1]

print(f"topic name : {TOPIC_NAME}")

audios_path = list(Path('audios').glob('*.*'))

audios_name = [p.name for p in audios_path]


for idx, image in enumerate(audios_path):

    with open(image, "rb") as image_file:
        encoded_image = base64.b64encode(image_file.read()).decode('ascii')
    # send to consumer
    message = {
        'audio_id': audios_name[idx],
        'data': encoded_image
    }
    print('SENDING : ', audios_name[idx])
    producer.send(TOPIC_NAME, value=message)
    producer.flush()
