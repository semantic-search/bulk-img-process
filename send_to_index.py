from pathlib import Path
from kafka import KafkaProducer
import json
import base64

from topics import topicsList

producer = KafkaProducer(
    bootstrap_servers=["40.88.35.171:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

images_path = list(Path('images').glob('*.*'))

images_name = [ p.name for p in images_path]


for topic, topicDict in topicsList.items():
    endpoint, topicName = topicDict.items()
    endpoint = endpoint[1] 
    topicName = topicName[1]
    print(topicName, endpoint)

    for idx, image in enumerate(images_path):
        
        with open(image, "rb") as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode('ascii')
        #send to consumer
        message = {
            'image_id' : images_name[idx], 
             'data' : encoded_image
        }
        
        producer.send(topicName, value=message)
        producer.flush()
        