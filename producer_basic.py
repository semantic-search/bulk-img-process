from kafka import KafkaProducer
import json

#TOPIC_NAME = "IMAGE_RESULTS"
#TOPIC_NAME = "EASY_OCR"
# TOPIC_NAME = "DENSECAP"

ALL_TOPICS = ["EASY_OCR", "DENSECAP"]

producer = KafkaProducer(
    bootstrap_servers=["40.88.35.171:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

# future = producer.send(TOPIC_NAME, value="Some message to kafka topic")
# result = future.get(timeout=60)
# print(result)

for topic in ALL_TOPICS:
    future = producer.send(topic, value=f"Some message to kafka topic {topic}")
    result = future.get(timeout=60)
    print(result)