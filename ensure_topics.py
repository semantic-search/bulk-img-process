from kafka.admin import KafkaAdminClient, NewTopic
import kafka

admin_client = KafkaAdminClient(
    bootstrap_servers=["40.88.35.171:9092"], 
    client_id='admin-test'
)
consumer = kafka.KafkaConsumer(group_id='my-group', bootstrap_servers=['40.88.35.171:9092'])
topics_kafka = consumer.topics()

# no space in topic names, but can use - . _
all_topics = {"EASY_OCR", "DENSECAP", "KERAS_OCR", "TESSER_OCR", "MAX_CAPTION", "NEURAL_TALK"} 


topics_to_create = all_topics - topics_kafka 

print(f"Topics to ensure : {all_topics}")
print(f"Current Topics in kafka : {topics_kafka}")
print(f"Topics to create : {topics_to_create}")

topic_list = []
for topic in topics_to_create:
    print('creating topic : ' + topic)
    topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))

if topic_list:
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except Exception as e:
        print(e)
    final_topics = consumer.topics()
    print(f'final topics list : {final_topics}')
