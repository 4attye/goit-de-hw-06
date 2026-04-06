from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from configs import kafka_config


my_id = kafka_config['my_id']

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

topic_list = [
    f"{my_id}_building_sensors",
    f"{my_id}_temperature_alerts",
    f"{my_id}_humidity_alerts",
    f"{my_id}_alerts"
]

new_topics = [
    NewTopic(name=topic, num_partitions=3, replication_factor=1) 
    for topic in topic_list
]

try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"Топіки успішно створені: {', '.join(topic_list)}")
except TopicAlreadyExistsError:
    print("Деякі топіки вже існують.")
except Exception as e:
    print(f"Виникла помилка: {e}")
finally:
    admin_client.close()