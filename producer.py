import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from configs import kafka_config


TOPIC_NAME = f"{kafka_config['my_id']}_building_sensors"
SENSOR_ID = random.randint(1000, 9999)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Запуск датчика ID: {SENSOR_ID}")
print(f"Відправка даних у топік: {TOPIC_NAME}")

try:
    while True:
        data = {
            "id": SENSOR_ID,
            "timestamp": int(datetime.now().timestamp()),
            "temperature": random.uniform(25, 45),
            "humidity": random.uniform(15, 85)
        }
        
        producer.send(TOPIC_NAME, value=data)
        print(f"Відправлено: {data}")
        time.sleep(2)

except KeyboardInterrupt:
    print(f"\nДатчик {SENSOR_ID} зупинено.")
finally:
    producer.close()