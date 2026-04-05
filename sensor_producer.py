import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from configs import kafka_config, building_sensors_topic


producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

sensor_id = random.randint(1000, 9999)
print(f"sensor id: {sensor_id}")

try:
    for _ in range(12):
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85)
        }
        producer.send(building_sensors_topic, value=data)
        producer.flush()
        print(f"Data was sent: {data}")
        time.sleep(5)
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()