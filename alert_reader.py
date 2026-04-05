import json
from kafka import KafkaConsumer
from configs import kafka_config, alerts_topic


# consumer для читання алертів з Kafka
consumer = KafkaConsumer(
    alerts_topic,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="alina_n_hw06_alert_reader"
)

print(f"Reading messages from topic: {alerts_topic}")

try:
    for message in consumer:
        print("\nReceived alert:")
        print(message.value)
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()