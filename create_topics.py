from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, building_sensors_topic, alerts_topic


admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"]
)

topics_to_create = [
    NewTopic(name=building_sensors_topic, num_partitions=1, replication_factor=1),
    NewTopic(name=alerts_topic, num_partitions=1, replication_factor=1),
]

try:
    admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
    print("Topics created.")
except Exception as e:
    print(f"Error: {e}")
finally:
    print("\nRelevant topics list:")
    for topic in admin_client.list_topics():
        if "alina_n" in topic:
            print(topic)
    admin_client.close()