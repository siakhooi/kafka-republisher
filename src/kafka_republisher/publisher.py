import time
from kafka_republisher.config import RepublisherConfig


def delayed_publish(producer, config: RepublisherConfig, key, value):
    """Publish a message after a configured delay.

    Args:
        producer: Kafka producer instance
        config: RepublisherConfig with to_topic and sleep_time
        key: Message key
        value: Message value
    """
    time.sleep(config.sleep_time)
    producer.produce(config.to_topic, key=key, value=value)
    producer.flush()
    print(f"Republished to {config.to_topic}: {value}")
