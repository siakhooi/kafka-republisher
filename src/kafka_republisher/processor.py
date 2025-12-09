import threading
from kafka_republisher.config import RepublisherConfig
from kafka_republisher.publisher import delayed_publish


def process_message(msg, producer, config: RepublisherConfig):
    """Process a single Kafka message and schedule delayed republish.

    Args:
        msg: Kafka message from consumer.poll()
        producer: Kafka producer instance
        config: RepublisherConfig with topic and timing settings

    Returns:
        bool: True if message was processed, False if skipped
    """
    if msg is None:
        return False

    if msg.error():
        print("Consumer error:", msg.error())
        return False

    value = msg.value().decode('utf-8')
    key = msg.key().decode('utf-8') if msg.key() else None
    print(
        f"Received: {value}, scheduling publish in "
        f"{config.sleep_time}s"
    )

    threading.Thread(
        target=delayed_publish,
        args=(producer, config, key, value),
        daemon=True
    ).start()

    return True
