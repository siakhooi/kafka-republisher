from kafka_republisher.config import get_config_from_env, RepublisherConfig
from kafka_republisher.kafka_client import get_consumer, get_producer
from kafka_republisher.processor import process_message


def consume_and_process_loop(consumer, producer, config: RepublisherConfig):
    """Main consumption loop that polls and processes messages.

    Args:
        consumer: Kafka consumer instance
        producer: Kafka producer instance
        config: RepublisherConfig with configuration settings

    This function runs indefinitely until KeyboardInterrupt.
    """
    try:
        while True:
            msg = consumer.poll(1.0)
            process_message(msg, producer, config)
    except KeyboardInterrupt:
        print("Stopping...")


def run():
    config = get_config_from_env()

    print("Starting Kafka Delayer (parallel mode)")
    print(f"  Bootstrap: {config.bootstrap_servers}")
    print(f"  From: {config.from_topic}")
    print(f"  To: {config.to_topic}")
    print(f"  Delay: {config.sleep_time}s")

    consumer = get_consumer(config.bootstrap_servers, config.group_id)
    producer = get_producer(config.bootstrap_servers)

    consumer.subscribe([config.from_topic])

    try:
        consume_and_process_loop(consumer, producer, config)
    finally:
        consumer.close()
