import signal
import threading
from kafka_republisher.config import get_config_from_env, RepublisherConfig
from kafka_republisher.kafka_client import get_consumer, get_producer
from kafka_republisher.processor import process_message


def consume_and_process_loop(
    consumer, producer, config: RepublisherConfig, shutdown_event
):
    """Main consumption loop that polls and processes messages.

    Args:
        consumer: Kafka consumer instance
        producer: Kafka producer instance
        config: RepublisherConfig with configuration settings
        shutdown_event: threading.Event to signal shutdown

    This function runs until shutdown_event is set.
    """
    try:
        while not shutdown_event.is_set():
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

    shutdown_event = threading.Event()

    def handle_signal(signum, frame):
        print(f"Received signal {signum}, shutting down gracefully...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        consume_and_process_loop(consumer, producer, config, shutdown_event)
    finally:
        consumer.close()
