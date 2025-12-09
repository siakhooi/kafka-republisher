import threading
from kafka_republisher.config import get_config_from_env
from kafka_republisher.kafka_client import get_consumer, get_producer
from kafka_republisher.publisher import delayed_publish


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
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

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

    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()
