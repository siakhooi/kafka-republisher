import os
import time
import threading
from kafka_republisher.kafka_client import get_consumer, get_producer


def run():
    bootstrap = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    from_topic = os.getenv("FROM_TOPIC", "topicA")
    to_topic = os.getenv("TO_TOPIC", "topicB")
    sleep_time = int(os.getenv("SLEEP_TIME", "30"))
    group_id = os.getenv("GROUP_ID", "delayer")

    print("Starting Kafka Delayer (parallel mode)")
    print(f"  Bootstrap: {bootstrap}")
    print(f"  From: {from_topic}")
    print(f"  To: {to_topic}")
    print(f"  Delay: {sleep_time}s")

    consumer = get_consumer(bootstrap, group_id)
    producer = get_producer(bootstrap)

    consumer.subscribe([from_topic])

    def delayed_publish(key, value):
        time.sleep(sleep_time)
        producer.produce(to_topic, key=key, value=value)
        producer.flush()
        print(f"Republished to {to_topic}: {value}")

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
            print(f"Received: {value}, scheduling publish in {sleep_time}s")

            threading.Thread(
                target=delayed_publish,
                args=(key, value),
                daemon=True
            ).start()

    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()
