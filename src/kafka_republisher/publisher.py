import logging
import time
from kafka_republisher.config import RepublisherConfig

logger = logging.getLogger(__name__)


def delayed_publish(producer, config: RepublisherConfig, key, value):
    """Publish a message after a configured delay.

    Args:
        producer: Kafka producer instance
        config: RepublisherConfig with to_topic and sleep_time
        key: Message key
        value: Message value
    """
    logger.debug(
        f"Delaying publish for {config.sleep_time}s to topic {config.to_topic}"
    )
    time.sleep(config.sleep_time)
    try:
        producer.produce(config.to_topic, key=key, value=value)
        producer.flush()
        logger.info(f"Republished to {config.to_topic}: {value}")
    except Exception as e:
        logger.error(f"Failed to republish to {config.to_topic}: {e}")
