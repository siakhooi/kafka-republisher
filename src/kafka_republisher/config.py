import os
from dataclasses import dataclass


@dataclass
class RepublisherConfig:
    """Configuration for Kafka Republisher"""
    bootstrap_servers: str
    from_topic: str
    to_topic: str
    sleep_time: int
    group_id: str


def get_config_from_env() -> RepublisherConfig:
    """Load configuration from environment variables"""
    return RepublisherConfig(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS", "localhost:9092"),
        from_topic=os.getenv("FROM_TOPIC", "topicA"),
        to_topic=os.getenv("TO_TOPIC", "topicB"),
        sleep_time=int(os.getenv("SLEEP_TIME", "30")),
        group_id=os.getenv("GROUP_ID", "delayer"),
    )
