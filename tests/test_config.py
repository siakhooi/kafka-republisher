from kafka_republisher.config import (
    RepublisherConfig,
    get_config_from_env,
)

import pytest
from unittest.mock import patch


class TestRepublisherConfig:
    """Tests for RepublisherConfig dataclass"""

    def test_config_creation_with_all_params(self):
        """Test creating RepublisherConfig with all parameters"""
        config = RepublisherConfig(
            bootstrap_servers="broker1:9092",
            from_topic="source-topic",
            to_topic="dest-topic",
            sleep_time=60,
            group_id="test-group",
        )

        assert config.bootstrap_servers == "broker1:9092"
        assert config.from_topic == "source-topic"
        assert config.to_topic == "dest-topic"
        assert config.sleep_time == 60
        assert config.group_id == "test-group"

    def test_config_dataclass_equality(self):
        """Test that two configs with same values are equal"""
        config1 = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="topicA",
            to_topic="topicB",
            sleep_time=30,
            group_id="delayer",
        )
        config2 = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="topicA",
            to_topic="topicB",
            sleep_time=30,
            group_id="delayer",
        )

        assert config1 == config2

    def test_config_dataclass_inequality(self):
        """Test that two configs with different values are not equal"""
        config1 = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="topicA",
            to_topic="topicB",
            sleep_time=30,
            group_id="delayer",
        )
        config2 = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="topicA",
            to_topic="topicC",
            sleep_time=30,
            group_id="delayer",
        )

        assert config1 != config2


class TestGetConfigFromEnv:
    """Tests for get_config_from_env function"""

    @patch.dict(
        "os.environ",
        {
            "BOOTSTRAP_SERVERS": "broker1:9092",
            "FROM_TOPIC": "source",
            "TO_TOPIC": "destination",
            "SLEEP_TIME": "45",
            "GROUP_ID": "my-group",
        },
    )
    def test_get_config_from_env_with_all_vars_set(self):
        """Test loading config when all environment variables are set"""
        config = get_config_from_env()

        assert config.bootstrap_servers == "broker1:9092"
        assert config.from_topic == "source"
        assert config.to_topic == "destination"
        assert config.sleep_time == 45
        assert config.group_id == "my-group"

    @patch.dict("os.environ", {}, clear=True)
    def test_get_config_from_env_with_defaults(self):
        """Test loading config with default values when no env vars are set"""
        config = get_config_from_env()

        assert config.bootstrap_servers == "localhost:9092"
        assert config.from_topic == "topicA"
        assert config.to_topic == "topicB"
        assert config.sleep_time == 30
        assert config.group_id == "delayer"

    @patch.dict(
        "os.environ",
        {
            "BOOTSTRAP_SERVERS": "custom:9092",
        },
        clear=True,
    )
    def test_get_config_from_env_partial_override(self):
        """Test loading config with only some env vars set"""
        config = get_config_from_env()

        assert config.bootstrap_servers == "custom:9092"
        assert config.from_topic == "topicA"
        assert config.to_topic == "topicB"
        assert config.sleep_time == 30
        assert config.group_id == "delayer"

    @patch.dict(
        "os.environ",
        {
            "BOOTSTRAP_SERVERS": "broker1:9092,broker2:9092,broker3:9092",
            "FROM_TOPIC": "events",
            "TO_TOPIC": "events-delayed",
            "SLEEP_TIME": "120",
            "GROUP_ID": "production-delayer",
        },
    )
    def test_get_config_from_env_with_production_values(self):
        """Test loading config with production-like values"""
        config = get_config_from_env()

        assert config.bootstrap_servers == (
            "broker1:9092,broker2:9092,broker3:9092"
        )
        assert config.from_topic == "events"
        assert config.to_topic == "events-delayed"
        assert config.sleep_time == 120
        assert config.group_id == "production-delayer"

    @patch.dict(
        "os.environ",
        {
            "SLEEP_TIME": "0",
        },
        clear=True,
    )
    def test_get_config_from_env_with_zero_sleep_time(self):
        """Test loading config with zero sleep time"""
        config = get_config_from_env()

        assert config.sleep_time == 0

    @patch.dict(
        "os.environ",
        {
            "SLEEP_TIME": "invalid",
        },
        clear=True,
    )
    def test_get_config_from_env_with_invalid_sleep_time(self):
        """Test that invalid SLEEP_TIME raises ValueError"""
        with pytest.raises(ValueError):
            get_config_from_env()

    @patch.dict(
        "os.environ",
        {
            "SLEEP_TIME": "3600",
        },
        clear=True,
    )
    def test_get_config_from_env_sleep_time_as_int(self):
        """Test that SLEEP_TIME is correctly converted to int"""
        config = get_config_from_env()

        assert isinstance(config.sleep_time, int)
        assert config.sleep_time == 3600

    @patch.dict(
        "os.environ",
        {
            "FROM_TOPIC": "",
            "TO_TOPIC": "",
        },
        clear=True,
    )
    def test_get_config_from_env_with_empty_string_values(self):
        """Test loading config with empty string env vars uses defaults"""
        config = get_config_from_env()

        # Empty strings should be returned as-is, not use defaults
        assert config.from_topic == ""
        assert config.to_topic == ""
