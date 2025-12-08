from kafka_republisher.kafka_client import get_consumer, get_producer

import pytest
from unittest.mock import Mock, patch
from confluent_kafka import Consumer, Producer


class TestGetProducer:
    """Tests for get_producer function"""

    @patch("kafka_republisher.kafka_client.Producer")
    def test_get_producer_creates_producer_with_correct_config(
        self, mock_producer_class
    ):
        """Test that get_producer creates a Producer with the correct
        bootstrap servers"""
        bootstrap_servers = "localhost:9092"
        mock_producer_instance = Mock(spec=Producer)
        mock_producer_class.return_value = mock_producer_instance

        result = get_producer(bootstrap_servers)

        mock_producer_class.assert_called_once_with(
            {"bootstrap.servers": bootstrap_servers}
        )
        assert result == mock_producer_instance

    @patch("kafka_republisher.kafka_client.Producer")
    def test_get_producer_with_multiple_brokers(self, mock_producer_class):
        """Test get_producer with multiple broker addresses"""
        bootstrap_servers = "broker1:9092,broker2:9092,broker3:9092"
        mock_producer_instance = Mock(spec=Producer)
        mock_producer_class.return_value = mock_producer_instance

        result = get_producer(bootstrap_servers)

        mock_producer_class.assert_called_once_with(
            {"bootstrap.servers": bootstrap_servers}
        )
        assert result == mock_producer_instance

    @patch("kafka_republisher.kafka_client.Producer")
    def test_get_producer_returns_producer_instance(self, mock_producer_class):
        """Test that get_producer returns a Producer instance"""
        bootstrap_servers = "localhost:9092"
        mock_producer_instance = Mock(spec=Producer)
        mock_producer_class.return_value = mock_producer_instance

        result = get_producer(bootstrap_servers)

        assert result is not None
        assert result == mock_producer_instance


class TestGetConsumer:
    """Tests for get_consumer function"""

    @patch("kafka_republisher.kafka_client.Consumer")
    def test_get_consumer_creates_consumer_with_default_config(
        self, mock_consumer_class
    ):
        """Test that get_consumer creates a Consumer with default
        auto_offset_reset"""
        bootstrap_servers = "localhost:9092"
        group_id = "test-group"
        mock_consumer_instance = Mock(spec=Consumer)
        mock_consumer_class.return_value = mock_consumer_instance

        result = get_consumer(bootstrap_servers, group_id)

        mock_consumer_class.assert_called_once_with(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
            }
        )
        assert result == mock_consumer_instance

    @patch("kafka_republisher.kafka_client.Consumer")
    def test_get_consumer_with_custom_auto_offset_reset(
        self, mock_consumer_class
    ):
        """Test get_consumer with custom auto_offset_reset parameter"""
        bootstrap_servers = "localhost:9092"
        group_id = "test-group"
        auto_offset_reset = "latest"
        mock_consumer_instance = Mock(spec=Consumer)
        mock_consumer_class.return_value = mock_consumer_instance

        result = get_consumer(bootstrap_servers, group_id, auto_offset_reset)

        mock_consumer_class.assert_called_once_with(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
            }
        )
        assert result == mock_consumer_instance

    @patch("kafka_republisher.kafka_client.Consumer")
    def test_get_consumer_with_multiple_brokers(self, mock_consumer_class):
        """Test get_consumer with multiple broker addresses"""
        bootstrap_servers = "broker1:9092,broker2:9092,broker3:9092"
        group_id = "multi-broker-group"
        mock_consumer_instance = Mock(spec=Consumer)
        mock_consumer_class.return_value = mock_consumer_instance

        result = get_consumer(bootstrap_servers, group_id)

        mock_consumer_class.assert_called_once_with(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
            }
        )
        assert result == mock_consumer_instance

    @patch("kafka_republisher.kafka_client.Consumer")
    def test_get_consumer_returns_consumer_instance(self, mock_consumer_class):
        """Test that get_consumer returns a Consumer instance"""
        bootstrap_servers = "localhost:9092"
        group_id = "test-group"
        mock_consumer_instance = Mock(spec=Consumer)
        mock_consumer_class.return_value = mock_consumer_instance

        result = get_consumer(bootstrap_servers, group_id)

        assert result is not None
        assert result == mock_consumer_instance

    @patch("kafka_republisher.kafka_client.Consumer")
    @pytest.mark.parametrize(
        "auto_offset_reset",
        ["earliest", "latest", "none"],
    )
    def test_get_consumer_with_various_offset_reset_values(
        self, mock_consumer_class, auto_offset_reset
    ):
        """Test get_consumer with various auto_offset_reset values"""
        bootstrap_servers = "localhost:9092"
        group_id = "test-group"
        mock_consumer_instance = Mock(spec=Consumer)
        mock_consumer_class.return_value = mock_consumer_instance

        result = get_consumer(bootstrap_servers, group_id, auto_offset_reset)

        mock_consumer_class.assert_called_once_with(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
            }
        )
        assert result == mock_consumer_instance

    @patch("kafka_republisher.kafka_client.Consumer")
    def test_get_consumer_with_different_group_ids(self, mock_consumer_class):
        """Test get_consumer with different group IDs"""
        bootstrap_servers = "localhost:9092"
        group_id = "unique-consumer-group-123"
        mock_consumer_instance = Mock(spec=Consumer)
        mock_consumer_class.return_value = mock_consumer_instance

        result = get_consumer(bootstrap_servers, group_id)

        mock_consumer_class.assert_called_once_with(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
            }
        )
        assert result == mock_consumer_instance
