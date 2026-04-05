from kafka_republisher.publisher import delayed_publish
from kafka_republisher.config import RepublisherConfig

from unittest.mock import Mock, patch, call


class TestDelayedPublish:
    """Tests for delayed_publish function"""

    def test_delayed_publish_sleeps_for_configured_time(self):
        """Test that delayed_publish sleeps for the configured duration"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=45,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep") as mock_sleep:
            delayed_publish(mock_producer, config, "key1", "value1")
            mock_sleep.assert_called_once_with(45)

    def test_delayed_publish_produces_message_with_correct_params(self):
        """Test that delayed_publish produces message with correct params"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep"):
            delayed_publish(mock_producer, config, "key1", "value1")

            mock_producer.produce.assert_called_once_with(
                "destination", key="key1", value="value1"
            )

    def test_delayed_publish_flushes_producer(self):
        """Test that delayed_publish flushes the producer after producing"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep"):
            delayed_publish(mock_producer, config, "key1", "value1")

            mock_producer.flush.assert_called_once()

    def test_delayed_publish_prints_correct_message(self, caplog):
        """Test that delayed_publish prints the republished message"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep"):
            with caplog.at_level("INFO"):
                delayed_publish(mock_producer, config, "key1", "value1")

            assert "Republished to destination: value1" in caplog.text

    def test_delayed_publish_with_none_key(self):
        """Test delayed_publish works with None key"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep"):
            delayed_publish(mock_producer, config, None, "value1")

            mock_producer.produce.assert_called_once_with(
                "destination", key=None, value="value1"
            )

    def test_delayed_publish_with_zero_sleep_time(self):
        """Test delayed_publish with zero sleep time"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=0,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep") as mock_sleep:
            delayed_publish(mock_producer, config, "key1", "value1")

            mock_sleep.assert_called_once_with(0)
            mock_producer.produce.assert_called_once()

    def test_delayed_publish_call_order(self):
        """Test that sleep happens before produce and flush"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep") as mock_sleep:
            delayed_publish(mock_producer, config, "key1", "value1")

            # Verify order: sleep -> produce -> flush
            expected_calls = [
                call.produce("destination", key="key1", value="value1"),
                call.flush(),
            ]
            assert mock_producer.mock_calls == expected_calls
            # Sleep should be called before any producer methods
            mock_sleep.assert_called_once_with(30)

    def test_delayed_publish_with_different_topics(self, caplog):
        """Test delayed_publish with various topic names"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="events",
            to_topic="events-delayed",
            sleep_time=60,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep"):
            with caplog.at_level("INFO"):
                delayed_publish(mock_producer, config, "key1", "test-value")

            mock_producer.produce.assert_called_once_with(
                "events-delayed", key="key1", value="test-value"
            )
            assert "Republished to events-delayed: test-value" in caplog.text

    def test_delayed_publish_with_empty_value(self):
        """Test delayed_publish with empty string value"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep"):
            delayed_publish(mock_producer, config, "key1", "")

            mock_producer.produce.assert_called_once_with(
                "destination", key="key1", value=""
            )

    def test_delayed_publish_with_long_sleep_time(self):
        """Test delayed_publish with large sleep time"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=3600,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep") as mock_sleep:
            delayed_publish(mock_producer, config, "key1", "value1")

            mock_sleep.assert_called_once_with(3600)
            mock_producer.produce.assert_called_once()
            mock_producer.flush.assert_called_once()

    def test_delayed_publish_logs_error_on_produce_failure(self, caplog):
        """Test that produce exception is caught and logged"""
        mock_producer = Mock()
        mock_producer.produce.side_effect = Exception("Broker unavailable")
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep"):
            with caplog.at_level("ERROR"):
                delayed_publish(mock_producer, config, "key1", "value1")

        assert "Failed to republish to destination" in caplog.text
        assert "Broker unavailable" in caplog.text

    def test_delayed_publish_logs_error_on_flush_failure(self, caplog):
        """Test that flush exception is caught and logged"""
        mock_producer = Mock()
        mock_producer.flush.side_effect = Exception("Flush timeout")
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        with patch("kafka_republisher.publisher.time.sleep"):
            with caplog.at_level("ERROR"):
                delayed_publish(mock_producer, config, "key1", "value1")

        assert "Failed to republish to destination" in caplog.text
        assert "Flush timeout" in caplog.text
