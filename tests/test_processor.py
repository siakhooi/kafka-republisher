from kafka_republisher.processor import process_message
from kafka_republisher.config import RepublisherConfig

from unittest.mock import Mock, patch


class TestProcessMessage:
    """Tests for process_message function"""

    def test_process_message_with_none_returns_false(self):
        """Test that process_message returns False for None message"""
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        result = process_message(None, mock_producer, config)

        assert result is False
        mock_producer.produce.assert_not_called()

    def test_process_message_with_error_returns_false(self, capsys):
        """Test that process_message returns False for error message"""
        mock_msg = Mock()
        mock_msg.error.return_value = "Kafka error occurred"
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        result = process_message(mock_msg, mock_producer, config)

        assert result is False
        captured = capsys.readouterr()
        assert "Consumer error: Kafka error occurred" in captured.out
        mock_producer.produce.assert_not_called()

    @patch("kafka_republisher.processor.threading.Thread")
    def test_process_message_decodes_value_correctly(self, mock_thread):
        """Test that process_message decodes message value"""
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b"test-value"
        mock_msg.key.return_value = b"test-key"

        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        result = process_message(mock_msg, mock_producer, config)

        assert result is True
        mock_msg.value.assert_called_once()
        mock_msg.key.assert_called()

    @patch("kafka_republisher.processor.threading.Thread")
    def test_process_message_handles_none_key(self, mock_thread):
        """Test that process_message handles None key correctly"""
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b"test-value"
        mock_msg.key.return_value = None

        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        result = process_message(mock_msg, mock_producer, config)

        assert result is True
        mock_thread.assert_called_once()

    @patch("kafka_republisher.processor.threading.Thread")
    def test_process_message_starts_thread_with_correct_args(
        self, mock_thread
    ):
        """Test that process_message starts thread with correct arguments"""
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b"test-value"
        mock_msg.key.return_value = b"test-key"

        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=45,
            group_id="test-group",
        )

        mock_thread_instance = Mock()
        mock_thread.return_value = mock_thread_instance

        result = process_message(mock_msg, mock_producer, config)

        assert result is True
        mock_thread.assert_called_once()
        call_kwargs = mock_thread.call_args[1]
        assert "target" in call_kwargs
        assert "args" in call_kwargs
        assert call_kwargs["daemon"] is True
        # Check args passed to thread
        args = call_kwargs["args"]
        assert args[0] == mock_producer
        assert args[1] == config
        assert args[2] == "test-key"
        assert args[3] == "test-value"
        mock_thread_instance.start.assert_called_once()

    @patch("kafka_republisher.processor.threading.Thread")
    def test_process_message_prints_received_message(
        self, mock_thread, capsys
    ):
        """Test that process_message prints received message info"""
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b"my-message"
        mock_msg.key.return_value = b"my-key"

        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=60,
            group_id="test-group",
        )

        process_message(mock_msg, mock_producer, config)

        captured = capsys.readouterr()
        assert "Received: my-message" in captured.out
        assert "scheduling publish in 60s" in captured.out

    @patch("kafka_republisher.processor.threading.Thread")
    def test_process_message_creates_daemon_thread(self, mock_thread):
        """Test that process_message creates daemon thread"""
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b"test-value"
        mock_msg.key.return_value = b"test-key"

        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        process_message(mock_msg, mock_producer, config)

        mock_thread.assert_called_once()
        call_kwargs = mock_thread.call_args[1]
        assert call_kwargs["daemon"] is True

    @patch("kafka_republisher.processor.threading.Thread")
    def test_process_message_with_utf8_characters(self, mock_thread):
        """Test process_message handles UTF-8 characters correctly"""
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = "Hello 世界".encode('utf-8')
        mock_msg.key.return_value = "键".encode('utf-8')

        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        result = process_message(mock_msg, mock_producer, config)

        assert result is True
        call_kwargs = mock_thread.call_args[1]
        args = call_kwargs["args"]
        assert args[2] == "键"
        assert args[3] == "Hello 世界"

    @patch("kafka_republisher.processor.threading.Thread")
    def test_process_message_with_empty_value(self, mock_thread):
        """Test process_message with empty string value"""
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b""
        mock_msg.key.return_value = b"key1"

        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        result = process_message(mock_msg, mock_producer, config)

        assert result is True
        call_kwargs = mock_thread.call_args[1]
        args = call_kwargs["args"]
        assert args[3] == ""

    def test_process_message_error_does_not_start_thread(self):
        """Test that error messages don't start threads"""
        mock_msg = Mock()
        mock_msg.error.return_value = "Some error"

        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        with patch("kafka_republisher.processor.threading.Thread") as mt:
            result = process_message(mock_msg, mock_producer, config)

            assert result is False
            mt.assert_not_called()

    @patch("kafka_republisher.processor.threading.Thread")
    def test_process_message_returns_true_on_success(self, mock_thread):
        """Test that process_message returns True on successful processing"""
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b"value"
        mock_msg.key.return_value = b"key"

        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        result = process_message(mock_msg, mock_producer, config)

        assert result is True
