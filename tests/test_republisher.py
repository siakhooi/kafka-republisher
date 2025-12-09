from kafka_republisher.republisher import consume_and_process_loop
from kafka_republisher.config import RepublisherConfig

from unittest.mock import Mock, patch


class TestConsumeAndProcessLoop:
    """Tests for consume_and_process_loop function"""

    @patch("kafka_republisher.republisher.process_message")
    def test_consume_and_process_loop_calls_poll(self, mock_process):
        """Test that loop calls consumer.poll"""
        mock_consumer = Mock()
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        # Make poll return None, then raise KeyboardInterrupt
        mock_consumer.poll.side_effect = [None, KeyboardInterrupt()]

        consume_and_process_loop(mock_consumer, mock_producer, config)

        assert mock_consumer.poll.call_count == 2
        mock_consumer.poll.assert_called_with(1.0)

    @patch("kafka_republisher.republisher.process_message")
    def test_consume_and_process_loop_processes_messages(self, mock_process):
        """Test that loop passes messages to process_message"""
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_msg = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        # Return a message then interrupt
        mock_consumer.poll.side_effect = [mock_msg, KeyboardInterrupt()]

        consume_and_process_loop(mock_consumer, mock_producer, config)

        # Only 1 call (KeyboardInterrupt prevents 2nd process_message call)
        assert mock_process.call_count == 1
        mock_process.assert_called_once_with(mock_msg, mock_producer, config)

    @patch("kafka_republisher.republisher.process_message")
    def test_consume_and_process_loop_handles_keyboard_interrupt(
        self, mock_process, capsys
    ):
        """Test that loop handles KeyboardInterrupt gracefully"""
        mock_consumer = Mock()
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        mock_consumer.poll.side_effect = KeyboardInterrupt()

        consume_and_process_loop(mock_consumer, mock_producer, config)

        captured = capsys.readouterr()
        assert "Stopping..." in captured.out

    @patch("kafka_republisher.republisher.process_message")
    def test_consume_and_process_loop_continuous_polling(self, mock_process):
        """Test that loop continues polling until interrupted"""
        mock_consumer = Mock()
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        # Simulate multiple polls before interrupt
        mock_msg1 = Mock()
        mock_msg2 = Mock()
        mock_msg3 = Mock()
        mock_consumer.poll.side_effect = [
            mock_msg1,
            None,
            mock_msg2,
            None,
            mock_msg3,
            KeyboardInterrupt(),
        ]

        consume_and_process_loop(mock_consumer, mock_producer, config)

        assert mock_consumer.poll.call_count == 6
        # 5 process_message calls (KeyboardInterrupt stops before 6th)
        assert mock_process.call_count == 5

    @patch("kafka_republisher.republisher.process_message")
    def test_consume_and_process_loop_passes_correct_config(
        self, mock_process
    ):
        """Test that loop passes correct config to process_message"""
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_msg = Mock()
        config = RepublisherConfig(
            bootstrap_servers="broker:9092",
            from_topic="events",
            to_topic="events-delayed",
            sleep_time=60,
            group_id="consumer-group",
        )

        mock_consumer.poll.side_effect = [mock_msg, KeyboardInterrupt()]

        consume_and_process_loop(mock_consumer, mock_producer, config)

        # Verify config was passed correctly
        calls = mock_process.call_args_list
        assert calls[0][0][2] == config
        assert calls[0][0][2].sleep_time == 60
        assert calls[0][0][2].to_topic == "events-delayed"

    @patch("kafka_republisher.republisher.process_message")
    def test_consume_and_process_loop_with_multiple_none_messages(
        self, mock_process
    ):
        """Test loop handles multiple None messages (no data)"""
        mock_consumer = Mock()
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        # Simulate timeout scenarios with None messages
        mock_consumer.poll.side_effect = [
            None,
            None,
            None,
            KeyboardInterrupt(),
        ]

        consume_and_process_loop(mock_consumer, mock_producer, config)

        assert mock_consumer.poll.call_count == 4
        # 3 process_message calls (KeyboardInterrupt stops before 4th)
        assert mock_process.call_count == 3

    @patch("kafka_republisher.republisher.process_message")
    def test_consume_and_process_loop_poll_timeout(self, mock_process):
        """Test that loop uses correct poll timeout"""
        mock_consumer = Mock()
        mock_producer = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        mock_consumer.poll.side_effect = [None, KeyboardInterrupt()]

        consume_and_process_loop(mock_consumer, mock_producer, config)

        # Verify poll was called with 1.0 second timeout
        for call_args in mock_consumer.poll.call_args_list:
            assert call_args[0][0] == 1.0

    @patch("kafka_republisher.republisher.process_message")
    def test_consume_and_process_loop_passes_producer(self, mock_process):
        """Test that loop passes producer to process_message"""
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_msg = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        mock_consumer.poll.side_effect = [mock_msg, KeyboardInterrupt()]

        consume_and_process_loop(mock_consumer, mock_producer, config)

        # Verify producer was passed
        calls = mock_process.call_args_list
        assert calls[0][0][1] == mock_producer

    @patch("kafka_republisher.republisher.process_message")
    def test_consume_and_process_loop_order_of_operations(
        self, mock_process
    ):
        """Test that loop polls before processing"""
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_msg = Mock()
        config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )

        call_order = []

        def track_poll(*args, **kwargs):
            call_order.append("poll")
            if len(call_order) >= 2:
                raise KeyboardInterrupt()
            return mock_msg

        def track_process(*args, **kwargs):
            call_order.append("process")

        mock_consumer.poll.side_effect = track_poll
        mock_process.side_effect = track_process

        consume_and_process_loop(mock_consumer, mock_producer, config)

        # Verify poll happens before process
        assert call_order == ["poll", "process", "poll"]
