from kafka_republisher.republisher import consume_and_process_loop, run
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


class TestRun:
    """Tests for run function"""

    @patch("kafka_republisher.republisher.consume_and_process_loop")
    @patch("kafka_republisher.republisher.get_producer")
    @patch("kafka_republisher.republisher.get_consumer")
    @patch("kafka_republisher.republisher.get_config_from_env")
    def test_run_loads_config(
        self, mock_get_config, mock_get_consumer, mock_get_producer, mock_loop
    ):
        """Test that run() loads configuration from environment"""
        mock_config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )
        mock_get_config.return_value = mock_config
        mock_consumer = Mock()
        mock_get_consumer.return_value = mock_consumer

        run()

        mock_get_config.assert_called_once()

    @patch("kafka_republisher.republisher.consume_and_process_loop")
    @patch("kafka_republisher.republisher.get_producer")
    @patch("kafka_republisher.republisher.get_consumer")
    @patch("kafka_republisher.republisher.get_config_from_env")
    def test_run_creates_consumer_and_producer(
        self, mock_get_config, mock_get_consumer, mock_get_producer, mock_loop
    ):
        """Test that run() creates Kafka consumer and producer"""
        mock_config = RepublisherConfig(
            bootstrap_servers="broker:9092",
            from_topic="events",
            to_topic="events-delayed",
            sleep_time=60,
            group_id="consumer-group",
        )
        mock_get_config.return_value = mock_config
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_get_consumer.return_value = mock_consumer
        mock_get_producer.return_value = mock_producer

        run()

        mock_get_consumer.assert_called_once_with(
            "broker:9092", "consumer-group"
        )
        mock_get_producer.assert_called_once_with("broker:9092")

    @patch("kafka_republisher.republisher.consume_and_process_loop")
    @patch("kafka_republisher.republisher.get_producer")
    @patch("kafka_republisher.republisher.get_consumer")
    @patch("kafka_republisher.republisher.get_config_from_env")
    def test_run_subscribes_to_topic(
        self, mock_get_config, mock_get_consumer, mock_get_producer, mock_loop
    ):
        """Test that run() subscribes consumer to from_topic"""
        mock_config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="input-topic",
            to_topic="output-topic",
            sleep_time=30,
            group_id="test-group",
        )
        mock_get_config.return_value = mock_config
        mock_consumer = Mock()
        mock_get_consumer.return_value = mock_consumer

        run()

        mock_consumer.subscribe.assert_called_once_with(["input-topic"])

    @patch("kafka_republisher.republisher.consume_and_process_loop")
    @patch("kafka_republisher.republisher.get_producer")
    @patch("kafka_republisher.republisher.get_consumer")
    @patch("kafka_republisher.republisher.get_config_from_env")
    def test_run_calls_consume_loop(
        self, mock_get_config, mock_get_consumer, mock_get_producer, mock_loop
    ):
        """Test that run() calls consume_and_process_loop with correct
        arguments"""
        mock_config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )
        mock_get_config.return_value = mock_config
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_get_consumer.return_value = mock_consumer
        mock_get_producer.return_value = mock_producer

        run()

        mock_loop.assert_called_once_with(
            mock_consumer, mock_producer, mock_config
        )

    @patch("kafka_republisher.republisher.consume_and_process_loop")
    @patch("kafka_republisher.republisher.get_producer")
    @patch("kafka_republisher.republisher.get_consumer")
    @patch("kafka_republisher.republisher.get_config_from_env")
    def test_run_closes_consumer_on_normal_completion(
        self, mock_get_config, mock_get_consumer, mock_get_producer, mock_loop
    ):
        """Test that run() closes consumer after loop completes normally"""
        mock_config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )
        mock_get_config.return_value = mock_config
        mock_consumer = Mock()
        mock_get_consumer.return_value = mock_consumer

        run()

        mock_consumer.close.assert_called_once()

    @patch("kafka_republisher.republisher.consume_and_process_loop")
    @patch("kafka_republisher.republisher.get_producer")
    @patch("kafka_republisher.republisher.get_consumer")
    @patch("kafka_republisher.republisher.get_config_from_env")
    def test_run_closes_consumer_on_exception(
        self, mock_get_config, mock_get_consumer, mock_get_producer, mock_loop
    ):
        """Test that run() closes consumer even when exception occurs"""
        mock_config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )
        mock_get_config.return_value = mock_config
        mock_consumer = Mock()
        mock_get_consumer.return_value = mock_consumer
        mock_loop.side_effect = Exception("Test exception")

        try:
            run()
        except Exception:
            pass

        mock_consumer.close.assert_called_once()

    @patch("kafka_republisher.republisher.consume_and_process_loop")
    @patch("kafka_republisher.republisher.get_producer")
    @patch("kafka_republisher.republisher.get_consumer")
    @patch("kafka_republisher.republisher.get_config_from_env")
    def test_run_prints_startup_info(
        self, mock_get_config, mock_get_consumer, mock_get_producer,
        mock_loop, capsys
    ):
        """Test that run() prints startup information"""
        mock_config = RepublisherConfig(
            bootstrap_servers="broker:9092",
            from_topic="input",
            to_topic="output",
            sleep_time=45,
            group_id="test-group",
        )
        mock_get_config.return_value = mock_config
        mock_consumer = Mock()
        mock_get_consumer.return_value = mock_consumer

        run()

        captured = capsys.readouterr()
        assert "Starting Kafka Delayer (parallel mode)" in captured.out
        assert "Bootstrap: broker:9092" in captured.out
        assert "From: input" in captured.out
        assert "To: output" in captured.out
        assert "Delay: 45s" in captured.out

    @patch("kafka_republisher.republisher.consume_and_process_loop")
    @patch("kafka_republisher.republisher.get_producer")
    @patch("kafka_republisher.republisher.get_consumer")
    @patch("kafka_republisher.republisher.get_config_from_env")
    def test_run_correct_call_sequence(
        self, mock_get_config, mock_get_consumer, mock_get_producer, mock_loop
    ):
        """Test that run() calls functions in correct order"""
        mock_config = RepublisherConfig(
            bootstrap_servers="localhost:9092",
            from_topic="source",
            to_topic="destination",
            sleep_time=30,
            group_id="test-group",
        )
        mock_get_config.return_value = mock_config
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_get_consumer.return_value = mock_consumer
        mock_get_producer.return_value = mock_producer

        call_order = []

        mock_get_config.side_effect = lambda: (
            call_order.append("config"),
            mock_config,
        )[1]
        mock_get_consumer.side_effect = lambda *args: (
            call_order.append("consumer"),
            mock_consumer,
        )[1]
        mock_get_producer.side_effect = lambda *args: (
            call_order.append("producer"),
            mock_producer,
        )[1]
        mock_consumer.subscribe.side_effect = lambda *args: call_order.append(
            "subscribe"
        )
        mock_loop.side_effect = (
            lambda *args, **kwargs: call_order.append("loop")
        )
        mock_consumer.close.side_effect = lambda: call_order.append("close")

        run()

        assert call_order == [
            "config",
            "consumer",
            "producer",
            "subscribe",
            "loop",
            "close",
        ]

    @patch("kafka_republisher.republisher.consume_and_process_loop")
    @patch("kafka_republisher.republisher.get_producer")
    @patch("kafka_republisher.republisher.get_consumer")
    @patch("kafka_republisher.republisher.get_config_from_env")
    def test_run_passes_correct_config_values(
        self, mock_get_config, mock_get_consumer, mock_get_producer, mock_loop
    ):
        """Test that run() passes config with correct values to consume loop"""
        mock_config = RepublisherConfig(
            bootstrap_servers="test-broker:9092",
            from_topic="test-input",
            to_topic="test-output",
            sleep_time=120,
            group_id="test-consumer-group",
        )
        mock_get_config.return_value = mock_config
        mock_consumer = Mock()
        mock_producer = Mock()
        mock_get_consumer.return_value = mock_consumer
        mock_get_producer.return_value = mock_producer

        run()

        # Verify the config object passed to loop
        call_args = mock_loop.call_args
        passed_config = call_args[0][2]
        assert passed_config.bootstrap_servers == "test-broker:9092"
        assert passed_config.from_topic == "test-input"
        assert passed_config.to_topic == "test-output"
        assert passed_config.sleep_time == 120
        assert passed_config.group_id == "test-consumer-group"
