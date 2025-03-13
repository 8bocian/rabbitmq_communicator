import pytest
from unittest import mock
import asyncio
import aio_pika
from aio_pika.abc import AbstractRobustChannel
from rabbitmq_communicator.communicator import RabbitMQCommunicator
from rabbitmq_communicator.message_processor import MessageProcessor
from unittest.mock import AsyncMock, MagicMock, patch
from typing import cast


@pytest.fixture
def message_processor() -> MessageProcessor:
    processor = MagicMock(spec=MessageProcessor)
    processor.process_message = MagicMock(return_value=("processed_response", {"header_key": "header_value"}))
    return processor


@pytest.fixture
def communicator(message_processor: MessageProcessor) -> RabbitMQCommunicator:
    comm = RabbitMQCommunicator(message_processor, "test_in_queue", "test_out_queue")
    comm.connection = AsyncMock(spec=aio_pika.RobustConnection)
    comm.channel = cast(AbstractRobustChannel, AsyncMock(spec=aio_pika.RobustChannel))
    comm.in_queue = AsyncMock(spec=aio_pika.RobustQueue)
    comm.out_queue = AsyncMock(spec=aio_pika.RobustQueue)
    return comm


@pytest.mark.asyncio
async def test_connect_success(communicator: RabbitMQCommunicator):
    # Mock the RabbitMQ connection and channel
    mock_connection = mock.AsyncMock()
    mock_connection.is_closed = True

    mock_channel = mock.AsyncMock()
    mock_channel.is_closed = False

    mock_connection.channel.return_value = mock_channel

    async def connect_side_effect(*args, **kwargs):
        mock_connection.is_closed = False
        return mock_connection

    # Use the mock connection in the method
    with mock.patch("aio_pika.connect_robust", return_value=mock_connection,
                    side_effect=connect_side_effect) as mock_connect:
        await communicator.connect("amqp://user:password@localhost:5672/")
        # Check if the connection and channel are set correctly
        mock_connect.assert_called_once()
        mock_channel.set_qos.assert_called_once_with(prefetch_count=2)
        print(communicator.check_connection())
        assert communicator.check_connection() is True


@pytest.mark.asyncio
async def test_connect_failure(communicator: RabbitMQCommunicator):
    """Test that connect raises an exception if the connection fails."""
    # Simulate an exception in the connect_robust method
    with patch("aio_pika.connect_robust", side_effect=Exception("Connection failed")) as p:
        with pytest.raises(ConnectionError, match="Failed to connect to RabbitMQ"):
            await communicator.connect("amqp://guest:guest@localhost")


@pytest.mark.asyncio
async def test_check_connection(communicator: RabbitMQCommunicator):
    """Test that check_connection returns correct status"""
    communicator.connection.is_closed = False
    communicator.channel.is_closed = False
    assert communicator.check_connection() is True

    communicator.connection.is_closed = True
    assert communicator.check_connection() is False

    communicator.connection.is_closed = False
    communicator.channel.is_closed = True
    assert communicator.check_connection() is False


@pytest.mark.asyncio
async def test_send_message(communicator: RabbitMQCommunicator):
    """Test sending a message"""
    communicator.check_connection = MagicMock(return_value=True)
    communicator.channel.default_exchange = AsyncMock()

    await communicator.send_message("test message", {"key": "value"})

    communicator.channel.default_exchange.publish.assert_called_once()
    message = communicator.channel.default_exchange.publish.call_args[1]["message"]

    assert message.body == b"test message"
    assert message.headers == {"key": "value"}


@pytest.mark.asyncio
async def test_start_consuming(communicator: RabbitMQCommunicator):
    """Test that consuming messages starts correctly"""
    communicator.check_connection = MagicMock(return_value=True)
    communicator.in_queue.consume = AsyncMock()

    task = asyncio.create_task(communicator.start_consuming())
    await asyncio.sleep(0.1)

    communicator.in_queue.consume.assert_called_once()
    task.cancel()


@pytest.mark.asyncio
async def test_on_message(communicator: RabbitMQCommunicator):
    """Test message processing and response sending"""
    communicator.check_connection = MagicMock(return_value=True)
    mock_message = AsyncMock(spec=aio_pika.abc.AbstractIncomingMessage)
    mock_message.body = b"test message"
    mock_message.headers = {"key": "value"}
    mock_message.message_id = 1

    with mock.patch.object(communicator, 'send_message', AsyncMock()) as mock_send_message:
        await communicator._RabbitMQCommunicator__on_message(mock_message)

        communicator.message_processor.process_message.assert_called_once_with(mock_message)
        mock_send_message.assert_called_once_with("processed_response", {"header_key": "header_value"})
        mock_message.ack.assert_called_once()
        mock_message.nack.assert_not_called()


@pytest.mark.asyncio
async def test_on_message_error_handling(communicator: RabbitMQCommunicator):
    """Test error handling during message processing"""
    communicator.message_processor.process_message = MagicMock(side_effect=Exception("Processing Error"))
    mock_message = AsyncMock(spec=aio_pika.abc.AbstractIncomingMessage)
    mock_message.body = b"test message"
    mock_message.headers = {"key": "value"}
    mock_message.message_id = 1
    with mock.patch.object(communicator.message_processor, 'process_message',
                           MagicMock(side_effect=Exception("Processing Error"))) as mock_send_message:
        await communicator._RabbitMQCommunicator__on_message(mock_message)

    mock_message.ack.assert_not_called()
    mock_message.nack.assert_called_once()


@pytest.mark.asyncio
async def test_send_message_invalid_content(communicator: RabbitMQCommunicator):
    """Test sending invalid message content"""
    with mock.patch.object(communicator, "check_connection", return_value=True):
        with pytest.raises(TypeError):
            await communicator.send_message("", {"key": "value"})


@pytest.mark.asyncio
async def test_multiple_concurrent_messages(communicator: RabbitMQCommunicator):
    """Test processing multiple messages concurrently"""
    communicator.check_connection = MagicMock(return_value=True)
    communicator.max_messages = 3

    mock_messages = [AsyncMock(spec=aio_pika.abc.AbstractIncomingMessage) for _ in range(5)]
    for i, mock_message in enumerate(mock_messages):
        mock_message.body = f"test message {i}".encode()
        mock_message.headers = {"key": "value"}
        mock_message.message_id = i

    with mock.patch.object(communicator, 'send_message', AsyncMock()) as mock_send_message:
        tasks = [communicator._RabbitMQCommunicator__on_message(msg) for msg in mock_messages]
        await asyncio.gather(*tasks)

        assert len(mock_send_message.call_args_list) == 5  # Ensure all messages are processed


@pytest.mark.asyncio
async def test_connection_loss(communicator: RabbitMQCommunicator):
    """Test the behavior when connection is lost during message processing"""
    communicator.check_connection = MagicMock(return_value=True)
    communicator.connection.is_closed = True  # Simulate loss of connection

    mock_message = AsyncMock(spec=aio_pika.abc.AbstractIncomingMessage)
    mock_message.body = b"test message"
    mock_message.headers = {"key": "value"}
    mock_message.message_id = 1
    with mock.patch.object(communicator, 'check_connection', MagicMock(return_value=False)):
        with pytest.raises(ConnectionError):
            await communicator._RabbitMQCommunicator__on_message(mock_message)
