from typing import Optional
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractRobustQueue, AbstractIncomingMessage, \
    DeliveryMode
import aio_pika
import asyncio
import logging
from rabbitmq_communicator.decorators import requires_connection
from rabbitmq_communicator.message_processor import MessageProcessor


class RabbitMQCommunicator:
    def __init__(self, message_processor: MessageProcessor, in_queue_name: str, out_queue_name: str,
                 max_processes: int = 2):
        """
        Communicator for duplex communication with other instance of communicator via rabbitmq queue service and heavy tasks processing utilizing multiprocessing
        :param message_processor: MessageProcessor object that implements all required methods
        :param in_queue_name: queue to listen to
        :param out_queue_name: queue to which to send the response
        :param max_processes: number of max messages that will be processed concurrently
        """
        self.in_queue_name = in_queue_name
        self.out_queue_name = out_queue_name
        self.connection: Optional[AbstractRobustConnection] = None
        self.channel: Optional[AbstractRobustChannel] = None
        self.in_queue: Optional[AbstractRobustQueue] = None
        self.out_queue: Optional[AbstractRobustQueue] = None

        self.max_messages = max_processes
        self.message_processor = message_processor
        self.max_message_retries = 3
        self.rabbitmq_url: Optional[str] = None

    async def connect(self, rabbitmq_url: str):
        """
        connect to the rabbitmq server
        :param rabbitmq_url: rabbitmq connection url amqp://{user}:{password}@{host}:{port}/
        :return:
        """
        self.rabbitmq_url = rabbitmq_url
        try:
            if not self.connection or self.connection.is_closed:
                self.connection = await aio_pika.connect_robust(rabbitmq_url)
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=self.max_messages)
                self.in_queue = await self.channel.declare_queue(self.in_queue_name, durable=True)
                self.out_queue = await self.channel.declare_queue(self.out_queue_name, durable=True)

                logging.debug("Connected to RabbitMQ")

        except Exception as e:
            logging.error(f"Failed to connect to RabbitMQ: {e}")
            raise ConnectionError(f"Failed to connect to RabbitMQ: {e}")

    def check_connection(self) -> bool:
        """
        Check status of whole connection
        :return: returns True if connection is opened and False if is closed
        """
        if not all([self.connection, self.channel]):
            logging.debug(f"Connection status: {self.connection}, Channel status: {self.channel}")
            return False

        if any([self.connection.is_closed, self.channel.is_closed]):
            logging.debug(f"Connection status: {self.connection.is_closed}, Channel status: {self.channel.is_closed}")
            return False

        return True

    @requires_connection
    async def start_consuming(self):
        await self.in_queue.consume(self.__on_message)
        logging.debug(f"Waiting for messages from {self.in_queue_name}...")
        await asyncio.Future()

    @requires_connection
    async def send_message(self, message_content: str, headers: Optional[dict] = None):
        """
        Sends message to out_queue
        :param message_content: string content of message
        :param headers: dict headers of message
        :return:
        """
        if not isinstance(message_content, str) or not message_content.strip():
            raise TypeError("message_content should be of type str and not be empty")

        message = aio_pika.Message(
            body=message_content.encode(),
            headers=headers,
            delivery_mode=DeliveryMode.PERSISTENT
        )

        await self.channel.default_exchange.publish(
            message=message,
            routing_key=self.out_queue_name,
        )

        logging.debug(f"Sent message: {message.message_id}")

    async def __on_message(self, message: AbstractIncomingMessage):
        """
        Processes message with the method process_message of message_processor
        :param message: message from rabbitmq queue
        :return:
        """
        try:
            async with message.process():
                logging.debug(f"Processing message: {message.message_id}")

                response_content, response_headers = self.message_processor.process_message(
                    message
                )
                await self.send_message(response_content, response_headers)
            logging.debug(f"Finished processing message: {message.message_id}")
            await message.ack()
        except ConnectionError as e:
            await self.connect(self.rabbitmq_url)
            if not self.check_connection():
                raise ConnectionError(f"Lost connection to the server and couldn't reconnect, {e}")
            await message.nack()
        except Exception as e:
            retries = message.headers.get("x-retries", 0)  # Get current retry count
            if retries >= self.max_message_retries:
                logging.error(f"Message {message.message_id} failed after {retries} retries. Rejecting permanently. {e}")
                await message.reject(requeue=False)  # Permanently reject the message
            else:
                logging.warning(f"Message {message.message_id} failed. Retrying {retries + 1}/{self.max_message_retries}... {e}")
                new_headers = message.headers.copy()
                new_headers["x-retries"] = retries + 1  # Increment retry count

                await self.send_message(message.body.decode(), new_headers)  # Requeue with updated header
                await message.ack()  # Acknowledge the failed message to prevent duplicate requeue
