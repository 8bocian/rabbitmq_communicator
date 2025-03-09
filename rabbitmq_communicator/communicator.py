import concurrent.futures
from typing import Optional
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractRobustQueue, AbstractIncomingMessage, \
    DeliveryMode
import aio_pika
import asyncio
import logging
from rabbitmq_communicator.decorators import requires_connection
from     rabbitmq_communicator.message_processor import MessageProcessor


class RabbitMQCommunicator:
    def __init__(self, message_processor: MessageProcessor, in_queue_name: str, out_queue_name: str,
                 max_processes: int = 2):
        """
        Communicator for duplex communication with other instance of communicator via rabbitmq queue service and heavy tasks processing utiliting multiprocessing
        :param in_queue_name: queue to listen to
        :param out_queue_name: queue to which to send the response
        """
        self.in_queue_name = in_queue_name
        self.out_queue_name = out_queue_name
        self.connection: Optional[AbstractRobustConnection] = None
        self.channel: Optional[AbstractRobustChannel] = None
        self.in_queue: Optional[AbstractRobustQueue] = None
        self.out_queue: Optional[AbstractRobustQueue] = None

        self.max_processes = max_processes
        self.pool = concurrent.futures.ProcessPoolExecutor(max_workers=max_processes)

        self.message_processor = message_processor

    async def connect(self, rabbitmq_url: str):
        """
        connect to the rabbitmq server
        :param rabbitmq_url: rabbitmq connection url amqp://{user}:{password}@{host}:{port}/
        :return:
        """
        try:
            if not self.connection or self.connection.is_closed:
                self.connection = await aio_pika.connect_robust(rabbitmq_url)
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=self.max_processes)

                self.in_queue = await self.channel.declare_queue(self.in_queue_name, durable=True)
                self.out_queue = await self.channel.declare_queue(self.out_queue_name, durable=True)

                logging.debug("Connected to RabbitMQ")

        except Exception as e:
            logging.error(f"Failed to connect to RabbitMQ: {e}")

    def check_connection(self) -> bool:
        """
        Check status of whole connection
        :return: returns True if connection is opened and False if is closed
        """
        if not all([self.connection, self.channel]):
            return False

        if any([self.connection.is_closed, self.channel.is_closed]):
            return False

        return True

    @requires_connection
    async def start_consuming(self):
        await self.in_queue.consume(self.__on_message)
        logging.debug(f"Waiting for messages from {self.in_queue_name}...")
        await asyncio.Future()

    @requires_connection
    async def send_message(self, message_content: str, headers: Optional[dict] = None,
                           queue_name: Optional[str] = None):
        """
        :param message_content: string content of message
        :param headers: dict headers of message
        :param queue_name: queue name if needed to be sent through another queue
        :return:
        """
        if not isinstance(message_content, str) or not message_content.strip():
            raise TypeError("message_content should be of type str and not be empty")

        queue_name = queue_name or self.out_queue_name

        message = aio_pika.Message(
            body=message_content.encode(),
            headers=headers,
            delivery_mode=DeliveryMode.PERSISTENT
        )

        await self.channel.default_exchange.publish(
            message=message,
            routing_key=queue_name,
        )

        logging.debug(f"Sent message: {message.message_id}")

    async def __on_message(self, message: AbstractIncomingMessage):
        """delegates message from rabbitmq queue to async task to free the current thread"""
        asyncio.create_task(self.__preprocess_message(message))

    async def __preprocess_message(self, message: AbstractIncomingMessage):
        """process message from rabbitmq on separate process"""
        # async with message.process():
        msg_content = message.body.decode()
        msg_headers = message.headers
        logging.debug(f"Processing message: {message.message_id}")

        loop = asyncio.get_running_loop()
        try:

            response_message, response_headers = (
                await loop.run_in_executor(self.pool, self.message_processor.process_message, msg_content, msg_headers))
            await self.send_message(response_message, response_headers)
            logging.debug(f"Finished processing message: {message.message_id}")
            await message.ack()
        except Exception as e:
            logging.error(f"Error processing message: {message.message_id}, {e}")
            await message.nack()
