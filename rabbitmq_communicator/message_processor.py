from abc import abstractmethod
from typing import Optional
from aio_pika.abc import AbstractIncomingMessage


class MessageProcessor:
    @abstractmethod
    def process_message(self, message: AbstractIncomingMessage) -> tuple[str, Optional[dict]]:
        """
        processes received message content and headers, returns new message content and new message headers
        :param message: message received from the queue
        :return: new message content and new message headers
        """
        raise NotImplementedError("Subclasses must implement this method")
