from abc import abstractmethod
from typing import Optional


class MessageProcessor:
    @abstractmethod
    def process_message(self, message_content: str, message_headers: dict) -> tuple[str, Optional[dict]]:
        """
        processes received message content and headers, returns new message content and new message headers
        :param message_content: string content of received message
        :param message_headers: dict of headers of received message
        :return: new message content and new message headers
        """
        raise NotImplementedError("Subclasses must implement this method")