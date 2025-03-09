from functools import wraps
import aio_pika

def requires_connection(method):
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        if not self.check_connection():
            raise aio_pika.exceptions.AMQPConnectionError("RabbitMQ connection is not open.")
        return await method(self, *args, **kwargs)
    return wrapper