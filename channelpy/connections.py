from .rabbitpy_connection import RabbitConnection


def register(*args):
    return {arg.__name__: arg for arg in args}


connections = register(
    RabbitConnection)
