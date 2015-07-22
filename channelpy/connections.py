from .rabbitpy_connection import RabbitConnection


connections = {
    None: RabbitConnection,
    'RabbitConnection': RabbitConnection
}
