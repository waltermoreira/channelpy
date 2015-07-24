class ChannelException(Exception):
    """Base class of channelpy exceptions."""
    pass


class ChannelTimeoutException(ChannelException):
    """Channel timed out exception."""
    pass


class ChannelClosedException(ChannelException):
    """Channel connection instance is closed."""
    pass


class ChannelEventException(ChannelException):
    """Channel event propagation."""
    pass


class ChannelInitConnectionException(ChannelException):
    """Connection initialization exception."""
    pass


class ChannelCloseAllException(ChannelException):
    """Event to closee all produces and consumers."""
    pass