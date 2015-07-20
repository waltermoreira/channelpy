import json
import time
import uuid

import rabbitpy
import rabbitpy.exceptions


class ChannelException(Exception):
    pass


class ChannelTimeoutException(ChannelException):
    pass


class ChannelClosedException(ChannelException):
    pass


class ChannelEventException(ChannelException):
    pass


class RetryException(Exception):
    pass


class AbstractConnection(object):

    def connect(self):
        """Connect to the broker. """

    def close(self):
        """Close this instance of the connection. """

    def create_queue(self, name=None, local=False):
        """Create a queue.

        This method should be idempotent. Do nothing if the queue is
        already created.

        If name is None, create a unique name for the queue.

        If local is True, create a temporary queue private to this
        connection.

        :type name: str
        :type local: bool
        :rtype: queue
        """

    def create_pubsub(self, name):
        """Create a pubsub endpoint.

        This method should be idempotent. Do nothing if the endpoint is
        already created.

        :type name: str
        :rtype: pubsub
        """

    def delete_queue(self, queue):
        """Delete the queue in the broker. """

    def delete_pubsub(self, pubsub):
        """Delete the pubsub in the broker. """

    def subscribe(self, queue, pubsub):
        """Subscribe queue to pubsub.

        Messages in the pubsub are delivered to queue.

        :type queue: queue
        :type pubsub: pubsub
        """

    def publish(self, msg, pubsub):
        """Publish message in pubsub.

        :type msg: T
        :type pubsub: pubsub
        """

    def get(self, queue):
        """Non-blocking get.  Return None if empty.

        :type queue: queue
        :rtype: Optional[T]
        """

    def put(self, msg, queue):
        """Non-blocking put.

        :type msg: T
        :type queue: queue
        """

    def retrying(self, f, *args, **kwargs):
        """Evaluate f(*args, **kwargs). Retry on RetryException.

        :type f: Callable
        """
        pass


class RabbitConnection(AbstractConnection):

    def __init__(self, uri):
        self._uri = uri or 'amqp://127.0.0.1:5672'

        self._conn = None
        self._ch = None

    def connect(self):
        """Connect to the broker. """

        self._conn = rabbitpy.Connection(self._uri)
        self._ch = self._conn.channel()

    def close(self):
        """Close this instance of the connection. """

        self._ch.close()
        self._conn.close()

    def create_queue(self, name=None, local=False):
        """Create queue for messages.

        :type name: str
        :type local: bool
        :rtype: queue
        """
        _name = name or ''
        if local:
            _queue = rabbitpy.Queue(self._ch, name=_name, exclusive=True)
        else:
            _queue = rabbitpy.Queue(self._ch, name=_name, durable=True)
        _queue.declare()
        return _queue

    def create_pubsub(self, name):
        """Create a fanout exchange.

        :type name: str
        :rtype: pubsub
        """
        _exchange = rabbitpy.FanoutExchange(self._ch, name, durable=True)
        _exchange.declare()
        return _exchange

    def subscribe(self, queue, pubsub):
        queue.bind(pubsub)

    def publish(self, msg, pubsub):
        _msg = rabbitpy.Message(self._ch, msg)
        _msg.publish(pubsub, '')

    def delete_queue(self, queue):
        queue.delete()

    def delete_pubsub(self, pubsub):
        pubsub.delete()

    def get(self, queue):
        """Non-blocking get.  Return None if empty.

        :type queue: queue
        :rtype: Optional[T]
        """
        _msg = queue.get()
        if _msg is None:
            return None
        _msg.ack()
        return _msg.body

    def put(self, msg, queue):
        """Non-blocking put.

        :type msg: T
        :type queue: queue
        """
        _msg = rabbitpy.Message(self._ch, msg, {})
        _msg.publish('', queue.name)


class Queue(object):

    def __init__(self, name, connection=None):
        self.name = name
        self.connection = connection
        self._reconnect()

    def _reconnect(self):
        self.connection.connect()
        self._queue = self.connection.create_queue(self.name)
        self._event_queue = self.connection.create_queue(local=True)
        self._pubsub = self.connection.create_pubsub(self.name)
        self.connection.subscribe(self._event_queue, self._pubsub)

    def close(self):
        """Close this instance of the channel."""

        self.connection.close()

    def event(self, ev):
        """Publish an event."""

        self.connection.publish(ev)

    def delete(self):
        """Delete the queue completely."""

        self.connection.delete_queue(self._queue)
        self.connection.delete_pubsub(self._pubsub)
        self.close()

    def _check_for_events(self):
        ev = self.connection.get(self._event_queue)
        if ev is not None:
            raise ChannelEventException(ev)

    def _retrying(self, f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except rabbitpy.exceptions.AMQPNotFound:
                raise ChannelClosedException()
            except (rabbitpy.exceptions.AMQPException,
                    rabbitpy.exceptions.RabbitpyException):
                self._reconnect()
                return f(*args, **kwargs)
        return wrapper

    def _get(self):
        self._check_for_events()
        return self.connection.get(self._queue)

    def get(self):
        return self._retrying(self._get)()

    def _put(self, msg):
        self._check_for_events()
        self.connection.put(msg, self._queue)

    def put(self, msg):
        self._retrying(self._put)(msg)


class ChannelEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Channel):
            return {
                '__channel__': True,
                'name': obj._name,
                'uri': obj.uri
            }
        return super(ChannelEncoder, self).default(obj)


def as_channel(dct):
    if '__channel__' in dct:
        return Channel(dct['name'], dct['uri'])
    return dct


class Channel(object):

    POLL_FREQUENCY = 0.1  # seconds

    def __init__(self, name=None, uri=None, persist=False):
        """
        :type name: str
        :type uri: str
        """
        self.uri = uri
        self._name = name or uuid.uuid4().hex
        self._persist = persist
        self._conn = RabbitConnection(uri, self._name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del exc_type, exc_val, exc_tb
        if not self._persist:
            self.delete()
        else:
            self.close()

    def get(self, timeout=float('inf')):
        try:
            start = time.time()
            while True:
                if self._conn is None:
                    raise ChannelClosedException()
                msg = self._conn.get()
                if msg is not None:
                    return self._process(msg.decode('utf-8'))
                if time.time() - start > timeout:
                    raise ChannelTimeoutException()
                time.sleep(self.POLL_FREQUENCY)
        except ChannelEventException:
            self.close()
            raise ChannelClosedException()

    @staticmethod
    def _process(msg):
        return json.loads(msg, object_hook=as_channel)

    def put(self, value):
        if self._conn is None:
            raise ChannelClosedException()
        try:
            self._conn.put(
                json.dumps(value, cls=ChannelEncoder).encode('utf-8'))
        except ChannelEventException:
            self.close()
            raise ChannelClosedException()

    def close(self):
        if self._conn is None:
            raise ChannelClosedException()
        self._conn.close()
        self._conn = None

    def delete(self):
        if self._conn is None:
            raise ChannelClosedException()
        self._conn.delete()
        self.close()

    def close_all(self):
        if self._conn is None:
            raise ChannelClosedException()
        self._conn.event('close')
        self.close()

    def put_sync(self, value, timeout=float('inf')):
        """Synchronous put.

        Wraps the object ``value`` in the form::

            {"value": value, "reply_to": ch}

        and waits for a response in ``ch``.

        """
        with Channel(uri=self.uri) as ch:
            self.put({
                'value': value,
                'reply_to': ch
            })
            return ch.get(timeout=timeout)


def test(uri):
    """
    a: ChanIn[int]
    b: ChanIn[str]
    c: ChanOut[Dict]
    d: ChanOut[int]

    :type uri: str
    :rtype: ChanOut[int]
    """
    a = Channel(uri=uri)
    b = Channel(uri=uri)
    c = Channel(uri=uri)

    a.put(5)
    b.put('foo')

    x = a.get()
    assert isinstance(x, int)

    y = b.get()
    assert isinstance(y, basestring)

    c.put({'a': x, 'b': y})

    d = Channel(uri=a.uri)
    d.put(5)
    return d
