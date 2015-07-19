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


class AbstractConnection(object):

    def close(self):
        """Close this instance of the channel. """
        pass

    def delete(self):
        """Delete the queue in the broker. """
        pass

    def get(self):
        """Non-blocking get.  Return None if empty.

        :rtype: Optional[T]
        """
        pass

    def put(self, msg):
        """Non-blocking put.

        :type msg: T
        """
        pass


class RabbitConnection(AbstractConnection):

    _conn = None

    def __init__(self, uri, name):
        self._uri = uri or 'amqp://127.0.0.1:5672'
        self._name = name
        self._reconnect()

    def _reconnect(self):
        self._conn = rabbitpy.Connection(self._uri)
        self._ch = self._conn.channel()

        self._exchange = rabbitpy.FanoutExchange(
            self._ch, self._name, durable=True)
        self._exchange.declare()

        self._event_queue = rabbitpy.Queue(self._ch, exclusive=True)
        self._event_queue.declare()

        self._queue = rabbitpy.Queue(self._ch, self._name)
        self._queue.durable = True
        self._queue.declare()

        self._event_queue.bind(self._exchange)

    def close(self):
        """Close this instance of the channel."""

        self._ch.close()
        self._conn.close()

    def event(self, ev):
        """Close all connected instances to this channel."""
        msg = rabbitpy.Message(self._ch, ev)
        msg.publish(self._exchange, '')

    def delete(self):
        """Delete the queue completely."""

        self._queue.delete()
        self._exchange.delete()
        self.close()

    def _check_for_events(self):
        ev = self._event_queue.get()
        if ev is not None:
            ev.ack()
            raise ChannelEventException(ev.body)

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
        msg = self._queue.get()
        if msg is None:
            return None
        msg.ack()
        return msg.body

    def get(self):
        return self._retrying(self._get)()

    def _put(self, msg):
        self._check_for_events()
        _msg = rabbitpy.Message(self._ch, msg, {})
        _msg.publish('', self._name)

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
