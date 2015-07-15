import json
import time
import uuid

import rabbitpy


class ChannelTimeoutException(Exception):
    pass


class AbstractConnection(object):

    def close(self):
        pass

    def delete(self):
        """Delete the queue in the broker. """
        pass

    def get(self):
        """
        :rtype: Optional[T]
        """
        pass

    def put(self, msg):
        """
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
        self._queue = rabbitpy.Queue(self._ch, self._name)
        self._queue.durable = True
        self._queue.declare()

    def close(self):
        self._ch.close()
        self._conn.close()

    def delete(self):
        self._queue.delete()

    def _retrying(self, f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except rabbitpy.exceptions.AMQPException:
                self._reconnect()
                return f(*args, **kwargs)
        return wrapper

    def _get(self):
        msg = self._queue.get()
        if msg is None:
            return None
        msg.ack()
        return msg.body

    def get(self):
        return self._retrying(self._get)()

    def _put(self, msg):
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

    def __init__(self, name=None, uri=None):
        """
        :type name: str
        :type uri: str
        """
        self.uri = uri
        self._name = name or uuid.uuid4().hex
        self._conn = RabbitConnection(uri, self._name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del exc_type, exc_val, exc_tb
        self._conn.close()

    def get(self, timeout=float('inf')):
        start = time.time()
        while True:
            msg = self._conn.get()
            if msg is not None:
                return self._process(msg)
            if time.time() - start > timeout:
                raise ChannelTimeoutException()
            time.sleep(self.POLL_FREQUENCY)

    @staticmethod
    def _process(msg):
        return json.loads(msg, object_hook=as_channel)

    def put(self, value):
        self._conn.put(json.dumps(value, cls=ChannelEncoder))

    def close(self):
        self._conn.close()
        
    def delete(self):
        self._conn.delete()


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
