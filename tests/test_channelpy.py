import os
import threading

import pytest
import channelpy


BROKER_URI = os.environ['BROKER']


def make_channel(name=None, persist=False):
    return channelpy.Channel(name=name, persist=persist,
                             connection_type=channelpy.RabbitConnection,
                             uri=BROKER_URI)


@pytest.fixture(scope='module')
def anon_ch(request):

    ch = make_channel()

    def fin():
        ch2 = make_channel(name=ch.name)
        ch2.delete()

    request.addfinalizer(fin)

    return ch


@pytest.fixture(scope='function')
def closing_ch(request):

    ch = make_channel()

    def fin():
        ch2 = make_channel(name=ch.name)
        ch2.delete()

    request.addfinalizer(fin)

    return ch


def test_ch(anon_ch):
    assert anon_ch.put(5) is None
    assert anon_ch.get() == 5


def test_close(closing_ch):
    closing_ch.close()
    with pytest.raises(channelpy.ChannelClosedException):
        closing_ch.get()
    with pytest.raises(channelpy.ChannelClosedException):
        closing_ch.put(1)
    with pytest.raises(channelpy.ChannelClosedException):
        closing_ch.close()
    with pytest.raises(channelpy.ChannelClosedException):
        closing_ch.close_all()


def test_types(anon_ch):
    anon_ch.put(4)
    assert isinstance(anon_ch.get(), int)
    anon_ch.put('foo')
    assert isinstance(anon_ch.get(), basestring)
    anon_ch.put([3, 4])
    assert isinstance(anon_ch.get(), list)
    anon_ch.put({'hi': 'there'})
    assert isinstance(anon_ch.get(), dict)
    anon_ch.put(anon_ch)
    ch = anon_ch.get()
    assert isinstance(ch, channelpy.Channel)
    assert ch.name == anon_ch.name


def test_put_sync(anon_ch):

    def respond(ch):
        msg = ch.get()
        msg['reply_to'].put({'value': msg['value']})

    t = threading.Thread(target=respond, args=(anon_ch,))
    t.start()

    assert t.is_alive()
    response = anon_ch.put_sync(4)
    assert response['value'] == 4


def test_multiple_producers(anon_ch):
    y = make_channel(anon_ch.name)
    anon_ch.put(1)
    y.put(2)
    assert y.get() == 1
    assert y.get() == 2


def test_timeout(anon_ch):
    anon_ch.put(1)
    assert anon_ch.get(timeout=5) == 1
    anon_ch.put(2)
    assert anon_ch.get(timeout=0) == 2
    with pytest.raises(channelpy.ChannelTimeoutException):
        anon_ch.get(timeout=1)


def test_multiple_consumers(anon_ch):
    y = make_channel(name=anon_ch.name)
    anon_ch.put(1)
    anon_ch.put(2)
    assert y.get() == 1
    assert anon_ch.get() == 2


def test_close_all(closing_ch):
    with make_channel() as resp:
        a = make_channel(name=closing_ch.name)
        b = make_channel(name=closing_ch.name)

        def consume(name, ch, resp):
            try:
                ch.get()
            except channelpy.ChannelClosedException:
                resp.put(name)

        ta = threading.Thread(target=consume, args=('a', a, resp))
        ta.start()
        tb = threading.Thread(target=consume, args=('b', b, resp))
        tb.start()

        assert ta.is_alive() and tb.is_alive()

        closing_ch.close_all()

        results = set()
        results.add(resp.get(timeout=2))
        results.add(resp.get(timeout=2))

        assert results == {'a', 'b'}

