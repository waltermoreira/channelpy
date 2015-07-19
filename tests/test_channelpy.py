import os

import pytest
import channelpy


BROKER_URI = os.environ['BROKER']


@pytest.fixture(scope='module')
def anon_ch(request):

    ch = channelpy.Channel(uri=BROKER_URI)

    def fin():
        ch2 = channelpy.Channel(name=ch._name, uri=BROKER_URI)
        ch2.delete()

    request.addfinalizer(fin)

    return ch


def test_anon_ch(anon_ch):
    assert anon_ch.uri == BROKER_URI
    assert anon_ch.put(5) is None
    assert anon_ch.get() == 5


def test_close(anon_ch):
    anon_ch.close()
    with pytest.raises(channelpy.ChannelClosedException):
        anon_ch.get()
    with pytest.raises(channelpy.ChannelClosedException):
        anon_ch.put(1)
    with pytest.raises(channelpy.ChannelClosedException):
        anon_ch.close()
    with pytest.raises(channelpy.ChannelClosedException):
        anon_ch.close_all()

