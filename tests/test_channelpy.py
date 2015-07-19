import os

import pytest
import channelpy


BROKER_URI = os.environ['BROKER']


@pytest.fixture(scope='module')
def anon_ch(request):

    ch = channelpy.Channel(uri=BROKER_URI)

    def fin():
        ch.delete()

    return ch


def test_anon_ch(anon_ch):
    assert anon_ch.uri == BROKER_URI
    assert anon_ch.put(5) is None
    assert anon_ch.get() == 5
