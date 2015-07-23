=========
ChannelPy
=========

A basic implementation of Go_ inspired channels.

Use these channels to communicate across Python threads, processes, or
hosts, independent of network location.  Channels are first class
objects that can be passed themselves through the channels.

The current implementation uses RabbitMQ_ as a broker, but it can be
swapped by other queues or even a peer-to-peer transport mechanism
such as ZeroMQ_.  See `the implementation details`_.


Prerequisite
============

Start a RabbitMQ instance by executing the following command in the
root directory of this repository:

.. code-block:: bash

   docker-compose up -d


Quickstart
==========

- Create a channel with:

  .. code-block:: python

     >>> from channelpy import Channel
     >>> ch = Channel()

- Put and get objects to and from the channel:

  .. code-block:: python

     >>> ch.put('foo')
     >>> ch.get()
     'foo'
     >>> ch.get(timeout=1)
     # raises ChannelTimeoutException since channel is empty

- Pass channels into channels:

  .. code-block:: python

     >>> ch1 = Channel()
     >>> ch1.put(5)
     >>> ch2 = Channel()
     >>> ch2.put(ch1)
     >>> x = ch2.get()
     >>> x.get()
     5

- Channels can be instantiated by name:

  .. code-block:: python

     >>> ch = Channel()
     >>> ch.name
     'daa0a490f9254c69883335c9f925d74f'
     >>> another = Channel(name=ch.name)
     >>> another.put('foo')
     >>> ch.get()
     'foo'

  Or create them with a specific name:

  .. code-block:: python

     >>> ch = Channel(name='my_channel')

- Multiple consumers and producers can be attached to a
  channel. Messages are delivered in round-robin fashion to
  consumers. However, a ``close_all` can be delivered to all the
  consumers to signal a full termination of the channel.

  .. code-block:: python

     >>> ch = Channel()
     >>> def f(c):
     ...     while True:
     ...         print(c.get())
     ...
     >>> threading.Thread(target=f, args=(ch,)).start()
     >>> threading.Thread(target=f, args=(ch,)).start()
     >>> ch.put(4)        # one of the threads will print 4
     >>> ch.close_all()   # ChannelClosedException is raised in all threads

- The broker to use can be configured at instantiation time or by
  using the config file ``~/.channelpy.yml``.  For example:

  .. code-block:: YAML

     connection: RabbitConnection
       uri: amqp://192.168.35.10:5672



Tests
=====

Run the tests with:

.. code-block:: bash

   $ BROKER='amqp://localhost:5672' py.test -v


.. _implementation_details:

Implementation Details
======================

To be written.


.. _Go: http://golang.org/
.. _ZeroMQ: http://zeromq.org/
.. _RabbitMQ: http://www.rabbitmq.com/
.. _the implementation details: #implementation-details
