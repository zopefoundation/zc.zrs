##############################################################################
#
# Copyright (c) 2013 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

try:
    from hashlib import md5
except ImportError:
    from md5 import new as md5
from ZODB.TimeStamp import TimeStamp
from zope.testing import setupstack, renormalizing
from zc.zrs import loopback
import doctest
import ZEO.ClientStorage
import ZEO.tests.forker
import ZEO.tests.testZEO
import ZODB.blob
import ZODB.FileStorage
import ZODB.TimeStamp
import ZODB.tests.testblob
import ZODB.tests.testFileStorage
import ZODB.tests.StorageTestBase
import ZODB.utils
import cPickle
import logging
import mock
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
import transaction
import twisted.internet.base
import twisted.internet.error
import twisted.python.failure
import unittest
import zc.zk.testing
import zc.zrs.primary
import zc.zrs.reactor
import zc.zrs.secondary
import zc.zrs.sizedmessage


# start the reactor thread so that it isn't reported as left over:
zc.zrs.reactor.reactor()

def scan_from_back():
    r"""
Create the database:

    >>> from ZODB.DB import DB
    >>> import persistent.dict

    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> db = DB(fs)
    >>> conn = db.open()

    >>> for i in range(100):
    ...     conn.root()[i] = persistent.dict.PersistentDict()
    ...     commit()

Now, be evil, and muck up the beginning: :)

    >>> fs._file.seek(12)
    >>> fs._file.write('\xff'*8)
    >>> conn.root()[100] = persistent.dict.PersistentDict()
    >>> commit()

If we try to iterate from the beginning, we'll get an error:

    >>> condition = threading.Condition()
    >>> it = zc.zrs.primary.FileStorageIterator(fs, condition)
    >>> it.next()
    Traceback (most recent call last):
    ...
    CorruptedDataError: Error reading unknown oid.  Found '' at 4

    >>> def tid_from_time(t):
    ...     return eval(repr(TimeStamp(*(time.gmtime(t)[:5] + (t%60,)))))

    >>> tid = tid_from_time(time.time()-70)
    >>> zc.zrs.primary.FileStorageIterator(fs, condition, tid)
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    OverflowError: ...

But, if we iterate from near the end, we'll be OK:

    >>> tid = tid_from_time(time.time()-30)
    >>> it = zc.zrs.primary.FileStorageIterator(fs, condition, tid)
    >>> trans = it.next()
    >>> from ZODB import utils
    >>> print TimeStamp(trans.tid), [int(utils.u64(r.oid)) for r in trans]
    2007-03-21 20:34:09.000000 [0, 72]

    >>> print TimeStamp(tid)
    2007-03-21 20:34:08.000000

    >>> tid = tid_from_time(time.time()-29.5)
    >>> it = zc.zrs.primary.FileStorageIterator(fs, condition, tid)
    >>> trans = it.next()
    >>> from ZODB import utils
    >>> print TimeStamp(trans.tid), [int(utils.u64(r.oid)) for r in trans]
    2007-03-21 20:34:09.000000 [0, 72]

    >>> print TimeStamp(tid)
    2007-03-21 20:34:08.500000

    """

def primary_suspend_resume():
    """
The primary producer is supposed to be suspendable.

We'll create a file-storage:

    >>> import ZODB.FileStorage
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> from ZODB.DB import DB
    >>> db = DB(fs)

Now, we'll create a special transport that will output data when it is called:

    >>> class Reactor:
    ...     def callFromThread(self, f, *args, **kw):
    ...         f(*args, **kw)

    >>> class Transport:
    ...     def __init__(self):
    ...         self.reactor = Reactor()
    ...     def writeSequence(self, message):
    ...         message = message[1] # cheat. :)
    ...         if message:
    ...             message = cPickle.loads(message)
    ...             if type(message) is tuple:
    ...                 message = message[0]
    ...         print message
    ...     def registerProducer(self, producer, streaming):
    ...         print 'registered producer'
    ...     def unregisterProducer(self):
    ...         print 'unregistered producer'
    ...     def loseConnection(self):
    ...         print 'loseConnection'

And a producer based on the file storage and transport:

    >>> import time
    >>> producer = zc.zrs.primary.PrimaryProducer(
    ...            (fs, None, ZODB.utils.z64), Transport(), 'test'
    ...            ); time.sleep(0.1)
    registered producer
    T
    S
    <class 'persistent.mapping.PersistentMapping'>
    C

We get the initial transaction, because the producer starts producing
immediately.  Let's pause producing:

    >>> producer.pauseProducing()
    DEBUG zc.zrs.primary:
    test pausing

and we'll create another transaction:

    >>> conn = db.open()
    >>> ob = conn.root()
    >>> import persistent.mapping
    >>> ob.x = persistent.mapping.PersistentMapping()
    >>> commit()
    >>> producer.iterator.notify()
    >>> ob = ob.x
    >>> ob.x = persistent.mapping.PersistentMapping()
    >>> commit()
    >>> producer.iterator.notify()
    >>> time.sleep(0.1)

No output because we are paused.  Now let's resume:

    >>> producer.resumeProducing(); time.sleep(0.1)
    DEBUG zc.zrs.primary:
    test resuming
    T
    S
    <class 'persistent.mapping.PersistentMapping'>
    S
    <class 'persistent.mapping.PersistentMapping'>
    C
    T
    S
    <class 'persistent.mapping.PersistentMapping'>
    S
    <class 'persistent.mapping.PersistentMapping'>
    C

and pause again:

    >>> producer.pauseProducing()
    DEBUG zc.zrs.primary:
    test pausing

    >>> ob = ob.x
    >>> ob.x = persistent.mapping.PersistentMapping()
    >>> commit()
    >>> producer.iterator.notify()
    >>> time.sleep(0.1)

and resume:

    >>> producer.resumeProducing(); time.sleep(0.1)
    DEBUG zc.zrs.primary:
    test resuming
    T
    S
    <class 'persistent.mapping.PersistentMapping'>
    S
    <class 'persistent.mapping.PersistentMapping'>
    C

    >>> producer.close(); producer.thread.join()
    unregistered producer
    loseConnection

    >>> db.close()

"""

def secondary_close_edge_cases():
    r"""
There a number of cases to consider when closing a secondary:

- Closing while connecting

  The reactor.clients attribute has a list of secondaries that are
  "connecting". The reactor later attribute has requests to do things
  later.

    >>> reactor.clients
    []

    >>> reactor.later
    []

    >>> import zc.zrs.secondary
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> ss = zc.zrs.secondary.Secondary(fs, ('', 8000), reactor)
    INFO zc.zrs.secondary:
    Opening Data.fs ('', 8000)
    INFO zc.zrs.reactor:
    Starting factory <zc.zrs.secondary.SecondaryFactory instance>

    >>> len(reactor.clients)
    1

    >>> ss.close()
    INFO zc.zrs.secondary:
    Closing Data.fs ('', 8000)
    INFO zc.zrs.reactor:
    Stopping factory <zc.zrs.secondary.SecondaryFactory instance>

    >>> reactor.clients
    []

    >>> reactor.later
    []

- Closing while waiting to connect

  We'll reject the connection attempt, which will make the secondary
  queue a connection attempt for later:

    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> ss = zc.zrs.secondary.Secondary(fs, ('', 8000), reactor)
    INFO zc.zrs.secondary:
    Opening Data.fs ('', 8000)
    INFO zc.zrs.reactor:
    Starting factory <zc.zrs.secondary.SecondaryFactory instance>

    >>> reactor.reject()
    INFO zc.zrs.reactor:
    Stopping factory <zc.zrs.secondary.SecondaryFactory instance>

    >>> reactor.clients
    []

    >>> len(reactor.later)
    1

    >>> ss.close()
    INFO zc.zrs.secondary:
    Closing Data.fs ('', 8000)

    >>> reactor.doLater()
    INFO zc.zrs.reactor:
    Starting factory <zc.zrs.secondary.SecondaryFactory instance>
    INFO zc.zrs.reactor:
    Stopping factory <zc.zrs.secondary.SecondaryFactory instance at 0xb662b5cc>

    >>> reactor.later
    []

    >>> reactor.clients
    []

- Closing while connected but between transactions

    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> ss = zc.zrs.secondary.Secondary(fs, ('', 8000), reactor,
    ...         keep_alive_delay=60)
    INFO zc.zrs.secondary:
    Opening Data.fs ('', 8000)
    INFO zc.zrs.reactor:
    Starting factory <zc.zrs.secondary.SecondaryFactory instance>

    >>> connection = reactor.accept()
    INFO zc.zrs.secondary:
    IPv4Address(TCP, '127.0.0.1', 47248): Connected

    >>> reactor.later
    [<2 60 keep_alive () {}>]

    >>> reactor.clients
    []

    >>> ss.close() # doctest: +NORMALIZE_WHITESPACE
    INFO zc.zrs.secondary:
    Closing Data.fs ('', 8000)
    INFO zc.zrs.secondary:
    IPv4Address(TCP, '127.0.0.1', 47248):
    Disconnected <twisted.python.failure.Failure
      <class 'twisted.internet.error.ConnectionDone'>>

    >>> reactor.later
    []

    >>> reactor.clients
    []

- Closing while connected and recieving data

    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> ss = zc.zrs.secondary.Secondary(fs, ('', 8000), reactor,
    ...         keep_alive_delay=60)
    INFO zc.zrs.secondary:
    Opening Data.fs ('', 8000)
    INFO zc.zrs.reactor:
    Starting factory <zc.zrs.secondary.SecondaryFactory instance>

    >>> connection = reactor.accept()
    INFO zc.zrs.secondary:
    IPv4Address(TCP, '127.0.0.1', 47249): Connected

    >>> connection.read()
    'zrs2.0'
    >>> connection.read()
    '\x00\x00\x00\x00\x00\x00\x00\x00'

    >>> reactor.later
    [<3 60 keep_alive () {}>]

    >>> reactor.clients
    []

    >>> primary_fs = ZODB.FileStorage.FileStorage('primary.fs')
    >>> primary_data = zc.zrs.primary.FileStorageIterator(primary_fs)
    >>> from ZODB.DB import DB
    >>> primary_db = DB(primary_fs)
    >>> trans = primary_data.next()
    >>> connection.send(('T', (trans.tid, trans.status, trans.user,
    ...                        trans.description, trans._extension)))
    >>> record = trans.next
    ... connection.send(('S', (record.oid, record.tid, record.version,
    ...                        record.data, record.data_txn)))

    >>> ss.close() # doctest: +NORMALIZE_WHITESPACE
    INFO zc.zrs.secondary:
    Closing Data.fs ('', 8000)
    INFO zc.zrs.secondary:
    IPv4Address(TCP, '127.0.0.1', 47249):
    Disconnected <twisted.python.failure.Failure
      <class 'twisted.internet.error.ConnectionDone'>>

    >>> reactor.later
    []

    >>> reactor.clients
    []

    >>> print fs._transaction
    None

    >>> print fs._pos
    4

"""

def primary_data_input_errors():
    r"""
    There is no good reason for a primary to get a data input error. If
    it does, it should simply close the connection.

    >>> import ZODB.FileStorage, zc.zrs.primary
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> ps = zc.zrs.primary.Primary(fs, ('', 8000), reactor)
    INFO zc.zrs.primary:
    Opening Data.fs ('', 8000)

    >>> connection = reactor.connect((('', 8000)))
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245): Connected

    >>> connection.send("Hi") # doctest: +NORMALIZE_WHITESPACE
    ERROR zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245): Invalid protocol 'Hi'
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245):
    Disconnected <twisted.python.failure.Failure
    <class 'twisted.internet.error.ConnectionDone'>>
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245): Closed

    >>> connection = reactor.connect((('', 8000)))
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47246): Connected

    >>> connection.send("xxxxxxxxxxxxxxx") # doctest: +NORMALIZE_WHITESPACE
    ERROR zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47246): message too large: (8, 15L)
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47246):
    Disconnected <twisted.python.failure.Failure
      <class 'twisted.internet.error.ConnectionDone'>>
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47246): Closed

    >>> connection = reactor.connect((('', 8000)))
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47247): Connected

    >>> connection.send("zrs2.0")
    >>> connection.send("xxxxxxxxxxxxxxx") # doctest: +NORMALIZE_WHITESPACE
    ERROR zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47247): message too large: (8, 15L)
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47247):
    Disconnected <twisted.python.failure.Failure
      <class 'twisted.internet.error.ConnectionDone'>>
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47247): Closed

    >>> connection = reactor.connect((('', 8000)))
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47248): Connected

    >>> connection.send("zrs2.0")
    >>> connection.send("xxxxxxx") # doctest: +NORMALIZE_WHITESPACE
    ERROR zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47248): Invalid transaction id, 'xxxxxxx'
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47248):
    Disconnected <twisted.python.failure.Failure
      <class 'twisted.internet.error.ConnectionDone'>>
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47248): Closed

    Sending any message other than an empty message to the primary
    after the first two messages will result in an error:

    >>> connection = reactor.connect((('', 8000)))
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47249): Connected

    >>> connection.send("zrs2.0")
    >>> connection.send("\0"*8) # doctest: +NORMALIZE_WHITESPACE
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47249):
    start '\x00\x00\x00\x00\x00\x00\x00\x00' (1900-01-01 00:00:00.000000)

    >>> connection.send("")
    DEBUG zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47249): keep-alive
    >>> connection.send("")
    DEBUG zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47249): keep-alive

    >>> connection.send("Hi") # doctest: +ELLIPSIS
    ERROR zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47249): Too many messages
    ...
    IPv4Address(TCP, '127.0.0.1', 47249): Closed

    """

def secondary_data_input_errors():
    r"""

There is not good reason for a secondary to get a data input error. If
it does, it should simply close.

    >>> import zc.zrs.secondary
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> ss = zc.zrs.secondary.Secondary(fs, ('', 8000), reactor)
    INFO zc.zrs.secondary:
    Opening Data.fs ('', 8000)
    INFO zc.zrs.reactor:
    Starting factory <zc.zrs.secondary.SecondaryFactory instance>

    >>> connection = reactor.accept()
    INFO zc.zrs.secondary:
    IPv4Address(TCP, '127.0.0.1', 47245): Connected

    >>> connection.read()
    'zrs2.0'
    >>> connection.read()
    '\x00\x00\x00\x00\x00\x00\x00\x00'

    >>> connection.send('hi', raw=True)
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    CRITICAL zc.zrs.secondary:
    IPv4Address(TCP, '127.0.0.1', 47245): Input data error
    Traceback (most recent call last):
    ...
    BadPickleGet: 105
    INFO zc.zrs.secondary:
    IPv4Address(TCP, '127.0.0.1', 47245):
    Disconnected <twisted.python.failure.Failure
      <class 'twisted.internet.error.ConnectionDone'>>

    >>> reactor.later
    []

    >>> reactor.clients
    []

    """


def crashing_reactor_logs_as_such():
    """

We'll write a silly script that simply starts the reactor and tells it
to crash:

    >>> open('t.py', 'w').write('''
    ... import logging, time
    ... import twisted.internet
    ... import zc.zrs.reactor
    ...
    ... logging.getLogger().setLevel(1)
    ... handler = logging.StreamHandler(open('t.log', 'w'))
    ... logging.getLogger().addHandler(handler)
    ... zc.zrs.reactor.reactor()
    ... time.sleep(0.1)
    ... twisted.internet.reactor.callFromThread(twisted.internet.reactor.crash)
    ... time.sleep(0.1)
    ... #logging.error('failed')
    ... ''')

We'll run it:

    >>> env = os.environ.copy()
    >>> env['PYTHONPATH'] = os.pathsep.join(sys.path)
    >>> p = subprocess.Popen(
    ...       [sys.executable, 't.py'],
    ...       env=env)

It exits with a non-zero exit status:

    >>> bool(p.wait())
    False

And we get something in the log to the effect that it closed unexpectedly.

    >>> print open('t.log').read(),
    Main loop terminated.
    The twisted reactor quit unexpectedly

OTOH, if we exit without crashing:

    >>> open('t.py', 'w').write('''
    ... import logging, time
    ... import twisted.internet
    ... import zc.zrs.reactor
    ...
    ... logging.getLogger().setLevel(1)
    ... handler = logging.StreamHandler(open('t.log', 'w'))
    ... logging.getLogger().addHandler(handler)
    ... zc.zrs.reactor.reactor()
    ... time.sleep(0.1)
    ... ''')

    >>> p = subprocess.Popen(
    ...       [sys.executable, 't.py'],
    ...       env=env)

    >>> bool(p.wait())
    False

    >>> print open('t.log').read(),
    Main loop terminated.

    """

def leaking_file_handles_when_secondaries_disconnect():
    r"""

    >>> import sys, ZODB.FileStorage, zc.zrs.primary
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> ps = zc.zrs.primary.Primary(fs, ('', 8000), reactor)
    INFO zc.zrs.primary:
    Opening Data.fs ('', 8000)

    >>> oldrc = sys.getrefcount(zc.zrs.primary.FileStorageIterator)

    >>> from ZODB.DB import DB
    >>> import persistent.dict
    >>> db = DB(ps)
    >>> conn = db.open()
    >>> ob = conn.root()
    >>> for i in range(10):
    ...   ob.x = persistent.dict.PersistentDict()
    ...   commit()

    >>> for i in range(10):
    ...     connection = reactor.connect(('', 8000))
    ...     connection.send("zrs2.0")
    ...     connection.send("\0"*8)
    ...     _ = connection.read()
    ...     connection.close()
    ...     # doctest: +ELLIPSIS
    INFO ...

    >>> time.sleep(.01)
    >>> sys.getrefcount(zc.zrs.primary.FileStorageIterator) == oldrc
    True

    >>> db.close() # doctest: +ELLIPSIS
    INFO ...

    """

def close_writes_new_transactions():
    r"""
    We want close to try to write pending transactions, even if it
    means that close will take a long time.

    >>> import ZODB.FileStorage, zc.zrs.primary, persistent.dict
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> ps = zc.zrs.primary.Primary(fs, ('', 8000), reactor)
    INFO zc.zrs.primary:
    Opening Data.fs ('', 8000)

    >>> db = ZODB.DB(ps)
    >>> conn = db.open()
    >>> ob = conn.root()
    >>> ob.x = 0
    >>> commit()

    >>> committed = 2

    We'll open lots of connections: :)

    >>> nconnections = 10
    >>> connections = []
    >>> for i in range(nconnections):
    ...     connection = reactor.connect(('', 8000))
    ...     connection.send("zrs2.0")
    ...     connection.send("\0"*8)
    ...     connections.append(connection)
    ...     # doctest: +ELLIPSIS
    INFO zc.zrs.primary:...

    >>> import time
    >>> time.sleep(.1)

    >>> for i in range(300):
    ...     ob[i] = persistent.dict.PersistentDict()

    >>> commit()
    >>> committed += 1

    >>> db.close()
    ...     # doctest: +ELLIPSIS
    INFO zc.zrs.primary:...

    >>> for i in range(nconnections):
    ...     connection = connections[i]
    ...     trans = message_type = x = None
    ...     ntrans = 0
    ...     while connection.have_data():
    ...         message_type, data = connection.read()
    ...         if message_type == 'T':
    ...             trans = data
    ...             ntrans += 1
    ...         elif message_type == 'S':
    ...             x = connection.read(True)
    ...     if message_type != 'C' or ntrans != committed:
    ...         print i, message_type, ntrans

    """

def secondary_gives_a_tid_that_is_too_high():
    r"""
    We should error and close the connection if a secondary presents a
    tid that is higher than the largest tid seen by the primary.

    >>> import ZODB.FileStorage
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> import zc.zrs.primary
    >>> ps = zc.zrs.primary.Primary(fs, ('', 8000), reactor)
    INFO zc.zrs.primary:
    Opening Data.fs ('', 8000)

    >>> from ZODB.DB import DB
    >>> import persistent.dict
    >>> db = DB(ps)
    >>> conn = db.open()
    >>> ob = conn.root()
    >>> ob.x = persistent.dict.PersistentDict()
    >>> commit()

    >>> import ZODB.utils
    >>> too_high_tid = ZODB.utils.p64(ZODB.utils.u64(ob._p_serial)+1)

    >>> connection = reactor.connect(('', 8000))
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245): Connected

    >>> connection.send("zrs2.0") # doctest: +NORMALIZE_WHITESPACE
    >>> connection.send(too_high_tid); time.sleep(.01) # wait for thread :(
    ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245):
    start '\x03lk\x90\xf7wwx' (2007-03-21 20:32:58.000000)
    ERROR zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245):
    Traceback (most recent call last):
    ...
    TidTooHigh: '\x03lk\x90\xf7wwx'
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245):
    Disconnected
    <twisted.python.failure.Failure
      <class 'twisted.internet.error.ConnectionDone'>>

    """


class FauxScanControl(object):
    _v = True
    _n = 0
    def get(self):
        self._n += 1
        if self._n > 10:
            time.sleep(.01)
        if self._n > 20:
           print self._n
        return self._v
    def set(self, v):
        self._v = v
    not_stopped = property(get, set)

def scan_control_stops_scans_on_client_disconnects():
    r"""
    We want to be able to limit iterator scans if a client disconnects, for
    example, to limit impact on the server if a large scan is required.

    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> import persistent.dict
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> ob = conn.root()
    >>> for i in range(100):
    ...     ob[i] = persistent.dict.PersistentDict()
    ...     commit()
    >>> tid1 = ob._p_serial
    >>> for i in range(100, 200):
    ...     ob[i] = persistent.dict.PersistentDict()
    ...     commit()
    >>> tid2 = ob._p_serial
    >>> for i in range(200, 300):
    ...     ob[i] = persistent.dict.PersistentDict()
    ...     commit()

    >>> ps = zc.zrs.primary.Primary(fs, ('', 8000), reactor)
    INFO zc.zrs.primary:
    Opening Data.fs ('', 8000)

    >>> import time
    >>> ScanControl = zc.zrs.primary.ScanControl
    >>> zc.zrs.primary.ScanControl = FauxScanControl

    >>> connection = reactor.connect(('', 8000))
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245): Connected

    >>> connection.send("zrs2.0") # doctest: +NORMALIZE_WHITESPACE
    >>> connection.send(tid1) # doctest: +NORMALIZE_WHITESPACE
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245):
    start '\x03lk\x92\x9d\xdd\xdd\xdd' (2007-03-21 20:34:37.000000)
    >>> connection.loseConnection() # doctest: +NORMALIZE_WHITESPACE
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47245): Disconnected
    <twisted.python.failure.Failure
      <class 'twisted.internet.error.ConnectionDone'>>


    >>> connection = reactor.connect(('', 8000))
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47246): Connected

    >>> connection.send("zrs2.0") # doctest: +NORMALIZE_WHITESPACE
    >>> connection.send(tid2) # doctest: +NORMALIZE_WHITESPACE
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47246):
    start '\x03lk\x94H\x88\x88\x88' (2007-03-21 20:36:17.000000)

    >>> connection.loseConnection() # doctest: +NORMALIZE_WHITESPACE
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47246): Disconnected
    <twisted.python.failure.Failure
      <class 'twisted.internet.error.ConnectionDone'>>

    >>> time.sleep(.1)
    >>> zc.zrs.primary.ScanControl = ScanControl
    """

def record_iternext():
    """
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> ps = zc.zrs.primary.Primary(fs, ('', 8000), reactor)
    INFO zc.zrs.primary:
    Opening Data.fs ('', 8000)
    >>> ss = zc.zrs.secondary.Secondary(ps, ('', 9000), reactor)
    INFO zc.zrs.secondary:
    Opening Data.fs ('', 9000)
    INFO zc.zrs.reactor:
    Starting factory <zc.zrs.secondary.SecondaryFactory instance at 0xb654796c>

    >>> ps.record_iternext == fs.record_iternext
    True

    >>> ss.record_iternext == fs.record_iternext
    True

    """

def is_blob_record():
    r"""
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs', blob_dir='blobs')
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> conn.root()['blob'] = ZODB.blob.Blob()
    >>> transaction.commit()
    >>> zc.zrs.primary.is_blob_record(fs.load(ZODB.utils.p64(0), '')[0])
    False
    >>> zc.zrs.primary.is_blob_record(fs.load(ZODB.utils.p64(1), '')[0])
    True

    An invalid pickle yields a false value:

    >>> zc.zrs.primary.is_blob_record("Hello world!")
    False
    >>> zc.zrs.primary.is_blob_record('c__main__\nC\nq\x01.')
    False

    >>> db.close()
    """

def secondary_replicate_from_old_zrs_that_doesnt_send_checksums():
    r"""
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')

    >>> import zc.zrs.secondary
    >>> ss = zc.zrs.secondary.Secondary(fs, ('', 8000), reactor,
    ...                                 check_checksums=False)
    INFO zc.zrs.secondary:
    Opening Data.fs ('', 8000)
    INFO zc.zrs.reactor:
    Starting factory <zc.zrs.secondary.SecondaryFactory instance>

    >>> connection = reactor.accept()
    INFO zc.zrs.secondary:
    IPv4Address(TCP, '127.0.0.1', 47245): Connected

    >>> connection.read()
    'zrs2.0'

    >>> connection.read()
    '\x00\x00\x00\x00\x00\x00\x00\x00'

    >>> primary_fs = ZODB.FileStorage.FileStorage('primary.fs')
    >>> import zc.zrs.primary
    >>> primary_data = zc.zrs.primary.FileStorageIterator(primary_fs)

    >>> from ZODB.DB import DB
    >>> primary_db = DB(primary_fs)

    >>> trans = primary_data.next()

    >>> connection.send(('T', (trans.tid, trans.status, trans.user,
    ...                        trans.description, trans._extension)))

    >>> for record in trans:
    ...     connection.send(('S', (record.oid, record.tid, record.version,
    ...                            record.data_txn)))
    ...     connection.send(record.data, raw=True)

    >>> connection.send(('C', ()))

    >>> _ = fs.load('\0'*8, '')

    """

def secondary_gets_extension_data():
    """
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')

    >>> import zc.zrs.secondary
    >>> ss = zc.zrs.secondary.Secondary(fs, ('', 8000), reactor,
    ...                                 check_checksums=False)
    INFO zc.zrs.secondary:
    Opening Data.fs ('', 8000)
    INFO zc.zrs.reactor:
    Starting factory <zc.zrs.secondary.SecondaryFactory instance>

    >>> for n in ss.getExtensionMethods():
    ...     if getattr(ss, n) != getattr(fs, n):
    ...         print 'oops'

    >>> ss.close()
    INFO zc.zrs.secondary:
    Closing Data.fs ('', 8000)
    INFO zc.zrs.reactor:
    Stopping factory <zc.zrs.secondary.SecondaryFactory instance at 0x2bebb00>
    """

class DelayedCall:

    def __init__(self, later, n, delay, func, args, kw):
        self.later, self.n, self.delay = later, n, delay
        self.func, self.args, self.kw = func, args, kw

    def __repr__(self):
        return "<%s %s %s %r %r>" % (
            self.n, self.delay, self.func.__name__, self.args, self.kw)

    def cancel(self):
        if self in self.later:
            self.later.remove(self)
        self.later = None

    def active(self):
        return self.later is not None

    def __call__(self):
        self.later = None
        self.func(*self.args, **self.kw)

class TestReactor:

    def __init__(self):
        self._factories = {}
        self.clients = []
        self.client_port = 47245
        self.later = []

    def listenTCP(self, port, factory, backlog=50, interface=''):
        addr = interface, port
        assert addr not in self._factories
        self._factories[addr] = factory
        return TestListener(self, addr)

    def connect(self, addr):
        proto = self._factories[addr].buildProtocol(addr)
        transport = PrimaryTransport(self, addr, self.client_port, proto)
        self.client_port += 1
        proto.makeConnection(transport)
        return transport

    lock = threading.RLock()
    def callFromThread(self, f, *a, **k):
        self.lock.acquire()
        try:
            f(*a, **k)
        finally:
            self.lock.release()

    callLater_n = 0
    def callLater(self, delay, f, *a, **k):
        self.callLater_n += 1
        f = DelayedCall(self.later, self.callLater_n, delay, f, a, k)
        self.later.append(f)
        return f

    def doLater(self):
        l = len(self.later)
        later = self.later[:l]
        del self.later[:l]
        while later:
            self.callFromThread(later.pop(0))

    def connectTCP(self, host, port, factory, timeout=30):
        addr = host, port
        connector = TestConnector(self, addr, factory)
        connector.connect()
        return connector

    def accept(self):
        connector = self.clients.pop(0)
        return connector.accept()

    def reject(self):
        connector = self.clients.pop(0)
        return connector.reject()

class TestListener:

    def __init__(self, reactor, addr):
        self.reactor = reactor
        self.addr = addr

    def stopListening(self):
        if self.addr in self.reactor._factories:
            del self.reactor._factories[self.addr]

close_reason = twisted.python.failure.Failure(
    twisted.internet.error.ConnectionDone())

class MessageTransport:

    def __init__(self, reactor, addr, port, proto=None):
        self.data = ''
        self.cond = threading.Condition()
        self.closed = False
        self.reactor = reactor
        self.addr = addr
        self.peer = "IPv4Address(TCP, '127.0.0.1', %s)" % port
        self.proto = proto
        self.init_md5('\x00\x00\x00\x00\x00\x00\x00\x00')

    def getPeer(self):
        return self.peer

    def write(self, data):
        self.cond.acquire()
        self.data += data
        self.cond.notifyAll()
        self.cond.release()

    def writeSequence(self, data):
        self.write(''.join(data))

    def read(self):
        self.cond.acquire()

        if len(self.data) < 4:
            self.cond.wait(5)
            assert len(self.data) >= 4
        l, = struct.unpack(">I", self.data[:4])
        self.data = self.data[4:]

        if len(self.data) < l:
            self.cond.wait(5)
            assert len(self.data) >= l, (l, len(self.data))
        result = self.data[:l]
        self.data = self.data[l:]

        self.cond.release()

        return result

    def init_md5(self, data):
        self.md5 = md5(data)

    def send(self, data):
        record = zc.zrs.sizedmessage.marshal(data)
        self.md5.update(record[4:])
        dataReceived = self.proto.dataReceived

        # send data in parts to try to confuse the protocol
        n = 1
        while record:
            data, record = record[:n], record[n:]
            if data and not self.closed:
                dataReceived(data)
            n *= 2

    def have_data(self):
        return bool(self.data)

    def loseConnection(self):
        self.closed = True
        self.proto.connectionLost(close_reason)

    producer = None
    def registerProducer(self, producer, streaming):
        self.producer = producer

    def unregisterProducer(self):
        if self.producer is not None:
            self.producer = None
            if self.closed:
                self.proto.connectionLost(close_reason)

    def close(self):
        self.producer.stopProducing()
        self.proto.connectionLost(close_reason)

class PrimaryTransport(MessageTransport):

    def read(self, raw=False):
        data = MessageTransport.read(self)
        if raw:
            return data
        return cPickle.loads(data)

class SecondaryTransport(MessageTransport):

    def send(self, data, raw=False):
        if not raw:
            data = cPickle.dumps(data)
        MessageTransport.send(self, data or '')

    def fail(self):
        self.connectionLost('failed')

    def connectionLost(self, reason):
        """Notify of a lost connection

        This is a distillation of what happens in a tcp Client transport.
        """
        self.proto.connectionLost(reason)
        self.connector.connectionLost(reason)


    def failIfNotConnected(self, reason):
        if self.connector in self.reactor.clients:
            self.reactor.clients.remove(self.connector)
        self.connector.connectionFailed(reason)

class TestConnector(twisted.internet.base.BaseConnector):

    def __init__(self, reactor, addr, factory, timeout=None):
        twisted.internet.base.BaseConnector.__init__(
            self, factory, timeout, reactor)
        self.addr = addr

    def _makeTransport(self):
        reactor, addr = self.reactor, self.addr

        if addr in reactor._factories:
            # We have a server and a client.  We'll hook them together via
            # a loopback mechanism
            proto = self.buildProtocol(addr)
            server = reactor._factories[addr].buildProtocol(addr)
            loopback.loopbackAsync(server, proto, self)
            transport = proto.transport
        else:
            reactor.clients.append(self)
            transport = SecondaryTransport(reactor, addr, reactor.client_port)
            reactor.client_port += 1

        transport.connector = self
        return transport

    def accept(self):
        reactor, addr, transport = self.reactor, self.addr, self.transport
        proto = self.buildProtocol(addr)
        transport.proto = proto
        transport.connector = self
        proto.makeConnection(transport)
        return transport

    def reject(self):
        self.connectionFailed('rejected')


class Stdout:
    def write(self, data):
        sys.stdout.write(data)
    def flush(self):
        sys.stdout.flush()

stdout_handler = logging.StreamHandler(Stdout())
stdout_handler.setFormatter(logging.Formatter(
    "%(levelname)s %(name)s:\n%(message)s"))

def join(old):
    # Wait for any new threads created during a test to die.
    for thread in threading.enumerate():
        if thread not in old:
            thread.join(1.0)

def setUpTime(test):
    setupstack.register(test, join, threading.enumerate())
    setupstack.setUpDirectory(test)
    global now
    now = time.mktime((2007, 3, 21, 15, 32, 57, 2, 80, 0))
    oldtime = time.time
    setupstack.register(test, lambda : setattr(time, 'time', oldtime))
    time.time = lambda : now
    def commit():
        global now
        now += 1
        transaction.commit()
    test.globs['commit'] = commit

def setUp(test):
    setUpTime(test)

    test.globs['reactor'] = TestReactor()

    logger = logging.getLogger('zc.zrs')
    logger.setLevel(1)
    setupstack.register(test, logger.setLevel, 0)
    logger.addHandler(stdout_handler)
    setupstack.register(test, logger.removeHandler, stdout_handler)

##############################################################################
# Reuse ZODB Storage Tests

from ZODB.tests import StorageTestBase
from ZODB.tests import BasicStorage
from ZODB.tests import TransactionalUndoStorage
from ZODB.tests import RevisionStorage
from ZODB.tests import PackableStorage
from ZODB.tests import Synchronization
from ZODB.tests import ConflictResolution
from ZODB.tests import HistoryStorage
from ZODB.tests import IteratorStorage
from ZODB.tests import PersistentStorage
from ZODB.tests import MTStorage
from ZODB.tests import ReadOnlyStorage

class TestPrimary(zc.zrs.primary.Primary):

    _transaction_count = 0
    def tpc_finish(self, *args):
        self._transaction_count += 1
        zc.zrs.primary.Primary.tpc_finish(self, *args)
        if self._transaction_count%20 == 0:
            # be annoying and disconnect our clients every 20 transactions.
            # Hee hee.
            # Before we do that though, we'll call doLater on our reactor to
            # give previously disconnected clients a chance to reconnect.
            self._reactor.doLater()
            for instance in self._factory.instances:
                instance._stop()


class BasePrimaryStorageTests(StorageTestBase.StorageTestBase):

    use_blob_storage = False

    def setUp(self):
        self.__pack = None

        self.globs = {}
        setupstack.register(self, join, threading.enumerate())
        setupstack.setUpDirectory(self)
        self.globs['reactor'] = TestReactor()
        self.open(create=1)

    def tearDown(self):
        # Give any disconnected clients a chance to reconnect.
        self._storage._reactor.doLater()

        self._storage.close()
        reactor = self.globs['reactor']
        self.assert_(not reactor._factories) # Make sure we're not listening
        setupstack.tearDown(self)
        self.globs.clear()

    __port = 8000

    def catch_up(self, fs1, fs2):
        for i in range(2000):
            self._storage._reactor.doLater()
            if i:
                time.sleep(0.01)

            if fs1.lastTransaction() <= fs2.lastTransaction():
                return # caught up

            l1 = list(fs1.iterator())
            if not l1:
                return
            l2 = list(fs2.iterator())
            if l2:
                if (l1[-1].tid <= l2[-1].tid):
                    return

        raise AssertionError("Can't catch up.")


    def open(self, **kwargs):
        reactor = self.globs['reactor']
        self.__port += 1
        addr = '', self.__port
        if self.use_blob_storage:
            self.__pfs = ZODB.FileStorage.FileStorage(
                'primary.fs', blob_dir='primary_blobs', **kwargs)
            self.__sfs = ZODB.FileStorage.FileStorage(
                'secondary.fs', blob_dir='secondary_blobs')
        else:
            self.__pfs = ZODB.FileStorage.FileStorage('primary.fs', **kwargs)
            self.__sfs = ZODB.FileStorage.FileStorage('secondary.fs')

        self._storage = self._wrap(TestPrimary(self.__pfs, addr, reactor))
        self.__ss = zc.zrs.secondary.Secondary(
            self._wrap(self.__sfs), addr, reactor)

        p_pack = self._storage.pack
        def pack(*args, **kw):
            p_pack(*args, **kw)
            self.__pack = True
        self._storage.pack = pack

        p_close = self._storage.close
        def close():
            self.catch_up(self.__pfs, self.__sfs)

            if self.__pack:
                comparedbs_packed(self, self.__pfs, self.__sfs)
            else:
                self.__comparedbs(self.__pfs, self.__sfs)

            # Now, just recover from scratch to make sure we can:
            if self.use_blob_storage:
                sfs = ZODB.FileStorage.FileStorage(
                    'secondary2.fs', blob_dir='secondarys_blobs')
            else:
                sfs = ZODB.FileStorage.FileStorage('secondary2.fs')

            ss = zc.zrs.secondary.Secondary(sfs, addr, reactor)
            self.catch_up(self.__pfs, sfs)
            self.__comparedbs(self.__pfs, sfs)
            ss.close()

            p_close()
            self.__ss.close()

        self._storage.close = close

    _wrap = lambda self, s: s

    def __comparedbs(self, fs1, fs2):
        if fs1._pos != fs2._pos:
            time.sleep(0.1)
        self.assertEqual(fs1._pos, fs2._pos)

        self.compare(fs1, fs2)



def comparedbs_packed(self, fs1, fs2):

    # The primary was packed.  This introduces some significant
    # complications.  The secondary can end up with packed records
    # following unpacked records, depending on timing.  Or, it can
    # end up with packed records that don't exist on the primary,
    # except in pathological cases.  This can lead to the
    # secondary having extra records not present on the
    # primary. In any case it isn't reasonable to expect the
    # secondary to have exactly the same records as the primary.
    # We'll do a looser comparison that requires:
    #
    # - The primary's records are a subset of the secondary's, and
    # - The primary and secondary have the same current records.
    time.sleep(0.01)

    data2 = {}
    current2 = {}
    for trans in fs2.iterator():
        objects = {}
        data2[trans.tid] = (trans.user, trans.description,
                            trans._extension, objects)
        for r in trans:
            objects[r.oid] = r.tid, r.version, r.data
            current2[r.oid] = trans.tid


    current1 = {}
    for trans in fs1.iterator():
        self.assertEqual(
            data2[trans.tid][:3],
            (trans.user, trans.description, trans._extension),
            )
        objects = {}
        for r in trans:
            objects[r.oid] = r.tid, r.version, r.data
            current2[r.oid] = trans.tid

        objects2 = data2[trans.tid][3]
        for oid in objects:
            self.assertEqual(objects2[r.oid], objects[r.oid])

    for oid, tid in current1.items():
        self.assertEqual(current2[oid], tid)

def tsr(tid):
    return repr(str(TimeStamp(tid)))

def show_fs(fs):
    for t in fs.iterator():
        print tsr(t.tid), repr(t.status), repr(t.description), t._pos
        for r in t:
            print ' ', ZODB.utils.u64(r.oid), tsr(r.tid), repr(r.version),
            print r.data and len(r.data), r.data_txn and tsr(r.data_txn), r.pos

class PrimaryStorageTests(
    BasePrimaryStorageTests,
    BasicStorage.BasicStorage,
    TransactionalUndoStorage.TransactionalUndoStorage,
    RevisionStorage.RevisionStorage,
    PackableStorage.PackableStorage,
    PackableStorage.PackableUndoStorage,
    Synchronization.SynchronizedStorage,
    ConflictResolution.ConflictResolvingStorage,
    ConflictResolution.ConflictResolvingTransUndoStorage,
    HistoryStorage.HistoryStorage,
    IteratorStorage.IteratorStorage,
    IteratorStorage.IteratorDeepCompare,
    IteratorStorage.ExtendedIteratorStorage,
    PersistentStorage.PersistentStorage,
    MTStorage.MTStorage,
    ReadOnlyStorage.ReadOnlyStorage
    ):
    pass

class PrimaryStorageTestsWithBobs(PrimaryStorageTests):

    use_blob_storage = True

class PrimaryHexStorageTestsWithBobs(PrimaryStorageTestsWithBobs):

    def _wrap(self, s):
        return zc.zrs.xformstorage.HexStorage(s)

#
##############################################################################

##############################################################################
# ZEO Tests

class ZEOTests(ZEO.tests.testZEO.FullGenericTests):

    def getConfig(self):
        port = self._ZEOTests_port = ZEO.tests.testZEO.get_port()
        return """
        %%import zc.zrs

        <zrs>
          replicate-to %s
          <filestorage 1>
            path primary.fs
          </filestorage>
        </zrs>
        """ % port

    def setUp(self):
        self.globs = {}
        setupstack.register(self, join, threading.enumerate())
        setupstack.setUpDirectory(self)
        ZEO.tests.testZEO.FullGenericTests.setUp(self)
        self.__sfs = ZODB.FileStorage.FileStorage('secondary.fs')
        self.__s = zc.zrs.secondary.Secondary(
            self._wrap(self.__sfs),
            ('', self._ZEOTests_port), reconnect_delay=0.1)
        zc.zrs.reactor.reactor().callLater(0.1, self.__breakConnection)

    def __breakConnection(self):
        try:
            f = self.__s._factory.instance.transport.loseConnection
        except AttributeError:
            return
        f('broken by test')

    def tearDown(self):

        # Check whether secondary has same data as primary:
        for i in range(1000):
            try:
                fsp = ZODB.FileStorage.FileStorage('primary.fs',
                                                   read_only=True)
                comparedbs_packed(self, fsp, self.__sfs)
                break
            except:
                # Hm. Maybe we didn't wait long enough before starting
                # the compare.  Let's wait a tad longer.
                if i == 999:
                    raise
                time.sleep(.1)

        fsp.close()
        self.__s.close()
        ZEO.tests.testZEO.FullGenericTests.tearDown(self)
        setupstack.tearDown(self)

    _wrap = lambda self, s: s


class BlobWritableCacheTests(ZEO.tests.testZEO.BlobWritableCacheTests):

    def getConfig(self):
        port = self._ZEOTests_port = ZEO.tests.testZEO.get_port()
        return """
        %%import zc.zrs

        <zrs>
          replicate-to %s
          <filestorage 1>
            blob-dir blobs
            path primary.fs
          </filestorage>
        </zrs>
        """ % port

class ZEOHexTests(ZEOTests):

    def getConfig(self):
        port = self._ZEOTests_port = ZEO.tests.testZEO.get_port()
        return """
        %%import zc.zrs
        %%import zc.zrs.xformstorage

        <hexstorage>
          <zrs>
            replicate-to %s
            <filestorage 1>
              path primary.fs
            </filestorage>
          </zrs>
        </hexstorage>
        """ % port

    def _wrap(self, s):
        return zc.zrs.xformstorage.HexStorage(s)

class ZEOHexClientHexTests(ZEOHexTests):

    def _wrap_client(self, s):
        return zc.zrs.xformstorage.HexStorage(s)

class ZEOHexClientTests(ZEOHexTests):

    def getConfig(self):
        port = self._ZEOTests_port = ZEO.tests.testZEO.get_port()
        return """
        %%import zc.zrs
        %%import zc.zrs.xformstorage

        <serverhexstorage>
          <zrs>
            replicate-to %s
            <filestorage 1>
              path primary.fs
            </filestorage>
          </zrs>
        </serverhexstorage>
        """ % port

    def _wrap(self, s):
        return zc.zrs.xformstorage.HexStorage(s, True)

    def _wrap_client(self, s):
        return zc.zrs.xformstorage.HexStorage(s)


#
##############################################################################

##############################################################################
# Monitor test

def monitor_setUp(test):
    test.globs['__logging_dict'] = dict(logging.__dict__)
    reload(logging)

def monitor_tearDown(test):
    logging.__dict__.clear()
    logging.__dict__.update(test.globs['__logging_dict'])

#
##############################################################################

##############################################################################
# Run ZODB hex tests with our hex storage. We're doing this mainly
# to test our hex storage so we can use it to test zrs. :)
# The reason we need our own hex storage is that we need one
# that won't choke if records are already hexed to test some of the scenarios
# we care about.

class FileStorageHexTests(ZODB.tests.testFileStorage.FileStorageTests):
    def open(self, **kwargs):
        self._storage = zc.zrs.xformstorage.HexStorage(
            ZODB.FileStorage.FileStorage('FileStorageTests.fs',**kwargs))

class FileStorageHexTestsWithBlobsEnabled(
    ZODB.tests.testFileStorage.FileStorageTests
    ):
    def open(self, **kwargs):
        if 'blob_dir' not in kwargs:
            kwargs = kwargs.copy()
            kwargs['blob_dir'] = 'blobs'
        ZODB.tests.testFileStorage.FileStorageTests.open(self, **kwargs)
        self._storage = zc.zrs.xformstorage.HexStorage(self._storage)

class FileStorageHexRecoveryTest(
    ZODB.tests.testFileStorage.FileStorageRecoveryTest):
    def setUp(self):
        ZODB.tests.StorageTestBase.StorageTestBase.setUp(self)
        self._storage = zc.zrs.xformstorage.HexStorage(
            ZODB.FileStorage.FileStorage("Source.fs", create=True))
        self._dst = zc.zrs.xformstorage.HexStorage(
            ZODB.FileStorage.FileStorage("Dest.fs", create=True))

class FileStorageHexTests(ZEO.tests.testZEO.FileStorageTests):
    _expected_interfaces = (
        ('ZODB.interfaces', 'IStorageRestoreable'),
        ('ZODB.interfaces', 'IStorageIteration'),
        ('ZODB.interfaces', 'IStorageUndoable'),
        ('ZODB.interfaces', 'IStorageCurrentRecordIteration'),
        ('ZODB.interfaces', 'IExternalGC'),
        ('ZODB.interfaces', 'IStorage'),
        ('ZODB.interfaces', 'IStorageWrapper'),
        ('zope.interface', 'Interface'),
        )

    def getConfig(self):
        return """\
        %import zc.zrs.xformstorage
        <hexstorage>
        <filestorage 1>
        path Data.fs
        </filestorage>
        </hexstorage>
        """

class FileStorageClientHexTests(FileStorageHexTests):

    def getConfig(self):
        return """\
        %import zc.zrs.xformstorage
        <serverhexstorage>
        <filestorage 1>
        path Data.fs
        </filestorage>
        </serverhexstorage>
        """

    def _wrap_client(self, client):
        return ZODB.tests.hexstorage.HexStorage(client)

#
##############################################################################

old_base64 = """
RlMyMQOsOkgWySW7AAAAAAAAAJYgAAAAGQAAaW5pdGlhbCBkYXRhYmFzZSBjcmVhdGlvbgAAAAAA
AAAAA6w6SBbJJbsAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAPChjcGVyc2lzdGVudC5tYXBwaW5n
ClBlcnNpc3RlbnRNYXBwaW5nCnEBTnQufXECVQRkYXRhcQN9cQRzLgAAAAAAAACW
"""
current_base64 = """\
RlMyMQOsOkk7V3+IAAAAAAAAAJYgAAAAGQAAaW5pdGlhbCBkYXRhYmFzZSBjcmVhdGlvbgAAAAAA
AAAAA6w6STtXf4gAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAPChjcGVyc2lzdGVudC5tYXBwaW5n
ClBlcnNpc3RlbnRNYXBwaW5nCnEBTnQufXECVQRkYXRhcQN9cQRzLgAAAAAAAACW
"""

def setUpZK(test):
    setupstack.setUpDirectory(test)
    setupstack.context_manager(
        test, mock.patch('socket.getfqdn')).return_value = 'localhost'
    zc.zk.testing.setUp(test)

def setUpNagios(test):
    setUpZK(test)
    for name in 'old', 'current':
        with open(name+'.fs', 'w') as f:
            f.write(globals()[name+'_base64'].decode('base64'))
    import zc.zk.monitor
    del zc.zk.monitor._servers[:]

def setUpZKConfig(test):
    setupstack.context_manager(test, mock.patch('ZODB.FileStorage.FileStorage'))
    setupstack.context_manager(test, mock.patch('zc.zrs.zk.Primary'))
    setupstack.context_manager(test, mock.patch('zc.zrs.zk.Secondary'))


def test_suite():
    suite = unittest.TestSuite((
        doctest.DocFileSuite(
            'fsiterator.test',
            'primary.test', 'primary-blob.test', 'primary-blobstorage.test',
            'secondary.test', 'secondary-blob.test',
            'secondary-blobstorage.test',
            setUp=setUp, tearDown=setupstack.tearDown,
            checker=renormalizing.RENormalizing([
                (re.compile(' at 0x[a-fA-F0-9]+'), ''),
                ]),
            ),
        doctest.DocFileSuite(
            'zk.test',
            setUp=setUpZK, tearDown=setupstack.tearDown,
            checker=renormalizing.RENormalizing([
                (re.compile(r"PrimaryFactory starting on \d+"),
                 "PrimaryFactory starting on EPORT"),
                (re.compile(' at 0x[a-fA-F0-9]+'), ''),
                (re.compile(r"/127.0.0.1:\d+"), "/127.0.0.1:PORT"),
                (re.compile(r"/localhost:\d+"), "/127.0.0.1:PORT"),
                (re.compile(r"'127.0.0.1', \d+"), "'127.0.0.1', PORT"),
                (re.compile(r"pid = \d+"), "pid = PORT"),
                ]),
            ),
        doctest.DocFileSuite(
            'zkconfig.test',
            setUp=setUpZKConfig, tearDown=setupstack.tearDown,
            ),
        doctest.DocFileSuite(
            'config.test',
            checker=renormalizing.RENormalizing([
                (re.compile(' at 0x[a-fA-F0-9]+'), ''),
                ]),
            setUp=setUpTime, tearDown=setupstack.tearDown,
            ),
        doctest.DocFileSuite(
            'nagios.rst',
            checker=renormalizing.RENormalizing([
                (re.compile(r"localhost:\d+"), "127.0.0.1:PORT"),
                (re.compile(r"\[Errno \d+\]"), "[Errno NN]"),
                ]),
            setUp=setUpNagios, tearDown=setupstack.tearDown,
            ),
        doctest.DocTestSuite(
            setUp=setUp, tearDown=setupstack.tearDown,
            checker=renormalizing.RENormalizing([
                (re.compile(' at 0x[a-fA-F0-9]+'), ''),
                ]),
            ),
        ))

    def make(class_, *args):
        s = unittest.makeSuite(class_, *args)
        s.layer = ZODB.tests.util.MininalTestLayer(class_.__name__)
        suite.addTest(s)

    make(PrimaryStorageTests, "check")
    make(PrimaryStorageTestsWithBobs, "check")
    make(ZEOTests, "check")
    make(BlobWritableCacheTests, "check")

    try:
        from ZODB.tests import hexstorage
        import zc.zrs.xformstorage

        make(FileStorageHexTests, "check")
        make(FileStorageHexTestsWithBlobsEnabled, "check")
        make(FileStorageHexRecoveryTest, "check")
        make(ZEOHexTests, "check")
        make(ZEOHexClientHexTests, "check")
        make(ZEOHexClientTests, "check")

        suite.addTest(ZODB.tests.testblob.storage_reusable_suite(
            'BlobFileHexStorage',
            lambda name, blob_dir:
            zc.zrs.xformstorage.HexStorage(
                ZODB.FileStorage.FileStorage(
                    '%s.fs' % name, blob_dir=blob_dir)),
            test_blob_storage_recovery=True,
            test_packing=True,
            ))
    except ImportError:
        pass # pre wrapper-support ZODB

    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

