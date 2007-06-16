##############################################################################
#
# Copyright (c) 2006 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import cPickle, logging, os, re, shutil, struct, subprocess, sys
import tempfile, threading, time, unittest

import transaction
from ZODB.TimeStamp import TimeStamp
import ZODB.utils
import ZODB.FileStorage

import ZEO.tests.testZEO

from zope.testing import doctest, setupstack, renormalizing

import twisted.internet.base
import twisted.internet.error
from zrstests import loopback
import twisted.python.failure

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
    ...     return repr(TimeStamp(*(time.gmtime(t)[:5] + (t%60,))))

    >>> tid = tid_from_time(time.time()-70)
    >>> zc.zrs.primary.FileStorageIterator(fs, condition, tid)
    Traceback (most recent call last):
    ...
    OverflowError: long too big to convert

But, if we iterate from near the end, we'll be OK:

    >>> tid = tid_from_time(time.time()-30)
    >>> it = zc.zrs.primary.FileStorageIterator(fs, condition, tid)
    >>> trans = it.next()
    >>> from ZODB import utils
    >>> print TimeStamp(trans.tid), [utils.u64(r.oid) for r in trans]
    2007-03-21 20:34:09.000000 [0L, 72L]

    >>> print TimeStamp(tid)
    2007-03-21 20:34:08.000000

    >>> tid = tid_from_time(time.time()-29.5)
    >>> it = zc.zrs.primary.FileStorageIterator(fs, condition, tid)
    >>> trans = it.next()
    >>> from ZODB import utils
    >>> print TimeStamp(trans.tid), [utils.u64(r.oid) for r in trans]
    2007-03-21 20:34:09.000000 [0L, 72L]

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

Now, we'll create an iterator:

    >>> iterator = zc.zrs.primary.FileStorageIterator(fs)

And a special transport that will output data when it is called:

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

And a producer based on the iterator and transport:

    >>> import time
    >>> producer = zc.zrs.primary.PrimaryProducer(iterator, Transport(), 'test'
    ...            ); time.sleep(0.1)
    registered producer
    T
    S
    <class 'persistent.mapping.PersistentMapping'>
    C

We get the initial transaction, because the producer starts producing
immediately.  Let's oause producing:

    >>> producer.pauseProducing()

and we'll create another transaction:

    >>> conn = db.open()
    >>> ob = conn.root()
    >>> import persistent.mapping
    >>> ob.x = persistent.mapping.PersistentMapping()
    >>> commit()
    >>> iterator.notify()
    >>> ob = ob.x
    >>> ob.x = persistent.mapping.PersistentMapping()
    >>> commit()
    >>> iterator.notify()
    >>> time.sleep(0.1)
    
No output because we are paused.  Now let's resume:

    >>> producer.resumeProducing(); time.sleep(0.1)
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
    >>> ob = ob.x
    >>> ob.x = persistent.mapping.PersistentMapping()
    >>> commit()
    >>> iterator.notify()
    >>> time.sleep(0.1)

and resume:

    >>> producer.resumeProducing(); time.sleep(0.1)
    T
    S
    <class 'persistent.mapping.PersistentMapping'>
    S
    <class 'persistent.mapping.PersistentMapping'>
    C

    >>> producer.close()
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
    >>> ss = zc.zrs.secondary.Secondary(fs, ('', 8000), reactor)
    INFO zc.zrs.secondary:
    Opening Data.fs ('', 8000)
    INFO zc.zrs.reactor:
    Starting factory <zc.zrs.secondary.SecondaryFactory instance>

    >>> connection = reactor.accept()
    INFO zc.zrs.secondary:
    IPv4Address(TCP, '127.0.0.1', 47248): Connected
    
    >>> reactor.later
    []
    
    >>> reactor.clients
    []

    >>> ss.close() # doctest: +NORMALIZE_WHITESPACE
    INFO zc.zrs.secondary:
    Closing Data.fs ('', 8000)
    INFO zc.zrs.secondary:
    IPv4Address(TCP, '127.0.0.1', 47248):
    Disconnected <twisted.python.failure.Failure
    twisted.internet.error.ConnectionDone>
        
    >>> reactor.later
    []
    
    >>> reactor.clients
    []

- Closing while connected and recieving data
    
    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> ss = zc.zrs.secondary.Secondary(fs, ('', 8000), reactor)
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
    []
    
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
    twisted.internet.error.ConnectionDone>
        
    >>> reactor.later
    []
    
    >>> reactor.clients
    []

    >>> print fs._transaction
    None
    
    >>> fs._pos
    4L

"""

def primary_data_input_errors():
    """
    There is not good reason for a primary to get a data input error. If
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
    twisted.internet.error.ConnectionDone>
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
    twisted.internet.error.ConnectionDone>
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
    twisted.internet.error.ConnectionDone>
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
    twisted.internet.error.ConnectionDone>
    INFO zc.zrs.primary:
    IPv4Address(TCP, '127.0.0.1', 47248): Closed
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
    twisted.internet.error.ConnectionDone>
        
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

    def callLater(self, delay, f, *a, **k):
        self.later.append((delay, f, a, k))

    def doLater(self):
        while self.later:
            delay, f, a, k = self.later.pop(0)
            self.callFromThread(f, *a, **k)

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

    def send(self, data):
        record = zc.zrs.sizedmessage.marshal(data)
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

def setUp(test):
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
from ZODB.tests import VersionStorage
from ZODB.tests import TransactionalUndoVersionStorage
from ZODB.tests import PackableStorage
from ZODB.tests import Synchronization
from ZODB.tests import ConflictResolution
from ZODB.tests import HistoryStorage
from ZODB.tests import IteratorStorage
from ZODB.tests import PersistentStorage
from ZODB.tests import MTStorage
from ZODB.tests import ReadOnlyStorage

def catch_up(fs1, fs2):
    for i in range(2000):
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

class TestPrimary(zc.zrs.primary.Primary):

    _transaction_count = 0
    def tpc_finish(self, *args):
        self._transaction_count += 1
        zc.zrs.primary.Primary.tpc_finish(self, *args)
        if self._transaction_count%20 == 0:
            # be annoying and disconnect out clients every 20 transactions.
            # Hee hee.
            # Before we do that though, we'll call doLater on our reactor to
            # give previously disconnected clients a chance to reconnect.
            self._reactor.doLater()
            self._reactor.callFromThread(self._factory.close, lambda : None)


class BasePrimaryStorageTests(StorageTestBase.StorageTestBase):

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

    def open(self, **kwargs):
        reactor = self.globs['reactor']
        self.__port += 1
        addr = '', self.__port
        self.__pfs = ZODB.FileStorage.FileStorage('primary.fs', **kwargs)
        self._storage = TestPrimary(self.__pfs, addr, reactor)
        self.__sfs = ZODB.FileStorage.FileStorage('secondary.fs')
        self.__ss = zc.zrs.secondary.Secondary(self.__sfs, addr, reactor)

        p_pack = self._storage.pack
        def pack(*args, **kw):
            #catch_up(self.__pfs, self.__sfs)
            #import pdb; pdb.set_trace()
            p_pack(*args, **kw)
            #self.__ss.pack(*args, **kw)
            self.__pack = True
        self._storage.pack = pack
        
        p_close = self._storage.close
        def close():
            self._storage._reactor.doLater()
            catch_up(self.__pfs, self.__sfs)

            if self.__pack:
                comparedbs_packed(self, self.__pfs, self.__sfs)
            else:
                self.__comparedbs(self.__pfs, self.__sfs)
                
            # Now, just recover from scratch to make sure we can:
            sfs = ZODB.FileStorage.FileStorage('secondary2.fs')
            ss = zc.zrs.secondary.Secondary(sfs, addr, reactor)
            catch_up(self.__pfs, sfs)
            self.__comparedbs(self.__pfs, sfs)
            ss.close()

            p_close()
            self.__ss.close()

        self._storage.close = close

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
    VersionStorage.VersionStorage,
    TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
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

#
##############################################################################

##############################################################################
# ZEO Tests

class ZEOTests(ZEO.tests.testZEO.FullGenericTests):

    def getConfig(self):
        port = self.__port = ZEO.tests.testZEO.get_port()
        return """
        %%import zc.zrs

        <primary 1>
          address %s
          <filestorage 1>
            path primary.fs
          </filestorage>
        </primary>
        """ % port

    def setUp(self):
        self.globs = {}
        setupstack.register(self, join, threading.enumerate())
        setupstack.setUpDirectory(self)
        ZEO.tests.testZEO.FullGenericTests.setUp(self)
        self.__sfs = ZODB.FileStorage.FileStorage('secondary.fs')
        self.__s = zc.zrs.secondary.Secondary(
            self.__sfs, ('', self.__port), reconnect_delay=0.1)
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

#
##############################################################################

def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite(
            'fsiterator.txt', 'primary.txt', 'secondary.txt',
            setUp=setUp, tearDown=setupstack.tearDown,
            checker=renormalizing.RENormalizing([
                (re.compile(' at 0x[a-fA-F0-9]+'), ''),
                ]),
            ),
        doctest.DocTestSuite(
            setUp=setUp, tearDown=setupstack.tearDown,
            checker=renormalizing.RENormalizing([
                (re.compile(' at 0x[a-fA-F0-9]+'), ''),
                ]),
            ),
        unittest.makeSuite(PrimaryStorageTests, "check"),
        unittest.makeSuite(ZEOTests, "check"),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

