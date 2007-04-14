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
import cPickle, logging, os, re, shutil, struct, sys
import tempfile, threading, time, unittest

import transaction
from ZODB.TimeStamp import TimeStamp
import ZODB.utils

import ZEO.tests.testZEO

from zope.testing import doctest, setupstack, renormalizing

import twisted.internet.base
import twisted.internet.error
import twisted.protocols.loopback
import twisted.python.failure

import zc.zrs.fsiterator
import zc.zrs.sizedmessage
import zc.zrs.primary
import zc.zrs.secondary

_loopbackAsyncBody_orig = twisted.protocols.loopback._loopbackAsyncBody
def _loopbackAsyncBody(*args):
    _loopbackAsyncBody_orig(*args)



def scan_from_back():
    r"""
Create the database:

    >>> import ZODB.FileStorage
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
    >>> it = zc.zrs.fsiterator.FileStorageIterator(fs, condition)
    >>> it.next()
    Traceback (most recent call last):
    ...
    CorruptedDataError: Error reading unknown oid.  Found '' at 4

    >>> def tid_from_time(t):
    ...     return repr(TimeStamp(*(time.gmtime(t)[:5] + (t%60,))))

    >>> tid = tid_from_time(time.time()-70)
    >>> zc.zrs.fsiterator.FileStorageIterator(fs, condition, tid)
    Traceback (most recent call last):
    ...
    OverflowError: long too big to convert

But, if we iterate from near the end, we'll be OK:

    >>> tid = tid_from_time(time.time()-30)
    >>> it = zc.zrs.fsiterator.FileStorageIterator(fs, condition, tid)
    >>> trans = it.next()
    >>> from ZODB import utils
    >>> print TimeStamp(trans.tid), [utils.u64(r.oid) for r in trans]
    2007-03-21 20:34:09.000000 [0L, 72L]

    >>> print TimeStamp(tid)
    2007-03-21 20:34:08.000000

    >>> tid = tid_from_time(time.time()-29.5)
    >>> it = zc.zrs.fsiterator.FileStorageIterator(fs, condition, tid)
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

    >>> import zc.zrs.fsiterator
    >>> iterator = zc.zrs.fsiterator.FileStorageIterator(fs)

And a special transport that will output data when it is called:

    >>> class Reactor:
    ...     def callFromThread(self, f, *args, **kw):
    ...         f(*args, **kw)

    >>> class Transport:
    ...     def __init__(self):
    ...         self.reactor = Reactor()
    ...     def write(self, message):
    ...         message = message[4:] # cheat. :)
    ...         print cPickle.loads(message)[0]
    ...     def registerProducer(self, producer, streaming):
    ...         print 'registered producer'
    ...     def unregisterProducer(self):
    ...         print 'unregistered producer'
    ...     def loseConnection(self):
    ...         print 'loseConnection'

And a producer based on the iterator and transport:

    >>> import zc.zrs.primary
    >>> import time
    >>> producer = zc.zrs.primary.PrimaryProducer(iterator, Transport(), 'test'
    ...            ); time.sleep(0.1)
    registered producer
    T
    S
    C

We get the initial transaction, because the producer starts producing
immediately.  Let's oause producing:

    >>> producer.pauseProducing()

and we'll create another transaction:

    >>> conn = db.open()
    >>> ob = conn.root()
    >>> import persistent.dict
    >>> ob.x = persistent.dict.PersistentDict()
    >>> commit()
    >>> iterator.notify()
    >>> ob = ob.x
    >>> ob.x = persistent.dict.PersistentDict()
    >>> commit()
    >>> iterator.notify()
    >>> time.sleep(0.1)
    
No output because we are paused.  Now let's resume:

    >>> producer.resumeProducing(); time.sleep(0.1)
    T
    S
    S
    C
    T
    S
    S
    C

and pause again:

    >>> producer.pauseProducing()
    >>> ob = ob.x
    >>> ob.x = persistent.dict.PersistentDict()
    >>> commit()
    >>> iterator.notify()
    >>> time.sleep(0.1)

and resume:

    >>> producer.resumeProducing(); time.sleep(0.1)
    T
    S
    S
    C

    >>> producer.close()
    unregistered producer
    loseConnection

    >>> db.close()

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
        self.proto.dataReceived(zc.zrs.sizedmessage.marshal(data))

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

    def read(self):
        return cPickle.loads(MessageTransport.read(self))

class SecondaryTransport(MessageTransport):
    
    def send(self, data):
        MessageTransport.send(self, cPickle.dumps(data))

    def fail(self):
        reason = 'failed'
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
            twisted.protocols.loopback.loopbackAsync(server, proto)
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

class BasePrimaryStorageTests(StorageTestBase.StorageTestBase):

    def setUp(self):
        # Monkey patch loopback to work around bug:
        twisted.protocols.loopback._loopbackAsyncBody = _loopbackAsyncBody
        
        self.__pack = None

        self.globs = {}
        setupstack.register(self, join, threading.enumerate())
        setupstack.setUpDirectory(self)
        self.globs['reactor'] = TestReactor()
        
        self.__oldreactor = getattr(
            twisted.protocols.loopback._LoopbackTransport,
            'reactor', None)
        
        reactor = self.globs['reactor']
        twisted.protocols.loopback._LoopbackTransport.reactor = reactor
        self.open(create=1)

    def tearDown(self):
        
        self._storage.close()
        reactor = self.globs['reactor']
        if self.__oldreactor is None:
            del twisted.protocols.loopback._LoopbackTransport.reactor
        else:
            twisted.protocols.loopback._LoopbackTransport.reactor = (
                self.__oldreactor)

        self.assert_(not reactor._factories) # Make sure we're not listening
        setupstack.tearDown(self)
        self.globs.clear()

        # Remove monkey patch of loopback to work around bug:
        twisted.protocols.loopback._loopbackAsyncBody = _loopbackAsyncBody_orig

    __port = 8000

    def open(self, **kwargs):
        reactor = self.globs['reactor']
        self.__port += 1
        self.__pfs = ZODB.FileStorage.FileStorage('primary.fs', **kwargs)
        self._storage = zc.zrs.primary.Primary(
            self.__pfs, ('', self.__port), reactor)
        self.__sfs = ZODB.FileStorage.FileStorage('secondary.fs')
        self.__ss = zc.zrs.secondary.Secondary(
            self.__sfs, ('', self.__port), reactor)

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
            catch_up(self.__pfs, self.__sfs)

            if self.__pack:
                self.__comparedbs_packed(self.__pfs, self.__sfs)
            else:
                self.__comparedbs(self.__pfs, self.__sfs)
                
            p_close()
            self.__ss.close()
        self._storage.close = close
        
    def __comparedbs_packed(self, fs1, fs2):
                
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
            objects = data2[trans.tid][3]
            for r in trans:
                self.assertEqual(
                    objects[r.oid],
                    (r.tid, r.version, r.data),
                    )
                current1[r.oid] = trans.tid

        for oid, tid in current1.items():
            self.assertEqual(current2[oid], tid)

    def __comparedbs(self, fs1, fs2):
        if fs1._pos != fs2._pos:
            time.sleep(0.1)
        self.assertEqual(fs1._pos, fs2._pos)

        self.compare(fs1, fs2)    

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
    
##############################################################################

class ZEOTests(ZEO.tests.testZEO.FullGenericTests):

    def getConfig(self):
        filename = tempfile.mktemp()
        port = ZEO.tests.testZEO.get_port()
        return """
        %%import zc.zrs

        <primary 1>
          address %s
          <filestorage 1>
            path %s
          </filestorage>
        </primary>
        """ % (port, filename)
    

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
            setUp=setUp, tearDown=setupstack.tearDown),
        unittest.makeSuite(PrimaryStorageTests, "check"),
        unittest.makeSuite(ZEOTests, "check"),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

