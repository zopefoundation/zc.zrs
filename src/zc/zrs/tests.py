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
import cPickle, logging, os, shutil, struct, sys
import tempfile, threading, time, unittest

import transaction
from zope.testing import doctest, setupstack
import zc.zrs.fsiterator
from ZODB.TimeStamp import TimeStamp
import ZODB.utils

class TestReactor:

    def __init__(self):
        self._factories = {}
        self.client_port = 47245
            
    def listenTCP(self, port, factory, backlog=50, interface=''):
        addr = interface, port
        assert addr not in self._factories
        self._factories[addr] = factory

    def connect(self, addr):
        proto = self._factories[addr].buildProtocol(addr)
        transport = MessageTransport(
            proto, "IPv4Address(TCP, '127.0.0.1', %s)" % self.client_port)
        self.client_port += 1
        transport.reactor = self
        proto.makeConnection(transport)
        return proto

    def callFromThread(self, f, *a, **k):
        f(*a, **k)

class MessageTransport:

    def __init__(self, proto, peer):
        self.data = ''
        self.cond = threading.Condition()
        self.closed = False
        self.proto = proto
        self.peer = peer

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

        return cPickle.loads(result)

    def have_data(self):
        return bool(self.data)

    def loseConnection(self):
        print 'Transport closed!'
        self.closed = True

    producer = None
    def registerProducer(self, producer, streaming):
        self.producer = producer

    def close(self):
        self.producer.stopProducing()
        self.proto.connectionLost('closed')


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

    logger = logging.getLogger('zc.zrs.primary')
    logger.setLevel(1)
    setupstack.register(test, logger.setLevel, 0)
    logger.addHandler(stdout_handler)
    setupstack.register(test, logger.removeHandler, stdout_handler)


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


def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite(
            'fsiterator.txt', 'primary.txt', 
            setUp=setUp, tearDown=setupstack.tearDown),
        doctest.DocTestSuite(
            setUp=setUp, tearDown=setupstack.tearDown),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

