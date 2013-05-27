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

import cPickle
import logging
import os
import sys
import threading
import time
import twisted.internet.interfaces
import twisted.internet.protocol
import zc.zrs.reactor
import zc.zrs.sizedmessage
import ZODB.BaseStorage
import ZODB.blob
import ZODB.FileStorage
import ZODB.FileStorage.format
import ZODB.interfaces
import ZODB.TimeStamp
import ZODB.utils
import zope.interface

if sys.version_info > (2, 5):
    from hashlib import md5
else:
    from md5 import md5

if not hasattr(ZODB.blob.BlobStorage, 'restoreBlob'):
    import zc.zrs.restoreblob

logger = logging.getLogger(__name__)

class Base(object):

    def __init__(self, storage, addr, reactor):
        if reactor is None:
            reactor = zc.zrs.reactor.reactor()

        self._reactor = reactor

        self._storage = storage

        try:
            fn = storage.getExtensionMethods
        except AttributeError:
            pass # no extension methods
        else:
            self.getExtensionMethods = storage.getExtensionMethods
            for name in fn():
                assert not hasattr(self, name)
                setattr(self, name, getattr(storage, name))

        self._addr = addr

    def __len__(self):
        return len(self._storage)

class Primary(Base):

    def __init__(self, storage, addr, reactor=None):
        Base.__init__(self, storage, addr, reactor)

        if ZODB.interfaces.IBlobStorage.providedBy(storage):
            zope.interface.directlyProvides(self, ZODB.interfaces.IBlobStorage)
            for name in ('storeBlob', 'loadBlob', 'temporaryDirectory',
                         'restoreBlob', 'openCommittedBlobFile', 'fshelper'):
                setattr(self, name, getattr(storage, name))

        if not isinstance(storage, (ZODB.blob.BlobStorage,
                                    ZODB.FileStorage.FileStorage)
                          ):
            raise ValueError("Invalid storage", storage)

        self._changed = threading.Condition()

        # required methods
        for name in (
            'getName', 'getSize', 'history', 'isReadOnly', 'lastTransaction',
            'load', 'loadBefore', 'loadSerial', 'new_oid', 'pack',
            'registerDB', 'sortKey', 'store', 'tpc_abort', 'tpc_begin',
            'tpc_vote',
            ):
            setattr(self, name, getattr(storage, name))

        # Optional methods:
        for name in (
            'restore', 'iterator', 'cleanup', 'loadEx', 'getSerial',
            'supportsTransactionalUndo',
            'tpc_transaction', 'getTid', 'lastInvalidations',
            'supportsUndo', 'undoLog', 'undoInfo', 'undo',
            'supportsVersions', 'abortVersion', 'commitVersion',
            'versionEmpty', 'modifiedInVersion', 'versions',
            'record_iternext', 'deleteObject',
            'checkCurrentSerialInTransaction',
            ):
            if hasattr(storage, name):
                setattr(self, name, getattr(storage, name))

        self._factory = PrimaryFactory(storage, self._changed)
        logger.info("Opening %s %s", self.getName(), addr)
        self._reactor.callFromThread(self.cfr_listen)

    # StorageServer accesses _transaction directly. :(
    @property
    def _transaction(self):
        return self._storage._transaction

    _listener = None
    def cfr_listen(self):
        if isinstance(self._addr, basestring):
            self._listener = self._reactor.listenUNIX(self._addr, self._factory)
        else:
            interface, port = self._addr
            self._listener = self._reactor.listenTCP(
                port, self._factory, interface=interface)

    def cfr_stop_listening(self):
        if self._listener is not None:
            self._listener.stopListening()

    def tpc_finish(self, *args):
        self._storage.tpc_finish(*args)
        self._changed.acquire()
        self._changed.notifyAll()
        self._changed.release()

    def close(self):
        logger.info('Closing %s %s', self.getName(), self._addr)
        self._reactor.callFromThread(self.cfr_stop_listening)

        # Close the storage first to prevent more writes and to
        # give the secondaries more time to catch up.
        self._storage.close()
        self._factory.close()

class PrimaryFactory(twisted.internet.protocol.Factory):

    def __init__(self, storage, changed):
        self.storage = storage
        self.changed = changed
        self.instances = []
        self.threads = ThreadCounter()

    def close(self):
        for instance in list(self.instances):
            instance.close()
        self.threads.wait(60)

class PrimaryProtocol(twisted.internet.protocol.Protocol):

    __protocol = None
    __start = None
    __producer = None

    def connectionMade(self):           # cfr
        self.__stream = zc.zrs.sizedmessage.Stream(self.messageReceived, 8)
        self.__peer = str(self.transport.getPeer()) + ': '
        self.factory.instances.append(self)
        self.info("Connected")

    def connectionLost(self, reason):   # cfr
        self.info("Disconnected %r", reason)
        if self.__producer is not None:
            self.__producer.stopProducing()
        self.factory.instances.remove(self)

    def close(self):
        if self.__producer is not None:
            self.__producer.close()
        else:
            self.transport.reactor.callFromThread(
                self.transport.loseConnection)
        self.info('Closed')

    def _stop(self):
        # for tests
        if self.__producer is not None:
            self.__producer._stop()
        else:
            self.transport.reactor.callFromThread(
                self.transport.loseConnection)


    def error(self, message, *args):
        logger.error(self.__peer + message, *args)
        self.close()

    def info(self, message, *args):
        logger.info(self.__peer + message, *args)

    def dataReceived(self, data):       # cfr
        try:
            self.__stream(data)
        except zc.zrs.sizedmessage.LimitExceeded, v:
            self.error('message too large: '+str(v))

    def messageReceived(self, data):    # cfr
        if self.__protocol is None:
            if data == 'zrs2.0':
                if ZODB.interfaces.IBlobStorage.providedBy(
                    self.factory.storage):
                    return self.error("Invalid protocol %r. Require >= 2.1",
                                      data)
            elif data != 'zrs2.1':
                return self.error("Invalid protocol %r", data)
            self.__protocol = data
        else:
            if self.__start is not None:
                if not data:
                    logger.debug(self.__peer + "keep-alive")
                    return # ignore empty messages
                return self.error("Too many messages")
            self.__start = data
            if len(data) != 8:
                return self.error("Invalid transaction id, %r", data)

            self.info("start %r (%s)", data, ZODB.TimeStamp.TimeStamp(data))
            self.__producer = PrimaryProducer(
                (self.factory.storage, self.factory.changed, self.__start),
                self.transport, self.__peer,
                self.factory.threads.run)

PrimaryFactory.protocol = PrimaryProtocol


class PrimaryProducer:

    zope.interface.implements(twisted.internet.interfaces.IPushProducer)

    # stopped indicates that we should stop sending output to a client
    stopped = False

    # closed means that we're closed.  This is mainly to deal with a
    # possible race at startup.  When we close a producer, we continue
    # sending data to the client as long as data are available.
    closed = False

    def __init__(self, iterator_args, transport, peer,
                 run=lambda f, *args: f(*args)):
        self.iterator_scan_control = ScanControl()
        self.storage = iterator_args[0]
        self.iterator = None
        self.iterator_args = iterator_args + (self.iterator_scan_control,)
        self.start_tid = iterator_args[2]
        self.transport = transport
        self.peer = peer
        transport.registerProducer(self, True)
        self.callFromThread = transport.reactor.callFromThread
        self.consumer_event = threading.Event()
        self.consumer_event.set()
        thread = threading.Thread(target=run, args=(self.run, ),
                                  name='Producer(%s)' % peer)
        thread.setDaemon(True)
        thread.start()
        self.thread = thread

    def pauseProducing(self):
        logger.debug(self.peer+" pausing")
        self.consumer_event.clear()

    def resumeProducing(self):
        logger.debug(self.peer+" resuming")
        self.consumer_event.set()


    def close(self):
        # We use the closed flag to handle a race condition in
        # iterator setup.  We set the close flag before checking for
        # the iterator.  If we find the iterator, we tell it to stop.
        # If not, then when the run method below finishes creating the
        # iterator, it will find the close flag and not use the iterator.
        self.closed = True

        iterator = self.iterator
        if iterator is not None:
            iterator.catch_up_then_stop()

    def _stop(self):
        # for tests
        iterator = self.iterator
        if iterator is not None:
            iterator.stop()

    def cfr_close(self):
        if not self.stopped:
            self.transport.unregisterProducer()
            self.stopProducing()
            self.transport.loseConnection()


    def stopProducing(self): # cfr
        self.stopped = True
        iterator = self.iterator
        if iterator is not None:
            iterator.stop()
        else:
            self.iterator_scan_control.not_stopped = False

        self.consumer_event.set() # unblock wait calls

    def cfr_write(self, data):
        if not self.stopped:
            self.transport.writeSequence(data)

    def write(self, data):
        data = zc.zrs.sizedmessage.marshals(data)
        self.md5.update(data[1])
        self.consumer_event.wait()
        self.callFromThread(self.cfr_write, data)


    def run(self):
        try:
            self.iterator = FileStorageIterator(*self.iterator_args)
        except:
            logger.exception(self.peer)
            self.iterator = None
            self.callFromThread(self.cfr_close)
            return

        if self.closed:
            self.callFromThread(self.cfr_close)
            return

        if self.stopped:
            # make sure our iterator gets stopped, as stopProducing might
            # have been called while we were creating the iterator.
            self.iterator.stop()

        pickler = cPickle.Pickler(1)
        pickler.fast = 1
        self.md5 = md5(self.start_tid)

        blob_block_size = 1 << 16

        try:
            for trans in self.iterator:
                self.write(
                    pickler.dump(('T', (trans.tid, trans.status, trans.user,
                                        trans.description, trans._extension)),
                                 1)
                    )
                for record in trans:
                    if record.data and is_blob_record(record.data):
                        try:
                            fname = self.storage.loadBlob(
                                record.oid, record.tid)
                            f = open(fname, 'rb')
                        except (IOError, ZODB.POSException.POSKeyError):
                            pass
                        else:
                            f.seek(0, 2)
                            blob_size = f.tell()
                            blocks, r = divmod(blob_size, blob_block_size)
                            if r:
                                blocks += 1

                            self.write(
                                pickler.dump(
                                    ('B',
                                     (record.oid, record.tid, record.version,
                                      record.data_txn, long(blocks))),
                                    1)
                                )
                            self.write(record.data or '')
                            f.seek(0)
                            while blocks > 0:
                                data = f.read(blob_block_size)
                                if not data:
                                    raise AssertionError("Too much blob data")
                                blocks -= 1
                                self.write(data)

                            f.close()
                            continue

                    self.write(
                        pickler.dump(('S',
                                       (record.oid, record.tid, record.version,
                                        record.data_txn)),
                                     1)
                        )
                    self.write(record.data or '')

                self.write(pickler.dump(('C', (self.md5.digest(), )), 1))

        except:
            logger.exception(self.peer)

        self.iterator = None
        self.callFromThread(self.cfr_close)


class TidTooHigh(Exception):
    """The last tid for an iterator is higher than any tids in a file.
    """

class ScanControl:

    not_stopped = True

class FileStorageIterator(ZODB.FileStorage.format.FileStorageFormatter):

    _file_size = 1 << 64 # To make base class check happy.

    def __init__(self, fs, condition=None, start=ZODB.utils.z64,
                 scan_control=None):
        self._ltid = start
        self._fs = fs
        self._stop = False
        if scan_control is None:
            scan_control = ScanControl()
        self._scan_control = scan_control
        self._open()
        if condition is None:
            condition = threading.Condition()
        self._condition = condition
        self._catch_up_then_stop = False

    def _open(self):
        self._old_file = self._fs._file
        try:
            file = open(self._fs._file_name, 'rb', 0)
        except IOError, v:
            if os.path.exists(self._fs._file_name):
                raise

            # The file is gone.  It must have been removed -- probably
            # in a test.  We won't die here because we're probably
            # never going to be used.  If we are ever used

            def _next():
                raise v
            self._next = _next
            return

        self._file = file
        self._pos = 4L
        ltid = self._ltid
        if ltid > ZODB.utils.z64:
            # We aren't starting at the beginning.  We need to find the
            # first transaction after _ltid. We can either search from the
            # beginning, or from the end.
            file.seek(4)
            tid = file.read(8)
            if len(tid) < 8:
                raise TidTooHigh(repr(ltid))

            t1 = ZODB.TimeStamp.TimeStamp(tid).timeTime()
            tid = self._fs.lastTransaction()
            t2 = ZODB.TimeStamp.TimeStamp(tid).timeTime()
            t = ZODB.TimeStamp.TimeStamp(self._ltid).timeTime()

            if (t - t1) < (t2 - t1)/2:
                # search from beginning:
                self._scan_forward(4, ltid)
            else:
                # search from end
                pos = self._fs._pos - 8
                if pos < 27:
                    # strangely small position
                    return self._scan_forward(4, ltid)

                file.seek(pos)
                tlen = ZODB.utils.u64(file.read(8))
                pos -= tlen
                if pos <= 4:
                    # strangely small position
                    return self._scan_forward(4, ltid)

                file.seek(pos)
                h = self._read_txn_header(pos)
                if h.tid <= ltid:
                    self._scan_forward(pos, ltid)
                else:
                    self._scan_backward(pos, ltid)

    def _scan_forward(self, pos, ltid):
        file = self._file
        while self._scan_control.not_stopped:
            # Read the transaction record
            try:
                h = self._read_txn_header(pos)
            except ZODB.FileStorage.format.CorruptedDataError, err:
                # If buf is empty, we've reached EOF.
                if not err.buf:
                    # end of file.
                    self._pos = pos
                    raise TidTooHigh(repr(ltid))
                raise

            if h.status == "c":
                raise TidTooHigh(repr(ltid))

            if h.tid > ltid:
                # This is the one we want to read next
                self._pos = pos
                return

            pos += h.tlen + 8
            if h.tid == ltid:
                # We just read the one we want to skip past
                self._pos = pos
                return

    def _scan_backward(self, pos, ltid):
        file = self._file
        seek = file.seek
        read = file.read
        while self._scan_control.not_stopped:
            pos -= 8
            seek(pos)
            tlen = ZODB.utils.u64(read(8))
            pos -= tlen
            h = self._read_txn_header(pos)
            if h.tid <= ltid:
                self._pos = pos + tlen + 8
                return

    def __iter__(self):
        return self

    def stop(self):
        self._condition.acquire()
        self._stop = True
        self._condition.notifyAll()
        self._condition.release()

    def catch_up_then_stop(self):
        self._condition.acquire()
        self._catch_up_then_stop = True
        self._condition.notifyAll()
        self._condition.release()

    def next(self):
        self._condition.acquire()
        try:
            while 1:
                if self._stop:
                    raise StopIteration
                r = self._next()
                if r is not None:
                    return r
                if self._catch_up_then_stop:
                    raise StopIteration
                self._condition.wait()
        finally:
            self._condition.release()

    def _next(self):

        if self._old_file is not self._fs._file:
            # Our file-storage must have been packed.  We need to
            # reopen the file:
            self._open()

        file = self._file
        pos = self._pos

        while 1:
            # Read the transaction record
            try:
                h = self._read_txn_header(pos)
            except ZODB.FileStorage.format.CorruptedDataError, err:
                # If buf is empty, we've reached EOF.
                if not err.buf:
                    return None
                raise

            if h.tid <= self._ltid:
                logger.warning("%s time-stamp reduction at %s",
                               self._file.name, pos)

            if h.status == "c":
                # Assume we've hit the last, in-progress transaction
                # Wait until there is more data.
                return None

            if pos + h.tlen + 8 > self._file_size:
                # Hm, the data were truncated or the checkpoint flag wasn't
                # cleared.  They may also be corrupted,
                # in which case, we don't want to totally lose the data.
                logger.critical("%s truncated, possibly due to"
                                " damaged records at %s", self._file.name, pos)
                raise ZODB.FileStorage.format.CorruptedDataError(None, '', pos)

            if h.status not in " up":
                logger.warning('%s has invalid status,'
                               ' %s, at %s', self._file.name, h.status, pos)

            if h.tlen < h.headerlen():
                logger.critical("%s has invalid transaction header at %s",
                                self._file.name, pos)
                raise ZODB.FileStorage.format.CorruptedDataError(None, '', pos)

            tpos = pos
            tend = tpos + h.tlen

            # Check the (intentionally redundant) transaction length
            self._file.seek(tend)
            rtl = ZODB.utils.u64(self._file.read(8))
            if rtl != h.tlen:
                logger.warning("%s redundant transaction length check"
                               " failed at %s", self._file.name, tend)
                break

            self._pos = tend + 8
            self._ltid = h.tid

            if h.status == "u":
                # Undone trans.  It's unlikely to see these anymore
                continue

            pos += h.headerlen()
            if h.elen:
                e = cPickle.loads(h.ext)
            else:
                e = {}

            result = RecordIterator(
                h.tid, h.status, h.user, h.descr,
                e, pos, tend, self._file, tpos)

            return result

    def notify(self):
        self._condition.acquire()
        self._condition.notifyAll()
        self._condition.release()

class RecordIterator(ZODB.FileStorage.format.FileStorageFormatter):
    """Iterate over the transactions in a FileStorage file."""

    def __init__(self, tid, status, user, desc, ext, pos, tend, file, tpos):
        self.tid = tid
        self.status = status
        self.user = user
        self.description = desc
        self._extension = ext
        self._pos = pos
        self._tend = tend
        self._file = file
        self._tpos = tpos

    def __iter__(self):
        return self

    def next(self):
        pos = self._pos
        if pos < self._tend:
            # Read the data records for this transaction
            h = self._read_data_header(pos)
            dlen = h.recordlen()

            if pos + dlen > self._tend or h.tloc != self._tpos:
                logger.critical("%s data record exceeds transaction"
                                " record at %s", file.name, pos)
                raise ZODB.FileStorage.format.CorruptedDataError(
                    h.oid, '', pos)

            self._pos = pos + dlen
            prev_txn = None
            if h.plen:
                data = self._file.read(h.plen)
            else:
                if h.back == 0:
                    # If the backpointer is 0, then this transaction
                    # undoes the object creation.  It either aborts
                    # the version that created the object or undid the
                    # transaction that created it.  Return None
                    # instead of a pickle to indicate this.
                    data = None
                else:
                    data, tid = self._loadBackTxn(h.oid, h.back, False)
                    # Caution:  :ooks like this only goes one link back.
                    # Should it go to the original data like BDBFullStorage?
                    prev_txn = self.getTxnFromData(h.oid, h.back)

            return Record(h.oid, h.tid, '', data, prev_txn, pos)

        raise StopIteration

class Record:
    """An abstract database record."""
    def __init__(self, oid, tid, version, data, prev, pos):
        self.oid = oid
        self.tid = tid
        self.version = version
        self.data = data
        self.data_txn = prev
        self.pos = pos

class ThreadCounter:
    """Keep track of running threads

    We use the run method to run the threads and the wait method to
    wait until they're all done:
    """

    def __init__(self):
        self.count = 0
        self.condition = threading.Condition()

    def run(self, func, *args):
        self.condition.acquire()
        self.count += 1
        self.condition.release()
        try:
            func(*args)
        finally:
            self.condition.acquire()
            self.count -= 1
            self.condition.notifyAll()
            self.condition.release()

    def wait(self, timeout):
        deadline = time.time()+timeout
        self.condition.acquire()
        while self.count > 0:
            w = deadline-time.time()
            if w <= 0:
                break
            self.condition.wait(w)
        self.condition.release()

def is_blob_record(record):
    try:
        return cPickle.loads(record) is ZODB.blob.Blob
    except (MemoryError, KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        return False
