##############################################################################
#
# Copyright (c) 2002 Zope Corporation.  All Rights Reserved.
#
# This software is subject to the provisions of the Zope Visible Source
# License, Version 1.0 (ZVSL).  A copy of the ZVSL should accompany this
# distribution.
#
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

import cPickle
import logging
import threading

import ZODB.BaseStorage
import ZODB.FileStorage
import ZODB.FileStorage.format
import ZODB.TimeStamp
import ZODB.utils

import zope.interface

import twisted.internet.protocol
import twisted.internet.interfaces

import zc.zrs.reactor
import zc.zrs.sizedmessage

logger = logging.getLogger(__name__)

class Primary:

    def __init__(self, storage, addr, reactor=None):
        if reactor is None:
            reactor = zc.zrs.reactor.reactor()
            
        self._reactor = reactor
            
        self._storage = storage
        self._changed = threading.Condition()

        # required methods
        for name in (
            'getName', 'getSize', 'history', 'isReadOnly', 'lastTransaction',
            '__len__', 'load', 'loadBefore', 'loadSerial', 'new_oid', 'pack', 
            'registerDB', 'sortKey', 'store', 'tpc_abort', 'tpc_begin',
            'tpc_vote',
            ):
            setattr(self, name, getattr(storage, name))

        # Optional methods:
        for name in (
            'restore', 'iterator', 'cleanup', 'loadEx', 'getSerial',
            'getExtensionMethods', 'supportsTransactionalUndo',
            'tpc_transaction', 'getTid', 'lastInvalidations',
            'supportsUndo', 'undoLog', 'undoInfo', 'undo',
            'supportsVersions', 'abortVersion', 'commitVersion',
            'versionEmpty', 'modifiedInVersion', 'versions', 
            ):
            if hasattr(storage, name):
                setattr(self, name, getattr(storage, name))

        self._factory = PrimaryFactory(storage, self._changed)
        self._addr = addr
        logger.info("Opening %s %s", self.getName(), addr)
        reactor.callFromThread(self._listen)

    # StorageServer accesses _transaction directly. :(
    @property
    def _transaction(self):
        return self._storage._transaction

    _listener = None
    def _listen(self):
        interface, port = self._addr
        self._listener = self._reactor.listenTCP(
            port, self._factory, interface=interface)

    def _stop_listening(self):
        if self._listener is not None:
            self._listener.stopListening()

    def tpc_finish(self, *args):
        self._storage.tpc_finish(*args)
        self._changed.acquire()
        self._changed.notifyAll()
        self._changed.release()

    def close(self):
        logger.info('Closing %s %s', self.getName(), self._addr)
        self._reactor.callFromThread(self._stop_listening)
        event = threading.Event()
        self._reactor.callFromThread(self._factory.close, event.set)
        event.wait()
        self._factory.join()
        self._storage.close()

class PrimaryFactory(twisted.internet.protocol.Factory):

    def __init__(self, storage, changed):
        self.storage = storage
        self.changed = changed
        self.instances = []
        self.joins = []

    def close(self, callback):
        for instance in list(self.instances):
            instance.close()
        callback()

    def join(self):
        while self.joins:
            self.joins.pop()()

class PrimaryProtocol(twisted.internet.protocol.Protocol):

    __protocol = None
    __start = None
    __producer = None

    def connectionMade(self):
        self.__stream = zc.zrs.sizedmessage.Stream(self.messageReceived, 8)
        self.__peer = str(self.transport.getPeer()) + ': '
        self.factory.instances.append(self)
        self.info("Connected")

    def connectionLost(self, reason):
        self.info("Disconnected %r", reason)
        if self.__producer is not None:
            self.factory.joins.append(self.__producer.join)
        self.factory.instances.remove(self)

    def close(self):
        if self.__producer is not None:
            self.__producer.close()
        else:
            self.transport.loseConnection()
            
        self.info('Closed')

    def error(self, message, *args):
        logger.error(self.__peer + message, *args)
        self.close()

    def info(self, message, *args):
        logger.info(self.__peer + message, *args)

    def dataReceived(self, data):
        try:
            self.__stream(data)
        except zc.zrs.sizedmessage.LimitExceeded, v:
            self.error('message too large: '+str(v))

    def messageReceived(self, data):
        if self.__protocol is None:
            if data != 'zrs2.0':
                return self.error("Invalid protocol %r", data)
            self.__protocol = data
        else:
            if self.__start is not None:
                return self.error("Too many messages")
            self.__start = data
            if len(data) != 8:
                return self.error("Invalid transaction id, %r", data)

            self.info("start %r (%s)", data, ZODB.TimeStamp.TimeStamp(data))
            iterator = FileStorageIterator(
                self.factory.storage, self.factory.changed, self.__start)
            self.__producer = PrimaryProducer(
                iterator, self.transport, self.__peer)

PrimaryFactory.protocol = PrimaryProtocol

 
class PrimaryProducer:

    zope.interface.implements(twisted.internet.interfaces.IPushProducer)

    stopped = False

    def __init__(self, iterator, transport, peer):
        self.iterator = iterator
        self.transport = transport
        self.peer = peer
        transport.registerProducer(self, True)
        self.callFromThread = transport.reactor.callFromThread
        self.event = threading.Event()
        self.pauseProducing = self.event.clear
        self.resumeProducing = self.event.set
        self.wait = self.event.wait
        self.resumeProducing()
        thread = threading.Thread(target=self.run)
        thread.setDaemon(True)
        self.join = thread.join
        thread.start()

    def close(self):
        self.transport.unregisterProducer()
        self.stopProducing()
        self.transport.loseConnection()
    
    def stopProducing(self):
        self.iterator.stop()
        self.stopped = True
        self.event.set()

    def _write(self, data):
        if not self.stopped:
            self.transport.writeSequence(data)
        
    def write(self, data):
        data = zc.zrs.sizedmessage.marshals(data)
        self.event.wait()
        self.callFromThread(self._write, data)
            
    def run(self):
        pickler = cPickle.Pickler(1)
        pickler.fast = 1
        try:
            for trans in self.iterator:
                self.write(
                    pickler.dump(('T', (trans.tid, trans.status, trans.user,
                                        trans.description, trans._extension)),
                                 1)
                    )
                for record in trans:
                    self.write(
                        pickler.dump(('S',
                                       (record.oid, record.tid, record.version,
                                        record.data_txn)),
                                     1)
                        )
                    self.write(record.data or '')
                self.write(pickler.dump(('C', ()), 1))
                if self.stopped:
                    break
        except:
            logger.exception(self.peer)
            self.callFromThread(self.close)


class TidTooHigh(Exception):
    """The last tid for an iterator is higher than any tids in a file.
    """

class FileStorageIterator(ZODB.FileStorage.format.FileStorageFormatter):

    _file_size = 1 << 64 # To make base class check happy.

    def __init__(self, fs, condition=None, start=ZODB.utils.z64):
        self._ltid = start
        self._fs = fs
        self._open()
        if condition is None:
            condition = threading.Condition()
        self._condition = condition
        self._stopped = False

    def _open(self):
        self._old_file = self._fs._file
        file = self._file = open(self._fs._file_name, 'rb', 0)
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

    def _ltid_too_high(self, ltid):
        raise ValueError("Transaction id too high", repr(ltid))

    def _scan_forward(self, pos, ltid):
        file = self._file
        while 1:
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
        while 1:
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
        self._stopped = True
        self._condition.notifyAll()
        self._condition.release()

    def next(self):
        self._condition.acquire()
        try:
            if self._stopped:
                raise StopIteration
            while 1:
                r = self._next()
                if r is not None:
                    return r
                self._condition.wait()
                if self._stopped:
                    raise StopIteration
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

            return Record(h.oid, h.tid, h.version, data, prev_txn, pos)

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
