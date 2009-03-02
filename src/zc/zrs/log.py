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
import os
import re
import struct
import tempfile
import zc.zrs.secondary

logger = logging.getLogger(__name__)

def readlen(f, pos=None):
    if pos is not None:
        f.seek(pos)
    return struct.unpack(">I", f.read(4))[0]

def readback(f, pos):
    l = readlen(f, pos-4)
    pos -= l+8
    assert l == readlen(f, pos)
    return pos, f.read(1)

class LogFile:

    def __init__(self, destination, max_size):
        self.destination = destination
        self.max_size = max_size
        files = sorted(name for name in os.listdir(destination)
                       if re.match("[0-9a-f]{16}$", name))

        while files:
            filename = files.pop()
            if self._open(os.path.join(destination, filename)):
                return

        self.tid = '\0'*8
        self._create()

    def _create(self):
        self.file = open(os.path.join(self.destination,
                                      self.tid.encode('hex')),
                         'w+b')
        self.pos = self.tpos = 0

    def _open(self, filename):
        f = None
        try:
            f = open(filename, 'r+b')
            f.seek(0, 2)
            pos = opos = f.tell()
            while pos:
                p, message_type = readback(f, pos)
                if message_type == 'C':
                    if pos != opos:
                        f.truncate(pos)
                    assert readlen(f, p) == 9
                    f.read(1)
                    self.tid = f.read(8)
                    if pos >= self.max_size:
                        # File is already full, open the next one
                        f.close()
                        self._create()
                    else:
                        f.seek(pos)
                        self.file = f
                        self.pos = pos
                    self.tpos = pos
                    return True
                pos = p
        except:
            logger.exception("Error reading log file %s", filename)

        f.close()
        os.remove(filename)

    def __repr__(self):
        return "ZRSReplicationLog(%r)" % self.destination

    def lastTransaction(self):
        return self.tid

    def tpc_abort(self, trans=None):
        self.file.truncate(self.tpos)
        self.file.flush()
        self.pos = self.tpos

    def close(self):
        self.tpc_abort()
        self.file.close()

    def output(self, message_type, data):
        l = len(data)+1
        ls = struct.pack(">I", l)
        self.file.seek(self.pos)
        self.file.write(ls+message_type)
        self.file.write(data)
        self.file.write(ls)
        self.pos += l+8
        if message_type == 'C':
            self.tid = data
            self.file.flush()
            if self.pos >= self.max_size:
                self.file.close()
                self._create()
            self.tpos = self.pos

class RecorderProtocol(zc.zrs.secondary.SecondaryProtocol):

    logger = logger

    __data_record_next = False
    __blob_file_blocks = None
    def messageReceived(self, message):
        try:
            if self.__data_record_next:
                self.__data_record_next = False
                self.factory.storage.output('d', message)
                return
            if self.__blob_file_blocks:
                self.__blob_file_blocks -= 1
                self.factory.storage.output('b', message)
                return

            message_type, data = cPickle.loads(message)
            if message_type == 'T':
                assert self._zrs_transaction is None
                self._zrs_transaction = data[0]
            elif message_type == 'B':
                assert not self.__data_record_next
                self.__data_record_next = True
                assert not self.__blob_file_blocks
                self.__blob_file_blocks = data[-1]
            elif message_type == 'C':
                self._check_replication_stream_checksum(data[0])
                assert self._zrs_transaction is not None
                data = self._zrs_transaction
                self._zrs_transaction = None
                return self.factory.storage.output(message_type, data)
            elif message_type == 'S':
                assert not self.__data_record_next
                self.__data_record_next = True
            else:
                raise ValueError("Invalid message type, %r" % message_type)

            self.factory.storage.output(message_type, cPickle.dumps(data, 1))
        finally:
            self._replication_stream_md5.update(message)

class RecorderFactory(zc.zrs.secondary.SecondaryFactory):

    protocol = RecorderProtocol


class Recorder(zc.zrs.secondary.Secondary):

    factoryClass = RecorderFactory
    logger = logger

    def __init__(self, addr, destination, size=500*(1<<20),
                 *args, **kw):
        storage = LogFile(destination, size)
        zc.zrs.secondary.Secondary.__init__(self, storage, addr, *args, **kw)

    def getName(self):
        return str(self._storage)

    def copyMethods(self, storage):
        return 'zrs2.1'


def replay(log, storage):
    stid = storage.lastTransaction().encode('hex')
    filenames = sorted(name for name in os.listdir(log)
                       if re.match("[0-9a-f]{16}$", name))
    before = [filename for filename in filenames if filename <= stid]

    if before:
        before = before.pop()
        f = open(os.path.join(log, before), 'rb')
        pos = 0
        if before < stid:
            # Scan to the transaction just past stid:
            f.seek(0, 2); size = f.tell()
            tid = None
            while pos < size:
                l = readlen(f, pos)
                pos += l + 8
                if f.read(1) == 'C':
                    assert l == 9
                    tid = f.read(8).encode('hex')
                    if tid >= stid:
                        break

            if tid != stid:
                raise AssertionError("storage last transaction not in log")

        stid = _replay(f, pos, storage)

    else:
        raise AssertionError("storage last transaction not in log")

    for filename in (fname for fname in filenames if fname >= stid):
        assert filename == stid, (filename, stid)
        stid = _replay(open(os.path.join(log, filename), 'rb'), 0, storage)

def _replay(f, pos, storage):
    f.seek(0, 2); size = f.tell()
    nblob = 0
    blob_file_name = blob_file_handle = record = transaction = None
    stid = None
    while pos < size:
        l = readlen(f, pos)
        message_type = f.read(1)
        data = f.read(l-1)
        pos += l+8
        assert readlen(f) == l

        if message_type == 'd':
            dbdata = data
            if not nblob:
                oid, serial, version, data_txn = record
                if blob_file_handle is None:
                    storage.restore(oid, serial, dbdata, version, data_txn,
                                    transaction)
                else:
                    storage.restoreBlob(oid, serial, dbdata, blob_file_name,
                                        data_txn, transaction)
                    blob_file_handle = None
                record = None
            continue

        if message_type == 'b':
            assert nblob > 0
            os.write(blob_file_handle, data)
            nblob -= 1
            if nblob == 0:
                os.close(blob_file_handle)
                oid, serial, version, data_txn = record
                storage.restoreBlob(oid, serial, dbdata, blob_file_name,
                                    data_txn, transaction)
                blob_file_handle = None
                record = None
            continue

        if message_type == 'C':
            storage.tpc_vote(transaction)
            storage.tpc_finish(transaction)
            assert data == transaction.id, (data, transaction.id)
            stid = data.encode('hex')
            transaction = None
            continue

        data = cPickle.loads(data)
        if message_type == 'T':
            assert transaction is None
            transaction = zc.zrs.secondary.Transaction(*data)
            storage.tpc_begin(transaction, transaction.id, transaction.status)
        elif message_type == 'S':
            record = data
        elif message_type == 'B':
            nblob = data[-1]
            record = data[:-1]
            blob_file_handle, blob_file_name = tempfile.mkstemp(
                'blob', 'replay', storage.temporaryDirectory())

    return stid
