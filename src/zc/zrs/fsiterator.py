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

import logging
import threading
import ZODB.BaseStorage
import ZODB.FileStorage
import ZODB.FileStorage.format
import ZODB.utils
import ZODB.TimeStamp

logger = logging.getLogger(__name__)

class TidTooHigh(Exception):
    """The last tid for an iterator is higher than any tids in a file.
    """


class FileStorageIterator(ZODB.FileStorage.format.FileStorageFormatter):

    _file_size = 1 << 64 # To make base class check happy.

    def __init__(self, fs, condition, start=ZODB.utils.z64):
        self._ltid = start
        self._fs = fs
        self._open()
        self._condition = condition

    def _open(self):
        self._old_file = self._fs._file
        file = self._file = open(self._fs._file_name, 'rb')
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
        raise ValueErr
        

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

    def next(self):
        self._condition.acquire()
        try:
            while 1:
                r = self._next()
                if r is not None:
                    return r
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
                e = loads(h.ext)
            else:
                e = {}
                
            result = RecordIterator(
                h.tid, h.status, h.user, h.descr,
                e, pos, tend, self._file, tpos)

            return result

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


