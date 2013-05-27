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
import binascii
import ZODB.blob
import ZODB.interfaces
import zope.interface

class XformStorage(object):

    zope.interface.implements(ZODB.interfaces.IStorageWrapper)

    copied_methods = (
        'close', 'getName', 'getSize', 'history', 'isReadOnly',
        'lastTransaction', 'new_oid', 'sortKey',
        'tpc_abort', 'tpc_begin', 'tpc_finish', 'tpc_vote',
        'loadBlob', 'openCommittedBlobFile', 'temporaryDirectory',
        'supportsUndo', 'undo', 'undoLog', 'undoInfo',

        # The following should use base methods when in server mode
        'load', 'loadBefore', 'loadSerial', 'store', 'restore',
        'iterator', 'storeBlob', 'restoreBlob', 'record_iternext',
        )

    def __init__(self, base, transform, untransform, prefix, server=False):
        if len(prefix) != 1:
            raise ValueError("Must give a 1-character prefix", prefix)
        prefix = '.' + prefix

        for name in self.copied_methods:
            v = getattr(base, name, None)
            if v is not None:
                setattr(self, name, v)

        if transform is not None:
            def _transform(data):
                if data and (data[:2] != prefix):
                    transformed = transform(data)
                    if transformed is not data:
                        data = prefix + transformed
                return data
            def transform_record_data(data):
                return _transform(self._db_transform(data))
        else:
            _transform = lambda data: data
            def transform_record_data(data):
                return self._db_transform(data)
        self.transform_record_data = transform_record_data

        def _untransform(data):
            if data and (data[:2] == prefix):
                return untransform(data[2:])
            else:
                return data
        self._untransform = _untransform
        def untransform_record_data(data):
            return self._db_untransform(_untransform(data))
        self.untransform_record_data = untransform_record_data


        if (not server) and (transform is not None):
            def store(oid, serial, data, version, transaction):
                return base.store(
                    oid, serial, _transform(data), version, transaction)
            self.store = store

            def restore(oid, serial, data, version, prev_txn, transaction):
                return base.restore(oid, serial, _transform(data),
                                    version, prev_txn, transaction)
            self.restore = restore

            def storeBlob(oid, oldserial, data, blobfilename, version,
                          transaction):
                return base.storeBlob(oid, oldserial, _transform(data),
                                      blobfilename, version, transaction)
            self.storeBlob = storeBlob

            def restoreBlob(oid, serial, data, blobfilename, prev_txn,
                            transaction):
                return base.restoreBlob(oid, serial, _transform(data),
                                        blobfilename, prev_txn, transaction)
            self.restoreBlob = restoreBlob

        if not server:

            def load(oid, version=''):
                data, serial = base.load(oid, version)
                return _untransform(data), serial
            self.load = load

            def loadBefore(oid, tid):
                r = base.loadBefore(oid, tid)
                if r is not None:
                    data, serial, after = r
                    return _untransform(data), serial, after
                else:
                    return r
            self.loadBefore = loadBefore

            def loadSerial(oid, serial):
                return _untransform(base.loadSerial(oid, serial))
            self.loadSerial = loadSerial

            def iterator(start=None, stop=None):
                for t in base.iterator(start, stop):
                    yield Transaction(t, _untransform)
            self.iterator = iterator

            def record_iternext(next=None):
                oid, tid, data, next = self.base.record_iternext(next)
                return oid, tid, data[2:].decode('hex'), next
            self.record_iternext = record_iternext

        zope.interface.directlyProvides(self, zope.interface.providedBy(base))

        self.base = base
        base.registerDB(self)

    def __getattr__(self, name):
        return getattr(self.base, name)

    def __len__(self):
        return len(self.base)

    def registerDB(self, db):
        self.db = db
        self._db_transform = db.transform_record_data
        self._db_untransform = db.untransform_record_data

    _db_transform = _db_untransform = lambda self, data: data

    def pack(self, pack_time, referencesf, gc=True):
        _untransform = self._untransform
        def refs(p, oids=None):
            return referencesf(_untransform(p), oids)
        return self.base.pack(pack_time, refs, gc)

    def invalidateCache(self):
        return self.db.invalidateCache()

    def invalidate(self, transaction_id, oids, version=''):
        return self.db.invalidate(transaction_id, oids, version)

    def references(self, record, oids=None):
        return self.db.references(self._untransform(record), oids)

    def copyTransactionsFrom(self, other):
        ZODB.blob.copyTransactionsFromTo(other, self)

def xiterator(baseit, untransform, prefix):
    if len(prefix) != 1:
        raise ValueError("Most give a 1-character prefix", prefix)
    prefix = '.' + prefix

    def _untransform(data):
        if data and (data[:2] == prefix):
            return untransform(data[2:])
        else:
            return data

    def iterator(start=None, stop=None):
        for t in baseit:
            yield Transaction(t, _untransform)

class Transaction(object):
    def __init__(self, transaction, untransform):
        self.__transaction = transaction
        self.__untransform = untransform

    def __iter__(self):
        untransform = self.__untransform
        for r in self.__transaction:
            if r.data:
                r.data = untransform(r.data)
            yield r

    def __getattr__(self, name):
        return getattr(self.__transaction, name)

def HexStorage(base, server=False):
    return XformStorage(base, binascii.b2a_hex, binascii.a2b_hex, 'h', server)


class ZConfigHex:

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

    def open(self):
        return HexStorage(self.config.base.open())

class ZConfigServerHex(ZConfigHex):

    def open(self):
        return HexStorage(self.config.base.open(), True)
