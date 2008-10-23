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
import md5
import os
import tempfile
import threading
import twisted.internet.protocol
import zc.zrs.reactor
import zc.zrs.sizedmessage
import ZODB.blob
import ZODB.POSException
import zope.interface

logger = logging.getLogger(__name__)

if not hasattr(ZODB.blob.BlobStorage, 'restoreBlob'):
    import zc.zrs.restoreblob
    blob_iface = ZODB.interfaces.IBlobStorage
else:
    blob_iface = ZODB.interfaces.IBlobStorageRestoreable
    

class Secondary:

    def __init__(self, storage, addr, reactor=None, reconnect_delay=60,
                 check_checksums=True):
        if reactor is None:
            reactor = zc.zrs.reactor.reactor()
        self._reactor = reactor
            
        self._storage = storage
        if blob_iface.providedBy(storage) and hasattr(storage, 'restoreBlob'):
            zope.interface.directlyProvides(self, blob_iface)
            for name in ('loadBlob', 'temporaryDirectory', 'restoreBlob'):
                setattr(self, name, getattr(storage, name))
            zrs_proto = 'zrs2.1'
        else:
            zrs_proto = 'zrs2.0'

        # required methods
        for name in (
            'getName', 'getSize', 'history', 'lastTransaction',
            '__len__', 'load', 'loadBefore', 'loadSerial', 'pack', 
            'sortKey',
            ):
            setattr(self, name, getattr(storage, name))

        # Optional methods:
        for name in (
            'iterator', 'cleanup', 'loadEx', 'getSerial',
            'getExtensionMethods', 'supportsTransactionalUndo',
            'tpc_transaction', 'getTid', 'lastInvalidations',
            'supportsUndo', 'undoLog', 'undoInfo',
            'supportsVersions',
            'versionEmpty', 'modifiedInVersion', 'versions', 
            'record_iternext',
            ):
            if hasattr(storage, name):
                setattr(self, name, getattr(storage, name))

        self._factory = SecondaryFactory(reactor, storage, reconnect_delay,
                                         check_checksums, zrs_proto)
        self._addr = addr
        host, port = addr
        logger.info("Opening %s %s", self.getName(), addr)
        reactor.callFromThread(reactor.connectTCP, host, port, self._factory)

    def isReadOnly(self):
        return True

    def write_method(*args, **kw):
        raise ZODB.POSException.ReadOnlyError()
    new_oid = tpc_begin = undo = write_method

    def registerDB(self, db, limit=None):
        self._factory.db = db

    def close(self):
        logger.info('Closing %s %s', self.getName(), self._addr)
        event = threading.Event()
        self._reactor.callFromThread(self._factory.close, event.set)
        event.wait()
        self._storage.close()

class SecondaryFactory(twisted.internet.protocol.ClientFactory):

    db = None
    connector = None
    closed = False

    # We'll keep track of the connected instance, if any mainly
    # for the convenience of some tests that want to force disconnects to
    # stress the secondaries.
    instance = None

    def __init__(self, reactor, storage, reconnect_delay, check_checksums,
                 zrs_proto):
        self.protocol = SecondaryProtocol
        self.reactor = reactor
        self.storage = storage
        self.reconnect_delay = reconnect_delay
        self.check_checksums = check_checksums
        self.zrs_proto = zrs_proto

    def close(self, callback):
        self.closed = True
        if self.connector is not None:
            self.connector.disconnect()
        callback()

    def startedConnecting(self, connector):
        if self.closed:
            connector.disconnect()
        else:
            self.connector = connector

    def clientConnectionFailed(self, connector, reason):
        self.connector = None
        if not self.closed:
            self.reactor.callLater(self.reconnect_delay, connector.connect)

    def clientConnectionLost(self, connector, reason):
        self.connector = None
        if not self.closed:
            self.reactor.callLater(self.reconnect_delay, connector.connect)


class SecondaryProtocol(twisted.internet.protocol.Protocol):

    __transaction = None

    def connectionMade(self):
        self.__stream = zc.zrs.sizedmessage.Stream(self.messageReceived)
        self.__peer = str(self.transport.getPeer()) + ': '
        self.factory.instance = self
        self.transport.write(zc.zrs.sizedmessage.marshal(
            self.factory.zrs_proto))
        tid = self.factory.storage.lastTransaction()
        self.__md5 = md5.new(tid)
        self.transport.write(zc.zrs.sizedmessage.marshal(tid))
        self.info("Connected")

    def connectionLost(self, reason):
        self.factory.instance = None
        if self.__transaction is not None:
            self.factory.storage.tpc_abort(self.__transaction)
            self.__transaction = None
        self.info("Disconnected %r", reason)

    def error(self, message, *args, **kw):
        logger.critical(self.__peer + message, *args, **kw)
        self.factory.connector.disconnect()

    def info(self, message, *args):
        logger.info(self.__peer + message, *args)

    def dataReceived(self, data):
        try:
            self.__stream(data)
        except:
            self.error("Input data error", exc_info=True)


    __blob_file_blocks = None
    __blob_file_handle = None
    __blob_file_name = None
    __blob_record = None
    __record = None
    def messageReceived(self, message):
        if self.__record:
            # store or store blob data record
            oid, serial, version, data_txn = self.__record
            self.__record = None
            data = message or None
            key = serial, version
            oids = self.__inval.get(key)
            if oids is None:
                oids = self.__inval[key] = {}
            oids[oid] = 1

            if self.__blob_file_blocks:
                # We have to collect blob data
                self.__blob_record = oid, serial, data, version, data_txn
                
                (self.__blob_file_handle, self.__blob_file_name
                 ) = tempfile.mkstemp('blob', 'secondary',
                                      self.factory.storage.temporaryDirectory()
                                      )                
            else:
                self.factory.storage.restore(
                    oid, serial, data, version, data_txn,
                    self.__transaction)

        elif self.__blob_record:
            os.write(self.__blob_file_handle, message)
            self.__blob_file_blocks -= 1
            if self.__blob_file_blocks == 0:
                # We're done collecting data, we can write the data:
                os.close(self.__blob_file_handle)
                oid, serial, data, version, data_txn = self.__blob_record
                self.__blob_record = None
                self.factory.storage.restoreBlob(
                    oid, serial, data, self.__blob_file_name, data_txn,
                    self.__transaction)

        else:
            # Ordinary message
            message_type, data = cPickle.loads(message)
            if message_type == 'T':
                assert self.__transaction is None
                assert self.__record is None
                transaction = Transaction(*data)
                self.__inval = {}
                self.factory.storage.tpc_begin(
                    transaction, transaction.id, transaction.status)
                self.__transaction = transaction
            elif message_type == 'S':
                self.__record = data
            elif message_type == 'B':
                self.__record = data[:-1]
                self.__blob_file_blocks = data[-1]
            elif message_type == 'C':
                if self.factory.check_checksums and (
                    data[0] != self.__md5.digest()):
                    raise AssertionError(
                        "Bad checksum", data[0], self.__md5.digest())
                assert self.__transaction is not None            
                assert self.__record is None
                self.factory.storage.tpc_vote(self.__transaction)

                def invalidate(tid):
                    if self.factory.db is not None:
                        for (tid, version), oids in self.__inval.items():
                            self.factory.db.invalidate(
                                tid, oids, version=version)

                self.factory.storage.tpc_finish(self.__transaction, invalidate)
                self.__transaction = None
            else:
                raise ValueError("Invalid message type, %r" % message_type)

        self.__md5.update(message)

class Transaction:

    def __init__(self, tid, status, user, description, extension):
        self.id = tid
        self.status = status
        self.user = user
        self.description = description
        self._extension = extension

