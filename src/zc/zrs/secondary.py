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
import threading

import ZODB.POSException

import twisted.internet.protocol

import zc.zrs.reactor
import zc.zrs.sizedmessage

logger = logging.getLogger(__name__)

class Secondary:

    def __init__(self, storage, addr, reactor=None, reconnect_delay=60,
                 check_checksums=True):
        if reactor is None:
            reactor = zc.zrs.reactor.reactor()
        self._reactor = reactor
            
        self._storage = storage

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
                                         check_checksums)
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

    def __init__(self, reactor, storage, reconnect_delay, check_checksums):
        self.protocol = SecondaryProtocol
        self.reactor = reactor
        self.storage = storage
        self.reconnect_delay = reconnect_delay
        self.check_checksums = check_checksums

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

    __protocol = None
    __start = None
    __transaction = None
    __record = None

    def connectionMade(self):
        self.__stream = zc.zrs.sizedmessage.Stream(self.messageReceived)
        self.__peer = str(self.transport.getPeer()) + ': '
        self.factory.instance = self
        self.transport.write(zc.zrs.sizedmessage.marshal("zrs2.0"))
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

    def messageReceived(self, message):
        if self.__record is None:
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
        else:
            oid, serial, version, data_txn = self.__record
            self.__record = None
            data = message or None
            key = serial, version
            oids = self.__inval.get(key)
            if oids is None:
                oids = self.__inval[key] = {}
            oids[oid] = 1

            self.factory.storage.restore(
                oid, serial, data, version, data_txn,
                self.__transaction)
        self.__md5.update(message)

class Transaction:

    def __init__(self, tid, status, user, description, extension):
        self.id = tid
        self.status = status
        self.user = user
        self.description = description
        self._extension = extension

