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

import ZODB.POSException

import twisted.internet.protocol

import zc.zrs.sizedmessage

logger = logging.getLogger(__name__)

class Secondary:

    def __init__(self, storage, addr, reactor=None):
        if reactor is None:
            import zc.zrs.reactor
            reactor = zc.zrs.reactor.reactor
        self._reactor = reactor
            
        self._storage = storage

        for name in ('getName', 'sortKey', 'getSize', 'load', 'loadSerial',
                     'loadBefore', 'supportsUndo',
                     'supportsVersions', 
                     'history', 'lastTransaction',
                     'iterator', 'undoLog', 'undoInfo', 'pack', 'versionEmpty',
                     'modifiedInVersion', 'versions', 'cleanup',
                     'loadEx', 'getSerial', 'getExtensionMethods', '__len__',
                     'supportsTransactionalUndo',
                     ):
            setattr(self, name, getattr(storage, name))

        self._factory = SecondaryFactory(reactor, storage)
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

    def __init__(self, reactor, storage):
        self.protocol = SecondaryProtocol
        self.reactor = reactor
        self.storage = storage

    def close(self, callback):
        self.closed = True
        if self.connector is not None:
            self.connector.disconnect()
        callback()

    def startedConnecting(self, connector):
        if self.closed:
            connector.disconnect()
        self.connector = connector

    def clientConnectionFailed(self, connector, reason):
        if not self.closed:
            self.reactor.callLater(60, connector.connect)

    def clientConnectionLost(self, connector, reason):
        if not self.closed:
            self.reactor.callLater(60, connector.connect)


class SecondaryProtocol(twisted.internet.protocol.Protocol):

    __protocol = None
    __start = None
    __transaction = None

    def connectionMade(self):
        self.__stream = zc.zrs.sizedmessage.Stream(self.messageReceived)
        self.__peer = str(self.transport.getPeer()) + ': '
        self.transport.write(zc.zrs.sizedmessage.marshal("zrs2.0"))
        tid = self.factory.storage.lastTransaction()
        self.transport.write(zc.zrs.sizedmessage.marshal(tid))
        self.info("Connected")

    def connectionLost(self, reason):
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
        message_type, data = cPickle.loads(message)
        if message_type == 'T':
            assert self.__transaction is None
            transaction = self.__transaction = Transaction(*data)
            self.__inval = {}
            self.factory.storage.tpc_begin(
                transaction, transaction.id, transaction.status)
        elif message_type == 'S':
            oid, serial, version, data, data_txn = data
            key = serial, version
            oids = self.__inval.get(key)
            if oids is None:
                oids = self.__inval[key] = {}
            oids[oid] = 1
            
            self.factory.storage.restore(oid, serial, data, version, data_txn,
                                         self.__transaction)
        elif message_type == 'C':
            assert self.__transaction is not None            
            self.factory.storage.tpc_vote(self.__transaction)

            def invalidate(tid):
                if self.factory.db is not None:
                    for (tid, version), oids in self.__inval.items():
                        self.factory.db.invalidate(tid, oids, version=version)
            
            self.factory.storage.tpc_finish(self.__transaction, invalidate)
            self.__transaction = None
        else:
            raise ValueError("Invalid message type, %r" % message_type)

class Transaction:

    def __init__(self, tid, status, user, description, extension):
        self.id = tid
        self.status = status
        self.user = user
        self.description = description
        self._extension = extension

