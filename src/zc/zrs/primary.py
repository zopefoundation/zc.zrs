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

import ZODB.TimeStamp

import zope.interface

import twisted.internet.protocol
import twisted.internet.interfaces

import zc.zrs.fsiterator
import zc.zrs.sizedmessage

logger = logging.getLogger(__name__)

class Primary:

    def __init__(self, storage, addr, reactor=None):
        if reactor is None:
            import zc.zrs.reactor
            reactor = zc.zrs.reactor.reactor
            
        self._storage = storage
        self._changed = threading.Condition()

        for name in ('getName', 'sortKey', 'getSize', 'load', 'loadSerial',
                     'loadBefore', 'new_oid', 'store', 'supportsUndo',
                     'supportsVersions', 'tpc_abort', 'tpc_begin', 'tpc_vote',
                     'history', 'registerDB', 'lastTransaction', 'isReadOnly',
                     'iterator', 'undo', 'undoLog', 'undoInfo', 'pack',
                     'abortVersion', 'commitVersion', 'versionEmpty',
                     'modifiedInVersion', 'versions', 'cleanup',
                     'loadEx', 'getSerial', 'getExtensionMethods', '__len__',
                     'supportsTransactionalUndo',
                     ):
            setattr(self, name, getattr(storage, name))

        self._factory = PrimaryFactory(storage, self._changed)
        self._addr = addr
        interface, port = addr
        logger.info("Opening %s %s", self.getName(), addr)
        reactor.callFromThread(reactor.listenTCP, port, self._factory,
                               interface=interface)

    def tpc_finish(self, *args):
        self._storage.tpc_finish(*args)
        self._changed.acquire()
        self._changed.notifyAll()
        self._changed.release()

    def close(self):
        logger.info('Closing %s %s', self.getName(), self._addr)
        self._factory.close()
        self._storage.close()


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
        self.factory.instances.remove(self)

    def close(self):
        if self.__producer is not None:
            self.__producer.close()
        self.info('Closed')

    def error(self, message, *args):
        logger.error(self.__peer + message, *args)
        if self.__producer is not None:
            self.__producer.close()
        else:
            self.transport.loseConnection()

    def info(self, message, *args):
        logger.info(self.__peer + message, *args)

    def dataReceived(self, data):
        try:
            self.__stream(data)
        except zc.zrs.sizedmessage.LimitExceeded, v:
            self.error(str(v))

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
            iterator = zc.zrs.fsiterator.FileStorageIterator(
                self.factory.storage, self.factory.changed, self.__start)
            self.__producer = PrimaryProducer(
                iterator, self.transport, self.__peer)
 
class PrimaryFactory(twisted.internet.protocol.Factory):

    protocol = PrimaryProtocol

    def __init__(self, storage, changed):
        self.storage = storage
        self.changed = changed
        self.instances = []

    def close(self):
        for instance in list(self.instances):
            instance.close()

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
        self.close_event = threading.Event()
        thread = threading.Thread(target=self.run)
        thread.setDaemon(True)
        thread.start()

    def close(self):
        self.stopProducing()
        self.callFromThread(self.transport.unregisterProducer)
        self.callFromThread(self.transport.loseConnection)
        self.close_event.wait()
    
    def stopProducing(self):
        self.iterator.stop()
        self.stopped = True
        self.event.set()

    def write(self, data):
        data = zc.zrs.sizedmessage.marshal(data)
        self.event.wait()
        if not self.stopped:
            self.callFromThread(self.transport.write, data)
            
    def run(self):
        try:
            for trans in self.iterator:
                self.write(
                    cPickle.dumps(('T', (trans.tid, trans.status, trans.user,
                                         trans.description, trans._extension)))
                    )
                for record in trans:
                    self.write(
                        cPickle.dumps(('S',
                                       (record.oid, record.tid, record.version,
                                        record.data, record.data_txn)))
                        )
                self.write(cPickle.dumps(('C', ())))
                if self.stopped:
                    break
            self.close_event.set()
        except:
            logger.exception(self.peer)
            self.callFromThread(self.transport.unregisterProducer)
            self.callFromThread(self.transport.loseConnection)
