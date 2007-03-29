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

import threading
import cPickle
import zope.interface

import twisted.internet
import twisted.internet.protocol
import twisted.internet.interfaces

import zc.zrs.fsiterator
import zc.zrs.sizedmessage

class Primary:

    def __init__(self, storage, addr, reactor):
        self._storage = storage
        self._changed = threading.Condition()

        for name in ('getName', 'sortKey', 'getSize', 'load', 'loadSerial',
                     'loadBefore', 'new_oid', 'store', 'supportsUndo',
                     'supportsVersions', 'tpc_abort', 'tpc_begin', 'tpc_vote',
                     'history', 'registerDB', 'lastTransaction', 'isReadOnly',
                     'iterator', 'undo', 'undoLog', 'undoInfo', 'pack',
                     'abortVersion', 'commitVersion', 'versionEmpty',
                     'modifiedInVersion', 'versions'):
            setattr(self, name, getattr(storage, name))

        self._factory = SecondaryServerFactory(storage, self._changed)
        interface, port = addr
        reactor.callFromThread(reactor.listenTCP, port, self._factory,
                               interface=interface)

    def tpc_finish(self, *args):
        self._storage.tpc_finish(*args)
        self._changed.acquire()
        self._changed.notifyAll()
        self._changed.release()

    def close(self):
        self._factory.close()
        self._storage.close()


class SecondaryServerProtocol(twisted.internet.protocol.Protocol):

    __protocol = None
    __start = None

    def connectionMade(self):
        self.__stream = zc.zrs.sizedmessage.Stream(self.messageReceived, 8)

    def dataReceived(self, data):
        try:
            self.__stream(data)
        except zc.zrs.sizedmessage.LimitExceeded:
            self.transport.loseConnection()

    def messageReceived(self, data):
        if self.__protocol is None:
            if data != 'zrs2.0':
                self.transport.loseConnection()
            self.__protocol = data
        else:
            if self.__start is not None:
                self.transport.loseConnection()
            self.__start = data
            iterator = zc.zrs.fsiterator.FileStorageIterator(
                self.factory.storage, self.factory.changed, self.__start)
            producer = SecondaryServerProducer(iterator, self.transport)
            self.transport.registerProducer(producer, True)
            thread = threading.Thread(target=producer.run)
            thread.setDaemon(True)
            thread.start()
 
class SecondaryServerFactory(twisted.internet.protocol.Factory):

    protocol = SecondaryServerProtocol

    def __init__(self, storage, changed):
        self.storage = storage
        self.changed = changed

class SecondaryServerProducer:

    zope.interface.implements(twisted.internet.interfaces.IPushProducer)

    stopped = False

    def __init__(self, iterator, transport):
        self.iterator = iterator
        self.transport = transport
        self.callFromThread = transport.reactor.callFromThread
        self.event = threading.Event()
        self.pauseProducing = self.event.clear
        self.resumeProducing = self.event.set
        self.wait = self.event.wait
    
    def stopProducing(self):
        self.stopped = True
        self.event.set()

    def write(self, data):
        self.event.wait()
        if not self.stopped:
            self.callFromThread(self.transport.write, data)
            
    def run(self):
        for trans in self.iterator:
            self.write(
                cPickle.dumps(('T', (trans.tid, trans.status, trans.user,
                                     trans.description, trans._extension)))
                )
            for record in trans:
                assert record.version == ''
                self.write(
                    cPickle.dumps(('S', (record.oid, record.tid, record.data)))
                    )
            self.write(cPickle.dumps(('C', ())))
            if self.stopped:
                break


class SecondaryServer:

    def __init__(self, storage, changed, connection):
        self._storage = storage
        self._connection = BlockingWriter(connection)
        self._protocol = None
        self._start = None
        self._changed = changed
        connection.setHandler(self)        

    def handle_input(self, connection, data):
        if self._protocol is None:
            assert data == 'zrs 2'
            self._protocol = data
        else:
            assert self._start is None
            self._start = data
            thread = threading.Thread(target=self.run)
            thread.setDaemon(True)
            thread.start()

    def handle_close(self, connection, reason):
        self._connection = None

    def run(self):
        write = self._connection.write
        iterator = zc.zrs.fsiterator.FileStorageIterator(
            self._storage, self._changed, self._start)
        for trans in iterator:
            write(cPickle.dumps(('T', (trans.id, trans.status, trans.user,
                                       trans.description, trans._extension)))
                  )
            for record in trans:
                assert record.version == ''
                write(
                    cPickle.dumps(('S', (record.oid, record.tid, record.data)))
                    )
            write(cPickle.dumps(('C', ())))
            
            
            


    
    def run(self):
        # Write transactions starting at self._start
        
        iterator = self._storage.iterator(self._start)
        self.send(iterator)
        self._changed.acquire()

        while self._connection is not None:
            self._changed.wait()
            self.send(iterator)
            
                
    def send(self, iterator):
        # Send data from an iterator until it is exhausted

        # XXX what happens if the connection is closed while we are
        # writing?  It would be nice if there was an exception that we
        # could catch reliably.  Probably something we need in ngi.

        while self._connection is not None:
            try:
                trans = iterator.next()
            except IndexError:
                return
            
            self._connection.write(
                cPickle.dumps(('T', (trans.id, trans.status, trans.user,
                                     trans.description, trans._extension)))
                )
            for record in trans:
                assert record.version == ''
                self._connection.write(
                    cPickle.dumps(('S', (record.oid, record.tid, record.data)))
                    )
            self._connection.write(cPickle.dumps(('C', ())))
        
