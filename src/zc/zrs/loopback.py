# Copyright (c) 2001-2004 Twisted Matrix Laboratories.
# See TWISTED-LICENSE for details.

"""Loopback-transport support

This is derived from and will eventually feed back to
twisted.protocols.loopback.
"""


# system imports
from zope.interface import implements

# Twisted Imports
from twisted.internet import interfaces, protocol, main, defer
from twisted.python import failure
from twisted.internet.interfaces import IAddress

class _LoopbackQueue(object):
    """
    Trivial wrapper around a list to give it an interface like a queue, which
    the addition of also sending notifications by way of a Deferred whenever
    the list has something added to it.
    """

    _notificationDeferred = None
    disconnect = False

    def __init__(self):
        self._queue = []


    def put(self, v):
        self._queue.append(v)
        if self._notificationDeferred is not None:
            d, self._notificationDeferred = self._notificationDeferred, None
            d.callback(None)


    def __nonzero__(self):
        return bool(self._queue)


    def get(self):
        return self._queue.pop(0)



class _LoopbackAddress(object):
    implements(IAddress)


class _LoopbackTransport(object):
    implements(interfaces.ITransport, interfaces.IConsumer)

    disconnecting = False
    producer = None

    # ITransport
    def __init__(self, q, proto):
        self.q = q
        self.proto = proto

    def write(self, bytes):
        self.q.put(bytes)

    def writeSequence(self, iovec):
        self.q.put(''.join(iovec))

    def loseConnection(self):
        self.q.disconnect = True
        self.q.put('')

    def getPeer(self):
        return _LoopbackAddress()

    def getHost(self):
        return _LoopbackAddress()

    # IConsumer
    def registerProducer(self, producer, streaming):
        assert self.producer is None
        self.producer = producer
        self.streamingProducer = streaming
        self._pollProducer()

    def unregisterProducer(self):
        assert self.producer is not None
        self.producer = None

    def _pollProducer(self):
        if self.producer is not None and not self.streamingProducer:
            self.producer.resumeProducing()

    def connectionLost(self, reason):
        if self.producer is not None:
            self.producer.stopProducing()
            self.unregisterProducer()
        self.proto.connectionLost(reason)

class _ClientLoopbackTransport(_LoopbackTransport):

    connector = None
    def connectionLost(self, reason):
        self.proto.connectionLost(reason)
        if self.connector is not None:
            self.connector.connectionLost(reason)

def loopbackAsync(server, client, connector):
    serverToClient = _LoopbackTransport(_LoopbackQueue(), server)
    serverToClient.reactor = connector.reactor

    clientToServer = _ClientLoopbackTransport(_LoopbackQueue(), client)
    clientToServer.reactor = connector.reactor
    clientToServer.connector = connector

    server.makeConnection(serverToClient)
    client.makeConnection(clientToServer)

    _loopbackAsyncBody(serverToClient, clientToServer)



def _loopbackAsyncBody(serverToClient, clientToServer):

    def pump(source, q, target):
        sent = False
        while q:
            sent = True
            bytes = q.get()
            if bytes:
                target.dataReceived(bytes)

        # A write buffer has now been emptied.  Give any producer on that side
        # an opportunity to produce more data.
        source.transport._pollProducer()

        return sent

    server = serverToClient.proto
    client = clientToServer.proto

    while 1:
        disconnect = clientSent = serverSent = False

        # Deliver the data which has been written.
        serverSent = pump(server, serverToClient.q, client)
        clientSent = pump(client, clientToServer.q, server)

        if not clientSent and not serverSent:
            # Neither side wrote any data.  Wait for some new data to be added
            # before trying to do anything further.
            d = defer.Deferred()
            clientToServer.q._notificationDeferred = d
            serverToClient.q._notificationDeferred = d
            d.addCallback(_loopbackAsyncContinue,
                          serverToClient, clientToServer)
            break

        if serverToClient.q.disconnect:
            # The server wants to drop the connection.  Flush any remaining
            # data it has.
            disconnect = True
            pump(server, serverToClient.q, client)
        elif clientToServer.q.disconnect:
            # The client wants to drop the connection.  Flush any remaining
            # data it has.
            disconnect = True
            pump(client, clientToServer.q, server)

        if disconnect:
            # Someone wanted to disconnect, so okay, the connection is gone.
            serverToClient.connectionLost(
                failure.Failure(main.CONNECTION_DONE))
            clientToServer.connectionLost(
                failure.Failure(main.CONNECTION_DONE))
            break


def _loopbackAsyncContinue(ignored, serverToClient, clientToServer):
    # Clear the Deferred from each message queue, since it has already fired
    # and cannot be used again.
    clientToServer.q._notificationDeferred = None
    serverToClient.q._notificationDeferred = None

    # Push some more bytes around.
    _loopbackAsyncBody(serverToClient, clientToServer)
