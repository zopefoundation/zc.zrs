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

class Primary:

    def __init__(self, storage, addr, listener):
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

        
        listener(addr, self)

    def __call__(self, connection):
        # We got a client connection.
        SecondaryServer(self._storage, self._changed, connection)

    def tpc_finish(self, *args):
        self.storage.tpc_finish(*args)
        self._changed.notifyAll()

    def close(self):
        pass


class SecondaryServer:

    def __init__(self, storage, changed, connection):
        self._storage = storage
        self._connection = connection
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
        
