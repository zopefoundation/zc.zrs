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
import ngi.adapters

logger = logging.getLogger('zc.zrs.secondary')

class Transaction:

    def __init__(self, id, status, user, description, _extension):
        self.id = id
        self.status = status
        self.user = user
        self.description = description
        self._extension = _extension
        self.oids = {}

class Secondary:

    def __init__(self, addresses, storage, connector):
        self._addresses = addresses
        self._storage = storage
        for meth in (
            'getSize', 'load', 'loadSerial', 'loadBefore', 'history',
            'lastTransaction', pack
            ):
            setattr(self, meth, getattr(storage, meth))

        
        self._connection = None
        self._connector = connector
        self._transaction = None
        self._db = None
        self._closed = False
        self._connect_lock = threading.Lock()

        self._connect()

    def registerDB(self, db):
        self._db = db

    def _connect(self):
        for addr in self.addresses:
            self._connector(addr, self)

    def connected(self, connection):
        self._connect_lock.acquire()
        try:
            if self._connection is not None or self._closed:
                # We are already connected (or closed).  Close the new one
                connection.close()
                return
 
            connection = ngi.adapters.Sized(connection)
            self._connection = connection
            connection.write('zrs 2') # protocol
            connection.write(self.lastTransaction())
            connection.setHandler(self)
        finally:
            self._connect_lock.release()

    def handle_input(self, connection, data):
        rtype, record = cPickle.loads(data) # XXX prevent class loading
        if rtype == 'T':
            # start transaction
            assert self._transaction is None
            self._transaction = Transaction(*record)
            self._storage.tpc_begin(self._transaction, id, status)
        elif rtype == 'C':
            # commit
            self._storage.tpc_vote(self._transaction)
            self._storage.tpc_finish(self._transaction)
            if self._db is not None:
                self._db.invalidate(self._transaction.id,
                                    self._transaction.oids)
            self._transaction = None
        else:
            # store
            assert rtype == 'S'
            oid, tid, data = record
            self._transaction.oids[oid] = 1
            self._storage.restore(oid, tid, data, '', None, trans)

    def close(self):
        self._connect_lock.acquire()
        try:
            self._closed = True
            if self._connection is not None:
                self._connection.close()
        finally:
            self._connect_lock.release()
        
        self._storage.close()

    def handle_close(self, connnection, reason):
        self._connect()

    def getName(self):
        return 'Secondary(%r)' % self._addrs

    def sortKey(self):
        return ''
        
    def supportsUndo(self):
        return False

    def isReadOnly(self):
        return True
        

    
