#!/usr/bin/zpython
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

"""Make sure ZRS primaries and secondaries are up and synchronized.

Usage: %(PROGRAM)s [options]

The test will connect to a ZRS primary server, and attempt to update a
`heartbeat' counter in a special sub-folder of root.  It will report success
for the primary if it updates the counter.

This script will then connect to a configurable number of secondaries and
attempt to read the heartbeat.  It will report success or failure for each
secondary, depending on whether it found the correct value in the heartbeat or
not.  The secondaries must be active (i.e. running a ZEO server) for this to
work.

The return code reflects the number of errors that have occurred.  A return
value of zero means the primary and all secondaries are healthy.

Options:

    -p addr
        Address of the primary's ZEO server.  Use `host:port' to specify the
        host and port number, or just `port' if you want to use gethostname.

    -s addr
        Address of a secondary's ZEO server.  The format of addr is the same
        as with -p, and multiple -s options are allowed.

    -S storage
        storage name (defaults to '1')

    -q / --quiet
        Do not print individual statuses, but still set the return code.

    -t timeout
        Use timeout (in float seconds) to specify how long this tool will wait
        on responses from the primary or secondaries before it will assume the
        server is unresponsive.  Default is to wait %(TIMEOUT)s seconds.

    -r retries
        If a secondary returns a heartbeat value that has an incorrect value,
        it may be because the secondaries are running a little slow.  The
        secondary will be retried this number of times (default %(RETRIES)s).

    -h / --help
        Print this text and exit.

You must specify at least one -p option.
"""

import sys
import time
import Queue
import getopt
import socket
import threading

import ZODB
from ZODB.POSException import ConflictError
from ZODB.tests.MinPO import MinPO
from ZEO.ClientStorage import ClientStorage, ClientDisconnected
from ZODB.PersistentMapping import PersistentMapping
import transaction

PROGRAM = sys.argv[0]
TIMEOUT = 30.0 # seconds
COMMASPACE = ', '
RETRIES = 5


def usage(code, msg=''):
    print __doc__ % globals()
    if msg:
        print msg
    sys.exit(code)


class Heartbeat(MinPO):
    # It's possible that an update to the root has happened between our read
    # of it and the setting of the heartbeat object.  This tells the
    # Connection that we don't care -- we just want to read the current
    # version of the object even if it's inconsistent with the other data read
    # in the current transaction.
    def _p_independent(self):
        return 1


class ZRSThread(threading.Thread):
    def __init__(self, addr, storage, queue):
        threading.Thread.__init__(self)
        self._addr = addr
        self._storage = storage
        self._queue = queue
        # We want to be able to exit even if one of these threads is hanging
        self.setDaemon(1)

    def getaddr(self):
        return self._addr

    def getstorage(self):
        return self._storage


class GetFromSecondary(ZRSThread):
    def run(self):
        reason = objvalue = None
        try:
            cs = ClientStorage(self._addr, storage=self._storage,
                               wait=1, read_only=1)
            db = ZODB.DB(cs)
            conn = db.open()
            root = conn.root()
            monitor = root['monitor']
            obj = monitor['zrsup']
            objvalue = obj.value
            conn.close()
            db.close()
        except ClientDisconnected:
            reason = 'ClientDisconnected'
        except Exception, e:
            reason = str(e)
        self._queue.put((reason, objvalue))


class PutToPrimary(ZRSThread):
    def run(self):
        reason = objvalue = None
        try:
            cs = ClientStorage(self._addr, self._storage, wait=1)
            db = ZODB.DB(cs)
            conn = db.open()
            root = conn.root()
            # We store the data in a special `monitor' dict under the root,
            # where other tools may also store such heartbeat and bookkeeping
            # type data.
            monitor = root.get('monitor')
            if monitor is None:
                monitor = root['monitor'] = PersistentMapping()
            obj = monitor.get('zrsup')
            if obj is None or not isinstance(obj, Heartbeat):
                obj = monitor['zrsup'] = Heartbeat(0)
            obj.value += 1
            objvalue = obj.value
            txn = transaction.get()
            txn.note("zrsup")
            txn.commit()
            conn.close()
            db.close()
        except ClientDisconnected:
            reason = 'ClientDisconnected'
        except ConflictError:
            reason = 'ConflictError'
        self._queue.put((reason, objvalue))


def parse_addr(arg):
    if "/" in arg:                      # AF_UNIX domain socket
        return arg
    if ":" in arg:                      # AF_INET host:port
        host, port = arg.split(":")
        port = int(port)
        return host, port
    # Assume AF_INET port only
    port = int(arg)
    return "", port


def main():
    primary = None
    secondaries = []
    storage = '1'
    quiet = 0
    timeout = TIMEOUT
    retries = RETRIES

    try:
        opts, args = getopt.getopt(sys.argv[1:],
                                   'hqp:s:S:t:r:',
                                   ['help', 'quiet'])
    except getopt.error, msg:
        usage(1, msg)

    if args:
        usage(1, 'Invalid extra arguments: %s' % COMMASPACE.join(args))

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage(0)
        elif opt in ('-q', '--quiet'):
            quiet = 1
        elif opt == '-t':
            timeout = float(arg)
        elif opt == '-r':
            retries = int(arg)
        elif opt == '-p':
            if primary is not None:
                usage(1, 'Only one -p option allowed')
            try:
                primary = parse_addr(arg)
            except ValueError:
                usage(1, 'Bad -p option: %s' % arg)
        elif opt == '-s':
            try:
                addr = parse_addr(arg)
            except ValueError:
                usage(1, 'Bad -s option: %s' % arg)
            secondaries.append(addr)
        elif opt == '-S':
            storage = arg

    if primary is None:
        usage(1, '-p option is required')

    # connect to and write to the primary
    queue = Queue.Queue()
    t0 = time.time()
    tpri = PutToPrimary(primary, storage, queue)
    tpri.start()
    tpri.join(timeout)
    if tpri.isAlive():
        if not quiet:
            print 'FAILED %s:%s primary: timed out writing update' % primary
        return 1
    # Don't block
    try:
        reason, objvalue = queue.get(0)
    except Queue.Empty:
        if not quiet:
            print 'FAILED %s:%s primary: no results available' % primary
        return 1

    if reason is not None:
        if not quiet:
            print 'FAILED %s:%s primary:' % primary, reason
        return 1

    if not quiet:
        print 'healthy %s:%s primary' % primary

    # Okay, the primary was fine, now let's check the secondaries.  First
    # create all the secondary threads
    if not secondaries:
        return 0
    tsecs = {}
    status = {}
    for addr in secondaries:
        q = Queue.Queue()
        t = GetFromSecondary(addr, storage, q)
        # Value is threadobj, queueobj, retries, deadline
        tsecs[addr] = (t, q, 0, time.time() + timeout)
        status[addr] = None
        t.start()

    # Now let's check each one until we have a definitive answer
    while tsecs:
        for addr, (t, q, r, d) in tsecs.items():
            # Wait only 1 second per server.  If we haven't gotten an answer
            # after that and we haven't hit the timeout deadline, we'll just
            # continue.  This should play nice with retries.
            t.join(1.0)
            if t.isAlive():
                if time.time() >= d:
                    # No answer from this secondary within the timeout period
                    status[addr] = 'no answer'
                    del tsecs[addr]
                continue
            # Don't block
            try:
                reason, secvalue = q.get(0)
            except Queue.Empty:
                # Hmm, the thread is dead but there's nothing in the queue
                status[addr] = 'no results available'
                del tsecs[addr]
                continue
            # Check for values in the thread
            if reason is not None:
                status[addr] = reason
                del tsecs[addr]
            # Did the values match?
            elif secvalue == objvalue:
                status[addr] = None # means success
                del tsecs[addr]
            # Have we maxed out our retries?
            elif r+1 >= retries:
                status[addr] = "values didn't match after %d retries" % r
                del tsecs[addr]
            # Let's retry this one
            else:
                t = GetFromSecondary(t.getaddr(), t.getstorage(), q)
                t.start()
                tsecs[addr] = (t, q, r+1, d)

    t1 = time.time()
    rtncode = 0
    sorted = status.keys()
    sorted.sort()
    for addr in sorted:
        reason = status[addr]
        if reason is None:
            if not quiet:
                print 'healthy %s:%s secondary' % addr
        else:
            if not quiet:
                print 'FAILED %s:%s secondary:' % addr, reason
            rtncode += 1
    if not quiet:
        print 'Total elapsed time: %.2f' % (t1 - t0)
    return rtncode



if __name__ == '__main__':
    status = main()
    sys.exit(status)
