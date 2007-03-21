##############################################################################
#
# Copyright (c) 2006 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import os, shutil, tempfile, threading, time, unittest
import transaction
from zope.testing import doctest
import zc.zrs.fsiterator
from ZODB.TimeStamp import TimeStamp


def setUp(test):
    global now
    now = time.mktime((2007, 3, 21, 15, 32, 57, 2, 80, 0))
    td = test.globs['__tear_down__'] = []
    oldtime = time.time
    td.append(lambda : setattr(time, 'time', oldtime))
    time.time = lambda : now
    def commit():
        global now
        now += 1
        transaction.commit()
    test.globs['commit'] = commit
    here = os.getcwd()
    td.append(lambda : os.chdir(here))
    tmp = tempfile.mkdtemp('test')
    td.append(lambda : shutil.rmtree(tmp))
    os.chdir(tmp)
    
def tearDown(test):
    for f in test.globs['__tear_down__']:
        f()

def scan_from_back():
    r"""
Create the database:

    >>> import ZODB.FileStorage
    >>> from ZODB.DB import DB
    >>> import persistent.dict

    >>> fs = ZODB.FileStorage.FileStorage('Data.fs')
    >>> db = DB(fs)
    >>> conn = db.open()

    >>> for i in range(100):
    ...     conn.root()[i] = persistent.dict.PersistentDict()
    ...     commit()

Now, be evil, and muck up the beginning: :)

    >>> fs._file.seek(12)
    >>> fs._file.write('\xff'*8)
    >>> conn.root()[100] = persistent.dict.PersistentDict()
    >>> commit()

If we try to iterate from the beginning, we'll get an error:

    >>> condition = threading.Condition()
    >>> it = zc.zrs.fsiterator.FileStorageIterator(fs, condition)
    >>> it.next()
    Traceback (most recent call last):
    ...
    CorruptedDataError: Error reading unknown oid.  Found '' at 4

    >>> def tid_from_time(t):
    ...     return repr(TimeStamp(*(time.gmtime(t)[:5] + (t%60,))))

    >>> tid = tid_from_time(time.time()-70)
    >>> zc.zrs.fsiterator.FileStorageIterator(fs, condition, tid)
    Traceback (most recent call last):
    ...
    OverflowError: long too big to convert

But, if we iterate from near the end, we'll be OK:

    >>> tid = tid_from_time(time.time()-30)
    >>> it = zc.zrs.fsiterator.FileStorageIterator(fs, condition, tid)
    >>> trans = it.next()
    >>> from ZODB import utils
    >>> print TimeStamp(trans.tid), [utils.u64(r.oid) for r in trans]
    2007-03-21 20:34:09.000000 [0L, 72L]

    >>> print TimeStamp(tid)
    2007-03-21 20:34:08.000000

    >>> tid = tid_from_time(time.time()-29.5)
    >>> it = zc.zrs.fsiterator.FileStorageIterator(fs, condition, tid)
    >>> trans = it.next()
    >>> from ZODB import utils
    >>> print TimeStamp(trans.tid), [utils.u64(r.oid) for r in trans]
    2007-03-21 20:34:09.000000 [0L, 72L]

    >>> print TimeStamp(tid)
    2007-03-21 20:34:08.500000

    """


def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite(
            'fsiterator.txt',
            setUp=setUp, tearDown=tearDown),
        doctest.DocTestSuite(
            setUp=setUp, tearDown=tearDown),
        ))

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

