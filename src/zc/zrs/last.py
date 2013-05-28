##############################################################################
#
# Copyright (c) 2013 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import logging
import sys
import time
import ZEO.ClientStorage
import ZODB.TimeStamp

logging.getLogger().addHandler(logging.StreamHandler())
logging.getLogger().setLevel(logging.WARNING)

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    for a in args:
        a = a.split(':')
        a = a[0], int(a[1])
        print a,
        sys.stdout.flush()
        cs = ZEO.ClientStorage.ClientStorage(a, read_only=True)
        print time.ctime(
            ZODB.TimeStamp.TimeStamp(cs._server.lastTransaction()).timeTime()
            ), '(local)'

