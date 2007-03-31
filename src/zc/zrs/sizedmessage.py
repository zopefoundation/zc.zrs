##############################################################################
#
# Copyright (c) 2005 Zope Corporation. All Rights Reserved.
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

import struct

def marshal(message):
    return struct.pack(">I", len(message)) + message



class LimitExceeded(Exception):
    """Too much data
    """

class Stream:

    def __init__(self, callback, limit = 1 << 32):
        self._callback = callback
        self.limit = limit
        self.data = ''
        self.length = 0

    def __call__(self, data):
        self.data += data
        while 1:
            if self.length:
                if len(self.data) < self.length:
                    return

                result = self.data[:self.length]
                self.data = self.data[self.length:]
                self.length = 0
                self._callback(result)

            if len(self.data) < 4:
                return

            self.length, = struct.unpack(">I", self.data[:4])
            self.data = self.data[4:]
            if self.length > self.limit:
                raise LimitExceeded(self.limit, self.length)
            
