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

import struct

def marshal(message):
    return struct.pack(">I", len(message)) + message

def marshals(message):
    return struct.pack(">I", len(message)), message

class LimitExceeded(Exception):
    """Too much data
    """

class Stream:

    def __init__(self, callback, limit = 1 << 32):
        self._callback = callback
        self.limit = limit
        self.data = []
        self.size = 0
        self.length = 0

    def __call__(self, data):
        self.data.append(data)
        self.size += len(data)
        while 1:
            if self.length:
                if self.size < self.length:
                    return

                data = ''.join(self.data)
                result = data[:self.length]
                data = data[self.length:]
                self.data = [data]
                self.size = len(data)
                self.length = 0
                self._callback(result)

            if self.size < 4:
                return

            if len(self.data[0]) < 4:
                self.data = [''.join(self.data)]

            self.length, = struct.unpack(">I", self.data[0][:4])
            self.data[0] = self.data[0][4:]
            self.size -= 4
            if self.length == 0:
                self._callback('')
            elif self.length > self.limit:
                raise LimitExceeded(self.limit, long(self.length))

