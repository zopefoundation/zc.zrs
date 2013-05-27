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
"""ZooKeeper support
"""
import zc.zrs.primary
import zc.zrs.secondary

class Primary(zc.zrs.primary.Primary):

    def __init__(self, storage, addr, connection_string, path):
        self.__zk = zc.zk.ZooKeeper(connection_string)
        self.__path = path
        super(Primary, self).__init__(storage, addr)

    def cfr_listen(self):
        super(Primary, self).cfr_listen()
        try:
            addr = self._listener.getHost()
            self.__zk.register_server(self.__path, (self._addr[0], addr.port))
        except:
            import traceback; traceback.print_exc()
            raise

    def close(self):
        self.__zk.close()
        super(Primary, self).close()

def first_addr(addrs):
    host, port = iter(addrs).next().split(':')
    return host, int(port)

class Secondary(zc.zrs.secondary.Secondary):

    def __init__(self, storage, connection_string, path, *args, **kw):
        self.__zk = zc.zk.ZooKeeper(connection_string)
        self.__addrs = self.__zk.children(path)
        super(Secondary, self).__init__(
            storage, first_addr(self.__addrs) if self.__addrs else None,
            *args, **kw)

        @self.__addrs
        def update(addrs):
            if addrs:
                self.setReplicationAddress(first_addr(addrs))

    def close(self):
        self.__zk.close()
        super(Secondary, self).close()
