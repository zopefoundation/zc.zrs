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

import zc.zrs.primary
import zc.zrs.secondary

class ZRS:

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

    def open(self):
        config = self.config
        replicate_to = config.replicate_to
        replicate_from = config.replicate_from
        zookeeper = config.zookeeper

        storage = config.base.open()

        if replicate_to is not None:
            if replicate_from is not None:
                if replicate_to.address == replicate_from.address:
                    raise ValueError(
                        "Values for replicate-to and "
                        "replicate-from must be different.")

            if zookeeper:
                from zc.zrs.zk import Primary
                storage = Primary(storage, ('', 0),
                                  zookeeper, replicate_to.address)
            else:
                storage = zc.zrs.primary.Primary(storage, replicate_to.address)

        elif replicate_from is None:
            raise ValueError(
                "You must specify replicate-to and/or replicate-from.")

        if replicate_from is not None:
            zookeeper = config.replicate_from_zookeeper or zookeeper
            if zookeeper:
                from zc.zrs.zk import Secondary
                storage = Secondary(
                    storage, zookeeper, replicate_from.address,
                    keep_alive_delay=self.config.keep_alive_delay,
                    )
            else:
                storage = zc.zrs.secondary.Secondary(
                    storage, replicate_from.address,
                    keep_alive_delay=self.config.keep_alive_delay,
                    )

        return storage
