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

        storage = config.base.open()

        if replicate_to is not None:
            if replicate_from is not None:
                if replicate_to.address == replicate_from.address:
                    raise ValueError(
                        "Values for replicate-to and "
                        "replicate-from must be different.")

            storage = zc.zrs.primary.Primary(storage, replicate_to.address)

        elif replicate_from is None:
            raise ValueError(
                "You must specify replicate-to and/or replicate-from.")

        if replicate_from is not None:
            storage = zc.zrs.secondary.Secondary(
                storage, replicate_from.address,
                check_checksums=self.config.check_checksums,
                keep_alive_delay=self.config.keep_alive_delay,
                )

        return storage

class Primary(ZRS):

    def open(self):
        config = self.config

        address = config.replicate_to
        if not address:
            address = config.address
            if not address:
                raise ValueError(
                    "The required replicate-to option wasn't specified.")
        elif config.address:
            raise ValueError("Can't specify both replicate-to and address.")

        base = config.base
        if base is not None:
            if config.filestorage_path:
                raise ValueError(
                    "Can't specify filestorage-path if a storage section"
                    " is used.")
            base = base.open()
        elif config.filestorage_path:
            import ZODB.FileStorage
            base = ZODB.FileStorage.FileStorage(config.filestorage_path)
        else:
            raise ValueError(
                "You must specify a base storage or a filestorage-path.")

        return zc.zrs.primary.Primary(base, address.address)

class Secondary(Primary):

    def open(self):
        config = self.config

        address = config.replicate_from
        if not address:
            address = config.address
            if not address:
                raise ValueError(
                    "The required replicate-to option wasn't specified.")
        elif config.address:
            raise ValueError("Can't specify both replicate-to and address.")

        base = config.base
        if base is not None:
            if config.filestorage_path:
                raise ValueError(
                    "Can't specify filestorage-path if a storage section"
                    " is used.")
            base = base.open()
        elif config.filestorage_path:
            import ZODB.FileStorage
            base = ZODB.FileStorage.FileStorage(config.filestorage_path)
        else:
            raise ValueError(
                "You must specify a base storage or a filestorage-path.")
            
        if config.replicate_to:
            if isinstance(base, zc.zrs.primary.Primary):
                base.close()
                raise ValueError(
                    "Can't specify replicate-to if a primary storage section"
                    " is used.")
            base = zc.zrs.primary.Primary(base, config.replicate_to.address)

        return zc.zrs.secondary.Secondary(
            base, address.address,
            check_checksums=self.config.check_checksums,
            keep_alive_delay=self.config.keep_alive_delay,
            )
