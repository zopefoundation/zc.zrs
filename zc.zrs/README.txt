************************
Zope Replication Service
************************

Zope replication service (ZRS) provides database replication for ZODB.
For each database, a primary storage and one or more
secondary storages can be defined.  The secondary storages will automatically
replicate data from the primary storage.

Changes
*******

2.3.1 (2010-05-20)
==================

- Fixed some spurious test failures.

2.3.0 (2010-03-01)
==================

- Updated tests to work with Python 2.6 and Twisted 9.

2.2.4 (2009-09-02)
==================

Updated tests to reflect changes to PersistentMapping pickle sizes.

2.2.3 (2009-06-26)
==================

- Fixed bug: ZRS didn't work with older versions of ZRS 2 that didn't send
  checksum data.

- Made checking checkums the default behavior. Previously, the default
  behavior was to not check checksums when configuring with ZConfig.

2.2.2
======

First public release.

