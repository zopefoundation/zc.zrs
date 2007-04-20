************************
Zope Replication Service
************************

Zope replication service (ZRS) provides database replication for ZODB.
For each database, a primary storage and one or more
secondary storages can be defined.  The secondary storages will automatically
replicate data from the primary storage.

Changes
*******

2.0b3 2.0b4 (2007-4-18)
=======================

Updated the network protocol to more efficiently handle large data pickles.
Improved secondary handling of very large data records.

2.0b2 (2007-4-18)
=================

Added compatibility with older ZODB releases.
(Tested with ZODB 3.17.)

2.0b1 (2007-4-17)
=================

Initial release.

