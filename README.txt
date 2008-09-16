************************
Zope Replication Service
************************

Zope replication service (ZRS) provides database replication for ZODB.
For each database, a primary storage and one or more
secondary storages can be defined.  The secondary storages will automatically
replicate data from the primary storage.

Changes
*******

2.0.3
=====

New Features
------------

- Now delay primary-server shutdown to give secondaries time to
  recieve recent transactions.

- Added a monitoring script to support monitoring that ZRS servers are
  up and replicating.

- Added a utility script to print the last committed transaction for
  multiple storage servers.

- Use the publically releases Twisted package.

2.0.2 (2007-7-13)
=================

Bugs Fixed
----------

In primary servers, resources for closed secondary connections were
leaked.  In a situation where a secondary connected and disconnected
many times, this could cause primaries to runout of resources, like
open files.

2.0.2 (2007-7-16)
=================

Bugs fixed
----------

- In ZRS primaries, resources were leaked for each secondary
  connection. In situations where there were many secondary
  connections, this could cause primaries to fail due to resource
  exhaustion.

2.0.1 (2007-6-15)
=================

Added additional logging.

2.0.0 (2007-6-1)
================

First stable release.

2.0b5 2.0b4 (2007-4-27)
=======================

Updated to work with recent ZODB API changes.

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

