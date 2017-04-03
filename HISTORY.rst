Release History
===============

2.5.3 2015-02-16
----------------

- Fixed: monitor got the primary last transaction time before before
  getting the secondary last transaction time, sometimes leading to
  spurious reports of the primary being behind the secondary.

2.5.2 2015-02-07
----------------

Fixed: the nagios monitor only worked if the primary and secondary ran
       in the same process (as in they did in the tests.)

2.5.1 2015-01-28
----------------

Include ``src/**/*.rst`` files in sdist.

2.5.0 2015-01-25
----------------

Added nagios plugins for monitoring replication.

2.4.4 2013-08-17
----------------

Fixed packaging bug: component.xml was left out.

2.4.3 2013-08-15
----------------

Packaging update: allow installation without setuptools.

2.4.2 2013-05-27
----------------

Initial open-source release for ZODB 3.9 or later.

2.4.1 2012-07-10
----------------

Fixed: When listening for replication on all IPV4 addresses, the
       registered IP address was 0.0.0.0, rather than the
       fully-qualified host name.

2.4.0 2012-07-10
----------------

Added support for registering and looking up replication addresses
with ZooKeeper.

2.3.4 2010-10-01
----------------

- Added support for ZODB 3.10.

- Added support for layered storages, e.g., zc.zlibstorage.


2.3.3 (2010-09-29)
==================

Added proxying for a new ZODB test method.

2.3.2 (2010-09-03)
==================

Added proxying for checkCurrentSerialInTransaction, wich was
introduced in ZODB 3.10.0b5.


2.3.1 (2010-05-20)
==================

- Fixed some spurious test failures.

2.3.0 (2010-03-01)
==================

- Updated tests to work with Python 2.6 and Twisted 9.

2.2.5 2010-09-21
----------------

- Fixed a bug in support for shared ZEO blob directories.

2.2.4 2009-09-02
----------------

- Updated tests to reflect ZODB changes.

2.2.3 2009-06-26
----------------

- Updated to work with older releases that didn’t send checksum data.

2.2.2 2009-06-04
----------------

- Support for ZODB 3.9 and blobs

- Updated the configuration syntax.

- Added a replication checksum feature

- Added support for unix-domain sockets for replication.

2.0.4 2009-03-09
----------------

- Updated the configuration syntax.

2.0.3 2008-12-19
----------------

- Updated the configuration syntax.

- Added keep-alive option to deal with VPNs that break TCP connections
  in non-standard ways.

2.0.2 2007-07-16
----------------

Initial ZRS 2 release.
