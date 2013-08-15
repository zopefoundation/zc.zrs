=======================
ZODB Replicated Storage
=======================

ZODB replicated storage (ZRS) provides database replication for
ZODB. For each database, a primary storage and one or more secondary
storages may be defined. The secondary storages will automatically
replicate data from the primary storage.

Replication is superior to back-ups because as long as secondaries are
running, secondary data is kept updated. In the event of a failure of
a primary storage, just reconfigure a secondary to be the primary, and
it can begin handling application requests.

.. contents::

Features
========

- Primary/secondary replication

- Support for read-only secondary storages

- Service registration and discovery with ZooKeeper

Requirements
============

- ZODB 3.9 or later.

Installation
=============

ZRS is installed like any other Python package, by installing the
``zc.zrs`` package with `easy_install
<http://peak.telecommunity.com/DevCenter/EasyInstall>`_, `zc.buildout
<http://pypi.python.org/pypi/zc.buildout>`_, `pip
<http://pypi.python.org/pypi/pip>`_, etc.

Using ZRS
=========

ZRS provides two ZODB storage implementations: a primary storage and a
secondary storage. Both storages are used with file storages.

- The primary storage is a readable and writable storage.

- The secondary storage is read-only.

  A secondary storage can be used by read-only application clients to
  reduce server load.

  Secondary storages replicate from primary storages, or other
  secondary storages.

Theoretically, ZRS storages can be used wherever a ZODB storage would
be used.  In practice, however, they're used in ZEO servers.

Configuration with ZConfig
--------------------------

If using an application, like a ZEO server or Zope, that uses `ZConfig
<http://pypi.python.org/pypi/ZConfig>`_ to configure ZODB storages,
the configuration of a ZRS primary or secondary storage may be
included in the configuration file as with any other storage e.g, to
configure a primary storage, use something like::

  %import zc.zrs

  <zrs>
   replicate-to 5000

   <filestorage>
       path /path/to/data/file
   </filestorage>
  </zrs>

Here is a line-by-line walk through::

  %import zc.zrs

The import statement is necessary to load the ZConfig schema
definitions for ZRS.

::

  <zrs>

The zrs section defines a ZRS storage. A ZRS storage may be a primary
storage or a secondary storage.  A ZRS storage without a
``replicate-from`` option (as in the example above) is a primary
storage.

::

  replicate-to 5000

The replicate-to option specifies the replication address. Secondary
storages will connect to this address to download replication
data. This address can be a port number or a host name (interface
name) and port number separated by a colon.

::

  <filestorage>
    path /path/to/data/file
  </filestorage>

A ZRS storage section must include a filestorage section specifying a
file storage to contain the data.

Configuring a secondary storage is similar to configuring a primary
storage::

  %import zc.zrs

  <zrs>
   replicate-from primary-host:5000
   replicate-to 5000
   keep-alive-delay 60

   <filestorage>
       path /path/to/secondary/data/file
   </filestorage>
  </zrs>

For a secondary storage, a ``replicate-from`` option is used to specify
the address to replicate data from.

Because primary and secondary storages are generally on separate
machines, the host is usually specified in a ``replicate-from``
option.

A secondary storage can also specify a ``replicate-to`` option.  If this
option is used, other secondary storages can then replicate from the
secondary, rather than replicating from the primary.

Secondary storages also support the following optional option:

keep-alive-delay SECONDS
  In some network configurations, TCP connections are broken after
  extended periods of inactivity.  This may even be done in a way that
  a client doesn't detect the disconnection.  To prevent this, you can
  use the ``keep-alive-delay`` option to cause the secondary storage
  to send periodic no-operation messages to the server.

ZooKeeper
=========

Primaries and secondaries can register with ZooKeeper, so Secondaries
can find primaries to replicate from without needing to configure a
specific address.  See ``zk.test`` and ``zkconfig.test`` in the source
directory for more details.

Code and contributions
======================

https://bitbucket.org/zc/zc.zrs

Changes
=======

2.4.3 2013-08-15
----------------

Packaging update: allow installation without setuptools.

2.4.2 2013-05-27
----------------

Initial open-source release for ZODB 3.9 or later.

2.0.5 2013-05-27
----------------

Initial open-source release for ZODB 3.8.

Initial open-source release. Requires ZODB 3.9 or later.
