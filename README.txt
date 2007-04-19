************************
Zope Replication Service
************************

Zope replication service (ZRS) provides database replication for ZODB.
For each database, you can define a primary storage and one or more
secondary storages.  The secondary storages will automatically
replicate data from the primary storage.

Replication is superior to back ups because, as long as secondaries
are running, secondary data are kept up to date.  In the event of a
failure of a primary storage, you can simply reconfigure a secondary
to be the primary and it can begin handling application requests.

.. contents::

Features
********

- Primary/secondary replication.

- Support for read-only secondary storages [#bug]_. 

Improvements from earlier versions of ZRS
=========================================

- ZRS no-longer uses spread. This makes installation much simpler.

- ZRS no-longer provides a dedicated server.  You can use ZRS with
  normal ZEO servers, or without ZEO.

- Secondary storages can connect to primary storages through normal
  TCP tunnels.

- Configuration is much simpler.

- Lower memory usage

- When used with ZODB 3.8, much better performance when secondary
  storages are disconnected from and reconnect to primary storages.

Limitations
===========

- As mentioned in [#bug]_, due to a bug in ZEO, read-only secondary
  storages run in ZEO servers cannot be used by ZEO clients.  We
  expect this bug to be fixed in ZODB 3.8.

- No support for ZODB BLOBs.  BLOB support will be added in ZRS 2.1.


Installation
************

The most common way to use ZRS is in a Zope Enterprise Objects (ZEO)
server.  The recommended way to set up a ZEO server is with `zc.buildout`.
Using `zc.buildout` to set up a ZRS storage in a ZEO storage server
is described in a later section of this document. Impatient readers
might want to skip to that section.

Note that secondary storages will typically be run in ZEO servers,
despite the fact that these servers can't correctly serve ZEO clients.
The reason for this is that secondary storages can only replicate
primary storages while they are used in a running process, and ZEO
servers provide convenient processes to host them.

Add the ZRS egg to the Python Path
==================================

ZRS is distributed as a Python egg, which is just a zip file
containing Python modules.  ZRS provides two ZODB storage
implementations. a primary storage and a secondary storage.  Both
storages are used with file storages.  The primary storage is a
readable and writable storage. The secondary storage is read-only.

To use these storages, you need to arrange that the ZRS egg is included in
the configured Python path.  For example, to use these storages in a
ZEO server, you'll need to arrange that the path to the egg is
included in the `sys.path` variable in the server script.

Configuration with ZConfig
==========================

If you're using an application, like a ZEO server or Zope, that uses
ZConfig to configure ZODB storages, you can include the configuration
of a ZRS primary or secondary storage in your configuration file as
you would with any other storage.  For example, to configure a primary
storage, use something like::

   %import zc.zrs

   <primary 1>
      address 5000

      <filestorage>
          path /path/to/data/file
      </filestorage>

   </primary>

let's walk through this line by line::

   %import zc.zrs

The import statement is necessary to load the ZConfig schema
definitions for ZRS.  (Of course, as mentioned earlier, the ZRS egg
must be included in the application's `sys.path`.)

::

   <primary 1>

The primary section defines the primary storage.  In ZEO, the section
must have a name, which is the name that ZEO will use to publish the
storage. In this example, we've used the name "1".

::

      address 5000

The address option specifies the address of the primary replication
server.  Secondary storages will connect to this address to download
replication data.  This address can be a port number or a host name
(interface name) and port number separated by a colon.  On Unix
systems, using just a port causes the replication server to listen on
all of the machine's interfaces. On Windows, using just a port causes
the server to only listen for local connections.

::

      <filestorage>
          path /path/to/data/file
      </filestorage>

A ZRS storage section must include a filestorage section specifying a
file storage to contain the data.  The filestorage section has a path
option specifying the file-storage file path.

Configuring a secondary storage is very similar to configuring a
primary storage::

   %import zc.zrs

   <secondary 1>
      address primary-host:5000

      <filestorage>
          path /path/to/secondary/data/file
      </filestorage>

   </secondary>

For a secondary storage, the `secondary` rather than the `primary` section
is used. While the address has the same meaning, for a secondary
storage, it is an address that the secondary will connect to, rather
than an address it will listen on.  Because primary and secondary
storages are usually on separate machines, the host is usually
specified in a secondary address option.

Configuring ZRS storages in Python
==================================

ZRS storages can be defined in Python.  To do this, simply call the
storage constructors, `zc.zrs.primary.Primary` or
`zc.zrs.secondary.Secondary`.  Let's look at an example::

  import sys
  sys.path.append(path_to_zrs_egg)

  import zc.zrs.primary
  import ZODB.FileStorage
  from ZODB.DB import DB

  filestorage = ZODB.FileStorage.FileStorage(path_to_file_storage)

  address = ('', 8101) # listen on port 8101 on all addresses (on Unix)

  primary = zc.zrs.primary.Primary(filestorage, address)

  database = DB(primary)

Let's walk through this one part at a time::

  import sys
  sys.path.append(path_to_zrs_egg)

We have to make sure that the path to the ZRS egg is in sys.path.
(This example assumes that the packages needed for ZODB are already in
directories or eggs in the path.)

::

  import zc.zrs.primary
  import ZODB.FileStorage
  from ZODB.DB import DB

Import either the primary or secondary ZRS module, along with any
needed ZODB modules or objects.

::

  filestorage = ZODB.FileStorage.FileStorage(path_to_file_storage)

A ZRS storage is defined using a file storage.

::

  address = ('', 8101) # listen on port 8101 on all addresses (on Unix)

We specify an address as a host-port tuple. An empty string can be
used.  On Unix, this causes the primary to listen on all
interfaces. On Windows, it causes the primary to listen only on the
localhost interface.

::

  primary = zc.zrs.primary.Primary(filestorage, address)

The primary storage is constructed using the file storage and the
address.

Constructing a secondary storage is very similar.  The
`zc.zrs.secondary.Secondary` constructor is used and the address is
the address to connect to rather than the address to listen on.  An
empty host name in this case indicates a local host connection.

Installing ZEO and ZRS with `zc.buildout` on Unix-like systems
================================================================

An easier way to install ZEO is with `zc.buildout`.  `zc.buildout`
is a tool for assembling systems using recipe.

For more information on `zc.buildout`, see the `zc.buildout` tutorial at:

  http://us.pycon.org/common/talkdata/PyCon2007/116/buildout.pdf

or the `zc.buildout` documentation at:

  http://www.python.org/pypi/zc.buildout/1.0.0b23

We'll walk through steps to create a ZRS primary installation using
`zc.buildout`.

1. Create a directory for your installation and change to it.

2. Download a copy of the `zc.buildout` bootstrap script from:

     http://svn.zope.org/\*checkout\*/zc.buildout/trunk/bootstrap/bootstrap.py

   For example, if you have the wget command::

     wget http://svn.zope.org/*checkout*/zc.buildout/trunk/bootstrap/bootstrap.py

3. Run the boostrap script using Python 2.4::

     python2.4 bootstrap.py

   This will create several directories. It will also create a
   buildout configuration file, `buildout.cfg`.  

4. Update `buildout.cfg` to install the ZEO and ZRS software and to
   define a storage server::

     [buildout]
     parts = zeo primary

     [zeo]
     recipe = zc.recipe.egg:scripts
     eggs = ZODB3
            zc.zrs_eval
     find-links = zc.zrs_eval-2.0-py2.4.egg

     [primary]
     recipe = zc.zodbrecipes:server
     zeo.conf = 
       <zeo>
          address 8100
       </zeo>
       %import zc.zrs
       <primary 1>
          address 8101
          <filestorage>
             path ${buildout:directory}/primary.fs
          </filestorage>
       </primary>

   Buildout configuration files consist of sections and options.
   Sections are defined with names in square braces.  Let's walk
   through the configuration file section by section::

     [buildout]
     parts = zeo primary

   All buildout configuration files have a buildout section defining a
   list of parts.  In this example we've defined 2 parts, `zeo` and
   `primary`.

   ::

     [zeo]
     recipe = zc.recipe.egg:scripts
     eggs = ZODB3
            zc.zrs_eval
     find-links = zc.zrs_eval-2.0-py2.4.egg

   Each part has a recipe that names the software that will install
   the part.  The `zeo` part uses the `zc.recipe.egg:scripts`
   recipe, which installs one or more Python scripts.  The `eggs`
   option lists the software to be used to define the scripts.  Here
   we've listed the `ZODB3` package and the `zc.zrs_eval` package.  The
   ZRS package name you use will depend on which ZRS distribution you
   have.  `zc.zrs` is used for a visible-source distribution.  For
   an evaluation distribution, use `zc.zrs_eval`, and for a binary
   distribution, use `zc.zrs_binary`.  The modules provided by these
   packages will be usable in the scripts.  The ZODB package defines
   a number of scripts, including the `runzeo` script that runs the ZEO
   server.  This and other ZODB scripts will be installed in the
   buildout bin directory.

   The `find-links` option provides the buildout with locations to find
   distributions. In this case, we point it directly at the zrs egg.

   ::

     [primary]
     recipe = zc.zodbrecipes:server
     zeo.conf = 
       <zeo>
          address 8100
       </zeo>
       %import zc.zrs
       <primary 1>
          address 8101
          <filestorage>
             path ${buildout:directory}/primary.fs
          </filestorage>
       </primary>

   The primary part defines a ZEO server that hosts the ZRS primary
   storage.  We use the `zc.zodbrecipes:server` recipe to specify
   the software that will set up the server.  We also specify the
   contents of the ZEO configuration file, `zeo.conf` file as a
   multi-line option.  We specify that the ZEO server should listen on
   port 8100 and that the primary storage should listen on port 8101.
   The primary storage will store it's data in the file `primary.fs` in
   the buildout directory.

5. Run the buildout by running the bin/buildout script::

     bin/buildout

   This will download several software packages and install the
   scripts and server configuration. The server configuration will be
   in the parts/primary directory.  A server control script will be
   installed in the bin directory.

   To start the primary server, use the bin/primary script::

     bin/primary start

   To stop it, use:

     bin/primary stop

The `zc.zodbrecipes:server` provides a number of additional options for
controlling a server installation, especially for production
deployment on Unix systems.  For more information, see:

  http://www.python.org/pypi/zc.zodbrecipes

Lets also set up a secondary.  We'll assume that this is on a
different machine.  We'll use the ame procedure as for the primary
storage, but the configuration file will be a bit different::

     [buildout]
     parts = zeo secondary

     [zeo]
     recipe = zc.recipe.egg:scripts
     eggs = ZODB3
            zc.zrs_eval
     find-links = zc.zrs_eval-2.0-py2.4.egg

     [secondary]
     recipe = zc.zodbrecipes:server
     zeo.conf = 
       <zeo>
          address 8100
       </zeo>
       %import zc.zrs
       <secondary 1>
          address primary-host:8101
          <filestorage>
             path ${buildout:directory}/secondary.fs
          </filestorage>
       </secondary>

Here we've named the server part `secondary` and we've used a
`secondary` configuration section in the `zeo.conf` section.  We've
specified the primary host name in the secondary address option, where
the host name should anme the machine where the primary storage was
installed.


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

.. [#bug] ZEO has a bug that prevents updates to secondary storages used
   in a ZEO server from being propagated to ZEO clients.  Clients
   can conect to the ZEO server and read data from the secondary
   storage, but they will not see changes as they are synchronized
   from the primary and may see inconsistent object versions.  

   We still recommend using ZEO servers as hosts for secondary
   storages.  A secondary storage can only replicate from a
   primary storage if it is used in a running program and ZEO
   servers provide such programs.

