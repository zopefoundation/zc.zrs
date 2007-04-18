****************************
Zope Replication Service ZRS
****************************

Zope replication service provides database replication for ZODB.  For
each database, you can define a primary storage and one or more
secondary stprages.  The secondary storages will automatically
replicate data from the primary storage. 

Installation
************

ZRS is distributes as a Python egg, which is just a zip file
containing Python modules.  ZRS provides two ZODB storage
implementations. a primary storage and a secondary storage.  Both
storages are used with file (wrap) storages.  The primary storage is a
readable and writable storage. The socondary storage is read-only.

Running ZRS in Zope Enterprise Objects storage servers
======================================================

ZRS storages are normally installed in Zope Enterprise Objects (ZEO)
storage servers, although they can be used directly with ZODB
applications.  Note that due to a bug in ZEO, a ZEO storage server
with a secondary storage can't be used as a read-only database.  This
bug will be fixed soon.  Despite this bug, it is still useful to use
ZEO servers as a means of running secondary storages in long-running
processes.

To use with ZEO, first install ZEO as usual.  If installing ZEO by
hand, modify the ZEO starup script, runzeo, to add the ZRS egg to
``sys.path``.  In your ``zeo.conf`` file, define a primary storage
using a primary section::

   %import zc.zrs
   <primary 1>
      address 5000

      <filestorage>
          path /path/to/data/file
      </filestorage>
   </primary>

The import statement is necessary to load the definition of the ZRS
configuration sections, primary and secondary.

The primary storage has an address option for specifying a replication
address.  This is the address that secondary servers will use to
connect to the primary. The address can be a port or a hostname and a
port separated by a colon, to specify a specific interface to lsiten
on.  The primary storage definition also takes a file-storage
definition.  (ZRS primaries only work with file storages.)  In the
file storage definition, specify the path to the file-storage data
file.

To use a secondary storage, use a secondary section rather than a
primary section::

   %import zc.zrs
   <secondary 1>
      address primary-host:5000

      <filestorage>
          path /path/to/data/file
      </filestorage>
   </secondary>

The secondary section takes an address option that specifies the
primary replication address.  Because secondaries and primaries are
usually on different machines, this address will normally be of the
form host:port, but it can be just a port of the primary is on the
same machine.  In the example above, a file storage was used with the
secondary, but any writable storage can be used.

Installing ZEO and ZRS with zc.buildout
=======================================

An easier way to install ZEO is with zc.buildout.  We'll walk through
steps to create a ZRS primary installation using zc.buildout.

1. Create a directory for your installation.

2. Download a copy of the zc.buildout bootstrap script to the
   directory from:

     http://svn.zope.org/*checkout*/zc.buildout/trunk/bootstrap/bootstrap.py

   For example, if you have the wget command::

     wget -O bootstrap.py \
       http://svn.zope.org/*checkout*/zc.buildout/trunk/bootstrap/bootstrap.py

3. Run the boostrap script using Python 2.4::

     python2.4 bootstrap.py

   This will create several directories. It will also create a
   buildout configuration file, buildout.cfg.  

4. Update buildout.cfg to install the ZEO and ZRS software and to
   define a storage server::

     [buildout]
     parts = zeo primary

     [zeo]
     recipe = zc.recipe.egg:scripts
     eggs = ZODB3
            zc.zrs
     find-links = zc.zrs-2.0b2-py2.4.egg

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
             path primary.fs
          </filestorage>
       </primary>

   Buildout configuration files consist of sections and options.
   Sections are defined with names in square braces.  All buildout
   configuration files have a buildout section defining a list of
   parts.  In this example we've defined 2 parts, zeo and primary.

   Each part has a recipe that names the software that will install
   the part.  The zeo part uses the zc.recipe.egg:scripts recipe,
   which installs one or more Python scripts.  The eggs option lists
   the software to be used to define the scripts.  Here we've listed
   the ZODB3 package and the zc.zrs package.  The modules provided by
   these packages will be useable in the scripts.  The ZODB package
   defines a number of scripts, including the runzeo script that runs
   the ZEO server.  This and other ZODB scripts will be installed in
   the buildout bin directory. 

   The find-links option provides buildout with locations to find
   distributions. In thsi case, we point it directly to the zrs eggs.

   The primary part defines a ZEO server that hosts the ZRS primary
   storage.  We use the zc.zodbrecipes:server recipe to specify the
   software that will set up the server.  We also specify a zeo.conf
   file as a multi-line option.  We specify that the ZEO server should
   listedn on port 8100 and that the primary storage should listen on
   port 8101.  The primary storage will store it's data in the file
   primary.fs in the buildout directory.

5. Run the buildout by running the bin/buildout script::

     bin/buildout

   This will download serveral software packages and install the
   scripts and server configuration. The server configuration will be
   in the parts/primary directory.  A server control script will be
   installed in the bin directory.

   To start the primary server, use the bin/primary script::

     bin/primary start

   To stop it, use:

     bin/primary stop

For more information on buildout, see the buildout tutorial at:

  http://us.pycon.org/common/talkdata/PyCon2007/116/buildout.pdf

or the buildout documentation at:

  http://www.python.org/pypi/zc.buildout/1.0.0b23

The zc.zodbrecipes:server privides a number of additional options for
controlling a server installation, especially for production
deployment on Unix systems.  For more information, see:

  http://www.python.org/pypi/zc.zodbrecipes

Using ZRS storages from Python
==============================

ZRS storages can be set from Python.  To do this, simply call the
storage contructors, zc.zrs.primary.Primary or
zc.zrs.secondary.Secondary.  Let's look at an example::

  import sys
  sys.path.append(path_to_zrs_egg)

  import zc.zrs.primary
  import ZODB.FileStorage
  from ZODB.DB import DB

  filestorage = ZODB.FileStorage.FileStorage(path_to_file_storage)
  address = ('', 8101) # listen on port 8101 on all addresses (on Unix)
  primary = zc.zrs.primary.Primary(filestorage, address)
  database = DB(primary)

The zc.zrs.primary.Primary contructor takes a file storage object and
a replication address, given as a host name and port pair.  The
zc.zrs.secondary.Secondary takes the same arguments.


Changes
*******

2.0b3 (2007-4-18)
=================

Updated the network protocol to more efficiently handle large adta pickles.

2.0b2 (2007-4-18)
=================

Added compatibility with older ZODB releases.
(Tested with ZODB 3.17.)

2.0b1 (2007-4-17)
=================

Initial release.









== 2.0 ==

The focus of 2.0 will be to allow us to replace our current ZRS
installations in the cluster with a spreadless version of ZRS that
doesn't crush a server during recover.
This release:

 * Will allow replication to secondaries that can be quickly
   reconfigured as primaries.

 * Will scan files from end when a transaction presented by a
   connecting secondary is closer to the last transaction than the
   first.

 * Won't support read-only secondaries

 * Won't support BLOBs

 * Won't provide automatic failover.

 * Won't require a specialized server.  A ZRS primary will simply be
   installed as an ordinary storage in a ZEO server.

 * Won't leak memory

== ZODB Improvements ==

This isn't a ZRS release.  In the spririt of reducing existing pain, I
will also work on improving:

- Server restarts

  Now when a server restarts, clients are likely to have to validate
  their caches.  This can be disastrous if there are a lot of clients
  with large caches.

- Persistent caches

  Persistent caches should allow us to avoid long warming periods that
  we experience now. Work is needed to make them more robust and to do
  a better job of detecting when they become damaged.

- Packing

  Packing is extremely disk intensive.  In large part, this was done
  to reduce memory usage.  I'm going to look at using more memory to
  significantly reduce the amount of disk activity.

- Compression

  Implement (or at least investigate) client-side compression of data
  records.  This should allow us to cut down on I/O.  Normally,
  compression and decompression will be done solely on clients. The
  exception to this is packing, which will require decompressing
  records.

- Python 2.5 compatibility? (proposed by Gary)

  Last I (Gary) checked, not only did Zope not work with Python 2.5,
  but ZODB tests also failed.  I believe that others can do much of
  the Zope work (for instance, Classifieds has a dependency on, and
  I have experience with the RestrictedPython code) but I'd prefer if
  Jim did the necessary ZODB changes.  I hope that the advantages of
  Jim doing this work are obvious; at the very least, I posit that
  the documentation is not sufficient ATM for others to do a job that
  ZC would want to trust.  This will get Zope on the path to getting
  back with Python releases, which I feel is an (eventual)
  requirement.  Prioritization is a reasonable question, but we are
  6 months since 2.5 final as of this writing.

My thought was that we would make these improvememts before pursuing
ZRS 2.1 and later.  The most urgent issue is addressing the
server-restart issue.

== 2.1 ==

Add Blob support

== 2.2 ==

Add read-only secondaries

== 2.3 ==

Add automatic failover using a quorum-based primary-election
algorithm.

== Notes ==

=== Cold restart problem ===

Here are some other ideas and notes on the cold restart problem.

* I think the best short-term mitigation is to make persistent caches work

* Share caches accross instances, maybe using something like memcached.

* Try to anticipate needed objects. I can't think of a good way to do this.

* Try to anticipate needed objects based on past access: this is effectively what a persistent cache does.

* Use multiple threads while an application is warming up.
