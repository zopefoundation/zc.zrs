===================
ZRS Nagios monitors
===================

The ZRS nagios monitor replication status only. The ZEO (or zkzeo)
nagios monitor should be used to monitor ZEO status and metrics for
ZRS servers.

There are 2 monitors, a basic monitor, zrs-nagios, and and a monitor
that uses ZooKeeper to find server addresses, zkzrs-nagios.

Both monitors accept the following options:

-w/--warn seconds
  Warn if the lag time between the primary and secondary is more than
  the given b=number of seconds (and less than the number of seconds
  given for the -e/--error option.

-e/--error seconds
  Error if the lag time between the primary and secondary is more than
  the given b=number of seconds.

  The monitor also errors if the lag time is negative.

-m/--metrics
  Output the lag time in Nagios metric format.

At least one of the above options must be provided.

Basic monitor
=============

The basic monitor, ``zrs-nagios`` takes the primary and secondary ZRS
addresses as positional arguments.

.. test

    Load the monitor:

    >>> import pkg_resources
    >>> nagios = pkg_resources.load_entry_point(
    ...     'zc.zrs', 'console_scripts', 'zrs-nagios')

    Start some servers:

    >>> import ZEO
    >>> addr_old,     stop_old     = ZEO.server('old.fs')
    >>> addr_current, stop_current = ZEO.server('current.fs')
    >>> addrs = ["%s:%s" % addr_current, "%s:%s" % addr_old]

    Note that old is about 60 seconds behind current.

    Run monitor w no arguments or no options errors and outputs usage:

    >>> nagios([])
    Usage: zrs-nagios [options] PRIMARY_ADDRESS SECONDARY_ADDRESS
    <BLANKLINE>
    2
    >>> nagios(addrs)
    Usage: zrs-nagios [options] PRIMARY_ADDRESS SECONDARY_ADDRESS
    <BLANKLINE>
    2

    Just metrics:

    >>> nagios('-m'  .split()+addrs)
    Secondary behind primary by 68.5678 seconds|'lag'=68.5678seconds

    Just warning:

    >>> nagios('-w99'  .split()+addrs)
    Secondary behind primary by 68.5678 seconds
    >>> nagios('-w30'  .split()+addrs)
    Secondary behind primary by 68.5678 seconds > 30
    1

    Just error:

    >>> nagios('-e99'  .split()+addrs)
    Secondary behind primary by 68.5678 seconds
    >>> nagios('-e30'  .split()+addrs)
    Secondary behind primary by 68.5678 seconds > 30
    2

    All:

    >>> nagios('-w99 -e999 -m'  .split()+addrs)
    Secondary behind primary by 68.5678 seconds|'lag'=68.5678seconds
    >>> nagios('-w33 -e99 -m'  .split()+addrs)
    Secondary behind primary by 68.5678 seconds > 33|'lag'=68.5678seconds
    1
    >>> nagios('-w33 -e44 -m'  .split()+addrs)
    Secondary behind primary by 68.5678 seconds > 44|'lag'=68.5678seconds
    2

    Can't connect

    >>> stop_old()
    >>> nagios('-w33 -e44 -m'  .split()+addrs)
    Can't connect to secondary at 'localhost:25441': [Errno 61] Connection refused
    2

    >>> stop_current()
    >>> nagios('-w33 -e44 -m'  .split()+addrs)
    Can't connect to primary at 'localhost:28234': [Errno 61] Connection refused
    2

    Multiple storages (sigh):

    >>> addr_old, stop_old = ZEO.server(
    ...     storage_conf = """
    ... <filestorage first>
    ...    path old.fs
    ... </filestorage>
    ... <mappingstorage second>
    ... </mappingstorage>
    ... <mappingstorage 1>
    ... </mappingstorage>
    ... <mappingstorage thursday>
    ... </mappingstorage>
    ... """)
    >>> addr_current, stop_current = ZEO.server(
    ...     storage_conf = """
    ... <filestorage first>
    ...    path current.fs
    ... </filestorage>
    ... <mappingstorage second>
    ... </mappingstorage>
    ... <mappingstorage 1>
    ... </mappingstorage>
    ... <mappingstorage friday>
    ... </mappingstorage>
    ... """)
    >>> addrs = ["%s:%s" % addr_current, "%s:%s" % addr_old]
    >>> nagios('-w33 -e99 -m'  .split()+addrs)
    Secondary up to date.|'lag'=0.0000seconds
    Secondary (u'first') behind primary by 68.5678 seconds > 33
    Storage u'friday' in primary, but not secondary
    Secondary (u'second') up to date.
    Storage u'thursday' in secondary, but not primary| 'lagfirst'=68.5678seconds
     'lagsecond'=0.0000seconds
    2
    >>> nagios('-w33 -e44'  .split()+addrs)
    Secondary up to date.
    Secondary (u'first') behind primary by 68.5678 seconds > 44
    Storage u'friday' in primary, but not secondary
    Secondary (u'second') up to date.
    Storage u'thursday' in secondary, but not primary
    2

    >>> stop_old(); stop_current()

ZooKeeper-based monitor
=======================

The ZooKeeoer monitor, zkzrs-nagios, takes a ZooKeeer connection
string and a path (see the -p/--primary option).  It accepts the
following options:

-p/--primary path
   The ZooKeeper path of the primary server.

   If specified, then the path given as a positional parameter is the
   ZooKeeper path of the secondary server.

   If this option isn't used, then the positional argument is a base
   path. In this case, the primary server path is the base path
   concatinated with ``/providers`` and the secondary server path is
   the base path concatinated with ``/secondary/provides``.

-M/--zc-monitor address
   The address of a monitor server to find what server is running on
   the secondary machine.

   If you have more than one secondary server, you need this option to
   help the monitor figure out which one to use.  You need to have set
   up a zc.monitor server in your `zkzeo
   <https://pypi.python.org/pypi/zc.zkzeo>`_ on your secondary server
   and use here the address you specified in your zkzeo configuration.

.. tests

    >>> import zc.zk, zc.zkzeo.runzeo
    >>> zk = zc.zk.ZK('zookeeper.example.com:2181')
    >>> zk.import_tree('''
    ... /databases
    ...   /demo
    ...     /providers
    ...     /secondary
    ...       /providers
    ... ''')
    >>> stop_current = zc.zkzeo.runzeo.test("""
    ...   <zeo>
    ...      address 127.0.0.1
    ...   </zeo>
    ...
    ...   <zookeeper>
    ...      connection zookeeper.example.com:2181
    ...      path /databases/demo/providers
    ...   </zookeeper>
    ...
    ...   <filestorage>
    ...      path current.fs
    ...   </filestorage>
    ... """)
    >>> import zc.zk.monitor

    We need to clear the primary server info in memory, bacause normally, the
    secondary doesn't run in the same process as the primary. :)

    >>> del zc.zk.monitor._servers[:]

    >>> stop_old = zc.zkzeo.runzeo.test("""
    ...   <zeo>
    ...      address 127.0.0.1
    ...   </zeo>
    ...
    ...   <zookeeper>
    ...      connection zookeeper.example.com:2181
    ...      path /databases/demo/secondary/providers
    ...      monitor-server ./old.sock
    ...   </zookeeper>
    ...
    ...   <filestorage>
    ...      path old.fs
    ...   </filestorage>
    ... """)

    >>> nagios = pkg_resources.load_entry_point(
    ...     'zc.zrs', 'console_scripts', 'zkzrs-nagios')
    >>> nagios('-m zookeeper.example.com:2181 /databases/demo'.split())
    Secondary behind primary by 68.5678 seconds|'lag'=68.5678seconds
    >>> nagios('-w33 -m -p /databases/demo/providers '
    ...        'zookeeper.example.com:2181 /databases/demo/secondary/providers'
    ...        .split())
    Secondary behind primary by 68.5678 seconds > 33|'lag'=68.5678seconds
    1

    If there are multiple secondaries, we'll get an error.

    >>> _ = zk.create("/databases/demo/secondary/providers/foo:1234")
    >>> nagios('-m zookeeper.example.com:2181 /databases/demo'.split())
    Couldn't find server in ZooKeeper
    2

    We need to provide a monitor adress:

    >>> nagios('-m -e1 zookeeper.example.com:2181 -M./old.sock /databases/demo'
    ...        .split())
    Secondary behind primary by 68.5678 seconds > 1|'lag'=68.5678seconds
    2

    >>> stop_old().exception
    >>> stop_current().exception
    >>> import zc.monitor
    >>> zc.monitor.last_listener.close()

    >>> zk.close()
