===================
ZRS Nagios monitors
===================

The ZRS nagios monitor replication status only. The ZEO
nagios monitor should be used to monitor ZEO status and metrics for
ZRS servers.

The monitor accepts the following options:

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

The monitor, ``zrs-nagios`` takes the primary and secondary ZRS
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

    >>> stop_current()
    >>> nagios('-w33 -e44 -m'  .split()+addrs)
    Can't connect to primary at 'localhost:28234': [Errno 111] Connection refused
    2

    >>> stop_old()
    >>> nagios('-w33 -e44 -m'  .split()+addrs)
    Can't connect to secondary at 'localhost:25441': [Errno 111] Connection refused
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
