from __future__ import print_function
##############################################################################
#
# Copyright (c) 2015 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import optparse
import json
import re
import socket
import struct
import sys
import ZODB.TimeStamp

def connect(addr):
    if isinstance(addr, tuple):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    else:
        m = re.match(r'\[(\S+)\]:(\d+)$', addr)
        if m:
            addr = m.group(1), int(m.group(2))
            s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        else:
            m = re.match(r'(\S+):(\d+)$', addr)
            if m:
                addr = m.group(1), int(m.group(2))
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            else:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    s.connect(addr)
    fp = s.makefile()
    return fp, s

def _standard_options(parser):
    parser.add_option(
        '-m', '--output-metrics', action="store_true",
        help="Output replication lag as metric.",
        )
    parser.add_option(
        '-w', '--warning', type="int", default=0,
        help="Warning lag, in seconds",
        )
    parser.add_option(
        '-e', '--error', type="int", default=0,
        help="Error lag, in seconds",
        )

def get_ts(addr, name):
    try:
        fp, s = connect(addr)
    except socket.error as err:
        print("Can't connect to %s at %r: %s" % (name, addr, err))
        sys.exit(2)
    fp = s.makefile()
    fp.write('\x00\x00\x00\x04ruok')
    fp.flush()
    proto = fp.read(struct.unpack(">I", fp.read(4))[0])
    datas = fp.read(struct.unpack(">I", fp.read(4))[0])
    fp.close()
    s.close()
    return dict(
        (sid,
         ZODB.TimeStamp.TimeStamp(
             sdata['last-transaction'].decode('hex')
             ).timeTime())
        for (sid, sdata) in json.loads(datas).items()
        )

def check(paddr, saddr, warn, error, output_metrics):
    try:
        secondary = get_ts(saddr, 'secondary')
        primary   = get_ts(paddr, 'primary')
    except SystemExit as e:
        return e.code
    output = []
    metrics = []
    level = 0
    if not (primary or secondary):
        return print("No storages") or 1
    for sid, ts in sorted(primary.items()):
        shown_sid = "" if sid == '1' else " (%r)" % sid
        sts = secondary.get(sid)
        if sts is None:
            output.append("Storage %r in primary, but not secondary" % sid)
            level = 2
            continue
        delta = ts - sts
        if output_metrics:
            metrics.append(
                "'lag%s'=%.4fseconds" % ('' if sid=='1' else sid, delta))
        if delta < 0:
            output.append(
                "Primary%s behind secondary by %s seconds" % (shown_sid, delta)
                )
            level = 2
        else:
            if delta > 0:
                output.append(
                    "Secondary%s behind primary by %.4f seconds" %
                    (shown_sid, delta))
                if error and delta > error:
                    output[-1] += ' > %s' % error
                    level = 2
                elif warn and delta > warn:
                    output[-1] += ' > %s' % warn
                    level = max(level, 1)
            else:
                output.append("Secondary%s up to date." % shown_sid)
    for sid in sorted(secondary):
        if sid not in primary:
            output.append("Storage %r in secondary, but not primary" % sid)
            level = 2

    if metrics:
        output[0] += '|' + metrics.pop(0)
    if metrics:
        if len(output) == 1:
            output.append('')
        output[-1] += '| ' + '\n '.join(metrics)
    print('\n'.join(output))
    return level or None

def basic(args=None):
    """zrs-nagios [options] PRIMARY_ADDRESS SECONDARY_ADDRESS
    """
    if args is None:
        args = sys.argv[1:]

    parser = optparse.OptionParser(__doc__)
    _standard_options(parser)
    (options, args) = parser.parse_args(args)
    if len(args) != 2 or not (options.output_metrics or
                              options.warning or options.error):
        return print('Usage: ' + basic.__doc__) or 2

    paddr, saddr = args
    return check(
        paddr, saddr, options.warning, options.error, options.output_metrics)

def zk(args=None):
    """zkzrs-nagios [options] ZOOKEEPER PATH
    """
    if args is None:
        args = sys.argv[1:]

    parser = optparse.OptionParser(__doc__)
    _standard_options(parser)
    parser.add_option(
        '-p', '--primary',
        help="ZooKeeper path of primary server",
        )
    parser.add_option(
        '-M', '--zc-monitor-address',
        help=
        "Address of a zc.monitor server to use to determine which"
        " secondary to check",
        )

    (options, args) = parser.parse_args(args)
    if len(args) != 2 or not (options.output_metrics or
                              options.warning or options.error):
        return print('Usage: ' + zk.__doc__) or 2
    zookeeper, path = args

    from zc.zkzeo.nagios import find_server
    paddr = find_server(
        zookeeper,
        options.primary if options.primary else path + '/providers',
        None)
    saddr = find_server(
        zookeeper,
        path if options.primary else path + '/secondary/providers',
        options.zc_monitor_address)
    if paddr is None or saddr is None:
        return 2
    return check(
        paddr, saddr, options.warning, options.error, options.output_metrics)
