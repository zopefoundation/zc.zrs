##############################################################################
#
# Copyright (c) 2013 Zope Corporation and Contributors.
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
"""Monitor ZRS servers
"""

import ConfigParser
import datetime
import logging
import logging.config
import sys
import threading
import time
import ZEO.ClientStorage, ZEO.Exceptions
import ZODB.TimeStamp
import ZODB.utils

event = threading.Event()
stop = event.set

def main(args=None, testing=False):
    if args is None:
        args = sys.argv[1:]

    [config_path] = args

    logging.config.fileConfig(config_path)
    global logger, monitor_logger
    logger = logging.getLogger(__name__)
    monitor_logger = logging.getLogger('monitor')    
    parser = ConfigParser.RawConfigParser()
    parser.read(config_path)
    data = dict(
        [(section, dict(parser.items(section)))
         for section in parser.sections()]
        )
    options = data["monitor"]
    frequency = float(options.get('frequency', 5))
    message = options.get(
        'message',
        '%(hostname)s %(service)s %(comment)s')

    clusters = [
        Cluster(cluster_name, data[cluster_name], frequency, message)
        for cluster_name in options['clusters'].split()
        ]

    if not testing:
        # This is a round-about way to run forever
        event.wait()
    else:
        return clusters

zeo_options = dict(wait= False, read_only=True,
                   min_disconnect_poll=1, max_disconnect_poll=30,
                   )

class Base(object):

    def __init__(self, name, address_string, service, message):
        address = tuple(address_string.split(':'))
        address = address[0], int(address[1])
        self.address = address
        self.name = name
        self.message = message
        self.service = name + '-' + service
        self.storage = ZEO.ClientStorage.ClientStorage(address, **zeo_options)

    def _report(self, log, severity, comment):
        return log(
            self.message % dict(
                hostname = self.address[0],
                port = self.address[1],
                name = self.name,
                service = self.service,
                severity = severity,
                comment = comment,
                utc = datetime.datetime.utcnow().strftime("%m-%d-%Y %H:%M:%S"),
                ))

    def ok(self, comment):
        self._report(monitor_logger.info, 'INFO', comment)

    def warning(self, comment):
        self._report(monitor_logger.warning, 'WARNING', comment)

    def critical(self, comment):
        self._report(monitor_logger.critical, 'CRITICAL', comment)

class Cluster(Base):

    def __init__(self, name, options, frequency, message):
        super(Cluster, self).__init__(name, options['primary'], 'primary',
                                      message)
        self.frequency = frequency

        self.secondaries = [
            Secondary(name, address_string, message)
            for address_string in options['secondaries'].split()
            ]
        thread = self.thread = threading.Thread(target=self.run, name=name)
        thread.setDaemon(True)
        thread.start()

    def run(self):
        try:
            delta_seconds = self.frequency * 60
            storage = self.storage
            secondaries = self.secondaries
            last_connected = 'Never connected'

            next = time.time()
            next -= next % delta_seconds

            ts_seconds = 0
            
            while not event.isSet():
                now = time.time()
                next += delta_seconds
                wait = next - now
                while wait < 0.0:
                    next += delta_seconds
                    wait = next - now

                time.sleep(wait)
                if event.isSet():
                    break

                try:
                    ts = storage._server.lastTransaction()
                except ZEO.Exceptions.ClientDisconnected:
                    self.critical("Disconnected since: %s" % last_connected)
                else:
                    last_connected = time.ctime(time.time())
                    ts_seconds = ZODB.TimeStamp.TimeStamp(ts).timeTime()
                    if ts == ZODB.utils.z64:
                        self.ok("No transactions")
                    else:
                        self.ok("Committed: %s" % time.ctime(ts_seconds))

                for secondary in secondaries:
                    secondary.check(ts_seconds, now)

        except:
            logger.exception("Error in cluster %s", self.name)
            monitor_logger.error("Error in cluster %s", self.name)
            raise
            
class Secondary(Base):

    def __init__(self, name, address_string, message):
        super(Secondary, self).__init__(name, address_string, 'secondary',
                                        message)
        self.last_connected = 'Never connected'
        self.last_seconds = -2208988800.0

    def check(self, primary_seconds, primary_start):
        try:
            ts = self.storage._server.lastTransaction()
        except ZEO.Exceptions.ClientDisconnected:
            self.critical("Disconnected since: %s" % self.last_connected)
        else:
            self.last_connected = time.ctime(time.time())
            ts_seconds = ZODB.TimeStamp.TimeStamp(ts).timeTime()
            if (ts_seconds - primary_seconds) > (time.time()-primary_start):
                if primary_seconds < 0:
                    self.critical(
                        "Secondary has data, %s, but primary doesn't."
                        % time.ctime(ts_seconds)
                        )
                else:
                    self.critical(
                        "Secondary is ahead of primary s=%s p=%s"
                        % (time.ctime(ts_seconds), time.ctime(primary_seconds))
                        )
            elif ((primary_seconds - ts_seconds) > 60
                  or
                  ((primary_seconds > ts_seconds)
                   and 
                   (time.time() - primary_seconds) > 60
                   )
                ):
                # We're out of date.  Maybe we're making progress.
                if ts_seconds > self.last_seconds:
                    meth = self.warning
                else:
                    meth = self.critical

                if ts == ZODB.utils.z64:
                    meth("Secondary has no data, but primary does: %s" %
                         (time.ctime(primary_seconds))
                         )
                else:
                    meth("Secondary out of date: s=%s p=%s" %
                         (time.ctime(ts_seconds), time.ctime(primary_seconds))
                         )
            else:
                if ts == ZODB.utils.z64:
                    self.ok("No data")
                else:
                    self.ok("Committed: %s" % time.ctime(ts_seconds))

            self.last_seconds = ts_seconds
