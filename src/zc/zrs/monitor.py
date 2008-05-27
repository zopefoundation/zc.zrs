##############################################################################
#
# Copyright (c) 2005 Zope Corporation. All Rights Reserved.
#
# This software is subject to the provisions of the Zope Visible Source
# License, Version 1.0 (ZVSL).  A copy of the ZVSL should accompany this
# distribution.
#
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Monitor ZRS servers
"""

import ConfigParser
import ZEO.ClientStorage, ZEO.Exceptions
import ZODB.TimeStamp
import logging
import logging.config
import sys
import threading
import time

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

    clusters = [
        Cluster(cluster_name, data[cluster_name], frequency)
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

    def __init__(self, name, address_string, service):
        address = tuple(address_string.split(':'))
        address = address[0], int(address[1])
        self.address = address
        self.name = name
        self.service = name + '-' + service
        self.storage = ZEO.ClientStorage.ClientStorage(address, **zeo_options)

    def _report(self, log, comment):
        return log("%s %s %s", self.address[0], self.service, comment)

    def ok(self, comment):
        self._report(monitor_logger.info, comment)

    def warning(self, comment):
        self._report(monitor_logger.warning, comment)

    def critical(self, comment):
        self._report(monitor_logger.critical, comment)

class Cluster(Base):

    def __init__(self, name, options, frequency):
        super(Cluster, self).__init__(name, options['primary'], 'primary')
        self.frequency = frequency

        self.secondaries = [
            Secondary(name, address_string)
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
                    self.ok("Committed: %s" % time.ctime(ts_seconds))

                for secondary in secondaries:
                    secondary.check(ts_seconds, now)

        except:
            logger.exception("Error in cluster %s", self.name)
            monitor_logger.error("Error in cluster %s", self.name)
            raise
            
class Secondary(Base):

    def __init__(self, name, address_string):
        super(Secondary, self).__init__(name, address_string, 'secondary')
        self.last_connected = 'Never connected'
        self.last_seconds = 0

    def check(self, primary_seconds, primary_start):
        try:
            ts = self.storage._server.lastTransaction()
        except ZEO.Exceptions.ClientDisconnected:
            self.critical("Disconnected since: %s" % self.last_connected)
        else:
            self.last_connected = time.ctime(time.time())
            ts_seconds = ZODB.TimeStamp.TimeStamp(ts).timeTime()
            if (ts_seconds - primary_seconds) > (time.time()-primary_start):
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

                meth("Secondary out of date: s=%s p=%s" %
                     (time.ctime(ts_seconds), time.ctime(primary_seconds))
                     )
            else:
                self.ok("Committed: %s" % time.ctime(ts_seconds))
            self.last_seconds = ts_seconds
