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
"""Set's up the reactor used by zrs
"""

import atexit, logging, os, threading

import twisted.internet
import twisted.python.log

logger = logging.getLogger(__name__)

def log_twisted(data):
    message = data['message']
    message = '\n'.join([str(m) for m in message])
    if data['isError']:
        logger.error(message)
    else:
        logger.info(message)

twisted.python.log.startLoggingWithObserver(log_twisted, setStdout=False)

_shutdown_called = False
_started = False

def _run(reactor):
    global _shutdown_called
    try:
        reactor.run(False)
    except:
        logger.exception("The reactor errored")

    if not _shutdown_called:
        logger.critical("The twisted reactor quit unexpectedly")
        _shutdown_called = True
        #logging.shutdown()
        # Force the process to exit. I wish there was a less violent way.
        #os._exit(os.EX_SOFTWARE)

def reactor():
    global _started
    if not _started:
        _started = True
        from twisted.internet import reactor
        thread = threading.Thread(target=_run,
                                  args=(twisted.internet.reactor, ))
        thread.setDaemon(True)
        thread.start()
        atexit.register(shutdown)

    return twisted.internet.reactor

def _shutdown(event):
    twisted.internet.reactor.stop()
    event.set()

def shutdown():
    global _shutdown_called
    if not _started or _shutdown_called:
        return
    
    _shutdown_called = True
    event = threading.Event()
    twisted.internet.reactor.callFromThread(_shutdown, event)
    event.wait(60)

