import logging
import sys
import time
import ZEO.ClientStorage
import ZODB.TimeStamp

logging.getLogger().addHandler(logging.StreamHandler())
logging.getLogger().setLevel(logging.WARNING)

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    for a in args:
        a = a.split(':')
        a = a[0], int(a[1])
        print a,
        sys.stdout.flush()
        cs = ZEO.ClientStorage.ClientStorage(a, read_only=True)
        print time.ctime(
            ZODB.TimeStamp.TimeStamp(cs._server.lastTransaction()).timeTime()
            ), '(local)'

