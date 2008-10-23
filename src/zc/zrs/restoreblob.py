# Backward compatibility support for ZODB 3.8, which doesn't provide a
# restoreBlob method on BlobStorage.

import os
import ZODB.blob
import zope.proxy

@zope.proxy.non_overridable
def restoreBlob(self, oid, serial, data, blobfilename, prev_txn,
                           transaction):
    """Write blob data already committed in a separate database
    """
    self.restore(oid, serial, data, '', prev_txn, transaction)

    self._lock_acquire()
    try:
        targetpath = self.fshelper.getPathForOID(oid)
        if not os.path.exists(targetpath):
            os.makedirs(targetpath, 0700)

        targetname = self.fshelper.getBlobFilename(oid, serial)
        ZODB.blob.rename_or_copy_blob(blobfilename, targetname)

        # if oid already in there, something is really hosed.
        # The underlying storage should have complained anyway
        self.dirty_oids.append((oid, serial))
    finally:
        self._lock_release()

    return self._tid

if getattr(ZODB.blob.BlobStorage, 'restoreBlob', None) is None:
    ZODB.blob.BlobStorage.restoreBlob = restoreBlob
