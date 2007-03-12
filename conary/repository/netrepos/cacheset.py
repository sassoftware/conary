#
# Copyright (c) 2005-2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import os
import tempfile
import cPickle

from conary import dbstore
from conary.lib import util
from conary.local import schema, sqldb, versiontable
from conary.dbstore import idtable, sqlerrors

CACHE_SCHEMA_VERSION = 18

class NullCacheSet:
    def __init__(self, tmpDir):
        self.tmpDir = tmpDir

    def getEntry(self, item, recurse, withFiles, withFileContents,
                 excludeAutoSource, csVersion):
        return None

    def addEntry(self, item, recurse, withFiles, withFileContents,
                 excludeAutoSource, returnVal, size, csVersion):
        (fd, path) = tempfile.mkstemp(dir = self.tmpDir,
                                      suffix = '.ccs-out')
        os.close(fd)
        return None, path

    def invalidateEntry(self, repos, name, version, flavor):
        pass

    def invalidateEntries(self, repos, troveList):
        pass

def retry(fn):
    """Decorator to retry database operations if the database is locked"""
    def wrap(*args, **kwargs):
        # First arg is self
        count = args[0].deadlockRetry
        while count > 0:
            count -= 1
            try:
                return fn(*args, **kwargs)
            except sqlerrors.DatabaseLocked:
                # Roll back, try again
                args[0].db.rollback()
        else:
            # Re-raise the last error
            raise
    return wrap

class CacheSet:
    filePattern = "%s/cache-%s.ccs-out"

    def __init__(self, cacheDB, tmpDir, deadlockRetry=5):
        self.deadlockRetry = deadlockRetry
	self.tmpDir = tmpDir
        self.db = dbstore.connect(cacheDB[1], driver = cacheDB[0])
        self.db.loadSchema()

        cu = self.db.cursor()
        if "CacheContents" in self.db.tables:
            self.__cleanDatabase(cu)
        # previous one might have dropped it...
        if "CacheContents" not in self.db.tables:
            cu.execute("""
            CREATE TABLE CacheContents(
               row              %(PRIMARYKEY)s,
               troveName        VARCHAR(254),
               oldFlavorId      INTEGER,
               oldVersionId     INTEGER,
               newFlavorId      INTEGER,
               newVersionId     INTEGER,
               absolute         BOOLEAN,
               recurse          BOOLEAN,
               withFiles        BOOLEAN,
               withFileContents BOOLEAN,
               excludeAutoSource BOOLEAN,
               returnValue      BINARY,
               size             INTEGER,
               csVersion        INTEGER
            ) %(TABLEOPTS)s""" % self.db.keywords)
            cu.execute("CREATE INDEX CacheContentsIdx "
                       "ON CacheContents(troveName)")
        idtable.createIdTable(self.db, "Versions", "versionId", "version")
        self.versions = versiontable.VersionTable(self.db)
        schema.createFlavors(self.db)
        self.flavors = sqldb.Flavors(self.db)
        self.db.commit()
        self.db.loadSchema()

    def getEntry(self, item, recurse, withFiles, withFileContents,
                 excludeAutoSource, csVersion):
        (name, (oldVersion, oldFlavor), (newVersion, newFlavor), absolute) = \
            item

        oldVersionId = 0
        oldFlavorId = 0
        newFlavorId = 0

        if oldVersion:
            oldVersionId = self.versions.get(oldVersion, None)
            if oldVersionId is None:
                return None

        if oldFlavor is not None:
            oldFlavorId = self.flavors.get(oldFlavor, None)
            if oldFlavorId is None:
                return None

        if newFlavor is not None:
            newFlavorId = self.flavors.get(newFlavor, None)
            if newFlavorId is None:
                return None

        newVersionId = self.versions.get(newVersion, None)
        if newVersionId is None:
            return None

        cu = self.db.cursor()
        cu.execute("""
            SELECT row, returnValue, size FROM CacheContents WHERE
                troveName=? AND
                oldFlavorId=? AND oldVersionId=? AND
                newFlavorId=? AND newVersionId=? AND
                absolute=? AND recurse=? AND withFiles=?
                AND withFileContents=? AND excludeAutoSource=?
                AND csVersion=?
            """, (name, oldFlavorId, oldVersionId, newFlavorId,
                  newVersionId, absolute, recurse, withFiles, withFileContents,
                  excludeAutoSource, csVersion))

        # since we begin and commit a transaction inside the loop
        # over the returned rows, we must use fetchall() here so that we
        # release our read lock.
        for (row, returnVal, size) in cu.fetchall():
            path = self.filePattern % (self.tmpDir, row)
            # if we have no size or we can't access the file, it's
            # bad entry.  delete it.
            if not size or not os.access(path, os.R_OK):
                cu.execute("DELETE FROM CacheContents WHERE row=?", row)
                self.db.commit()
                continue
            return (path, cPickle.loads(returnVal), size)

        return None

    @retry
    def addEntry(self, item, recurse, withFiles, withFileContents,
                 excludeAutoSource, returnVal, size, csVersion):
        (name, (oldVersion, oldFlavor), (newVersion, newFlavor), absolute) = \
            item

        oldVersionId = 0
        oldFlavorId = 0
        newFlavorId = 0

        # start a transaction now to avoid race conditions when getting
        # or adding IDs for versions and flavors
        cu = self.db.transaction()

        try:
            if oldVersion:
                oldVersionId = self.versions.get(oldVersion, None)
                if oldVersionId is None:
                    oldVersionId = self.versions.addId(oldVersion)

            if oldFlavor is not None:
                oldFlavorId = self.flavors.get(oldFlavor, None)
                if oldFlavorId is None:
                    oldFlavorId = self.flavors.addId(oldFlavor)

            if newFlavor is not None:
                newFlavorId = self.flavors.get(newFlavor, None)
                if newFlavorId is None:
                    newFlavorId = self.flavors.addId(newFlavor)

            newVersionId = self.versions.get(newVersion, None)
            if newVersionId is None:
                newVersionId = self.versions.addId(newVersion)

            cu.execute("""
            INSERT INTO CacheContents
            (row, troveName, size,
            oldFlavorId, oldVersionId, newFlavorId, newVersionId,
            absolute, recurse, withFiles, withFileContents,
            excludeAutoSource, csVersion,
            returnValue)
            VALUES (NULL,?,?,   ?,?,?,?,   ?,?,?,?,   ?,?,   ?)""",
                       (name, size,
                       oldFlavorId, oldVersionId, newFlavorId, newVersionId,
                       absolute, recurse, withFiles, withFileContents,
                       excludeAutoSource, csVersion,
                       cPickle.dumps(returnVal, protocol = -1)))

            row = cu.lastrowid
            path = self.filePattern % (self.tmpDir, row)

            self.db.commit()
        except:
            # something went wrong.  make sure that we roll
            # back any pending change
            self.db.rollback()
            raise

        return (row, path)

    def __invalidateRows(self, invList):
        # start a transaction to retain a consistent state
        cu = self.db.transaction()
        # for speed reasons we use these local functions to avoid
        # repeated freeze/thaws. These functions also cache negative
        # responses to minimize database roundtrips
        _flavCache = {}
        def _getFlavor(cu, frozenFlavor):
            ret = _flavCache.get(frozenFlavor, False)
            if ret is not False:
                return ret
            if frozenFlavor == '':
                # short circuit: the empty flavor is always flavorId 0
                # cu.execute("select flavorId from Flavors where flavor is NULL")
                ret = [[0]]
            else:
                cu.execute("select flavorId from Flavors where flavor=?",
                           frozenFlavor)
                ret = cu.fetchall()

            if not ret:
                ret = None
            else:
                ret = ret[0][0]
            _flavCache[frozenFlavor] = ret
            return ret
        _verCache = {}
        def _getVersion(cu, frozenVersion):
            ret = _verCache.get(frozenVersion, False)
            if ret is not False:
                return ret
            cu.execute("select versionId from Versions where version=?",
                       frozenVersion)
            ret = cu.fetchall()
            if not ret:
                ret = None
            else:
                ret = ret[0][0]
            _verCache[frozenVersion] = ret
            return ret
        for name, version, flavor in invList:
            flavorId = _getFlavor(cu, flavor)
            versionId = _getVersion(cu, version)
            if flavorId is None or versionId is None:
                # this should not happen, but we'll handle it anyway
                continue
            cu.execute("""
            delete from CacheContents
            WHERE troveName=? AND newFlavorId=? AND newVersionId=?
            """, (name, flavorId, versionId))
            # unlinks are tempting here, but it's possible that
            # some outstanding request still references these files.
            # gafton suggested hard linking the files for consumption to
            # allow this remove
        self.db.commit()
        return

    @retry
    def invalidateEntry(self, repos, name, version, flavor):
        """
        invalidates (and deletes) any cached changeset that matches
        the given name, version, flavor.
        """
        invList = [ (name, version.asString(), flavor.freeze()) ]
        if repos is not None:
            invList.extend(repos.getParentTroves(invList))
        return self.__invalidateRows(invList)

    @retry
    def invalidateEntries(self, repos, troveList):
        """
        invalidates (and deletes) any cached changeset that match
        the given list of (name, version, flavor) tuples
        """
        invSet = set(troveList)
        if repos is not None:
            ret = repos.getParentTroves(invSet)
            invSet.update(set(ret))
        return self.__invalidateRows(invSet)

    @retry
    def __cleanDatabase(self, cu = None):
        global CACHE_SCHEMA_VERSION
        if self.db.version != CACHE_SCHEMA_VERSION:
            if cu is None:
                cu = self.db.cursor()
            for t in self.db.tables:
                cu.execute("DROP TABLE %s" % (t,))
            self.db.setVersion(CACHE_SCHEMA_VERSION)
            self.db.loadSchema()

