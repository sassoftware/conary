#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import itertools
import time

from conary import dbstore
from conary import deps, errors, files, streams, trove, versions
from conary.dbstore import idtable, sqlerrors
from conary.local import deptable, troveinfo, versiontable, schema
from conary.lib import api
from conary.trovetup import TroveTuple

OldDatabaseSchema = schema.OldDatabaseSchema

class Tags(idtable.CachedIdTable):
    def __init__(self, db):
        idtable.CachedIdTable.__init__(self, db, "Tags", "tagId", "tag")

class VersionCache(dict):
    def get(self, vs, ts):
        key = vs, ts
        if self.has_key(key):
            return self[key]
        ts = [ float(x) for x in ts.split(":") ]
        v = versions.VersionFromString(vs, timeStamps = ts)
        self[key] = v
        return v

class FlavorCache(dict):
    def get(self, frozen):
        if self.has_key(frozen):
            return self[frozen]
        if frozen is None:
            f = deps.deps.Flavor()
        else:
            f = deps.deps.ThawFlavor(frozen)
        self[frozen] = f
        return f

class DBTroveFiles:
    """
    pathId, versionId, path, instanceId, stream
    """

    addItemStmt = "INSERT INTO DBTroveFiles (pathId, versionId, path, " \
                                            "fileId, instanceId, isPresent, " \
                                            "stream) " \
                                            "VALUES (?, ?, ?, ?, ?, ?, ?)"

    def __init__(self, db):
        self.db = db
        schema.createDBTroveFiles(db)
        self.tags = Tags(self.db)

    def __getitem__(self, instanceId):
        cu = self.db.cursor()
        cu.execute("SELECT path, stream FROM DBTroveFiles "
                   "WHERE instanceId=? and isPresent=1", instanceId)
        for path, stream in cu:
            yield (path, stream)

    def getByInstanceId(self, instanceId, justPresent = True):
        cu = self.db.cursor()

        if justPresent:
            cu.execute("SELECT path, stream FROM DBTroveFiles "
                       "WHERE instanceId=? and isPresent=1", instanceId)
        else:
            cu.execute("SELECT path, stream FROM DBTroveFiles "
                       "WHERE instanceId=?", instanceId)

        for path, stream in cu:
            yield (path, stream)

    def delInstance(self, instanceId):
        cu = self.db.cursor()
        cu.execute("""DELETE FROM DBFileTags WHERE streamId IN
        (SELECT streamId from DBTroveFiles WHERE instanceId=?)""", instanceId)
        cu.execute("DELETE from DBTroveFiles WHERE instanceId=?", instanceId)

    def getFileByFileId(self, fileId, justPresent = True):
        cu = self.db.cursor()
        if justPresent:
            cu.execute("SELECT path, stream FROM DBTroveFiles "
                       "WHERE fileId=? AND isPresent = 1", fileId)
        else:
            cu.execute("SELECT path, stream FROM DBTroveFiles "
                       "WHERE fileId=?", fileId)
        # there could be multiple matches, but they should all be redundant
        try:
            path, stream = cu.next()
            return (path, stream)
        except StopIteration:
            raise KeyError, fileId

    def addItem(self, cu, pathId, versionId, path, fileId, instanceId,
                stream, tags, addItemSql = None):
        assert(len(pathId) == 16)

        if addItemSql is None:
            addItemSql = self.addItemStmt

        cu.execute(addItemSql, pathId, versionId, path, fileId, instanceId,
                    1, stream)

        streamId = cu.lastrowid

        for tag in tags:
            cu.execute("INSERT INTO DBFileTags(streamId, tagId) VALUES (?, ?)",
                       streamId, self.tags[tag])

    def iterPath(self, path):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM DBTroveFiles WHERE "
                   "isPresent=1 AND path=?", path)
        for instanceId in cu:
            yield instanceId[0]

    def removePath(self, instanceId, path):
        cu = self.db.cursor()
        cu.execute("UPDATE DBTroveFiles SET isPresent=0 WHERE path=? "
                   "AND instanceId=?", (path, instanceId))

    def _updatePathIdsPresent(self, instanceId, pathIdList, isPresent):
        # Max number of bound params
        chunkSize = 990
        plen = len(pathIdList)
        cu = self.db.cursor()
        i = 0
        while i < plen:
            clen = min(chunkSize, plen - i)
            bvals = [ isPresent, instanceId ] + pathIdList[i : i + clen]
            bparams = ','.join('?' * clen)
            cu.execute("UPDATE DBTroveFiles "
                       "SET isPresent=? "
                       "WHERE instanceId=? AND pathId in (%s)" % bparams,
                       bvals)
            i += clen

    def removePathIds(self, instanceId, pathIdList):
        self._updatePathIdsPresent(instanceId, pathIdList, isPresent = 0)

    def restorePathIds(self, instanceId, pathIdList):
        self._updatePathIdsPresent(instanceId, pathIdList, isPresent = 1)

    def iterFilesWithTag(self, tag):
        cu = self.db.cursor()
        cu.execute("""
            SELECT path FROM Tags
                INNER JOIN DBFileTags ON Tags.tagId = DBFileTags.tagId
                INNER JOIN DBTroveFiles ON
                    DBFileTags.streamId = DBTroveFiles.streamId
                WHERE tag=? ORDER BY DBTroveFiles.path
        """, tag)

        for path, in cu:
            yield path

class DBInstanceTable:
    """
    Generic table for assigning id's to (name, version, isnSet, use)
    tuples, along with a isPresent flag
    """
    def __init__(self, db):
        self.db = db
        schema.createInstances(db)

    def iterNames(self):
        cu = self.db.cursor()
        cu.execute("SELECT DISTINCT troveName FROM Instances "
                    "WHERE isPresent=1")
        for match in cu:
            yield match[0]

    def hasName(self, name):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances "
                   "WHERE troveName=? AND isPresent=1",
                   name)
        return cu.fetchone() != None

    def iterByName(self, name):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId, versionId, troveName, flavorId FROM "
                   "Instances WHERE troveName=? AND isPresent=1", name)
        for match in cu:
            yield match

    def addId(self, troveName, versionId, flavorId, timeStamps,
              isPresent = True, pinned = False):
        assert(min(timeStamps) > 0)
        if isPresent:
            isPresent = 1
        else:
            isPresent = 0

        cu = self.db.cursor()
        cu.execute("INSERT INTO Instances(troveName, versionId, flavorId, "
                                        " timeStamps, isPresent, pinned) "
                   "VALUES (?, ?, ?, ?, ?, ?)",
                   (troveName, versionId, flavorId,
                    ":".join([ "%.3f" % x for x in timeStamps]), isPresent,
                    pinned))
        return cu.lastrowid

    def delId(self, theId):
        assert(type(theId) is int)
        cu = self.db.cursor()
        cu.execute("DELETE FROM Instances WHERE instanceId=?", theId)

    def getId(self, theId, justPresent = True):
        cu = self.db.cursor()

        if justPresent:
            pres = "AND isPresent=1"
        else:
            pres = ""

        cu.execute("SELECT troveName, versionId, flavorId, isPresent "
                   "FROM Instances WHERE instanceId=? %s" % pres, theId)
        try:
            return cu.next()
        except StopIteration:
            raise KeyError, theId

    def isPresent(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT isPresent FROM Instances WHERE "
                   "troveName=? AND versionId=? AND flavorId=?",
                   item)

        val = cu.fetchone()
        if not val:
            return 0

        return val[0]

    def idIsPresent(self, instanceId):
        cu = self.db.cursor()
        cu.execute("SELECT isPresent FROM Instances WHERE "
                        "instanceId=?", instanceId)

        val = cu.fetchone()
        if not val:
            return 0

        return val[0]

    def setPresent(self, theId, val, pinned):
        cu = self.db.cursor()
        cu.execute("UPDATE Instances SET isPresent=?, pinned=? WHERE instanceId=%d"
                        % theId, val, pinned)

    def has_key(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
                   "troveName=? AND versionId=? AND flavorId=?",
                   item)
        return not(cu.fetchone() == None)

    def __getitem__(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
                   "troveName=? AND versionId=? AND flavorId=?",
                   item)
        try:
            return cu.next()[0]
        except StopIteration:
            raise KeyError, item

    def get(self, item, defValue, justPresent = True):
        cu = self.db.cursor()

        if justPresent:
            pres = " AND isPresent=1"
        else:
            pres = ""

        cu.execute("SELECT instanceId FROM Instances WHERE "
                        "troveName=? AND versionId=? AND "
                        "flavorId=? %s" % pres, item)
        item = cu.fetchone()
        if not item:
            return defValue
        return item[0]

    def getVersion(self, instanceId):
        cu = self.db.cursor()
        cu.execute("""SELECT version, timeStamps FROM Instances
                      INNER JOIN Versions ON
                            Instances.versionId = Versions.versionId
                      WHERE instanceId=?""", instanceId)
        try:
            (s, t) = cu.next()
            ts = [ float(x) for x in t.split(":") ]
            v = versions.VersionFromString(s, timeStamps=ts)
            return v
        except StopIteration:
            raise KeyError, instanceId

class Flavors(idtable.IdTable):

    def addId(self, flavor):
        return idtable.IdTable.addId(self, flavor.freeze())

    def __getitem__(self, flavor):
        if flavor is None:
            raise KeyError, "Can not lookup deps.Flavor(None)"
        # XXX: We really should be testing for a deps.deps.Flavor
        # instance, but the split of Flavor from DependencySet would
        # cause too much code breakage right now....
        assert(isinstance(flavor, deps.deps.DependencySet))
        if flavor.isEmpty():
            return 0
        return idtable.IdTable.__getitem__(self, flavor.freeze())

    def getId(self, flavorId):
        return deps.deps.ThawFlavor(idtable.IdTable.getId(self, flavorId))

    def get(self, flavor, defValue):
        if flavor is None:
            return 0
        # XXX: We really should be testing for a deps.deps.Flavor
        # instance, but the split of Flavor from DependencySet would
        # cause too much code breakage right now....
        assert(isinstance(flavor, deps.deps.DependencySet))
        if flavor.isEmpty():
            return 0
        return idtable.IdTable.get(self, flavor.freeze(), defValue)

    def __delitem__(self, flavor):
        # XXX: We really should be testing for a deps.deps.Flavor
        # instance, but the split of Flavor from DependencySet would
        # cause too much code breakage right now....
        assert(isinstance(flavor, deps.deps.DependencySet))
        if flavor.isEmpty():
            return
        idtable.IdTable.__delitem__(self, flavor.freeze())

    def getItemDict(self, itemSeq):
        cu = self.db.cursor()
        cu.execute("SELECT %s, %s FROM %s WHERE %s in (%s)"
                   % (self.strName, self.keyName, self.tableName, self.strName,
                      ",".join(["'%s'" % x.freeze() for x in itemSeq])))
        return dict(cu)

    def __init__(self, db):
        idtable.IdTable.__init__(self, db, "Flavors", "flavorId", "flavor")
        cu = db.cursor()
        cu.execute("SELECT FlavorID from Flavors")
        if cu.fetchone() == None:
            # reserve flavor 0 for "no flavor information"
            cu.execute("INSERT INTO Flavors (flavorId, flavor) VALUES (0, NULL)")

class DBFlavorMap(idtable.IdMapping):
    def __init__(self, db):
        idtable.IdMapping.__init__(self, db, "DBFlavorMap", "instanceId",
                                   "flavorId")


class Database:
    timeout = 30000
    def __init__(self, path, timeout = None):
        if timeout is not None:
            self.timeout = timeout
        self.db = None
        try:
            self.db = dbstore.connect(path, driver = "sqlite",
                                      timeout=self.timeout)
            self.schemaVersion = self.db.getVersion().major
        except sqlerrors.DatabaseLocked:
            raise errors.DatabaseLockedError
        self.db.dbh._BEGIN = "BEGIN"

        try:
            # dbstore? what's dbstore
            cu = self.db.cursor()
            cu.execute("BEGIN IMMEDIATE")
        except sqlerrors.ReadOnlyDatabase:
            readOnly = True
        else:
            readOnly = False
            self.db.rollback()
        if readOnly and self.schemaVersion < schema.VERSION:
            raise OldDatabaseSchema(
                "The Conary database on this system is too old.  It will be \n"
                "automatically converted as soon as you run Conary with \n"
                "write permissions for the database (which normally means \n"
                "as root). \n")
        elif self.schemaVersion > schema.VERSION:
            raise schema.NewDatabaseSchema()
        self.db.loadSchema()

        newCursor = self.schemaVersion < schema.VERSION

        schema.checkVersion(self.db)
        if newCursor:
            cu = self.db.cursor()

        if self.schemaVersion == 0:
            schema.createSchema(self.db)
        schema.setupTempDepTables(self.db, cu)
        schema.setupTempTables(self.db, cu)

        self.troveFiles = DBTroveFiles(self.db)
        self.instances = DBInstanceTable(self.db)
        self.versionTable = versiontable.VersionTable(self.db)
        self.flavors = Flavors(self.db)
        self.flavorMap = DBFlavorMap(self.db)
        self.depTables = deptable.DependencyTables(self.db)
        self.troveInfoTable = troveinfo.TroveInfoTable(self.db)

        self.needsCleanup = False
        self.addVersionCache = {}
        self.flavorsNeeded = {}

    def __del__(self):
        if self.db and not self.db.closed:
            self.db.close()
        del self.db

    def begin(self):
        """
        Force the database to begin a transaction; this locks the database
        so no one can touch it until a commit() or rollback().
        """
        return self.db.transaction()

    def rollback(self):
        self.needsCleanup = False
        self.db.rollback()

    def iterAllTroveNames(self):
        return self.instances.iterNames()

    def iterFindByName(self, name, pristine = False):
        instanceIds = [x[0] for x in self.instances.iterByName(name)]
        return self._iterTroves(instanceIds=instanceIds, pristine = pristine)

    def iterVersionByName(self, name, withFlavors):
        cu = self.db.cursor()

        if withFlavors:
            flavorCol = "flavor"
            flavorClause = """INNER JOIN Flavors ON
                            Flavors.flavorId = Instances.flavorId"""
        else:
            flavorCol = "NULL"
            flavorClause = ""

        cu.execute("""SELECT DISTINCT version, timeStamps, %s
                        FROM Instances NATURAL JOIN Versions
                        %s
                        WHERE troveName='%s' AND isPresent=1"""
                            % (flavorCol, flavorClause, name))

        flavors = {}

        for (match, timeStamps, flavorStr) in cu:
            ts = [float(x) for x in timeStamps.split(':')]
            version = versions.VersionFromString(match, timeStamps=ts)

            if withFlavors:
                f = flavors.get(flavorStr, None)
                if f is None:
                    f = deps.deps.ThawFlavor(flavorStr)
                    flavors[flavorStr] = f

                yield (version, f)
            else:
                yield (version)

    def getAllTroveFlavors(self, troveDict):
        outD = {}
        cu = self.db.cursor()
        versionCache = VersionCache()
        flavorCache = FlavorCache()
        for name, versionList in troveDict.iteritems():
            d = {}.fromkeys(versionList)
            outD[name] = d
            for key in d:
                d[key] = []
            cu.execute("""
                SELECT version, timeStamps, flavor FROM Instances
                    NATURAL JOIN Versions
                    INNER JOIN Flavors
                        ON Instances.flavorid = Flavors.flavorid
                WHERE troveName=? AND isPresent=1""", name)
            for (match, timeStamps, flavor) in cu:
                version = versionCache.get(match, timeStamps)
                if outD[name].has_key(version):
                    outD[name][version].append(flavorCache.get(flavor))
        return outD

    def iterAllTroves(self, withPins = False):
        cu = self.db.cursor()
        cu.execute("""
            SELECT troveName, version, timeStamps, flavor, pinned
                FROM Instances NATURAL JOIN Versions
                INNER JOIN Flavors
                    ON Instances.flavorid = Flavors.flavorid
            WHERE isPresent=1""")
        versionCache = VersionCache()
        flavorCache = FlavorCache()
        for (troveName, version, timeStamps, flavor, pinned) in cu:
            version = versionCache.get(version, timeStamps)
            flavor = flavorCache.get(flavor)
            nvf = TroveTuple(troveName, version, flavor)
            if withPins:
                yield nvf, (pinned != 0)
            else:
                yield nvf

    def pinTroves(self, name, version, flavor, pin = True):
        if flavor is None or flavor.isEmpty():
            flavorClause = "IS NULL"
        else:
            flavorClause = "= '%s'" % flavor.freeze()

        cu = self.db.cursor()
        cu.execute("""
            UPDATE Instances set pinned=? WHERE
                instanceId = (SELECT instanceId FROM Instances
                    JOIN Flavors ON
                        Instances.flavorId = Flavors.flavorId
                    JOIN Versions ON
                        Instances.versionID = Versions.versionId
                    WHERE
                        troveName=? AND
                        version = ? AND
                        flavor %s)
        """ % flavorClause, pin, name, version.asString())

    @api.publicApi
    def trovesArePinned(self, troveList):
        """
        Get a list of which troves in troveList are pinned

        @param troveList: a list of troves in (name, version, flavor) form
        @type troveList: list

        @note:
            This function makes database calls and may raise any exceptions
            defined in L{conary.dbstore.sqlerrors}

        @raises AssertionError:
        """
        cu = self.db.cursor()
        cu.execute("""
        CREATE TEMPORARY TABLE tlList(
            name        %(STRING)s,
            version     %(STRING)s,
            flavor      %(STRING)s
        )""" % self.db.keywords, start_transaction = False)
        def _iter(tl):
            for name, version, flavor in troveList:
                yield (name, version.asString(), flavor.freeze())
        cu.executemany("INSERT INTO tlList VALUES(?, ?, ?)", _iter(troveList),
                       start_transaction = False)
        # count the number of items we're inserting
        count = cu.execute('SELECT count(*) FROM tlList').next()[0]
        cu.execute("""
select
    pinned
from
    tlList, Instances, Versions, Flavors
where
        Instances.troveName = tlList.name
    and Versions.version = tlList.version
    and Instances.versionId = Versions.versionId
    and (    Flavors.flavor = tlList.flavor
          or Flavors.flavor is NULL and tlList.flavor = '' )
    and Instances.flavorId = Flavors.flavorId
order by
    tlList.rowId asc
""")
        # we use == 1 here to make sure that if we have NULL for
        # pinned, we convert that to a boolean
        results = [ x[0] == 1 for x in cu ]
        # make sure that we got the same number of results as our query
        assert(len(results) == count)
        cu.execute("DROP TABLE tlList", start_transaction = False)

        return results

    def hasByName(self, name):
        return self.instances.hasName(name)

    def getVersionId(self, version, cache):
        theId = cache.get(version, None)
        if theId:
            return theId

        theId = self.versionTable.get(version, None)
        if theId == None:
            theId = self.versionTable.addId(version)

        cache[version] = theId

        return theId

    def getInstanceId(self, troveName, versionId, flavorId,
                      timeStamps, isPresent = True):
        theId = self.instances.get((troveName, versionId, flavorId),
                                   None)
        if theId is None:
            theId = self.instances.addId(troveName, versionId, flavorId,
                                         timeStamps, isPresent = isPresent)

        return theId

    def _findTroveInstanceId(self, cu, name, version, flavor):
        if flavor.isEmpty():
            flavorStr = "IS NULL"
        else:
            flavorStr = "= '%s'" % flavor.freeze()

        cu.execute("""
        SELECT instanceId
        FROM Instances
        JOIN Versions USING (versionId)
        JOIN Flavors ON (Instances.flavorId = Flavors.flavorId)
        WHERE Instances.troveName = ?
        AND Versions.version = ?
        AND Flavors.flavor %s
        """ % flavorStr, name, str(version))

        rows = list(cu)

        if not len(rows):
            raise errors.TroveNotFound

        return rows[0][0]

    def addTrove(self, trove, pin = False, oldTroveSpec = None):
        cu = self.db.cursor()

        troveName = trove.getName()
        troveVersion = trove.getVersion()
        troveVersionId = self.getVersionId(troveVersion, {})
        self.addVersionCache[troveVersion] = troveVersionId

        if oldTroveSpec is not None:
            oldTroveId = self._findTroveInstanceId(cu, *oldTroveSpec)
        else:
            oldTroveId = None

        troveFlavor = trove.getFlavor()
        if not troveFlavor.isEmpty():
            self.flavorsNeeded[troveFlavor] = True

        for (name, version, flavor) in trove.iterTroveList(strongRefs=True,
                                                           weakRefs=True):
            if not flavor.isEmpty():
                self.flavorsNeeded[flavor] = True

        if self.flavorsNeeded:
            # create all of the flavor id's we'll need
            cu.execute("""
            CREATE TEMPORARY TABLE flavorsNeeded(
                empty INTEGER,
                flavor %(STRING)s
            )""" % self.db.keywords)
            for flavor in self.flavorsNeeded.keys():
                cu.execute("INSERT INTO flavorsNeeded VALUES(?, ?)",
                           None, flavor.freeze())
            cu.execute("""
            INSERT INTO Flavors (flavorId, flavor)
            SELECT flavorsNeeded.empty, flavorsNeeded.flavor
            FROM flavorsNeeded LEFT OUTER JOIN Flavors USING(flavor)
            WHERE Flavors.flavorId is NULL
            """)
            cu.execute("DROP TABLE flavorsNeeded")
            self.flavorsNeeded = {}

        # get all of the flavor id's we might need; this could be somewhat
        # more efficient for an update, but it's not clear making it
        # more efficient is actually a speedup (as we'd have to figure out
        # which flavorId's we need). it could be that all of this code
        # would get faster if we just added the files to a temporary table
        # first and insert'd into the final table???
        flavors = {}
        if not troveFlavor.isEmpty():
            flavors[troveFlavor] = True
        for (name, version, flavor) in trove.iterTroveList(strongRefs=True,
                                                           weakRefs=True):
            if not flavor.isEmpty():
                flavors[flavor] = True

        flavorMap = self.flavors.getItemDict(flavors.iterkeys())
        del flavors

        if troveFlavor.isEmpty():
            troveFlavorId = 0
        else:
            troveFlavorId = flavorMap[troveFlavor.freeze()]

        # the instance may already exist (it could be referenced by a package
        # which has already been added, or it may be in the database as
        # not present)
        troveInstanceId = self.instances.get((troveName, troveVersionId,
                                    troveFlavorId), None, justPresent = False)
        if troveInstanceId:
            self.instances.setPresent(troveInstanceId, 1, pinned=pin)
        else:
            assert(min(troveVersion.timeStamps()) > 0)
            troveInstanceId = self.instances.addId(troveName, troveVersionId,
                                       troveFlavorId, troveVersion.timeStamps(),
                                       pinned = pin)

        assert(cu.execute("SELECT COUNT(*) FROM TroveTroves WHERE "
                          "instanceId=?", troveInstanceId).next()[0] == 0)

        cu.execute("""
        CREATE TEMPORARY TABLE IncludedTroves(
            troveName   %(STRING)s,
            versionId   INTEGER,
            flavorId    INTEGER,
            timeStamps  %(STRING)s,
            flags       INTEGER
        ) """ % self.db.keywords)
        def _iter(trove):
            for (name, version, flavor), byDefault, isStrong \
                                                in trove.iterTroveListInfo():
                versionId = self.getVersionId(version, self.addVersionCache)
                if flavor.isEmpty():
                    flavorId = 0
                else:
                    flavorId = flavorMap[flavor.freeze()]

                flags = 0
                if not isStrong:
                    flags |= schema.TROVE_TROVES_WEAKREF
                if byDefault:
                    flags |= schema.TROVE_TROVES_BYDEFAULT;
                yield (name, versionId, flavorId,
                       ":".join([ "%.3f" % x for x in version.timeStamps()]),
                       flags)

        cu.executemany("INSERT INTO IncludedTroves VALUES(?, ?, ?, ?, ?)",
                       _iter(trove))

        # make sure every trove we include has an instanceid
        cu.execute("""
            INSERT INTO Instances (troveName, versionId, flavorId,
                                   timeStamps, isPresent, pinned)
                                    SELECT IncludedTroves.troveName,
                                           IncludedTroves.versionId,
                                           IncludedTroves.flavorId,
                                           IncludedTroves.timeStamps, 0, 0
                FROM IncludedTroves LEFT OUTER JOIN Instances ON
                    IncludedTroves.troveName == Instances.troveName AND
                    IncludedTroves.versionId == Instances.versionId AND
                    IncludedTroves.flavorId == Instances.flavorId
                WHERE
                    instanceId is NULL
            """)

        # now include the troves in this one
        cu.execute("""
            INSERT INTO TroveTroves(instanceId, includedId, flags, inPristine)
                SELECT ?, instanceId, flags, ?
                FROM IncludedTroves JOIN Instances ON
                    IncludedTroves.troveName == Instances.troveName AND
                    IncludedTroves.versionId == Instances.versionId AND
                    IncludedTroves.flavorId == Instances.flavorId
            """, troveInstanceId, True)

        cu.execute("DROP TABLE IncludedTroves")

        trove.troveInfo.installTime.set(time.time())
        self.depTables.add(cu, trove, troveInstanceId)
        self.troveInfoTable.addInfo(cu, trove, troveInstanceId)

        # these are collections that _could_ include trove (they have
        # an empty slot where this trove might fit)
        cu.execute('''SELECT TroveTroves.instanceId FROM Instances
                      JOIN TroveTroves
                        ON (Instances.instanceId = TroveTroves.includedId)
                      WHERE troveName = ? AND isPresent=0''', trove.getName())
        collections = cu.fetchall()

        cu.execute("select instanceId from trovetroves where includedid=?", troveInstanceId)
        collections += cu.fetchall()

        for x, in collections:
            self._sanitizeTroveCollection(cu, x, nameHint = trove.getName())

        self._sanitizeTroveCollection(cu, troveInstanceId)

        cu.execute("""CREATE TEMPORARY TABLE NewFiles (
                        pathId BLOB,
                        versionId INTEGER,
                        path %(PATHTYPE)s,
                        fileId BLOB,
                        stream BLOB,
                        isPresent INTEGER)""" % self.db.keywords)

        cu.execute("""CREATE TEMPORARY TABLE NewFileTags (
                        pathId BLOB,
                        tag %(STRING)s)""" % self.db.keywords)

        stmt = cu.compile("""
                INSERT INTO NewFiles (pathId, versionId, path, fileId,
                                      stream, isPresent)
                        VALUES (?, ?, ?, ?, ?, ?)""")

        return (cu, troveInstanceId, stmt, oldTroveId)

    def _sanitizeTroveCollection(self, cu, instanceId, nameHint = None):
        # examine the list of present, missing, and not inPristine troves
        # for a collection and make sure the set is sane
        if nameHint:
            nameClause = "Instances.troveName = '%s' AND" % nameHint
        else:
            nameClause = ""


        cu.execute("""
            SELECT includedId, troveName, version, flavor, isPresent,
                   inPristine, timeStamps
                FROM TroveTroves JOIN Instances ON
                    TroveTroves.includedId = Instances.instanceId
                JOIN Versions ON
                    Instances.versionId = Versions.versionId
                JOIN Flavors ON
                    Instances.flavorId = Flavors.flavorId
                WHERE
                    %s
                    TroveTroves.instanceId = ?
        """ % nameClause, instanceId)

        pristineTrv = trove.Trove('foo', versions.NewVersion(),
                                  deps.deps.Flavor(), None)
        currentTrv = trove.Trove('foo', versions.NewVersion(),
                                  deps.deps.Flavor(), None)
        instanceDict = {}
        origIncluded = set()
        versionCache = VersionCache()
        flavorCache = FlavorCache()
        for (includedId, name, version, flavor, isPresent,
                                            inPristine, timeStamps) in cu:
            flavor = flavorCache.get(flavor)
            version = versionCache.get(version, timeStamps)

            instanceDict[(name, version, flavor)] = includedId
            origIncluded.add((name, version, flavor))
            if isPresent:
                currentTrv.addTrove(name, version, flavor)
            if inPristine:
                pristineTrv.addTrove(name, version, flavor)

        linkByName = {}
        trvChanges = currentTrv.diff(pristineTrv)[2]
        for (name, (oldVersion, oldFlavor), (newVersion, newFlavor), isAbs) \
                                                in trvChanges:
            if oldVersion is None:
                badInstanceId = instanceDict[(name, newVersion, newFlavor)]
                # we know it isn't in the pristine; if it was, it would
                # be in both currentTrv and pristineTrv, and not show up
                # as a diff
                cu.execute("DELETE FROM TroveTroves WHERE instanceId=? AND "
                           "includedId=?", instanceId, badInstanceId)
                origIncluded.discard((name, newVersion, newFlavor))
            elif newVersion is None:
                # this thing should be linked to something else.
                linkByName.setdefault(name, set()).add(oldVersion.branch())

        if not linkByName: return

        for (name, version, flavor) in self.findByNames(linkByName):
            if version.branch() in linkByName[name]:
                currentTrv.addTrove(name, version, flavor, presentOkay = True)

        trvChanges = currentTrv.diff(pristineTrv)[2]
        for (name, (oldVersion, oldFlavor), (newVersion, newFlavor), isAbs) \
                                                in trvChanges:
            if newVersion is None: continue
            if (name, newVersion, newFlavor) in origIncluded: continue
            # don't add this linkage if it's just saying that we should
            # link to something that's already a part of this linkage.

            if (name, oldVersion, oldFlavor) not in instanceDict: continue
            oldIncludedId = instanceDict[name, oldVersion, oldFlavor]

            flags = cu.execute("""
                    SELECT flags FROM TroveTroves WHERE
                        instanceId=? and includedId=?""",
                    instanceId, oldIncludedId).next()[0]

            if newFlavor.isEmpty():
                flavorStr = "IS NULL"
            else:
                flavorStr = "= '%s'" % newFlavor.freeze()

            cu.execute("""
                INSERT INTO TroveTroves (instanceId, includedId, flags,
                                         inPristine) SELECT ?, instanceId, ?, 0
                    FROM Instances JOIN Versions ON
                        Instances.versionId = Versions.versionId
                    JOIN Flavors ON
                        Instances.flavorId = Flavors.flavorId
                    WHERE
                        troveName = ? AND
                        version = ? AND
                        flavor %s
                """ % flavorStr, instanceId, flags, name,
                        newVersion.asString())

    def addFile(self, troveInfo, pathId, path, fileId, fileVersion,
                fileStream = None, isPresent = True):
        (cu, troveInstanceId, addFileStmt, oldInstanceId) = troveInfo
        versionId = self.getVersionId(fileVersion, self.addVersionCache)

        if fileStream:
            cu.execstmt(addFileStmt, pathId, versionId, path, fileId,
                        fileStream, isPresent)

            tags = files.frozenFileTags(fileStream)

            if tags:
                cu.executemany("INSERT INTO NewFileTags VALUES (?, ?)",
                               itertools.izip(itertools.repeat(pathId), tags))
        else:
            cu.execute("""
              UPDATE DBTroveFiles
                  SET instanceId=?, isPresent=?, path=?, versionId=?
                  WHERE pathId=? AND instanceId=?""",
                    troveInstanceId, isPresent, path, versionId,
                    pathId, oldInstanceId)

    def addTroveDone(self, troveInfo):
        (cu, troveInstanceId, addFileStmt, oldInstanceId) = troveInfo

        cu.execute("""
            INSERT INTO DBTroveFiles (pathId, versionId, path, fileId,
                                      instanceId, isPresent, stream)
                        SELECT pathId, versionId, path, fileId, %d,
                               isPresent, stream FROM NewFiles"""
               % troveInstanceId)
        cu.execute("""
            INSERT INTO Tags (tag) SELECT DISTINCT
                NewFileTags.tag FROM NewFileTags
                LEFT OUTER JOIN Tags USING (tag)
                WHERE Tags.tag is NULL
        """)
        cu.execute("""
            INSERT INTO DBFileTags (streamId, tagId)
                SELECT streamId, tagId FROM
                    DBTroveFiles JOIN NewFileTags USING (pathId)
                    JOIN Tags USING (tag)
                    WHERE instanceId = ?""", troveInstanceId)

        cu.execute("DROP TABLE NewFiles")
        cu.execute("DROP TABLE NewFileTags")

        return troveInstanceId

    def markUserReplacedFiles(self, userReplaced):
        cu = self.db.cursor()
        cu.execute("""CREATE TEMPORARY TABLE UserReplaced(
                        name STRING, version STRING, flavor STRING,
                        pathId BLOB)""")
        for (name, version, flavor), fileList in userReplaced.iteritems():
            for pathId, content, fileObj in fileList:
                flavorStr = flavor.freeze()
                if not flavorStr:
                    flavorStr = None

                cu.execute("""
                    INSERT INTO UserReplaced(name, version, flavor, pathId)
                        VALUES (?, ?, ?, ?)
                """, name, version.asString(), flavorStr, pathId)

        cu.execute("""
            UPDATE DBTroveFiles SET isPresent = 0 WHERE
                rowId IN (SELECT DBTroveFiles.rowId FROM UserReplaced
                    JOIN Versions ON
                        UserReplaced.version = Versions.version
                    JOIN Flavors ON
                        UserReplaced.flavor = Flavors.flavor OR
                        (UserReplaced.flavor IS NULL AND
                         Flavors.flavor IS NULL)
                    JOIN Instances ON
                        Instances.troveName = UserReplaced.name AND
                        Instances.versionId = versions.versionId AND
                        Instances.flavorId = flavors.flavorId
                    JOIN DBTroveFiles ON
                        DBTroveFiles.instanceId = Instances.instanceId AND
                        DBTroveFiles.pathId = UserReplaced.pathId)
        """)

        cu.execute("DROP TABLE UserReplaced")

    def checkPathConflicts(self, instanceIdList, replaceCheck, sharedFiles):
        cu = self.db.cursor()
        cu2 = self.db.cursor()
        cu.execute("CREATE TEMPORARY TABLE NewInstances (instanceId integer)")
        for instanceId in instanceIdList:
            cu.execute("INSERT INTO NewInstances (instanceId) VALUES (?)",
                       instanceId)

        cu.execute("""
            SELECT AddedFiles.path,
                   ExistingInstances.instanceId, ExistingFiles.pathId,
                   ExistingFiles.stream,
                   ExistingInstances.troveName, ExistingVersions.version,
                   ExistingFlavors.flavor,
                   AddedInstances.instanceId, AddedFiles.pathId,
                   AddedFiles.stream,
                   AddedInstances.troveName,
                   AddedVersions.version, AddedFlavors.flavor

                FROM NewInstances
                JOIN DBTroveFiles AS AddedFiles USING (instanceId)
                JOIN DBTroveFiles AS ExistingFiles ON
                    AddedFiles.path = ExistingFiles.path AND
                    AddedFiles.instanceId != ExistingFiles.instanceId

                JOIN Instances AS ExistingInstances ON
                    ExistingFiles.instanceId = ExistingInstances.instanceId
                JOIN Versions AS ExistingVersions ON
                    ExistingInstances.versionId = ExistingVersions.versionId
                JOIN Flavors AS ExistingFlavors ON
                    ExistingInstances.flavorId = ExistingFlavors.flavorId

                JOIN Instances AS AddedInstances ON
                    AddedInstances.instanceId = NewInstances.instanceId
                JOIN Versions AS AddedVersions ON
                    AddedInstances.versionId = AddedVersions.versionId
                JOIN Flavors AS AddedFlavors ON
                    AddedInstances.flavorId = AddedFlavors.flavorId

                WHERE
                    AddedFiles.isPresent = 1 AND
                    ExistingFiles.isPresent = 1 AND
                    AddedFiles.fileId != ExistingFiles.fileId
        """)

        conflicts = []
        replaced = {}
        #import epdb;epdb.st()
        for (path, existingInstanceId, existingPathId, existingStream,
             existingTroveName, existingVersion, existingFlavor,
             addedInstanceId, addedPathId, addedStream, addedTroveName,
             addedVersion, addedFlavor) in cu:
            if existingPathId in sharedFiles.get(
                       (existingTroveName,
                        versions.VersionFromString(existingVersion),
                        deps.deps.ThawDependencySet(existingFlavor)), set()):
                continue

            replaceExisting = False

            addedFile = files.ThawFile(addedStream, addedPathId)
            existingFile = files.ThawFile(existingStream, existingPathId)

            if addedFile.compatibleWith(existingFile):
                continue

            if (addedFile.flags.isEncapsulatedContent() and
                existingFile.flags.isEncapsulatedContent()):
                # When we install RPMs we allow 64 bit ELF files to
                # silently replace 32 bit ELF files. This check matches
                # one in rpmcapsule.py. We don't restrict it to RPM
                # capsules here (we should, but that would involve walking
                # into troveinfo), but restricting to just capsules ought
                # to be close enough? XXX
                #
                # This logic only replaces "existing". It depends on seeing
                # the conflict twice to replace "added". Gross, again.
                cmp = files.rpmFileColorCmp(addedFile, existingFile)
                if cmp == 1:
                    # "added" is better than "existing"
                    replaceExisting = True
                elif cmp == -1:
                    # "existing" is better than "added"
                    continue

                if path.startswith('/usr/share/doc/'):
                    # gross. following a hack in a rhel4/rhel5 patch to rpm.
                    continue

            if replaceCheck(path):
                replaceExisting = True

            if replaceExisting:
                cu2.execute("UPDATE DBTroveFiles SET isPresent = 0 "
                           "WHERE instanceId = ? AND pathId = ?",
                           existingInstanceId, existingPathId)
                existingTroveInfo = (existingTroveName,
                          versions.VersionFromString(existingVersion),
                          deps.deps.ThawFlavor(existingFlavor))
                l = replaced.setdefault(existingTroveInfo, [])
                # None's here are for compatibility with the replacement
                # list the filesystem job generates. They tells the rollback
                # generation code not to look on the disk for file contents.
                l.append((existingPathId, None, None))
            else:
                conflicts.append((path,
                        (existingPathId,
                         (existingTroveName,
                          versions.VersionFromString(existingVersion),
                          deps.deps.ThawFlavor(existingFlavor))),
                        (addedPathId,
                         (addedTroveName,
                          versions.VersionFromString(addedVersion),
                          deps.deps.ThawFlavor(addedFlavor)))))

        cu.execute("DROP TABLE NewInstances")

        if conflicts:
            raise errors.DatabasePathConflicts(conflicts)

        return replaced

    def getFile(self, pathId, fileId, pristine = False):
        stream = self.troveFiles.getFileByFileId(fileId,
                                                 justPresent = not pristine)[1]
        return files.ThawFile(stream, pathId)

    def getFileStream(self, fileId, pristine = False):
        return self.troveFiles.getFileByFileId(fileId,
                                               justPresent = not pristine)[1]

    def findFileVersion(self, fileId):
        cu = self.db.cursor()
        cu.execute("""SELECT stream FROM DBTroveFiles
                          INNER JOIN Versions ON
                              DBTroveFiles.versionId == Versions.versionId
                      WHERE fileId == ?""", fileId)

        for (stream,) in cu:
            return files.ThawFile(stream, None)

        return None

    def iterFiles(self, l):
        cu = self.db.cursor()

        schema.resetTable(cu, 'getFilesTbl')
        cu.executemany('INSERT INTO getFilesTbl VALUES (?, ?)',
                       ((x[0], x[1][1]) for x in enumerate(l)),
                       start_transaction = False)

        # there may be duplicate fileId entries in getFilesTbl
        # and DBTrovefiles.  To avoid searching through potentially
        # millions of rows, we perform some more complicated sql.
        cu.execute("""
                SELECT row, (SELECT stream
                              FROM DBTroveFiles AS dbt
                              WHERE dbt.fileId = gft.fileId LIMIT 1) AS stream
                    FROM getfilesTbl AS gft
                    WHERE stream IS NOT NULL
        """)

        l2 = [ None ] * len(l)

        for (row, stream) in cu:
            fObj = files.ThawFile(stream, l[row][0])
            assert(l[row][1] == fObj.fileId())
            l2[row] = fObj

        return l2

    def hasTroves(self, troveList):
        instances = self._lookupTroves(troveList)
        result = [ False ] * len(troveList)
        for i, instanceId in enumerate(instances):
            if instanceId is not None:
                result[i] = True

        return result

    def getTroves(self, troveList, pristine = True, withFiles = True,
                  withDeps = True, withFileObjects = False):
        # returns a list parallel to troveList, with nonexistant troves
        # filled in w/ None
        instances = self._lookupTroves(troveList)
        toFind = {}

        # go through some hoops to give the same order
        # when troves are missing - _iterTroves doesn't
        # handle missing troves.
        for i, instanceId in enumerate(instances):
            if instanceId is not None:
                toFind.setdefault(instanceId, []).append(i)

        results = [ None for x in instances ]
        instances = list(self._iterTroves(pristine,
                                          instanceIds = toFind,
                                          withFiles = withFiles,
                                          withDeps = withDeps,
                                          withFileObjects = withFileObjects))
        for instanceId, instance in itertools.izip(toFind, instances):
            for slot in toFind[instanceId]:
                results[slot] = instance
        return results

    def getTroveFiles(self, troveList, onlyDirectories = False):
        instanceIds = self._lookupTroves(troveList)
        if None in instanceIds:
            raise KeyError

        trvByInstanceId = dict([ (instId, trvInfo) for
                instId, trvInfo in itertools.izip(instanceIds, troveList)
                if instId is not None ])
        instanceIds = trvByInstanceId.keys()

        cu = self.db.cursor()

        cu.execute("""CREATE TEMPORARY TABLE getTrovesTbl(
                                idx %(PRIMARYKEY)s,
                                instanceId INT)
                   """ % self.db.keywords, start_transaction = False)

        cu.executemany("INSERT INTO getTrovesTbl VALUES (?, ?)",
                       list(enumerate(instanceIds)), start_transaction=False)

        if onlyDirectories:
            dirClause = "AND stream LIKE 'd%'"
        else:
            dirClause = ""

        cu.execute("""SELECT instanceId, path, stream FROM getTrovesTbl JOIN
                        DBTroveFiles USING (instanceId)
                        WHERE isPresent = 1 %s
                        ORDER BY path""" % dirClause)

        lastId = None
        for instanceId, path, stream in cu:
            yield trvByInstanceId[instanceId], path, stream

        cu.execute("DROP TABLE getTrovesTbl", start_transaction = False)

    def _lookupTroves(self, troveList):
        # returns a list parallel to troveList, with nonexistant troves
        # filled in w/ None
        cu = self.db.cursor()

        cu.execute("""
        CREATE TEMPORARY TABLE getTrovesTbl(
            idx %(PRIMARYKEY)s,
            troveName %(STRING)s,
            version %(STRING)s,
            flavor %(STRING)s
        ) """ % self.db.keywords, start_transaction = False)

        def _iter(tl):
            for i, (name, version, flavor) in enumerate(tl):
                yield (i, name, str(version), flavor.freeze())

        cu.executemany("INSERT INTO getTrovesTbl VALUES(?, ?, ?, ?)",
                       _iter(troveList),
                       start_transaction = False)

        cu.execute("""SELECT idx, Instances.instanceId FROM getTrovesTbl
                        JOIN Instances ON
                            Versions.versionId == Instances.versionId
                        JOIN Versions ON (Instances.versionId = Versions.versionId)
                        JOIN Flavors ON Instances.flavorId = Flavors.flavorId
                        WHERE Instances.troveName = getTrovesTbl.troveName
                              AND isPresent=1
                              AND Versions.version = getTrovesTbl.version
                              AND (Flavors.flavor = getTrovesTbl.flavor or getTrovesTbl.flavor = '' and Flavors.flavorId=0)
                    """)

        r = [ None ] * len(troveList)
        for (idx, instanceId) in cu:
            r[idx] = instanceId
        cu.execute("DROP TABLE getTrovesTbl", start_transaction = False)

        return r

    def _iterTroves(self, pristine, instanceIds, withFiles = True,
                    withDeps = True, errorOnMissing=True,
                    withFileObjects = False):
        """
        Iterates over the troves associated with a list of instanceIds.

        @param pristine: Return the trove unmodified based on the local system.
        @type pristine: boolean
        @param instanceIds: Instance ids to iterate over.
        @type instanceIds: list of int
        @param withFiles: Include (pathId, path, fileId, version) information
        for the files referenced by troves.
        @type withFiles: boolean
        @param errorOnMissing: Raise an error on a missing instanceId,
        otherwise return None
        @type errorOnMissing: boolean
        @param withFileObjects: Return Trove objects w/ file objects included.
        @type withFileObjects: boolean
        """
        instanceIds = list(instanceIds)

        if withFileObjects:
            troveClass = trove.TroveWithFileObjects
        else:
            troveClass = trove.Trove

        cu = self.db.cursor()
        cu.execute("""CREATE TEMPORARY TABLE getTrovesTbl(
                                idx %(PRIMARYKEY)s,
                                instanceId INT)
                   """ % self.db.keywords, start_transaction = False)

        cu.executemany("INSERT INTO getTrovesTbl VALUES (?, ?)",
                       list(enumerate(instanceIds)), start_transaction=False)

        cu.execute("""SELECT idx, troveName, version, flavor, timeStamps
                      FROM getTrovesTbl
                      JOIN Instances USING(instanceId)
                      JOIN Versions USING(versionId)
                      JOIN Flavors ON (Instances.flavorId = Flavors.flavorId)
                  """)

        versionCache = VersionCache()
        flavorCache = FlavorCache()
        results = [ None for x in instanceIds ]

        for idx, troveName, versionStr, flavorStr, timeStamps in cu:
            troveFlavor = flavorCache.get(flavorStr)
            troveVersion = versionCache.get(versionStr, timeStamps)

            trv = troveClass(troveName, troveVersion, troveFlavor, None,
                             setVersion = False)
            results[idx] = trv

        # add all of the troves which are references from this trove; the
        # flavor cache is already complete
        cu = self.db.cursor()
        if pristine:
            pristineClause = "TroveTroves.inPristine = 1"
        else:
            pristineClause = "Instances.isPresent = 1"

        cu.execute("""
            SELECT idx, troveName, version, flags, timeStamps, flavor
                FROM getTrovesTbl
                JOIN TroveTroves USING(instanceId)
                JOIN Instances
                JOIN Versions ON
                    Versions.versionId = Instances.versionId
                JOIN Flavors ON
                    TroveTroves.includedId = Instances.instanceId AND
                    Flavors.flavorId = Instances.flavorId
                WHERE %s
                ORDER BY idx
        """ % pristineClause)

        for idx, name, versionStr, flags, timeStamps, flavorStr in cu:
            version = versionCache.get(versionStr, timeStamps)
            flavor = flavorCache.get(flavorStr)

            byDefault = (flags & schema.TROVE_TROVES_BYDEFAULT) != 0
            weakRef = (flags & schema.TROVE_TROVES_WEAKREF) != 0

            results[idx].addTrove(name, version, flavor, byDefault = byDefault,
                                  weakRef = weakRef)

        for idx, instanceId in enumerate(instanceIds):
            trv = results[idx]

            if withDeps:
                self.depTables.get(cu, trv, instanceId)
            self.troveInfoTable.getInfo(cu, trv, instanceId)
            if not withFiles:
                yield trv

        if not pristine or withFiles:
            if withFileObjects:
                streamStr = "stream"
            else:
                streamStr = "NULL"

            cu.execute("""SELECT idx, pathId, path, version, fileId, isPresent,
                          %s
                          FROM getTrovesTbl
                          JOIN DBTroveFiles USING(instanceId)
                          JOIN Versions ON
                              Versions.versionId = DBTroveFiles.versionId
                          ORDER BY idx
                          """ % streamStr)
            curIdx = 0
            for (idx, pathId, path, version, fileId, isPresent, stream) in cu:
                if not pristine and not isPresent:
                    continue
                version = versions.VersionFromString(version)
                results[idx].addFile(pathId, path, version, fileId)
                if stream:
                    results[idx].addFileObject(fileId,
                                               files.ThawFile(stream, pathId))
                while idx != curIdx:
                    yield results[curIdx]
                    curIdx += 1

            while curIdx < len(results):
                if not pristine:
                    results[idx].computePathHashes()
                if not withFiles:
                    results[idx].removeAllFiles()

                yield results[curIdx]
                curIdx += 1

        cu.execute("DROP TABLE getTrovesTbl", start_transaction = False)

    def eraseTrove(self, troveName, troveVersion, troveFlavor):
        cu = self.db.cursor()

        if not self.needsCleanup:
            self.needsCleanup = True
            cu.execute("CREATE TEMPORARY TABLE RemovedVersions "
                       "(rmvdVer %(PRIMARYKEY)s)" % self.db.keywords)

        troveVersionId = self.versionTable[troveVersion]
        if troveFlavor is None:
            troveFlavorId = 0
        else:
            troveFlavorId = self.flavors[troveFlavor]
        troveInstanceId = self.instances[(troveName, troveVersionId,
                                          troveFlavorId)]

        cu.execute("INSERT OR IGNORE INTO RemovedVersions "
                   "VALUES (?)", troveVersionId)
        cu.execute("""
                INSERT OR IGNORE INTO RemovedVersions
                    SELECT DISTINCT DBTroveFiles.versionId FROM DBTroveFiles
                        WHERE
                            DBTroveFiles.instanceId = ?""", troveInstanceId)
        cu.execute("""
                INSERT OR IGNORE INTO RemovedVersions
                    SELECT DISTINCT Instances.versionId FROM
                        TroveTroves JOIN Instances ON
                            TroveTroves.includedId = Instances.instanceId
                        WHERE
                            TroveTroves.instanceId = ?""", troveInstanceId)

        wasIn = [ x for x in cu.execute("select distinct troveTroves.instanceId from instances join trovetroves on instances.instanceid = trovetroves.includedId where troveName=? and (trovetroves.inPristine = 0 or instances.isPresent = 0)", troveName) ]

        self.troveFiles.delInstance(troveInstanceId)
        cu.execute("DELETE FROM TroveTroves WHERE instanceId=?",
                   troveInstanceId)
        cu.execute("DELETE FROM TroveTroves WHERE includedId=? AND "
                   "inPristine=0", troveInstanceId)
        self.depTables.delete(self.db.cursor(), troveInstanceId)

        # mark this trove as not present
        self.instances.setPresent(troveInstanceId, 0, False)

        for x, in wasIn:
            self._sanitizeTroveCollection(cu, x, nameHint = troveName)

    def commit(self):
        if self.needsCleanup:
            # this join could be slow; it would be much better if we could
            # restrict the select on Instances by instanceId, but that's
            # not so easy and may require multiple passes (since we may
            # now be able to remove a trove which was included by a trove
            # which was included by a trove which was removed; getting that
            # closure may have to be iterative?). that process may be faster
            # then the full join?
            # NOTE: if we could assume that we have weak references this
            # would be a two-step process
            cu = self.db.cursor()

            cu.execute("""DELETE FROM TroveInfo WHERE
                    instanceId IN (
                        SELECT Instances.instanceId FROM Instances
                        WHERE isPresent = 0)
                      """)

            cu.execute("""DELETE FROM Instances WHERE
                    instanceId IN (
                        SELECT Instances.instanceId
                        FROM
                        Instances LEFT OUTER JOIN TroveTroves
                        ON Instances.instanceId = TroveTroves.includedId
                        WHERE isPresent = 0 AND TroveTroves.includedId IS NULL)
                      """)

            cu.execute("""DELETE FROM Versions WHERE Versions.versionId IN
                            (SELECT rmvdVer FROM RemovedVersions
                                LEFT OUTER JOIN Instances ON
                                    rmvdVer == Instances.versionId
                                LEFT OUTER JOIN DBTroveFiles ON
                                    rmvdVer == DBTroveFiles.versionId
                                WHERE
                                    Instances.versionId is NULL AND
                                    DBTroveFiles.versionId is NULL)""")
            cu.execute("DROP TABLE RemovedVersions")
            self.needsCleanup = False

        self.db.commit()
        self.addVersionCache = {}
        self.flavorsNeeded = {}

    def dependencyChecker(self, troveSource, findOrdering = True,
                          ignoreDepClasses = set()):
        return deptable.DependencyChecker(self.db, troveSource,
                                          findOrdering = findOrdering,
                                          ignoreDepClasses = ignoreDepClasses)

    def pathIsOwned(self, path):
        for instanceId in self.troveFiles.iterPath(path):
            if self.instances.idIsPresent(instanceId):
                return True

        return False

    def iterFindByPath(self, path, pristine = False):
        return self._iterTroves(instanceIds=self.troveFiles.iterPath(path),
                                pristine=pristine)

    def pathsOwned(self, pathList):
        if not pathList:
            return []

        cu = self.db.cursor()
        cu.execute("""
        CREATE TEMPORARY TABLE pathList(
            path        %(STRING)s
        )""" % self.db.keywords, start_transaction = False)
        self.db.bulkload("pathList", [ (x,) for x in pathList ], [ "path" ],
                         start_transaction = False)
        cu.execute("""
            SELECT path FROM pathList JOIN DBTroveFiles USING(path) WHERE
                DBTroveFiles.isPresent = 1
        """)

        pathsFound = set( x[0] for x in cu )
        cu.execute("DROP TABLE pathList", start_transaction = False)

        return [ path in pathsFound for path in pathList ]

    def iterFindPathReferences(self, path, justPresent = False,
                               withStream = False):
        if withStream:
            stream = "stream"
        else:
            stream = "NULL"

        cu = self.db.cursor()
        cu.execute("""SELECT troveName, version, flavor, pathId, fileId,
                             DBTroveFiles.isPresent, %s
                            FROM DBTroveFiles JOIN Instances ON
                                DBTroveFiles.instanceId = Instances.instanceId
                            JOIN Versions ON
                                Instances.versionId = Versions.versionId
                            JOIN Flavors ON
                                Flavors.flavorId = Instances.flavorId
                            WHERE
                                path = ?
                    """ % stream, path)

        for (name, version, flavor, pathId, fileId, isPresent, stream) in cu:
            if not isPresent and justPresent:
                continue

            version = versions.VersionFromString(version)
            if flavor is None:
                flavor = deps.deps.Flavor()
            else:
                flavor = deps.deps.ThawFlavor(flavor)

            if stream:
                yield (name, version, flavor, pathId, fileId, stream)
            else:
                yield (name, version, flavor, pathId, fileId)

    def removeFileFromTrove(self, trove, path):
        versionId = self.versionTable[trove.getVersion()]
        flavorId = self.flavors[trove.getFlavor()]
        instanceId = self.instances[(trove.getName(), versionId, flavorId)]
        self.troveFiles.removePath(instanceId, path)

    def removePathIdsFromTrove(self, troveName, troveVersion, troveFlavor,
                               pathIdList):
        versionId = self.versionTable[troveVersion]
        flavorId = self.flavors[troveFlavor]
        instanceId = self.instances[(troveName, versionId, flavorId)]
        self.troveFiles.removePathIds(instanceId, pathIdList)

    def restorePathIdsToTrove(self, troveName, troveVersion, troveFlavor,
                              pathIdList):
        versionId = self.versionTable[troveVersion]
        flavorId = self.flavors[troveFlavor]
        instanceId = self.instances[(troveName, versionId, flavorId)]
        self.troveFiles.restorePathIds(instanceId, pathIdList)

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False,
                         pristine = False):
        if sortByPath:
            sort = " ORDER BY path"
        else:
            sort =""
        cu = self.db.cursor()

        troveVersionId = self.versionTable[version]
        if flavor.isEmpty():
            troveFlavorId = 0
        else:
            troveFlavorId = self.flavors[flavor]
        troveInstanceId = self.instances[(troveName, troveVersionId,
                                          troveFlavorId)]
        versionCache = {}

        if pristine:
            cu.execute("SELECT pathId, path, fileId, versionId, stream FROM "
                       "DBTroveFiles WHERE instanceId = ? "
                       "%s" % sort, troveInstanceId)
        else:
            cu.execute("SELECT pathId, path, fileId, versionId, stream FROM "
                       "DBTroveFiles WHERE instanceId = ? "
                       "AND isPresent=1 %s" % sort, troveInstanceId)

        versionCache = {}
        for (pathId, path, fileId, versionId, stream) in cu:
            version = versionCache.get(versionId, None)
            if not version:
                version = self.versionTable.getBareId(versionId)
                versionCache[versionId] = version

            if withFiles:
                fileObj = files.ThawFile(stream, pathId)
                yield (pathId, path, fileId, version, fileObj)
            else:
                yield (pathId, path, fileId, version)

    def mapPinnedTroves(self, mapList):
        if not mapList:
            return

        cu = self.db.cursor()
        cu.execute("""
        CREATE TEMPORARY TABLE mlt(
            name             %(STRING)s,
            pinnedVersion    %(STRING)s,
            pinnedFlavor     %(STRING)s,
            mappedVersion    %(STRING)s,
            mappedTimestamps %(STRING)s,
            mappedFlavor     %(STRING)s
        ) """ % self.db.keywords)

        def _iter(ml):
            for (name, pinnedInfo, mapInfo) in ml:
                assert(sum(mapInfo[0].timeStamps()) > 0)
                if pinnedInfo[1] is None or pinnedInfo[1].isEmpty():
                    pinnedFlavor = None
                else:
                    pinnedFlavor = pinnedInfo[1].freeze()

                if mapInfo[1] is None or mapInfo[1].isEmpty():
                    mapFlavor = None
                else:
                    mapFlavor = mapInfo[1].freeze()
                yield (name, pinnedInfo[0].asString(), pinnedFlavor,
                       mapInfo[0].asString(),
                       ":".join([ "%.3f" % x for x in mapInfo[0].timeStamps()]),
                       mapFlavor)

            cu.executemany("INSERT INTO mlt VALUES(?, ?, ?, ?, ?, ?)",
                           _iter(mapList))

        # now add link collections to these troves
        cu.execute("""INSERT INTO TroveTroves (instanceId, includedId,
                                               flags, inPristine)
                        SELECT TroveTroves.instanceId, pinnedInst.instanceId,
                               TroveTroves.flags, 0 FROM
                            mlt JOIN Flavors AS pinFlv ON
                                pinnedFlavor == pinFlv.flavor OR
                                pinnedFlavor IS NULL and pinFlv.flavor IS NULL
                            JOIN Versions AS pinVers ON
                                pinnedVersion == pinVers.version
                            JOIN Instances as pinnedInst ON
                                pinnedInst.troveName == mlt.name AND
                                pinnedInst.flavorId == pinFlv.flavorId AND
                                pinnedInst.versionId == pinVers.versionId
                            JOIN Flavors AS mapFlv ON
                                mappedFlavor == mapFlv.flavor OR
                                mappedFlavor IS NULL and mapFlv.flavor IS NULL
                            JOIN Versions AS mapVers ON
                                mappedVersion == mapVers.version
                            JOIN Instances as mapInst ON
                                mapInst.troveName == mlt.name AND
                                mapInst.flavorId == mapFlv.flavorId AND
                                mapInst.versionId == mapVers.versionId
                            JOIN TroveTroves ON
                                TroveTroves.includedId == mapInst.instanceId
                            LEFT JOIN TroveTroves AS dup ON
                                (dup.instanceId == TroveTroves.instanceId AND
                                 dup.includedId == pinnedInst.instanceId)
                            WHERE dup.instanceId IS NULL
                    """)

        cu.execute("DROP TABLE mlt")

    def getTroveContainers(self, l):
        """
        Return the troves which include the troves listed in l as strong
        references.
        """
        return self._getTroveInclusions(l, False, weakRefs = False,
                                        pristineOnly = False)

    def getTroveTroves(self, l, weakRefs = False, justPresent = False,
                           pristineOnly = True):
        """
        Return the troves which the troves in l include as strong references.
        If weakRefs is True, also include the troves included as weak
        references. If justPresent is True, only include troves present
        in the database. If pristineOnly is True, inferred references aren't
        included.
        """
        return self._getTroveInclusions(l, True, weakRefs = weakRefs,
                                        justPresent = justPresent,
                                        pristineOnly = pristineOnly)

    def _getTroveInclusions(self, l, included, weakRefs = False,
                            justPresent = False, pristineOnly = True):
        cu = self.db.cursor()
        cu.execute("""
        CREATE TEMPORARY TABLE ftc(
            idx     INTEGER PRIMARY KEY,
            name    %(STRING)s,
            version %(STRING)s,
            flavor  %(STRING)s
        ) """ % self.db.keywords, start_transaction = False)
        result = []
        def _iter(infoList, resultList):
            for idx, info in enumerate(infoList):
                resultList.append([])
                yield (idx, info[0], info[1].asString(), info[2].freeze())
        cu.executemany("INSERT INTO ftc VALUES(?, ?, ?, ?)",
                       _iter(l, result), start_transaction = False)

        if included:
            sense = ("instanceId", "includedId")
        else:
            sense = ("includedId", "instanceId")

        if justPresent:
            presentFilter = "Instances.isPresent = 1 AND"
        else:
            presentFilter = ""

        if pristineOnly:
            pristineFilter = "TroveTroves.inPristine = 1 AND"
        else:
            pristineFilter = ""

        if weakRefs:
            weakRefsFilter = 0
        else:
            weakRefsFilter = schema.TROVE_TROVES_WEAKREF

        sql = """SELECT idx, instances.troveName, Versions.version,
                             Flavors.flavor, instances.timeStamps, flags
                      FROM ftc
                      JOIN Instances AS IncInst ON
                          IncInst.troveName = ftc.name
                      JOIN Versions AS IncVersion ON
                          IncVersion.versionId = IncInst.versionId
                      JOIN Flavors AS IncFlavor ON
                          IncFlavor.flavorId = IncInst.flavorId
                      JOIN TroveTroves ON
                          TroveTroves.%s = IncInst.instanceId
                      JOIN Instances ON
                          Instances.instanceId = TroveTroves.%s
                      JOIN Versions ON
                          Versions.versionId = Instances.versionId
                      JOIN Flavors ON
                          Flavors.flavorId = Instances.flavorId
                      WHERE
                          %s
                          %s
                          IncVersion.version = ftc.version AND
                          (IncFlavor.flavor = ftc.flavor OR
                           (IncFlavor.flavor IS NULL AND ftc.flavor = "")) AND
                          (TroveTroves.flags & %d) == 0
                   """ % (sense + (presentFilter, pristineFilter,
                                   weakRefsFilter))
        cu.execute(sql)
        for (idx, name, version, flavor, ts, flags) in cu:
            ts = [ float(x) for x in ts.split(":") ]
            result[idx].append((name,
                                versions.VersionFromString(version,
                                                           timeStamps = ts),
                                deps.deps.ThawFlavor(flavor)))

        cu.execute("DROP TABLE ftc", start_transaction = False)

        return result

    def findTroveContainers(self, names):
        # XXX this fn could be factored out w/ getTroveContainers above
        cu = self.db.cursor()
        cu.execute("""
        CREATE TEMPORARY TABLE ftc(
            idx INTEGER,
            name %(STRING)s
        )""" % self.db.keywords, start_transaction = False)
        cu.executemany("INSERT INTO ftc VALUES(?, ?)", enumerate(names),
                       start_transaction = False)

        cu.execute("""SELECT idx, Instances.troveName, Versions.version,
                             Flavors.flavor
                        FROM ftc
                        JOIN Instances AS IncInst ON
                            ftc.name = IncInst.troveName
                        JOIN TroveTroves ON
                            IncInst.instanceId = TroveTroves.includedId
                        JOIN Instances ON
                            TroveTroves.instanceId = Instances.instanceId
                        JOIN Flavors ON
                            Instances.flavorId = Flavors.flavorId
                        JOIN Versions ON
                            Instances.versionId = Versions.versionId
                """)
        result = [ [] for x in names ]
        for (idx, name, version, flavor) in cu:
            result[idx].append((name, versions.VersionFromString(version),
                                deps.deps.ThawFlavor(flavor)))

        cu.execute("DROP TABLE ftc", start_transaction = False)

        return result

    def findTroveReferences(self, names):
        """ return trove tuples that a) have a name in the given list of
            names and b) are referenced by the pristine version of troves
            installed in this system.
            Note that the trove tuples returned may not be installed - they
            merely must be referenced by an installed trove.
        """
        cu = self.db.cursor()
        cu.execute("""
        CREATE TEMPORARY TABLE ftc(
            idx INTEGER,
            name %(STRING)s
        )""" % self.db.keywords, start_transaction = False)
        cu.executemany("INSERT INTO ftc VALUES(?, ?)",
                       enumerate(names), start_transaction = False)

        # the JOIN TroveTroves on includedId ensures that this trove
        # is pointed to somewhere!
        cu.execute("""SELECT idx, Instances.troveName, Versions.version,
                             Flavors.flavor
                        FROM ftc
                        JOIN Instances AS IncInst ON
                            ftc.name = IncInst.troveName
                        JOIN TroveTroves ON
                            (IncInst.instanceId = TroveTroves.includedId
                             AND TroveTroves.inPristine = 1)
                        JOIN Instances ON
                            IncInst.instanceId = Instances.instanceId
                        JOIN Flavors ON
                            IncInst.flavorId = Flavors.flavorId
                        JOIN Versions ON
                            IncInst.versionId = Versions.versionId
                """)
        result = [ [] for x in names ]
        for (idx, name, version, flavor) in cu:
            result[idx].append((name, versions.VersionFromString(version),
                                deps.deps.ThawFlavor(flavor)))
        cu.execute("DROP TABLE ftc", start_transaction = False)
        return result

    def findUnreferencedTroves(self):
        cu = self.db.cursor()
        cu.execute("""
                SELECT troveName, version, flavor FROM Instances
                    LEFT OUTER JOIN TroveTroves ON
                        Instances.instanceId = TroveTroves.includedId
                    JOIN Versions ON
                        Instances.versionId = Versions.versionId
                    JOIN Flavors ON
                        Instances.flavorId = Flavors.flavorId
                    WHERE
                        includedid IS NULL AND
                        version NOT LIKE "%/local@LOCAL:%"
        """)

        l = []
        for (name, version, flavorStr) in cu:
            if flavorStr is None:
                flavorStr = deps.deps.Flavor()
            else:
                flavorStr = deps.deps.ThawFlavor(flavorStr)

            l.append((name, versions.VersionFromString(version), flavorStr))

        return l


    def iterUpdateContainerInfo(self, troveNames=None):
        """
            Returns information about troves and their containers that should
            be enough to determine what local updates have been made to the
            system.

            If troveNames are specified, returns enough information to be
            used to determine what local updates have been made to the
            given troves.

            Yields ((name, version, flavor), parentInfo, isPresent)
            tuples, for troves on the system that may be part of a local
            update.  parentInfo may be (name, version, flavor) or None.
        """
        cu = self.db.cursor()

        if troveNames:
            # Return information needed for determining local updates
            # concerning the given troves only.  To do that, we need a
            # list of all troves that could potentially affect whether this
            # trove is an update - that all parents of these troves
            # and all troves with the same name as the parents of these troves.
            cu.execute("CREATE TEMPORARY TABLE tmpInst(instanceId INT)",
                       start_transaction = False)

            cu.executemany(
                """INSERT INTO tmpInst SELECT instanceId FROM Instances
                WHERE troveName = ?""", troveNames, start_transaction = False)
            # Summary, Insert into this tmpInst all troves that are parents
            # of the troves already in tmpIst + all troves with the same
            # name.
            cu.execute('''
                INSERT INTO tmpInst
                    SELECT DISTINCT SameName.instanceId
                        FROM tmpInst
                        JOIN TroveTroves
                           ON (TroveTroves.includedId = tmpInst.instanceId)
                        JOIN Instances
                           ON (TroveTroves.instanceId = Instances.instanceId)
                        JOIN Instances AS SameName
                           ON (Instances.troveName == SameName.troveName)
                        WHERE SameName.instanceId NOT IN
                              (SELECT instanceId from tmpInst)
                ''', start_transaction = False)
            fromClause = 'FROM tmpInst JOIN Instances USING(instanceId)'
        else:
            fromClause = 'FROM Instances'

        # Select troves where:
        # 1. The trove instanceId is listed in tmpInst
        # 2. There is another trove with the same name that is on the system -
        #    we don't list removals as local updates (maybe we should?)
        #    This trove must also not be referenced (this is why we join
        #    TroveTroves as NotReferenced)
        # 3. This trove is not both present and referenced - such troves
        #    are definitely not parts of local updates - they are intended
        #    installs.
        cu.execute("""
        SELECT Instances.isPresent, Instances.troveName, Versions.version,
               Instances.timeStamps, Flavors.flavor,
               Parent.troveName, ParentVersion.version, Parent.timeStamps,
               ParentFlavor.flavor, TroveTroves.flags
        %s
        /* There must be something with this name that is present */
        JOIN Instances AS InstPresent ON
            (InstPresent.troveName=Instances.troveName and
             InstPresent.isPresent)
        /* And that thing must not be referenced
         * or any reference must be weak
         * (we ensure later that this TroveTroves.instanceId != NULL)
         */
        LEFT JOIN TroveTroves AS NotReferenced ON
            (InstPresent.instanceId=NotReferenced.includedId
             AND NotReferenced.inPristine=1
             AND NotReferenced.flags IN (0,2))
        JOIN Versions ON
            Instances.versionId = Versions.versionId
        JOIN Flavors ON
            Instances.flavorId = Flavors.flavorId
        /* Find the actual parents for this trove */
        LEFT OUTER JOIN TroveTroves ON
            (Instances.instanceId = TroveTroves.includedId
             AND TroveTroves.inPristine=1)
        LEFT OUTER JOIN Instances AS Parent ON
            TroveTroves.instanceId=Parent.instanceId
        LEFT OUTER JOIN Versions AS ParentVersion ON
            ParentVersion.versionId=Parent.versionId
        LEFT OUTER JOIN Flavors AS ParentFlavor ON
            ParentFlavor.flavorId=Parent.flavorId
        /* Conditions:
            1. These must be a trove with the same name
               that is not referenced or is only referenced
               weakly.
            2. We only want parents that are pristine
         */
        WHERE (NotReferenced.instanceId IS NULL
              AND (TroveTroves.inPristine=1
                    OR TroveTroves.inPristine is NULL)
              )
        """ % fromClause)

        versionCache = VersionCache()
        flavorCache = FlavorCache()
        for (isPresent, name, versionStr, timeStamps, flavorStr,
             parentName, parentVersionStr, parentTimeStamps, parentFlavor,
             flags) in cu:
            if parentName:
                weakRef = flags & schema.TROVE_TROVES_WEAKREF
                v = versionCache.get(parentVersionStr, parentTimeStamps)
                f = flavorCache.get(parentFlavor)
                parentInfo = (parentName, v, f)
            else:
                weakRef = False
                parentInfo = None

            version = versionCache.get(versionStr, timeStamps)
            flavor = flavorCache.get(flavorStr)
            yield ((name, version, flavor), parentInfo, isPresent, weakRef)

        if troveNames:
            cu.execute("DROP TABLE tmpInst", start_transaction = False)

    def getAllTroveInfo(self, troveInfoTag):
        cu = self.db.cursor()
        cu.execute("""
            SELECT troveName, version, timeStamps, flavor, data FROM TroveInfo
                JOIN Instances USING (instanceId)
                JOIN Flavors USING (flavorId)
                JOIN Versions ON Instances.versionId = Versions.versionId
                WHERE infoType = ?
        """, troveInfoTag)

        versionCache = VersionCache()
        flavorCache = FlavorCache()
        return [ (
                TroveTuple(name=x[0],
                    version=versionCache.get(x[1], x[2]),
                    flavor=flavorCache.get(x[3])),
                x[4]) for x in cu ]

    def _getTroveInfo(self, troveList, troveInfoTag):
        # returns a list parallel to troveList, None for troveinfo not present
        cu = self.db.cursor()

        cu.execute("""
        CREATE TEMPORARY TABLE getTrovesTbl(
            idx       %(PRIMARYKEY)s,
            troveName %(STRING)s,
            versionId INTEGER,
            flavorId  INTEGER
        ) """ % self.db.keywords, start_transaction = False)

        # avoid walking troveList multiple times in case it's a generator
        r = [ ]
        def _iter(tl, r):
            for i, (name, version, flavor) in enumerate(tl):
                flavorId = self.flavors.get(flavor, None)
                if flavorId is None:
                    continue
                versionId = self.versionTable.get(version, None)
                if versionId is None:
                    continue
                r.append(None)
                yield (i, name, versionId, flavorId)

        cu.executemany("INSERT INTO getTrovesTbl VALUES(?, ?, ?, ?)",
                       _iter(troveList, r), start_transaction = False)

        cu.execute("""SELECT idx, TroveInfo.data FROM getTrovesTbl
                        INNER JOIN Instances ON
                            getTrovesTbl.troveName == Instances.troveName AND
                            getTrovesTbl.flavorId == Instances.flavorId AND
                            getTrovesTbl.versionId == Instances.versionId AND
                            Instances.isPresent == 1
                        INNER JOIN TroveInfo USING (instanceId)
                        WHERE TroveInfo.infoType = ?
                    """, troveInfoTag)

        for (idx, data) in cu:
            r[idx] = trove.TroveInfo.streamDict[troveInfoTag][1](data)

        cu.execute("DROP TABLE getTrovesTbl", start_transaction = False)

        return r

    def getPathHashesForTroveList(self, troveList):
        """
            Returns the pathHashes for the given trove list.
        """
        return self._getTroveInfo(troveList, trove._TROVEINFO_TAG_PATH_HASHES)

    def getCapsulesTroveList(self, troveList):
        """ 
            Returns the capsule data for the given trove list.
        """
        return self._getTroveInfo(troveList, trove._TROVEINFO_TAG_CAPSULE)

    def getTroveScripts(self, troveList):
        """
            Returns the trove scripts for the given trove list. None is
            returned for troves with no scripts. Returns a list of
            trove.TroveScripts objects.
        """
        return self._getTroveInfo(troveList, trove._TROVEINFO_TAG_SCRIPTS)

    def getTroveCompatibilityClass(self, name, version, flavor):
        if flavor is None or flavor.isEmpty():
            flavorClause = "IS NULL"
        else:
            flavorClause = "= '%s'" % flavor.freeze()

        cu = self.db.cursor()
        cu.execute("""
            SELECT data FROM Instances
                    JOIN Versions USING (versionId)
                    JOIN Flavors ON Instances.flavorId = Flavors.flavorId
                    LEFT OUTER JOIN TroveInfo ON
                        Instances.instanceId = TroveInfo.instanceId AND
                        TroveInfo.infoType = ?
                    WHERE
                        Instances.troveName = ? AND
                        Versions.version = ? AND
                        Flavors.flavor %s
        """ % flavorClause, trove._TROVEINFO_TAG_COMPAT_CLASS,
                            name, str(version))
        l = cu.fetchall()
        if not l:
            # no match for the instance
            raise KeyError
        elif l[0][0] is None:
            # instance match, but no entry in TroveInfo
            return 0

        return streams.ShortStream(l[0][0])()

    def findRemovedByName(self, name):
        """
        Returns information on erased troves with a given name.
        """

        cu = self.db.cursor()

        cu.execute("""SELECT troveName, version, flavor FROM
                            Instances JOIN Versions ON
                                Instances.versionId = Versions.versionId
                            JOIN Flavors ON
                                Instances.flavorId = Flavors.flavorId
                            WHERE
                                isPresent = 0 AND
                                troveName = (?)""", name)

        return [ (n, versions.VersionFromString(v),
                  deps.deps.ThawFlavor(f)) for (n, v, f) in cu ]

    def findByNames(self, nameList):
        cu = self.db.cursor()

        cu.execute("""SELECT troveName, version, flavor, timeStamps FROM
                            Instances JOIN Versions ON
                                Instances.versionId = Versions.versionId
                            JOIN Flavors ON
                                Instances.flavorId = Flavors.flavorId
                            WHERE
                                isPresent = 1 AND
                                troveName IN (%s)""" %
                    ",".join(["'%s'" % x for x in nameList]))

        versionCache = VersionCache()
        flavorCache = FlavorCache()
        l = []
        for (name, version, flavor, timeStamps) in cu:
            version = versionCache.get(version, timeStamps)
            flavor = flavorCache.get(flavor)
            l.append((name, version, flavor))

        return l

    def troveIsIncomplete(self, name, version, flavor):
        cu = self.db.cursor()

        if isinstance(flavor, deps.deps.Flavor) and not flavor.isEmpty():
            flavorStr = 'flavor = ?'
            flavorArgs = [flavor.freeze()]
        else:
            flavorStr = 'flavor IS NULL'
            flavorArgs = []

        cu.execute("""
                SELECT data FROM Instances
                    JOIN Versions USING (versionId)
                    JOIN Flavors ON
                        Instances.flavorId = Flavors.flavorId
                    JOIN TroveInfo ON
                        Instances.instanceId = TroveInfo.instanceId
                WHERE
                    infoType = ? AND
                    troveName = ? AND
                    version = ? AND
                    %s""" % flavorStr,
                        [trove._TROVEINFO_TAG_INCOMPLETE,
                         name, str(version)] + flavorArgs)
        frzIncomplete = cu.next()[0]
        return streams.ByteStream(frzIncomplete)() != 0

    def iterFilesWithTag(self, tag):
        return self.troveFiles.iterFilesWithTag(tag)

    def getTrovesWithProvides(self, depSetList):
        return self.depTables.getLocalProvides(depSetList)

    def getCompleteTroveSet(self, names):
        # returns three sets; one is all of the troves which are installed,
        # and are not included by any other installed troves, one is troves
        # which are installed, and are included by some other trove that is
        # installed, and the other is all of the troves which are referenced
        # but not installed
        cu = self.db.cursor()
        cu.execute("CREATE TEMPORARY TABLE "
                   "gcts(troveName %(STRING)s)" % self.db.keywords,
                   start_transaction = False)
        cu.executemany("INSERT INTO gcts VALUES (?)", names,
                       start_transaction = False)
        cu.execute("""
                SELECT Instances.troveName, version, flavor, isPresent,
                       timeStamps, TroveTroves.flags, TroveTroves.inPristine
                    FROM
                    gcts LEFT OUTER JOIN Instances
                        USING (troveName)
                    JOIN Versions
                        USING(versionId)
                    JOIN Flavors ON
                        Instances.flavorId = Flavors.flavorId
                    LEFT JOIN TroveTroves ON
                        Instances.instanceId = TroveTroves.includedId
                WHERE
                    Instances.troveName IS NOT NULL
            """)

        # it's much faster to build up lists and then turn them into
        # sets than build up the set one member at a time
        installedNotReferenced = []
        installedAndReferenced = []
        referencedStrong = []
        referencedWeak = []

        versionCache = VersionCache()
        flavorCache = FlavorCache()
        for (name, version, flavor, isPresent, timeStamps, flags,
             hasParent) in cu:
            v = versionCache.get(version, timeStamps)
            f = flavorCache.get(flavor)
            info = (name, v, f)

            if isPresent:
                if hasParent:
                    installedAndReferenced.append(info)
                else:
                    installedNotReferenced.append(info)
            elif flags & schema.TROVE_TROVES_WEAKREF:
                referencedWeak.append(info)
            else:
                referencedStrong.append(info)

        cu.execute("DROP TABLE gcts", start_transaction = False)

        referencedStrong = set(referencedStrong)
        installedAndReferenced = set(installedAndReferenced)

        return (set(installedNotReferenced) - installedAndReferenced,
                installedAndReferenced,
                referencedStrong,
                set(referencedWeak) - referencedStrong)

    def getMissingPathIds(self, name, version, flavor):
        cu = self.db.cursor()

        flavorId = self.flavors.get(flavor, None)
        if flavorId is None:
            raise KeyError
        versionId = self.versionTable.get(version, None)
        if versionId is None:
            raise KeyError

        cu.execute("""
            SELECT pathId FROM Instances JOIN DBTroveFiles USING (instanceId)
                WHERE Instances.troveName = ? AND Instances.versionId = ?
                AND Instances.flavorId = ? AND DBTroveFiles.isPresent = 0""",
                name, versionId, flavorId)

        return [ x[0] for x in cu ]

    def _getTransactionCounter(self, field):
        """Get transaction counter
        Return (Boolean, value) with boolean being True if the counter was
        found in the table"""
        if 'DatabaseAttributes' not in self.db.tables:
            # We should already have converted the schema to have the table in
            # place. This may mean an update code path run with --info as
            # non-root (or owner of the schema)
            # incrementTransactionCounter should fail though.
            return False, 0

        cu = self.db.cursor()
        cu.execute("SELECT value FROM DatabaseAttributes WHERE name = ?",
                   field)
        try:
            row = cu.next()
            counter = row[0]
        except StopIteration:
            return False, 0

        try:
            counter = int(counter)
        except ValueError:
            return True, 0

        return True, counter

    def getTransactionCounter(self):
        """Get transaction counter"""
        field = "transaction counter"
        return self._getTransactionCounter(field)[1]

    def incrementTransactionCounter(self):
        """Increment the transaction counter.
        To work reliably, you should already have the database locked, you
        don't want the read and update to be interrupted by another update"""

        field = "transaction counter"

        exists, counter = self._getTransactionCounter(field)

        cu = self.db.cursor()
        if not exists:
            # Row is not in the table
            cu.execute("INSERT INTO DatabaseAttributes (name, value) "
                       "VALUES (?, ?)", field, '1')
            return 1

        counter += 1
        cu.execute("UPDATE DatabaseAttributes SET value = ? WHERE name = ?",
                   str(counter), field)
        return counter

    def close(self):
        self.db.close()
