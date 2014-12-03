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


import os
import itertools

from conary import files, trove, versions, changelog, callbacks
from conary.cmds import metadata
from conary.dbstore import idtable
from conary.deps import deps
from conary.lib import util, tracelog
from conary.local import deptable
from conary.local.sqldb import VersionCache, FlavorCache
from conary.local import versiontable
from conary.repository import errors
from conary.repository.netrepos import instances, items, keytable, flavors,\
     troveinfo, versionops, cltable, accessmap
from conary.server import schema


class TroveAdder:

    def addStream(self, fileId, fileStream = None, withContents = True):
        existing = self.newStreamsByFileId.get(fileId)
        if existing and existing[1]:
            # we have the stream for this fileId already. we don't need
            # it again
            return

        if fileStream and withContents:
            sha1 = None

            if (not files.frozenFileFlags(fileStream).isEncapsulatedContent()
                    and files.frozenFileHasContents(fileStream)):
                cont = files.frozenFileContentInfo(fileStream)
                sha1 = cont.sha1()
        else:
            sha1 = None

        self.newStreamsByFileId[fileId] = (self.cu.binary(fileId),
                                           self.cu.binary(fileStream),
                                           self.cu.binary(sha1))

    def addFile(self, pathId, path, fileId, fileVersion,
                fileStream = None, withContents = True):
        dirname, basename = os.path.split(path)

        pathChanged = 1

        versionId = self.troveStore.getVersionId(fileVersion)

        self.newFilesInsertList.append((self.cu.binary(pathId), versionId,
                                        self.cu.binary(fileId),
                                        self.dirMap[dirname],
                                        self.baseMap[basename],
                                        pathChanged))

        changeInfo = self.changeMap.get(pathId, None)
        if changeInfo:
            if not changeInfo[1]:
                pathChanged = 0
            if not changeInfo[3]:
                versionChanged = False
            else:
                versionChanged = True
        else:
            versionChanged = (pathId in self.newSet)

        # If the file version is the same as in the old trove, or if we have
        # seen this fileId before, ignore the new stream data
        if not versionChanged:
            fileStream = None

        self.addStream(fileId, fileStream = fileStream,
                       withContents = withContents)

    def __init__(self, troveStore, cu, trv, trvCs, hidden, newSet, changeMap,
                 dirMap, baseMap, newStreamsByFileId):
        self.troveStore = troveStore
        self.cu = cu
        self.trv = trv
        self.trvCs = trvCs
        self.hidden = hidden
        self.newFilesInsertList = []
        self.newStreamsByFileId = newStreamsByFileId
        self.newSet = newSet
        self.changeMap = changeMap
        self.dirMap = dirMap
        self.baseMap = baseMap


class TroveStore:
    def __init__(self, db, log = None):
        self.db = db

        self.items = items.Items(self.db)
        self.flavors = flavors.Flavors(self.db)
        self.branchTable = versionops.BranchTable(self.db)
        self.changeLogs = cltable.ChangeLogTable(self.db)

        self.versionTable = versiontable.VersionTable(self.db)
        self.versionOps = versionops.SqlVersioning(
            self.db, self.versionTable, self.branchTable)
        self.instances = instances.InstanceTable(self.db)
        self.latest = versionops.LatestTable(self.db)
        self.ri = accessmap.RoleInstances(self.db)

        self.keyTable = keytable.OpenPGPKeyTable(self.db)
        self.depTables = deptable.DependencyTables(self.db)
        self.metadataTable = metadata.MetadataTable(self.db, create = False)
        self.troveInfoTable = troveinfo.TroveInfoTable(self.db)

        self.needsCleanup = False
        self.log = log or tracelog.getLog(None)

        self.versionIdCache = {}
        self.itemIdCache = {}

    def __del__(self):
        self.db = self.log = None

    def getLabelId(self, label):
        self.versionOps.labels.getOrAddId(label)

    def getItemId(self, item):
        itemId = self.itemIdCache.get(item, None)
        if itemId is not None:
            return itemId
        itemId = self.items.get(item, None)
        if itemId is None:
            itemId = self.items.addId(item)
        self.itemIdCache[item] = itemId
        return itemId

    def getInstanceId(self, itemId, versionId, flavorId, clonedFromId,
                      troveType, isPresent = instances.INSTANCE_PRESENT_NORMAL):
        theId = self.instances.get((itemId, versionId, flavorId), None)
        if theId == None:
            theId = self.instances.addId(itemId, versionId, flavorId,
                                         clonedFromId,
                                         troveType, isPresent = isPresent)
        # XXX we shouldn't have to do this unconditionally
        if isPresent != instances.INSTANCE_PRESENT_MISSING:
            self.instances.update(theId, isPresent=isPresent, clonedFromId=clonedFromId)
            self.items.setTroveFlag(itemId, 1)
        return theId

    def getVersionId(self, version):
        theId = self.versionIdCache.get(version, None)
        if theId:
            return theId

        theId = self.versionTable.get(version, None)
        if theId == None:
            theId = self.versionTable.addId(version)

        self.versionIdCache[version] = theId
        return theId

    def getFullVersion(self, item, version):
        """
        Updates version with full timestamp information.
        """
        cu = self.db.cursor()
        cu.execute("""
        SELECT timeStamps
        FROM Nodes
        WHERE itemId=(SELECT itemId FROM Items WHERE item=?)
          AND versionId=(SELECT versionId FROM Versions WHERE version=?)
        """, item, version.asString())

        timeStamps = cu.fetchone()[0]
        version.setTimeStamps([float(x) for x in timeStamps.split(":")])

    def createTroveBranch(self, troveName, branch):
        itemId = self.getItemId(troveName)
        self.versionOps.createBranch(itemId, branch)

    def getTroveFlavors(self, troveDict):
        cu = self.db.cursor()
        vMap = {}
        outD = {}
        # I think we might be better of intersecting subqueries rather
        # then using all of the and's in this join

        schema.resetTable(cu, 'tmpIVF')
        for troveName in troveDict.keys():
            outD[troveName] = {}
            for version in troveDict[troveName]:
                outD[troveName][version] = []
                versionStr = version.asString()
                vMap[versionStr] = version
                cu.execute("INSERT INTO tmpIVF VALUES (?, ?, ?)",
                           (troveName, versionStr, versionStr),
                           start_transaction = False)
        self.db.analyze("tmpIVF")
        cu.execute("""
        SELECT Items.item, fullVersion, Flavors.flavor
        FROM tmpIVF
        JOIN Items on tmpIVF.item = Items.item
        JOIN Versions on tmpIVF.version = Versions.version
        JOIN Instances on
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId
        JOIN Flavors using (flavorId)
        ORDER BY Items.item, fullVersion
        """)

        for (item, verString, flavor) in cu:
            ver = vMap[verString]
            outD[item][ver].append(flavor)

        return outD

    def iterTroveNames(self):
        cu = self.db.cursor()
        cu.execute("""
        SELECT DISTINCT Items.item as item
        FROM Instances
        JOIN Items USING(itemId)
        WHERE Instances.isPresent = ?
        ORDER BY item
        """, instances.INSTANCE_PRESENT_NORMAL)
        for (item,) in cu:
            yield item

    # refresh the latest for all the recently presented troves
    def presentHiddenTroves(self):
        cu = self.db.cursor()
        schema.resetTable(cu, 'tmpNewTroves')
        cu.execute("""
            INSERT INTO tmpNewTroves (itemId, branchId, flavorId,
                versionId, instanceId, hidden, finaltimestamp)
            SELECT itemId, branchId, flavorId, 0, instanceId, 0, 0
            FROM Instances
            JOIN Nodes USING (itemId, versionId)
            WHERE isPresent = ?
            """, instances.INSTANCE_PRESENT_HIDDEN)
        cu.execute("""UPDATE Instances SET isPresent = ?
            WHERE instanceId IN (SELECT instanceId FROM tmpNewTroves)
            """, instances.INSTANCE_PRESENT_NORMAL)
        self.latest.updateFromNewTroves('tmpNewTroves')

    def addTrove(self, trv, trvCs, hidden = False):
        cu = self.db.cursor()
        changeMap = dict((x[0], x) for x in trvCs.getChangedFileList())
        newSet = set(x[0] for x in trvCs.getNewFileList())
        return TroveAdder(self, cu, trv, trvCs, hidden, newSet, changeMap,
                          self.dirMap, self.baseMap, self.newStreamsByFileId)

    # walk the trove and insert any missing flavors we need into the Flavors table
    def _addTroveNewFlavors(self, cu, trv):
        # XXX: a lot of the work from this function can be cached so
        #      that we don't repeat it for every identically flavored trove we commit
        # XXX: seems to me that if we run this once for the top of stack
        #      group/package we don't need to repeat it for its members
        troveFlavor = trv.getFlavor()

        # start off by creating the flavors we need; we could combine this
        # to some extent with the file table creation below, but there are
        # normally very few flavors per trove so this probably better
        flavorsNeeded = {}
        if troveFlavor is not None:
            flavorsNeeded[troveFlavor] = True

        for (name, version, flavor) in trv.iterTroveList(strongRefs = True,
                                                           weakRefs = True):
            if flavor is not None:
                flavorsNeeded[flavor] = True

        for (name, branch, flavor) in trv.iterRedirects():
            if flavor is not None:
                flavorsNeeded[flavor] = True

        if len(flavorsNeeded) == 1:
            newFlavor = flavorsNeeded.keys()[0]
            i = self.flavors.get(newFlavor, None)
            if i is None:
                i = self.flavors.createFlavor(newFlavor)

            flavors = { newFlavor : i }
        else:
            flavorIndex = {}
            schema.resetTable(cu, "tmpItems")
            for flavor in flavorsNeeded.iterkeys():
                flavorIndex[flavor.freeze()] = flavor
                cu.execute("INSERT INTO tmpItems(item) VALUES(?)",
                           flavor.freeze(), start_transaction=False)
            del flavorsNeeded
            self.db.analyze("tmpItems")

            # it seems like there must be a better way to do this, but I can't
            # figure it out. I *think* inserting into a view would help, but I
            # can't with sqlite.
            cu.execute("""
            select tmpItems.item as flavor
            from tmpItems
            where not exists ( select flavor from Flavors
                               where Flavors.flavor = tmpItems.item ) """)
            # make a list of the flavors we're going to create.  Add them
            # after we have retrieved all of the rows from this select
            l = []
            for (flavorStr,) in cu:
                l.append(flavorIndex[flavorStr])
            for flavor in l:
                self.flavors.createFlavor(flavor)

            flavors = {}
            cu.execute("""
            SELECT Flavors.flavor, Flavors.flavorId
            FROM tmpItems
            JOIN Flavors ON tmpItems.item = Flavors.flavor""")
            for (flavorStr, flavorId) in cu:
                flavors[flavorIndex[flavorStr]] = flavorId

            del flavorIndex

        if troveFlavor is not None:
            troveFlavorId = flavors[troveFlavor]
        else:
            troveFlavorId = 0
        return troveFlavorId

    # tmpNewFiles can contain duplicate entries for the same fileId,
    # (with stream being NULL or not), so the FileStreams update
    # is happening in three steps:
    # 1. Update existing fileIds
    # 2. Insert new fileIds with non-NULL streams
    # 3. Insert new fileIds that might have NULL streams.
    def _mergeTroveNewFiles(self, cu):
        # We split these steps into separate queries because most DB
        # backends I have tried will get the optimization of a single
        # query wrong --gafton
        self.db.analyze("tmpNewFiles")

        if len(self.newStreamsByFileId):
            self.db.bulkload("tmpNewStreams", self.newStreamsByFileId.values(),
                             [ "fileId", "stream", "sha1" ] )
        self.db.analyze("tmpNewStreams")

        # In the extreme case of binary shadowing this might require a
        # bit of of memory for larger troves, but it is preferable to
        # constant full table scans in the much more common cases
        cu.execute("""
        SELECT tmpNewStreams.fileId, tmpNewStreams.stream, tmpNewStreams.sha1
        FROM tmpNewStreams
        JOIN FileStreams USING(fileId)
        WHERE FileStreams.stream IS NULL
        AND tmpNewStreams.stream IS NOT NULL
        """)
        for (fileId, stream, sha1) in cu.fetchall():
            cu.execute("UPDATE FileStreams SET stream = ? WHERE fileId = ?",
                       (cu.binary(stream), cu.binary(fileId)))
            if sha1:
                cu.execute("UPDATE FileStreams SET sha1 = ? "
                           "WHERE fileId = ?",
                           (cu.binary(sha1), cu.binary(fileId)))

        # select the new non-NULL streams out of tmpNewFiles and Insert
        # them in FileStreams
        cu.execute("""
        INSERT INTO FileStreams (fileId, stream, sha1)
        SELECT DISTINCT NS.fileId, NS.stream, NS.sha1
        FROM tmpNewStreams AS NS
        LEFT JOIN FileStreams AS FS USING(fileId)
        WHERE FS.fileId IS NULL
          AND NS.stream IS NOT NULL
          """)
        # now insert the other fileIds. select the new non-NULL streams
        # out of tmpNewFiles and insert them in FileStreams
        cu.execute("""
        INSERT INTO FileStreams (fileId, stream, sha1)
        SELECT DISTINCT NS.fileId, NS.stream, NS.sha1
        FROM tmpNewStreams AS NS
        LEFT JOIN FileStreams AS FS USING(fileId)
        WHERE FS.fileId IS NULL
        """)

        cu.execute("""INSERT INTO FilePaths(pathId, dirnameId, basenameId)
                        SELECT DISTINCT tnf.pathId, tnf.dirnameId,
                                        tnf.basenameId
                        FROM tmpNewFiles AS tnf
                        LEFT OUTER JOIN FilePaths AS fp ON
                            fp.pathId = tnf.pathId and
                            fp.dirnameId = tnf.dirnameId and
                            fp.basenameId = tnf.basenameId
                        WHERE
                            tnf.pathChanged = 1 AND
                            fp.pathId IS NULL""")

        # create the TroveFiles links for this trove's files.
        cu.execute(""" insert into TroveFiles (instanceId, streamId, versionId, filePathId)
        select tnf.instanceId, fs.streamId, tnf.versionId, fp.filePathId
        from tmpNewFiles as tnf
        join FilePaths as fp on
            fp.pathId = tnf.pathId and
            fp.dirnameId = tnf.dirnameId and
            fp.basenameId = tnf.basenameId
        join FileStreams as fs on tnf.fileId = fs.fileId
        """)

    def _mergeIncludedTroves(self, cu):
        cu.execute("""
        INSERT INTO Items (item)
        SELECT DISTINCT tmpTroves.item FROM tmpTroves
        LEFT JOIN Items USING (item)
        WHERE
            Items.item is NULL
        """)

        cu.execute("""
        INSERT INTO Versions (version)
        SELECT DISTINCT tmpTroves.version FROM tmpTroves
        LEFT JOIN Versions USING (version)
        WHERE
            Versions.version is NULL
        """)

        cu.execute("""
        INSERT INTO Branches (branch)
        SELECT DISTINCT tmpTroves.branch FROM tmpTroves
        LEFT JOIN Branches USING (branch)
        WHERE
            Branches.branch is NULL
        """)

        cu.execute("""
        INSERT INTO Labels (label)
        SELECT DISTINCT tmpTroves.label FROM tmpTroves
        LEFT JOIN Labels USING (label)
        WHERE
            Labels.label is NULL
        """)

        cu.execute("""
        INSERT INTO Nodes(itemId, branchId, versionId, timestamps,
                          finalTimestamp)
        SELECT DISTINCT Items.itemId, Branches.branchId, Versions.versionId,
                        tmpTroves.timeStamps, tmpTroves.finalTimestamp
        FROM tmpTroves
        JOIN Items USING (item)
        JOIN Versions ON Versions.version = tmpTroves.version
        JOIN Branches ON Branches.branch = tmpTroves.branch
        LEFT JOIN Nodes ON
            Items.itemId = Nodes.itemId AND
            Versions.versionId = Nodes.versionId
        WHERE
            Nodes.nodeId is NULL
        """)

        cu.execute("""
        INSERT INTO LabelMap(itemId, branchId, labelId)
        SELECT DISTINCT Items.itemId, Branches.branchId, Labels.labelId
        FROM tmpTroves
        JOIN Items USING (item)
        JOIN Branches ON Branches.branch = tmpTroves.branch
        JOIN Labels ON Labels.label = tmpTroves.label
        LEFT JOIN LabelMap ON
            Items.itemId = LabelMap.itemId AND
            Branches.branchId = LabelMap.branchId AND
            Labels.labelId = LabelMap.labelId
        WHERE
            LabelMap.itemId is NULL
        """)

        cu.execute("""
        INSERT INTO Instances (itemId, versionId, flavorId, isPresent,
                               troveType)
        SELECT DISTINCT Items.itemId, Versions.versionId, flavors.flavorId, ?,
               tmpTroves.troveType
        FROM tmpTroves
        JOIN Items USING (item)
        JOIN Flavors ON Flavors.flavor = tmpTroves.flavor
        JOIN Versions ON Versions.version = tmpTroves.version
        LEFT JOIN Instances ON
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        WHERE
            Instances.instanceId is NULL
        """, instances.INSTANCE_PRESENT_MISSING)

        schema.resetTable(cu, 'tmpGroupInsertShim')
        cu.execute("""
        INSERT INTO tmpGroupInsertShim (itemId, versionId, flavorId, flags, instanceId)
        SELECT itemId, versionId, flavorId, flags, instanceId
        FROM tmpTroves
        JOIN Items USING (item)
        JOIN Versions ON Versions.version = tmpTroves.version
        JOIN Flavors ON Flavors.flavor = tmpTroves.flavor
        """)

        cu.execute("""
        INSERT INTO TroveTroves (instanceId, includedId, flags)
        SELECT tmpGroupInsertShim.instanceId, Instances.instanceId, flags
        FROM tmpGroupInsertShim
        JOIN Instances ON
            tmpGroupInsertShim.itemId = Instances.itemId AND
            tmpGroupInsertShim.versionId = Instances.versionId AND
            tmpGroupInsertShim.flavorId = Instances.flavorId
        """)

    def addTroveDone(self, troveInfo, mirror=False):
        cu = troveInfo.cu
        trv = troveInfo.trv
        trvCs = troveInfo.trvCs
        hidden = troveInfo.hidden

        newFilesInsertList = troveInfo.newFilesInsertList

        self.log(3, trv)

        troveVersion = trv.getVersion()
        troveItemId = self.getItemId(trv.getName())
        sourceName = trv.troveInfo.sourceName()

        # Pull out the clonedFromId
        clonedFrom = trv.troveInfo.clonedFrom()
        clonedFromId = None
        if clonedFrom:
            clonedFromId = self.versionTable.get(clonedFrom, None)
            if clonedFromId is None:
                clonedFromId = self.versionTable.addId(clonedFrom)

        isPackage = (not trv.getName().startswith('group') and
                     not trv.getName().startswith('fileset') and
                     ':' not in trv.getName())

        troveVersionId = self.versionTable.get(troveVersion, None)
        if troveVersionId is not None:
            nodeId = self.versionOps.nodes.getRow(
                troveItemId, troveVersionId, None)

        # this also adds all the missing flavors into the mix
        troveFlavorId = self._addTroveNewFlavors(cu, trv)

        if troveVersionId is None or nodeId is None:
            (nodeId, troveVersionId) = self.versionOps.createVersion(
                troveItemId, troveVersion, troveFlavorId, sourceName)
            if trv.getChangeLog() and trv.getChangeLog().getName():
                self.changeLogs.add(nodeId, trv.getChangeLog())
        elif sourceName: # make sure the sourceItemId matches for the trove we are comitting
            sourceItemId = self.items.getOrAddId(sourceName)
            self.versionOps.nodes.updateSourceItemId(nodeId, sourceItemId, mirrorMode=mirror)

        # the instance may already exist (it could be referenced by a package
        # which has already been added)
        if hidden:
            presence = instances.INSTANCE_PRESENT_HIDDEN
        else:
            presence = instances.INSTANCE_PRESENT_NORMAL

        troveInstanceId = self.getInstanceId(troveItemId, troveVersionId,
                         troveFlavorId, clonedFromId, trv.getType(),
                         isPresent = presence)
        # check that we don't have any TroveTroves entries yet
        cu.execute("select includedId from TroveTroves "
                   "where instanceId = ? limit 1", troveInstanceId)
        assert(len(cu.fetchall()) == 0), "troveId=%d %s has TroveTroves entries" % (
            troveInstanceId, trv.getNameVersionFlavor())

        troveBranchId = self.branchTable[troveVersion.branch()]
        self.depAdder.add(trv, troveInstanceId)

        if trvCs.getOldVersion():
            oldTroveVersionId = self.versionTable.get(trvCs.getOldVersion(),
                                                      None)
            oldFlavorId = self.flavors.get(trvCs.getOldFlavor(), None)
            oldInstanceId = self.instances[(troveItemId,
                                            oldTroveVersionId,
                                            oldFlavorId)]
        else:
            oldInstanceId = None

        cu.execute("""
        INSERT INTO tmpNewTroves (itemId, branchId, flavorId,
                                  instanceId, versionId,
                                  finalTimeStamp, troveType,
                                  oldInstanceId, hidden)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, troveItemId, troveBranchId, troveFlavorId,
             troveInstanceId, troveVersionId,
             '%.3f' % trv.getVersion().timeStamps()[-1],
             trv.getType(), oldInstanceId, int(hidden))

        # Fold tmpNewFiles into FileStreams
        if len(newFilesInsertList):
            self.db.bulkload("tmpNewFiles",
                    [ x + (troveInstanceId,) for x in newFilesInsertList ],
                    [ "pathId", "versionId", "fileId",
                      "dirnameId", "basenameId", "pathChanged",
                      "instanceId" ])

        # iterate over both strong and weak troves, and set weakFlag to
        # indicate which kind we're looking at when
        insertList = []
        for ((name, version, flavor), weakFlag) in itertools.chain(
                itertools.izip(trv.iterTroveList(strongRefs = True,
                                                   weakRefs   = False),
                               itertools.repeat(0)),
                itertools.izip(trv.iterTroveList(strongRefs = False,
                                                   weakRefs   = True),
                               itertools.repeat(schema.TROVE_TROVES_WEAKREF))):

            flags = weakFlag
            if trv.includeTroveByDefault(name, version, flavor):
                flags |= schema.TROVE_TROVES_BYDEFAULT

            # sanity check - version/flavor of components must match the
            # version/flavor of the package
            assert(not isPackage or version == trv.getVersion())
            assert(not isPackage or flavor == trv.getFlavor())
            insertList.append((name, str(version), version.freeze(),
                               ":".join(["%.3f" % x for x in
                                            version.timeStamps()]),
                               '%.3f' %version.timeStamps()[-1],
                               str(version.branch()),
                               str(version.trailingLabel()),
                               flavor.freeze(), flags, troveInstanceId,
                               trv.getType()))

        if len(insertList):
            self.db.bulkload("tmpTroves", insertList, [
                "item", "version", "frozenVersion", "timestamps",
                "finalTimestamp",
                "branch", "label", "flavor", "flags", "instanceId",
                "troveType"])

        # process troveInfo and metadata...
        self.troveInfoTable.addInfo(cu, trv, troveInstanceId)

        if len(list(trv.iterRedirects())):
            # don't bother with any of this unless there actually are redirects
            schema.resetTable(cu, 'tmpNewRedirects')
            # now add the redirects
            for (name, branch, flavor) in trv.iterRedirects():
                if flavor is None:
                    frz = None
                else:
                    frz = flavor.freeze()
                cu.execute("INSERT INTO tmpNewRedirects (item, branch, flavor) "
                           "VALUES (?, ?, ?)", (name, str(branch), frz),
                           start_transaction=False)
            self.db.analyze("tmpNewRedirects")

            cu.execute("""
            INSERT INTO Items (item)
            SELECT tmpNewRedirects.item
            FROM tmpNewRedirects
            LEFT JOIN Items USING (item)
            WHERE Items.itemId is NULL
            """)

            cu.execute("""
            INSERT INTO Branches (branch)
            SELECT tmpNewRedirects.branch
            FROM tmpNewRedirects
            LEFT JOIN Branches USING (branch)
            WHERE Branches.branchId is NULL
            """)

            cu.execute("""
            INSERT INTO Flavors (flavor)
            SELECT tmpNewRedirects.flavor
            FROM tmpNewRedirects
            LEFT JOIN Flavors USING (flavor)
            WHERE
                Flavors.flavor is not NULL
                AND Flavors.flavorId is NULL
            """)

            cu.execute("""
            INSERT INTO TroveRedirects (instanceId, itemId, branchId, flavorId)
            SELECT %d, Items.itemId, Branches.branchId, Flavors.flavorId
            FROM tmpNewRedirects
            JOIN Items USING (item)
            JOIN Branches ON tmpNewRedirects.branch = Branches.branch
            LEFT JOIN Flavors ON tmpNewRedirects.flavor = Flavors.flavor
            """ % troveInstanceId)

    def addTroveSetStart(self, oldTroveInfoList, dirNames, baseNames):
        cu = self.db.cursor()
        schema.resetTable(cu, 'tmpTroves')
        schema.resetTable(cu, 'tmpNewFiles')
        schema.resetTable(cu, 'tmpNewTroves')
        schema.resetTable(cu, 'tmpNewStreams')
        schema.resetTable(cu, 'tmpNewLatest')
        self.depAdder = deptable.BulkDependencyLoader(self.db, cu)
        self.newStreamsByFileId = dict()

        schema.resetTable(cu, 'tmpNewPaths')
        l = [(cu.binary(x),) for x in dirNames]
        self.db.bulkload("tmpNewPaths", l, [ "path" ])
        cu.execute("""
            INSERT INTO Dirnames (dirName)
                SELECT path FROM tmpNewPaths
                    LEFT OUTER JOIN Dirnames ON
                        Dirnames.dirName = tmpNewPaths.path
                    WHERE Dirnames.dirNameId IS NULL
        """)
        cu.execute("""
                SELECT Dirnames.dirName, Dirnames.dirNameId FROM
                    tmpNewPaths JOIN Dirnames ON
                        Dirnames.dirName = tmpNewPaths.path
        """)
        self.dirMap = dict((cu.frombinary(x[0]), x[1]) for x in cu)

        schema.resetTable(cu, 'tmpNewPaths')
        l = [(cu.binary(x),) for x in baseNames]
        self.db.bulkload("tmpNewPaths", l, [ "path" ])
        cu.execute("""
            INSERT INTO Basenames (baseName)
                SELECT path FROM tmpNewPaths
                    LEFT OUTER JOIN Basenames ON
                        Basenames.baseName = tmpNewPaths.path
                    WHERE Basenames.baseNameId IS NULL
        """)
        cu.execute("""
                SELECT Basenames.baseName, Basenames.baseNameId FROM
                    tmpNewPaths JOIN Basenames ON
                        Basenames.baseName = tmpNewPaths.path
        """)
        self.baseMap = dict((cu.frombinary(x[0]), x[1]) for x in cu)

        schema.resetTable(cu, 'tmpNVF')
        self.db.bulkload("tmpNVF",
            [ (x[0], str(x[1]), x[2].freeze()) for x in oldTroveInfoList ],
            [ "name", "version", "flavor" ])

        cu.execute("""
            SELECT DISTINCT baseName, baseNameId FROM tmpNVF
                JOIN Items ON
                    tmpNVF.name = Items.item
                JOIN Versions ON
                    tmpNVF.version = Versions.version
                JOIN Flavors ON
                    tmpNVF.flavor = Flavors.flavor
                JOIN Instances ON
                    Items.itemId = Instances.itemId AND
                    Versions.versionId = Instances.versionId AND
                    Flavors.flavorId = Instances.flavorId
                JOIN TroveFiles USING (instanceId)
                JOIN FilePaths USING (filePathId)
                JOIN BaseNames USING (baseNameId)
        """)
        self.baseMap.update(dict((cu.frombinary(x[0]), x[1]) for x in cu))

        cu.execute("""
            SELECT DISTINCT dirName, dirNameId FROM tmpNVF
                JOIN Items ON
                    tmpNVF.name = Items.item
                JOIN Versions ON
                    tmpNVF.version = Versions.version
                JOIN Flavors ON
                    tmpNVF.flavor = Flavors.flavor
                JOIN Instances ON
                    Items.itemId = Instances.itemId AND
                    Versions.versionId = Instances.versionId AND
                    Flavors.flavorId = Instances.flavorId
                JOIN TroveFiles USING (instanceId)
                JOIN FilePaths USING (filePathId)
                JOIN DirNames USING (dirNameId)
        """)
        self.dirMap.update(dict((cu.frombinary(x[0]), x[1]) for x in cu))

    def addTroveSetDone(self, callback=None):
        self.dirMap = None
        self.baseMap = None

        if not callback:
            callback = callbacks.UpdateCallback()
        cu = self.db.cursor()

        self._mergeIncludedTroves(cu)
        self._mergeTroveNewFiles(cu)

        self.newStreamsByFileId = None

        self.ri.addInstanceIdSet('tmpNewTroves', 'instanceId')
        self.depAdder.done()
        self.depAdder = None

        callback.updatingDatabase('latest', 1, 1)
        self.latest.updateFromNewTroves('tmpNewTroves')

    def updateMetadata(self, troveName, branch, shortDesc, longDesc,
                    urls, licenses, categories, source, language):
        itemId = self.getItemId(troveName)
        branchId = self.branchTable[branch]

        # if we're updating the default language, always create a new version
        # XXX we can remove one vesionTable.get call from here...
        # XXX this entire mass of code can probably be improved.
        #     surely someone does something similar someplace else...
        latestVersion = self.metadataTable.getLatestVersion(itemId, branchId)
        if language == "C":
            if latestVersion: # a version exists, increment it
                version = versions.VersionFromString(latestVersion).copy()
                version.incrementSourceCount()
            else: # otherwise make a new version
                version = versions._VersionFromString("1-1", defaultBranch=branch)

            if not self.versionTable.get(version, None):
                self.versionTable.addId(version)
        else: # if this is a translation, update the current version
            if not latestVersion:
                raise KeyError, troveName
            version = versions.VersionFromString(latestVersion)

        versionId = self.versionTable.get(version, None)
        return self.metadataTable.add(itemId, versionId, branchId, shortDesc, longDesc,
                                      urls, licenses, categories, source, language)

    def getMetadata(self, troveName, branch, version=None, language="C"):
        itemId = self.items.get(troveName, None)
        if not itemId:
            return None

        # follow the branch tree up until we find metadata
        md = None
        while not md:
            # make sure we're on the same server
            if self.branchTable.has_key(branch):
                branchId = self.branchTable[branch]
            else:
                return None

            if not version:
                latestVersion = self.metadataTable.getLatestVersion(itemId, branchId)
            else:
                latestVersion = version.asString()
            cu = self.db.cursor()
            cu.execute("SELECT versionId FROM Versions WHERE version=?", latestVersion)

            versionId = cu.fetchone()
            if versionId:
                versionId = versionId[0]
            else:
                if branch.hasParentBranch():
                    branch = branch.parentBranch()
                else:
                    return None

            md = self.metadataTable.get(itemId, versionId, branchId, language)

        md["version"] = versions.VersionFromString(latestVersion).asString()
        md["language"] = language
        return metadata.Metadata(md)

    def hasFileContents(self, sha1iter):
        cu = self.db.cursor()
        schema.resetTable(cu, 'tmpSha1s')
        self.db.bulkload("tmpSha1s", ((cu.binary(x),) for x in sha1iter),
                         [ "sha1" ], start_transaction = False)
        cu.execute("""
        select case when exists
            (select 1 from FileStreams where filestreams.sha1 = tmpSha1s.sha1)
        then 1 else 0 end from tmpSha1s;
        """)

        return [ x[0] for x in cu ]

    def hasTrove(self, troveName, troveVersion = None, troveFlavor = None):
        self.log(3, troveName, troveVersion, troveFlavor)

        if not troveVersion:
            return self.items.has_key(troveName)

        assert(troveFlavor is not None)

        # if we can not find the ids for the troveName, troveVersion
        # or troveFlavor in their respective tables, than this trove
        # can't possibly exist...
        troveItemId = self.items.get(troveName, None)
        if troveItemId is None:
            return False
        troveVersionId = self.versionTable.get(troveVersion, None)
        if troveVersionId is None:
            return False
        troveFlavorId = self.flavors.get(troveFlavor, None)
        if troveFlavorId is None:
            return False

        return self.instances.isPresent((troveItemId, troveVersionId,
                                         troveFlavorId))

    def getTrove(self, troveName, troveVersion, troveFlavor, withFiles = True,
                 hidden = False):
        troveIter = self.iterTroves(( (troveName, troveVersion, troveFlavor), ),
                                    withFiles = withFiles, hidden = hidden)
        trv = [ x for x in troveIter ][0]
        if trv is None:
            raise errors.TroveMissing(troveName, troveVersion)
        return trv

    def iterTroves(self, troveInfoList, withFiles = True,
                   withFileStreams = False,
                   hidden = False, permCheckFilter = None):
        self.log(3, troveInfoList, "withFiles=%s withFileStreams=%s hidden=%s" % (
                        withFiles, withFileStreams, hidden))

        cu = self.db.cursor()
        schema.resetTable(cu, 'tmpNVF')
        schema.resetTable(cu, 'tmpInstanceId')

        l = []
        for idx, (n,v,f) in enumerate(troveInfoList):
            l.append((idx, n, v.asString(), f.freeze()))
        self.db.bulkload("tmpNVF", l, [ "idx", "name", "version", "flavor" ],
                         start_transaction = False)
        del l
        self.db.analyze("tmpNVF")

        args = [instances.INSTANCE_PRESENT_NORMAL]
        d = dict(presence = "Instances.isPresent = ?")
        if hidden:
            args.append(instances.INSTANCE_PRESENT_HIDDEN)
            d = dict(presence = "Instances.isPresent in (?,?)")
        d.update(self.db.keywords)
        cu.execute("""
        SELECT %(STRAIGHTJOIN)s tmpNVF.idx, Instances.instanceId, Instances.troveType,
               Nodes.timeStamps,
               Changelogs.name, ChangeLogs.contact, ChangeLogs.message
        FROM tmpNVF
        JOIN Items on tmpNVF.name = Items.item
        JOIN Versions on tmpNVF.version = Versions.version
        JOIN Flavors on tmpNVF.flavor = Flavors.flavor
        JOIN Instances on
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        JOIN Nodes on
            Instances.itemId = Nodes.itemId AND
            Instances.versionId = Nodes.versionId
        LEFT JOIN ChangeLogs using(nodeId)
        WHERE %(presence)s
        ORDER BY tmpNVF.idx
        """ % d, args)
        troveIdList = [ x for x in cu ]
        # short-circuit for cases when nothing matches
        if not troveIdList:
            for i in xrange(len(troveInfoList)):
                yield None
            return
        self.db.bulkload("tmpInstanceId",
                         [ x[0:2] for x in troveIdList ],
                         [ "idx", "instanceId" ],
                         start_transaction = False)
        self.db.analyze("tmpInstanceId")

        # filter out troves we don't have permissions for
        if permCheckFilter:
            permCheckFilter(cu, "tmpInstanceId")

            cu.execute("select idx from tmpInstanceId")
            validIndexes = set(x[0] for x in cu)
            troveIdList = [ x for x in troveIdList if x[0] in validIndexes ]

        # unfortunately most cost-based optimizers will get the
        # following troveTrovesCursor queries wrong. Details in CNY-2695

        # for PostgreSQL we have to force an execution plan that
        # uses the join order as coded in the query
        if self.db.kind == 'postgresql':
            cu.execute("set join_collapse_limit to 2")
            cu.execute("set enable_seqscan to off")

        troveTrovesCursor = self.db.cursor()
        # the STRAIGHTJOIN hack will ask MySQL to execute the query as written
        troveTrovesCursor.execute("""
        SELECT %(STRAIGHTJOIN)s tmpInstanceId.idx, Items.item, Versions.version,
            Flavors.flavor, TroveTroves.flags, Nodes.timeStamps
        FROM tmpInstanceId
        JOIN TroveTroves using(instanceId)
        JOIN Instances on TroveTroves.includedId = Instances.instanceId
        JOIN Items on Instances.itemId = Items.itemId
        JOIN Versions on Instances.versionId = Versions.versionId
        JOIN Flavors on Instances.flavorId = Flavors.flavorId
        JOIN Nodes on
            Instances.itemId = Nodes.itemId and
            Instances.versionId = Nodes.versionId
        ORDER BY tmpInstanceId.idx
        """ % self.db.keywords)
        troveTrovesCursor = util.PushIterator(troveTrovesCursor)

        # revert changes we forced on the postgresql optimizer
        if self.db.kind == 'postgresql':
            cu.execute("set join_collapse_limit to default")
            cu.execute("set enable_seqscan to default")

        troveFilesCursor = iter(())
        if withFileStreams or withFiles:
            troveFilesCursor = self.db.cursor()
            streamSel = "NULL"
            if withFileStreams:
                streamSel = "FileStreams.stream"
            troveFilesCursor.execute("""
            SELECT tmpInstanceId.idx, FilePaths.pathId,
                   Dirnames.dirname, Basenames.basename,
                   Versions.version, FileStreams.fileId, %s
            FROM tmpInstanceId
            JOIN TroveFiles using(instanceId)
            JOIN FileStreams using(streamId)
            JOIN FilePaths ON TroveFiles.filePathId = FilePaths.filePathId
            JOIN Dirnames ON FilePaths.dirnameId = Dirnames.dirnameId
            JOIN Basenames ON FilePaths.basenameId = Basenames.basenameId
            JOIN Versions ON TroveFiles.versionId = Versions.versionId
            ORDER BY tmpInstanceId.idx """ % (streamSel,))
        troveFilesCursor = util.PushIterator(troveFilesCursor)

        troveRedirectsCursor = self.db.cursor()
        troveRedirectsCursor.execute("""
        SELECT tmpInstanceId.idx, Items.item, Branches.branch, Flavors.flavor
        FROM tmpInstanceId
        JOIN TroveRedirects using (instanceId)
        JOIN Items using (itemId)
        JOIN Branches ON TroveRedirects.branchId = Branches.branchId
        LEFT JOIN Flavors ON TroveRedirects.flavorId = Flavors.flavorId
        ORDER BY tmpInstanceId.idx
        """)
        troveRedirectsCursor = util.PushIterator(troveRedirectsCursor)

        neededIdx = 0
        versionCache = VersionCache()
        flavorCache = FlavorCache()
        while troveIdList:
            (idx, troveInstanceId, troveType, timeStamps,
             clName, clVersion, clMessage) =  troveIdList.pop(0)

            # make sure we've returned something for everything up to this
            # point
            while neededIdx < idx:
                neededIdx += 1
                yield None

            # we need the one after this next time through
            neededIdx += 1

            singleTroveInfo = troveInfoList[idx]

            if clName is not None:
                changeLog = changelog.ChangeLog(clName, clVersion, clMessage)
            else:
                changeLog = None

            v = singleTroveInfo[1]
            key = (v, timeStamps)
            if versionCache.has_key(key):
                v = versionCache(key)
            else:
                v = v.copy()
                v.setTimeStamps([ float(x) for x in timeStamps.split(":") ])

            trv = trove.Trove(singleTroveInfo[0], v,
                              singleTroveInfo[2], changeLog,
                              type = troveType,
                              setVersion = False)

            try:
                next = troveTrovesCursor.next()
                while next[0] == idx:
                    idxA, name, version, flavor, flags, timeStamps = next
                    version = versionCache.get(version, timeStamps)
                    flavor = flavorCache.get(flavor)
                    byDefault = (flags & schema.TROVE_TROVES_BYDEFAULT) != 0
                    weakRef = (flags & schema.TROVE_TROVES_WEAKREF) != 0
                    trv.addTrove(name, version, flavor, byDefault = byDefault,
                                 weakRef = weakRef)
                    next = troveTrovesCursor.next()

                troveTrovesCursor.push(next)
            except StopIteration:
                # we're at the end; that's okay
                pass

            fileContents = {}
            try:
                next = troveFilesCursor.next()
                while next[0] == idx:
                    (idxA, pathId, dirname, basename, versionId, fileId,
                     stream) = next
                    fileId = cu.frombinary(fileId)
                    path = os.path.join(cu.frombinary(dirname),
                            cu.frombinary(basename))
                    version = versions.VersionFromString(versionId)
                    trv.addFile(cu.frombinary(pathId), path, version, fileId)
                    if stream is not None:
                        fileContents[fileId] = stream
                    next = troveFilesCursor.next()

                troveFilesCursor.push(next)
            except StopIteration:
                # we're at the end; that's okay
                pass

            try:
                next = troveRedirectsCursor.next()
                while next[0] == idx:
                    idxA, targetName, targetBranch, targetFlavor = next
                    targetBranch = versions.VersionFromString(targetBranch)
                    if targetFlavor is not None:
                        targetFlavor = deps.ThawFlavor(targetFlavor)

                    trv.addRedirect(targetName, targetBranch, targetFlavor)
                    next = troveRedirectsCursor.next()

                troveRedirectsCursor.push(next)
            except StopIteration:
                # we're at the end; that's okay
                pass

            self.depTables.get(cu, trv, troveInstanceId)
            self.troveInfoTable.getInfo(cu, trv, troveInstanceId)

            if withFileStreams:
                yield trv, fileContents
            else:
                yield trv

        # yield None for anything not found at the end
        while neededIdx < len(troveInfoList):
            neededIdx += 1
            yield None

    def findFileVersion(self, fileId):
        cu = self.db.cursor()
        cu.execute("SELECT stream FROM FileStreams WHERE fileId=?", (fileId,))

        for (stream,) in cu:
            # if stream is None, it means that this is just a reference
            # to a stream that actually lives in another repository.
            # there is a (unlikely) chance that there is another
            # row inthe table that matches this fileId, since there
            # isn't a unique constraint on fileId.
            if stream is None:
                continue
            return files.ThawFile(cu.frombinary(stream), cu.frombinary(fileId))

        return None

    def iterFilesInTrove(self, troveName, troveVersion, troveFlavor,
                         sortByPath = False, withFiles = False):
        troveInstanceId = self.instances.getInstanceId(
            troveName, troveVersion, troveFlavor)
        sort = ""
        if sortByPath:
            sort = " order by d.dirname, b.basename"
        # iterfiles
        cu = self.db.cursor()
        cu.execute("""
        select fp.pathId, d.dirname, b.basename, fs.fileId,
               tf.versionId, fs.stream
        from TroveFiles as tf
        join FileStreams as fs using (streamId)
        join FilePaths as fp on tf.filePathId = fp.filePathId
        join Dirnames as d on fp.dirnameId = d.dirnameId
        join Basenames as b on fp.basenameId = b.basenameId
        where tf.instanceId = ?
        %s""" % sort, troveInstanceId)

        versionCache = {}
        for (pathId, dirname, basename, fileId, versionId, stream) in cu:
            version = versionCache.get(versionId, None)
            path = os.path.join(cu.frombinary(dirname),
                    cu.frombinary(basename))
            if not version:
                version = self.versionTable.getBareId(versionId)
                versionCache[versionId] = version

            if stream: # try thawing as a sanity check
                files.ThawFile(cu.frombinary(stream), cu.frombinary(fileId))

            if withFiles:
                yield (cu.frombinary(pathId), path, cu.frombinary(fileId),
                       version, cu.frombinary(stream))
            else:
                yield (cu.frombinary(pathId), path, cu.frombinary(fileId),
                       version)

    def getFile(self, pathId, fileId):
        cu = self.db.cursor()
        cu.execute("SELECT stream FROM FileStreams WHERE fileId=?",
                   cu.binary(fileId))
        try:
            stream = cu.next()[0]
        except StopIteration:
            raise errors.FileStreamMissing(fileId)

        if stream is not None:
            return files.ThawFile(cu.frombinary(stream), cu.frombinary(pathId))
        else:
            return None

    def getFiles(self, l):
        # this only needs a list of (pathId, fileId) pairs, but it sometimes
        # gets (pathId, fileId, version) pairs instead (which is what
        # the network repository client uses)
        if not l:
            return {}

        retr = FileRetriever(self.db, self.log)
        d = retr.get(l)
        del retr
        return d

    def _cleanCache(self):
        self.versionIdCache = {}
        self.itemIdCache = {}

    def begin(self, serialize=False):
        self._cleanCache()
        cu = self.db.transaction()
        if serialize:
            schema.lockCommits(self.db)
        return cu

    def rollback(self):
        self._cleanCache()
        return self.db.rollback()

    def _removeTrove(self, name, version, flavor, markOnly = False):
        cu = self.db.cursor()

        cu.execute("""
        SELECT I.instanceId, I.itemId, I.versionId, I.flavorId, I.troveType
        FROM Instances as I
        JOIN Items USING (itemId)
        JOIN Versions ON I.versionId = Versions.versionId
        JOIN Flavors ON I.flavorId = Flavors.flavorId
        WHERE Items.item = ?
          AND Versions.version = ?
          AND Flavors.flavor = ?
        """, (name, version.asString(), flavor.freeze()))

        try:
            instanceId, itemId, versionId, flavorId, troveType = cu.next()
        except StopIteration:
            raise errors.TroveMissing(name, version)

        if troveType == trove.TROVE_TYPE_REMOVED:
            # double removes are okay; they just get ignored
            return []

        assert(troveType == trove.TROVE_TYPE_NORMAL or
               troveType == trove.TROVE_TYPE_REDIRECT)

        # remove all dependencies which are used only by this instanceId
        cu.execute("""
        select prov.depId as depId from
        ( select a.depId as depId from
          ( select depId, instanceId from Provides where instanceId = :instanceId
            union
            select depId, instanceId  from Requires where instanceId = :instanceId
          ) as a
          left join Provides as p on a.depId = p.depId and p.instanceId != a.instanceId
          where p.depId is NULL
        ) as prov
        join
        ( select a.depId as depId from
          ( select depId, instanceId from Provides where instanceId = :instanceId
            union
            select depId, instanceId  from Requires where instanceId = :instanceId
          ) as a
          left join Requires as r on a.depId = r.depId and r.instanceId != a.instanceId
          where r.depId is NULL
        ) as reqs
        on prov.depId = reqs.depId
        """, instanceId = instanceId )
        depsToRemove = [ x[0] for x in cu ]

        cu.execute("DELETE FROM Provides WHERE instanceId = ?", instanceId)
        cu.execute("DELETE FROM Requires WHERE instanceId = ?", instanceId)

        if depsToRemove:
            cu.execute("DELETE FROM Dependencies WHERE depId IN (%s)"
                       % ",".join([ "%d" % x for x in depsToRemove ]))

        # Remove from TroveInfo
        cu.execute("DELETE FROM TroveInfo WHERE instanceId = ?", instanceId)

        # Look for path/pathId combinations we don't need anymore
        cu.execute("""
        select FilePaths.filePathId
        from TroveFiles as TF
        join FilePaths using(filePathId)
        where TF.instanceId = ?
          and not exists (
              select instanceId from TroveFiles as Others
              where Others.filePathId = TF.filePathId
                and Others.instanceId != ? )
        """, (instanceId, instanceId))

        filePathIdsToRemove = [ x[0] for x in cu ]

        # Now remove the files. Gather a list of sha1s of files to remove
        # from the filestore.
        cu.execute("""
        select FileStreams.streamId, FileStreams.sha1
        from TroveFiles as TF
        join FileStreams using(streamId)
        where TF.instanceId = ?
          and not exists (
              select instanceId from TroveFiles as Others
              where Others.streamId = TF.streamId
                and Others.instanceId != ? )
        """, (instanceId, instanceId))

        r = cu.fetchall()
        # if sha1 is None, the file has no contents
        candidateSha1sToRemove = [ cu.frombinary(x[1]) for x in r
                if x[1] is not None ]
        streamIdsToRemove = [ x[0] for x in r ]

        cu.execute("DELETE FROM TroveFiles WHERE instanceId = ?", instanceId)
        if streamIdsToRemove:
            cu.execute("DELETE FROM FileStreams WHERE streamId IN (%s)"
                       % ",".join("%d"%x for x in streamIdsToRemove))

        if filePathIdsToRemove:
            cu.execute("DELETE FROM FilePaths WHERE filePathId IN (%s)"
                       % ",".join("%d"%x for x in filePathIdsToRemove))
            # XXX: these cleanups are more expensive than they're worth, probably
            cu.execute(""" delete from Dirnames where not exists (
                select 1 from FilePaths as fp where fp.dirnameId = Dirnames.dirnameId )
                """)
            cu.execute(""" delete from Basenames where not exists (
                select 1 from FilePaths as fp where fp.basenameId = Basenames.basenameId ) """)

        # we need to double check filesToRemove against other streams which
        # may need the same sha1
        filesToRemove = []
        for sha1 in candidateSha1sToRemove:
            cu.execute("SELECT COUNT(*) FROM FileStreams WHERE sha1=?", cu.binary(sha1))
            if cu.next()[0] == 0:
                filesToRemove.append(sha1)

        # tmpRemovals drives the removal of most of the shared tables
        schema.resetTable(cu, 'tmpRemovals')
        cu.execute("SELECT nodeId, branchId FROM Nodes "
                   "WHERE itemId = ? AND versionId = ?", (itemId, versionId))
        nodeId, branchId = cu.next()
        cu.execute("INSERT INTO tmpRemovals (itemId, versionId, flavorId, branchId) "
                   "VALUES (?, ?, ?, ?)", (itemId, versionId, flavorId, branchId),
                   start_transaction=False)
        # Look for troves which this trove references which aren't present
        # on this repository (if they are present, we shouldn't remove them)
        # and aren't referenced by anything else
        cu.execute("""
        INSERT INTO tmpRemovals (instanceId, itemId, versionId, flavorId, branchId)
        SELECT Instances.instanceId, Instances.itemId, Instances.versionId,
               Instances.flavorId, Nodes.branchId
        FROM TroveTroves
        JOIN Instances ON TroveTroves.includedId = Instances.instanceId
        JOIN Nodes ON
            Instances.itemId = Nodes.itemId AND
            Instances.versionId = Nodes.versionId
        WHERE
         TroveTroves.instanceId = ?
         and Instances.isPresent = ?
         and not exists (select instanceId from TroveTroves as Others
                          where Others.instanceId != ?
                            and Others.includedId = Instances.instanceId )
        """, (instanceId, instances.INSTANCE_PRESENT_MISSING, instanceId),
                   start_transaction=False)
        cu.execute("""
        INSERT INTO tmpRemovals (itemId, flavorId, branchId)
        SELECT TroveRedirects.itemId, TroveRedirects.flavorId,
               TroveRedirects.branchId
        FROM TroveRedirects WHERE TroveRedirects.instanceId = ?
        """, instanceId, start_transaction=False)
        self.db.analyze("tmpRemovals")

        # remove access to troves we're about to remove
        self.ri.deleteInstanceIds("tmpRemovals")
        cu.execute("DELETE FROM TroveTroves WHERE instanceId=?", instanceId)
        cu.execute("DELETE FROM TroveRedirects WHERE instanceId=?", instanceId)
        cu.execute("DELETE FROM Instances WHERE instanceId IN "
                        "(SELECT instanceId FROM tmpRemovals)")
        if markOnly:
            # We don't actually remove anything here; we just mark the trove
            # as removed instead
            cu.execute("UPDATE Instances SET troveType=? WHERE instanceId=?",
                       trove.TROVE_TYPE_REMOVED, instanceId)
            self.latest.update(cu, itemId, branchId, flavorId)
        else:
            self.ri.deleteInstanceId(instanceId)
            cu.execute("DELETE FROM Instances WHERE instanceId = ?", instanceId)

        # look for troves referenced by this one
        schema.resetTable(cu, 'tmpId')
        cu.execute("""
        INSERT INTO tmpId(id)
        SELECT Nodes.nodeId
        FROM tmpRemovals
        JOIN Nodes USING (itemId, versionId)
        LEFT JOIN Instances USING (itemId, versionId)
        WHERE Instances.itemId IS NULL
        """, start_transaction=False)
        self.db.analyze("tmpId")

        # Was this the only Instance for the node?
        cu.execute("""
        DELETE FROM Changelogs
        WHERE Changelogs.nodeId IN (SELECT id FROM tmpId)
        """)

        cu.execute("""
        DELETE FROM Nodes
        WHERE Nodes.nodeId IN (SELECT id FROM tmpId)
        """)

        # Now update the latest table
        self.latest.update(cu, itemId, branchId, flavorId)

        # Delete flavors which are no longer needed
        cu.execute("""
        delete from Flavors
        where flavorId in (
            select r.flavorId
            from tmpRemovals as r
            where not exists (select flavorId from Instances as i where i.flavorId = r.flavorId)
            and not exists (select flavorId from TroveRedirects as tr where tr.flavorId = r.flavorId)
        )""")

        cu.execute("""
        delete from FlavorMap
        where flavorId in (
            select r.flavorId
            from tmpRemovals as r
            where not exists (select flavorId from Flavors as f where f.flavorId = r.flavorId)
        )""")

        # do we need the labelmap entry anymore?
        schema.resetTable(cu, 'tmpId')
        cu.execute("""insert into tmpId(id)
        select lm.labelmapId
        from tmpRemovals as r
        join LabelMap as lm using(itemId, branchId)
        where not exists ( select nodeId from Nodes as n
                           where n.itemId = lm.itemId
                           and n.branchId = lm.branchId ) """, start_transaction=False)
        self.db.analyze("tmpId")
        cu.execute("delete from LabelMap where labelmapId in (select id from tmpId)")

        # do we need these branchIds anymore?
        cu.execute("""
        delete from Branches
        where branchId in (
            select r.branchId
            from tmpRemovals as r
            where not exists (select branchId from Nodes as n where n.branchId = r.branchId)
              and not exists (select branchId from TroveRedirects as tr where tr.branchId = r.branchId)
        )""")

        # XXX It would be nice to narrow this down based on tmpRemovals, but
        # in reality the labels table never gets that big.
        schema.resetTable(cu, 'tmpId')
        cu.execute("""insert into tmpId(id)
        select l.labelId
        from Labels as l
        where not exists (select labelId from LabelMap as lm where lm.labelId = l.labelId)
        and not exists (select labelId from Permissions as p where p.labelId = l.labelId)
        and l.labelId != 0 """, start_transaction=False)
        self.db.analyze("tmpId")
        cu.execute("delete from Labels where labelId in (select id from tmpId)")

        # clean up Versions
        cu.execute("""
        delete from Versions
        where versionId in (
            select r.versionId
            from tmpRemovals as r
            where not exists (select versionId from Instances as i where i.versionId = r.versionId)
            and not exists (select versionId from Nodes as n where n.versionId = r.versionId)
            and not exists (select versionId from TroveFiles as tf where tf.versionId = r.versionId)
        )""")

        # clean up Items
        cu.execute("""
        delete from Items
        where itemId in (
            select r.itemId
            from tmpRemovals as r
            where not exists (select itemId from Instances as i where i.itemId = r.itemId)
            and not exists (select itemId from Nodes as n where n.itemId = r.itemId)
            and not exists (select itemId from TroveRedirects as tr where tr.itemId = r.itemId)
            and not exists (select itemId from Permissions as p where p.itemId = r.itemId)
        )""")

        # XXX what about metadata?
        return filesToRemove

    def markTroveRemoved(self, name, version, flavor):
        self.log(2, name, version, flavor)
        return self._removeTrove(name, version, flavor, markOnly = True)

    def getParentTroves(self, troveList):
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpNVF")
        schema.resetTable(cu, "tmpInstanceId")
        for (n,v,f) in troveList:
            cu.execute("insert into tmpNVF(name,version,flavor) values (?,?,?)",
                       (n,v,f), start_transaction=False)
        self.db.analyze("tmpNVF")
        # get the instanceIds of the parents of what we can find
        cu.execute("""
        insert into tmpInstanceId(instanceId)
        select distinct TroveTroves.instanceId
        from tmpNVF
        join Items on tmpNVF.name = Items.item
        join Versions on tmpNVF.version = Versions.version
        join Flavors on tmpNVF.flavor = Flavors.flavor
        join Instances on
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        join TroveTroves on TroveTroves.includedId = Instances.instanceId
        """, start_transaction=False)
        self.db.analyze("tmpInstanceId")
        # tmpInstanceId now has instanceIds of the parents
        cu.execute("""
        select Items.item, Versions.version, Flavors.flavor
        from tmpInstanceId
        join Instances on tmpInstanceId.instanceId = Instances.instanceId
        join Items on Instances.itemId = Items.itemId
        join Versions on Instances.versionId = Versions.versionId
        join Flavors on Instances.flavorId = Flavors.flavorId
        """)
        return cu.fetchall()

    def commit(self):
        if self.needsCleanup:
            assert(0)
            self.instances.removeUnused()
            self.items.removeUnused()
            self.needsCleanup = False

        if self.versionOps.needsCleanup:
            assert(0)
            self.versionTable.removeUnused()
            self.branchTable.removeUnused()
            self.versionOps.labelMap.removeUnused()
            self.versionOps.needsCleanup = False

        self.db.commit()
        self._cleanCache()

class FileRetriever:
    def __init__(self, db, log = None):
        self.db = db
        self.cu = db.cursor()
        schema.resetTable(self.cu, 'tmpFileId')
        self.log = log or tracelog.getLog(None)

    def get(self, l):
        lookup = range(len(l))

        insertL = []
        for itemId, tup in enumerate(l):
            (pathId, fileId) = tup[:2]
            insertL.append((itemId, self.cu.binary(fileId)))
            lookup[itemId] = (pathId, fileId)

        self.db.bulkload("tmpFileId", insertL, [ "itemId", "fileId" ],
                         start_transaction = False)

        self.db.analyze("tmpFileId")
        self.cu.execute("SELECT itemId, stream FROM tmpFileId "
                        "JOIN FileStreams using (fileId) ")
        d = {}
        for itemId, stream in self.cu:
            pathId, fileId = lookup[itemId]
            if stream is not None:
                f = files.ThawFile(self.cu.frombinary(stream), pathId)
            else:
                f = None
            d[(pathId, fileId)] = f
        schema.resetTable(self.cu, "tmpFileId")
        return d
