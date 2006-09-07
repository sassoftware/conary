#
# Copyright (c) 2004-2006 rPath, Inc.
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

import itertools

from conary import files, metadata, trove, versions, changelog
from conary.deps import deps
from conary.lib import util, tracelog
from conary.local import deptable
from conary.local import versiontable
from conary.repository import errors
from conary.repository.netrepos import instances, items, keytable, flavors
from conary.repository.netrepos import troveinfo, versionops, cltable
from conary.server import schema

class LocalRepVersionTable(versiontable.VersionTable):

    def getId(self, theId, itemId):
        cu = self.db.cursor()
        cu.execute("""
        SELECT Versions.version, Nodes.timeStamps
        FROM Nodes JOIN Versions USING (versionId)
        WHERE Nodes.versionId=? AND Nodes.itemId=?""",
		   theId, itemId)
	try:
	    (s, t) = cu.next()
	    v = self._makeVersion(s, t)
	    return v
	except StopIteration:
            raise KeyError, theId

    def getTimeStamps(self, version, itemId):
        cu = self.db.cursor()
        cu.execute("""
        SELECT timeStamps FROM Nodes
        WHERE versionId = (SELECT versionId from Versions WHERE version=?)
          AND itemId=?""", version.asString(), itemId)
	try:
	    (t,) = cu.next()
	    return [ float(x) for x in t.split(":") ]
	except StopIteration:
            raise KeyError, itemId

class TroveStore:
    def __init__(self, db, log = None):
	self.db = db

	self.items = items.Items(self.db)
	self.flavors = flavors.Flavors(self.db)
        self.branchTable = versionops.BranchTable(self.db)
        self.changeLogs = cltable.ChangeLogTable(self.db)

	self.versionTable = LocalRepVersionTable(self.db)
	self.versionOps = versionops.SqlVersioning(
            self.db, self.versionTable, self.branchTable)
	self.instances = instances.InstanceTable(self.db)

        self.keyTable = keytable.OpenPGPKeyTable(self.db)
        self.depTables = deptable.DependencyTables(self.db)
        self.metadataTable = metadata.MetadataTable(self.db, create = False)
        self.troveInfoTable = troveinfo.TroveInfoTable(self.db)

        self.streamIdCache = {}
	self.needsCleanup = False
        self.log = log or tracelog.getLog(None)

    def __del__(self):
        self.db = self.log = None

    def getLabelId(self, label):
        self.versionOps.labels.getOrAddId(label)

    def getItemId(self, item):
        return self.items.getOrAddId(item)

    def getInstanceId(self, itemId, versionId, flavorId, clonedFromId,
                      isRedirect, isPresent = True):
 	theId = self.instances.get((itemId, versionId, flavorId), None)
	if theId == None:
	    theId = self.instances.addId(itemId, versionId, flavorId,
                                         clonedFromId,
					 isRedirect, isPresent = isPresent)
        # XXX we shouldn't have to do this unconditionally
        if isPresent:
	    self.instances.setPresent(theId, 1)
            self.items.setTroveFlag(itemId, 1)
 	return theId

    def getVersionId(self, version, cache):
	theId = cache.get(version, None)
	if theId:
	    return theId

	theId = self.versionTable.get(version, None)
	if theId == None:
	    theId = self.versionTable.addId(version)

	cache[version] = theId

	return theId

    def getFullVersion(self, item, version):
	"""
	Updates version with full timestamp information.
	"""
	cu = self.db.cursor()
	cu.execute("""
	    SELECT timeStamps FROM Nodes WHERE
		itemId=(SELECT itemId FROM Items WHERE item=?) AND
		versionId=(SELECT versionId FROM Versions WHERE version=?)
	""", item, version.asString())

	timeStamps = cu.fetchone()[0]
	version.setTimeStamps([float(x) for x in timeStamps.split(":")])

    def createTroveBranch(self, troveName, branch):
	itemId = self.getItemId(troveName)
	branchId = self.versionOps.createBranch(itemId, branch)

    def troveLatestVersion(self, troveName, branch):
	"""
	Returns None if no versions of troveName exist on the branch.
	"""
	cu = self.db.cursor()
	cu.execute("""
	    SELECT version, timeStamps FROM
		(SELECT itemId AS AitemId, branchId as AbranchId FROM labelMap
		    WHERE itemId=(SELECT itemId from Items
				WHERE item=?)
		    AND branchId=(SELECT branchId FROM Branches
				WHERE branch=?)
		) INNER JOIN Latest ON
		    AitemId=Latest.itemId AND AbranchId=Latest.branchId
		INNER JOIN Nodes ON
		    AitemId=Nodes.itemId AND Latest.versionId=Nodes.versionId
		INNER JOIN Versions ON
		    Nodes.versionId = versions.versionId
                ORDER BY
                    Nodes.finalTimeStamp
                LIMIT 1
	""", troveName, branch.asString())
        try:
	    (verStr, timeStamps) = cu.next()
            return versions.VersionFromString(verStr,
		    timeStamps = [ float(x) for x in timeStamps.split(":") ] )
        except StopIteration:
            raise KeyError, (troveName, branch)

    def getTroveFlavors(self, troveDict):
	cu = self.db.cursor()
	vMap = {}
	outD = {}
	# I think we might be better of intersecting subqueries rather
	# then using all of the and's in this join

        schema.resetTable(cu, 'itf')

        for troveName in troveDict.keys():
            outD[troveName] = {}
            for version in troveDict[troveName]:
                outD[troveName][version] = []
                versionStr = version.asString()
                vMap[versionStr] = version
                cu.execute("INSERT INTO itf VALUES (?, ?, ?)",
                           (troveName, versionStr, versionStr),
                           start_transaction = False)

        cu.execute("""
            SELECT aItem, fullVersion, Flavors.flavor FROM
                (SELECT Items.itemId AS aItemId,
                        versions.versionId AS aVersionId,
                        Items.item AS aItem,
                        fullVersion FROM
                    itf INNER JOIN Items ON itf.item = Items.item
                        INNER JOIN versions ON itf.version = versions.version) as ItemVersions
                INNER JOIN instances ON
                    aItemId = instances.itemId AND
                    aVersionId = instances.versionId
                INNER JOIN flavors ON
                    instances.flavorId = flavors.flavorId
                ORDER BY aItem, fullVersion
        """)

        for (item, verString, flavor) in cu:
            ver = vMap[verString]
            outD[item][ver].append(flavor)

	return outD

    def iterTroveNames(self):
        cu = self.db.cursor()
        cu.execute("SELECT DISTINCT Items.item as item "
                   " FROM Instances JOIN Items USING(itemId) "
                   " WHERE Instances.isPresent=1 ORDER BY item");

        for (item,) in cu:
            yield item

    def addTrove(self, trv):
	cu = self.db.cursor()

        schema.resetTable(cu, 'NewFiles')
        schema.resetTable(cu, 'NeededFlavors')

	self.fileVersionCache = {}
	return (cu, trv)

    def addTroveDone(self, troveInfo):
	versionCache = {}
	(cu, trv) = troveInfo

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

	# does this version already exist (for another flavor?)
	newVersion = False
	troveVersionId = self.versionTable.get(troveVersion, None)
	if troveVersionId is not None:
	    nodeId = self.versionOps.nodes.getRow(troveItemId,
						  troveVersionId, None)

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

	flavorIndex = {}
	for flavor in flavorsNeeded.iterkeys():
	    flavorIndex[flavor.freeze()] = flavor
	    cu.execute("INSERT INTO NeededFlavors VALUES(?)", flavor.freeze())

	del flavorsNeeded

	# it seems like there must be a better way to do this, but I can't
	# figure it out. I *think* inserting into a view would help, but I
	# can't with sqlite.

	cu.execute("""SELECT NeededFlavors.flavor FROM
			NeededFlavors LEFT OUTER JOIN Flavors ON
			    NeededFlavors.flavor = Flavors.Flavor
			WHERE Flavors.flavorId is NULL""")
        # make a list of the flavors we're going to create.  Add them
        # after we have retreived all of the rows from this select
        l = []
	for (flavorStr,) in cu:
            l.append(flavorIndex[flavorStr])
        for flavor in l:
	    self.flavors.createFlavor(flavor)

	flavors = {}
	cu.execute("""SELECT Flavors.flavor, Flavors.flavorId FROM
			NeededFlavors INNER JOIN Flavors ON
			NeededFlavors.flavor = Flavors.flavor""")
	for (flavorStr, flavorId) in cu:
	    flavors[flavorIndex[flavorStr]] = flavorId

	del flavorIndex

	if troveFlavor is not None:
	    troveFlavorId = flavors[troveFlavor]
	else:
	    troveFlavorId = 0

	if troveVersionId is None or nodeId is None:
	    (nodeId, troveVersionId) = self.versionOps.createVersion(
                troveItemId, troveVersion, troveFlavorId, sourceName)
	    newVersion = True

	    if trv.getChangeLog() and trv.getChangeLog().getName():
		self.changeLogs.add(nodeId, trv.getChangeLog())
            updateLatest = False
        else:
            updateLatest = True

	# the instance may already exist (it could be referenced by a package
	# which has already been added)
	troveInstanceId = self.getInstanceId(troveItemId, troveVersionId,
					     troveFlavorId, clonedFromId,
                                             trv.isRedirect(),
                                             isPresent = True)
        assert(cu.execute("SELECT COUNT(*) from TroveTroves WHERE "
                          "instanceId=?", troveInstanceId).next()[0] == 0)

        if updateLatest:
            # this name/version already exists, so this must be a new
            # flavor. update the latest table as needed
            troveBranchId = self.branchTable[troveVersion.branch()]
            cu.execute("""
            DELETE FROM Latest WHERE branchId=? AND itemId=? AND flavorId=?
            """, troveBranchId, troveItemId, troveFlavorId)
            cu.execute("""
            INSERT INTO Latest
                (itemId, branchId, flavorId, versionId)
            SELECT %d, %d, %d, Instances.versionId
            FROM
                Instances JOIN Nodes USING(itemId, versionId)
            WHERE
                Instances.itemId=?
            AND Instances.flavorId=?
            AND Nodes.branchId=?
            ORDER BY finalTimestamp DESC
            LIMIT 1
            """ %(troveItemId, troveBranchId, troveFlavorId),
                       (troveItemId, troveFlavorId, troveBranchId))

        self.depTables.add(cu, trv, troveInstanceId)

        # Fold NewFiles into FileStreams
        #
        # NewFiles can contain duplicate entries for the same fileId,
        # (with stream being NULL or not), so the FileStreams update
        # is happening in three steps:
        # 1. Update existing fileIds (while avoiding  a full table scan)
        # 2. Insert new fileIds with non-NULL streams
        # 3. Insert new fileIds that might have NULL streams.

        # Note: writing the next two steps in a single query causes
        # very slow full table scans on FileStreams. Don't get fancy.

        # get the common entries we're gonna update
        cu2 = self.db.cursor()
        cu2.execute("""
        SELECT NewFiles.fileId, NewFiles.stream
        FROM NewFiles
        JOIN FileStreams USING(fileId)
        WHERE FileStreams.stream IS NULL
        AND NewFiles.stream IS NOT NULL
        """)
        # Note: PostgreSQL and MySQL have support for non-SQL standard
        # multi-table updates that we could use to do this in one step.
        # This two step should work on everything though --gafton
        for (fileId, stream) in cu2:
            cu.execute("UPDATE FileStreams SET stream = ? "
                       "WHERE fileId = ?", (stream, fileId))
        del cu2

        # select the new non-NULL streams out of NewFiles and Insert them in FileStreams
        cu.execute("""
        INSERT INTO FileStreams (fileId, stream)
        SELECT DISTINCT NF.fileId, NF.stream
        FROM NewFiles AS NF
        LEFT OUTER JOIN FileStreams AS FS USING(fileId)
        WHERE FS.fileId IS NULL
          AND NF.stream IS NOT NULL
        """)
        # now insert the other fileIds
        # select the new non-NULL streams out of NewFiles and Insert them in FileStreams
        cu.execute("""
        INSERT INTO FileStreams (fileId, stream)
        SELECT DISTINCT NF.fileId, NF.stream
        FROM NewFiles AS NF
        LEFT OUTER JOIN FileStreams AS FS USING(fileId)
        WHERE FS.fileId IS NULL
        """)

        # create the TroveFiles links for this trove's files.
        cu.execute("""
        INSERT INTO TroveFiles
            (instanceId, streamId, versionId, pathId, path)
        SELECT %d, FS.streamId, NF.versionId, NF.pathId, NF.path
        FROM NewFiles as NF
        JOIN FileStreams as FS USING(fileId)
        """ % (troveInstanceId,))

        # iterate over both strong and weak troves, and set weakFlag to
        # indicate which kind we're looking at when
        for ((name, version, flavor), weakFlag) in itertools.chain(
                itertools.izip(trv.iterTroveList(strongRefs = True,
                                                   weakRefs   = False),
                               itertools.repeat(0)),
                itertools.izip(trv.iterTroveList(strongRefs = False,
                                                   weakRefs   = True),
                               itertools.repeat(schema.TROVE_TROVES_WEAKREF))):
	    itemId = self.getItemId(name)

	    if flavor is not None:
		flavorId = flavors[flavor]
	    else:
		flavorId = 0

	    # make sure the versionId and nodeId exists for this (we need
	    # a nodeId, or the version doesn't get timestamps)
	    versionId = self.versionTable.get(version, None)

            # sanity check - version/flavor of components must match the
            # version/flavor of the package
            assert(trv.isRedirect() or
                            (not isPackage or versionId == troveVersionId))
            assert(trv.isRedirect() or
                            (not isPackage or flavorId == troveFlavorId))

	    if versionId is not None:
		nodeId = self.versionOps.nodes.getRow(itemId,
						      versionId, None)
		if nodeId is None:
		    (nodeId, versionId) = self.versionOps.createVersion(
						    itemId, version,
						    flavorId, sourceName,
						    updateLatest = False)
		del nodeId
            else:
                (nodeId, versionId) = self.versionOps.createVersion(
                                                itemId, version,
                                                flavorId, sourceName,
                                                updateLatest = False)

	    instanceId = self.getInstanceId(itemId, versionId, flavorId,
                                            clonedFromId,
                                            trv.isRedirect(),
                                            isPresent = False)

            flags = weakFlag
            if trv.includeTroveByDefault(name, version, flavor):
                flags |= schema.TROVE_TROVES_BYDEFAULT

            if trv.includeTroveByDefault(name, version, flavor):
                byDefault = 1
            else:
                byDefault = 0

            cu.execute("INSERT INTO TroveTroves "
                       "(instanceId, includedId, flags) "
                       "VALUES (?, ?, ?)", (
                troveInstanceId, instanceId, flags))

        self.troveInfoTable.addInfo(cu, trv, troveInstanceId)

        # now add the redirects
        cu.execute("DELETE FROM NewRedirects")
        for (name, branch, flavor) in trv.iterRedirects():
            if flavor is None:
                frz = None
            else:
                frz = flavor.freeze()
            cu.execute("INSERT INTO NewRedirects (item, branch, flavor) "
                       "VALUES (?, ?, ?)", name, str(branch), frz)

        cu.execute("""
                INSERT INTO Items (item)
                    SELECT NewRedirects.item FROM
                        NewRedirects LEFT OUTER JOIN Items USING (item)
                        WHERE Items.itemId is NULL
                   """)

        cu.execute("""
                INSERT INTO Branches (branch)
                    SELECT NewRedirects.branch FROM
                        NewRedirects LEFT OUTER JOIN Branches USING (branch)
                        WHERE Branches.branchId is NULL
                   """)

        cu.execute("""
                INSERT INTO Flavors (flavor)
                    SELECT NewRedirects.flavor FROM
                        NewRedirects LEFT OUTER JOIN Flavors USING (flavor)
                        WHERE 
                            Flavors.flavor is not NULL 
                            AND Flavors.flavorId is NULL
                   """)

        cu.execute("""
                INSERT INTO TroveRedirects 
                    (instanceId, itemId, branchId, flavorId)
                    SELECT %d, itemId, branchId, flavorId FROM
                        NewRedirects JOIN Items USING (item)
                        JOIN Branches ON
                            NewRedirects.branch = Branches.branch
                        LEFT OUTER JOIN Flavors ON
                            NewRedirects.flavor = Flavors.flavor
        """ % troveInstanceId)

	del self.fileVersionCache

    def updateMetadata(self, troveName, branch, shortDesc, longDesc,
                    urls, licenses, categories, source, language):
        cu = self.db.cursor()

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

    def getTrove(self, troveName, troveVersion, troveFlavor, withFiles = True):
	iter = self.iterTroves(( (troveName, troveVersion, troveFlavor), ),
                               withFiles = withFiles)
        trv = [ x for x in iter ][0]

        if trv is None:
	    raise errors.TroveMissing(troveName, troveVersion)

        return trv

    def iterTroves(self, troveInfoList, withFiles = True, withFileStreams = False):
	cu = self.db.cursor()

        schema.resetTable(cu, 'gtl')
        schema.resetTable(cu, 'gtlInst')

        for idx, info in enumerate(troveInfoList):
            flavorStr = "'%s'" % info[2].freeze()
            cu.execute("INSERT INTO gtl VALUES (?, ?, ?, %s)" %(flavorStr,),
                       idx, info[0], info[1].asString(),
                       start_transaction = False)

        cu.execute("""SELECT %(STRAIGHTJOIN)s gtl.idx, I.instanceId, 
                             I.isRedirect, Nodes.timeStamps, Changelogs.name,
                             ChangeLogs.contact, ChangeLogs.message
                            FROM
                                gtl, Items, Versions, Flavors, Instances as I,
                                Nodes
                            LEFT OUTER JOIN ChangeLogs ON
                                Nodes.nodeId = ChangeLogs.nodeId
                            WHERE
                                Items.item = gtl.name AND
                                Versions.version = gtl.version AND
                                Flavors.flavor = gtl.flavor AND
                                I.itemId = Items.itemId AND
                                I.versionId = Versions.versionId AND
                                I.flavorId = flavors.flavorId AND
                                I.itemId = Nodes.itemId AND
                                I.versionId = Nodes.versionId
                            ORDER BY
                                gtl.idx""" % self.db.keywords)

        troveIdList = [ x for x in cu ]

        for singleTroveIds in troveIdList:
            cu.execute("INSERT INTO gtlInst VALUES (?, ?)",
                       singleTroveIds[0], singleTroveIds[1],
                       start_transaction = False)
        troveTrovesCursor = self.db.cursor()
        troveTrovesCursor.execute("""
                        SELECT %(STRAIGHTJOIN)s idx, item, version, flavor, 
                               flags, Nodes.timeStamps
                        FROM
                            gtlInst, TroveTroves, Instances, Items,
                            Versions, Flavors, Nodes
                        WHERE
                            gtlInst.instanceId = TroveTroves.instanceId AND
                            TroveTroves.includedId = Instances.instanceId AND
                            Instances.itemId = Items.itemId AND
                            Instances.versionId = versions.versionId AND
                            Instances.flavorId = Flavors.flavorId AND
                            Instances.itemId = Nodes.itemId AND
                            Instances.versionId = Nodes.versionId
                        ORDER BY
                            gtlInst.idx
                   """ % self.db.keywords)

        troveTrovesCursor = util.PeekIterator(troveTrovesCursor)

        troveFilesCursor = self.db.cursor()
	if withFileStreams:
            troveFilesCursor.execute("""
                        SELECT idx, pathId, path, version, fileId, stream
                        FROM
                            gtlInst, TroveFiles, Versions, FileStreams
                        WHERE
                            gtlInst.instanceId = TroveFiles.instanceId AND
                            TroveFiles.versionId = versions.versionId AND
                            TroveFiles.streamId = FileStreams.streamId
                        ORDER BY
                            gtlInst.idx
                       """)
            troveFilesCursor = util.PeekIterator(troveFilesCursor)
        elif withFiles:
            troveFilesCursor.execute("""
                        SELECT idx, pathId, path, version, fileId, NULL
                        FROM
                            gtlInst, TroveFiles, Versions, FileStreams
                        WHERE
                            gtlInst.instanceId = TroveFiles.instanceId AND
                            TroveFiles.versionId = versions.versionId AND
                            TroveFiles.streamId = FileStreams.streamId
                        ORDER BY
                            gtlInst.idx
                       """)
            troveFilesCursor = util.PeekIterator(troveFilesCursor)
        else:
            troveFilesCursor = util.PeekIterator(iter(()))

        troveRedirectsCursor = self.db.cursor()
        troveRedirectsCursor.execute("""
                    SELECT idx, item, branch, flavor 
                    FROM gtlInst 
                        JOIN TroveRedirects using (instanceId)
                        JOIN Items USING (itemId)
                        JOIN Branches ON
                            TroveRedirects.branchId = Branches.branchId
                        LEFT OUTER JOIN Flavors ON
                            TroveRedirects.flavorId = Flavors.flavorId
                    ORDER BY
                        gtlInst.idx
                    """)
        troveRedirectsCursor = util.PeekIterator(troveRedirectsCursor)


        neededIdx = 0
        while troveIdList:
            (idx, troveInstanceId, isRedirect, timeStamps,
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

            v = singleTroveInfo[1].copy()
            v.setTimeStamps([ float(x) for x in timeStamps.split(":") ])

            trv = trove.Trove(singleTroveInfo[0], v,
                              singleTroveInfo[2], changeLog,
                              isRedirect = isRedirect,
                              setVersion = False)

            try:
                while troveTrovesCursor.peek()[0] == idx:
                    idxA, name, version, flavor, flags, timeStamps = \
                                                troveTrovesCursor.next()
                    ts = [ float(x) for x in timeStamps.split(":") ]
                    version = versions.VersionFromString(version, timeStamps=ts)
                    flavor = deps.ThawFlavor(flavor)
                    byDefault = (flags & schema.TROVE_TROVES_BYDEFAULT) != 0
                    weakRef = (flags & schema.TROVE_TROVES_WEAKREF) != 0
                    trv.addTrove(name, version, flavor, byDefault = byDefault,
                                 weakRef = weakRef)
            except StopIteration:
                # we're at the end; that's okay
                pass

	    fileContents = {}
            try:
                while troveFilesCursor.peek()[0] == idx:
                    idxA, pathId, path, versionId, fileId, stream = \
                            troveFilesCursor.next()
                    version = versions.VersionFromString(versionId)
                    trv.addFile(cu.frombinary(pathId), path, version, 
                                cu.frombinary(fileId))
		    if stream is not None:
			fileContents[fileId] = stream
            except StopIteration:
                # we're at the end; that's okay
                pass

            try:
                while troveRedirectsCursor.peek()[0] == idx:
                    idxA, targetName, targetBranch, targetFlavor = \
                            troveRedirectsCursor.next()
                    targetBranch = versions.VersionFromString(targetBranch)
                    if targetFlavor is not None:
                        targetFlavor = deps.ThawFlavor(targetFlavor)

                    trv.addRedirect(targetName, targetBranch, targetFlavor)
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
	if sortByPath:
	    sort = " ORDER BY path"
	else:
	    sort =""
	cu = self.db.cursor()

	troveItemId = self.items[troveName]
	troveVersionId = self.versionTable[troveVersion]
	troveFlavorId = self.flavors[troveFlavor]
	troveInstanceId = self.instances[(troveItemId, troveVersionId,
					  troveFlavorId)]
	versionCache = {}

	cu.execute("SELECT pathId, path, fileId, versionId, stream FROM "
		   "TroveFiles JOIN FileStreams USING (streamId)"
		   "WHERE instanceId = ? %s" %sort,
		   troveInstanceId)

	versionCache = {}
	for (pathId, path, fileId, versionId, stream) in cu:
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getBareId(versionId)
		versionCache[versionId] = version

            if stream:
                fObj = files.ThawFile(cu.frombinary(stream), 
                                      cu.frombinary(fileId))

	    if withFiles:
		yield (cu.frombinary(pathId), path, cu.frombinary(fileId), 
                       version, cu.frombinary(stream))
	    else:
		yield (cu.frombinary(pathId), path, cu.frombinary(fileId), 
                       version)

    def addFile(self, troveInfo, pathId, fileObj, path, fileId, fileVersion,
                fileStream = None):
	cu = troveInfo[0]
	versionId = self.getVersionId(fileVersion, self.fileVersionCache)

        if fileObj or fileStream:
            if fileStream is None:
                fileStream = fileObj.freeze()
	    cu.execute("INSERT INTO NewFiles VALUES(?, ?, ?, ?, ?)",
		       (cu.binary(pathId), versionId, cu.binary(fileId), 
                        cu.binary(fileStream), path))
	else:
	    cu.execute("INSERT INTO NewFiles VALUES(?, ?, ?, NULL, ?)",
		       (cu.binary(pathId), versionId, cu.binary(fileId), path))

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
        retr = FileRetriever(self.db, self.log)
        d = retr.get(l)
        del retr
        return d

    def resolveRequirements(self, label, depSetList, troveList=[]):
        return self.depTables.resolve(label, depSetList, troveList=troveList)

    def begin(self):
        return self.db.transaction()

    def rollback(self):
        return self.db.rollback()

    def commit(self):
	if self.needsCleanup:
	    assert(0)
	    self.instances.removeUnused()
	    self.fileStreams.removeUnusedStreams()
	    self.items.removeUnused()
	    self.needsCleanup = False

	if self.versionOps.needsCleanup:
	    assert(0)
	    self.versionTable.removeUnused()
	    self.branchTable.removeUnused()
	    self.versionOps.labelMap.removeUnused()
	    self.versionOps.needsCleanup = False

	self.db.commit()

class FileRetriever:

    def __init__(self, db, log = None):
        self.cu = db.cursor()
        schema.resetTable(self.cu, 'getFilesTbl')
        self.log = log or tracelog.getLog(None)

    def get(self, l):
	self.log(3, "start FileRetriever inserts")
        lookup = range(len(l))
        for itemId, tup in enumerate(l):
            (pathId, fileId) = tup[:2]
            self.cu.execute("INSERT INTO getFilesTbl VALUES(?, ?)",
                            (itemId, self.cu.binary(fileId)),
                            start_transaction = False)
            lookup[itemId] = (pathId, fileId)

	self.log(3, "start FileRetriever select")
        self.cu.execute("""
            SELECT itemId, stream FROM getFilesTbl INNER JOIN FileStreams ON
                    getFilesTbl.fileId = FileStreams.fileId
        """)

        d = {}
        for itemId, stream in self.cu:
            pathId, fileId = lookup[itemId]
            if stream is not None:
                f = files.ThawFile(self.cu.frombinary(stream), pathId)
            else:
                f = None
            d[(pathId, fileId)] = f
        self.cu.execute("DELETE FROM getFilesTbl", start_transaction = False)

	self.log(3, "stop FileRetriever")

        return d
