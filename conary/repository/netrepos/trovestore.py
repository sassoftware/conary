#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
# 

import changelog
import cltable
import copy
from conary.deps import deps
from local import deptable
import instances
import items
import keytable
from conary import files
import flavors
from conary import metadata
import sqlite3
from conary import trove
from conary import trovefiles
import versionops
from conary import versions

from local from conary import troveinfo
from local import versiontable
from local import sqldb

from lib.tracelog import logMe

class LocalRepVersionTable(versiontable.VersionTable):

    def getId(self, theId, itemId):
        cu = self.db.cursor()
        cu.execute("""SELECT version, timeStamps FROM Versions
		      INNER JOIN Nodes ON Versions.versionId = Nodes.versionId
		      WHERE Versions.versionId=? AND Nodes.itemId=?""", 
		   theId, itemId)
	try:
	    (s, t) = cu.next()
	    v = self._makeVersion(s, t)
	    return v
	except StopIteration:
            raise KeyError, theId

    def getTimeStamps(self, version, itemId):
        cu = self.db.cursor()
        cu.execute("""SELECT timeStamps FROM Nodes
		      WHERE versionId=(
			SELECT versionId from Versions WHERE version=?
		      )
		      AND itemId=?""", version.asString(), itemId)
	try:
	    (t,) = cu.next()
	    return [ float(x) for x in t.split(":") ]
	except StopIteration:
            raise KeyError, itemId

class TroveStore:

    def _createSchema(self, cu):
        cu.execute("SELECT COUNT(*) FROM sqlite_master WHERE "
                   "name='TroveTroves'")
        if cu.next()[0] == 0:
            cu.execute("""
            CREATE TABLE TroveTroves(
                instanceId      INTEGER, 
                includedId      INTEGER,
                byDefault       BOOLEAN,
                CONSTRAINT TroveTroves_instanceId_fk
                    FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                    ON DELETE RESTRICT ON UPDATE CASCADE,
                CONSTRAINT TroveTroves_includedId_fk
                    FOREIGN KEY (includedId) REFERENCES Instances(instanceId)
                    ON DELETE RESTRICT ON UPDATE CASCADE,
                CONSTRAINT TroveTroves_instance_included_uq
                    UNIQUE(instanceId, includedId)
            )""")
            # ideally we would attempt to create a unique index on (instance, included)
            # for sqlite as well for integrity checking, but sqlite's performance will hurt            
	    cu.execute("CREATE INDEX TroveTrovesInstanceIdx ON TroveTroves(instanceId)")
	    # this index is so we can quickly tell what troves are needed
	    # by another trove
	    cu.execute("CREATE INDEX TroveTrovesIncludedIdx ON TroveTroves(includedId)")

    def __init__(self, db):
	self.db = db

	cu = self.db.cursor()
	#cu.execute("PRAGMA temp_store = MEMORY", start_transaction = False)
				 
        self.begin()
	self._createSchema(cu)

        # Order matters!
        # Create the simple (leaf) tables first, and then the ones
        # that have foreign keys
	trovefiles.TroveFiles(self.db)
	instances.FileStreams(self.db)

	self.items = items.Items(self.db)
	self.flavors = flavors.Flavors(self.db)
        self.branchTable = versionops.BranchTable(self.db)
        self.changeLogs = cltable.ChangeLogTable(self.db)

	self.versionTable = LocalRepVersionTable(self.db)
	self.versionOps = versionops.SqlVersioning(self.db, self.versionTable,
                                                   self.branchTable)
	self.instances = instances.InstanceTable(self.db)

        self.keyTable = keytable.OpenPGPKeyTable(self.db)
        flavors.FlavorScores(self.db)
        self.depTables = deptable.DependencyTables(self.db)
        self.metadataTable = metadata.MetadataTable(self.db)
        self.troveInfoTable = troveinfo.TroveInfoTable(self.db)

        sqldb._doAnalyze(self.db)
        self.db.commit()

	self.streamIdCache = {}
	self.needsCleanup = False

    def __del__(self):
        try:
            self.db.close()
        except sqlite3.ProgrammingError:
            pass
        del self.db

    def getLabelId(self, label):
        self.versionOps.labels.getOrAddId(label)
        
    def getItemId(self, item):
        return self.items.getOrAddId(item)

    def getInstanceId(self, itemId, versionId, flavorId, isRedirect,
                      isPresent = True):
 	theId = self.instances.get((itemId, versionId, flavorId), None)
	if theId == None:
	    theId = self.instances.addId(itemId, versionId, flavorId,
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
	cu.execute("""
	    CREATE TEMPORARY TABLE itf(item STRING, version STRING,
				      fullVersion STRING)
	""", start_transaction = False)
        try:
            for troveName in troveDict.keys():
                outD[troveName] = {}
                for version in troveDict[troveName]:
                    outD[troveName][version] = []
                    versionStr = version.asString()
                    vMap[versionStr] = version
                    cu.execute("""
                        INSERT INTO itf VALUES (?, ?, ?)
                    """, 
                    (troveName, versionStr, versionStr), start_transaction = False)

            cu.execute("""
                SELECT aItem, fullVersion, Flavors.flavor FROM
                    (SELECT Items.itemId AS aItemId, 
                            versions.versionId AS aVersionId,
                            Items.item AS aItem,
                            fullVersion FROM
                        itf INNER JOIN Items ON itf.item = Items.item
                            INNER JOIN versions ON itf.version = versions.version)
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
        finally:
            cu.execute("DROP TABLE itf", start_transaction = False)

	return outD

    def iterTroveNames(self):
        cu = self.db.cursor()
        cu.execute("SELECT DISTINCT Items.item as item "
                   " FROM Instances JOIN Items USING(itemId) "
                   " WHERE Instances.isPresent=1 ORDER BY item");

        for (item,) in cu:
            yield item

    def addTrove(self, trove):
	cu = self.db.cursor()

	cu.execute("""
	    CREATE TEMPORARY TABLE NewFiles(pathId BINARY,
					    versionId INTEGER,
					    fileId BINARY,
					    stream BINARY,
					    path STRING)
	""")

	self.fileVersionCache = {}
	
	return (cu, trove)

    def addTroveDone(self, troveInfo):
	versionCache = {}
	(cu, trove) = troveInfo

        logMe(3, trove)
        
	troveVersion = trove.getVersion()
	troveItemId = self.getItemId(trove.getName())

        isPackage = (not trove.getName().startswith('group') and
                     not trove.getName().startswith('fileset') and
                     ':' not in trove.getName())

	# does this version already exist (for another flavor?)
	newVersion = False
	troveVersionId = self.versionTable.get(troveVersion, None)
	if troveVersionId is not None:
	    nodeId = self.versionOps.nodes.getRow(troveItemId, 
						  troveVersionId, None)

	troveFlavor = trove.getFlavor()

	# start off by creating the flavors we need; we could combine this
	# to some extent with the file table creation below, but there are
	# normally very few flavors per trove so this probably better
	flavorsNeeded = {}
	if troveFlavor:
	    flavorsNeeded[troveFlavor] = True

	for (name, version, flavor) in trove.iterTroveList():
	    if flavor:
		flavorsNeeded[flavor] = True

	flavorIndex = {}
	cu.execute("CREATE TEMPORARY TABLE NeededFlavors(flavor STR)")
	for flavor in flavorsNeeded.iterkeys():
	    flavorIndex[flavor.freeze()] = flavor
	    cu.execute("INSERT INTO NeededFlavors VALUES(?)", 
		       flavor.freeze())
	    
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
	cu.execute("DROP TABLE NeededFlavors")

	if troveFlavor:
	    troveFlavorId = flavors[troveFlavor]
	else:
	    troveFlavorId = 0

	if troveVersionId is None or nodeId is None:
	    (nodeId, troveVersionId) = self.versionOps.createVersion(
					    troveItemId, troveVersion,
                                            troveFlavorId)
	    newVersion = True

	    if trove.getChangeLog() and trove.getChangeLog().getName():
		self.changeLogs.add(nodeId, trove.getChangeLog())
            updateLatest = False
        else:
            updateLatest = True

	# the instance may already exist (it could be referenced by a package
	# which has already been added)
	troveInstanceId = self.getInstanceId(troveItemId, troveVersionId, 
					     troveFlavorId,
                                             trove.isRedirect(),
                                             isPresent = True)
        assert(cu.execute("SELECT COUNT(*) from TroveTroves WHERE "
                          "instanceId=?", troveInstanceId).next()[0] == 0)

        if updateLatest:
            # this name/version already exists, so this must be a new
            # flavor. update the latest table as needed
            troveBranchId = self.branchTable[troveVersion.branch()]
            cu.execute("DELETE FROM Latest WHERE branchId=? AND itemId=? "
                       "AND flavorId=?", troveBranchId, troveItemId, 
                       troveFlavorId)
            cu.execute("""INSERT INTO Latest 
                            SELECT ?, ?, ?, Instances.versionId 
                                FROM Instances INNER JOIN Nodes ON
                                    Instances.itemId = Nodes.itemId AND
                                    Instances.versionId = Nodes.versionId
                                WHERE 
                                    Instances.itemId=? AND 
                                    Instances.flavorId=? AND
                                    Nodes.branchId=?
                                ORDER BY 
                                    finalTimestamp DESC 
                                LIMIT 1
                       """, troveItemId, troveBranchId, troveFlavorId,
                       troveItemId, troveFlavorId, troveBranchId)

        self.depTables.add(cu, trove, troveInstanceId)

        cu.execute("""
	    INSERT INTO FileStreams SELECT DISTINCT NULL,
					   NewFiles.fileId,
					   NewFiles.stream
		FROM NewFiles LEFT OUTER JOIN FileStreams ON
		    NewFiles.fileId = FileStreams.fileId 
		WHERE FileStreams.streamId is NULL
                """)

        # this update the streamId for streams where streamId is NULL
        # (because they were originally added from a distributed branch)
        # for items whose stream is present in NewFiles
        cu.execute("""
            INSERT OR REPLACE INTO FileStreams 
                SELECT FileStreams.streamId, FileStreams.fileId, 
                       NewFiles.stream
                FROM NewFiles INNER JOIN FileStreams ON
                    NewFiles.fileId = FileStreams.FileId
                WHERE
                    FileStreams.stream IS NULL AND
                    NewFiles.stream IS NOT NULL
        """)

        cu.execute("""
	    INSERT INTO TroveFiles SELECT ?,
					  FileStreams.streamId,
					  NewFiles.versionId,
					  NewFiles.pathId,
					  NewFiles.path
		FROM NewFiles INNER JOIN FileStreams ON
                    NewFiles.fileId == FileStreams.fileId
                    """, troveInstanceId)
        cu.execute("DROP TABLE NewFiles")

	for (name, version, flavor) in trove.iterTroveList():
	    itemId = self.getItemId(name)

	    if flavor:
		flavorId = flavors[flavor]
	    else:
		flavorId = 0

	    # make sure the versionId and nodeId exists for this (we need
	    # a nodeId, or the version doesn't get timestamps)
	    versionId = self.versionTable.get(version, None)

            # sanity check - version/flavor of components must match the
            # version/flavor of the package
            assert(trove.isRedirect() or 
                            (not isPackage or versionId == troveVersionId))
            assert(trove.isRedirect() or 
                            (not isPackage or flavorId == troveFlavorId))

	    if versionId is not None:
		nodeId = self.versionOps.nodes.getRow(itemId, 
						      versionId, None)
		if nodeId is None:
		    (nodeId, versionId) = self.versionOps.createVersion(
						    itemId, version,
						    flavorId, 
						    updateLatest = False)
		del nodeId
            else:
                (nodeId, versionId) = self.versionOps.createVersion(
                                                itemId, version,
                                                flavorId, 
                                                updateLatest = False)

	    instanceId = self.getInstanceId(itemId, versionId, flavorId,
                                            trove.isRedirect(),
                                            isPresent = False)
            cu.execute("INSERT INTO TroveTroves VALUES(?, ?, ?)",
	               troveInstanceId, instanceId,
                       trove.includeTroveByDefault(name, version, flavor))

        self.troveInfoTable.addInfo(cu, trove, troveInstanceId)

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
                version = versions.VersionFromString(latestVersion)
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

    def hasTrove(self, troveName, troveVersion = None, troveFlavor = 0):
        logMe(3, troveName, troveVersion, troveFlavor)
        
	if not troveVersion:
	    return self.items.has_key(troveName)
	
	assert(troveFlavor is not 0)

	troveItemId = self.items.get(troveName, None)
	if troveItemId is None:
	    return False

	troveVersionId = self.versionTable.get(troveVersion, None)
	if troveVersionId is None:
            # there is no version in the versionId for this version
            # in the table, so we can't have a trove with that version
            return False

	troveFlavorId = self.flavors.get(troveFlavor, 0)
	if troveFlavorId == 0:
            return False
	
	return self.instances.isPresent((troveItemId, troveVersionId, 
					 troveFlavorId))

    def getTrove(self, troveName, troveVersion, troveFlavor, withFiles = True):
	return self._getTrove(troveName = troveName, 
			      troveVersion = troveVersion,
			      troveFlavor = troveFlavor, withFiles = withFiles)

    def _getTrove(self, troveName = None, troveNameId = None, 
		  troveVersion = None, troveVersionId = None,
		  troveFlavor = 0, troveFlavorId = None, withFiles = True):
	if not troveNameId:
	    troveNameId = self.items[troveName]
	if not troveName:
	    troveName = self.items.getId(troveNameId)
	if not troveVersion:
	    troveVersion = self.versionTable.getId(troveVersionId, troveNameId)
	if not troveVersionId:
	    troveVersionId = self.versionTable[troveVersion]
	if troveFlavor is 0:
	    troveFlavor = self.flavors.getId(troveFlavorId)
	if troveFlavorId is None:
	    troveFlavorId = self.flavors[troveFlavor]

	if min(troveVersion.timeStamps()) == 0:
	    # XXX this would be more efficient if it used troveVersionId
	    # for the lookup
	    troveVersion.setTimeStamps(
		self.versionTable.getTimeStamps(troveVersion, troveNameId))

	cu = self.db.cursor()
	cu.execute("""SELECT instances.instanceId, 
                             instances.isRedirect, ChangeLogs.name, 
			     ChangeLogs.contact,
			     ChangeLogs.message FROM
		      Instances INNER JOIN Nodes ON 
		             Instances.itemId=Nodes.itemId AND
			     Instances.versionId=Nodes.versionId
		        LEFT OUTER JOIN ChangeLogs ON
			     Nodes.nodeId = ChangeLogs.NodeId
		      WHERE  Instances.itemId=? AND
			     Instances.versionId=? AND
			     Instances.flavorId=?""",
		      troveNameId, troveVersionId, troveFlavorId)

	result = cu.fetchone()
	troveInstanceId = result[0]
        isRedirect = result[1]
	if result[2] is not None:
	    changeLog = changelog.ChangeLog(*result[2:5])
	else:
	    changeLog = None

        assert(troveFlavor is not None)

	trv = trove.Trove(troveName, troveVersion, troveFlavor,
			      changeLog, isRedirect = isRedirect)
        cu.execute("SELECT includedId, byDefault from TroveTroves WHERE "
                   "instanceId=?", troveInstanceId)
        for (instanceId, byDefault) in cu:
	    (itemId, versionId, flavorId, isPresent) = \
                     self.instances.getId(instanceId)
	    name = self.items.getId(itemId)
	    flavor = self.flavors.getId(flavorId)
	    version = self.versionTable.getId(versionId, itemId)

	    trv.addTrove(name, version, flavor, byDefault = byDefault)

        if withFiles:
            versionCache = {}
            cu.execute("SELECT pathId, path, versionId, fileId FROM "
                   "TroveFiles NATURAL JOIN FileStreams WHERE instanceId = ?", 
                   troveInstanceId)
            for (pathId, path, versionId, fileId) in cu:
                version = versionCache.get(versionId, None)
                if not version:
                    version = self.versionTable.getBareId(versionId)
                    versionCache[versionId] = version

                trv.addFile(pathId, path, version, fileId)

        self.depTables.get(cu, trv, troveInstanceId)
        self.troveInfoTable.getInfo(cu, trv, troveInstanceId)

	return trv

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
            return files.ThawFile(stream, fileId)

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
		   "TroveFiles NATURAL JOIN FileStreams "
		   "WHERE instanceId = ? %s" %sort, 
		   troveInstanceId)

	versionCache = {}
	for (pathId, path, fileId, versionId, stream) in cu:
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getBareId(versionId)
		versionCache[versionId] = version

            if stream:
                fObj = files.ThawFile(stream, fileId)

	    if withFiles:
		yield (pathId, path, fileId, version, stream)
	    else:
		yield (pathId, path, fileId, version)

    def addFile(self, troveInfo, pathId, fileObj, path, fileId, fileVersion,
                fileStream = None):
	cu = troveInfo[0]
	versionId = self.getVersionId(fileVersion, self.fileVersionCache)

	if fileObj:
            if fileStream is None:
                fileStream = fileObj.freeze()
	    cu.execute("INSERT INTO NewFiles VALUES(?, ?, ?, ?, ?)", 
		       (pathId, versionId, fileId, fileStream, path))
	else:
	    cu.execute("INSERT INTO NewFiles VALUES(?, ?, ?, NULL, ?)", 
		       (pathId, versionId, fileId, path))

    def getFile(self, pathId, fileId):
        cu = self.db.cursor()
        cu.execute("SELECT stream FROM FileStreams WHERE fileId=?", fileId)
        try:
            stream = cu.next()[0]
        except StopIteration:
            raise KeyError, (pathId, fileId)

        if stream is not None:
            return files.ThawFile(stream, pathId)
        else:
            return None

    def getFiles(self, l):
        # this only needs a list of (pathId, fileId) pairs, but it sometimes
        # gets (pathId, fileId, version) pairs instead (which is what
        # the network repository client uses)
	cu = self.db.cursor()

	cu.execute("""
	    CREATE TEMPORARY TABLE getFilesTbl(rowId INTEGER PRIMARY KEY,
					       fileId BINARY)
	""", start_transaction = False)

        try:
            verCache = {}
            lookup = range(len(l) + 1)
            for tup in l:
                (pathId, fileId) = tup[:2]
                cu.execute("INSERT INTO getFilesTbl VALUES(NULL, ?)",
                           fileId, start_transaction = False)
                lookup[cu.lastrowid] = (pathId, fileId)

            cu.execute("""
                SELECT rowId, stream FROM getFilesTbl INNER JOIN FileStreams ON
                        getFilesTbl.fileId = FileStreams.fileId 
            """)

            d = {}
            for rowId, stream in cu:
                pathId, fileId = lookup[rowId]
                if stream is not None:
                    f = files.ThawFile(stream, pathId)
                else:
                    f = None
                d[(pathId, fileId)] = f
        finally:
            cu.execute("DROP TABLE getFilesTbl", start_transaction = False)
	return d

    def resolveRequirements(self, label, depSetList):
        return self.depTables.resolve(label, depSetList)

    def begin(self):
	"""
	Force the database to begin a transaction; this locks the database
	so no one can touch it until a commit() or rollback().
	"""
	self.db._begin()

    def rollback(self):
	self.db.rollback()

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
