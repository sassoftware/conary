#
# Copyright (c) 2004 Specifix, Inc.
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
from deps import deps
from local import deptable
import instances
import items
import files
import flavors
import metadata
import sqlite3
import trove
import trovefiles
import versionops
import versions

from local import trovetroves
from local import versiontable

class LocalRepVersionTable(versiontable.VersionTable):

    def getId(self, theId, itemId):
        cu = self.db.cursor()
        cu.execute("""SELECT version, timeStamps FROM Versions
		      JOIN Nodes ON Versions.versionId = Nodes.versionId
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

    def __init__(self, path):
	self.db = sqlite3.connect(path, timeout = 30000)

	cu = self.db.cursor()
	#cu.execute("PRAGMA temp_store = MEMORY", start_transaction = False)
				 
        self.begin()
	self.troveTroves = trovetroves.TroveTroves(self.db)
	self.troveFiles = trovefiles.TroveFiles(self.db)
	self.fileStreams = instances.FileStreams(self.db)
	self.items = items.Items(self.db)
	self.instances = instances.InstanceTable(self.db)
	self.versionTable = LocalRepVersionTable(self.db)
        self.branchTable = versionops.BranchTable(self.db)
        self.changeLogs = cltable.ChangeLogTable(self.db)
	self.versionOps = versionops.SqlVersioning(self.db, self.versionTable,
                                                   self.branchTable)
	self.flavors = flavors.Flavors(self.db)
        self.depTables = deptable.DependencyTables(self.db)
        self.metadataTable = metadata.MetadataTable(self.db)
        self.db.commit()
        
	self.streamIdCache = {}
	self.needsCleanup = False

    def __del__(self):
        try:
            self.db.close()
        except sqlite3.ProgrammingError:
            pass
        del self.db

    def getItemId(self, item):
	theId = self.items.get(item, None)
	if theId == None:
	    theId = self.items.addId(item)

	return theId

    def getInstanceId(self, itemId, versionId, flavorId, isPresent = True):
	theId = self.instances.get((itemId, versionId, flavorId), None)
	if theId == None:
	    theId = self.instances.addId(itemId, versionId, flavorId,
					 isPresent = isPresent)
	elif isPresent:
	    # XXX we shouldn't have to do this unconditionally
	    self.instances.setPresent(theId, 1)

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

    def iterTroveVersions(self, troveName):
	cu = self.db.cursor()
	cu.execute("""
	    SELECT version, Nodes.timeStamps FROM Items JOIN Nodes 
		    ON Items.itemId = Nodes.itemId NATURAL
		JOIN Versions WHERE item=?""", troveName)
	for (versionStr, timeStamps) in cu:
	    version = versions.VersionFromString(versionStr)
	    version.setTimeStamps([float(x) for x in timeStamps.split(":")])
	    yield version

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
		) JOIN Latest ON 
		    AitemId=Latest.itemId AND AbranchId=Latest.branchId
		JOIN Nodes ON
		    AitemId=Nodes.itemId AND Latest.versionId=Nodes.versionId
		JOIN Versions ON
		    Nodes.versionId = versions.versionId
	""", troveName, branch.asString())
        try:
	    (verStr, timeStamps) = cu.next()
            return versions.VersionFromString(verStr,
		    timeStamps = [ float(x) for x in timeStamps.split(":") ] )
        except StopIteration:
            raise KeyError, (troveName, branch)

    def iterTroveLeafsByLabelBulk(self, troveNameList, labelStr):
	cu = self.db.cursor()
	cu.execute("CREATE TEMPORARY TABLE itlblb(troveName str)", 
		   start_transaction = False)
	for name in troveNameList:
	    cu.execute("INSERT INTO itlblb VALUES (?)", name,
		       start_transaction = False)

	cu.execute("""
	    SELECT Items.item, Versions.version, Nodes.timeStamps FROM
		itlblb JOIN Items ON
		    itlblb.troveName = Items.item
		JOIN LabelMap
		    ON Items.itemId = LabelMap.itemId
		JOIN (SELECT labelId as aLabelId FROM Labels 
				WHERE Labels.label=?)
		    ON LabelMap.labelId = aLabelId
		JOIN Latest ON 
		    Items.itemId=Latest.itemId AND LabelMap.branchId=Latest.branchId
		JOIN Nodes ON
		    Items.itemId=Nodes.itemId AND Latest.versionId=Nodes.versionId
		JOIN Versions ON
		    Nodes.versionId = versions.versionId
	""", labelStr)

	d = {}
	for (troveName, versionStr, timeStamps) in cu:
	    v = versions.VersionFromString(versionStr, 
		    timeStamps = [ float(x) for x in timeStamps.split(":") ] )
	    if d.has_key(troveName):
		d[troveName].append(v)
	    else:
		d[troveName] = [ v ]

	cu.execute("DROP TABLE itlblb", start_transaction = False)

	return d

    def iterTroveLeafsByLabel(self, troveName, labelStr):
	#cu = self.db.cursor()
	# set up a table which lists the branchIds and the latest version
	# id's for this search. the versionid will be NULL if it's an
	# empty branch
	cu = self.db.cursor()
	cu.execute("""
	    SELECT Versions.version, Nodes.timeStamps FROM 
		(SELECT itemId AS AitemId, branchId as AbranchId FROM labelMap
		    WHERE itemId=(SELECT itemId from Items 
				WHERE item=?)
		    AND labelId=(SELECT labelId FROM Labels 
				WHERE label=?)
		) 
		JOIN Latest ON 
		    AitemId=Latest.itemId AND AbranchId=Latest.branchId
		JOIN Nodes ON
		    AitemId=Nodes.itemId AND Latest.versionId=Nodes.versionId
		JOIN Versions ON
		    Nodes.versionId = versions.versionId
	""", troveName, labelStr)

	for (versionStr, timeStamps) in cu:
	    v = versions.VersionFromString(versionStr, 
		    timeStamps = [ float(x) for x in timeStamps.split(":") ] )
	    yield v

    def iterTroveVersionsByLabel(self, troveName, labelStr):
	cu = self.db.cursor()
	# set up a table which lists the branchIds and the latest version
	# id's for this search. the versionid will be NULL if it's an
	# empty branch
	cu.execute("""
	    SELECT Versions.version, Nodes.timeStamps FROM 
		(SELECT itemId AS AitemId, branchId as AbranchId FROM labelMap
		    WHERE itemId=(SELECT itemId from Items 
				WHERE item=?)
		    AND labelId=(SELECT labelId FROM Labels 
				WHERE label=?)
		) JOIN Nodes ON
		    AitemId=Nodes.itemId AND Nodes.branchId=AbranchId 
		JOIN Versions ON
		    Nodes.versionId = versions.versionId
		ORDER BY Nodes.finalTimeStamp
	""", troveName, labelStr)

	for (versionStr, timeStamps) in cu:
	    v = versions.VersionFromString(versionStr, 
		    timeStamps = [ float(x) for x in timeStamps.split(":") ] )
	    yield v

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
		    itf JOIN Items ON itf.item = Items.item
			JOIN versions ON itf.version = versions.version)
		JOIN instances ON
		    aItemId = instances.itemId AND
		    aVersionId = instances.versionId
		JOIN flavors ON
		    instances.flavorId = flavors.flavorId
		ORDER BY aItem, fullVersion
	""")

	for (item, verString, flavor) in cu:
	    ver = vMap[verString]
	    outD[item][ver].append(flavor)

	cu.execute("DROP TABLE itf", start_transaction = False)

	return outD

    def iterTroveNames(self):
	return self.items.iterkeys()

    def addTrove(self, trove):
	cu = self.db.cursor()

	cu.execute("""
	    CREATE TEMPORARY TABLE NewFiles(fileId BINARY,
					    versionId INTEGER,
					    stream BINARY,
					    path STRING)
	""")

	self.fileVersionCache = {}
	
	return (cu, trove)

    def addTroveDone(self, troveInfo):
	versionCache = {}
	(cu, trove) = troveInfo

	troveVersion = trove.getVersion()
	troveItemId = self.getItemId(trove.getName())

	# does this version already exist (for another flavor?)
	newVersion = False
	troveVersionId = self.versionTable.get(troveVersion, None)
	if troveVersionId is not None:
	    nodeId = self.versionOps.nodes.getRow(troveItemId, 
						  troveVersionId, None)

	if troveVersionId is None or nodeId is None:
	    (nodeId, troveVersionId) = self.versionOps.createVersion(
					    troveItemId, troveVersion)
	    newVersion = True

	    if trove.getChangeLog() and trove.getChangeLog().getName():
		self.changeLogs.add(nodeId, trove.getChangeLog())

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
			NeededFlavors JOIN Flavors ON
			NeededFlavors.flavor = Flavors.flavor""")
	for (flavorStr, flavorId) in cu:
	    flavors[flavorIndex[flavorStr]] = flavorId

	del flavorIndex
	cu.execute("DROP TABLE NeededFlavors")

	if troveFlavor:
	    troveFlavorId = flavors[troveFlavor]
	else:
	    troveFlavorId = 0

	# the instance may already exist (it could be referenced by a package
	# which has already been added)
	troveInstanceId = self.getInstanceId(troveItemId, troveVersionId, 
					     troveFlavorId, isPresent = True)
	assert(not self.troveTroves.has_key(troveInstanceId))

        self.depTables.add(cu, trove, troveInstanceId)

        cu.execute("""
	    INSERT INTO FileStreams SELECT NULL,
					   NewFiles.fileId,
					   NewFiles.versionId,
					   NewFiles.stream
		FROM NewFiles LEFT OUTER JOIN FileStreams ON
		    NewFiles.fileId = FileStreams.fileId AND
		    NewFiles.versionId = FileStreams.versionId
		WHERE FileStreams.streamId is NULL
                """)
        cu.execute("""
	    INSERT INTO TroveFiles SELECT ?,
					  FileStreams.streamId,
					  NewFiles.path
		FROM NewFiles JOIN FileStreams ON
		    NewFiles.fileId = FileStreams.fileId AND
		    NewFiles.versionId = FileStreams.versionId
                    """, troveInstanceId)
        cu.execute("DROP TABLE NewFiles")

	for (name, version, flavor) in trove.iterTroveList():
	    versionId = self.getVersionId(version, versionCache)
	    itemId = self.getItemId(name)
	    if flavor:
		flavorId = flavors[flavor]
	    else:
		flavorId = 0

	    instanceId = self.getInstanceId(itemId, versionId, flavorId,
					    isPresent = False)
	    self.troveTroves.addItem(troveInstanceId, instanceId)

	del self.fileVersionCache 

    def updateMetadata(self, troveName, branch, shortDesc, longDesc,
                    urls, licenses, categories, language):
        cu = self.db.cursor()
       
        itemId = self.getItemId(troveName)
        branchId = self.branchTable[branch]
       
        # if we're updating the default language, always create a new version
        latestVersion = self.metadataTable.getLatestVersion(itemId, branchId)
        if language == "C":
            if latestVersion:
                version = versions.VersionFromString(latestVersion)
                version.incrementRelease()
            else:
                version = versions._VersionFromString("1-1", defaultBranch=branch)
            
            self.versionTable.addId(version)
        else: # if this is a translation, update the current version
            if not latestVersion:
                raise KeyError, troveName
            version = versions.VersionFromString(latestVersion)
           
        versionId = self.versionTable.get(version, None)

        return self.metadataTable.add(itemId, versionId, branchId, shortDesc, longDesc,
                                      urls, licenses, categories, language)

    def getMetadata(self, troveName, branch, version=None, language="C"):
        itemId = self.getItemId(troveName)
        branchId = self.branchTable[branch]
        
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
            return None
       
        metadata = self.metadataTable.get(itemId, versionId, branchId, language)
        return metadata

    def hasTrove(self, troveName, troveVersion = None, troveFlavor = 0):
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

    def iterAllTroveLeafs(self, troveNameList):
	cu = self.db.cursor()

	cu.execute("""
	    SELECT item, version FROM Items NATURAL JOIN Latest 
				      NATURAL JOIN Versions
		WHERE item in (%s)""" % 
	    ",".join(["'%s'" % x for x in troveNameList]))

	lastName = None
	leafList = []
	for (name, version) in cu:
	    if lastName != name and lastName:
		yield (lastName, leafList)
		leafList = []
		lastName = name
	    elif not lastName:
		lastName = name

	    leafList.append(version)
		
	yield (lastName, leafList)

    def branchesOfTroveLabel(self, troveName, label):
	troveId = self.items[troveName]
	for branchId in self.versionOps.branchesOfLabel(troveId, label):
	    yield self.branchTable.getId(branchId)

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
	cu.execute("""SELECT instances.instanceId, ChangeLogs.name, 
			     ChangeLogs.contact,
			     ChangeLogs.message FROM
		      Instances JOIN Nodes ON 
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
	if result[1] is not None:
	    changeLog = changelog.ChangeLog(*result[1:4])
	else:
	    changeLog = None

	trv = trove.Trove(troveName, troveVersion, troveFlavor,
			      changeLog)
	for instanceId in self.troveTroves[troveInstanceId]:
	    (itemId, versionId, flavorId, isPresent) = \
		    self.instances.getId(instanceId)
	    name = self.items.getId(itemId)
	    flavor = self.flavors.getId(flavorId)
	    version = self.versionTable.getId(versionId, itemId)

	    trv.addTrove(name, version, flavor)

        if withFiles:
            versionCache = {}
            cu.execute("SELECT fileId, path, versionId FROM "
                   "TroveFiles NATURAL JOIN FileStreams WHERE instanceId = ?", 
                   troveInstanceId)
            for (fileId, path, versionId) in cu:
                version = versionCache.get(versionId, None)
                if not version:
                    version = self.versionTable.getBareId(versionId)
                    versionCache[versionId] = version

                trv.addFile(fileId, path, version)

        self.depTables.get(cu, trv, troveInstanceId)

	return trv

    def findFileVersion(self, troveName, troveVersion, fileId, fileVersion):
        cu = db.cursor()
        cu.execute("""
                SELECT fsStream from Versions JOIN 
                        JOIN Instances ON
                            Versions.versionId == Instances.versionId 
                        JOIN Items
                            Items.itemId == Instances.instanceId JOIN
                        JOIN TroveFiles ON
                            Instances.instanceId == TroveFiles.instanceId 
                        JOIN (
                            SELECT stream AS fsStream FROM
                                FileStreams JOIN Versions ON
                                    FileStreams.versionId == Versions.versionId
                                WHERE fileId == ? AND version == ?
                            )
                        WHERE
                            Item.item == ? AND
                            Versions.version == ?
            """, fileId, fileVersion.asString(), troveName, 
                 troveVersion.asString())
                            
        for (stream,) in cu:
            return stream

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

	cu.execute("SELECT fileId, path, versionId, stream FROM "
		   "TroveFiles NATURAL JOIN FileStreams "
		   "WHERE instanceId = ? %s" %sort, 
		   troveInstanceId)

	versionCache = {}
	for (fileId, path, versionId, stream) in cu:
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getBareId(versionId)
		versionCache[versionId] = version

	    if withFiles:
		yield (fileId, path, version, stream)
	    else:
		yield (fileId, path, version)

    def iterTrovePerFlavorLeafs(self, troveName, branch):
	# this needs to return a list sorted by version, from oldest to
	# newest

	# find out what flavors are provided by the head of the branch
	# found yet
	cu = self.db.cursor()
	l = []

	if branch.count("/") > 2:
	    brVersion = versions.VersionFromString(branch)
	    parent = brVersion.parentNode()
	    brVersion.appendVersionReleaseObject(parent.trailingVersion())

	    cu.execute("""
		SELECT DISTINCT Nodes.timeStamps, Flavors.flavor 
		    FROM Items JOIN Instances 
		JOIN Flavors JOIN versions ON 
		    items.itemId = instances.itemId AND 
		    versions.versionId = instances.versionId AND 
		    flavors.flavorId = instances.flavorId 
		JOIN Nodes ON
		    Nodes.itemId = instances.itemId AND
		    Nodes.versionId = instances.versionId
		WHERE item=? AND version=?""", troveName, parent.asString())

	    l = [ (brVersion.asString(), x[0], x[1]) for x in cu ]

	    del parent
	    del brVersion

	cu.execute("""
	   SELECT Versions.version, Nodes.timeStamps, Flavors.flavor FROM 
		Nodes JOIN Instances ON Nodes.itemId=Instances.itemId AND 
				        Nodes.versionId=Instances.versionId 
		      JOIN Versions ON Instances.versionId=Versions.versionId
		      JOIN Flavors ON Instances.flavorId = Flavors.flavorId
	   WHERE Nodes.itemId=(SELECT itemId FROM Items WHERE item=?)
	     AND branchId=(SELECT branchId FROM Branches WHERE branch=?)
	   ORDER BY finalTimeStamp
	""", troveName, branch)

	latest = {}	
	deleteList = []
	fullList = []
	l += [ x for x in cu ]
	for i, (version, timeStamps, flavor) in enumerate(l):
	    if latest.has_key(flavor):
		deleteList.append(latest[flavor])

	    latest[flavor] = i
	    fullList.append((version, timeStamps, flavor))

	deleteList.sort()
	deleteList.reverse()
	for i in deleteList:
	    del fullList[i]

	return fullList
	    
    def addFile(self, troveInfo, fileId, fileObj, path, fileVersion):
	cu = troveInfo[0]
	versionId = self.getVersionId(fileVersion, self.fileVersionCache)

	if fileObj:
	    stream = fileObj.freeze()
	    cu.execute("INSERT INTO NewFiles VALUES(?, ?, ?, ?)", 
		       (fileId, versionId, stream, path))
	else:
	    cu.execute("INSERT INTO NewFiles VALUES(?, ?, NULL, ?)", 
		       (fileId, versionId, path))

    def getFile(self, fileId, fileVersion):
	versionId = self.versionTable[fileVersion]
	stream = self.fileStreams[(fileId, versionId)]
	return files.ThawFile(stream, fileId)

    def getFiles(self, l):
	cu = self.db.cursor()

	cu.execute("""
	    CREATE TEMPORARY TABLE getFilesTbl(rowId INTEGER PRIMARY KEY,
					       fileId STRING,
					       versionId INT)
	""", start_transaction = False)

	verCache = {}
	lookup = range(len(l) + 1)
	for (fileId, fileVersion) in l:
	    versionId = verCache.get(fileVersion, None)
	    if versionId is None:
		versionId = self.versionTable[fileVersion]
		verCache[fileVersion] = versionId

	    cu.execute("INSERT INTO getFilesTbl VALUES(NULL, ?, ?)",
		       (fileId, versionId), 
		       start_transaction = False)
	    lookup[cu.lastrowid] = (fileId, fileVersion)

	cu.execute("""
	    SELECT rowId, stream FROM getFilesTbl JOIN FileStreams ON
		    getFilesTbl.versionId = FileStreams.versionId AND
		    getFilesTbl.fileId = FileStreams.fileId 
	""")

	d = {}
	for rowId, stream in cu:
	    fileId, version = lookup[rowId]
	    d[(fileId, version)] = files.ThawFile(stream, fileId)

	cu.execute("DROP TABLE getFilesTbl", start_transaction = False)

	return d

    def hasFile(self, fileId, fileVersion):
	versionId = self.versionTable.get(fileVersion, None)
	if not versionId: return False
	return self.fileStreams.has_key((fileId, versionId))

    def resolveRequirements(self, label, depSetList):
        return self.depTables.resolve(label, depSetList)

    def eraseFile(Self, fileId, fileVersion):
	# we automatically remove files when no troves reference them. 
	# cool, huh?
	assert(0)

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
