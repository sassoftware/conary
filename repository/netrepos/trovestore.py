#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changelog
import cltable
import deps.deps
import instances
import items
import files
import flavors
import package
import sqlite
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
		      WHERE Versions.versionId=%d AND Nodes.itemId=%s""", 
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
			SELECT versionId from Versions WHERE version=%s
		      )
		      AND itemId=%s""", version.asString(), itemId)
	try:
	    (t,) = cu.next()
	    return [ float(x) for x in t.split(":") ]
	except StopIteration:
            raise KeyError, theId

class TroveStore:

    def __init__(self, path):
	self.db = sqlite.connect(path, timeout = 30000)

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
        self.db.commit()
        
	self.streamIdCache = {}
	self.needsCleanup = False
	self.filesToAdd = {}

    def __del__(self):
        try:
            self.db.close()
        except sqlite.ProgrammingError:
            pass
        del self.db

    def getItemId(self, item):
	theId = self.items.get(item, None)
	if theId == None:
	    theId = self.items.addId(item)

	return theId

    def getInstanceId(self, itemId, versionId, flavorId):
	theId = self.instances.get((itemId, versionId, flavorId), None)
	if theId == None:
	    theId = self.instances.addId(itemId, versionId, flavorId)

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
		itemId=(SELECT itemId FROM Items WHERE item=%s) AND
		versionId=(SELECT versionId FROM Versions WHERE version=%s)
	""", item, version.asString())

	timeStamps = cu.fetchone()[0]
	version.setTimeStamps([float(x) for x in timeStamps.split(":")])

    def createTroveBranch(self, troveName, branch):
	itemId = self.getItemId(troveName)
	branchId = self.versionOps.createBranch(itemId, branch)

    def iterTroveVersions(self, troveName):
	cu = self.db.cursor()
	cu.execute("""
	    SELECT version FROM Items JOIN Nodes 
		    ON Items.itemId = Nodes.itemId NATURAL
		JOIN Versions WHERE item=%s""", troveName)
	for (versionStr, ) in cu:
	    yield versionStr

    def troveLatestVersion(self, troveName, branch):
	"""
	Returns None if no versions of troveName exist on the branch.
	"""
	cu = self.db.cursor()
	cu.execute("""
	    SELECT version, timeStamps FROM 
		(SELECT itemId AS AitemId, branchId as AbranchId FROM labelMap
		    WHERE itemId=(SELECT itemId from Items 
				WHERE item=%s)
		    AND branchId=(SELECT branchId FROM Branches
				WHERE branch=%s)
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
	    cu.execute("INSERT INTO itlblb VALUES (%s)", name,
		       start_transaction = False)

	cu.execute("""
	    SELECT Items.item, Versions.version, Nodes.timeStamps FROM
		itlblb JOIN Items ON
		    itlblb.troveName = Items.item
		JOIN LabelMap
		    ON Items.itemId = LabelMap.itemId
		JOIN (SELECT labelId as aLabelId FROM Labels 
				WHERE Labels.label=%s)
		    ON LabelMap.labelId = aLabelId
		JOIN Latest ON 
		    Items.itemId=Latest.itemId AND LabelMap.branchId=Latest.branchId
		JOIN Nodes ON
		    Items.itemId=Nodes.itemId AND Latest.versionId=Nodes.versionId
		JOIN Versions ON
		    Nodes.versionId = versions.versionId
	    ;
	""", labelStr)

	d = {}
	for (troveName, versionStr, timeStamps) in cu:
	    v = versions.VersionFromString(versionStr, 
		    timeStamps = [ float(x) for x in timeStamps.split(":") ] )
	    if d.has_key(troveName):
		d.append(v)
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
				WHERE item=%s)
		    AND labelId=(SELECT labelId FROM Labels 
				WHERE label=%s)
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
				WHERE item=%s)
		    AND labelId=(SELECT labelId FROM Labels 
				WHERE label=%s)
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
		    INSERT INTO itf VALUES (%s, %s, %s)
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
	versionCache = {}

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

	    if trove.getChangeLog():
		self.changeLogs.add(nodeId, trove.getChangeLog())

	troveFlavor = trove.getFlavor()

	cu = self.db.cursor()

	# start off by creating the flavors we need; we could combine this
	# to some extent with the file table creation below, but there are
	# normally very few flavors per trove so this probably better
	flavorsNeeded = {}
	if troveFlavor:
	    flavorsNeeded[troveFlavor] = True

	for (fileId, path, version) in trove.iterFileList():
	    fileObj = self.filesToAdd.get((fileId, version), None)
	    if not fileObj or not fileObj.hasContents: continue
	    flavor = fileObj.flavor.value()
	    if flavor and not flavorsNeeded.has_key(flavor):
		flavorsNeeded[flavor] = True

	for (name, version, flavor) in trove.iterTroveList():
	    if flavor:
		flavorsNeeded[flavor] = True

	flavorIndex = {}
	cu.execute("CREATE TEMPORARY TABLE NeededFlavors(flavor STR)")
	for flavor in flavorsNeeded.iterkeys():
	    flavorIndex[flavor.freeze()] = flavor
	    cu.execute("INSERT INTO NeededFlavors VALUES(%s)", 
		       flavor.freeze())
	    
	del flavorsNeeded

	# it seems like there must be a better way to do this, but I can't
	# figure it out. I *think* inserting into a view would help, but I
	# can't with sqlite.

	cu.execute("""SELECT NeededFlavors.flavor FROM	
			NeededFlavors LEFT OUTER JOIN Flavors ON
			    NeededFlavors.flavor = Flavors.Flavor 
			WHERE Flavors.flavorId is NULL""")
	for (flavorStr,) in cu:
	    self.flavors.createFlavor(flavorIndex[flavorStr])

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
	#
	# the instance may already exist (it could be referenced by a package
	# which has already been added)
	troveInstanceId = self.getInstanceId(troveItemId, troveVersionId, 
					     troveFlavorId)
	assert(not self.troveTroves.has_key(troveInstanceId))

	# this table could well have incorrect flavorId and stream
	# information, but it will be right for new files, and that's
	# all that matters (since existing files don't get updated
	# in the FileStreams table)
	cu.execute("""
	    CREATE TEMPORARY TABLE NewFiles(instanceId INTEGER, 
					    fileId STRING,
					    versionId INTEGER,
					    flavorId INTEGER,
					    stream BINARY,
					    path STRING)
	""")
	
	for (fileId, path, version) in trove.iterFileList():
	    fileObj = self.filesToAdd.get((fileId, version), None)

	    stream = None
	    flavorId = 0

	    if fileObj:
		del self.filesToAdd[(fileId, version)]
		stream = sqlite.encode(fileObj.freeze())

		if fileObj.hasContents:
		    flavor = fileObj.flavor.value()
		    if flavor:
			flavorId = flavors[flavor]

	    versionId = self.getVersionId(version, versionCache)

	    cu.execute("""
		INSERT INTO NewFiles VALUES(%d, %s, %d, %d, %s, %s)
	    """, (troveInstanceId, fileId, versionId, flavorId, stream, path))

        cu.execute("""
	    INSERT INTO FileStreams SELECT NULL,
					   NewFiles.fileId,
					   NewFiles.versionId,
					   NewFiles.flavorId,
					   NewFiles.stream
		FROM NewFiles LEFT OUTER JOIN FileStreams ON
		    NewFiles.fileId = FileStreams.fileId AND
		    NewFiles.versionId = FileStreams.versionId
		WHERE FileStreams.streamId is NULL
                """)
        cu.execute("""
	    INSERT INTO TroveFiles SELECT NewFiles.instanceId,
					  FileStreams.streamId,
					  NewFiles.path
		FROM NewFiles JOIN FileStreams ON
		    NewFiles.fileId = FileStreams.fileId AND
		    NewFiles.versionId = FileStreams.versionId
                    """)
        cu.execute("DROP TABLE NewFiles")

	for (name, version, flavor) in trove.iterTroveList():
	    versionId = self.getVersionId(version, versionCache)
	    itemId = self.getItemId(name)
	    if flavor:
		flavorId = flavors[flavor]
	    else:
		flavorId = 0

	    instanceId = self.getInstanceId(itemId, versionId, flavorId)
	    self.troveTroves.addItem(troveInstanceId, instanceId)

    def eraseTrove(self, troveName, troveVersion, troveFlavor):
	assert(0)
	# the garbage collection isn't right

	troveItemId = self.items[troveName]
	troveVersionId = self.versionTable[troveVersion]
	troveFlavorId = self.flavors[troveFlavor]
	troveInstanceId = self.instances[(troveItemId, troveVersionId, 
					  troveFlavorId)]

	del self.troveFiles[troveInstanceId]
	del self.troveTroves[troveInstanceId]

	# mark this trove as not present
	self.instances.setPresent(troveInstanceId, 0)
	self.needsCleanup = True

	self.versionOps.eraseVersion(troveItemId, troveVersionId)

	verStr = troveVersion.asString()
	left = verStr + '/'
	right = verStr + '0'

	# XXX if this is the base of any branches, we need to erase the heads
	# of those branches as well. this shouldn't be hard?

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

    def getTrove(self, troveName, troveVersion, troveFlavor):
	return self._getTrove(troveName = troveName, 
			      troveVersion = troveVersion,
			      troveFlavor = troveFlavor)

    def _getTrove(self, troveName = None, troveNameId = None, 
		  troveVersion = None, troveVersionId = None,
		  troveFlavor = 0, troveFlavorId = None):
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
		      WHERE  Instances.itemId=%d AND
			     Instances.versionId=%d AND
			     Instances.flavorId=%d""",
		      troveNameId, troveVersionId, troveFlavorId)

	result = cu.fetchone()
	troveInstanceId = result[0]
	if result[1] is not None:
	    changeLog = changelog.ChangeLog(*result[1:4])
	else:
	    changeLog = None

	trove = package.Trove(troveName, troveVersion, troveFlavor,
			      changeLog)
	for instanceId in self.troveTroves[troveInstanceId]:
	    (itemId, versionId, flavorId, isPresent) = \
		    self.instances.getId(instanceId)
	    name = self.items.getId(itemId)
	    flavor = self.flavors.getId(flavorId)
	    version = self.versionTable.getId(versionId, itemId)

	    trove.addTrove(name, version, flavor)

	versionCache = {}
	cu.execute("SELECT fileId, path, versionId FROM "
		   "TroveFiles NATURAL JOIN FileStreams WHERE instanceId = %d", 
		   troveInstanceId)
	for (fileId, path, versionId) in cu:
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getBareId(versionId)
		versionCache[versionId] = version

	    trove.addFile(fileId, path, version)

	return trove

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
		   "WHERE instanceId = %%d %s" % sort, 
		   troveInstanceId)

	versionCache = {}
	for (fileId, path, versionId, stream) in cu:
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getBareId(versionId)
		versionCache[versionId] = version

	    if withFiles:
		fileObj = files.ThawFile(stream, fileId)
		yield (fileId, path, version, fileObj)
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
		WHERE item=%s AND version=%s""", troveName, parent.asString())

	    l = [ (brVersion.asString(), x[0], x[1]) for x in cu ]

	    del parent
	    del brVersion

	cu.execute("""
	   SELECT Versions.version, Nodes.timeStamps, Flavors.flavor FROM 
		Nodes JOIN Instances ON Nodes.itemId=Instances.itemId AND 
				        Nodes.versionId=Instances.versionId 
		      JOIN Versions ON Instances.versionId=Versions.versionId
		      JOIN Flavors ON Instances.flavorId = Flavors.flavorId
	   WHERE Nodes.itemId=(SELECT itemId FROM Items WHERE item=%s)
	     AND branchId=(SELECT branchId FROM Branches WHERE branch=%s)
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
	    
    def addFile(self, fileObj, fileVersion):
	self.filesToAdd[(fileObj.id(), fileVersion)] = fileObj

    def getFile(self, fileId, fileVersion):
	versionId = self.versionTable[fileVersion]
	stream = self.fileStreams[(fileId, versionId)]
	return files.ThawFile(stream, fileId)

    def hasFile(self, fileId, fileVersion):
	versionId = self.versionTable.get(fileVersion, None)
	if not versionId: return False
	return self.fileStreams.has_key((fileId, versionId))

    def eraseFile(Self, fileId, fileVersion):
	# we automatically remove files when no troves reference them. 
	# cool, huh?
	pass

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
	    self.instances.removeUnused()
	    self.fileStreams.removeUnusedStreams()
	    self.items.removeUnused()
	    self.needsCleanup = False

	if self.versionOps.needsCleanup:
	    self.versionTable.removeUnused()
	    self.branchTable.removeUnused()
	    self.versionOps.labelMap.removeUnused()
	    self.versionOps.needsCleanup = False

	self.filesToAdd = {}
	self.db.commit()
