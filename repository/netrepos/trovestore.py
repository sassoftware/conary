#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import deps.deps
import instances
import items
import files
import flavors
import package
import sqlite
import trovecontents
import versionops
import versions

class LocalRepVersionTable(versionops.VersionTable):

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
	self.troveTroves = trovecontents.TroveTroves(self.db)
	self.troveFiles = trovecontents.TroveFiles(self.db)
	self.fileStreams = instances.FileStreams(self.db)
	self.items = items.Items(self.db)
	self.instances = instances.InstanceTable(self.db)
	self.versionTable = LocalRepVersionTable(self.db)
        self.branchTable = versionops.BranchTable(self.db)
	self.versionOps = versionops.SqlVersioning(self.db, self.versionTable,
                                                   self.branchTable)
	self.flavors = flavors.Flavors(self.db)
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

    def getInstanceId(self, itemId, versionId, archId):
	theId = self.instances.get((itemId, versionId, archId), None)
	if theId == None:
	    theId = self.instances.addId(itemId, versionId, archId)

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
		versionId=(SELECT versionId FROM Versions WHERE version=%s);
	""", item, version.asString())

	timeStamps = cu.fetchone()[0]
	version.setTimeStamps([float(x) for x in timeStamps.split(":")])


    def createParentReference(self, itemId, branch):
	if branch.hasParent():
	    headVersion = branch.copy()
	    headVersion.appendVersionReleaseObject(
			    headVersion.parentNode().trailingVersion())

	    parentVersion = branch.parentNode()
	    parentVersionId = self.versionTable.get(parentVersion, None)
	    if parentVersionId is None:
		return None

	    # we need the timestamp for the parent version; this is the
	    # easiest way to get it
	    parentVersion = self.versionTable.getId(parentVersionId, itemId)
	    headVersion.setTimeStamps(parentVersion.timeStamps())
	    headVersionId = self.getVersionId(headVersion, {})

	    if parentVersionId is not None:
		self.instances.addRedirect(itemId, headVersionId, 
					   parentVersionId)

	    return (headVersionId, headVersion.timeStamps())
	else:
	    return (None, None)

    def createTroveBranch(self, troveName, branch):
	# there's no doubt that this could be more efficient.
	itemId = self.getItemId(troveName)

	(headVersionId, headVersionTimestamps) = \
			    self.createParentReference(itemId, branch)
	if headVersionId:
	    branchId = self.versionOps.createBranch(itemId, branch, 
				topVersionId = headVersionId,
				topVersionTimestamps = headVersionTimestamps)

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
	    SELECT version FROM 
		(SELECT itemId AS AitemId, branchId as AbranchId FROM labelMap
		    WHERE itemId=(SELECT itemId from Items 
				WHERE item=%s)
		    AND branchId=(SELECT branchId FROM Branches
				WHERE branch=%s)
		) JOIN Latest ON 
		    AitemId=Latest.itemId AND AbranchId=Latest.branchId
		NATURAL JOIN Versions
	""", troveName, branch.asString())
	
	latest = cu.fetchone()[0]

	return versions.VersionFromString(latest)

    def iterTroveLeafsByLabel(self, troveName, labelStr):
	cu = self.db.cursor()
	# set up a table which lists the branchIds and the latest version
	# id's for this search. the versionid will be NULL if it's an
	# empty branch
	cu.execute("""
	    SELECT version FROM 
		(SELECT itemId AS AitemId, branchId as AbranchId FROM labelMap
		    WHERE itemId=(SELECT itemId from Items 
				WHERE item=%s)
		    AND labelId=(SELECT labelId FROM Labels 
				WHERE label=%s)
		) JOIN Latest ON 
		    AitemId=Latest.itemId AND AbranchId=Latest.branchId
		NATURAL JOIN Versions
	""", troveName, labelStr)

	for (versionStr,) in cu:
	    yield versionStr

    def iterTroveFlavors(self, troveName, troveVersion):
	cu = self.db.cursor()
	# I think we might be better of intersecting subqueries rather
	# then using all of the and's in this join
	cu.execute("""
	    SELECT DISTINCT Flavors.flavor FROM Items JOIN Instances 
	    JOIN Flavors JOIN versions ON 
		items.itemId = instances.itemId AND 
		versions.versionId = instances.versionId AND 
		flavors.flavorId = instances.flavorId 
	    WHERE item = %s AND version=%s""", 
	    troveName, troveVersion.canon().asString())
	for (flavorStr,) in cu:
	    if flavorStr == 'none':
		yield None
	    else:
		yield deps.deps.ThawDependencySet(flavorStr)

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
	    versionExists = self.versionOps.nodes.hasRow(troveItemId, 
							      troveVersionId)
	if troveVersionId is None or not versionExists:
	    troveVersionId = self.versionOps.createVersion(troveItemId, 
							   troveVersion)
	    newVersion = True
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

	# the instance may already exist (it could be referenced by a package
	# which has already been added)
	if troveFlavor:
	    troveFlavorId = flavors[troveFlavor]
	else:
	    troveFlavorId = 0
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
	    CREATE INDEX NewFilesIdx on NewFiles(fileId, versionId);

	    INSERT INTO FileStreams SELECT NULL,
					   NewFiles.fileId,
					   NewFiles.versionId,
					   NewFiles.flavorId,
					   NewFiles.stream
		FROM NewFiles LEFT OUTER JOIN FileStreams ON
		    NewFiles.fileId = FileStreams.fileId AND
		    NewFiles.versionId = FileStreams.versionId
		WHERE FileStreams.streamId is NULL;

	    INSERT INTO TroveFiles SELECT NewFiles.instanceId,
					  FileStreams.streamId,
					  NewFiles.path
		FROM NewFiles JOIN FileStreams ON
		    NewFiles.fileId = FileStreams.fileId AND
		    NewFiles.versionId = FileStreams.versionId;

            DROP INDEX NewFilesIdx;
	    DROP TABLE NewFiles;
	""")

	for (name, version, flavor) in trove.iterTroveList():
	    versionId = self.getVersionId(version, versionCache)
	    itemId = self.getItemId(name)
	    if flavor:
		flavorId = flavors[flavor]
	    else:
		flavorId = 0

	    instanceId = self.getInstanceId(itemId, versionId, flavorId)
	    self.troveTroves.addItem(troveInstanceId, instanceId)

	# we could have just added a version which something else
	# branches from, which means that branch also needs to include
	# the node we just added
	if not newVersion: return
	
	cu.execute("""
	    SELECT branch, Branches.branchId FROM Branches NATURAL JOIN Latest 
		WHERE parentNode=%d AND itemId=%d
	""", troveVersionId, troveItemId)

	for (branchStr, branchId) in cu:
	    branch = versions.VersionFromString(branchStr)
	    (headVersionId, headVersionTimestamp) = \
			self.createParentReference(troveItemId, branch)

	    self.versionOps.nodes.addRow(troveItemId, branchId, headVersionId,
					 headVersionTimestamp)


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

	troveVersionId = self.versionTable.get(troveVersion.canon(), None)
	if troveVersionId is None:
            # there is no version in the versionId for this version
            # in the table, so we can't have a trove with that version
            return False

	troveFlavorId = self.flavors.get(troveFlavor, 0)
	if troveFlavorId == 0:
            return False
	
	return self.instances.isPresent((troveItemId, troveVersionId, 
					 troveFlavorId))

    def iterAllTroveLeafs(self, troveName):
	cu = self.db.cursor()
	cu.execute("""
	    SELECT version FROM Items NATURAL JOIN Latest 
				      NATURAL JOIN Versions
		WHERE item = %s""", troveName)
	for (versionStr,) in cu:
	    yield versionStr

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
	    troveVersionId = self.versionTable[troveVersion.canon()]
	if troveFlavor is 0:
	    troveFlavor = self.flavors.getId(troveFlavorId)
	if troveFlavorId is None:
	    troveFlavorId = self.flavors[troveFlavor]

	if min(troveVersion.timeStamps()) == 0:
	    # don't use troveVersionId here as it could have come from
	    # troveVersion.canon(), which won't have all of the timestamps
	    troveVersion.setTimeStamps(
		self.versionTable.getTimeStamps(troveVersion, troveNameId))

	troveInstanceId = self.instances[(troveNameId, troveVersionId, 
					  troveFlavorId)]
	trove = package.Trove(troveName, troveVersion, troveFlavor)
	for instanceId in self.troveTroves[troveInstanceId]:
	    (itemId, versionId, flavorId, isPresent) = \
		    self.instances.getId(instanceId)
	    name = self.items.getId(itemId)
	    flavor = self.flavors.getId(flavorId)
	    version = self.versionTable.getId(versionId, itemId)

	    trove.addTrove(name, version, flavor)

	versionCache = {}
	cu = self.db.cursor()
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
	    sort = " ORDER BY path";
	else:
	    sort =""
	cu = self.db.cursor()

	troveItemId = self.items[troveName]
	troveVersionId = self.versionTable[troveVersion.canon()]
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
