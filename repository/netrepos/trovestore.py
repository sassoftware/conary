#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import instances
import items
import files
import package
import sqlite
import trovecontents
import versionops

class TroveStore:

    def __init__(self, path):
	self.db = sqlite.connect(path)
	self.troveTroves = trovecontents.TroveTroves(self.db)
	self.troveFiles = trovecontents.TroveFiles(self.db)
	self.fileStreams = instances.FileStreams(self.db)
	self.items = items.Items(self.db)
	self.instances = instances.InstanceTable(self.db)
	self.versionTable = versionops.VersionTable(self.db)
        self.branchTable = versionops.BranchTable(self.db)
	self.versionOps = versionops.SqlVersioning(self.db, self.versionTable,
                                                   self.branchTable)
	self.streamIdCache = {}
	self.needsCleanup = False

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

    def createTroveBranch(self, troveName, branch):
	itemId = self.getItemId(troveName)
	self.versionOps.createBranch(itemId, branch)

    def iterTroveBranches(self, troveName):
	itemId = self.items[troveName]
	for branchId in self.versionOps.branchesOfItem(itemId):
	    yield self.branchTable.getId(branchId)

    def iterTroveVersions(self, troveName):
	itemId = self.items[troveName]
	for versionId in self.versionOps.versionsOfItem(itemId):
	    yield self.versionTable.getId(versionId)

    def troveLatestVersion(self, troveName, branch):
	"""
	Returns None if no versions of troveName exist on the branch.
	"""
	itemId = self.items.get(troveName, None)
	if not itemId:
	    return None
	branchId = self.branchTable[branch]
	latestId = self.versionOps.latestOnBranch(itemId, branchId)
	return self.versionTable.getId(latestId)

    def getLatestTrove(self, troveName, branch):
	itemId = self.items[troveName]
	branchId = self.branchTable[branch]
	latestId = self.versionOps.latestOnBranch(itemId, branchId)
	return self._getTrove(troveName = troveName, troveNameId = itemId,
			      troveVersionId = latestId)

    def createFileBranch(self, fileId, branch):
	itemId = self.getItemId(fileId)
	self.versionOps.createBranch(itemId, branch)

    def iterTroveNames(self):
	return self.items.iterkeys()

    def addTrove(self, trove):
	versionCache = {}

	troveVersion = trove.getVersion()
	troveItemId = self.getItemId(trove.getName())
	troveVersionId = self.versionOps.createVersion(troveItemId, 
						       troveVersion)
	# the instance may already exist (it could be referenced by a package
	# which has already been added)
	troveInstanceId = self.getInstanceId(troveItemId, troveVersionId, 0)
	
	assert(not self.troveTroves.has_key(troveInstanceId))
	
	j = 0
	for (fileId, path, version) in trove.iterFileList():
	    j += 1
	    if j % 500 == 0: print j
	    versionId = self.getVersionId(version, versionCache)
	    streamId = self.streamIdCache.get((fileId, versionId), None)
	    if not streamId:
		# shared file
		streamId = self.fileStreams.getStreamId((fileId, versionId))
	    else:
		del self.streamIdCache[(fileId, versionId)]

	    self.troveFiles.addItem(troveInstanceId, streamId, path)

	for (name, version) in trove.iterPackageList():
	    versionId = self.getVersionId(version, versionCache)
	    itemId = self.getItemId(name)
	    instanceId = self.getInstanceId(itemId, versionId, 0)
	    self.troveTroves.addItem(troveInstanceId, instanceId)

    def eraseTrove(self, troveName, troveVersion):
	troveItemId = self.items[troveName]
	troveVersionId = self.versionTable[troveVersion]
	troveInstanceId = self.instances[(troveItemId, troveVersionId, 0)]

	del self.troveFiles[troveInstanceId]
	del self.troveTroves[troveInstanceId]

	# mark this trove as not present
	self.instances.setPresent(troveInstanceId, 0)
	self.needsCleanup = True

	self.versionOps.eraseVersion(troveItemId, troveVersionId)

    def hasTrove(self, troveName, troveVersion = None):
	if not troveVersion:
	    return self.items.has_key(troveName)

	troveItemId = self.items[troveName]
        try:
            troveVersionId = self.versionTable[troveVersion]
        except KeyError:
            # there is no version in the versionId for this version
            # in the table, so we can't have a trove with that version
            return False
	
	return self.instances.isPresent((troveItemId, troveVersionId, 0))

    def branchesOfTroveLabel(self, troveName, label):
	troveId = self.items[troveName]
	for branchId in self.versionOps.branchesOfLabel(troveId, label):
	    yield self.branchTable.getId(branchId)

    def getTrove(self, troveName, troveVersion):
	return self._getTrove(troveName = troveName, 
			      troveVersion = troveVersion)

    def _getTrove(self, troveName = None, troveNameId = None, 
		  troveVersion = None, troveVersionId = None):
	if not troveNameId:
	    troveNameId = self.items[troveName]
	if not troveName:
	    troveName = self.items.getId(troveNameId)
	if not troveVersion:
	    troveVersion = self.versionTable.getId(troveVersionId)
	if not troveVersionId:
	    troveVersionId = self.versionTable[troveVersion]

	if not troveVersion.timeStamp:
	    troveVersion.timeStamp = \
		    self.versionTable.getTimestamp(troveVersionId)

	troveInstanceId = self.instances[(troveNameId, troveVersionId, 0)]
	trove = package.Trove(troveName, troveVersion)
	versionCache = {}
	for instanceId in self.troveTroves[troveInstanceId]:
	    (itemId, versionId, archId, isPresent) = \
		    self.instances.getId(instanceId)
	    troveName = self.items.getId(itemId)
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getId(versionId)
		versionCache[versionId] = version

	    trove.addPackageVersion(troveName, version)

	cu = self.db.cursor()
	cu.execute("SELECT fileId, path, versionId FROM "
		   "FileStreams NATURAL JOIN TroveFiles WHERE instanceId = %d", 
		   troveInstanceId)
	for (fileId, path, versionId) in cu:
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getId(versionId)
		versionCache[versionId] = version

	    trove.addFile(fileId, path, version)

	return trove

    def iterFilesInTrove(self, trove, sortByPath = False, withFiles = False):
	if sortByPath:
	    sort = " ORDER BY path";
	else:
	    sort =""
	cu = self.db.cursor()

	troveItemId = self.items[trove.getName()]
	troveVersionId = self.versionTable[trove.getVersion()]
	troveInstanceId = self.instances[(troveItemId, troveVersionId, 0)]
	versionCache = {}

	cu.execute("SELECT fileId, path, versionId, stream FROM "
		   "FileStreams NATURAL JOIN TroveFiles "
		   "WHERE instanceId = %%d %s" % sort, 
		   troveInstanceId)

	versionCache = {}
	for (fileId, path, versionId, stream) in cu:
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getId(versionId)
		versionCache[versionId] = version

	    if withFiles:
		fileObj = files.ThawFile(stream, fileId)
		yield (fileId, path, version, fileObj)
	    else:
		yield (fileId, path, version)
	    
    def addFile(self, file, fileVersion):
	versionId = self.getVersionId(fileVersion, {})
	i = self.fileStreams.addStream((file.id(), versionId), file.freeze())
	self.streamIdCache[(file.id(), versionId)] = i

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

	self.db.commit()
