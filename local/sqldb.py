#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

from repository.localrep import idtable
from repository.localrep import instructionsets
from repository.localrep import trovecontents
from repository.localrep import versionops
import sqlite
import package
import files
import deps.arch
import deps.deps
import versions

class DBTroveFiles:
    """
    fileId, versionId, path, instanceId, stream
    """
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "DBTroveFiles" not in tables:
            cu.execute("""CREATE TABLE DBTroveFiles(
					  fileId STR,
					  versionId INTEGER,
					  path STR,
					  instanceId INTEGER,
					  isPresent INTEGER,
					  stream BINARY)
		       """)
	    cu.execute("CREATE INDEX DBTroveFilesIdx ON "
		       "DBTroveFiles(fileId, versionId)")
	    cu.execute("CREATE INDEX DBTroveFilesInstanceIdx ON "
		       "DBTroveFiles(instanceId)")
	    cu.execute("CREATE INDEX DBTroveFilesPathIdx ON "
		       "DBTroveFiles(path)")

    def __getitem__(self, instanceId):
	cu = self.db.cursor()
	cu.execute("SELECT path, stream FROM DBTroveFiles "
		   "WHERE instanceId=%s and isPresent=1", instanceId)
	for match in cu:
	    yield match

    def getByInstanceId(self, instanceId, justPresent = True):
	cu = self.db.cursor()

	if justPresent:
	    cu.execute("SELECT path, stream FROM DBTroveFiles "
		       "WHERE instanceId=%s and isPresent=1", instanceId)
	else:
	    cu.execute("SELECT path, stream FROM DBTroveFiles "
		       "WHERE instanceId=%s", instanceId)

	for match in cu:
	    yield match

    def delInstance(self, instanceId):
        cu = self.db.cursor()
	
        cu.execute("DELETE from DBTroveFiles WHERE instanceId=%s", instanceId)

    def hasFileId(self, fileId, versionId, pristine):
	cu = self.db.cursor()
	if pristine:
	    cu.execute("SELECT path, stream FROM DBTroveFiles "
		       "WHERE fileId=%s AND versionId = %d", fileId, versionId)
	else:
	    cu.execute("SELECT path, stream FROM DBTroveFiles "
		       "WHERE fileId=%s AND versionId = %d "
		       "AND isPresent=1", fileId, versionId)
	return cu.fetchone() != None

    def getFileByFileId(self, fileId, versionId, justPresent = True):
	cu = self.db.cursor()
	if justPresent:
	    cu.execute("SELECT path, stream FROM DBTroveFiles "
		       "WHERE fileId=%s AND versionId=%d AND isPresent = 1", 
		       fileId, versionId)
	else:
	    cu.execute("SELECT path, stream FROM DBTroveFiles "
		       "WHERE fileId=%s AND versionId=%d",
		       fileId, versionId)
	# there could be multiple matches, but they should all be redundant
	try:
	    return cu.next()
	except StopIteration:
            raise KeyError, (fileId, versionId)

    def addItem(self, fileId, versionId, path, instanceId, stream):
        cu = self.db.cursor()
        cu.execute("""
	    INSERT INTO DBTroveFiles VALUES (%s, %d, %s, %d, %d, %s)
	""",
	   (fileId, versionId, path, instanceId, 1, sqlite.encode(stream)))

    def updateItem(self, instanceId, fileId, oldVersionId, newVersionId, 
		   newStream):
        cu = self.db.cursor()
	cu.execute("UPDATE DBTroveFiles SET versionId=%d, stream=%s "
		   "WHERE fileId=%s AND versionId=%d AND instanceId=%d",
		   newVersionId, sqlite.encode(newStream), fileId, 
		   oldVersionId, instanceId)

    def iterPath(self, path):
        cu = self.db.cursor()
	cu.execute("SELECT instanceId FROM DBTroveFiles WHERE "
		   "isPresent=1 AND path=%s", path)
	for instanceId in cu:
	    yield instanceId[0]

    def removePath(self, instanceId, path):
        cu = self.db.cursor()
	cu.execute("UPDATE DBTroveFiles SET isPresent=0 WHERE path=%s "
		   "AND instanceId=%d", (path, instanceId))

    def removeFileIds(self, instanceId, fileIdList, forReal = False):
        cu = self.db.cursor()
	if forReal:
	    cu.execute("DELETE FROM DBTroveFiles WHERE instanceId=%d "
		       "AND fileId in (%s)" % (instanceId,
			       ",".join(["'%s'" % x for x in fileIdList])))
	else:
	    cu.execute("UPDATE DBTroveFiles SET isPresent=0 WHERE "
		       "instanceId=%d AND fileId in (%s)" % (instanceId,
			       ",".join(["'%s'" % x for x in fileIdList])))

class DBInstanceTable:
    """
    Generic table for assigning id's to (name, version, isnSet, use)
    tuples, along with a isPresent flag
    """
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "DBInstances" not in tables:
            cu.execute("""CREATE TABLE DBInstances(
				instanceId INTEGER PRIMARY KEY, 
				troveName STR, 
				versionId INT, 
				flavorId INT,
				timeStamps STR,
				isPresent INT)""")
	    cu.execute("CREATE INDEX InstancesNameIdx ON "
		       "DBInstances(troveName)")
	    cu.execute("CREATE UNIQUE INDEX InstancesIdx ON "
		       "DBInstances(troveName, versionId, flavorId)")

    def iterNames(self):
	cu = self.db.cursor()
	cu.execute("SELECT DISTINCT troveName FROM DBInstances "
		    "WHERE isPresent=1")
	for match in cu:
	    yield match[0]

    def hasName(self, name):
	cu = self.db.cursor()
	cu.execute("SELECT instanceId FROM DBInstances "
		   "WHERE troveName=%s AND isPresent=1", 
		   name)
	return cu.fetchone() != None

    def iterByName(self, name):
	cu = self.db.cursor()
	cu.execute("SELECT instanceId, versionId, troveName, flavorId FROM "
		   "DBInstances WHERE troveName=%s AND isPresent = 1", name)
 	for match in cu:
	    yield match

    def addId(self, troveName, versionId, flavorId, timeStamps):
	assert(min(timeStamps) > 0)
        cu = self.db.cursor()
        cu.execute("INSERT INTO DBInstances VALUES (NULL, %s, %d, %d, "
						   "%s, %d)",
                   (troveName, versionId, flavorId, 
		    ":".join([ "%.3f" % x for x in timeStamps]), 1))
	return cu.lastrowid

    def delId(self, theId):
        assert(type(theId) is int)
        cu = self.db.cursor()
        cu.execute("DELETE FROM DBInstances WHERE instanceId=%d", theId)

    def getId(self, theId, justPresent = True):
        cu = self.db.cursor()

	if justPresent:
	    pres = "AND isPresent=1"
	else:
	    pres = ""

        cu.execute("SELECT troveName, versionId, flavorId, isPresent "
		   "FROM DBInstances WHERE instanceId=%%d %s" % pres, theId)
	try:
	    return cu.next()
	except StopIteration:
            raise KeyError, theId

    def isPresent(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT isPresent FROM DBInstances WHERE "
			"troveName=%s AND versionId=%d AND "
			"flavorId=%d", item)

	val = cu.fetchone()
	if not val:
	    return 0

	return val[0]

    def idIsPresent(self, instanceId):
        cu = self.db.cursor()
        cu.execute("SELECT isPresent FROM DBInstances WHERE "
			"instanceId=%d", instanceId)

	val = cu.fetchone()
	if not val:
	    return 0

	return val[0]

    def setPresent(self, theId, val):
        cu = self.db.cursor()
	cu.execute("UPDATE DBInstances SET isPresent=%%d WHERE instanceId=%d" 
			% theId, val)

    def has_key(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM DBInstances WHERE "
			"troveName=%s AND versionId=%d AND "
			"flavorId=%d", item)
	return not(cu.fetchone() == None)

    def __getitem__(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM DBInstances WHERE "
			"troveName=%s AND versionId=%d AND "
			"flavorId=%d", item)
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

        cu.execute("SELECT instanceId FROM DBInstances WHERE "
			"troveName=%%s AND versionId=%%d AND "
			"flavorId=%%d%s" % pres, item)
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

    def getVersion(self, instanceId):
        cu = self.db.cursor()
        cu.execute("""SELECT version, timeStamps FROM DBInstances
		      JOIN Versions ON 
			    DBInstances.versionId = Versions.versionId
		      WHERE instanceId=%d""", instanceId)
	try:
	    (s, t) = cu.next()
	    v = versions.VersionFromString(s)
	    v.setTimeStamps([ float(x) for x in t.split(":") ])
	    return v
	except StopIteration:
            raise KeyError, instanceId

class DBTarget:

    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "DBTarget" not in tables:
            cu.execute("""CREATE TABLE DBTarget(
					  base STR,
					  flags STR)
		       """)
	    # this table is small, so we don't create any indicies. we
	    # may actually need them though?

	    insSet = deps.arch.current()
	    for flag in insSet.flags:
		cu.execute("INSERT INTO DBTarget VALUES (%s,%s)", 
			   insSet.name, flag)

class DBFlavors(idtable.IdTable):

    def addId(self, flavor):
	idtable.IdTable.addId(self, flavor.freeze())

    def __getitem__(self, flavor):
        if flavor is None:
            return 0
	return idtable.IdTable.__getitem__(self, flavor.freeze())

    def getId(self, flavorId):
	return deps.deps.ThawDependencySet(idtable.IdTable.getId(self, 
								 flavorId))

    def get(self, flavor, defValue):
        if flavor is None:
            return 0
	return idtable.IdTable.get(self, flavor.freeze(), defValue)

    def __delitem__(self, flavor):
        assert(flavor is not None)
	idtable.IdTable.__delitem__(self, flavor.freeze())

    def getItemDict(self, itemSeq):
	cu = self.db.cursor()
        cu.execute("SELECT %s, %s FROM %s WHERE %s in (%s)"
                   % (self.strName, self.keyName, self.tableName, self.strName,
		      ",".join(["'%s'" % x.freeze() for x in itemSeq])))
	return dict(cu)

    def __init__(self, db):
	idtable.IdTable.__init__(self, db, "DBFlavors", "flavorId", "flavor")
	cu = db.cursor()
	cu.execute("SELECT FlavorID from DBFlavors")
	if cu.fetchone() == None:
	    # reserve flavor 0 for "no flavor information"
	    cu.execute("INSERT INTO DBFlavors VALUES (0, NULL)")
	
class DBFlavorMap(idtable.IdMapping):

    def __init__(self, db):
	idtable.IdMapping.__init__(self, db, "DBFlavorMap", "instanceId", 
				   "flavorId")

class Database:

    def __init__(self, path):
	self.db = sqlite.connect(path)
        self.db._begin()
	self.troveTroves = trovecontents.TroveTroves(self.db)
	self.troveFiles = DBTroveFiles(self.db)
	self.instances = DBInstanceTable(self.db)
	self.versionTable = versionops.VersionTable(self.db)
	self.targetTable = DBTarget(self.db)
	self.flavors = DBFlavors(self.db)
	self.flavorMap = DBFlavorMap(self.db)
        self.db.commit()
	self.streamCache = {}
	self.needsCleanup = False
	self.addVersionCache = {}
	self.flavorsNeeded = {}

    def __del__(self):
	self.db.close()
        del self.db

    def iterAllTroveNames(self):
	return self.instances.iterNames()

    def iterFindByName(self, name, pristine = False):
	for (instanceId, versionId, troveName, flavorId) in self.instances.iterByName(name):
	    yield self._getTrove(troveName = troveName,
				 troveInstanceId = instanceId, 
				 troveVersionId = versionId,
				 troveFlavorId = flavorId,
				 pristine = pristine)

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
		      timeStamps):
	theId = self.instances.get((troveName, versionId, flavorId), 
				   None)
	if theId is None:
	    theId = self.instances.addId(troveName, versionId, flavorId,
					 timeStamps)

	return theId

    def addTrove(self, trove, oldVersion = None):
	troveName = trove.getName()
	troveVersion = trove.getVersion()
	troveVersionId = self.getVersionId(troveVersion, {})
	self.addVersionCache[troveVersion] = troveVersionId

	troveFlavor = trove.getFlavor()
	if troveFlavor:
	    self.flavorsNeeded[troveFlavor] = True

	for (name, version, flavor) in trove.iterTroveList():
	    if flavor:
		self.flavorsNeeded[flavor] = True

	if self.flavorsNeeded:
	    # create all of the flavor id's we'll need
	    cu = self.db.cursor()
	    cu.execute("CREATE TEMPORARY TABLE flavorsNeeded(empty INTEGER, "
							    "flavor STRING)")
	    for flavor in self.flavorsNeeded.keys():
		cu.execute("INSERT INTO flavorsNeeded VALUES(%s, %s)", 
			   None, flavor.freeze())
	    cu.execute("""INSERT INTO DBFlavors 
			  SELECT flavorsNeeded.empty, flavorsNeeded.flavor
			  FROM flavorsNeeded LEFT OUTER JOIN DBFlavors
			      ON flavorsNeeded.flavor = DBFlavors.flavor
			      WHERE DBFlavors.flavorId is NULL
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
	if troveFlavor:
	    flavors[troveFlavor] = True

	for (name, version, flavor) in trove.iterTroveList():
	    if flavor:
		flavors[flavor] = True

	for (fileId, path, version) in trove.iterFileList():
	    versionId = self.getVersionId(version, self.addVersionCache)
	    result = self.streamCache.get((fileId, versionId), None)
	    if result and result[1]:
		flavors[result[1]] = True
	flavorMap = self.flavors.getItemDict(flavors.iterkeys())
	del flavors

	if troveFlavor:
	    troveFlavorId = flavorMap[troveFlavor.freeze()]
	else:
	    troveFlavorId = 0

	# the instance may already exist (it could be referenced by a package
	# which has already been added, or it may be in the database as
	# not present)
	troveInstanceId = self.instances.get((troveName, troveVersionId, 
				    troveFlavorId), None, justPresent = False)
	if troveInstanceId:
	    self.instances.setPresent(troveInstanceId, 1)
	else:
	    assert(min(troveVersion.timeStamps()) > 0)
	    troveInstanceId = self.instances.addId(troveName, troveVersionId, 
				       troveFlavorId, troveVersion.timeStamps())
	
	assert(not self.troveTroves.has_key(troveInstanceId))

	# we're updating from a previous version; there are a number of ways
	# to make this faster; this takes the (easy, simplistic, and slower) 
	# approach of prepopulating the streamCache with the streams for all 
	# of the files in the old version; the smarter methods would tend
	# to reuse the same rows in DBTroveFiles that the old version did,
	# but that's a pretty big semantic change (in particular, update.py
	# expects those to stick around a while longer)
	existingFiles = {}
	if oldVersion:
	    cu = self.db.cursor()
	    oldVersionId = self.getVersionId(oldVersion, self.addVersionCache)
	    oldInstanceId = self.instances[(troveName, oldVersionId, 
					    troveFlavorId)]
	    cu.execute("SELECT fileId, versionId FROM DBTroveFiles "
		       "WHERE instanceId=%d", oldInstanceId)
	    for (fileId, versionId) in cu:
		existingFiles[fileId] = versionId

	    cu.execute("UPDATE DBTroveFiles SET instanceId=%d WHERE "
		       "instanceId=%d", troveInstanceId, oldInstanceId)

	for (fileId, path, version) in trove.iterFileList():
	    # we know this is in the cache from above where we looked up
	    # all of the flavorId's we need for this add
	    versionId = self.addVersionCache[version]

	    existingId = existingFiles.get(fileId, None)
	    if existingId == versionId:
		# the file is in the table from a previous version, and
		# hasn't changed. nothing to do here.
		del existingFiles[fileId]
		continue

	    result = self.streamCache.get((fileId, versionId), None)
	    if result is None:
		(stream, flavor) = self.troveFiles.getFileByFileId(fileId, 
							 versionId)[1:3]
	    else:
		(stream, flavor) = result
		del self.streamCache[(fileId, versionId)]

	    if existingId:
		# existing file, new troveInstanceId and 
		del existingFiles[fileId]
		self.troveFiles.updateItem(troveInstanceId, fileId, existingId,
					   versionId, stream)
	    else:
		if flavor is None:
		    flavorId = 0
		else:
		    flavorId = flavorMap[flavor.freeze()]

		self.troveFiles.addItem(fileId, versionId, path, 
				        troveInstanceId, stream)

	if existingFiles:
	    self.troveFiles.removeFileIds(troveInstanceId,
					  existingFiles.iterkeys(),
					  forReal = True)

	for (name, version, flavor) in trove.iterTroveList():
	    versionId = self.getVersionId(version, self.addVersionCache)
	    if flavor:
		flavorId = flavorMap[flavor.freeze()]
	    else:
		flavorId = 0
	    instanceId = self.getInstanceId(name, versionId, flavorId,
					    version.timeStamps())
	    self.troveTroves.addItem(troveInstanceId, instanceId)

    def addFile(self, fileObj, fileVersion):
	versionId = self.getVersionId(fileVersion, self.addVersionCache)
	if fileObj.hasContents:
	    flavor = fileObj.flavor.deps
	    if flavor:
		self.flavorsNeeded[flavor] = True
	    else:
		flavor = None
	else:
	    flavor = None

	self.streamCache[(fileObj.id(), versionId)] = \
		(fileObj.freeze(), flavor)

    def getFile(self, fileId, fileVersion, pristine = False):
	versionId = self.versionTable[fileVersion]
	stream = self.troveFiles.getFileByFileId(fileId, versionId,
						 justPresent = not pristine)[1]
	return files.ThawFile(stream, fileId)

    def getTrove(self, troveName, troveVersion, troveFlavor, pristine = False):
	return self._getTrove(troveName = troveName, 
			      troveVersion = troveVersion, 
			      troveFlavor = troveFlavor,
			      pristine = pristine)

    def _getTrove(self, pristine, troveName = None, troveInstanceId = None, 
		  troveVersion = None, troveVersionId = None,
		  troveFlavor = 0, troveFlavorId = None):
	if not troveName:
	    (troveName, troveVersionId, troveFlavorId) = \
		    self.instances.getId(troveInstanceId)[0:3]

	if not troveVersionId:
	    troveVersionId = self.versionTable[troveVersion]

	if troveFlavorId is None:
	    if troveFlavor is None:
		troveFlavorId = 0
	    else:
		troveFlavorId = self.flavors[troveFlavor]
	
	if troveFlavor == 0:
	    if troveFlavorId == 0:
		troveFlavor = None
	    else:
		troveFlavor = self.flavors.getId(troveFlavorId)

	if not troveInstanceId:
	    troveInstanceId = self.instances.get((troveName, 
			    troveVersionId, troveFlavorId), None)
	    if troveInstanceId is None:
		raise KeyError, troveName

	if not troveVersion or min(troveVersion.timeStamps()) == 0:
	    troveVersion = self.instances.getVersion(troveInstanceId)

	trove = package.Trove(troveName, troveVersion, troveFlavor)

	flavorCache = {}

	# add all of the troves which are references from this trove; the
	# flavor cache is already complete
	cu = self.db.cursor()
	cu.execute("""
	    SELECT troveName, versionId, timeStamps, DBFlavors.flavorId, flavor FROM 
		TroveTroves JOIN DBInstances JOIN DBFlavors ON 
		    TroveTroves.includedId = DBInstances.instanceId AND
		    DBFlavors.flavorId = DBInstances.flavorId 
		WHERE TroveTroves.instanceId = %d
	""", troveInstanceId)

	versionCache = {}
	for (name, versionId, timeStamps, flavorId, flavorStr) in cu:
	    version = self.versionTable.getBareId(versionId)
	    version.setTimeStamps([ float(x) for x in timeStamps.split(":") ])

	    if not flavorId:
		flavor = None
	    else:
		flavor = flavorCache.get(flavorId, None)
		if flavor is None:
		    flavor = deps.deps.ThawDependencySet(flavorStr)
		    flavorCache[flavorId] = flavor

	    trove.addTrove(name, version, flavor)

	cu = self.db.cursor()
	cu.execute("SELECT fileId, path, versionId, isPresent FROM "
		   "DBTroveFiles WHERE instanceId = %d", troveInstanceId)
	for (fileId, path, versionId, isPresent) in cu:
	    if not pristine and not isPresent:
		continue
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getBareId(versionId)
		versionCache[versionId] = version

	    trove.addFile(fileId, path, version)

	return trove

    def eraseTrove(self, troveName, troveVersion, troveFlavor):
	troveVersionId = self.versionTable[troveVersion]
	if troveFlavor is None:
	    troveFlavorId = 0
	else:
	    troveFlavorId = self.flavors[troveFlavor]
	troveInstanceId = self.instances[(troveName, troveVersionId, 
					  troveFlavorId)]

	self.troveFiles.delInstance(troveInstanceId)
	del self.troveTroves[troveInstanceId]

	# mark this trove as not present
	self.instances.setPresent(troveInstanceId, 0)
	self.needsCleanup = True

    def commit(self):
	if self.needsCleanup:
	    # this join could be slow; it would be much better if we could
	    # restrict the select on DBInstances by instanceId, but that's
	    # not so easy and may require multiple passes (since we may
	    # now be able to remove a trove which was included by a trove
	    # which was included by a trove which was removed; getting that
	    # closure may have to be iterative?). that process may be faster 
	    # then the full join?
	    cu = self.db.cursor()
	    cu.execute("""
		DELETE FROM DBInstances WHERE instanceId IN 
		    (SELECT DBInstances.instanceId FROM 
			DBInstances LEFT OUTER JOIN TroveTroves 
			ON DBInstances.instanceId = troveTroves.includedId 
			WHERE isPresent = 0 AND troveTroves.includedId is NULL
		    );
		""")
	    self.needCleanup = False

	self.db.commit()
	self.addVersionCache = {}
	self.flavorsNeeded = {}
	
    def pathIsOwned(self, path):
	for instanceId in self.troveFiles.iterPath(path):
	    if self.instances.idIsPresent(instanceId):
		return True

	return False

    def iterFindByPath(self, path, pristine = False):
	for instanceId in self.troveFiles.iterPath(path):
	    if not self.instances.idIsPresent(instanceId):
		continue

	    trv = self._getTrove(troveInstanceId = instanceId, 
				 pristine = pristine)
	    yield trv

    def removeFileFromTrove(self, trove, path):
	versionId = self.versionTable[trove.getVersion()]
        flavorId = self.flavors[trove.getFlavor()]
	instanceId = self.instances[(trove.getName(), versionId, flavorId)]
	self.troveFiles.removePath(instanceId, path)

    def removeFilesFromTrove(self, troveName, troveVersion, troveFlavor, fileIdList):
	versionId = self.versionTable[troveVersion]
        flavorId = self.flavors[troveFlavor]
	instanceId = self.instances[(troveName, versionId, flavorId)]
	self.troveFiles.removeFileIds(instanceId, fileIdList)

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False,
			 pristine = False):
	if sortByPath:
	    sort = " ORDER BY path";
	else:
	    sort =""
	cu = self.db.cursor()

	troveVersionId = self.versionTable[version]
	if flavor is None:
	    troveFlavorId = 0
	else:
	    troveFlavorId = self.flavors[flavor]
	troveInstanceId = self.instances[(troveName, troveVersionId, 
					  troveFlavorId)]
	versionCache = {}

	if pristine:
	    cu.execute("SELECT fileId, path, versionId, stream FROM "
		       "DBTroveFiles WHERE instanceId = %%d "
		       "%s" % sort, troveInstanceId)
	else:
	    cu.execute("SELECT fileId, path, versionId, stream FROM "
		       "DBTroveFiles WHERE instanceId = %%d "
		       "AND isPresent=1 %s" % sort, troveInstanceId)

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

    def close(self):
	return
	self.db.close()
