from repository.localrep import idtable
from repository.localrep import instructionsets
from repository.localrep import trovecontents
from repository.localrep import versionops
import sqlite
import package
import files
import time

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
		       "WHERE fileId=%s AND versionId=%d", fileId, versionId)
	# there could be multiple matches, but they should all be redundant
	try:
	    return cu.next()
	except StopIteration:
            raise KeyError, (fileId, versionId)

    def addItem(self, fileId, versionId, path, instanceId, stream):
        cu = self.db.cursor()
        cu.execute("INSERT INTO DBTroveFiles VALUES (%s, %d, %s, %d, %d, %s)",
                   (fileId, versionId, path, instanceId, 1,
		    sqlite.encode(stream)))

    def updateItem(self, instanceId, fileId, oldVersionId, newVersionId, 
		   newStream):
        cu = self.db.cursor()
	cu.execute("UPDATE DBTroveFiles SET versionId=%d, stream=%s "
		   "WHERE fileId=%s AND versionId=%d AND instanceId=%d",
		   newVersionId, sqlite.encode(newStream), fileId, 
		   oldVersionId, instanceId)

    def iterPath(self, path):
        cu = self.db.cursor()
	cu.execute("SELECT instanceId FROM DBTroveFiles WHERE path=%s", path)
	for instanceId in cu:
	    yield instanceId[0]

    def removePath(self, instanceId, path):
        cu = self.db.cursor()
	cu.execute("UPDATE DBTroveFiles SET isPresent=0 WHERE path=%s "
		   "AND instanceId=%d", (path, instanceId))

    def removeFileIds(self, instanceId, fileIdList):
        cu = self.db.cursor()
	cu.execute("UPDATE DBTroveFiles SET isPresent=0 WHERE instanceId=%d "
		   "AND fileId in (%s)" % (instanceId,
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
				insSetId INT,
				useId INT,
				isPresent INT)""")
	    cu.execute("CREATE INDEX InstancesNameIdx ON "
		       "DBInstances(troveName)")
	    cu.execute("CREATE UNIQUE INDEX InstancesIdx ON "
		       "DBInstances(troveName, versionId, insSetId, useId)")

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
	cu.execute("SELECT instanceId, versionId troveName FROM DBInstances "
		   "WHERE troveName=%s AND isPresent = 1", name)
 	for match in cu:
	    yield match

    def addId(self, troveName, versionId, insSetId, useId):
        cu = self.db.cursor()
        cu.execute("INSERT INTO DBInstances VALUES (NULL, %s, %d, %d, %d, %d)",
                   (troveName, versionId, insSetId, useId, 1))
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

        cu.execute("SELECT troveName, versionId, insSetId, useId, isPresent "
		   "FROM DBInstances WHERE instanceId=%%d %s" % pres, theId)
	try:
	    return cu.next()
	except StopIteration:
            raise KeyError, theId

    def isPresent(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT isPresent FROM DBInstances WHERE "
			"troveName=%s AND versionId=%d AND "
			"insSetId=%d AND useId=%d", item)

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
			"insSetId=%d AND useId=%d", item)
	return not(cu.fetchone() == None)

    def __getitem__(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM DBInstances WHERE "
			"troveName=%s AND versionId=%d AND "
			"insSetId=%d AND useId=%d", item)
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, item

    def get(self, item, defValue, justPresent = True):
        cu = self.db.cursor()

	if justPresent:
	    pres = "AND isPresent=1"
	else:
	    pres = ""

        cu.execute("SELECT instanceId FROM DBInstances WHERE "
			"troveName=%%s AND versionId=%%d AND "
			"insSetId=%%d AND useId=%%d %s" % pres, item)
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

class Database:

    def __init__(self, path):
	self.db = sqlite.connect(path)
	self.troveTroves = trovecontents.TroveTroves(self.db)
	self.troveFiles = DBTroveFiles(self.db)
	self.instances = DBInstanceTable(self.db)
	self.versionTable = versionops.VersionTable(self.db)
	self.streamCache = {}
	self.needsCleanup = False
	self.addVersionCache = {}

    def __del__(self):
	self.db.close()
        del self.db

    def iterAllTroveNames(self):
	return self.instances.iterNames()

    def iterFindByName(self, name, pristine = False):
	for (instanceId, versionId) in self.instances.iterByName(name):
	    yield self._getTrove(troveName = name,
				 troveInstanceId = instanceId, 
				 troveVersionId = versionId,
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

    def getInstanceId(self, troveName, versionId, insSetId, useId):
	theId = self.instances.get((troveName, versionId, insSetId, useId), 
				   None)
	if theId is None:
	    theId = self.instances.addId(troveName, versionId, insSetId, useId)

	return theId

    def addTrove(self, trove, local = False, oldVersion = None):
	troveName = trove.getName()
	troveVersion = trove.getVersion()
	troveVersionId = self.getVersionId(troveVersion, {})
	self.addVersionCache[troveVersion] = troveVersionId

	# the instance may already exist (it could be referenced by a package
	# which has already been added, or it may be in the database as
	# not present)
	troveInstanceId = self.instances.get((troveName, troveVersionId, 0, 0),
					     None, justPresent = False)
	if troveInstanceId:
	    self.instances.setPresent(troveInstanceId, 1)
	else:
	    troveInstanceId = self.instances.addId(troveName, troveVersionId, 
						  0, 0)
	
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
	    oldInstanceId = self.instances[(troveName, oldVersionId, 0, 0)]
	    cu.execute("SELECT fileId, versionId FROM DBTroveFiles "
		       "WHERE instanceId=%d", oldInstanceId)
	    for (fileId, versionId) in cu:
		existingFiles[fileId] = versionId

	    cu.execute("UPDATE DBTroveFiles SET instanceId=%d WHERE "
		       "instanceId=%d", troveInstanceId, oldInstanceId)

	for (fileId, path, version) in trove.iterFileList():
	    versionId = self.getVersionId(version, self.addVersionCache)

	    existingId = existingFiles.get(fileId, None)
	    if existingId == versionId:
		# the file is in the table from a previous version, and
		# hasn't changed. nothing to do here.
		del existingFiles[fileId]
		continue

	    if local:
		# this is on the disk
		stream = ""
	    else:
		stream = self.streamCache.get((fileId, versionId), None)
		if stream is None:
		    stream = self.troveFiles.getFileByFileId(fileId, 
							     versionId)[1]
		else:
		    del self.streamCache[(fileId, versionId)]

	    if existingId:
		# existing file, new troveInstanceId and 
		del existingFiles[fileId]
		self.troveFiles.updateItem(troveInstanceId, fileId, existingId,
					   versionId, stream)
	    else:
		self.troveFiles.addItem(fileId, versionId, path, 
				       troveInstanceId, stream)

	if existingFiles:
	    self.troveFiles.removeFileIds(troveInstanceId,
					  existingFiles.iterkeys())

	for (name, version) in trove.iterPackageList():
	    versionId = self.getVersionId(version, self.addVersionCache)
	    instanceId = self.getInstanceId(name, versionId, 0, 0)
	    self.troveTroves.addItem(troveInstanceId, instanceId)

    def addFile(self, file, fileVersion):
	versionId = self.getVersionId(fileVersion, self.addVersionCache)
	self.streamCache[(file.id(), versionId)] = file.freeze()

    def getFile(self, fileId, fileVersion, pristine = False):
	versionId = self.versionTable[fileVersion]
	stream = self.troveFiles.getFileByFileId(fileId, versionId,
						 justPresent = not pristine)[1]
	return files.ThawFile(stream, fileId)

    def getTrove(self, troveName, troveVersion, pristine = False):
	return self._getTrove(troveName = troveName, 
			      troveVersion = troveVersion, 
			      pristine = pristine)

    def _getTrove(self, pristine, troveName = None, troveInstanceId = None, 
		  troveVersion = None, troveVersionId = None):
	if not troveName:
	    (troveName, troveVersionId) = \
		    self.instances.getId(troveInstanceId)[0:2]

	if not troveVersion:
	    troveVersion = self.versionTable.getId(troveVersionId)

	if not troveVersionId:
	    troveVersionId = self.versionTable[troveVersion]

	if not troveInstanceId:
	    troveInstanceId = self.instances.get((troveName, 
			    troveVersionId, 0, 0), None)
	    if troveInstanceId is None:
		raise KeyError, troveName

	if not troveVersion.timeStamp:
	    troveVersion.timeStamp = \
		    self.versionTable.getTimestamp(troveVersionId)

	trove = package.Trove(troveName, troveVersion)
	versionCache = {}
	for instanceId in self.troveTroves[troveInstanceId]:
	    (name, versionId, insSetId, useId, isPresent) = \
		    self.instances.getId(instanceId, justPresent = False)
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getId(versionId)
		versionCache[versionId] = version

	    trove.addPackageVersion(name, version)

	cu = self.db.cursor()
	cu.execute("SELECT fileId, path, versionId, isPresent FROM "
		   "DBTroveFiles WHERE instanceId = %d", troveInstanceId)
	for (fileId, path, versionId, isPresent) in cu:
	    if not pristine and not isPresent:
		continue
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getId(versionId)
		versionCache[versionId] = version

	    trove.addFile(fileId, path, version)

	return trove

    def eraseTrove(self, troveName, troveVersion):
	troveVersionId = self.versionTable[troveVersion]
	troveInstanceId = self.instances[(troveName, troveVersionId, 0, 0)]

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
	instanceId = self.instances[(trove.getName(), versionId, 0, 0)]
	self.troveFiles.removePath(instanceId, path)

    def removeFilesFromTrove(self, troveName, troveVersion, fileIdList):
	versionId = self.versionTable[troveVersion]
	instanceId = self.instances[(troveName, versionId, 0, 0)]
	self.troveFiles.removeFileIds(instanceId, fileIdList)

    def iterFilesInTrove(self, trove, sortByPath = False, withFiles = False,
			 pristine = False):
	if sortByPath:
	    sort = " ORDER BY path";
	else:
	    sort =""
	cu = self.db.cursor()

	troveVersionId = self.versionTable[trove.getVersion()]
	troveInstanceId = self.instances[(trove.getName(), troveVersionId, 
					  0, 0)]
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
		version = self.versionTable.getId(versionId)
		versionCache[versionId] = version

	    if withFiles:
		fileObj = files.ThawFile(stream, fileId)
		yield (fileId, path, version, fileObj)
	    else:
		yield (fileId, path, version)

    def close(self):
	return
	self.db.close()
