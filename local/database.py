#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

from repository import changeset
from repository import filecontents
from repository import fsrepos
import log
import localrep
import os
from repository import repository
import sqldb
import update
import util
import versions

class RootChangeSetJob(fsrepos.ChangeSetJob):

    def addPackage(self, pkg):
	self.packages.append(pkg)

    def newPackageList(self):
	return self.packages

    def oldPackage(self, pkg):
	self.oldPackages.append(pkg)

    def oldPackageList(self):
	return self.oldPackages

    def oldFile(self, fileId, fileVersion, fileObj):
	self.oldFiles.append((fileId, fileVersion, fileObj))

    def oldFileList(self):
	return self.oldFiles

    def addFile(self, fileObject):
	self.files[fileObject.fileId()] = fileObject

    def getFile(self, fileId):
	return self.files[fileId]

    def newFileList(self):
	return self.files.keys()

    def __init__(self, repos, absCs):
	self.packages = []
	self.oldPackages = []
	self.oldFiles = []
	self.files = {}
	fsrepos.ChangeSetJob.__init__(self, repos, absCs)

class SqlDbRepository(repository.DataStoreRepository,
		      repository.AbstractRepository):

    def iterAllTroveNames(self):
	return self.db.iterAllTroveNames()

    def getTrove(self, name, version, flavor, pristine = False):
	l = [ x for x in self.db.iterFindByName(name, pristine = pristine)
		 if version == x.getVersion() and flavor == x.getFlavor()]
	if not l:
	    raise repository.PackageMissing(name, version)
	assert(len(l) == 1)
	return l[0]

    def getTroveLatestVersion(self, name, branch):
	l = [ x.getVersion() for x in self.db.iterFindByName(name)
		     if branch == x.getVersion().branch() ]
	if not l:
	    return None

	return l[0]

    def pkgVersionFlavors(self, pkgName, version):
	l = [ x.getFlavor() for x in self.db.iterFindByName(pkgName)
		     if version == x.getVersion() ]

	return l

    def hasPackage(self, name):
	return self.db.hasByName(name)

    def hasTrove(self, pkgName, version, flavor):
	for x in self.db.iterFindByName(pkgName):
	     if version == x.getVersion() and flavor == x.getFlavor():
		return True

	return False

    def getPackageVersionList(self, name):
	"""
	Returns a list of all of the versions of a trove available
	in the repository.

	@param name: trove
	@type name: str
	@rtype: list of versions.Version
	"""
	return [ x.getVersion() for x in self.db.iterFindByName(name) ]

    def getFileVersion(self, fileId, version, withContents = 0):
	file = self.db.getFile(fileId, version, pristine = True)
	if withContents:
	    if file.hasContents:
		cont = filecontents.FromRepository(self, file.contents.sha1(), 
						   file.contents.size())
	    else:
		cont = None

	    return (file, cont)

	return file

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False,
			 pristine = False):
	return self.db.iterFilesInTrove(troveName, version, flavor,
                                        sortByPath = sortByPath, 
                                        withFiles = withFiles,
                                        pristine = pristine)

    def addFileVersion(self, fileId, version, file):
	self.db.addFile(file, version)

    def addPackage(self, pkg, oldVersion = None):
	self.db.addTrove(pkg, oldVersion = oldVersion)

    def commit(self):
	self.db.commit()

    def close(self):
	self.db.close()

    def eraseTrove(self, pkgName, version, flavor):
	self.db.eraseTrove(pkgName, version, flavor)

    def eraseFileVersion(self, fileId, version):
	# files get removed with their troves
	pass

    def __init__(self, path):
	repository.DataStoreRepository.__init__(self, path)
	repository.AbstractRepository.__init__(self)
	self.db = sqldb.Database(path + "/srsdb")

class Database(SqlDbRepository):

    # XXX some of these interfaces are horribly inefficient as we have
    # to instantiate a full package object to do anything... 
    # FilesystemRepository has the same problem

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
	return SqlDbRepository.iterFilesInTrove(self, troveName, version,
			flavor, sortByPath = sortByPath,
			withFiles = withFiles, pristine = False)

    # takes an absolute change set and creates a differential change set 
    # against a branch of the repository
    def rootChangeSet(self, absSet):
	assert(absSet.isAbsolute())

	# this has an empty source path template, which is only used to
	# construct the eraseFiles list anyway
	
	# we don't use our localrep.ChangeSetJob here as it can't deal with
	# absolute change sets
	job = RootChangeSetJob(self, absSet)

	# absolute change sets cannot have eraseLists
	#assert(not eraseList)
	#assert(not eraseFiles)

	cs = changeset.ChangeSetFromAbsoluteChangeSet(absSet)

	for (name, version, flavor) in absSet.getPrimaryPackageList():
	    cs.addPrimaryPackage(name, version, flavor)

	oldVersion = None

	for newPkg in job.newPackageList():
	    pkgName = newPkg.getName()
	    oldVersions = self.getPackageVersionList(pkgName)
	    if len(oldVersions) > 1:
		# try and pick the one which looks like a good match
		# for the new version
		newBranch = newPkg.getNewVersion().branch()
		for ver in oldVersions:
		    if ver.branch() == newBranch:
			# make sure it's the right flavor
			flavors = self.pkgVersionFlavors(pkgName, ver)
			if newPkg.getFlavor() in flavors:
			    oldVersion = ver
			    break

		if not oldVersion:
		    raise AmbiguousOperation
	    elif oldVersions:
		# make sure it's the right flavor
		flavors = self.pkgVersionFlavors(pkgName, oldVersions[0])
		if newPkg.getFlavor() in flavors:
		    oldVersion = oldVersions[0]

	    if not oldVersion:
		# new package; the Package.diff() right after this never
		# sets the absolute flag, so the right thing happens
		old = None
	    else:
		old = self.getTrove(pkgName, oldVersion, newPkg.getFlavor(),
					     pristine = True)

	    # we ignore pkgsNeeded; it doesn't mean much in this case
	    (pkgChgSet, filesNeeded, pkgsNeeded) =	    \
		    newPkg.diff(old, absolute = 0)
	    cs.newPackage(pkgChgSet)

	    for (fileId, oldVersion, newVersion, newPath) in filesNeeded:
		fileObj = job.getFile(fileId)
		assert(newVersion == fileObj.version())
		
		oldFile = None
		if oldVersion:
		    (oldFile, oldCont) = self.getFileVersion(fileId, 
					    oldVersion, withContents = 1)

		(filecs, hash) = changeset.fileChangeSet(fileId, oldFile, 
							 fileObj.file())

		cs.addFile(fileId, oldVersion, newVersion, filecs)
		if hash: 
		    contType = changeset.ChangedFileTypes.file
		    cont = filecontents.FromChangeSet(absSet, fileId)
		    if oldVersion:
			(contType, cont) = changeset.fileContentsDiff(oldFile, 
				    oldCont, fileObj.file(), cont)

		    cs.addFileContents(fileId, contType, cont, 
					fileObj.file().flags.isConfig())

	assert(not cs.validate())

	return cs

    # local changes includes the A->A.local portion of a rollback; if it
    # doesn't exist we need to compute that and save a rollback for this
    # transaction
    def commitChangeSet(self, cs, isRollback = False, toStash = True,
                        replaceFiles = False):
	assert(not cs.isAbsolute())
        flags = 0
        if replaceFiles:
            flags |= update.REPLACEFILES

	for pkg in cs.iterNewPackageList():
	    if pkg.getName().endswith(":source"): raise SourcePackageInstall

	# Make sure this change set doesn't unintentionally restore troves
	# which have been removed.  take a look at which packages were removed
	# from the primary packages, and remove those packages from the change
	# set as well. Bleah.
	#
	# XXX This is expensive; we need hash's of version/name
	# pairs. it also isn't quite right, as change sets
	# can't actually store multiple versions of the same
	# trove
	remove = {}
	for (name, version, flavor) in cs.getPrimaryPackageList():
	    try:
		pkgCs = cs.getNewPackageVersion(name, version, flavor)
	    except KeyError:
		continue

	    oldVersion = pkgCs.getOldVersion()
	    if not oldVersion: continue

	    pristine = self.getTrove(name, oldVersion, flavor, pristine = True)
	    changed = self.getTrove(name, oldVersion, flavor)

	    for (subName, subVersion, subFlavor) in pristine.iterTroveList():
		if not changed.hasTrove(subName, subVersion, subFlavor):
		    remove[(subName, version, subFlavor)] = True

	for (name, version, flavor) in remove.iterkeys():
	    cs.delNewPackage(name, version, flavor)

	# create the change set from A->A.local
	pkgList = []
	for newPkg in cs.iterNewPackageList():
	    name = newPkg.getName()
	    old = newPkg.getOldVersion()
	    flavor = newPkg.getFlavor()
	    if self.hasPackage(name) and old:
		ver = old.fork(versions.LocalBranch(), sameVerRel = 1)
		pkg = self.getTrove(name, old, flavor)
		origPkg = self.getTrove(name, old, flavor, pristine = 1)
		assert(pkg)
		pkgList.append((pkg, origPkg, ver))

	result = update.buildLocalChanges(self, pkgList, root = self.root)
	if not result: return

	(localChanges, retList) = result
	fsPkgDict = {}
	for (changed, fsPkg) in retList:
	    fsPkgDict[fsPkg.getName()] = fsPkg

	if not isRollback:
	    inverse = cs.makeRollback(self, configFiles = 1)
            flags |= update.MERGE

	# build the list of changes to the filesystem
	fsJob = update.FilesystemJob(self, cs, fsPkgDict, self.root, 
				     flags = flags)

	# look through the directories which have had files removed and
	# see if we can remove the directories as well
	set = fsJob.getDirectoryCountSet()
	list = set.keys()
	list.sort()
	list.reverse()
	directoryCandidates = {}
	while (list):
	    path = list[0]
	    del list[0]
	    entries = len(os.listdir(path))
	    entries -= set[path]

	    # listdir excludes . and ..
	    if (entries) != 0: continue

	    directoryCandidates[path] = True

	    parent = os.path.dirname(path)
	    if set.has_key(parent):
		set[parent] += 1
	    else:
		set[parent] = 1
		list.append(parent)
		# insertion is linear, sort is n log n
		# oh well.
		list.sort()
		list.reverse()

	# -------- database and system are updated below this line ---------

	# Build A->B
	if toStash:
	    # this updates the database from the changeset; the change
	    # isn't committed until the self.commit below
	    # an object for historical reasons
	    localrep.LocalRepositoryChangeSetJob(self, cs)

	errList = fsJob.getErrorList()
	if errList:
	    for err in errList: log.error(err)
	    # FIXME need a --force for this
	    return

	# everything is in the database... save this so we can undo
	# it later. 
	if not isRollback:
	    self.addRollback(inverse, localChanges)

	fsJob.apply()

	for (troveName, troveVersion, troveFlavor, fileIdList) in fsJob.iterUserRemovals():
	    self.db.removeFilesFromTrove(troveName, troveVersion, troveFlavor, fileIdList)

	for (name, version, flavor) in fsJob.getOldPackageList():
	    if toStash:
		# if to stash if false, we're restoring the local
		# branch of a rollback
		self.db.eraseTrove(name, version, flavor)

	self.commit()

	# finally, remove old directories. right now this has to be done
	# after the sqldb has been updated
	list = directoryCandidates.keys()
	list.sort()
	list.reverse()
	keep = {}
	for path in list:
	    if keep.has_key(path):
		keep[os.path.dirname(path)] = True
		continue

	    relativePath = path[len(self.root):]
	    if relativePath[0] != '/': relativePath = '/' + relativePath
	    
	    if self.db.pathIsOwned(relativePath):
		list = [ x for x in self.db.iterFindByPath(path)]
		keep[os.path.dirname(path)] = True
		continue

	    os.rmdir(path)

    def removeFile(self, path, multipleMatches = False):
	if not multipleMatches:
	    # make sure there aren't too many
	    count = 0
	    for trv in self.db.iterFindByPath(path):
		count += 1
		if count > 1: 
		    raise DatabaseError, "multiple troves own %s" % path

	for trv in self.db.iterFindByPath(path):
	    self.db.removeFileFromTrove(trv, path)

	self.db.commit()

    def addRollback(self, reposChangeset, localChangeset):
	rpFn = self.rollbackCache + ("/rb.r.%d" % (self.lastRollback + 1))
	reposChangeset.writeToFile(rpFn)

	localFn = self.rollbackCache + ("/rb.l.%d" % (self.lastRollback + 1))
	localChangeset.writeToFile(localFn)

	self.lastRollback += 1
	self.writeRollbackStatus()

    # name looks like "r.%d"
    def removeRollback(self, name):
	rollback = int(name[2:])
	os.unlink(self.rollbackCache + "/rb.r.%d" % rollback)
	os.unlink(self.rollbackCache + "/rb.l.%d" % rollback)
	if rollback == self.lastRollback:
	    self.lastRollback -= 1
	    self.writeRollbackStatus()

    def writeRollbackStatus(self):
	newStatus = self.rollbackCache + ".new"

	f = open(newStatus, "w")
	f.write("%s %d\n" % (self.firstRollback, self.lastRollback))
	f.close()

	os.rename(newStatus, self.rollbackStatus)

    def getRollbackList(self):
	list = []
	for i in range(self.firstRollback, self.lastRollback + 1):
	    list.append("r.%d" % i)

	return list

    def readRollbackStatus(self):
	f = open(self.rollbackStatus)
	(first, last) = f.read()[:-1].split()
	self.firstRollback = int(first)
	self.lastRollback = int(last)
	f.close()

    def hasRollback(self, name):
	try:
	    num = int(name[2:])
	except ValueError:
	    return False

	if (num >= self.firstRollback and num <= self.lastRollback):
	    return True
	
	return False

    def getRollback(self, name):
	if not self.hasRollback(name): return None

	num = int(name[2:])

	rc = []
	for ch in [ "r", "l" ]:
	    name = self.rollbackCache + "/" + "rb.%c.%d" % (ch, num)
	    rc.append(changeset.ChangeSetFromFile(name,
						  justContentsForConfig = 1))

	return rc

    def applyRollbackList(self, names):
	last = self.lastRollback
	for name in names:
	    if not self.hasRollback(name):
		raise RollbackDoesNotExist(name)

	    num = int(name[2:])
	    if num != last:
		raise RollbackOrderError(name)
	    last -= 1

	for name in names:
	    (reposCs, localCs) = self.getRollback(name)
	    self.commitChangeSet(reposCs, isRollback = True)
	    self.commitChangeSet(localCs, isRollback = True, toStash = False)
	    self.removeRollback(name)
    
    def findTrove(self, troveName, versionStr = None):
	versionList = self.getPackageVersionList(troveName)

	if versionStr:
	    # filter the list of versions based on versionStr
	    if versionStr[0] == '/':
		version = versions.VersionFromString(versionStr)
		versionList = [ v for v in versionList if v == version ]
	    elif versionStr.find('@') != -1:
		versionList = [ v for v in versionList if 
				str(v.branch().label()) == versionStr ]
	    else:
		verRel = versions.VersionRelease(versionStr)
		try:
		    verRel = versions.VersionRelease(versionStr)
		except:
		    log.error("unknown version string: %s", versionStr)
		    return

		versionList = [ v for v in versionList if 
					v.trailingVersion() == verRel ]

	pkgList = []
	for version in versionList:
	    for flavor in self.pkgVersionFlavors(troveName, version):
		pkgList.append(self.getTrove(troveName, version, flavor))

	if not pkgList:
	    raise repository.PackageNotFound

	return pkgList

    def __init__(self, root, path):
	self.root = root

	top = util.joinPaths(root, path)

	self.rollbackCache = top + "/rollbacks"
	self.rollbackStatus = self.rollbackCache + "/status"
	if not os.path.exists(self.rollbackCache):
	    util.mkdirChain(self.rollbackCache)
	if not os.path.exists(self.rollbackStatus):
	    self.firstRollback = 0
	    self.lastRollback = -1
	    self.writeRollbackStatus()
	else:
	    self.readRollbackStatus()

	SqlDbRepository.__init__(self, root + path)

# Exception classes

class DatabaseError(Exception):
    """Base class for exceptions from the system database"""

    def __str__(self):
	return self.str

    def __init__(self, str = None):
	self.str = str

class RollbackError(Exception):

    """Base class for exceptions related to applying rollbacks"""

class RollbackOrderError(RollbackError):

    """Raised when an attempt is made to apply rollbacks in the
       wrong order"""

    def __str__(self):
	return "rollback %s can not be applied out of order" % self.name

    def __init__(self, rollbackName):
	"""Create new new RollbackOrderError
	@param rollbackName: string represeting the name of the rollback
	which was trying to be applied out of order"""
	self.name = rollbackName

class RollbackDoesNotExist(RollbackError):

    """Raised when the system tries to access a rollback which isn't in
       the database"""

    def __str__(self):
	return "rollback %s does not exist" % self.name

    def __init__(self, rollbackName):
	"""Create new new RollbackOrderError
	@param rollbackName: string represeting the name of the rollback
	which does not exist"""
	self.name = rollbackName

class SourcePackageInstall(DatabaseError):

    def __str__(self):
	return "cannot install a source package onto the local system"

class AmbigiousOperation(DatabaseError):

    pass
