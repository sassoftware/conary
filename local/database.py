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

class SqlDbRepository(repository.DataStoreRepository):

    def iterAllTroveNames(self):
	return self.db.iterAllTroveNames()

    def getPackageBranchList(self, name):
	return [ x.getVersion().branch() for x in 
		    self.db.iterFindByName(name)]

    def getPackageVersion(self, name, version, pristine = False):
	l = [ x for x in self.db.iterFindByName(name, pristine = pristine)
		 if version == x.getVersion()]
	if not l:
	    raise repository.PackageMissing(name, version)
	assert(len(l) == 1)
	return l[0]

    def pkgLatestVersion(self, name, branch):
	l = [ x.getVersion() for x in self.db.iterFindByName(name)
		     if branch == x.getVersion().branch()]
	if not l:
	    return None

	return l[0]

    def hasPackage(self, name):
	return self.db.hasByName(name)

    def hasPackageVersion(self, pkgName, version):
	for x in self.db.iterFindByName(pkgName):
	     if version == x.getVersion():
		return True

	return False

    def branchesOfTroveLabel(self, name, label):
	rc = []
	for x in self.db.iterFindByName(name):
	    b = x.getVersion().branch()
	    if b.label() == label:
		rc.append(b)

	return rc

    def getLatestPackage(self, name, branch, pristine = False):
	return [ x for x in self.db.iterFindByName(name, pristine = pristine)
		     if branch == x.getVersion().branch()][0]

    def getPackageVersionList(self, name):
	return [ x.getVersion() for x in self.db.iterFindByName(name) ]

    def getFileVersion(self, fileId, version, path = None, withContents = 0):
	file = self.db.getFile(fileId, version, pristine = True)
	if withContents:
	    if file.hasContents:
		cont = filecontents.FromRepository(self, file.contents.sha1(), 
						   file.contents.size())
	    else:
		cont = None

	    return (file, cont)

	return file

    def getFileContents(self, file):
	return filecontents.FromRepository(self, file.contents.sha1(), 
					   file.contents.size())

    def iterFilesInTrove(self, trove, sortByPath = False, withFiles = False,
			 pristine = False):
	return self.db.iterFilesInTrove(trove, sortByPath = sortByPath, 
				withFiles = withFiles, pristine = pristine)

    def buildJob(self, changeSet):
	return localrep.LocalRepositoryChangeSetJob(self, changeSet)

    def storeFileFromContents(self, contents, file, restoreContents):
	"""
	this is called when a Repository wants to store a file; we store
	config files only (since we made to patch them later)
	"""
	if file.flags.isConfig():
	    return repository.DataStoreRepository.storeFileFromContents(self, 
				contents, file, restoreContents)

    def addFileVersion(self, fileId, version, file):
	self.db.addFile(file, version)

    def addPackage(self, pkg, oldVersion = None):
	self.db.addTrove(pkg, oldVersion = oldVersion)

    def commit(self):
	self.db.commit()

    def close(self):
	self.db.close()

    def erasePackageVersion(self, pkgName, version):
	self.db.eraseTrove(pkgName, version)

    def eraseFileVersion(self, fileId, version):
	# files get removed with their troves
	pass

    def __init__(self, path):
	repository.DataStoreRepository.__init__(self, path)
	self.db = sqldb.Database(path + "/sql")

class AbstractDatabase(SqlDbRepository):

    # XXX some of these interfaces are horribly inefficient as we have
    # to instantiate a full package object to do anything... 
    # FilesystemRepository has the same problem

    def iterFilesInTrove(self, trove, sortByPath = False, withFiles = False):
	return SqlDbRepository.iterFilesInTrove(self, trove, 
			sortByPath = sortByPath, withFiles = withFiles, 
			pristine = False)

    # takes an absolute change set and creates a differential change set 
    # against a branch of the repository
    def rootChangeSet(self, absSet, branch):
	assert(absSet.isAbsolute())

	# this has an empty source path template, which is only used to
	# construct the eraseFiles list anyway
	
	# we don't use our buildJob here as it can't deal with
	# absolute change sets
	job = RootChangeSetJob(self, absSet)

	# absolute change sets cannot have eraseLists
	#assert(not eraseList)
	#assert(not eraseFiles)

	cs = changeset.ChangeSetFromAbsoluteChangeSet(absSet)

	for (name, version) in absSet.getPrimaryPackageList():
	    cs.addPrimaryPackage(name, version)

	for newPkg in job.newPackageList():
	    # FIXME
	    #
	    # this shouldn't be against branch, it should be against
	    # the version of the package already installed on the
	    # system. unfortunately we can't represent that yet. 
	    pkgName = newPkg.getName()
	    oldVersion = self.pkgLatestVersion(pkgName, branch)
	    if not oldVersion:
		# new package; the Package.diff() right after this never
		# sets the absolute flag, so the right thing happens
		old = None
	    else:
		old = self.getPackageVersion(pkgName, oldVersion, 
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
	for (name, version) in cs.getPrimaryPackageList():
	    try:
		pkgCs = cs.getNewPackageVersion(name, version)
	    except KeyError:
		continue

	    oldVersion = pkgCs.getOldVersion()
	    if not oldVersion: continue

	    pristine = self.getPackageVersion(name, oldVersion, 
					      pristine = True)
	    changed = self.getPackageVersion(name, oldVersion)

	    for (subName, subVersion) in pristine.iterPackageList():
		if not changed.hasPackageVersion(subName, subVersion):
		    remove[(subName, version)] = True

	for (name, version) in remove.iterkeys():
	    cs.delNewPackage(name, version)

	# create the change set from A->A.local
	pkgList = []
	for newPkg in cs.iterNewPackageList():
	    name = newPkg.getName()
	    old = newPkg.getOldVersion()
	    if self.hasPackage(name) and old:
		ver = old.fork(versions.LocalBranch(), sameVerRel = 1)
		pkg = self.getPackageVersion(name, old)
		origPkg = self.getPackageVersion(name, old, pristine = 1)
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
	    self.buildJob(cs)

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

	for (troveName, troveVersion, fileIdList) in fsJob.iterUserRemovals():
	    self.db.removeFilesFromTrove(troveName, troveVersion, fileIdList)

	for (name, version) in fsJob.getOldPackageList():
	    if toStash:
		# if to stash if false, we're restoring the local
		# branch of a rollback
		self.db.eraseTrove(name, version)

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

    def __init__(self, root, path):
	assert(self.__class__ != AbstractDatabase)
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

	SqlDbRepository.__init__(self, root + path + "/local.sql")

class Database(AbstractDatabase):

    """
    A system database which maintains a local repository cache.
    """

    def __init__(self, root, path, mode = "r"):
	AbstractDatabase.__init__(self, root, path)

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

