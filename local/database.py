#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
import filecontents
import fsrepos
import log
import localrep
import os
import trovedb
import repository
import update
import util
import versions

class AbstractDatabase(repository.AbstractRepository):

    # XXX some of these interfaces are horribly inefficient as we have
    # to instantiate a full package object to do anything... 
    # FilesystemRepository has the same problem

    def getAllTroveNames(self):
	return self.troveDb.getAllTroveNames()

    def getPackageBranchList(self, name):
	return [ x.getVersion().branch() for x in 
		    self.troveDb.iterFindByName(name)]

    def pkgLatestVersion(self, name, branch):
	return [ x.getVersion() for x in self.troveDb.iterFindByName(name)
		     if branch.equal(x.getVersion().branch())][0]

    def hasPackage(self, name):
	return self.troveDb.hasByName(name)

    def getPackageLabelBranches(self, name, nick):
	rc = []
	for x in self.troveDb.iterFindByName(name):
	    b = x.getVersion().branch()
	    if b.branchNickname().equal(nick):
		rc.append(b)

	return rc

    def getLatestPackage(self, name, branch):
	return [ x for x in self.troveDb.iterFindByName(name)
		     if branch.equal(x.getVersion().branch())][0]

    def getPackageVersionList(self, name):
	return [ x.getVersion() for x in self.troveDb.iterFindByName(name) ]

    def getFileVersion(self, fileId, version, path = None):
	return self.stash.getFileVersion(fileId, version)

    # takes an abstract change set and creates a differential change set 
    # against a branch of the repository
    def rootChangeSet(self, absSet, branch):
	assert(absSet.isAbstract())

	# this has an empty source path template, which is only used to
	# construct the eraseFiles list anyway
	
	# we don't use stash.buildJob here as it can't deal with
	# abstract change sets
	job = fsrepos.ChangeSetJob(self.stash, absSet)

	# abstract change sets cannot have eraseLists
	#assert(not eraseList)
	#assert(not eraseFiles)

	cs = changeset.ChangeSetFromAbstractChangeSet(absSet)

	for newPkg in job.newPackageList():
	    # FIXME
	    #
	    # this shouldn't be against branch, it should be against
	    # the version of the package already installed on the
	    # system. unfortunately we can't represent that yet. 
	    pkgName = newPkg.getName()
	    oldVersion = self.stash.pkgLatestVersion(pkgName, branch)
	    if not oldVersion:
		# new package; the Package.diff() right after this never
		# sets the abstract flag, so the right thing happens
		old = None
	    else:
		old = self.stash.getPackageVersion(pkgName, oldVersion)

	    # we ignore pkgsNeeded; it doesn't mean much in this case
	    (pkgChgSet, filesNeeded, pkgsNeeded) =	    \
		    newPkg.diff(old, abstract = 0)
	    cs.newPackage(pkgChgSet)

	    for (fileId, oldVersion, newVersion, newPath) in filesNeeded:
		fileObj = job.getFile(fileId)
		assert(newVersion.equal(fileObj.version()))
		
		oldFile = None
		if oldVersion:
		    (oldFile, oldCont) = self.stash.getFileVersion(fileId, 
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
					fileObj.file().isConfig())

	assert(not cs.validate())

	return cs

    # local changes includes the A->A.local portion of a rollback; if it
    # doesn't exist we need to compute that and save a rollback for this
    # transaction
    def commitChangeSet(self, cs, isRollback = False, toStash = True,
                        replaceFiles = False):
	assert(not cs.isAbstract())
        flags = 0
        if replaceFiles:
            flags |= update.REPLACEFILES

	for pkg in cs.getNewPackageList():
	    if pkg.name.endswith(":sources"): raise SourcePackageInstall

	# create the change set from A->A.local
	pkgList = []
	for newPkg in cs.getNewPackageList():
	    name = newPkg.getName()
	    old = newPkg.getOldVersion()
	    if self.stash.hasPackage(name) and old:
		ver = old.fork(versions.LocalBranch(), sameVerRel = 1)
		pkg = self.stash.getPackageVersion(name, old)
		assert(pkg)
		pkgList.append((pkg, pkg, ver))

	result = update.buildLocalChanges(self.stash, pkgList, 
					  root = self.root)
	if not result: return

	(localChanges, retList) = result
	fsPkgDict = {}
	for (changed, fsPkg) in retList:
	    fsPkgDict[fsPkg.getName()] = fsPkg

	if not isRollback:
	    inverse = cs.makeRollback(self.stash, configFiles = 1)
            flags |= update.MERGE

	# Build A->B
	if toStash:
	    job = self.stash.buildJob(cs)

	# build the list of changes to the filesystem
	fsJob = update.FilesystemJob(self.stash, cs, fsPkgDict, 
				     self.root, flags = flags)

	# -------- database and system are updated below this line ---------

	try:
	    # add new packages
	    if toStash: job.commit()

	    # remove old packages
	    errList = fsJob.getErrorList()
	    if errList:
		for err in errList: log.error(err)
		# FIXME need a --force for this
		job.undo()
		return

	    if toStash: job.removals()
	except:
	    # this won't work it things got too far, but it won't hurt
	    # anything either
	    if toStash: job.undo()
	    raise

	# everything is in the database... save this so we can undo
	# it later. 
	if not isRollback:
	    self.addRollback(inverse, localChanges)

	fsJob.apply()

	# it would be nice if this could be undone on failure
	for pkg in fsJob.getNewPackageList():
	    self.troveDb.addTrove(pkg)
	for (name, version) in fsJob.getOldPackageList():
	    self.troveDb.delTrove(name, version)

    # this is called when a Repository wants to store a file; we never
    # want to do this; we copy files onto the filesystem after we've
    # created the LocalBranch

    def open(self, mode):
	top = util.joinPaths(self.root, self.dbpath)

	self.rollbackCache = top + "/rollbacks"
	self.rollbackStatus = self.rollbackCache + "/status"
	if not os.path.exists(self.rollbackCache) and mode == "c":
	    util.mkdirChain(self.rollbackCache)
	if not os.path.exists(self.rollbackStatus):
	    self.firstRollback = 0
	    self.lastRollback = -1
	    self.writeRollbackStatus()
	else:
	    self.readRollbackStatus()

	self.troveDb = trovedb.TroveDatabase(top, mode)

    def close(self):
	self.troveDb = None

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

    def __init__(self, root, path, mode = "r"):
	assert(self.__class__ != AbstractDatabase)
	self.root = root
	self.dbpath = path
	self.open(mode)
	repository.AbstractRepository.__init__(self)

class Database(AbstractDatabase):

    """
    A system database which maintains a local repository cache.
    """

    def open(self, mode):
	AbstractDatabase.open(self, mode)
	self.stash = localrep.LocalRepository(self.root, self.dbpath, mode)

    def close(self):
	AbstractDatabase.close(self)
	self.stash = None

    def __init__(self, root, path, mode = "r"):
	AbstractDatabase.__init__(self, root, path, mode)

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

