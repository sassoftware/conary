#
# Copyright (c) 2004-2005 Specifix, Inc.
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

import datastore
from callbacks import UpdateCallback
from repository import changeset
import errno
from repository import filecontents
from repository import filecontainer
from lib import log
import localrep
import os
from repository import repository
import sqldb
import trove
import update
from lib import util
import versions
from build import tags
from deps import deps

OldDatabaseSchema = sqldb.OldDatabaseSchema

class SqlDbRepository(datastore.DataStoreRepository,
		      repository.AbstractRepository):

    def iterAllTroveNames(self):
	return self.db.iterAllTroveNames()

    def getTrove(self, name, version, flavor, pristine = False):
	l = [ x for x in self.db.iterFindByName(name, pristine = pristine)
		 if version == x.getVersion() and flavor == x.getFlavor()]
	if not l:
	    raise repository.TroveMissing(name, version)
	assert(len(l) == 1)
	return l[0]

    def getTroves(self, troveList, pristine = False):
        return self.db.getTroves(troveList, pristine)

    def getTroveLatestVersion(self, name, branch):
        cu = self.db.db.cursor()
	cu.execute("""SELECT version, timeStamps FROM DBInstances 
			JOIN Versions ON
			    DBInstances.versionId == Versions.versionId
			WHERE DBInstances.troveName == ? AND
			      isPresent == 1
		   """, name)

	last = None
	for versionStr, timeStamps in cu:
	    version = versions.VersionFromString(versionStr)
	    if version.branch() != branch:
		continue

	    version.setTimeStamps([ float(x) for x in timeStamps.split(":") ])
	    if not last or version.isAfter(last):
		last = version

	return last

    def getAllTroveFlavors(self, troveDict):
        return self.db.getAllTroveFlavors(troveDict)

    def pkgVersionFlavors(self, pkgName, version):
	l = [ x.getFlavor() for x in self.db.iterFindByName(pkgName)
		     if version == x.getVersion() ]

	return l

    def hasPackage(self, name):
	return self.db.hasByName(name)

    def hasTrove(self, pkgName, version, flavor):
        cu = self.db.db.cursor()

        if flavor:
            flavorTest = "== '%s'" % flavor.freeze()
        else:
            flavorTest = "is NULL";

        cu.execute("""SELECT count(*) FROM DBInstances
                        JOIN Versions ON
                            DBInstances.versionId == Versions.versionId
                        JOIN DBFlavors ON
                            DBInstances.flavorId == DBFlavors.flavorId
                        WHERE
                            DBInstances.troveName == ? AND
                            DBInstances.isPresent != 0 AND
                            Versions.version == ? AND
                            DBFlavors.flavor %s
                   """ % flavorTest, pkgName, version.asString())

        result = cu.next()[0] != 0

	return result;

    def getTroveVersionList(self, name, withFlavors = False):
	"""
	Returns a list of all of the versions of a trove available
	in the repository.. If withFlavors is True, (version, flavor)
        tuples are returned instead.

	@param name: trove
	@type name: str
        @param withFlavors: If True, flavor information is also returned.
        @type withFlavors: boolean
	@rtype: list of versions.Version
	"""
	return [ x for x in self.db.iterVersionByName(name, withFlavors) ]

    def getTroveList(self, name):
	"""
	Returns a list of all of the troves available in the
	repository.

	@param name: trove
	@type name: str
	@rtype: list of trove.Trove instances
	"""
	return [ x for x in self.db.iterFindByName(name) ]

    def getFileVersion(self, pathId, fileId, version, withContents = 0):
	fileObj = self.db.getFile(pathId, fileId, pristine = True)
	if withContents:
	    if fileObj.hasContents:
		cont = filecontents.FromDataStore(self.contentsStore,
					          fileObj.contents.sha1())
	    else:
		cont = None

	    return (fileObj, cont)

	return fileObj

    def findFileVersion(self, fileId):
        return self.db.findFileVersion(fileId)

    def getFileVersions(self, l):
	return self.db.iterFiles(l)

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False,
			 pristine = False):
	return self.db.iterFilesInTrove(troveName, version, flavor,
                                        sortByPath = sortByPath, 
                                        withFiles = withFiles,
                                        pristine = pristine)

    def iterFilesWithTag(self, tag):
	return self.db.iterFilesWithTag(tag)

    def addFileVersion(self, troveId, pathId, fileObj, path, fileId, version):
	self.db.addFile(troveId, pathId, fileObj, path, fileId, version)

    def addTrove(self, pkg):
	return self.db.addTrove(pkg)

    def addTroveDone(self, troveInfo):
	pass

    def lockTroves(self, troveList, lock):
        troves = self.getTroves(troveList)

        for trove in troves:
            for subTrove in self.walkTroveSet(trove):
                self.db.lockTrove(subTrove.getName(),
                                  subTrove.getVersion(),
                                  subTrove.getFlavor(), lock = lock)

        self.db.commit()

    def trovesAreLocked(self, troveList):
        return self.db.trovesAreLocked(troveList)

    def commit(self):
	self.db.commit()

    def close(self):
	self.db.close()

    def eraseTrove(self, pkgName, version, flavor):
	self.db.eraseTrove(pkgName, version, flavor)

    def pathIsOwned(self, path):
	return self.db.pathIsOwned(path)

    def eraseFileVersion(self, pathId, version):
	# files get removed with their troves
	pass

    def writeAccess(self):
        return os.access(self.dbpath, os.W_OK)

    def __init__(self, path):
        if path == ":memory:":
            self.dbpath = path
        else:
            self.dbpath = path + "/conarydb"

	self.db = sqldb.Database(self.dbpath)

        datastore.DataStoreRepository.__init__(self, path,
                           dataStore = localrep.SqlDataStore(self.db.db))
        repository.AbstractRepository.__init__(self)

class Database(SqlDbRepository):

    # XXX some of these interfaces are horribly inefficient as we have
    # to instantiate a full package object to do anything... 
    # FilesystemRepository has the same problem

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
	return SqlDbRepository.iterFilesInTrove(self, troveName, version,
			flavor, sortByPath = sortByPath,
			withFiles = withFiles, pristine = False)

    def iterTrovesByPath(self, path):
	return [ x for x in self.db.iterFindByPath(path) ]

    def outdatedTroves(self, l):
        """
        For a (troveName, troveVersion, troveFlavor) list return a dict indexed
        by elements in that list. Each item in the dict is the (troveName,
        troveVersion, troveFlavor) item for an already installed trove if
        installing that item doesn't cause a removal, otherwise it is which
        needs to be removed as part of the update. a (None, None) tuple means
        the item is new and nothing should be removed while no entry means that
        the item is already installed.
        """

        names = {}
        for (name, version, flavor) in l:
            names[name] = True

        instList = []
        for name in names.iterkeys():
            # get the current troves installed
            try:
                instList += self.findTrove(None, name)
            except repository.TroveNotFound, e:
                pass

        # now we need to figure out how to match up the version and flavors
        # pair. a shortcut is to stick the old troves in one group and
        # the new troves in another group; when we diff those groups
        # diff tells us how to match them up. anything which doesn't get
        # a match gets removed. got that? 
        instGroup = trove.Trove("@update", versions.NewVersion(), 
                                deps.DependencySet(), None)
        for (name, version, flavor) in instList:
            instGroup.addTrove(name, version, flavor)

        newGroup = trove.Trove("@update", versions.NewVersion(), 
                                deps.DependencySet(), None)
        for (name, version, flavor) in l:
            newGroup.addTrove(name, version, flavor)

        trvChgs = newGroup.diff(instGroup)[2]

        resultDict = {}
        eraseList = []
        for (name, oldVersion, newVersion, oldFlavor, newFlavor) in trvChgs:
            if not newVersion:
                eraseList.append((name, oldVersion, oldFlavor))
            else:
                resultDict[(name, newVersion, newFlavor)] = (name, oldVersion, 
                                                             oldFlavor)

        return resultDict, eraseList

    def depCheck(self, cs, findOrdering = False):
        return self.db.depCheck(cs, findOrdering = findOrdering)

    # local changes includes the A->A.local portion of a rollback; if it
    # doesn't exist we need to compute that and save a rollback for this
    # transaction
    def commitChangeSet(self, cs, isRollback = False, toStash = True,
                        replaceFiles = False, tagScript = None,
			keepExisting = False, test = False,
                        justDatabase = False, journal = None,
                        localRollbacks = False, 
                        callback = UpdateCallback()):
	assert(not cs.isAbsolute())
        flags = 0
        if replaceFiles:
            flags |= update.REPLACEFILES
        if isRollback:
            flags |= update.MISSINGFILESOKAY

	for pkg in cs.iterNewPackageList():
	    if pkg.getName().endswith(":source"): raise SourcePackageInstall

	tagSet = tags.loadTagDict(self.root + "/etc/conary/tags")

	# create the change set from A->A.local
	pkgList = []
	for newPkg in cs.iterNewPackageList():
	    name = newPkg.getName()
	    old = newPkg.getOldVersion()
	    flavor = newPkg.getOldFlavor()
	    if self.hasPackage(name) and old:
		ver = old.createBranch(versions.LocalLabel(), withVerRel = 1)
		pkg = self.getTrove(name, old, flavor)
		origPkg = self.getTrove(name, old, flavor, pristine = 1)
		assert(pkg)
		pkgList.append((pkg, origPkg, ver, 
                                flags & update.MISSINGFILESOKAY))

	if not keepExisting:
	    for (name, version, flavor) in cs.getOldPackageList():
		localVersion = version.createBranch(versions.LocalLabel(), 
					            withVerRel = 1)
		pkg = self.getTrove(name, version, flavor)
		origPkg = self.getTrove(name, version, flavor, pristine = 1)
		assert(pkg)
		pkgList.append((pkg, origPkg, localVersion, 
				update.MISSINGFILESOKAY))

        callback.creatingRollback()

	result = update.buildLocalChanges(self, pkgList, root = self.root)
	if not result: return

	(localChanges, retList) = result
	fsPkgDict = {}
	for (changed, fsPkg) in retList:
	    fsPkgDict[(fsPkg.getName(), fsPkg.getVersion())] = fsPkg

	if not isRollback:
            if localRollbacks:
                inverse = cs.makeRollback(self, configFiles = 1)
            else:
                inverse = changeset.RollbackRecord(changeSet = cs)
            flags |= update.MERGE
	if keepExisting:
	    flags |= update.KEEPEXISTING

	fsJob = update.FilesystemJob(self, cs, fsPkgDict, self.root, 
				     flags = flags, callback = callback)

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
            try:
                entries = len(os.listdir(path))
            except OSError, e:
                if e.errno != errno.ENOENT:
                    raise
                continue
            
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

	# XXX we have to do this before files get removed from the database,
	# which is a bit unfortunate since this rollback isn't actually
	# valid until a bit later
	if not isRollback and not test:
	    self.addRollback(inverse, localChanges)
	    del inverse
	    del localChanges

        if not justDatabase:
            # run preremove scripts before updating the database, otherwise
            # the file lists which get sent to them are incorrect. skipping
            # this makes --test a little inaccurate, but life goes on
            if not test:
                callback.runningPreTagHandlers()
                fsJob.preapply(tagSet, tagScript)

        # Build A->B
        if toStash:
            # this updates the database from the changeset; the change
            # isn't committed until the self.commit below
            # an object for historical reasons
            localrep.LocalRepositoryChangeSetJob(self, cs, keepExisting,
						 callback)

        errList = fsJob.getErrorList()
        if errList:
            for err in errList: log.error(err)
            raise CommitError, 'file system job contains errors'
        if test:
            return

        if not justDatabase:
            fsJob.apply(tagSet, tagScript, journal, callback)

        for (troveName, troveVersion, troveFlavor, pathIdList) in fsJob.iterUserRemovals():
            self.db.removeFilesFromTrove(troveName, troveVersion, 
                                         troveFlavor, pathIdList)

	for (name, version, flavor) in fsJob.getOldPackageList():
	    if toStash:
		# if to stash if false, we're restoring the local
		# branch of a rollback
		self.db.eraseTrove(name, version, flavor)

	# finally, remove old directories. right now this has to be done
	# after the sqldb has been updated (but before the changes are
	# committted)
        if not justDatabase:
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

                try:
                    # it would be nice if this was cheaper
                    os.rmdir(path)
                except OSError:
                    pass

	self.commit()

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
            try:
                rc.append(changeset.ChangeSetFromFile(name))
            except filecontainer.BadContainer:
                rc.append(changeset.RollbackRecord(fileName = name))

	return rc

    def applyRollbackList(self, repos, names, replaceFiles=False):
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
            if isinstance(reposCs, changeset.RollbackRecord):
                jobList = [ (x[0][0], (x[1][1], x[1][2]),
                                      (x[0][1], x[0][2]), False)
                                for x in reposCs.newPackages.iteritems() ]
                jobList += [ (x[0], (x[1], x[2]), (None, None), False)
                                for x in reposCs.oldPackages ]
                reposCs = repos.createChangeSet(jobList, recurse = False)

            try:
                self.commitChangeSet(reposCs, isRollback = True,
                                     replaceFiles = replaceFiles)
                self.commitChangeSet(localCs, isRollback = True,
                                     toStash = False,
                                     replaceFiles = replaceFiles)
                self.removeRollback(name)
            except CommitError:
                raise RollbackError(name)
    
    def findTrove(self, labelPath, troveName, reqFlavor=None, 
                                              versionStr = None):
        # unlike with netclient.findTrove, lack of a label path 
        # means search _all_ labels.
        versionList = self.getTroveVersionList(troveName)
        if not versionList:
            raise repository.TroveNotFound, \
                    "trove %s is not installed" % troveName
        if not labelPath:
            # we create a set of all labels that this trove is on
            # this allows us to treat requests that use a labelPath and
            # those that don't equivalently, but may be slightly slower 
            # generally this list should be relatively short, though.
            labelPath = set([x.branch().label() for x in versionList])
            labelPath = list(labelPath)
        if not type(labelPath) == list:
            labelPath = [ labelPath ]
        if not versionStr:
            versionList = [ x for x in versionList \
                                        if x.branch().label() in labelPath ] 
        elif versionStr[0] == '/':
            versionList = self.getTroveVersionList(troveName)
            try:
                version = versions.VersionFromString(versionStr)
            except ParseError:
                raise repository.TroveNotFound, \
                                    "invalid version %s" % versionStr
            if isinstance(version, versions.Version):
                versionList = [ v for v in versionList if v == version ]
            elif isinstance(version, versions.Branch):
                versionList = [ v for v in versionList if \
                                                    v.branch() == version ]
        elif not versionStr.count('@') and versionStr[0] != ':':
            versionList = self.getTroveVersionList(troveName)
            if versionStr.find('-') != -1:
                try:
                    verRel = versions.Revision(versionStr)
                    versionList = [ x for x in versionList \
                                    if (x.trailingRevision() == verRel 
                                        and x.branch().label() in labelPath) ] 
                except versions.ParseError, e:
                    raise RuntimeError, 'invalid revision %s' % versionStr
            else:
                versionList = [ x for x in versionList \
                                if (x.trailingRevision().version == versionStr 
                                    and x.branch().label() in labelPath) ] 
        else:
            if versionStr.count('/') != 0:
                raise repository.TroveNotFound, \
                            "invalid version %s" % versionStr
            # these versionStrs affect the labels on the labelPath

            if versionStr[0] == ':':
                # just a branch tag was specified
                if (versionStr.count('@'), versionStr.count(':')) != (0,1): 
                    raise repository.TroveNotFound, \
                            "invalid branch name %s" % versionStr

                repositories = [(x.getHost(), x.getNamespace()) \
                                                        for x in labelPath ]
                labelPath = []
                for serverName, namespace in repositories:
                    labelPath.append(
                        versions.Label("%s@%s%s" % (serverName, namespace, 
                                                                versionStr)))
            elif versionStr[0] == '@':
                # just a branch name was specified
                if (versionStr.count('@'), versionStr.count(':')) != (1,1): 
                    raise repository.TroveNotFound, \
                                             "invalid branch %s" % versionStr
                repositories = [ x.getHost() for x in labelPath ]
                labelPath = []
                for serverName in repositories:
                    labelPath.append(versions.Label("%s%s" % 
                                                    (serverName, versionStr)))
            else:
                # something approximating a label was given
                try:
                    label = versions.Label(versionStr)
                except ParseError:
                    raise repository.TroveNotFound, \
                                                "invalid label %s" % versionStr
                labelPath = [label]
            versionList = [ x for x in versionList \
                                    if x.branch().label() in labelPath ] 
        if not versionList and versionStr:
            raise repository.TroveNotFound, \
                    "version %s of %s was not on found" % (versionStr, 
                                                           troveName)
        pkgList = []
        if reqFlavor is None:
            for version in versionList:
                for flavor in self.pkgVersionFlavors(troveName, version):
                    pkgList.append((troveName, version, flavor))
        else:
            for version in versionList:
                for flavor in self.pkgVersionFlavors(troveName, version):
                    if flavor.stronglySatisfies(reqFlavor):
                        pkgList.append((troveName, version, flavor))
        return pkgList

    def __init__(self, root, path):
	self.root = root

        if path == ":memory:": # memory-only db
            SqlDbRepository.__init__(self, root)
        else:
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

    def __init__(self, rollbackName):
	"""
        Create new new RollbackrError
	@param rollbackName: string represeting the name of the rollback
        """
	self.name = rollbackName

    def __str__(self):
	return "rollback %s cannot be applied" % self.name

class RollbackOrderError(RollbackError):

    """Raised when an attempt is made to apply rollbacks in the
       wrong order"""

    def __str__(self):
	return "rollback %s cannot be applied out of order" % self.name

    def __init__(self, rollbackName):
	"""Create new new RollbackOrderError
	@param rollbackName: string represeting the name of the rollback
	which was trying to be applied out of order"""
        RollbackError.__init__(self, rollbackName)

class RollbackDoesNotExist(RollbackError):

    """Raised when the system tries to access a rollback which isn't in
       the database"""

    def __str__(self):
	return "rollback %s does not exist" % self.name

    def __init__(self, rollbackName):
	"""Create new new RollbackOrderError
	@param rollbackName: string represeting the name of the rollback
	which does not exist"""
        RollbackError.__init__(self, rollbackName)

class SourcePackageInstall(DatabaseError):

    def __str__(self):
	return "cannot install a source package onto the local system"

class OpenError(DatabaseError):

    def __str__(self):
        return 'Unable to open database %s: %s' % (self.path, self.msg)

    def __init__(self, path, msg):
	self.path = path
	self.msg = msg

class MissingDependencies(Exception):

    def __str__(self):
        l = []
        for (name, deps) in self.depList:
            l.append(name + ":")
            l.append("\t" + "\n\t".join(str(deps).split("\n")))

        return "\n".join(l)

    def __init__(self, depList):
        self.depList = depList

class CommitError(repository.CommitError):
    pass

