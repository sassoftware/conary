#
# Copyright (c) 2004-2005 rPath, Inc.
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

#stdlib
import errno
import os
import shutil

#conary
from conary import trove, versions
from conary.build import tags
from conary.errors import ConaryError, DatabaseError
from conary.callbacks import UpdateCallback
from conary.conarycfg import RegularExpressionList
from conary.deps import deps
from conary.lib import log, util
from conary.local import localrep, sqldb, schema, update
from conary.repository import changeset, datastore, errors, filecontents
from conary.repository import repository, trovesource

OldDatabaseSchema = schema.OldDatabaseSchema

class Rollback:

    reposName = "%s/repos.%d"
    localName = "%s/local.%d"

    def add(self, repos, local):
        repos.writeToFile(self.reposName % (self.dir, self.count))
        local.writeToFile(self.localName % (self.dir, self.count))
        self.count += 1
        open("%s/count" % self.dir, "w").write("%d\n" % self.count)

    def _getChangeSets(self, item):
        repos = changeset.ChangeSetFromFile(self.reposName % (self.dir, item))
        local = changeset.ChangeSetFromFile(self.localName % (self.dir, item))
        return (repos, local)

    def getLast(self):
        if not self.count:
            return (None, None)
        return self._getChangeSets(self.count - 1)

    def removeLast(self):
        if self.count == 0:
            return
        os.unlink(self.reposName % (self.dir, self.count - 1))
        os.unlink(self.localName % (self.dir, self.count - 1))
        self.count -= 1
        open("%s/count" % self.dir, "w").write("%d\n" % self.count)

    def iterChangeSets(self):
        for i in range(self.count):
            csList = self._getChangeSets(i)
            yield csList[0]
            yield csList[1]

    def getCount(self):
        return self.count

    def __init__(self, dir, load = False):
        self.dir = dir

        if load:
            self.stored = True
            self.count = int(open("%s/count" % self.dir).readline()[:-1])
        else:
            self.stored = False
            self.count = 0

class UpdateJob:

    def addPinMapping(self, name, pinnedVersion, neededVersion):
        self.pinMapping.add((name, pinnedVersion, neededVersion))

    def getPinMaps(self):
        return self.pinMapping

    def getRollback(self):
        return self.rollback

    def setRollback(self, rollback):
        self.rollback = rollback

    def getTroveSource(self):
        return self.troveSource

    def setSearchSource(self, *troveSources):
        if len(troveSources) > 1:
            troveSource = trovesource.TroveSourceStack(*troveSources)
        else:
            troveSource = troveSources[0]

        self.searchSource = troveSource

    def getSearchSource(self):
        return self.searchSource

    def addJob(self, job):
        self.jobs.append(job)

    def getJobs(self):
        return self.jobs

    def setPrimaryJobs(self, jobs):
        assert(type(jobs) == set)
        self.primaries = jobs

    def getPrimaryJobs(self):
        return self.primaries

    def __init__(self, db, searchSource = None):
        self.jobs = []
        self.pinMapping = set()
        self.rollback = None
        self.troveSource = trovesource.ChangesetFilesTroveSource(db)
        self.primaries = set()

        self.searchSource = searchSource

class SqlDbRepository(trovesource.SearchableTroveSource,
                      datastore.DataStoreRepository,
		      repository.AbstractRepository):

    def iterAllTroveNames(self):
	return self.db.iterAllTroveNames()

    def findRemovedByName(self, name):
        return self.db.findRemovedByName(name)

    def findByNames(self, nameList):
        return self.db.findByNames(nameList)

    def getTroveContainers(self, l):
        return self.db.getTroveContainers(l)

    def findTroveContainers(self, names):
        return self.db.findTroveContainers(names)

    def troveIsIncomplete(self, name, version, flavor):
        return self.db.troveIsIncomplete(name, version, flavor)

    def findTroveReferences(self, names):
        """ Return references to a trove on the system, whether or not
            that trove is actually installed
        """
        return self.db.findTroveReferences(names)

    def getTrove(self, name, version, flavor, pristine = True,
		 withFiles = True):
        try:
            return self.db.getTrove(name, version, flavor, pristine = pristine)
        except KeyError, e:
            raise errors.TroveMissing(name, version)

    def getTroves(self, troveList, pristine = True, withFiles = True):
        return self.db.getTroves(troveList, pristine, withFiles = withFiles)

    def getTroveLatestVersion(self, name, branch):
        cu = self.db.db.cursor()
	cu.execute("""SELECT version, timeStamps FROM Instances 
			JOIN Versions ON
			    Instances.versionId == Versions.versionId
			WHERE Instances.troveName == ? AND
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

    def troveVersionFlavors(self, troveName, version):
	l = [ x.getFlavor() for x in self.db.iterFindByName(troveName)
		     if version == x.getVersion() ]

	return l

    def hasTroveByName(self, name):
	return self.db.hasByName(name)

    def trovesByName(self, name):
	return [ (name, x[0], x[1]) \
                    for x in self.db.iterVersionByName(name, True) ]

    def hasTroves(self, troves):
        return self.db.hasTroves(troves)

    def hasTrove(self, troveName, version, flavor):
        cu = self.db.db.cursor()

        if flavor:
            flavorTest = "== '%s'" % flavor.freeze()
        else:
            flavorTest = "is NULL";

        cu.execute("""SELECT count(*) FROM Instances
                        JOIN Versions ON
                            Instances.versionId == Versions.versionId
                        JOIN Flavors ON
                            Instances.flavorId == Flavors.flavorId
                        WHERE
                            Instances.troveName == ? AND
                            Instances.isPresent != 0 AND
                            Versions.version == ? AND
                            Flavors.flavor %s
                   """ % flavorTest, troveName, version.asString())

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

    def getFileStream(self, fileId):
        return self.db.getFileStream(fileId, pristine = True)

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

    def findUnreferencedTroves(self):
        return self.db.findUnreferencedTroves()

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False,
			 pristine = False):
	return self.db.iterFilesInTrove(troveName, version, flavor,
                                        sortByPath = sortByPath, 
                                        withFiles = withFiles,
                                        pristine = pristine)

    def iterFilesWithTag(self, tag):
	return self.db.iterFilesWithTag(tag)

    def addFileVersion(self, troveId, pathId, fileObj, path, fileId, version,
                       fileStream = None):
	self.db.addFile(troveId, pathId, fileObj, path, fileId, version,
                        fileStream = fileStream)

    def addTrove(self, trove, pin = False):
	return self.db.addTrove(trove, pin = pin)

    def addTroveDone(self, troveInfo):
        self.db.addTroveDone(troveInfo)

    def pinTroves(self, troveList, pin):
        troves = self.getTroves(troveList)

        for trove in troves:
            for subTrove in self.walkTroveSet(trove):
                self.db.pinTroves(subTrove.getName(),
                                  subTrove.getVersion(),
                                  subTrove.getFlavor(), pin = pin)

        self.db.commit()

    def trovesArePinned(self, troveList):
        return self.db.trovesArePinned(troveList)

    def commit(self):
	self.db.commit()

    def close(self):
	self.db.close()

    def eraseTrove(self, troveName, version, flavor):
	self.db.eraseTrove(troveName, version, flavor)

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

        datastore.DataStoreRepository.__init__(self, 
                           dataStore = localrep.SqlDataStore(self.db.db))
        repository.AbstractRepository.__init__(self)
        trovesource.SearchableTroveSource.__init__(self)

class Database(SqlDbRepository):

    # XXX some of these interfaces are horribly inefficient as we have
    # to instantiate a full trove object to do anything... 
    # FilesystemRepository has the same problem

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
	return SqlDbRepository.iterFilesInTrove(self, troveName, version,
			flavor, sortByPath = sortByPath,
			withFiles = withFiles, pristine = False)

    def iterTrovesByPath(self, path):
	return [ x for x in self.db.iterFindByPath(path) ]

    def outdatedTroves(self, l, ineligible = set()):
        """
        For a (troveName, troveVersion, troveFlavor) list return a dict indexed
        by elements in that list. Each item in the dict is the (troveName,
        troveVersion, troveFlavor) item for an already installed trove if
        installing that item doesn't cause a removal, otherwise it is which
        needs to be removed as part of the update. a (None, None) tuple means
        the item is new and nothing should be removed while no entry means that
        the item is already installed. (name, version, flavor) tuples in
        the ineligible set will not be outdated.
        """

        names = {}
        newGroup = trove.Trove("@update", versions.NewVersion(), 
                                deps.DependencySet(), None)
        for name, version, flavor in l:
            names[name] = True
            newGroup.addTrove(name, version, flavor)

        instList = []
        for name in names.iterkeys():
            # get the current troves installed
            try:
                instList += self.trovesByName(name)
            except repository.TroveNotFound, e:
                pass

        # now we need to figure out how to match up the version and flavors
        # pair. a shortcut is to stick the old troves in one group and
        # the new troves in another group; when we diff those groups
        # diff tells us how to match them up. anything which doesn't get
        # a match gets removed. got that? 
        instGroup = trove.Trove("@update", versions.NewVersion(), 
                                deps.DependencySet(), None)
        for info in instList:
            if info not in ineligible:
                instGroup.addTrove(*info)

        trvChgs = newGroup.diff(instGroup)[2]

        resultDict = {}
        for (name, (oldVersion, oldFlavor), (newVersion, newFlavor),
                            isAbsolute) in trvChgs:
            if newVersion:
                resultDict[(name, newVersion, newFlavor)] = (name, oldVersion, 
                                                             oldFlavor)

        return resultDict

    def depCheck(self, jobSet, troveSource, findOrdering = False):
        return self.db.depCheck(jobSet, troveSource, 
                                findOrdering = findOrdering)

    # local changes includes the A->A.local portion of a rollback; if it
    # doesn't exist we need to compute that and save a rollback for this
    # transaction
    def commitChangeSet(self, cs, uJob,
                        isRollback = False, toStash = True,
                        replaceFiles = False, tagScript = None,
			test = False, justDatabase = False, journal = None,
                        localRollbacks = False, callback = UpdateCallback(),
                        removeHints = {}, 
                        autoPinList = RegularExpressionList(), threshold = 0):
	assert(not cs.isAbsolute())
        flags = 0
        if replaceFiles:
            flags |= update.REPLACEFILES
        if isRollback:
            flags |= update.MISSINGFILESOKAY

        self.db.begin()

	for trove in cs.iterNewTroveList():
	    if trove.getName().endswith(":source"):
                raise SourceComponentInstall

	tagSet = tags.loadTagDict(self.root + "/etc/conary/tags")

	# create the change set from A->A.local
	troveList = []
	for newTrove in cs.iterNewTroveList():
	    name = newTrove.getName()
	    old = newTrove.getOldVersion()
	    flavor = newTrove.getOldFlavor()
	    if self.hasTroveByName(name) and old:
		ver = old.createBranch(versions.LocalLabel(), withVerRel = 1)
		trove = self.getTrove(name, old, flavor, pristine = False)
		origTrove = self.getTrove(name, old, flavor, pristine = True)
		assert(trove)
		troveList.append((trove, origTrove, ver, 
                                  flags & update.MISSINGFILESOKAY))

        for (name, version, flavor) in cs.getOldTroveList():
            rollbackVersion = version.createBranch(versions.RollbackLabel(), 
                                                withVerRel = 1)
            trove = self.getTrove(name, version, flavor, pristine = False)
            origTrove = self.getTrove(name, version, flavor, 
                                      pristine = True)
            assert(trove)
            troveList.append((trove, origTrove, rollbackVersion, 
                              update.MISSINGFILESOKAY))

        callback.creatingRollback()

	result = update.buildLocalChanges(self, troveList, root = self.root)
	if not result: return

	localRollback, retList = result

	fsTroveDict = {}
	for (changed, fsTrove) in retList:
	    fsTroveDict[(fsTrove.getName(), fsTrove.getVersion())] = fsTrove

	if not isRollback:
            reposRollback = cs.makeRollback(self, configFiles = True,
                               redirectionRollbacks = (not localRollbacks))
            flags |= update.MERGE

	fsJob = update.FilesystemJob(self, cs, fsTroveDict, self.root, 
				     flags = flags, callback = callback,
                                     removeHints = removeHints)

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
            rollback = uJob.getRollback()
            if rollback is None:
                rollback = self.createRollback()
                uJob.setRollback(rollback)
            rollback.add(reposRollback, localRollback)
            del rollback

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
            localrep.LocalRepositoryChangeSetJob(
                self, cs, callback, autoPinList, threshold = threshold,
                allowIncomplete=isRollback)
            self.db.mapPinnedTroves(uJob.getPinMaps())

        errList = fsJob.getErrorList()
        if errList:
            for err in errList:
                log.error(err)
            raise CommitError, 'file system job contains errors'
        if test:
            self.db.rollback()
            return

        if not justDatabase:
            fsJob.apply(tagSet, tagScript, journal, callback)

        for (troveName, troveVersion, troveFlavor, pathIdList) in fsJob.iterUserRemovals():
            self.db.removeFilesFromTrove(troveName, troveVersion, 
                                         troveFlavor, pathIdList)

	for (name, version, flavor) in fsJob.getOldTroveList():
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

        # log everything
	for trvCs in cs.iterNewTroveList():
            if not trvCs.getOldVersion():
                log.syslog("installed %s=%s[%s]", trvCs.getName(),
                         trvCs.getNewVersion(), 
                         deps.formatFlavor(trvCs.getNewFlavor()))
            else:
                log.syslog("updated %s=%s[%s]--%s[%s]", trvCs.getName(),
                         trvCs.getOldVersion(), 
                         deps.formatFlavor(trvCs.getOldFlavor()),
                         trvCs.getNewVersion(), 
                         deps.formatFlavor(trvCs.getNewFlavor()))

	for (name, version, flavor) in cs.getOldTroveList():
            log.syslog("removed %s=%s[%s]", name, version,
                       deps.formatFlavor(flavor))

        callback.committingTransaction()
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
            log.syslog("removed file %s from %s", path, trv.getName())

        self.db.commit()

    def createRollback(self):
	rbDir = self.rollbackCache + ("/%d" % (self.lastRollback + 1))
        if os.path.exists(rbDir):
            shutil.rmtree(rbDir)
        os.mkdir(rbDir)
	self.lastRollback += 1
        self.writeRollbackStatus()
        return Rollback(rbDir)

    # name looks like "r.%d"
    def removeRollback(self, name):
	rollback = int(name[2:])
        try:
            shutil.rmtree(self.rollbackCache + "/%d" % rollback)
        except OSError, e:
            if e.errno == 2:
                pass
	if rollback == self.lastRollback:
	    self.lastRollback -= 1
	    self.writeRollbackStatus()

    def removeLastRollback(self):
        name = 'r.%d' %self.lastRollback
        self.removeRollback(name)

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
        dir = self.rollbackCache + "/" + "%d" % num
        return Rollback(dir, load = True)

    def applyRollbackList(self, repos, names, replaceFiles = False,
                          callback = UpdateCallback()):
	last = self.lastRollback
	for name in names:
	    if not self.hasRollback(name):
		raise RollbackDoesNotExist(name)

	    num = int(name[2:])
	    if num != last:
		raise RollbackOrderError(name)
	    last -= 1

	for name in names:
	    rb = self.getRollback(name)

            # we don't want the primary troves from reposCs to win, so get
            # rid of them (otherwise we're left with redirects!). primaries
            # don't really matter here anyway, so no reason to worry about
            # them
            (reposCs, localCs) = rb.getLast() 
            reposCs.setPrimaryTroveList([])
            while reposCs:
                # redirects in rollbacks mean we need to go get the real
                # changeset from a repository
                jobList = []
                for trvCs in reposCs.iterNewTroveList():
                    if not trvCs.isRedirect(): continue
                    jobList.append((trvCs.getName(),
                                (trvCs.getOldVersion(), trvCs.getOldFlavor()),
                                (trvCs.getNewVersion(), trvCs.getNewFlavor()),
                                False))

                newCs = repos.createChangeSet(jobList, recurse = False)
                newCs.setPrimaryTroveList([])
                # this overwrites old with new
                reposCs.merge(newCs)

                try:
                    self.commitChangeSet(reposCs, UpdateJob(None),
                                         isRollback = True,
                                         replaceFiles = replaceFiles,
                                         callback = callback)
                    self.commitChangeSet(localCs, UpdateJob(None),
                                         isRollback = True,
                                         toStash = False,
                                         replaceFiles = replaceFiles,
                                         callback = callback)
                    rb.removeLast()
                except CommitError:
                    raise RollbackError(name)

                (reposCs, localCs) = rb.getLast()

            self.removeRollback(name)

    def iterFindPathReferences(self, path):
        return self.db.iterFindPathReferences(path)

    def getTrovesWithProvides(self, depSetList):
        """Returns a dict { depSet : [troveTup, troveTup] } of local 
           troves that provide each dependency set listed.
        """
        return self.db.getTrovesWithProvides(depSetList)

    def getTransitiveProvidesClosure(self, depSetList):
        """
        Returns a dict { depSet : [troveTup, troveTup] } of local
        troves satisfying each dependencyset in depSetList, and
        all depSets provided by runtime requirements of any
        troves in the set, with dependency closure.
        """
        closureDepDict = {}
        closureTupSet = set()
        def recurseOne(depSetList):
            d = self.getTrovesWithProvides(depSetList)
            # look only at depSets with new info in this iteration
            s = set(depSet for depSet in d
                    if depSet not in closureDepDict or
                       d[depSet] != closureDepDict[depSet])

            # update closureDepDict with all possible trove tuples
            for depSet in d:
                if depSet in closureDepDict:
                    closureDepDict[depSet].extend(d[depSet])
                else:
                    closureDepDict[depSet] = d[depSet]

            # flatten list of all new troveTups for fastest lookup
            troveTupSet = set()
            for depSet in s:
                # look only at new troveTups from this iteration
                troveTupSet.update(d[depSet])
                newTupList = list(troveTupSet - closureTupSet)
                closureTupSet.update(troveTupSet)
                # now look up the requirements for these troves, and recurse
                newDepSetList = [trove.getRequires()
                    for trove in self.getTroves(newTupList)]
                recurseOne(newDepSetList)
        recurseOne(depSetList)
        return closureDepDict
    
    def __init__(self, root, path):
	self.root = root

        if path == ":memory:": # memory-only db
            SqlDbRepository.__init__(self, ':memory:')
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
class RollbackError(errors.ConaryError):

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

class SourceComponentInstall(DatabaseError):

    def __str__(self):
	return "cannot install a source component onto the local system"

class OpenError(DatabaseError):

    def __str__(self):
        return 'Unable to open database %s: %s' % (self.path, self.msg)

    def __init__(self, path, msg):
	self.path = path
	self.msg = msg

class CommitError(DatabaseError, errors.InternalConaryError):
    pass
