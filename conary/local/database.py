#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

#stdlib
import errno
import itertools
import os
import shutil

#conary
from conary import files, trove, versions
from conary.build import tags
from conary.errors import ConaryError, DatabaseError, DatabasePathConflicts
from conary.callbacks import UpdateCallback
from conary.conarycfg import RegularExpressionList, CfgLabelList
from conary.deps import deps
from conary.lib import log, util
from conary.local import localrep, sqldb, schema, update
from conary.local.errors import *
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

    def getLocalChangeset(self, i):
        local = changeset.ChangeSetFromFile(self.localName % (self.dir, i))
        return local

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
                 withFiles = True, withDeps = True):
        l = self.getTroves([ (name, version, flavor) ], pristine = pristine,
                           withDeps = withDeps, withFiles = withFiles)
        if l[0] is None:
            raise errors.TroveMissing(name, version)

        return l[0]

    def getTroves(self, troveList, pristine = True, withFiles = True,
                  withDeps = True):
        return self.db.getTroves(troveList, pristine, withFiles = withFiles,
                                 withDeps = withDeps)

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

        if flavor is None or flavor.isEmpty():
            flavorTest = "is NULL"
        else:
            flavorTest = "== '%s'" % flavor.freeze()

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

	return result

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
                       fileStream = None, isPresent = True):
	self.db.addFile(troveId, pathId, fileObj, path, fileId, version,
                        fileStream = fileStream, isPresent = isPresent)

    def addTrove(self, trove, pin = False):
	return self.db.addTrove(trove, pin = pin)

    def addTroveDone(self, troveInfo):
        return self.db.addTroveDone(troveInfo)

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
                                deps.Flavor(), None)
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
                                deps.Flavor(), None)
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
        """
        Check the database for closure against the operations in
        the passed changeSet.

        @param jobSet: The jobs which define the dependency check
        @type jobSet: set
        @param troveSource: Trove source troves in the job are
                            available from
        @type troveSource: AbstractTroveSource:
        @param findOrdering: If true, a reordering of the job is
                             returned which preserves dependency
                             closure at each step.
        @param findOrdering: boolean
        @rtype: tuple of dependency failures for new packages and
                dependency failures caused by removal of existing
                packages
        """

        checker = self.dependencyChecker(troveSource)
        checker.addJobs(jobSet)
        unsatisfiedList, unresolveableList, changeSetList = \
                checker.check(findOrdering = findOrdering)
        checker.done()

        return (unsatisfiedList, unresolveableList, changeSetList)

    def dependencyChecker(self, troveSource):
        return self.db.dependencyChecker(troveSource)

    # local changes includes the A->A.local portion of a rollback; if it
    # doesn't exist we need to compute that and save a rollback for this
    # transaction
    def commitChangeSet(self, cs, uJob,
                        isRollback = False, updateDatabase = True,
                        replaceFiles = False, tagScript = None,
			test = False, justDatabase = False, journal = None,
                        localRollbacks = False, callback = UpdateCallback(),
                        removeHints = {}, filePriorityPath = None,
                        autoPinList = RegularExpressionList(), threshold = 0):
	assert(not cs.isAbsolute())

        if filePriorityPath is None:
            filePriorityPath = CfgLabelList()

        flags = 0
        if replaceFiles:
            flags |= update.REPLACEFILES
        if isRollback:
            flags |= update.MISSINGFILESOKAY | update.IGNOREINITIALCONTENTS

        self.db.begin()

	for trove in cs.iterNewTroveList():
	    if trove.getName().endswith(":source"):
                raise SourceComponentInstall

	tagSet = tags.loadTagDict(self.root + "/etc/conary/tags")

        dbCache = DatabaseCacheWrapper(self)

	# create the change set from A->A.local
	troveList = []
	for newTrove in cs.iterNewTroveList():
	    name = newTrove.getName()
	    old = newTrove.getOldVersion()
	    flavor = newTrove.getOldFlavor()
	    if self.hasTroveByName(name) and old:
		ver = old.createShadow(versions.LocalLabel())
		trove = dbCache.getTrove(name, old, flavor, pristine = False)
		origTrove = dbCache.getTrove(name, old, flavor, pristine = True)
		assert(trove)
		troveList.append((trove, origTrove, ver, 
                                  flags & update.MISSINGFILESOKAY))

        for (name, version, flavor) in cs.getOldTroveList():
            rollbackVersion = version.createShadow(versions.RollbackLabel())
            trove = dbCache.getTrove(name, version, flavor, pristine = False)
            origTrove = dbCache.getTrove(name, version, flavor, 
                                         pristine = True)
            assert(trove)
            troveList.append((trove, origTrove, rollbackVersion, 
                              update.MISSINGFILESOKAY))

        callback.creatingRollback()

	result = update.buildLocalChanges(self, troveList, root = self.root)
	if not result: return

        retList = result[1]
        localRollback = changeset.ReadOnlyChangeSet()
        localRollback.merge(result[0])

	fsTroveDict = {}
	for (changed, fsTrove) in retList:
	    fsTroveDict[(fsTrove.getName(), fsTrove.getVersion())] = fsTrove

	if not isRollback:
            reposRollback = cs.makeRollback(dbCache, configFiles = True,
                               redirectionRollbacks = (not localRollbacks))
            flags |= update.MERGE

        fsJob = update.FilesystemJob(dbCache, cs, fsTroveDict, self.root,
                                     filePriorityPath, flags = flags,
                                     callback = callback,
                                     removeHints = removeHints)

        if not isRollback:
            removeRollback = fsJob.createRemoveRollback()

            # We now have two rollbacks we need to merge together, localRollback
            # (which is the changes already made to the local system) and
            # removeRollback, which contains local changes this update will do.
            # Those two could overlap, so we need to merge them carefully.
            for removeCs in [ x for x in removeRollback.iterNewTroveList() ]:
                newInfo = (removeCs.getName(), removeCs.getNewVersion(), 
                           removeCs.getNewFlavor())
                if not localRollback.hasNewTrove(*newInfo):
                    continue

                localCs = localRollback.getNewTroveVersion(*newInfo)

                if localCs.getOldVersion() != removeCs.getOldVersion() or \
                   localCs.getOldFlavor() != removeCs.getOldFlavor():
                    contine

                removeRollback.delNewTrove(*newInfo)

                pathIdList = set()
                for (pathId, path, fileId, version) in \
                                            removeCs.getNewFileList():
                    pathIdList.add(pathId)
                    localCs.newFile(pathId, path, fileId, version)

                changedList = localCs.getChangedFileList()
                l = [ x for x in localCs.getChangedFileList() if
                        x[0] not in pathIdList ]
                del changedList[:]
                changedList.extend(l)

                continue

            localRollback.merge(removeRollback)

	# look through the directories which have had files removed and
	# see if we can remove the directories as well
        dirSet = fsJob.getDirectoryCountSet()
        list = dirSet.keys()
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

            entries -= dirSet[path]

	    # listdir excludes . and ..
	    if (entries) != 0: continue

	    directoryCandidates[path] = True

	    parent = os.path.dirname(path)
            if dirSet.has_key(parent):
                dirSet[parent] += 1
	    else:
                dirSet[parent] = 1
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

        for (troveName, troveVersion, troveFlavor, fileDict) in fsJob.iterUserRemovals():
            if sum(fileDict.itervalues()) == 0:
                # Nothing to do (these are updates for a trove being installed
                # as part of this job rather than for a trove which is part
                # of this job)
                continue

            self.db.removeFilesFromTrove(troveName, troveVersion,
                                         troveFlavor, fileDict.keys())

        dbConflicts = []

        # Build A->B
        if updateDatabase:
            # this updates the database from the changeset; the change
            # isn't committed until the self.commit below
            # an object for historical reasons
            try:
                localrep.LocalRepositoryChangeSetJob(
                    dbCache, cs, callback, autoPinList, 
                    filePriorityPath,
                    threshold = threshold,
                    allowIncomplete = isRollback, 
                    pathRemovedCheck = fsJob.pathRemoved,
                    replaceFiles = replaceFiles)
            except DatabasePathConflicts, e:
                for (path, (pathId, (troveName, version, flavor)),
                           newTroveInfo) in e.getConflicts():
                    dbConflicts.append(DatabasePathConflictError(
                            util.joinPaths(self.root, path), 
                            troveName, version, flavor))

            self.db.mapPinnedTroves(uJob.getPinMaps())
        else:
            # When updateDatabase is False, we're applying the local part
            # of changeset. Files which are newly added by local changesets
            # need to be recorded in the database as being present (since
            # they were previously erased)
            localrep.markAddedFiles(self.db, cs)

        errList = fsJob.getErrorList()

        # Let DatabasePathConflictError mask FileInWayError (since they
        # are really very similar)
        newErrs = []
        for err in dbConflicts:
            found = None
            for i, otherErr in enumerate(errList):
                if isinstance(otherErr, FileInWayError) and \
                                   err.path == otherErr.path:
                    found = i
                    break

            if found is None:
                newErrs.append(err)
            else:
                errList[found] = err

        errList = newErrs + errList
        del newErrs, dbConflicts

        if errList:
            raise CommitError, ('applying update would cause errors:\n' + 
                                '\n\n'.join(str(x) for x in errList))
        if test:
            self.db.rollback()
            return

        if not justDatabase:
            fsJob.apply(tagSet, tagScript, journal, callback)

        if updateDatabase:
            for (name, version, flavor) in fsJob.getOldTroveList():
		# if to database if false, we're restoring the local
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

    def removeFiles(self, pathList):

        def _doRemove(self, rb, pathList):
            pathsByTrove = {}
            troves = {}

            for path in pathList:
                trvs = [ x for x in self.db.iterFindByPath(path) ]
                if len(trvs) > 1:
                    raise DatabaseError, "multiple troves own %s" % path
                elif not trvs:
                    raise DatabaseError, "no trove owns %s" % path

                trv = trvs[0]
                trvInfo = trv.getNameVersionFlavor()

                troves[trvInfo] = trv
                pathsByTrove.setdefault(trvInfo, []).append(path)

            reposCs = changeset.ChangeSet()
            localCs = changeset.ChangeSet()

            for trvInfo, pathList in pathsByTrove.iteritems():
                trv = troves[trvInfo]

                newTrv = trv.copy()
                newTrv.changeVersion(
                            trv.getVersion().createShadow(versions.RollbackLabel()))

                for path in pathList:
                    fileList = [ (x[0], x[2], x[3]) for x in trv.iterFileList() 
                                                        if x[1] in path ]
                    assert(len(fileList) == 1)
                    pathId, fileId, fileVersion = fileList[0]
                    trv.removeFile(pathId)
                    newTrv.removeFile(pathId)

                    fullPath = util.joinPaths(self.root, path)

                    try:
                        f = files.FileFromFilesystem(fullPath, pathId)
                    except OSError, e:
                        if e.errno != errno.ENOENT:
                            raise

                        stream = self.db.getFileStream(fileId)
                        newTrv.addFile(pathId, path, fileVersion, fileId)
                        localCs.addFile(None, fileId, stream)
                        localCs.addFileContents(pathId,
                                changeset.ChangedFileTypes.hldr,
                                filecontents.FromString(""), False)
                    else:
                        fileId = f.fileId()
                        newTrv.addFile(pathId, path, fileVersion, fileId)
                        localCs.addFile(None, fileId, f.freeze())
                        localCs.addFileContents(pathId,
                                changeset.ChangedFileTypes.file,
                                filecontents.FromFilesystem(fullPath), False)

                    self.db.removeFileFromTrove(trv, path)

                    log.syslog("removed file %s from %s", path, trv.getName())

                localCs.newTrove(newTrv.diff(trv)[0])

            rb.add(reposCs, localCs)

        rb = self.createRollback()
        try:
            _doRemove(self, rb, pathList)
        except Exception, e:
            self.removeRollback("r." + rb.dir.split("/")[-1])
            raise

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

        # Count the number of jobs in the rollback. We have to open the
        # local rollbacks to know if there is any work to do, which is
        # unfortunate. We don't want to include empty local rollbacks
        # in the work count though.
        totalCount = 0
        for name in names:
            rb = self.getRollback(name)
            totalCount += 0

            for i in xrange(rb.getCount()):
                (reposCs, localCs) = rb.getLast() 
                if not reposCs.isEmpty():
                    totalCount += 1
                if not localCs.isEmpty():
                    totalCount += 1

        itemCount = 0
        for i, name in enumerate(names):
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

                # we need to go ahead and note files which were removed
                # from in the local part of the changeset to prevent false
                # conflicts
                removalHints = {}
                for trvCs in localCs.iterNewTroveList():
                    info = (trvCs.getName(), trvCs.getOldVersion(),
                            trvCs.getOldFlavor())
                    l = removalHints.setdefault(info, [])
                    l.extend(trvCs.getOldFileList())

                try:
                    if not reposCs.isEmpty():
                        itemCount += 1
                        callback.setUpdateHunk(itemCount, totalCount)
                        callback.setUpdateJob(reposCs.getJobSet())
                        self.commitChangeSet(reposCs, UpdateJob(None),
                                             isRollback = True,
                                             replaceFiles = replaceFiles,
                                             removeHints = removalHints,
                                             callback = callback)

                    if not localCs.isEmpty():
                        itemCount += 1
                        callback.setUpdateHunk(itemCount, totalCount)
                        callback.setUpdateJob(localCs.getJobSet())
                        self.commitChangeSet(localCs, UpdateJob(None),
                                             isRollback = True,
                                             updateDatabase = False,
                                             replaceFiles = replaceFiles,
                                             callback = callback)

                    rb.removeLast()
                except CommitError, err:
                    raise RollbackError(name, err)

                (reposCs, localCs) = rb.getLast()

            self.removeRollback(name)

    def iterFindPathReferences(self, path, justPresent = False):
        return self.db.iterFindPathReferences(path, justPresent = justPresent)

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

    def iterUpdateContainerInfo(self, troveNames=None):
        return self.db.iterUpdateContainerInfo(troveNames)

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
            else:
                self.readRollbackStatus()
            SqlDbRepository.__init__(self, root + path)

class DatabaseCacheWrapper:

    def __getattr__(self, attr):
        return getattr(self.db, attr)

    def getTrove(self, name, version, flavor, pristine = True):
        l = self.getTroves([ (name, version, flavor) ], pristine = pristine)
        if l[0] is None:
            raise errors.TroveMissing(name, version)

        return l[0]

    def getTroves(self, l, pristine = True):
        retList = []
        for i, info in enumerate(l):
            retList.append(self.cache.get((info, pristine), None))

        missing = [ (x[0], x[1][1]) for x in 
                        enumerate(itertools.izip(retList, l)) if
                        x[1][0] is None ]

        if not missing:
            return retList

        trvs = self.db.getTroves([ x[1] for x in missing ], 
                                 pristine = pristine)
        for (idx, info), trv in itertools.izip(missing, trvs):
            retList[idx] = trv
            self.cache[(info, pristine)] = trv

        return retList

    def __init__(self, db):
        self.db = db
        self.cache = {}

# Exception classes
class RollbackError(errors.ConaryError):

    """Base class for exceptions related to applying rollbacks"""

    def __init__(self, rollbackName, errorMessage=''):
	"""
        Create new new RollbackrError
	@param rollbackName: string represeting the name of the rollback
        """
	self.name = rollbackName
        self.error = errorMessage

    def __str__(self):
	return "rollback %s cannot be applied:\n%s" % (self.name, self.error)

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
