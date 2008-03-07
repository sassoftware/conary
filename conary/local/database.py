#
# Copyright (c) 2004-2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

#stdlib
import errno, fcntl
import itertools
import os
import shutil

#conary
from conary import constants, files, trove, versions
from conary.build import tags
from conary.errors import ConaryError, DatabaseError, DatabasePathConflicts
from conary.errors import DatabaseLockedError, DecodingError
from conary.callbacks import UpdateCallback
from conary.conarycfg import RegularExpressionList
from conary.deps import deps
from conary.lib import log, util
from conary.local import localrep, sqldb, schema, update, journal
from conary.local.errors import DatabasePathConflictError, FileInWayError
from conary.repository import changeset, datastore, errors, filecontents
from conary.repository import repository, trovesource

OldDatabaseSchema = schema.OldDatabaseSchema

class CommitChangeSetFlags(util.Flags):

    __slots__ = [ 'replaceManagedFiles', 'replaceUnmanagedFiles',
                  'replaceModifiedFiles', 'justDatabase', 'localRollbacks',
                  'test', 'keepJournal', 'replaceModifiedConfigFiles' ]

class Rollback:

    reposName = "%s/repos.%d"
    localName = "%s/local.%d"

    def add(self, repos, local):
        repos.writeToFile(self.reposName % (self.dir, self.count), mode = 0600)
        local.writeToFile(self.localName % (self.dir, self.count), mode = 0600)
        self.count += 1
        fd = os.open("%s/count" % self.dir, os.O_CREAT | os.O_WRONLY |
                                            os.O_TRUNC, 0600)
        os.write(fd, "%d\n" % self.count)
        os.close(fd)

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
        """
        Iterate through the list of rollback changesets
        @raises errors.ConaryError: raised if there's an I/O Error opening a
        changeset file
        @raises repository.filecontainer.BadContainer: raised if the changeset
        file is malformed
        @raises AssertionError: could be raised if the changeset is malformed
        @raises IOError: raised if there's a I/O Error reading compressed files
        within a changeset
        """
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
    def __del__(self):
        try:
            self.close()
        except:
            pass

    def close(self):
        """Release resources associated with this update job"""

        # When the update job goes out of scope, we close the file descriptors
        # in the lazy cache. In the future we probably need a way to
        # track exactly which files were opened by this update job, and only
        # close those, but since most of the time we're the only users of the
        # update job, it's not a huge issue -- misa 20070807
        self.lzCache.release()
        # Close db too
        if self.troveSource.db:
            self.troveSource.db.close()
            self.troveSource.db = None
        if hasattr(self.searchSource, 'db') and self.troveSource.db:
            self.searchSource.db.close()
            self.searchSource.db = None

    def addPinMapping(self, name, pinnedVersion, neededVersion):
        self.pinMapping.add((name, pinnedVersion, neededVersion))

    def getPinMaps(self):
        return self.pinMapping

    def getRollback(self):
        return self.rollback

    def setRollback(self, rollback):
        self.rollback = rollback

    def getTroveSource(self):
        """
        @return: the TroveSource for this UpdateJob
        @rtype: L{repository.trovesource.ChangeSetFilesTroveSource} 
        """
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
        """
        @return: a list of jobs
        @rtype: dict
        """
        return self.jobs

    def setJobs(self, jobs):
        self.jobs = jobs

    def setPrimaryJobs(self, jobs):
        assert(type(jobs) == set)
        self.primaries = jobs

    def getPrimaryJobs(self):
        return self.primaries

    def setCriticalJobs(self, criticalJobs):
        self.criticalJobs = criticalJobs

    def getCriticalJobs(self):
        return self.criticalJobs

    def setTransactionCounter(self, transactionCounter):
        self.transactionCounter = transactionCounter

    def getTransactionCounter(self):
        return self.transactionCounter

    def setJobsChangesetList(self, csList):
        del self.jobsCsList[:]
        self.jobsCsList.extend(csList)

    def getJobsChangesetList(self):
        return self.jobsCsList

    def getItemList(self):
        return self._itemList

    def setItemList(self, itemList):
        self._itemList = itemList

    def getKeywordArguments(self):
        return self._kwargs

    def setKeywordArguments(self, kwargs):
        self._kwargs = kwargs

    def freeze(self, frzdir, withChangesetReferences=True):
        """
        If withChangesetReferences is False, instances of ChangeSetFromFile
        that are part of this update job will be re-frozen as changeset files
        in the freeze directory.
        If withChangesetReferences is True, only references to files (file
        paths) that were used to create the ChangeSetFromFile will be stored.
        Any other type of changeset will be frozen to a changeset file.

        @raises AssertionError: raised if frzdir is non-existent or non-empty
        @raises IOError: raised if there's an I/O Error writing the frozen job
        to jobfile
        @raises OSError: raised if there is another type of error opening the
        jobfile for writing
        """

        # Require clean directory
        assert os.path.isdir(frzdir), "Not a directory: %s" % frzdir
        assert not os.listdir(frzdir), "Directory %s not empty" % frzdir

        assert isinstance(self.troveSource, 
            trovesource.ChangesetFilesTroveSource), "Unsupported %s" % self.troveSource

        drep = self._saveInvocationInfo()

        drep['troveSource'] = self._freezeChangesetFilesTroveSource(
                        self.troveSource, frzdir, withChangesetReferences)
        drep['jobs'] = list(self._freezeJobs(self.getJobs()))
        drep['primaryJobs'] = list(self._freezeJobList(self.getPrimaryJobs()))
        drep['critical'] = self.getCriticalJobs()
        drep['transactionCounter'] = self.transactionCounter
        drep['jobsCsList'] = self.jobsCsList
        drep['invalidateRollbackStack'] = int(self._invalidateRollbackStack)
        drep['jobPreScripts'] = list(self._freezeJobPreScripts())
        drep['changesetsDownloaded'] = int(self._changesetsDownloaded)

        jobfile = os.path.join(frzdir, "jobfile")

        self._saveFrozenRepr(jobfile, drep)

        return drep

    def _saveFrozenRepr(self, jobfile, drep):
        f = open(jobfile, "w+")
        util.xmlrpcDump((drep, ), stream=f)
        return drep

    def _loadFrozenRepr(self, jobfile):
        # CNY-2580: putting null chars in an XML dump was a bad idea
        tmpfd, tmpfile = util.tempfile.mkstemp()
        os.unlink(tmpfile)
        jobf = os.fdopen(tmpfd, "w")
        # replace null chars with None
        oldjobf = open(jobfile)
        while 1:
            buf = oldjobf.read(4096)
            if not buf:
                break
            jobf.write(buf.replace('\0', 'None'))
        jobf.seek(0, 0)
        try:
            ((drep, ), _) = util.xmlrpcLoad(jobf)
        except util.xmlrpclib.ResponseError:
            raise DecodingError("Error loading marshaled data")

        if 'dumpVersion' not in drep or drep['dumpVersion'] > self._dumpVersion:
            # We don't understand this format
            raise errors.InternalConaryError("Unknown dump format")

        return drep

    def saveInvocationInfo(self, jobfile):
        """Only save the information that is critical to re-create this update
        job"""
        drep = self._saveInvocationInfo()
        self._saveFrozenRepr(jobfile, drep)
        return drep

    def _saveInvocationInfo(self):
        drep = {}
        # Add some info that is critical and common
        drep['dumpVersion'] = self._dumpVersion
        # Save the Conary version
        drep['conaryVersion'] = constants.version

        # Now invocation info
        drep['itemList'] = self._freezeItemList()
        drep['keywordArguments'] = self.getKeywordArguments()
        drep['fromChangesets'] = self._freezeFromChangesets()
        drep['commitChangesetFlags'] = self._freezeCommitChangesetFlags()
        return drep

    def loadInvocationInfo(self, jobfile):
        drep = self._loadFrozenRepr(jobfile)
        return self._loadInvocationInfo(drep)

    def _loadInvocationInfo(self, drep):
        self.setItemList(self._thawItemList(drep['itemList']))
        self.setKeywordArguments(drep['keywordArguments'])
        self.setFromChangesets(self._thawFromChangesets(drep.get('fromChangesets', [])))
        self.setCommitChangesetFlags(self._thawCommitChangesetFlags(
            drep.get('commitChangesetFlags', None)))
        return drep

    def thaw(self, frzdir):
        """
        @raises DecodingError: raised if there's a error parsing the frozen
        data
        @raises errors.InternalConaryError: raised if the frozen data is a
        newer than this conary version understands
        @raises IOError: raised if there's an I/O Error opening the jobfile
        @raises OSError: raised if there are other errors opening the jobfile 
        """
        jobfile = os.path.join(frzdir, "jobfile")
        drep = self._loadFrozenRepr(jobfile)

        # Need to keep a reference to the lazy cache, or else the changesets
        # are invalid
        self._thawChangesetFilesTroveSource(drep.get('troveSource'))

        self.setJobs(list(self._thawJobs(drep['jobs'])))
        self.setPrimaryJobs(set(self._thawJobList(drep['primaryJobs'])))
        self.setJobsChangesetList(drep['jobsCsList'])
        self.setItemList(self._thawItemList(drep['itemList']))
        self.setKeywordArguments(drep['keywordArguments'])

        self.setCriticalJobs(drep['critical'])
        self.transactionCounter = drep['transactionCounter']
        self._invalidateRollbackStack = bool(
                                        drep.get('invalidateRollbackStack'))
        self._jobPreScripts = list(self._thawJobPreScripts(
            list(drep.get('jobPreScripts', []))))
        self._changesetsDownloaded = bool(drep.get('changesetsDownloaded', 0))
        self._fromChangesets = self._thawFromChangesets(drep.get('fromChangesets', []))

    def _freezeJobs(self, jobs):
        for jobList in jobs:
            yield list(self._freezeJobList(jobList))

    def _thawJobs(self, jobs):
        for jobList in jobs:
            yield list(self._thawJobList(jobList))

    def _freezeJob(self, job):
        (trvName, (oRev, oFlv), (rev, flv), searchLocalRepo) = job
        return (trvName,
                (self._freezeRevision(oRev), self._freezeFlavor(oFlv)),
                (self._freezeRevision(rev), self._freezeFlavor(flv)),
                int(searchLocalRepo))

    def _freezeJobList(self, jobList):
        for job in jobList:
            yield self._freezeJob(job)

    def _thawJob(self, job):
        (trvName, (oRev, oFlv), (rev, flv), searchLocalRepo) = job
        return (trvName,
                (self._thawRevision(oRev), self._thawFlavor(oFlv)),
                (self._thawRevision(rev), self._thawFlavor(flv)),
                bool(searchLocalRepo))

    def _thawJobList(self, jobList):
        for job in jobList:
            yield self._thawJob(job)

    def _freezeRevision(self, rev):
        if rev is None:
            return ''
        return rev.freeze()

    def _thawRevision(self, rev):
        if '' == rev:
            return None
        return versions.ThawVersion(rev)

    def _freezeFlavor(self, flavor):
        if flavor is None:
            # Frozen flavors are either empty strings or start with a digit
            return '*None*'
        return flavor.freeze()

    def _thawFlavor(self, flavor):
        if '*None*' == flavor:
            return None
        return deps.ThawFlavor(flavor)

    def _freezeJobPreScripts(self):
        # Freeze the job and the string together
        for item in self._jobPreScripts:
            yield (self._freezeJob(item[0]), item[1], item[2], item[3])

    def _thawJobPreScripts(self, frzrepr):
        for item in frzrepr:
            yield (self._thawJob(item[0]), item[1], item[2], item[3])

    def _freezeChangesetFilesTroveSource(self, troveSource, frzdir,
                                        withChangesetReferences=True):
        assert isinstance(troveSource, trovesource.ChangesetFilesTroveSource)
        frzrepr = {'type' : 'ChangesetFilesTroveSource'}
        ccsdir = os.path.join(frzdir, 'changesets')
        util.mkdirChain(ccsdir)
        csList = []
        itersrc = itertools.izip(troveSource.csList, troveSource.csFileNameList)
        for i, (cs, (csFileName, includesFileContents)) in enumerate(itersrc):
            if withChangesetReferences and csFileName:
                fname = csFileName
            else:
                fname = os.path.join(ccsdir, "%03d.ccs" % i)
                cs.writeToFile(fname)

            csList.append((fname, int(includesFileContents)))

        frzrepr['changesets'] = csList

        return frzrepr

    def _thawChangesetFilesTroveSource(self, frzrepr):
        assert frzrepr.get('type') == 'ChangesetFilesTroveSource'

        troveSource = self.getTroveSource()
        csList = frzrepr['changesets']
        try:
            for (csFileName, includesFileContents) in csList:
                cs = changeset.ChangeSetFromFile(self.lzCache.open(csFileName))
                troveSource.addChangeSet(cs, bool(includesFileContents))
        except IOError, e:
            raise errors.InternalConaryError("Missing changeset file %s" % 
                                             e.filename)

        return self.lzCache

    def _freezeFromChangesets(self):
        ret = []
        cwd = os.getcwd()
        for f in self._fromChangesets:
            if not f.fileName:
                continue
            fn = f.fileName
            if not fn.startswith('/'):
                fn = util.joinPaths(cwd, fn)
            ret.append(fn)
        return ret

    def _thawFromChangesets(self, rlist):
        if not rlist:
            return []
        return [ changeset.ChangeSetFromFile(self.lzCache.open(x))
                 for x in rlist ]

    def __freezeVF(self, tup):
        ver = tup[0]
        if ver is None:
            ver = ''
        elif not isinstance(ver, str):
            # Make it a list (tuple would be better but XMLRPC will convert it
            # to a list anyway)
            ver = [ver.__class__.__name__, ver.asString()]
        flv = tup[1]
        if flv is None:
            flv = 'None'
        else:
            flv = str(flv)
        return (ver, flv)

    def __thawVF(self, tup):
        ver = tup[0]
        if ver == '':
            ver = None
        elif isinstance(ver, type([])):
            if ver[0] == 'Version':
                ver = versions.VersionFromString(ver[1])
            else:
                # This is not really something we know how to thaw.
                ver = ver[1]
        flv = tup[1]
        if flv == 'None':
            flv = None
        else:
            flv = deps.ThawFlavor(flv)
        return (ver, flv)

    def _freezeItemList(self):
        return [ (t[0], self.__freezeVF(t[1]), self.__freezeVF(t[2]), int(t[3]))
                 for t in self.getItemList() ]

    def _thawItemList(self, frzrep):
        return [ (t[0],self. __thawVF(t[1]), self.__thawVF(t[2]), bool(t[3]))
                 for t in frzrep ]

    def _freezeCommitChangesetFlags(self):
        flags = self.getCommitChangesetFlags()
        if flags is None:
            return {}
        return dict((x, int(getattr(flags, x))) for x in flags.__slots__)

    def _thawCommitChangesetFlags(self, frzrep):
        frzdict = dict()
        if frzrep is not None:
            for k, v in frzrep.items():
                if k in CommitChangeSetFlags.__slots__:
                    frzdict[k] = bool(v)
        flags = CommitChangeSetFlags(**frzdict)
        return flags

    def setInvalidateRollbacksFlag(self, flag):
        self._invalidateRollbackStack = bool(flag)

    def updateInvalidatesRollbacks(self):
        """
        @return: Does applying this update invalidate the rollback stack?
        @rtype: bool
        """
        return self._invalidateRollbackStack

    def addJobPreScript(self, job, script, oldCompatClass, newCompatClass):
        self._jobPreScripts.append((job, script, oldCompatClass,
                                    newCompatClass))

    def iterJobPreScripts(self):
            for i in self._jobPreScripts:
                yield i

    def splitCriticalJobs(self):
        criticalJobs = self.getCriticalJobs()
        if not criticalJobs:
            return [], self.getJobs()
        jobs = self.getJobs()
        firstCritical = criticalJobs[0]
        criticalJobs = jobs[:firstCritical + 1]
        remainingJobs = jobs[firstCritical + 1:]
        return criticalJobs, remainingJobs

    def loadCriticalJobsOnly(self):
        """Loads the critical jobs and returns the remaining jobs to be
        performed"""
        criticalJobs, remainingJobs = self.splitCriticalJobs()
        if not criticalJobs:
            # No critical jobs, so nothing remaining
            return []
        self.setJobs(criticalJobs)
        # This update job no longer has critical update jobs
        self.setCriticalJobs([])
        return remainingJobs

    def getChangesetsDownloaded(self):
        return self._changesetsDownloaded

    def setChangesetsDownloaded(self, downloaded):
        self._changesetsDownloaded = downloaded

    def setFromChangesets(self, fromChangesets):
        self._fromChangesets = fromChangesets

    def getFromChangesets(self):
        return self._fromChangesets

    def setRestartedFlag(self, flag):
        self._restartedFlag = flag

    def getRestartedFlag(self):
        return self._restartedFlag

    def setCommitChangesetFlags(self, commitFlags):
        self._commitFlags = commitFlags

    def getCommitChangesetFlags(self):
        return self._commitFlags

    def __init__(self, db, searchSource = None, lazyCache = None):
        # 20070714: lazyCache can be None for the users of the old API (when
        # an update job was instantiated directly, instead of using the
        # client's newUpdateJob(). At some point we should deprecate that.
        if lazyCache is None:
            lazyCache = util.LazyFileCache()
        self.lzCache = lazyCache
        self.jobs = []
        self.pinMapping = set()
        self.rollback = None
        self.troveSource = trovesource.ChangesetFilesTroveSource(db)
        self.primaries = set()
        self.criticalJobs = []
        # Changesets with files - a parallel list to self.jobs
        self.jobsCsList = []

        self.searchSource = searchSource
        self.transactionCounter = None
        # Version of the serialized format we support
        self._dumpVersion = 1
        # The options that created this update job
        self._itemList = []
        self._kwargs = {}
        self._fromChangesets = []
        # Applying this update job invalidates the rollback stack
        self._invalidateRollbackStack = False
        # List of pre scripts to run for a particular job. This is ordered.
        self._jobPreScripts = []
        # Changesets have been downloaded
        self._changesetsDownloaded = False
        # This flag gets set if the update job was loaded from the restart
        # information
        self._restartedFlag = False

        self._commitFlags = None

class SqlDbRepository(trovesource.SearchableTroveSource,
                      datastore.DataStoreRepository,
		      repository.AbstractRepository):

    def iterAllTroveNames(self):
	return self.db.iterAllTroveNames()

    def iterAllTroves(self):
	return self.db.iterAllTroves()

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
        """
        @raises TroveMissing:
        @note:
            As this calls database functions, it could also raise any type of
        DatabaseError defined in L{dbstore.sqlerrors}
        """
        l = self.getTroves([ (name, version, flavor) ], pristine = pristine,
                           withDeps = withDeps, withFiles = withFiles)
        if l[0] is None:
            raise errors.TroveMissing(name, version)

        return l[0]

    def getTroves(self, troveList, pristine = True, withFiles = True,
                  withDeps = True, callback = None):
        if not troveList:
            return []
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
        if not troves:
            return []
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

    def getTroveVersionList(self, name, withFlavors = False, troveTypes=None):
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

    def getConfigFileContents(self, sha1):
        return filecontents.FromDataStore(self.contentsStore, sha1)

    def findFileVersion(self, fileId):
        return self.db.findFileVersion(fileId)

    def getFileVersions(self, l, allowMissingFiles=False):
	return self.db.iterFiles(l)

    def getTransactionCounter(self):
        """
        @raises ConaryError: if the self._db is somehow not initialized and
        there's a DatabaseError on intialization, it is re-raised as
        L{errors.ConaryError}
        """
        return self.db.getTransactionCounter()

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
        self._updateTransactionCounter = True
	self.db.addFile(troveId, pathId, fileObj, path, fileId, version,
                        fileStream = fileStream, isPresent = isPresent)

    def addTrove(self, trove, pin = False, oldTroveSpec = None):
        self._updateTransactionCounter = True
	return self.db.addTrove(trove, pin = pin, oldTroveSpec = oldTroveSpec)

    def addTroveDone(self, troveInfo):
        self._updateTransactionCounter = True
        return self.db.addTroveDone(troveInfo)

    def pinTroves(self, troveList, pin):
        troves = self.getTroves(troveList)

        for trove in troves:
            for subTrove in self.walkTroveSet(trove):
                self.db.pinTroves(subTrove.getName(),
                                  subTrove.getVersion(),
                                  subTrove.getFlavor(), pin = pin)

        if troves:
            self._updateTransactionCounter = True
        self.commit()

    def trovesArePinned(self, troveList):
        """
        See L{local.sqldb.Database.trovesArePinned} and others for DB-specific
        implementation
        """
        return self.db.trovesArePinned(troveList)

    def commit(self):
        # At this point we should already have a write lock on the database, 
        # we can safely increment the transaction count
        # This works as long as the underlying database has only
        # database-level locking. If table locking or row locking are
        # available, we need a different technique
        if self._updateTransactionCounter:
            self.db.incrementTransactionCounter()
	self.db.commit()

    def close(self):
        if self.dbpath == ':memory:':
            # No resources associated with an in-memory database
            # And no locks either
            return

        if self._db:
            self.db.close()
            self._db = None

        # Close the lock file as well
        self.commitLock(False)

    def eraseTrove(self, troveName, version, flavor):
        self._updateTransactionCounter = True
	self.db.eraseTrove(troveName, version, flavor)

    def pathIsOwned(self, path):
	return self.db.pathIsOwned(path)

    def eraseFileVersion(self, pathId, version):
	# files get removed with their troves
	pass

    def writeAccess(self):
        assert(self.db) # when checking for write access, make sure the
                        # db has been initialized
        return os.access(self.dbpath, os.W_OK)

    def _initDb(self):
        self._db = sqldb.Database(self.dbpath, timeout = self._lockTimeout)
        datastore.DataStoreRepository.__init__(self,
                           dataStore = localrep.SqlDataStore(self.db.db))

    def _getDb(self):
        if not self._db:
            try:
                self._initDb()
            except sqldb.sqlerrors.DatabaseError, e:
                raise errors.ConaryError("Database error: %s" % (e, ))
        return self._db

    db = property(_getDb)

    def __init__(self, path, timeout=None):
        if path == ":memory:":
            self.dbpath = path
        else:
            self.dbpath = path + "/conarydb"

        self._db = None
        repository.AbstractRepository.__init__(self)
        trovesource.SearchableTroveSource.__init__(self)
        self._updateTransactionCounter = False
        # Locking timeout
        self._lockTimeout = timeout

class Database(SqlDbRepository):

    # XXX some of these interfaces are horribly inefficient as we have
    # to instantiate a full trove object to do anything... 
    # FilesystemRepository has the same problem

    ROLLBACK_PHASE_LOCAL = update.ROLLBACK_PHASE_LOCAL

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

    def depCheck(self, jobSet, troveSource, findOrdering = False,
                 linkedJobs = None, criticalJobs = None,
                 finalJobs = None, criticalOnly = False):
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
        @type findOrdering: boolean
        @param criticalJobs: list of jobs that should be applied as early
        as possible.
        @type criticalJobs: list of job tuples
        @param finalJobs: list of jobs that should be applied as a part of the
        last job.
        @type finalJobs: list of job tuples
        @rtype: tuple of dependency failures for new packages and
                dependency failures caused by removal of existing
                packages
        """

        checker = self.dependencyChecker(troveSource)
        checker.addJobs(jobSet)
        unsatisfiedList, unresolveableList, changeSetList, criticalUpdates = \
                checker.check(findOrdering = findOrdering,
                              linkedJobs = linkedJobs,
                              criticalJobs = criticalJobs,
                              finalJobs = finalJobs)

        if criticalOnly and criticalUpdates:
            changeSetList = changeSetList[:criticalUpdates[0] + 1]
            jobSet.clear()
            jobSet.update(itertools.chain(*changeSetList))
            if criticalUpdates and (unresolveableList or unsatisfiedList):
                # we're trying to apply only critical updates, but
                # there's a dep failure somewhere in the entire job.
                # Try again to resolve dependencies, using only
                # the critical changes
                checker.done()
                checker = self.dependencyChecker(troveSource)
                checker.addJobs(jobSet)
                (unsatisfiedList, unresolveableList, changeSetList,
                 criticalUpdates) = checker.check(findOrdering = findOrdering,
                                                  linkedJobs = linkedJobs)
                criticalUpdates = []
        checker.done()
        if criticalJobs is None and finalJobs is None:
            # backwards compatibility.  For future code, pass in 
            # criticalJobs = [] to make sure you get a consistant
            # return value.  FIXME when we can break bw compatibility,
            # we should remove this inconsistent
            return (unsatisfiedList, unresolveableList, changeSetList)
        return (unsatisfiedList, unresolveableList, changeSetList, 
                criticalUpdates)

    def dependencyChecker(self, troveSource):
        return self.db.dependencyChecker(troveSource)

    # local changes includes the A->A.local portion of a rollback; if it
    # doesn't exist we need to compute that and save a rollback for this
    # transaction
    def commitChangeSet(self, cs, uJob,
                        rollbackPhase = None, updateDatabase = True,
                        tagScript = None,
			journal = None,
                        callback = UpdateCallback(),
                        removeHints = {}, autoPinList = RegularExpressionList(),
                        deferredScripts = None, commitFlags = None):
	assert(not cs.isAbsolute())

        if commitFlags is None:
            commitFlags = CommitChangeSetFlags()

        flags = update.UpdateFlags(
            replaceManagedFiles = commitFlags.replaceManagedFiles,
            replaceUnmanagedFiles = commitFlags.replaceUnmanagedFiles,
            replaceModifiedFiles = commitFlags.replaceModifiedFiles,
            replaceModifiedConfigFiles = commitFlags.replaceModifiedConfigFiles)

        if rollbackPhase:
            flags.missingFilesOkay = True
            flags.ignoreInitialContents = True

        self.db.begin()

	for trv in cs.iterNewTroveList():
	    if trv.getName().endswith(":source"):
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
		trv = dbCache.getTrove(name, old, flavor, pristine = False)
		origTrove = dbCache.getTrove(name, old, flavor, pristine = True)
		assert(trv)
		troveList.append((trv, origTrove, ver, flags))

        for (name, version, flavor) in cs.getOldTroveList():
            rollbackVersion = version.createShadow(versions.RollbackLabel())
            trv = dbCache.getTrove(name, version, flavor, pristine = False)
            origTrove = dbCache.getTrove(name, version, flavor, 
                                         pristine = True)
            assert(trv)
            troveList.append((trv, origTrove, rollbackVersion,
                              update.UpdateFlags(missingFilesOkay = True)))

        callback.creatingRollback()

	result = update.buildLocalChanges(self, troveList, root = self.root,
                                          callback = callback)
	if not result: return

        retList = result[1]
        localRollback = changeset.ReadOnlyChangeSet()
        localRollback.merge(result[0])

        fsTroveDict = {}
        for (changed, fsTrove) in retList:
            fsTroveDict[fsTrove.getNameVersionFlavor()] = fsTrove

	if rollbackPhase is None:
            reposRollback = cs.makeRollback(dbCache, configFiles = True,
                       redirectionRollbacks = (not commitFlags.localRollbacks))
            flags.merge = True

        fsJob = update.FilesystemJob(dbCache, cs, fsTroveDict, self.root,
                                     flags = flags, callback = callback,
                                     removeHints = removeHints,
                                     rollbackPhase = rollbackPhase,
                                     deferredScripts = deferredScripts)

        if rollbackPhase is None:
            # this is the rollback for files which the user is forcing the
            # removal of (probably due to removeFiles)
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

                # troves can only be removed for one reason (either an update
                # to one thing or erased)
                assert(localCs.getOldVersion() == removeCs.getOldVersion() and
                       localCs.getOldFlavor() == removeCs.getOldFlavor())

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
	if (rollbackPhase is None) and not commitFlags.test:
            rollback = uJob.getRollback()
            if rollback is None:
                rollback = self.createRollback()
                uJob.setRollback(rollback)
            rollback.add(reposRollback, localRollback)
            del rollback

        if not commitFlags.justDatabase:
            # run preremove scripts before updating the database, otherwise
            # the file lists which get sent to them are incorrect. skipping
            # this makes --test a little inaccurate, but life goes on
            if not commitFlags.test:
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
                csJob = localrep.LocalRepositoryChangeSetJob(
                    dbCache, cs, callback, autoPinList, 
                    allowIncomplete = (rollbackPhase is not None),
                    pathRemovedCheck = fsJob.pathRemoved,
                    replaceFiles = flags.replaceManagedFiles)
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
            # make sure we release the lock on the database
            self.db.rollback()
            raise CommitError, ('applying update would cause errors:\n' + 
                                '\n\n'.join(str(x) for x in errList))
        if commitFlags.test:
            self.db.rollback()
            return

        if not commitFlags.justDatabase:
            fsJob.apply(tagSet, tagScript, journal,
                        keepJournal = commitFlags.keepJournal,
                        opJournalPath = self.opJournalPath)

        if updateDatabase:
            for (name, version, flavor) in fsJob.getOldTroveList():
		# if to database if false, we're restoring the local
		# branch of a rollback
		self.db.eraseTrove(name, version, flavor)

	# finally, remove old directories. right now this has to be done
	# after the sqldb has been updated (but before the changes are
	# committted)
        if not commitFlags.justDatabase:
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
        self._updateTransactionCounter = True
	self.commit()

        if rollbackPhase is None and updateDatabase and \
                csJob.invalidateRollbacks():
            self.invalidateRollbacks()

        if rollbackPhase is not None:
            return fsJob

        if not commitFlags.justDatabase:
            fsJob.runPostScripts(tagScript, rollbackPhase)

    def runPreScripts(self, uJob, callback, tagScript = None,
                      isRollback = False, justDatabase = False,
                      tmpDir = '/'):
        if isRollback or justDatabase:
           return True

        for (job, script, oldCompatClass, newCompatClass) in \
                                                uJob.iterJobPreScripts():
            scriptId = "%s preupdate" % job[0]
            rc = update.runTroveScript(job, script, tagScript, tmpDir,
                                       self.root, callback, isPre = True,
                                       scriptId = scriptId,
                                       oldCompatClass = oldCompatClass,
                                       newCompatClass = newCompatClass)
            if rc:
                return False

        return True


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
                        localCs.addFileContents(pathId, fileId,
                                changeset.ChangedFileTypes.hldr,
                                filecontents.FromString(""), False)
                    else:
                        fileId = f.fileId()
                        newTrv.addFile(pathId, path, fileVersion, fileId)
                        localCs.addFile(None, fileId, f.freeze())
                        localCs.addFileContents(pathId, fileId,
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

        self._updateTransactionCounter = True
        self.commit()

    def commitLock(self, acquire):
        if not acquire:
            if self.lockFileObj is not None:
                # closing frees the lockf() lock
                self.lockFileObj.close()
                self.lockFileObj = None
        else:
            if self.lockFile == ":memory:":
                # not sure how we can lock a :memory: database without
                # knowing we can drop a lock file (any|some)where
                return
            try:
                lockFd = os.open(self.lockFile, os.O_RDWR | os.O_CREAT |
                                                    os.O_EXCL)
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise

                lockFd = None

            if lockFd is None:
                lockFd = os.open(self.lockFile, os.O_RDWR)

            fcntl.fcntl(lockFd, fcntl.F_SETFD,
                        fcntl.fcntl(lockFd, fcntl.F_GETFD) | fcntl.FD_CLOEXEC)

            try:
                fcntl.lockf(lockFd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError, e:
                # Close the lock object file descriptor, we don't want leaks,
                # even on the error code path
                os.close(lockFd)
                if e.errno in (errno.EACCES, errno.EAGAIN):
                    raise DatabaseLockedError
                raise

            self.lockFileObj = os.fdopen(lockFd)

    def createRollback(self):
	rbDir = self.rollbackCache + ("/%d" % (self.lastRollback + 1))
        if os.path.exists(rbDir):
            shutil.rmtree(rbDir)
        os.mkdir(rbDir, 0700)
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

        fd = os.open(newStatus, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0600)
        os.write(fd, "%s %d\n" % (self.firstRollback, self.lastRollback))
        os.close(fd)

	os.rename(newStatus, self.rollbackStatus)

    def getRollbackList(self):
        self._ensureReadableRollbackStack()
	list = []
	for i in range(self.firstRollback, self.lastRollback + 1):
	    list.append("r.%d" % i)

	return list

    def iterRollbacksList(self):
        """Generator for rollback data.
        Returns a list of (rollbackName, rollback)
        """
        for rollbackName in reversed(self.getRollbackList()):
            rb = self.getRollback(rollbackName)
            yield (rollbackName, rb)

    def invalidateRollbacks(self):
        """Invalidate the rollback stack."""
        # Works nicely for the very beginning
        # (when firstRollback, lastRollback) = (0, -1)
        self.firstRollback = self.lastRollback + 1
        self.writeRollbackStatus()

    def readRollbackStatus(self):
        try:
            f = open(self.rollbackStatus)
            (first, last) = f.read()[:-1].split()
            self.firstRollback = int(first)
            self.lastRollback = int(last)
            f.close()
        except IOError, e:
            if e.errno == errno.EACCES:
                self.firstRollback = None
                self.lastRollback = None
            else:
                raise

    def _ensureReadableRollbackStack(self):
        if (self.firstRollback, self.lastRollback) == (None, None):
            raise ConaryError("Unable to open rollback directory")

    def hasRollback(self, name):
	try:
	    num = int(name[2:])
	except ValueError:
	    return False

        self._ensureReadableRollbackStack()

	if (num >= self.firstRollback and num <= self.lastRollback):
	    return True
	
	return False

    def getRollback(self, name):
	if not self.hasRollback(name): return None

	num = int(name[2:])
        dir = self.rollbackCache + "/" + "%d" % num
        return Rollback(dir, load = True)

    def applyRollbackList(self, *args, **kwargs):
        try:
            self.commitLock(True)
            return self._applyRollbackList(*args, **kwargs)
        finally:
            self.commitLock(False)
            self.close()

    def _applyRollbackList(self, repos, names, replaceFiles = False,
                          callback = UpdateCallback(), tagScript = None,
                          justDatabase = False, transactionCounter = None):
        assert transactionCounter is not None, ("The transactionCounter "
            "argument is mandatory")
        if transactionCounter != self.getTransactionCounter():
            raise RollbackError(names, "Database state has changed, please "
                "run the rollback command again")

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
                    if not trvCs.getType() == trove.TROVE_TYPE_REDIRECT: 
                        continue
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
                    fsJob = None
                    commitFlags = CommitChangeSetFlags(
                        replaceManagedFiles = replaceFiles,
                        replaceUnmanagedFiles = replaceFiles,
                        replaceModifiedFiles = replaceFiles,
                        justDatabase = justDatabase)

                    if not reposCs.isEmpty():
                        itemCount += 1
                        callback.setUpdateHunk(itemCount, totalCount)
                        callback.setUpdateJob(reposCs.getJobSet())
                        fsJob = self.commitChangeSet(
                                             reposCs, UpdateJob(None),
                                             rollbackPhase =
                                                update.ROLLBACK_PHASE_REPOS,
                                             removeHints = removalHints,
                                             callback = callback,
                                             tagScript = tagScript,
                                             commitFlags = commitFlags)

                    if not localCs.isEmpty():
                        itemCount += 1
                        callback.setUpdateHunk(itemCount, totalCount)
                        callback.setUpdateJob(localCs.getJobSet())
                        self.commitChangeSet(localCs, UpdateJob(None),
                                     rollbackPhase =
                                            update.ROLLBACK_PHASE_LOCAL,
                                     updateDatabase = False,
                                     callback = callback,
                                     tagScript = tagScript,
                                     commitFlags = commitFlags)

                    if fsJob:
                        # Because of the two phase update for rollbacks, we
                        # run postscripts by hand instead of commitChangeSet
                        # doing it automatically
                        fsJob.runPostScripts(tagScript, True)

                    rb.removeLast()
                except CommitError, err:
                    raise RollbackError(name, err)

                (reposCs, localCs) = rb.getLast()

            self.removeRollback(name)

    def getPathHashesForTroveList(self, troveList):
        return self.db.getPathHashesForTroveList(troveList)

    def getTroveCompatibilityClass(self, name, version, flavor):
        return self.db.getTroveCompatibilityClass(name, version, flavor)

    def iterFindPathReferences(self, path, justPresent = False):
        return self.db.iterFindPathReferences(path, justPresent = justPresent)

    def getTrovesWithProvides(self, depSetList, splitByDep=False):
        """
        Get the troves that provide each dependency set listed.
        @return: A dict { depSet : [troveTup, troveTup] } of local
           troves that provide each dependency set listed.
        @param splitByDep: (added for backwards compatibility) If True,
           returns dependeny solutions in the standard format, where the
           solution for each dependency set is a list of lists, where within
           each list is the solution for one dependency in the dependency set.
           If False (the default), all those lists are combined together into
           one list of solutions for the entire dependency set.
        @type splitByDep: bool
        @rtype: dict
        """
        rc = self.db.getTrovesWithProvides(depSetList)
        if splitByDep:
            return rc
        return dict((x[0], list(set(itertools.chain(*x[1]))))
                     for x in rc.items())


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

    @staticmethod
    def revertJournal(root, path):
        top = util.joinPaths(root, path)
        opJournalPath = top + '/journal'
        try:
            j = journal.JobJournal(opJournalPath, root)
        except OSError, e:
            raise OpenError(top, 'journal error: ' + e.strerror)

        j.revert()
        os.unlink(opJournalPath)

    def __init__(self, root, path, timeout = None):
        """
        Instantiate a database object
        @param root: the path to '/' for this operation
        @type root: string
        @param path: the path to the database relative to 'root'
        @type path: string
        @return: None
        @raises ExistingJournalError: Raised when a journal file exists,
        signifying a failed operation.
        @raises OpenError: raised when directory creation fails, usually due to
        a nonexistent directory in the path
        @raises OSError: raised when directory creation fails for reasons other
        than those caught by OpenError
        @raises IOError: raised when an I/O error occurs in readRollbackStatus
        """

	self.root = root

        if path == ":memory:": # memory-only db
            SqlDbRepository.__init__(self, ':memory:', timeout = timeout)
            # use :memory: as a marker not to bother with locking
            self.lockFile = path 
        else:
            self.opJournalPath = util.joinPaths(root, path) + '/journal'
            top = util.joinPaths(root, path)

            if os.path.exists(self.opJournalPath):
                raise ExistingJournalError(top,
                        'journal file exists. use revert command to '
                        'undo the previous (failed) operation')

            self.lockFile = top + "/syslock"
            self.lockFileObj = None
            self.rollbackCache = top + "/rollbacks"
            self.rollbackStatus = self.rollbackCache + "/status"
            if not os.path.exists(self.rollbackCache):
                try:
                    util.mkdirChain(top)
                    os.mkdir(self.rollbackCache, 0700)
                except OSError, e:
                    if e.errno == errno.ENOTDIR:
                        # when making a directory, the partent
                        # wat not a directory
                        d = os.path.dirname(e.filename)
                        raise OpenError(top, '%s is not a directory' %d)
                    elif e.errno == errno.EACCES:
                        d = os.path.dirname(e.filename)
                        raise OpenError(top, 'cannot create directory %s' %d)
                    else:
                        raise

            if not os.path.exists(self.rollbackStatus):
                self.firstRollback = 0
                self.lastRollback = -1
            else:
                self.readRollbackStatus()
            SqlDbRepository.__init__(self, root + path, timeout = timeout)

class DatabaseCacheWrapper:

    def __getattr__(self, attr):
        return getattr(self.db, attr)

    def getTrove(self, name, version, flavor, pristine = True, *args, **kw):
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

class ExistingJournalError(OpenError):

    pass

class CommitError(DatabaseError, errors.InternalConaryError):
    pass
