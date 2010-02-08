#
# Copyright (c) 2004-2009 rPath, Inc.
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
import tempfile

#conary
from conary import constants, files, trove, versions
from conary.build import tags
from conary.errors import ConaryError, DatabaseError, DatabasePathConflicts
from conary.errors import DatabaseLockedError, DecodingError
from conary.callbacks import UpdateCallback
from conary.conarycfg import RegularExpressionList
from conary.deps import deps
from conary.lib import log, sha1helper, sigprotect, util, api
from conary.local import localrep, sqldb, schema, update
from conary.local.errors import DatabasePathConflictError, FileInWayError
from conary.local.journal import JobJournal, NoopJobJournal
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

    def add(self, opJournal, repos, local, rollbackScripts):
        reposName = self.reposName % (self.dir, self.count)
        localName = self.localName % (self.dir, self.count)
        countName = "%s/count" % self.dir

        opJournal.create(reposName)
        opJournal.create(localName)

        if rollbackScripts:
            # XXX We need to import rollbacks here to avoid a circular
            # import. We should refactor rollbacks.py to not import
            # local/database.py
            from conary import rollbacks
            rbs = rollbacks._RollbackScripts()
            for job, sData, oldCompat, newCompat in rollbackScripts:
                rbs.add(job, sData, oldCompat, newCompat)

            # Mark the files to be created in the journal
            for fileName in rbs.getCreatedFiles(self.dir):
                opJournal.create(fileName)

            rbs.save(self.dir)

        repos.writeToFile(reposName, mode = 0600)
        local.writeToFile(localName, mode = 0600)

        if self.count:
            self.count += 1
            opJournal.backup(countName)
        else:
            self.count = 1
            opJournal.create(countName)

        fd, tmpname = tempfile.mkstemp('count', '.ct', self.dir)
        os.rename(tmpname, countName)

        os.write(fd, "%d\n" % self.count)
        os.close(fd)

    def _getChangeSets(self, item, repos = True, local = True):
        if repos:
            reposCs = changeset.ChangeSetFromFile(
                                        self.reposName % (self.dir, item))
        else:
            reposCs = False

        if local:
            localCs = changeset.ChangeSetFromFile(
                                        self.localName % (self.dir, item))
        else:
            localCs = False

        return (reposCs, localCs)

    def getLast(self):
        if not self.count:
            return (None, None)
        return self._getChangeSets(self.count - 1)

    def getLastPostRollbackScripts(self):
        if not self.count:
            return []

        from conary import rollbacks
        try:
            rbs = rollbacks._RollbackScripts.load(self.dir)
        except rollbacks.RollbackScriptsError:
            return

        ret = []
        for idx, job, script, oldCompCls, newCompCls in rbs:
            # We only use the first conversion, since only one was added
            ret.append((job, script, oldCompCls, newCompCls))
        return ret

    def getLocalChangeset(self, i):
        local = changeset.ChangeSetFromFile(self.localName % (self.dir, i))
        return local

    def isLocal(self):
        """
        Return True if every element of the rollback is locally available,
        False otherwise.
        """
        for i in range(self.count):
            (reposCs, localCs) = self._getChangeSets(i, repos = True,
                                                     local = False)
            for trvCs in reposCs.iterNewTroveList():
                if trvCs.getType() == trove.TROVE_TYPE_REDIRECT:
                    return False

        return True

    def removeLast(self):
        if self.count == 0:
            return
        os.unlink(self.reposName % (self.dir, self.count - 1))
        os.unlink(self.localName % (self.dir, self.count - 1))
        self.count -= 1
        open("%s/count" % self.dir, "w").write("%d\n" % self.count)

    @api.publicApi
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

class RollbackStack:

    def _readStatus(self):
        if not os.path.exists(self.statusPath):
            self.first = 0
            self.last = -1
            return

        try:
            f = open(self.statusPath)
            (first, last) = f.read()[:-1].split()
            self.first = int(first)
            self.last = int(last)
            f.close()
        except IOError, e:
            if e.errno == errno.EACCES:
                self.first = None
                self.last = None
            else:
                raise

    def _ensureReadableRollbackStack(self):
        if (self.first, self.last) == (None, None):
            raise ConaryError("Unable to open rollback directory")

    def writeStatus(self, opJournal = None):
        newStatus = self.statusPath + ".new"

        fd = os.open(newStatus, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0600)
        os.write(fd, "%s %d\n" % (self.first, self.last))
        os.close(fd)

        if opJournal:
            opJournal.backup(self.statusPath)

        os.rename(newStatus, self.statusPath)

    def new(self, opJournal = None):
        if not opJournal:
            opJournal = NoopJobJournal()
        rbDir = "%s/%d" % (self.dir, self.last + 1)
        if os.path.exists(rbDir):
            opJournal.backup(rbDir)
            shutil.rmtree(rbDir)

        opJournal.mkdir(rbDir)
        os.mkdir(rbDir, 0700)
        self.last += 1
        self.writeStatus(opJournal = opJournal)
        return Rollback(rbDir)

    def hasRollback(self, name):
        try:
            num = int(name[2:])
        except ValueError:
            return False

        self._ensureReadableRollbackStack()

        if (num >= self.first and num <= self.last):
            return True

        return False

    def getRollback(self, name):
        if not self.hasRollback(name): return None

        num = int(name[2:])
        dir = self.dir + "/" + "%d" % num
        return Rollback(dir, load = True)

    def removeFirst(self):
        name = 'r.%d' % self.first
        self.remove(name)

    def removeLast(self):
        name = 'r.%d' % self.last
        self.remove(name)

    def getList(self):
        self._ensureReadableRollbackStack()
        lst = []
        for i in range(self.first, self.last + 1):
            lst.append("r.%d" % i)

        return lst

    # name looks like "r.%d"
    def remove(self, name):
        rollback = int(name[2:])
        assert(rollback == self.first or rollback == self.last)

        try:
            shutil.rmtree(self.dir + "/%d" % rollback)
        except OSError, e:
            if e.errno == 2:
                pass
        if rollback == self.last:
            self.last -= 1
        elif rollback == self.first:
            self.first += 1
        else:
            assert(0)

        self.writeStatus()

    def invalidate(self):
        """
        Invalidate the rollback stack. It doesn't remove the rollbacks
        though.
        """
        # Works nicely for the very beginning
        # (when rollbackStack.first, rollbackStack.last) = (0, -1)
        self.first = self.last + 1
        self.writeStatus()

    def iter(self):
        """Generator for rollback data.
        Returns a list of (rollbackName, rollback)
        """
        for rollbackName in reversed(self.getList()):
            rb = self.getRollback(rollbackName)
            yield (rollbackName, rb)

    def __init__(self, rbDir):
        self.dir = rbDir
        self.statusPath = self.dir + '/status'

        if not os.path.exists(self.dir):
            try:
                util.mkdirChain(os.path.dirname(self.dir))
                os.mkdir(self.dir, 0700)
            except OSError, e:
                if e.errno == errno.ENOTDIR:
                    # when making a directory, the parent
                    # was not a directory
                    d = os.path.dirname(e.filename)
                    raise OpenError(self.dir, '%s is not a directory' %d)
                elif e.errno == errno.EACCES:
                    raise OpenError(self.dir, 'cannot create directory %s' %
                                               e.filename)
                else:
                    raise

        self._readStatus()

class UpdateJobFeatures(util.Flags):
    """
    Features of an update job.
    This class is used mostly when loading a saved update job from the restart
    directory, since it helps determine support for a specific feature in the
    version of Conary used for generating the serialized representation of the
    update job.
    """
    __slots__ = [ "postRollbackScriptsOnRollbackStack" ]

    def __init__(self):
        util.Flags.__init__(self)
        self.setAll(True)

    def setAll(self, value = True):
        """
        Set all flags to the specified value (True by default)
        @return: C{self}
        """
        for slot in self.__slots__:
            setattr(self, slot, value)
        return self

    def saveToFile(self, filePath):
        """
        Save features to a file
        """
        f = file(filePath, "w")
        for slot in self.__slots__:
            if getattr(self, slot):
                f.write(slot)
                f.write("\n")
        return self

    def loadFromFile(self, filePath):
        """
        Load features from a file
        """
        # Clear the features first
        self.setAll(False)
        if not os.path.exists(filePath):
            return self
        if not os.access(filePath, os.R_OK):
            return self
        slots = set(self.__slots__)
        f = file(filePath)
        for line in f:
            line = line.strip()
            if line in slots:
                setattr(self, line, True)
        return self

class UpdateJob:
    trvCsDir = 'trove-changesets'

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
        if self.closeDatabase:
            # Close db too
            if self.troveSource.db:
                self.troveSource.db.close()
                self.troveSource.db = None
            if hasattr(self.searchSource, 'db') and self.searchSource.db:
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

    @api.publicApi
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

    @api.publicApi
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

    @api.publicApi
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

    @api.publicApi
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
        drep['jobPreScriptsOrder'] = self._freezeJobPreScriptsOrder()
        drep['jobPreScriptsAlreadyRun'] = list(
            (x[0], self._freezeJob(x[1]))
                for x in self._jobPreScriptsAlreadyRun)
        drep['jobPostRBScripts'] = list(self._freezeJobPostRollbackScripts())
        drep['changesetsDownloaded'] = int(self._changesetsDownloaded)

        jobfile = os.path.join(frzdir, "jobfile")

        self._saveFrozenRepr(jobfile, drep)
        self.saveTroveMap(util.joinPaths(frzdir, self.trvCsDir))

        # Save features too. We will not load them automatically unless we
        # find a need for that.
        self.saveFeatures(os.path.join(frzdir, "features"))

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

    @api.publicApi
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
        self._jobPreScriptsByJob = self._thawJobPreScriptsOrder(
            drep.get('jobPreScriptsOrder', None))
        self._jobPostRBScripts = list(self._thawJobPostRollbackScripts(
            list(drep.get('jobPostRBScripts', []))))
        self._jobPreScriptsAlreadyRun = set(
            (x[0], self._thawJob(x[1]))
                for x in drep.get('jobPreScriptsAlreadyRun', []))
        self._changesetsDownloaded = bool(drep.get('changesetsDownloaded', 0))
        self._fromChangesets = self._thawFromChangesets(drep.get('fromChangesets', []))

        self.loadTroveMap(util.joinPaths(frzdir, self.trvCsDir))

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
        if isinstance(flavor, str):
            return deps.ThawFlavor(flavor)
        return flavor

    def _freezeJobPreScriptsOrder(self):
        if self._jobPreScriptsByJob is None:
            return []
        # Not much we have to do here, the data structures are marshallable
        return self._jobPreScriptsByJob

    def _thawJobPreScriptsOrder(self, drep):
        if not drep:
            return None

        ret = []
        # We only know about 3 action types
        for actionList in drep[:3]:
            al = []
            ret.append(al)
            for item in actionList:
                if len(item) >= 2:
                    al.append(tuple(item[:2]))
        return ret

    def _freezeJobPreScripts(self):
        # Freeze the job and the string together
        for item in self._jobPreScripts:
            yield (self._freezeJob(item[0]), item[1], item[2], item[3], item[4])

    def _thawJobPreScripts(self, frzrepr):
        # We might be thawing an update job generated by an older Conary, that
        # did not have support for preinstall scripts.
        for item in frzrepr:
            if len(item) == 4:
                action = "preupdate"
            else:
                action = item[4]
            yield (self._thawJob(item[0]), item[1], item[2], item[3], action)

    def _freezeJobPostRollbackScripts(self):
        for job, script, oldCompCls, newCompCls in self._jobPostRBScripts:
            yield (self._freezeJob(job), script, oldCompCls, newCompCls)

    def _thawJobPostRollbackScripts(self, frzrepr):
        for item in frzrepr:
            job, script, oldCompCls, newCompCls = item[:4]
            yield (self._thawJob(job), script, oldCompCls, newCompCls)

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

    def saveTroveMap(self, destdir):
        troveMap = self._troveMap
        util.mkdirChain(destdir)
        for i, (nvf, trv) in enumerate(troveMap.items()):
            destFile = util.joinPaths(destdir, '%s.ccs' % i)
            cs = changeset.ChangeSet()
            trvCs = trv.diff(None)[0]
            cs.newTrove(trvCs)
            cs.writeToFile(destFile)

    def loadTroveMap(self, destdir):
        ret = self._troveMap = {}
        if not destdir or not os.path.exists(destdir):
            return ret
        files = os.listdir(destdir)
        for f in files:
            if not f.endswith('.ccs'):
                continue
            fileName = util.joinPaths(destdir, f)
            cs = changeset.ChangeSetFromFile(
                util.ExtendedFile(fileName, buffering = False))
            trvCs = cs.iterNewTroveList().next()
            # We're only going to use the troves in troveMap for finding
            # references, so it's acceptable to skip integrity checks
            trv = trove.Trove(trvCs, skipIntegrityChecks = True)
            ret[trv.getNameVersionFlavor()] = trv
        return ret

    def setInvalidateRollbacksFlag(self, flag):
        self._invalidateRollbackStack = bool(flag)

    @api.publicApi
    def updateInvalidatesRollbacks(self):
        """
        @return: Does applying this update invalidate the rollback stack?
        @rtype: bool
        """
        return self._invalidateRollbackStack

    def addJobPreScript(self, job, script, oldCompatClass, newCompatClass,
                        action = None, troveObj = None):
        assert troveObj is not None
        if action is None:
            action = "preupdate"
        nvf = troveObj.getNameVersionFlavor()
        if nvf not in self._troveMap:
            self._troveMap[nvf] = troveObj
        self._jobPreScripts.append((job, script, oldCompatClass,
                                    newCompatClass, action))

    def iterJobPreScripts(self):
            for i in self._jobPreScripts:
                yield i

    def hasJobPreScriptsOrder(self):
        return self._jobPreScripts is not None

    def iterJobPreScriptsForJobSet(self, jobSetIdx):
        jl = self._jobPreScriptsByJob
        if jl is None:
            raise StopIteration
        actionsInOrder = [ 'prerollback', 'preinstall', 'preupdate', 'preerase' ]
        for action, lAction in zip(actionsInOrder, jl):
            for jidx, lScriptIdx in lAction:
                if jidx > jobSetIdx:
                    break
                if jidx < jobSetIdx:
                    continue
                for scriptIdx in lScriptIdx:
                    scriptData = self._jobPreScripts[scriptIdx]
                    assert(scriptData[-1] == action)
                    yield self._jobPreScripts[scriptIdx]

    def addJobPostRollbackScript(self, job, script, oldCompatCls, newCompatCls):
        self._jobPostRBScripts.append((job, script, oldCompatCls, newCompatCls))

    def iterJobPostRollbackScripts(self):
        return iter(self._jobPostRBScripts)

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

    def _getJobOrder(self):
        # Return a map from job names to job rank in the list ordered by the
        # dependency resolution
        return dict((self._normalizeJob(x), i)
                    for i, x in enumerate(itertools.chain(*self.jobs)))

    def _normalizeJob(self, job):
        if job[1][0] is None:
            # Old version is None, make None the flavor too
            return (job[0], (None, None), job[2], job[3])
        if job[2][0] is None:
            # New version is None, make None the flavor too
            return (job[0], job[1], (None, None), job[3])
        return job

    def _orderScriptListByBucket(self, scriptList, buckets):
        jobOrderHash = self._getJobOrder()

        # Sort scripts in some consistent manner
        # sorted by job within each group
        bucketLists = []
        for bucketName in buckets:
            al = [ x for x in scriptList if x[-1] == bucketName ]
            al.sort(key = lambda x: jobOrderHash[self._normalizeJob(x[0])])
            bucketLists.append(al)
        return bucketLists

    def orderScriptListByBucket(self, scriptList, buckets):
        bucketLists = self._orderScriptListByBucket(scriptList, buckets)
        return list(itertools.chain(*bucketLists))

    def reorderPreScripts(self, criticalUpdateInfo):
        # First, order scripts the way the old clients used to do it
        # This way, if an old client thaws a frozen update job generated by
        # a newer conary, behavior doesn't change.
        jobPreScripts = self._jobPreScripts
        actionsInOrder = [ 'prerollback', 'preinstall', 'preupdate', 'preerase' ]
        if criticalUpdateInfo and criticalUpdateInfo.criticalOnly:
            # We need to filter the scripts to only the ones in the critical
            # set
            jobOrderHash = self._getJobOrder()
            jobPreScripts = [ x for x in jobPreScripts
                              if x[0] in jobOrderHash ]
        self._jobPreScripts = self.orderScriptListByBucket(jobPreScripts,
                                                           actionsInOrder)
        # Now generate references into self._jobPreScripts
        jobPreScripts = self._jobPreScripts

        # Create the job set mappings (trove NVF to job set index in
        # self.jobs)
        mapErase = {}
        mapNonErase = {}
        for i, job in enumerate(self.jobs):
            for cSpec in job:
                # cSpec is (name, (oldV, oldF), (newV, newF), absolute)
                cSpec = self._normalizeJob(cSpec)
                if cSpec[2][0] is None:
                    # This is an erase
                    mapErase[(cSpec[0], cSpec[1][0], cSpec[1][1])] = i
                    continue
                mapNonErase[(cSpec[0], cSpec[2][0], cSpec[2][1])] = i

        scriptIdxMap = dict(prerollback=dict(), preerase=dict(),
                            preinstall=dict(), preupdate=dict())

        # Normally, we will not try to hit the repository again; we'll use the
        # job's _troveMap to minimize that. However, if we're part of a
        # critical update and the old Conary did not serialize _troveMap, we
        # will have to.

        getTroveFromSearchSource = self.getSearchSource().getTrove
        getTroveFromLocalDb = self.getTroveSource().db.getTrove
        # Iterate over the scripts
        # We try to pick the smallest job possible that still contains this
        # trove's subtroves
        for scriptIdx, scriptTup in enumerate(jobPreScripts):
            job = self._normalizeJob(scriptTup[0])
            action = scriptTup[4]
            if action == 'preerase':
                searchMap = mapErase
                trvSpec = (job[0], ) + job[1]
                getTrove = getTroveFromLocalDb
            else:
                searchMap = mapNonErase
                trvSpec = (job[0], ) + job[2]
                getTrove = getTroveFromSearchSource
            minIdx = searchMap[trvSpec]

            # Grab the trove
            trv = self._troveMap.get(trvSpec)
            if trv is None:
                trv = getTrove(*trvSpec, **dict(withFiles=False))
            # Iterate over its troves
            for subTrv in trv.iterTroveList(strongRefs = True, weakRefs = True):
                if subTrv not in searchMap:
                    continue
                # Get the smallest job set index
                minIdx = min(minIdx, searchMap.get(subTrv, minIdx))

            scriptIdxMap[action][job] = (job, scriptIdx, minIdx)

        # Make the resulting data structure slightly easier to store
        # A tuple (prerollback, preinstall, preupdate, preerase),
        # each of those being:
        #  a list of tuples (index, list of script spec indices)
        # a script spec index being an index into self._jobPreScripts
        ret = self._jobPreScriptsByJob = []
        for action in actionsInOrder:
            s_ = scriptIdxMap[action]
            scripts = s_.values()
            # group by index value in self.jobs
            groupBy = sorted(set(x[2] for x in s_.values()))
            # Order the scripts, drop the job and minIdx
            ordered = self._orderScriptListByBucket(scripts, groupBy)
            ordered = [ [y[1] for y in x] for x in ordered ]
            ret.append(zip(groupBy, ordered))

    def setPreviousVersion(self, version):
        self._previousVersion = version

    def getPreviousVersion(self):
        return self._previousVersion

    def saveFeatures(self, filePath):
        self._features.saveToFile(filePath)

    def loadFeatures(self, filePath):
        self._features.loadFromFile(filePath)

    def getFeatures(self):
        return self._features

    def recordPreScriptRun(self, action, job):
        self._jobPreScriptsAlreadyRun.add((action, job))

    def wasPreScriptRun(self, action, job):
        return (action, job) in self._jobPreScriptsAlreadyRun

    def setJobPreScriptsAlreadyRun(self, iterable):
        jobs = [ (x[0], self._thawJob(x[1])) for x in iterable ]
        self._jobPreScriptsAlreadyRun = set(jobs)

    def iterJobPreScriptsAlreadyRun(self):
        return iter(self._jobPreScriptsAlreadyRun)

    def __init__(self, db, searchSource = None, lazyCache = None,
                 closeDatabase = True):
        # 20070714: lazyCache can be None for the users of the old API (when
        # an update job was instantiated directly, instead of using the
        # client's newUpdateJob(). At some point we should deprecate that.
        if lazyCache is None:
            lazyCache = util.LazyFileCache()
        self.lzCache = lazyCache
        self.jobs = []
        self.pinMapping = set()
        self.rollback = None
        self.closeDatabase = closeDatabase
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
        self._jobPreScriptsByJob = None
        # We already ran pre scripts for these jobs
        self._jobPreScriptsAlreadyRun = set()
        # Trove map, needed to walk the references when ordering pre scripts
        self._troveMap = {}
        # Map of postrollback scripts (keyed on trove NVF)
        self._jobPostRBScripts = []
        # Changesets have been downloaded
        self._changesetsDownloaded = False
        # This flag gets set if the update job was loaded from the restart
        # information
        self._restartedFlag = False
        # Previous version of Conary, if the update job was loaded from the
        # restart information
        self._previousVersion = None
        # Features of the previous Conary
        self._features = UpdateJobFeatures()

        self._commitFlags = None

class DepCheckState:

    def __init__(self, db, troveSource, findOrdering = True,
                 ignoreDepClasses = []):
        """
        @param troveSource: Trove source troves in the job are
                            available from
        @type troveSource: AbstractTroveSource:
        @param findOrdering: If true, a reordering of the job is
                             returned which preserves dependency
                             closure at each step.
        @type findOrdering: boolean
        @param ignoreDepClasses: List of dependency classes which should
        not be enforced.
        @type ignoreDepClasses: list of deps.Depenendency
        """

        self.setTroveSource(troveSource)
        self.db = db
        self.ignoreDepClasses = ignoreDepClasses
        self.jobSet = set()
        self.checker = None
        self.findOrdering = findOrdering

    def setTroveSource(self, troveSource):
        self.troveSource = troveSource

    def done(self):
        if self.checker is not None:
            self.checker.done()
            self.checker = None
            self.jobSet = set()

    def __del__(self):
        self.done()

    def setup(self):
        if self.checker is None:
            self.checker = self.db.dependencyChecker(self.troveSource,
                                    findOrdering = self.findOrdering,
                                    ignoreDepClasses = self.ignoreDepClasses)

    def setJobs(self, newJobSet):
        newJobSet = set(newJobSet)
        removedJobs = self.jobSet - newJobSet
        if removedJobs:
            self.done()
            addedJobs = newJobSet
        else:
            addedJobs = newJobSet - self.jobSet

        self.setup()

        self.checker.addJobs(addedJobs)
        self.jobSet = newJobSet

    def depCheck(self, jobSet,
                 linkedJobs = None, criticalJobs = None,
                 finalJobs = None, criticalOnly = False):
        """
        Check the database for closure against the operations in
        the passed changeSet.

        @param jobSet: The jobs which define the dependency check
        @type jobSet: set
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

        self.setup()
        #print "--- adding jobs"
        self.setJobs(jobSet)
        #print "--- checking deps"
        result = self.checker.check(linkedJobs = linkedJobs,
                                    criticalJobs = criticalJobs,
                                    finalJobs = finalJobs)
        #print "--- done"

        if criticalOnly and result.getCriticalUpdates():
            changeSetList = result.getChangeSetList()
            criticalUpdates = result.getCriticalUpdates()
            changeSetList = changeSetList[:criticalUpdates[0] + 1]
            jobSet.clear()
            jobSet.update(itertools.chain(*changeSetList))
            if (criticalUpdates and
                        (result.unresolveableList or result.unsatisfiedList)):
                # we're trying to apply only critical updates, but
                # there's a dep failure somewhere in the entire job.
                # Try again to resolve dependencies, using only
                # the critical changes
                self.setJobs(jobSet)
                result = self.checker.check(linkedJobs = linkedJobs)

        return result

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

    def getTroveReferences(self, l, weakRefs = False, justPresent = False):
        return self.db.getTroveReferences(l, weakRefs = weakRefs,
                                          justPresent = justPresent)

    def findTroveContainers(self, names):
        return self.db.findTroveContainers(names)

    def troveIsIncomplete(self, name, version, flavor):
        return self.db.troveIsIncomplete(name, version, flavor)

    def findTroveReferences(self, names):
        """ Return references to a trove on the system, whether or not
            that trove is actually installed
        """
        return self.db.findTroveReferences(names)

    @api.publicApi
    def getTrove(self, name, version, flavor, pristine = True,
                 withFiles = True, withDeps = True,
                 withFileObjects = False):
        """
        @raises TroveMissing:
        @note:
            As this calls database functions, it could also raise any type of
        DatabaseError defined in L{dbstore.sqlerrors}
        """
        l = self.getTroves([ (name, version, flavor) ], pristine = pristine,
                           withDeps = withDeps, withFiles = withFiles,
                           withFileObjects = withFileObjects)
        if l[0] is None:
            raise errors.TroveMissing(name, version)

        return l[0]

    def getTroves(self, troveList, pristine = True, withFiles = True,
                  withDeps = True, callback = None, withFileObjects = False):
        if not troveList:
            return []
        return self.db.getTroves(troveList, pristine, withFiles = withFiles,
                                 withDeps = withDeps,
                                 withFileObjects = withFileObjects)

    def iterTroves(self, *args, **kwargs):
        # hidden is for compatibility with the repository call
        kwargs.pop('hidden', None)
        for x in self.getTroves(*args, **kwargs):
            yield x

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
                         pristine = False, capsules = False):
        for x in self.db.iterFilesInTrove(troveName, version, flavor,
                                          sortByPath = sortByPath,
                                          withFiles = withFiles,
                                          pristine = pristine):
            if capsules and x[0] == trove.CAPSULE_PATHID:
                yield x
            elif not capsules and x[0] != trove.CAPSULE_PATHID:
                yield x

    def iterFilesWithTag(self, tag):
	return self.db.iterFilesWithTag(tag)

    def addFileVersion(self, troveId, pathId, path, fileId, version,
                       fileStream = None, isPresent = True):
        self._updateTransactionCounter = True
	self.db.addFile(troveId, pathId, path, fileId, version,
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
            for troveInfo in self.walkTroveSet(trove, withFiles = False):
                self.db.pinTroves(pin = pin, *troveInfo)

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
        if self._db:
            self.db.close()
            self._db = None

        # No locks associated with an in-memory database
        if self.dbpath == ':memory:':
            return
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
                         sortByPath = False, withFiles = False,
                         capsules = True):
	return SqlDbRepository.iterFilesInTrove(self, troveName, version,
			flavor, sortByPath = sortByPath,
			withFiles = withFiles, pristine = False,
                        capsules = capsules)

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

    def getDepStateClass(self, troveSource, findOrdering = True,
                         ignoreDepClasses = set() ):
        """
        Return dependency state class which can be used for dependency checks
        against this repository. For parameter list and return
        value see DepCheckState class.
        """
        # the set() here makes sure we pass sets down even if we get lists
        # in
        return DepCheckState(self.db, troveSource,
                             findOrdering = findOrdering,
                             ignoreDepClasses = set(ignoreDepClasses))

    def getFileContents(self, l):
        # look for config files in the datastore first, then look for other
        # files in the filesystem
        inDataStore = SqlDbRepository.getFileContents(self, l)
        for i, contents in enumerate(inDataStore):
            if contents is not None: continue

            (fileId, fileVersion) = l[i][0:2]
            try:
                path, stream = self.db.troveFiles.getFileByFileId(fileId,
                                                        justPresent = True)
            except KeyError:
                # can't find the file in the database at all
                continue

            fileObj = files.ThawFile(stream, None)
            if not fileObj.hasContents:
                continue

            try:
                sha = sha1helper.sha1FileBin(self.root + path)
            except IOError:
                continue

            if fileObj.contents.sha1() == sha:
                inDataStore[i] = filecontents.FromFilesystem(self.root + path)

        return inDataStore

    def _doCommit(self, uJob, cs, commitFlags, opJournal, tagSet,
                  reposRollback, localRollback, rollbackPhase, fsJob,
                  updateDatabase, callback, tagScript, dbCache,
                  autoPinList, flags, journal, directoryCandidates,
                  storeRollback = True):
        if not (commitFlags.justDatabase or commitFlags.test):
            # run preremove scripts before updating the database, otherwise
            # the file lists which get sent to them are incorrect. skipping
            # this makes --test a little inaccurate, but life goes on
            callback.runningPreTagHandlers()
            fsJob.preapply(tagSet, tagScript)

        dbConflicts = []

        localChanges = None
        for trvCs in cs.iterNewTroveList():
            if (trvCs.getNewVersion().onLocalLabel() or
                trvCs.getNewVersion().onRollbackLabel() or
                trvCs.getOldVersion() and (
                    trvCs.getOldVersion().onLocalLabel() or
                    trvCs.getOldVersion().onRollbackLabel()
                )):
                assert(localChanges or localChanges is None)
                localChanges = True
            else:
                localChanges = False

        if rollbackPhase is None:
            # this is the rollback for files which the user is forcing the
            # removal of (probably due to removeFiles)
            self.mergeRemoveRollback(localRollback,
                         self.createRemoveRollback(fsJob.iterUserRemovals()))

        # Build A->B
        if (updateDatabase and not localChanges):
            # this updates the database from the changeset; the change
            # isn't committed until the self.commit below
            # an object for historical reasons
            try:
                csJob = localrep.LocalRepositoryChangeSetJob(
                    dbCache, cs, callback, autoPinList,
                    allowIncomplete = (rollbackPhase is not None),
                    userReplaced = fsJob.userRemovals,
                    replaceFiles = flags.replaceManagedFiles,
                    sharedFiles = fsJob.sharedFilesByTrove)
            except DatabasePathConflicts, e:
                for (path, (pathId, (troveName, version, flavor)),
                           newTroveInfo) in e.getConflicts():
                    dbConflicts.append(DatabasePathConflictError(
                            util.joinPaths(self.root, path), 
                            troveName, version, flavor))
                csJob = None

            self.db.mapPinnedTroves(uJob.getPinMaps())
        elif updateDatabase and localChanges:
            # We're applying the local part of changeset. Files which are newly
            # added by local changesets need to be recorded in the database as
            # being present (since they were previously erased)
            localrep.markChangedFiles(self.db, cs)
            csJob = None
        else:
            csJob = None

        fsJob.filterRemoves()

        if rollbackPhase is None and csJob:
            # this is the rollback for file conflicts which are in the
            # database only; the files may be missing in the filesystem
            # altogether
            self.mergeRemoveRollback(localRollback,
                         self.createRemoveRollback(csJob.iterDbRemovals(),
                                                   asMissing = True))

        # we have to do this before files get removed from the database,
        # which is a bit unfortunate since this rollback isn't actually
        # valid until a bit later, but that's why we jounral
        if (rollbackPhase is None) and not commitFlags.test and storeRollback:
            rollback = uJob.getRollback()
            rollbackScripts = None
            if rollback is None:
                rollback = self.rollbackStack.new(opJournal)
                uJob.setRollback(rollback)
                # Only save the rollback scripts once, and only if the job was
                # not restarted or the previous Conary didn't know how to save
                # scripts on the rollback stack (CNY-2845)
                prflag = uJob.getFeatures().postRollbackScriptsOnRollbackStack
                if not uJob.getRestartedFlag() or not prflag:
                    rollbackScripts = list(uJob.iterJobPostRollbackScripts())
            rollback.add(opJournal, reposRollback, localRollback,
                rollbackScripts)
            del rollback

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

        # find which directories we should try to remove right now this has to
        # be done after the sqldb has been updated (but before the changes are
        # committted). We let the journal commit remove the actual directories
        # because the directories could have backup files in them that the
        # journal will clear out.
        if not commitFlags.justDatabase:
            lst = directoryCandidates.keys()
            lst.sort()
            keep = {}
            for relativePath in lst:
                if keep.has_key(relativePath):
                    keep[os.path.dirname(relativePath)] = True
                    continue

                if self.db.pathIsOwned(relativePath):
                    keep[os.path.dirname(relativePath)] = True
                    continue

                opJournal.tryCleanupDir(self.root + relativePath)

        fsJob.apply(journal, opJournal = opJournal,
                    justDatabase = commitFlags.justDatabase)

        if (updateDatabase and not localChanges):
            for (name, version, flavor) in fsJob.getOldTroveList():
                # if to database if false, we're restoring the local
                # branch of a rollback
                self.db.eraseTrove(name, version, flavor)

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

        if csJob:
            return csJob.invalidateRollbacks()
        else:
            return False

    def createRemoveRollback(self, removalList, asMissing = False):
        """
        Returns a changeset which undoes the user removals.

        @param removalList: Dict specifying files which have been removed
        from troves. It is indexed by (name, version, flavor) tuples, and
        is a list of (pathId, content, fileObj) tuples. If content/fileObj
        are None, the file information is not placed into the changeset.
        @type removalList: dict
        @param asMissing: If True, files are placed in the changeset with the
        original file object, but as files.MissingFiles instead of a normal
        file type. This is used for removals which need to be repaired in the
        database, but where there is no filesystem information to restore. If
        this is used it is assumed that the content/fileObj elements of the
        file lists are both None.
        @type noteMissing: bool
        @rtype changeset.ChangeSet
        """
        cs = changeset.ChangeSet()

        for (info, fileList) in removalList:
            if (not asMissing and
                    not [ x for x in fileList if x[1] is not None ]):
                # skip the rest of this processing if there are no files
                # to handle (it's likely that the trove referred to here
                # isn't in the database yet because it's being installed
                # as part of the same job)
                continue

            localTrove = self.db.getTroves([ info ])[0]
            origTrove = localTrove.copy()
            localTrove.changeVersion(
                localTrove.getVersion().createShadow(
                                            label = versions.LocalLabel()))
            hasChanges = False
            for (pathId, content, fileObj) in fileList:
                if asMissing:
                    hasChanges = True
                    fileObj = files.MissingFile(pathId)
                elif not content:
                    continue

                fileId = fileObj.fileId()
                cs.addFile(None, fileId, fileObj.freeze())

                if fileObj.hasContents:
                    # this file is seen as *added* in the rollback
                    cs.addFileContents(pathId, fileId,
                       changeset.ChangedFileTypes.file, content,
                       fileObj.flags.isConfig())

                # this makes the file show up as added instead of changed,
                # which is easier for us here and makes no difference later
                # on since this is only the local piece of a change set
                origTrove.removeFile(pathId)
                localTrove.updateFile(pathId, None, localTrove.getVersion(),
                                      fileId)
                hasChanges = True

            if not hasChanges: continue

            # contents for this aren't in a capsule
            localTrove.troveInfo.capsule.reset()
            # this is a rollback so the diff is backwards
            trvCs = localTrove.diff(origTrove)[0]
            cs.newTrove(trvCs)

        return cs

    def mergeRemoveRollback(self, localRollback, removeRollback):
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

            changedList = localCs.getChangedFileList(raw = True)
            l = [ x for x in changedList if x[0] not in pathIdList ]
            del changedList[:]
            changedList.extend(l)

        localRollback.merge(removeRollback)

    # local changes includes the A->A.local portion of a rollback; if it
    # doesn't exist we need to compute that and save a rollback for this
    # transaction
    def commitChangeSet(self, cs, uJob,
                        rollbackPhase = None, updateDatabase = True,
                        tagScript = None,
			journal = None,
                        callback = UpdateCallback(),
                        removeHints = {}, autoPinList = RegularExpressionList(),
                        deferredScripts = None, commitFlags = None,
                        repair = False):
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

        if repair:
            flags.ignoreMissingFiles = True

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
                if old.onLocalLabel():
                    old = newTrove.getNewVersion()
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
            reposRollback = cs.makeRollback(dbCache,
                       redirectionRollbacks = (not commitFlags.localRollbacks))
            flags.merge = True
        else:
            reposRollback = None

        fsJob = update.FilesystemJob(dbCache, cs, fsTroveDict, self.root,
                                     flags = flags, callback = callback,
                                     removeHints = removeHints,
                                     rollbackPhase = rollbackPhase,
                                     deferredScripts = deferredScripts)

	# look through the directories which have had files removed and
	# see if we can remove the directories as well
        dirSet = fsJob.getDirectoryCountSet()
        lst = dirSet.keys()
	lst.sort()
	lst.reverse()
	directoryCandidates = {}
	while (lst):
	    path = lst[0]
	    del lst[0]
            try:
                entries = len(os.listdir(self.root + '/' + path))
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
		lst.append(parent)
		# insertion is linear, sort is n log n
		# oh well.
		lst.sort()
		lst.reverse()

	# -------- database and system are updated below this line ---------

        if self.opJournalPath:
            opJournal = JobJournal(self.opJournalPath, self.root, create = True,
                                   callback = callback)
        else:
            opJournal = NoopJobJournal()

        # Gross, but we need to protect against signals for this call.
        @sigprotect.sigprotect()
        def signalProtectedCommit():
            try:
                invalidateRollbacks = self._doCommit(uJob, cs, commitFlags,
                            opJournal, tagSet, reposRollback, localRollback,
                            rollbackPhase, fsJob, updateDatabase, callback,
                            tagScript, dbCache, autoPinList, flags, journal,
                            directoryCandidates, storeRollback = not repair)
            except Exception, e:
                if not issubclass(e.__class__, ConaryError):
                    callback.error("a critical error occured -- reverting "
                                   "filesystem changes")

                opJournal.revert()

                if not commitFlags.keepJournal:
                    opJournal.removeJournal()

                raise

            log.debug("committing journal")
            opJournal.commit()
            if not commitFlags.keepJournal:
                opJournal.removeJournal()

            return invalidateRollbacks

        invalidateRollbacks = signalProtectedCommit()

        #del opJournal

        if not (commitFlags.justDatabase or commitFlags.test):
            fsJob.runPostTagScripts(tagSet, tagScript)

        if rollbackPhase is None and updateDatabase and invalidateRollbacks:
            self.rollbackStack.invalidate()

        if rollbackPhase is not None:
            return fsJob

        if not commitFlags.justDatabase:
            fsJob.orderPostScripts(uJob)
            fsJob.runPostScripts(tagScript)

    def runPreScripts(self, uJob, callback, tagScript = None,
                      isRollback = False, justDatabase = False,
                      tmpDir = '/', jobIdx = None):
        if justDatabase:
           return True

        if jobIdx is not None:
            actionLists = [ uJob.iterJobPreScriptsForJobSet(jobIdx) ]
        else:
            actionLists = [ uJob.iterJobPreScripts() ]

        for (job, script, oldCompatClass, newCompatClass, action) in \
                    itertools.chain(*actionLists):

            if isRollback and action != 'preerase':
                continue
            if uJob.wasPreScriptRun(action, job):
                continue
            scriptId = "%s %s" % (job[0], action)
            rc = update.runTroveScript(job, script, tagScript, tmpDir,
                                       self.root, callback, isPre = True,
                                       scriptId = scriptId,
                                       oldCompatClass = oldCompatClass,
                                       newCompatClass = newCompatClass)
            uJob.recordPreScriptRun(action, job)
            if rc:
                return False

        return True

    def removeFiles(self, pathList):

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

        removeCs = changeset.ChangeSet()

        for trvInfo, pathList in pathsByTrove.iteritems():
            trv = troves[trvInfo]

            newTrv = trv.copy()
            newTrv.changeVersion(
                        trv.getVersion().createShadow(versions.LocalLabel()))

            for path in pathList:
                fileList = [ (x[0], x[2], x[3]) for x in trv.iterFileList() 
                                                    if x[1] in path ]
                assert(len(fileList) == 1)
                pathId, fileId, fileVersion = fileList[0]
                newTrv.removeFile(pathId)

            newTrv.computeDigests()
            removeCs.newTrove(newTrv.diff(trv)[0])

        uJob = UpdateJob(self)
        self.commitChangeSet(removeCs, uJob, callback = UpdateCallback())

    def _expandCollections(self, troveInfoList):
        troveSet = set()
        q = util.IterableQueue()
        for troveInfo in itertools.chain(sorted(troveInfoList), q):
            if troveInfo in troveSet:
                continue

            troveSet.add(troveInfo)

            if trove.troveIsCollection(troveInfo[0]):
                for subTroveInfo in \
                      itertools.chain(*self.db.getTroveReferences([troveInfo])):
                    q.add(subTroveInfo)

        return sorted(list(troveSet))

    def _getFiles(self, repos, cs, filesChanged):
        filesNeeded = []
        for (pathId, oldFileId, oldFileVersion, newFileId, newVersion) \
                                        in filesChanged:
            assert(oldFileId is None)
            filesNeeded.append((pathId, newFileId, newVersion))

        fileObjs = repos.getFileVersions(filesNeeded)
        contentsNeeded = []
        for (pathId, newFileId, newVersion), fileObj in \
                                    itertools.izip(filesNeeded, fileObjs):
            cs.addFile(None, newFileId, fileObj.freeze())
            if fileObj.hasContents:
                contentsNeeded.append((pathId, fileObj, newFileId,
                                       newVersion, fileObj.contents.sha1()))

        contents = repos.getFileContents([ x[2:] for x in contentsNeeded ],
                                         compressed = True)

        for (pathId, fileObj, newFileId, newVersion, sha1), contentObj in \
                        itertools.izip(contentsNeeded, contents):
            cs.addFileContents(pathId, newFileId,
                               changeset.ChangedFileTypes.file,
                               contentObj, fileObj.flags.isConfig(),
                               compressed = True)

    def restoreTroves(self, repos, troveInfoList):
        fullTroveInfoList = self._expandCollections(troveInfoList)
        pristineTroves = self.db.getTroves(fullTroveInfoList, pristine = True)
        localTroves = self.db.getTroves(fullTroveInfoList, pristine = False)

        restoreCs = changeset.ChangeSet()
        filesChanged = []
        for pristineTrv, localTrv in itertools.izip(pristineTroves,
                                                    localTroves):
            if pristineTrv is None:
                # this version isn't installed. that's okay.
                continue

            pristineTrv.changeVersion(
                pristineTrv.getVersion().createShadow(versions.LocalLabel()))

            pristineTrv.computeDigests()
            trvCs, thisFilesChanged = pristineTrv.diff(localTrv)[0:2]
            restoreCs.newTrove(trvCs)
            filesChanged += thisFilesChanged

        self._getFiles(repos, restoreCs, filesChanged)

        uJob = UpdateJob(self)
        self.commitChangeSet(restoreCs, uJob, callback = UpdateCallback())

    def repairTroves(self, repos, origTroveInfoList):
        # we don't care about collections here
        troveInfoList = [ x for x in self._expandCollections(origTroveInfoList)
                             if not trove.troveIsCollection(x[0]) ]
        pristineTroves = self.db.getTroves(troveInfoList, pristine = True)
        localTroves = self.db.getTroves(troveInfoList, pristine = False)

        troveList = []
        flags = update.UpdateFlags(ignoreMissingFiles = True)
        for (localTrv, pristineTrv) in itertools.izip(localTroves,
                                                      pristineTroves):
            if localTrv is None:
                # it's okay to have some bits not installed
                continue

            localVer = pristineTrv.getVersion().createShadow(
                                                        versions.LocalLabel())
            troveList.append( (localTrv, pristineTrv, localVer, flags) )

        cs, changedTroveList = update.buildLocalChanges(self, troveList,
                                        root = self.root)
        for (changed, trv) in changedTroveList:
            if not changed:
                cs.delNewTrove(*trv.getNameVersionFlavor())

        repairCs = cs.makeRollback(self, redirectionRollbacks = False,
                                   repos = repos)

        uJob = UpdateJob(self)
        self.commitChangeSet(repairCs, uJob, callback = UpdateCallback(),
                             repair = True)

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

    def getRollbackStack(self):
        return self.rollbackStack

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

    def getRollbackList(self):
        self._ensureReadableRollbackStack()
	lst = []
	for i in range(self.firstRollback, self.lastRollback + 1):
	    lst.append("r.%d" % i)

	return lst

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
        self.readRollbackStatus()
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

    def removeInvalidRollbacks(self):
        self.readRollbackStatus()
        dirEntries = os.listdir(self.rollbackCache)
        rollbacks = [ int(x) for x in dirEntries if x.isdigit() ]
        rollbacks.sort()
        for num in rollbacks:
            if num >= self.firstRollback:
                break

            shutil.rmtree(self.rollbackCache + '/' + "%d" % num)

    def applyRollbackList(self, *args, **kwargs):
        try:
            self.commitLock(True)
            return self._applyRollbackList(*args, **kwargs)
        finally:
            self.commitLock(False)
            self.close()

    def _applyRollbackList(self, repos, names, replaceFiles = False,
                          callback = UpdateCallback(), tagScript = None,
                          justDatabase = False, transactionCounter = None,
                          lazyCache = None):
        assert transactionCounter is not None, ("The transactionCounter "
            "argument is mandatory")
        if transactionCounter != self.getTransactionCounter():
            raise RollbackError(names, "Database state has changed, please "
                "run the rollback command again")

	last = self.rollbackStack.last
	for name in names:
	    if not self.rollbackStack.hasRollback(name):
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
            rb = self.rollbackStack.getRollback(name)
            totalCount += 0

            for i in xrange(rb.getCount()):
                (reposCs, localCs) = rb.getLast() 
                if not reposCs.isEmpty():
                    totalCount += 1
                if not localCs.isEmpty():
                    totalCount += 1

        itemCount = 0
        for i, name in enumerate(names):
	    rb = self.rollbackStack.getRollback(name)

            # we don't want the primary troves from reposCs to win, so get
            # rid of them (otherwise we're left with redirects!). primaries
            # don't really matter here anyway, so no reason to worry about
            # them
            (reposCs, localCs) = rb.getLast()
            reposCs.setPrimaryTroveList([])

            lastFsJob = None
            # Get the post-rollback scripts
            postRollbackScripts = rb.getLastPostRollbackScripts()
            while reposCs:
                # redirects in rollbacks mean we need to go get the real
                # changeset from a repository
                jobList = []
                for trvCs in reposCs.iterNewTroveList():
                    if not trvCs.getType() == trove.TROVE_TYPE_REDIRECT:
                        continue
                    jobList.append(trvCs.getJob())

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

                # Collect pre scripts
                updJob = UpdateJob(None, lazyCache = lazyCache)
                # As a side-effect, _getChangesetPreScripts will add the job
                # sets to the job, ordered alphabetically.
                preScripts = self._getChangesetPreScripts(reposCs, updJob)
                for x in preScripts:
                    updJob.addJobPreScript(*x)

                try:
                    fsJob = None
                    commitFlags = CommitChangeSetFlags(
                        replaceManagedFiles = replaceFiles,
                        replaceUnmanagedFiles = replaceFiles,
                        replaceModifiedFiles = replaceFiles,
                        justDatabase = justDatabase)

                    self.runPreScripts(updJob, callback = callback,
                                       tagScript = tagScript,
                                       isRollback = False,
                                       justDatabase = justDatabase)

                    fsUpdateJob = UpdateJob(None, lazyCache = lazyCache)
                    if not reposCs.isEmpty():
                        itemCount += 1
                        callback.setUpdateHunk(itemCount, totalCount)
                        callback.setUpdateJob(reposCs.getJobSet())
                        fsJob = self.commitChangeSet(
                                     reposCs, fsUpdateJob,
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
                        self.commitChangeSet(localCs,
                                     fsUpdateJob,
                                     rollbackPhase =
                                            update.ROLLBACK_PHASE_LOCAL,
                                     callback = callback,
                                     tagScript = tagScript,
                                     commitFlags = commitFlags)

                    if fsJob:
                        # We will use the last valid fsJob to run the
                        # post-rollback scripts
                        lastFsJob = fsJob
                        # Because of the two phase update for rollbacks, we
                        # run postscripts by hand instead of commitChangeSet
                        # doing it automatically
                        fsJob.orderPostScripts(updJob)
                        fsJob.runPostScripts(tagScript)
                    fsUpdateJob.close()

                    rb.removeLast()
                except CommitError, err:
                    updJob.close()
                    raise RollbackError(name, err)

                updJob.close()
                (reposCs, localCs) = rb.getLast()

            # Run post-rollback scripts at the very end of the rollback, when
            # all other operations have been performed
            if lastFsJob:
                if postRollbackScripts:
                    lastFsJob.clearPostScripts()
                    # Add the post-rollback scripts
                    for scriptData in postRollbackScripts:
                        lastFsJob.addPostRollbackScript(*scriptData)
                    lastFsJob.runPostScripts(tagScript)

            self.rollbackStack.removeLast()

    def _getChangesetPreScripts(self, cs, updJob):
        preScripts = []
        if cs.isEmpty():
            return preScripts

        jobs = []
        for trvCs in cs.iterNewTroveList():
            oldVersion = trvCs.getOldVersion()
            if oldVersion and oldVersion.onLocalLabel():
                # These were local changes, which have no scripts
                continue

            job = trvCs.getJob()
            jobs.append(job)
            newCompatClass = trvCs.getNewCompatibilityClass()
            if job[1][0] is None:
                # This is an install (rolling back an erase)
                script = trvCs._getPreInstallScript()
                if not script:
                    continue
                troveObj = trove.Trove(trvCs)
                preScripts.append((job, script, None, newCompatClass,
                                   "preinstall", troveObj))
                continue

            # This is an update
            # Get the old trove
            oldTrv = self.db.getTroves([(job[0], job[1][0], job[1][1])],
                withFiles=False, withDeps=False)[0]
            oldCompatClass = oldTrv.getCompatibilityClass()

            script = oldTrv.troveInfo.scripts.preRollback.script()
            if script:
                preScripts.append((job, script, oldCompatClass, newCompatClass,
                    "prerollback", oldTrv))

            script = trvCs.getPreUpdateScript()
            if script:
                if trvCs.isAbsolute():
                    troveObj = trove.Trove(trvCs)
                else:
                    troveObj = oldTrv.copy()
                    troveObj.applyChangeSet(trvCs)
                preScripts.append((job, script, oldCompatClass, newCompatClass,
                    "preupdate", troveObj))

        jobs.sort()
        updJob.addJob(jobs)

        jobs = []

        erasures = cs.getOldTroveList()
        if erasures:
            trvs = self.db.getTroves(erasures, withFiles=False, withDeps=False)
            for trv in trvs:
                trvCs = trv.diff(None)[0]
                j = trvCs.getJob()
                # Need to reverse the job
                j = (j[0], j[2], j[1], False)
                jobs.append(j)
                script = trvCs._getPreEraseScript()
                oldCompatClass = trv.getCompatibilityClass()
                if script:
                    preScripts.append((j, script, oldCompatClass, None,
                                      "preerase", trv))

                # This is the rollback of an install, we shall not run the
                # prerollback script (CNY-2844)

        jobs.sort()
        updJob.addJob(jobs)
        return preScripts

    def getTroveScripts(self, troveList):
        return self.db.getTroveScripts(troveList)

    def getPathHashesForTroveList(self, troveList):
        return self.db.getPathHashesForTroveList(troveList)

    def getCapsulesTroveList(self, troveList):
        return self.db.getCapsulesTroveList(troveList)

    def getTroveCompatibilityClass(self, name, version, flavor):
        return self.db.getTroveCompatibilityClass(name, version, flavor)

    def iterFindPathReferences(self, path, justPresent = False):
        return self.db.iterFindPathReferences(path, justPresent = justPresent)

    def pathsOwned(self, pathList):
        return self.db.pathsOwned(pathList)

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
            j = JobJournal(opJournalPath, root)
        except OSError, e:
            raise OpenError(top, 'journal error: ' + e.strerror)

        j.revert()
        os.unlink(opJournalPath)

    def _initDb(self):
        SqlDbRepository._initDb(self)
        if (self.opJournalPath and os.path.exists(self.opJournalPath) 
            and self.lockFile and not os.path.exists(self.lockFile)):
            raise ExistingJournalError(os.path.dirname(self.opJournalPath),
                    'journal file exists. use revert command to '
                    'undo the previous (failed) operation')

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
            self.opJournalPath = None
        else:
            conarydbPath = util.joinPaths(root, path) 
            SqlDbRepository.__init__(self, conarydbPath, timeout = timeout)
            self.opJournalPath = conarydbPath + '/journal'
            top = util.joinPaths(root, path)

            self.lockFile = top + "/syslock"
            self.lockFileObj = None
            self.rollbackCache = top + "/rollbacks"
            self.rollbackStatus = self.rollbackCache + "/status"
            try:
                self.rollbackStack = RollbackStack(self.rollbackCache)
            except OpenError, e:
                raise OpenError(top, e.msg)

class DatabaseCacheWrapper:

    def __getattr__(self, attr):
        return getattr(self.db, attr)

    def getTrove(self, name, version, flavor, pristine = True, *args, **kw):
        if version.onLocalLabel():
            # The local label is a handy fiction. It's the same as the
            # nonpristine parent.
            version = version.parentVersion()
            pristine = False

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
