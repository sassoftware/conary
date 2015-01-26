#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import itertools
import os
import sys
import tempfile
import weakref

from conary import errors
from conary import files
from conary import trove
from conary.lib import digestlib, util
from conary.local import journal
from conary.repository import changeset

class CapsuleOperation(object):

    def __init__(self, root, db, changeSet, callback, fsJob,
                 skipCapsuleOps = False):
        self.root = root
        self.db = db
        self.changeSet = changeSet
        self.fsJob = fsJob
        self.callback = callback
        self.errors = []
        self.skipCapsuleOps = skipCapsuleOps

    def apply(self, fileDict, justDatabase = False, noScripts = False):
        raise NotImplementedError

    def install(self, troveCs):
        raise NotImplementedError

    def remove(self, trove):
        raise NotImplementedError

    def getErrors(self):
        return self.errors

    def _error(self, e):
        self.errors.append(e)

class ConaryOwnedJournal(journal.JobJournal):

    # keep track of files which conary wants to own despite them being
    # in the underlying capsule; we back those up before the capsule
    # handler runs, and then restore them. this effectively takes ownership
    # of those files away from the underlying packaging tool

    def __init__(self, root = '/'):
        tmpfd, tmpname = tempfile.mkstemp()
        journal.JobJournal.__init__(self, tmpname, root = root, create = True)
        os.close(tmpfd)
        os.unlink(tmpname)

class SingleCapsuleOperation(CapsuleOperation):

    def __init__(self, *args, **kwargs):
        CapsuleOperation.__init__(self, *args, **kwargs)
        self.installs = []
        self.removes = []
        self.preserveSet = {}

    def _filesNeeded(self):
        return [ x[1] for x in self.installs ]

    def preservePath(self, path, unlink=True):
        self.preserveSet[path] = unlink

    def doApply(self, justDatabase = False, noScripts = False):
        raise NotImplementedError

    def apply(self, fileDict, justDatabase = False, noScripts = False):
        if not justDatabase and self.preserveSet:
            capsuleJournal = ConaryOwnedJournal(self.root)
            for path, unlink in self.preserveSet.iteritems():
                fullPath = self.root + path
                capsuleJournal.backup(fullPath, skipDirs = True)
                if unlink and not util.removeIfExists(fullPath):
                    capsuleJournal.create(fullPath)
        else:
            capsuleJournal = None

        try:
            self.doApply(fileDict, justDatabase = justDatabase, noScripts = noScripts)
        finally:
            if capsuleJournal:
                capsuleJournal.revert()

    def install(self, flags, troveCs):
        if troveCs.getOldVersion():
            oldTrv = self.db.getTrove(*troveCs.getOldNameVersionFlavor())
            trv = oldTrv.copy()
            trv.applyChangeSet(troveCs)
        else:
            oldTrv = None
            trv = trove.Trove(troveCs)

        #if oldTrv and oldTrv.troveInfo.capsule == trv.troveInfo.capsule:
            # the capsule hasn't changed, so don't reinstall it
            #return None

        for pathId, path, fileId, version in trv.iterFileList(capsules = True):
            # there should only be one...
            break

        assert(pathId == trove.CAPSULE_PATHID)

        if oldTrv:
            for oldPathId, oldPath, oldFileId, oldVersion in \
                            oldTrv.iterFileList(capsules = True):
                # there should only be one...
                break

            assert(oldPathId == trove.CAPSULE_PATHID)
            if (oldFileId == fileId or
                    oldTrv.troveInfo.capsule == trv.troveInfo.capsule):
                # good enough. this means changing capsule information
                # in trove info won't fool us into trying to reinstall
                # capsules which haven't changed. we check the capsule
                # information as well because derived packages change
                # the capsule fileIds. ugh.
                #
                # we do it in this order to make sure the test suite tests
                # both sides of the "or" above
                return

            self.remove(oldTrv)

        # is the capsule new or changed?
        changedFileInfos = [ x for x in troveCs.getChangedFileList()
                                if x[0] == trove.CAPSULE_PATHID ]
        if changedFileInfos:
            oldFileId = oldTrv.getFile(pathId)[1]
            oldFileObjs = self.db.getFileStream(oldFileId)
            fileObj = files.ThawFile(oldFileObjs, pathId)
            fileChange = self.changeSet.getFileChange(oldFileId, fileId)
            fileObj.twm(fileChange, fileObj)
            sha1 = fileObj.contents.sha1()
        else:
            fileStream = self.changeSet.getFileChange(None, fileId)
            sha1 = files.frozenFileContentInfo(fileStream).sha1()

        self.installs.append((troveCs, (pathId, path, fileId, sha1)))
        return (oldTrv, trv)

    def remove(self, trv):
        self.removes.append(trv)

class MetaCapsuleOperations(CapsuleOperation):

    availableClasses = { 'rpm' : ('conary.local.rpmcapsule',
                                  'RpmCapsuleOperation') }

    def __init__(self, root = '/', *args, **kwargs):
        CapsuleOperation.__init__(self, root, *args, **kwargs)
        self.capsuleClasses = {}

    def apply(self, justDatabase = False, noScripts = False,
              capsuleChangeSet = None):
        if capsuleChangeSet:
            # Previous jobs will have moved the pointer in the auxilliary
            # changeset, so reset it at the start of each job.
            capsuleChangeSet.reset()
        tmpDir = os.path.join(self.root, 'var/tmp')
        if not os.path.isdir(tmpDir):
            # For empty roots or roots that are not systems (e.g. source
            # checkouts), just put capsules in the root directory.
            tmpDir = self.root
        fileDict = {}
        for kind, obj in sorted(self.capsuleClasses.items()):
            fileDict.update(
                dict(((x[0], x[2], x[3]), x[1]) for x in obj._filesNeeded()))

        try:
            for ((pathId, fileId, sha1), path) in sorted(fileDict.items()):
                tmpfd, tmpname = tempfile.mkstemp(dir=tmpDir, prefix=path,
                        suffix='.conary')
                fType, fContents = self.changeSet.getFileContents(pathId,
                                                                  fileId)
                if (fType == changeset.ChangedFileTypes.hldr):
                    if (capsuleChangeSet):
                        try:
                            result = capsuleChangeSet.getFileContents(pathId,
                                                                      fileId)
                            fObj = result[1].get()
                        except KeyError:
                            raise errors.MissingRollbackCapsule('Cannot find '
                                'RPM %s to perform local rollback' % path)

                else:
                    fObj = fContents.get()

                d = digestlib.sha1()
                util.copyfileobj(fObj, os.fdopen(tmpfd, "w"), digest = d)
                actualSha1 = d.digest()
                if actualSha1 != sha1:
                    raise files.Sha1Exception(path)

                # tmpfd is closed when the file object created by os.fdopen
                # disappears
                fileDict[(pathId, fileId)] = tmpname

            for kind, obj in sorted(self.capsuleClasses.items()):
                obj.apply(fileDict, justDatabase = justDatabase, noScripts = noScripts)
        finally:
            for tmpPath in fileDict.values():
                try:
                    os.unlink(tmpPath)
                except:
                    pass

    @classmethod
    def preload(cls, kinds):
        for kind in kinds:
            modName, className = cls.availableClasses[kind]
            if modName not in sys.modules:
                __import__(modName)

    def getCapsule(self, kind):
        if kind not in self.capsuleClasses:
            module, klass = self.availableClasses[kind]

            if module not in sys.modules:
                __import__(module)
            self.capsuleClasses[kind] = \
                getattr(sys.modules[module], klass)(self.root, self.db,
                                                    self.changeSet,
                                                    self.callback,
                                                    self.fsJob)

        return self.capsuleClasses[kind]

    def install(self, flags, troveCs):
        absTroveInfo = troveCs.getFrozenTroveInfo()
        capsuleInfo = trove.TroveInfo.find(trove._TROVEINFO_TAG_CAPSULE,
                                             absTroveInfo)
        if not capsuleInfo or not capsuleInfo.type():
            return False

        if (troveCs.getOldVersion() and troveCs.getOldVersion().onLocalLabel()):
            # diff between a capsule and local label is represented
            # as a conary
            return False

        if self.skipCapsuleOps:
            return True

        if troveCs.getNewVersion().onPhantomLabel():
            # "Installing" a phantom trove is simply taking over an existing
            # installed capsule.
            return True

        capsule = self.getCapsule(capsuleInfo.type())
        capsule.install(flags, troveCs)

        return True

    def remove(self, trove):
        cType = trove.troveInfo.capsule.type()
        if not cType:
            return False

        if self.skipCapsuleOps:
            return True

        capsule = self.getCapsule(cType)
        capsule.remove(trove)
        return True

    def getErrors(self):
        e = []
        for capsule in self.capsuleClasses.values():
            e += capsule.getErrors()

        return e


class MetaCapsuleDatabase(object):
    """
    Top-level object for operations on different types of capsules at the
    whole-system level.
    """
    availablePlugins = {
            'rpm': ('conary.local.rpmcapsule', 'RpmCapsulePlugin',
                '/var/lib/rpm/Packages'),
            }

    def __init__(self, db):
        self._db = weakref.ref(db)
        self._loadedPlugins = {}

    def loadPlugins(self):
        """
        Determine which capsule plugins are relevant to this system, and load
        them.

        This uses a simple test such as the existence of a directory to
        determine whether each plugin is useful. At some point the contents of
        the conary database should also be factored in, so that deleting the
        capsule target database and running a sync should erase all of those
        capsule troves.
        """
        db = self._db()
        for kind, (module, className, checkFunc
                ) in self.availablePlugins.iteritems():
            if kind in self._loadedPlugins:
                continue
            if isinstance(checkFunc, basestring):
                path = util.joinPaths(db.root, checkFunc)
                try:
                    if not os.stat(path).st_size:
                        continue
                except OSError:
                    continue
            else:
                if not checkFunc():
                    continue
            __import__(module)
            cls = getattr(sys.modules[module], className)
            self._loadedPlugins[kind] = cls(db)

    def getChangeSetForCapsuleChanges(self, callback):
        self.loadPlugins()
        changeSet = changeset.ChangeSet()
        for plugin in self._loadedPlugins.itervalues():
            callback.capsuleSyncScan(plugin.kind)
            plugin.addCapsuleChangesToChangeSet(changeSet, callback)
        return changeSet


class PartialTuple(tuple):
    """
    Tuple that compares equality by skipping fields that are empty or None in
    one of the tuples. In other words, ('foo', 'bar') == ('foo', '') but not
    ('foo', 'baz').
    """
    __slots__ = ()
    numRequiredFields = 1

    def __eq__(self, other):
        if len(self) != len(other):
            return False
        for n, (a, b) in enumerate(zip(self, other)):
            if self[n] == other[n]:
                continue
            if n < self.numRequiredFields:
                # Required field is always compared
                return False
            if bool(a) and bool(b):
                # Present in both tuples
                return False
            # Missing in one tuple but it's optional so that's OK
        return True

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        # Hash only the required fields so tuples with and without an optional
        # field go in the same set/dict slot.
        return hash(tuple(self[:self.numRequiredFields]))


class BaseCapsulePlugin(object):
    kind = None

    def __init__(self, db):
        self.root = db.root
        self._db = weakref.ref(db)
        assert self.kind

    @property
    def db(self):
        return self._db()

    def getCapsuleKeysFromLocal(self):
        """
        Return a mapping of capsule keys to NVF tuples for all capsules of this
        type presently in the Conary database.
        """
        tupsByKey = {}
        for tup, data in self.db.db.getAllTroveInfo(
                trove._TROVEINFO_TAG_CAPSULE):
            capsuleInfo = trove.TroveCapsule(data)
            if capsuleInfo.type() != self.kind:
                continue
            key = self._getCapsuleKeyFromInfo(capsuleInfo)
            tupsByKey.setdefault(key, set()).add(tup)
        return tupsByKey

    def _getCapsuleKeyFromInfo(self, capsuleStream):
        """
        Convert a capsule troveinfo stream to a simple tuple that uniquely
        identifies the capsule.
        """
        raise NotImplementedError

    def getCapsuleKeysFromTarget(self):
        """
        Return a mapping of capsule keys to packages (opaque, target-specific
        objects) for all packages in the target capsule database.
        """
        raise NotImplementedError

    def getCapsuleChanges(self):
        """
        Return the rest of removed and added packages in the Conary database
        relative to the target database.
        """
        local = self.getCapsuleKeysFromLocal()
        localSet = set(local)
        target = self.getCapsuleKeysFromTarget()
        targetSet = set(target)
        removedTups = set(itertools.chain(*(x[1] for x in sorted(
            (y, local[y]) for y in localSet - targetSet))))
        addedPkgs = [x[1] for x in sorted(
            (y, target[y]) for y in targetSet - localSet)]
        # Remove duplicate phantom troves
        for key, tups in local.iteritems():
            if len(tups) == 1:
                continue
            real = [x for x in tups if not x[1].onPhantomLabel()]
            phantom = [x for x in tups if x[1].onPhantomLabel()]
            if real:
                # Real trove exists, remove all phantom troves
                removedTups.update(phantom)
            else:
                # Only phantom troves, remove all but one phantom trove
                removedTups.update(sorted(phantom)[:-1])
        return sorted(removedTups), addedPkgs

    def _addPhantomTrove(self, changeSet, package, callback, n, total):
        """
        Given an opaque, target-specific package object, create a phantom
        trove representing that package for the Conary database and add it to
        the given changeset.
        """
        raise NotImplementedError

    def addCapsuleChangesToChangeSet(self, changeSet, callback):
        """
        Find added or removed packages in the target capusle database and place
        the equivalent Conary operations into the given changeset.
        """
        removedTups, addedPkgs = self.getCapsuleChanges()
        removedPkgs = [(x[0].split(':')[0], x[1], x[2]) for x in removedTups]
        removedPkgs = [tup for (tup, exists)
                in zip(removedPkgs, self.db.hasTroves(removedPkgs)) if exists]
        for name, version, flavor in removedTups + removedPkgs:
            changeSet.oldTrove(name, version, flavor)
        for n, pkg in enumerate(addedPkgs):
            self._addPhantomTrove(changeSet, pkg, callback,
                    n + 1, len(addedPkgs))
