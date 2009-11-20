#
# Copyright (c) 2009 rPath, Inc.
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

import os, tempfile, sys

from conary import files, trove
from conary.lib import digestlib, util

class CapsuleOperation(object):

    def __init__(self, root, db, changeSet, callback, fsJob):
        self.root = root
        self.db = db
        self.changeSet = changeSet
        self.fsJob = fsJob
        self.callback = callback
        self.errors = []

    def apply(self, justDatabase = False):
        raise NotImplementedException

    def install(self, troveCs):
        raise NotImplementedException

    def remove(self, trove):
        raise NotImplementedException

    def getErrors(self):
        return self.errors

    def _error(self, e):
        self.errors.append(e)

class SingleCapsuleOperation(CapsuleOperation):

    def __init__(self, *args, **kwargs):
        CapsuleOperation.__init__(self, *args, **kwargs)
        self.installs = []
        self.removes = []

    def _filesNeeded(self):
        return [ x[1] for x in self.installs ]

    def apply(self, justDatabase = False):
        raise NotImplementedError

    def install(self, flags, troveCs):
        if troveCs.getOldVersion():
            oldTrv = self.db.getTrove(*troveCs.getOldNameVersionFlavor())
            self.remove(oldTrv)
            trv = oldTrv.copy()
            trv.applyChangeSet(troveCs)
        else:
            oldTrv = None
            trv = trove.Trove(troveCs)

        for pathId, path, fileId, version in trv.iterFileList(capsules = True):
            # there should only be one...
            break

        assert(pathId == trove.CAPSULE_PATHID)

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

    def __init__(self, *args, **kwargs):
        CapsuleOperation.__init__(self, *args, **kwargs)
        self.capsuleClasses = {}

    def apply(self, justDatabase = False):
        fileDict = {}
        for kind, obj in sorted(self.capsuleClasses.items()):
            fileDict.update(
                dict(((x[0], x[2], x[3]), x[1]) for x in obj._filesNeeded()))

        try:
            for ((pathId, fileId, sha1), path) in sorted(fileDict.items()):
                tmpfd, tmpname = tempfile.mkstemp(prefix = path,
                                                  suffix = '.conary')
                fObj = self.changeSet.getFileContents(pathId, fileId)[1].get()
                d = digestlib.sha1()
                util.copyfileobj(fObj, os.fdopen(tmpfd, "w"), digest = d)
                actualSha1 = d.digest()
                if actualSha1 != sha1:
                    raise files.Sha1Exception(path)

                # tmpfd is closed when the file object created by os.fdopen
                # disappears
                fileDict[(pathId, fileId)] = tmpname

            for kind, obj in sorted(self.capsuleClasses.items()):
                obj.apply(fileDict, justDatabase = justDatabase)
        finally:
            for tmpPath in fileDict.values():
                try:
                    os.unlink(tmpPath)
                except:
                    pass

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

        capsule = self.getCapsule(capsuleInfo.type())
        capsule.install(flags, troveCs)

        return True

    def remove(self, trove):
        cType = trove.troveInfo.capsule.type()
        if not cType:
            return False

        capsule = self.getCapsule(cType)
        capsule.remove(trove)
        return True

    def getErrors(self):
        e = []
        for capsule in self.capsuleClasses.values():
            e += capsule.getErrors()

        return e
