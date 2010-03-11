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

import itertools, rpm, os, pwd, stat, tempfile

from conary import files, trove
from conary.lib import util
from conary.local.capsules import SingleCapsuleOperation
from conary.local import errors, update
from conary.repository import filecontents

def rpmkey(hdr):
    return "%s-%s-%s.%s" % ( hdr['name'], hdr['version'],
                             hdr['release'], hdr['arch'])

class Callback:

    def __init__(self, callback, totalSize, logFd):
        self.fdnos = {}
        self.callback = callback
        self.totalSize = totalSize
        self.logFd = logFd
        self.lastAmount = 0
        self.rootFd = os.open("/", os.O_RDONLY)

    def flushRpmLog(self):
        s = os.read(self.logFd, 50000)
        data = ''
        while s:
            data += s
            s = os.read(self.logFd, 50000)

        lines = data.split('\n')
        if not lines:
            return

        # We're in RPM's chroot jail. We'll break out of it so that
        # the callbacks work as expected, but we need to restore both
        # the chroot and the cwd.
        thisRoot = os.open("/", os.O_RDONLY)
        thisDir = os.open(".", os.O_RDONLY)

        try:
            os.fchdir(self.rootFd)
            os.chroot(".")

            for line in lines:
                line.strip()
                if not line:
                    continue

                if line.startswith('error:'):
                    line = line[6:].strip()
                    self.callback.error(line)
                elif line.startswith('warning:'):
                    line = line[8:].strip()
                    self.callback.warning(line)
                else:
                    self.callback.warning(line)
        finally:
            os.fchdir(thisRoot)
            os.close(thisRoot)
            os.chroot(".")
            os.fchdir(thisDir)
            os.close(thisDir)

    def __call__(self, what, amount, total, mydata, wibble):
        self.flushRpmLog()

        if what == rpm.RPMCALLBACK_TRANS_START:
            pass
        elif what == rpm.RPMCALLBACK_INST_OPEN_FILE:
            hdr, path = mydata
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
            self.fdnos[rpmkey(hdr)] = fd
            self.lastAmount = 0
            return fd
        elif what == rpm.RPMCALLBACK_INST_CLOSE_FILE:
            hdr, path = mydata
            os.close(self.fdnos[rpmkey(hdr)])
            del self.fdnos[rpmkey(hdr)]
        elif what == rpm.RPMCALLBACK_INST_PROGRESS:
            self.callback.restoreFiles(amount - self.lastAmount,
                                       self.totalSize)
            self.lastAmount = amount

    def __del__(self):
        assert(not self.fdnos)
        self.flushRpmLog()
        os.close(self.rootFd)

class RpmCapsuleOperation(SingleCapsuleOperation):

    def doApply(self, fileDict, justDatabase = False):
        # force the nss modules to be loaded from outside of any chroot
        pwd.getpwall()

        rpmList = []

        ts = rpm.TransactionSet(self.root, rpm._RPMVSF_NOSIGNATURES)

        if justDatabase:
            ts.setFlags(rpm.RPMTRANS_FLAG_JUSTDB)

        # we use a pretty heavy hammer
        ts.setProbFilter(rpm.RPMPROB_FILTER_IGNOREOS        |
                         rpm.RPMPROB_FILTER_IGNOREARCH      |
                         rpm.RPMPROB_FILTER_REPLACEPKG      |
                         rpm.RPMPROB_FILTER_REPLACENEWFILES |
                         rpm.RPMPROB_FILTER_REPLACEOLDFILES |
                         rpm.RPMPROB_FILTER_OLDPACKAGE)

        for troveCs, (pathId, path, fileId, sha1) in self.installs:
            localPath = fileDict[(pathId, fileId)]
            fd = os.open(localPath, os.O_RDONLY)
            hdr = ts.hdrFromFdno(fd)
            os.close(fd)
            ts.addInstall(hdr, (hdr, localPath), "i")
            hasTransaction = True

            if (rpm.__dict__.has_key('RPMTAG_LONGARCHIVESIZE') and
                rpm.RPMTAG_LONGARCHIVESIZE in hdr.keys()):
                thisSize = hdr[rpm.RPMTAG_LONGARCHIVESIZE]
            else:
                thisSize = hdr[rpm.RPMTAG_ARCHIVESIZE]

            self.fsJob.addToRestoreSize(thisSize)

        removeList = []
        for trv in self.removes:
            ts.addErase("%s-%s-%s.%s" % (
                    trv.troveInfo.capsule.rpm.name(),
                    trv.troveInfo.capsule.rpm.version(),
                    trv.troveInfo.capsule.rpm.release(),
                    trv.troveInfo.capsule.rpm.arch()))

        ts.check()
        ts.order()

        # redirect RPM messages into a temporary file; we harvest them from
        # there and send them on to the callback via the rpm callback
        tmpfd, tmpPath = tempfile.mkstemp()
        writeFile = os.fdopen(tmpfd, "w+")
        readFile = os.open(tmpPath, os.O_RDONLY)

        rpm.setLogFile(writeFile)

        cb = Callback(self.callback, self.fsJob.getRestoreSize(), readFile)
        probs = ts.run(cb, '')

        # flush the RPM log
        del cb

        writeFile.close()
        os.close(readFile)

        if probs:
            raise ValueError(str(probs))

    def install(self, flags, troveCs):
        rc = SingleCapsuleOperation.install(self, flags, troveCs)
        if rc is None:
            # parent class thinks we should just ignore this troveCs; I'm
            # not going to argue with it (it's probably because the capsule
            # hasn't changed
            return None

        (oldTrv, trv) = rc
        trvInfo = troveCs.getNewNameVersionFlavor()
        oldTrvInfo = troveCs.getOldNameVersionFlavor()
        hasCapsule = troveCs.hasCapsule()

        # Updates the fsJob metadata for installing the current trove.
        # It assumes files are replaced on install, and complains if something
        # is in the way unless the appropriate flags are set. This is a very
        # much simplified version of FilesystemJob._singleTrove() which maps
        # out a complete install strategy for native packages. Note that
        # we walk all of the files in this trove, not just the new files
        # or the changed files, because RPM installs all of the files.
        toRestore = []

        changedByPathId = dict((x[0], x) for x in troveCs.getChangedFileList())

        # things which aren't change, new, or removed are unchanged
        unchangedByPathId = (set(x[0] for x in trv.iterFileList()) -
                             set(changedByPathId.iterkeys()) -
                             set(x[0] for x in troveCs.getNewFileList()) -
                             set(troveCs.getOldFileList()))

        l = []
        for oldFileInfo in troveCs.getChangedFileList():
            oldFileId, oldVersion = oldTrv.getFile(oldFileInfo[0])[1:3]
            l.append((oldFileInfo[0], oldFileId, oldVersion))

        for unchangedPathId in unchangedByPathId:
            unchangedFileId, unchangedFileVersion = \
                                    trv.getFile(unchangedPathId)[1:3]
            l.append((unchangedPathId, unchangedFileId, unchangedFileVersion))

        fileObjs = self.db.getFileVersions(l)
        fileObjsByPathId = dict(
                [ (x[0], y) for x, y in
                    itertools.izip(l, fileObjs) ] )

        for fileInfo in trv.iterFileList():
            pathId, path, fileId, version = fileInfo

            if pathId in changedByPathId:
                fileObj = fileObjsByPathId[pathId]
                oldFileId = oldTrv.getFile(pathId)[1]
                fileChange = self.changeSet.getFileChange(oldFileId, fileId)
                fileObj.twm(fileChange, fileObj)
            elif pathId in unchangedByPathId:
                fileObj = fileObjsByPathId[pathId]
            else:
                # if it's not changed and it's not unchanged, it must be new
                fileStream = self.changeSet.getFileChange(None, fileId)
                fileObj = files.ThawFile(fileStream, pathId)

            absolutePath = self.root + path

            if (fileObj.flags.isCapsuleAddition()):
                # this was added to the package outside of the RPM; we don't
                # have any responsibility for it
                continue
            elif (trove.conaryContents(hasCapsule, pathId, fileObj)
                  and fileObj.lsTag != 'd'):
                # this content isn't part of the capsule; remember to put
                # it back when RPM is done
                self.preservePath(path)
                continue

            s = util.lstat(absolutePath)
            if not s:
                # there is nothing in the way, so there is nothing which
                # concerns us here. Track the file for later.
                toRestore.append((fileInfo, fileObj))
                continue

            mayRestore = False

            existingOwners = list(
                self.db.iterFindPathReferences(path, justPresent = True))

            if existingOwners:
                # Don't complain about files owned by the previous version
                # of this trove.
                l = [ x for x in existingOwners if x[0:3] == oldTrvInfo ]
                if l:
                    existingOwners.remove(l[0])

                if not existingOwners:
                    mayRestore = True
            elif stat.S_ISDIR(s.st_mode) and fileObj.lsTag == 'd':
                # Don't let existing directories stop us from taking over
                # ownership of the directory
                mayRestore = True
            elif fileObj.flags.isInitialContents():
                # Initial contents files may be restored on top of things
                # already in the filesystem. They're ghosts or config files
                # and RPM will get the contents right either way, and we
                # should remove them either way.
                mayRestore = True

            if not mayRestore and existingOwners:
                if fileId in [ x[4] for x in existingOwners ]:
                    # The files share metadata same. Whatever it looks like on
                    # disk, RPM is going to blow it away with the new one.
                    for info in existingOwners:
                        self.fsJob.sharedFile(info[0], info[1], info[2],
                                              info[3])
                    mayRestore = True
                elif flags.replaceManagedFiles:
                    # The files are different. Bail unless we're supposed to
                    # replace managed files.
                    existingFile = files.FileFromFilesystem(absolutePath,
                                                            pathId)
                    for info in existingOwners:
                        self.fsJob.userRemoval(
                            fileObj = existingFile,
                            content = filecontents.FromFilesystem(absolutePath),
                            *info[0:4])
                    mayRestore = True
            elif flags.replaceUnmanagedFiles:
                # we don't own it, but it's on disk. RPM will just write over
                # it and we have the flag saying we're good with that
                mayRestore = True

            if mayRestore:
                # We may proceed, and RPM will replace this file for us. We
                # need to track that it's being restored to avoid conflicts
                # with other restorations though.
                toRestore.append((fileInfo, fileObj))
            else:
                # The file exists already, we can't share it, and we're not
                # allowed to overwrite it.
                self._error(errors.FileInWayError(util.normpath(path),
                                                  troveCs.getName(),
                                                  troveCs.getNewVersion(),
                                                  troveCs.getNewFlavor()))

        # toRestore is the list of what is going to be restored. We need to get
        # the fileObjects which will be created so we can track them in the
        # filesystem job. This lets the filesystem job look for resolveable
        # conflicts within this update. We handle newly created files first
        # and files which have changed (so we have to look up the diff)
        # a bit later.
        changedFiles = []
        for fileInfo, fileObj in toRestore:
            self.fsJob._restore(fileObj, self.root + path, trvInfo,
                                "restoring %s from RPM",
                                restoreFile = False,
                                fileId = fileId)

    def remove(self, trv):
        SingleCapsuleOperation.remove(self, trv)

        # make sure everything was erased which should have been; RPM's
        # shared file handling means it may not erase things which we think
        # ought to be
        for trv in self.removes:
            dbFileObjs = self.db.getFileVersions(
                        [ (x[0], x[2], x[3]) for x in trv.iterFileList() ] )

            for (pathId, path, fileId, version), fileObj in \
                    itertools.izip(trv.iterFileList(), dbFileObjs):
                fullPath = self.root + path
                if not os.path.exists(fullPath):
                    continue

                if (fileObj.flags.isCapsuleAddition()):
                    # this was added to the package outside of the RPM;
                    # we don't have any responsibility for it
                    continue
                elif (fileObj.hasContents and
                      not fileObj.flags.isEncapsulatedContent()):
                    # this content isn't part of the capsule; remember to put
                    # it back when RPM is done
                    self.preservePath(path)
                    continue

                fsFileObj = files.FileFromFilesystem(fullPath, pathId,
                                                     possibleMatch = fileObj)
                self.fsJob._remove(fsFileObj, path, fullPath,
                                   'removing rpm owned file %s',
                                   ignoreMissing = True)
