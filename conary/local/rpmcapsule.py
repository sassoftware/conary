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

from conary import files
from conary.lib import util
from conary.local.capsules import SingleCapsuleOperation
from conary.local import errors

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

    def flushRpmLog(self):
        s = os.read(self.logFd, 50000)
        data = ''
        while s:
            data += s
            s = os.read(self.logFd, 50000)

        lines = data.split('\n')
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

class RpmCapsuleOperation(SingleCapsuleOperation):

    def apply(self, fileDict):
        # force the nss modules to be loaded from outside of any chroot
        pwd.getpwall()

        rpmList = []

        ts = rpm.TransactionSet(self.root, rpm._RPMVSF_NOSIGNATURES)
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

            if rpm.RPMTAG_LONGARCHIVESIZE in hdr.keys():
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
        (oldTrv, trv) = SingleCapsuleOperation.install(self, flags, troveCs)
        trvInfo = troveCs.getNewNameVersionFlavor()
        oldTrvInfo = troveCs.getOldNameVersionFlavor()

        # Updates the fsJob metadata for installing the current trove.
        # It assumes files are replaced on install, and complains if something
        # is in the way unless the appropriate flags are set. This is a very
        # much simplified version of FilesystemJob._singleTrove() which maps
        # out a complete install strategy for native packages. Note that
        # we walk all of the files in this trove, not just the new files
        # or the changed files, because RPM installs all of the files.
        toRestore = []

        l = []
        for oldFileInfo in troveCs.getChangedFileList():
            oldFileId, oldVersion = oldTrv.getFile(oldFileInfo[0])[1:3]
            l.append((oldFileInfo[0], oldFileId, oldVersion))

        oldFileObjs = self.db.getFileVersions(l)
        oldFileObjsByPathId = dict(
                [ (x[0], y) for x, y in
                    itertools.izip(l, oldFileObjs) ] )
        changedByPathId = dict((x[0], x) for x in troveCs.getChangedFileList())

        for fileInfo in trv.iterFileList():
            pathId, path, fileId, version = fileInfo

            if pathId in changedByPathId:
                fileObj = oldFileObjsByPathId[pathId]
                oldFileId = oldTrv.getFile(pathId)[1]
                fileChange = self.changeSet.getFileChange(oldFileId, fileId)
                fileObj.twm(fileChange, fileObj)
            else:
                fileStream = self.changeSet.getFileChange(None, fileId)
                fileObj = files.ThawFile(fileStream, pathId)

            absolutePath = self.root + path
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
                    existingFile = files.FileFromFilesystem(path, pathId)
                    for info in existingOwners:
                        self.fsJob.userRemoval(
                            fileObj = existingFile,
                            content = filecontents.FromFilesystem(headRealPath),
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
