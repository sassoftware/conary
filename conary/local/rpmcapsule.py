#
# Copyright (c) 2009, 2010 rPath, Inc.
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
from conary.deps import deps
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
        concats = []

        try:
            os.fchdir(self.rootFd)
            os.chroot(".")

            for line in lines:
                line.strip()
                if not line:
                    continue

                # this passwd/group stuff is for CNY-3428. Basically group
                # info packages can create users before Red Hat's setup
                # package is installed. this fixes things up.
                if '/etc/passwd.rpmnew' in line:
                    concats.append( ('/etc/passwd', '/etc/passwd.rpmnew') )
                elif '/etc/group.rpmnew' in line:
                    concats.append( ('/etc/group', '/etc/group.rpmnew') )
                elif line.startswith('error:'):
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

        for (keepPath, fromPath) in concats:
            finalLines = open(fromPath).readlines() + open(keepPath).readlines()
            finalLines = [ (x.split(':')[0], x) for x in finalLines ]
            seen = set()
            f = open(keepPath, "w")
            for (name, line) in finalLines:
                if name not in seen:
                    seen.add(name)
                    f.write(line)

            f.close()
            os.unlink(fromPath)

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

    def doApply(self, fileDict, justDatabase = False, noScripts = False):
        # force the nss modules to be loaded from outside of any chroot
        pwd.getpwall()

        rpmList = []

        ts = rpm.TransactionSet(self.root, rpm._RPMVSF_NOSIGNATURES)

        tsFlags = 0
        if justDatabase:
            tsFlags |= rpm.RPMTRANS_FLAG_JUSTDB
        if noScripts:
            tsFlags |= rpm.RPMTRANS_FLAG_NOSCRIPTS
        ts.setFlags(tsFlags)

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
        ACTION_RESTORE = 1
        ACTION_SKIP = 2
        ACTION_CONFLICT = 3

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

            action = ACTION_CONFLICT

            existingOwners = list(
                self.db.iterFindPathReferences(path, justPresent = True,
                                               withStream = True))

            if existingOwners:
                # Don't complain about files owned by the previous version
                # of this trove.
                l = [ x for x in existingOwners if x[0:3] == oldTrvInfo ]
                if l:
                    existingOwners.remove(l[0])

                if not existingOwners:
                    action = ACTION_RESTORE
            elif stat.S_ISDIR(s.st_mode) and fileObj.lsTag == 'd':
                # Don't let existing directories stop us from taking over
                # ownership of the directory
                action = ACTION_RESTORE
            elif fileObj.flags.isInitialContents():
                # Initial contents files may be restored on top of things
                # already in the filesystem. They're ghosts or config files
                # and RPM will get the contents right either way, and we
                # should remove them either way.
                action = ACTION_RESTORE

            if action == ACTION_CONFLICT and existingOwners:
                if fileId in [ x[4] for x in existingOwners ]:
                    # The files share metadata same. Whatever it looks like on
                    # disk, RPM is going to blow it away with the new one.
                    for info in existingOwners:
                        self.fsJob.sharedFile(info[0], info[1], info[2],
                                              info[3])
                    action = ACTION_RESTORE
                elif path.startswith('/usr/share/doc/'):
                    # Mirror badness Red Hat patches into RPM for rhel4
                    # and rhel5
                    action = ACTION_RESTORE
                else:
                    existingFiles = [ files.ThawFile(x[5], pathId) for x
                                        in existingOwners ]

                    compatibility = [ 1 for x in existingFiles
                                        if fileObj.compatibleWith(x) ]


                    if 1 in compatibility:
                        # files can be shared even though the fileId's
                        # are different
                        for info in existingOwners:
                            self.fsJob.sharedFile(info[0], info[1], info[2],
                                                  info[3])
                        action = ACTION_RESTORE
                    elif 1 in [ files.rpmFileColorCmp(x, fileObj)
                              for x in existingFiles ]:
                        # rpm file colors and the default rpm setting for
                        # file color policy make elf64 files silently replace
                        # elf32 files. follow that behavior here.
                        #
                        # no, i'm not making this up
                        #
                        # yes, really
                        action = ACTION_SKIP
                    elif (flags.replaceManagedFiles or
                          1 in [ files.rpmFileColorCmp(fileObj, x)
                                 for x in existingFiles ]):
                        # The files are different. Bail unless we're supposed
                        # to replace managed files.
                        existingFile = files.FileFromFilesystem(absolutePath,
                                                                pathId)
                        for info in existingOwners:
                            self.fsJob.userRemoval(
                                fileObj = existingFile,
                                content =
                                    filecontents.FromFilesystem(absolutePath),
                                *info[0:4])
                        action = ACTION_RESTORE
            elif flags.replaceUnmanagedFiles:
                # we don't own it, but it's on disk. RPM will just write over
                # it and we have the flag saying we're good with that
                action = ACTION_RESTORE

            if action == ACTION_RESTORE:
                # We may proceed, and RPM will replace this file for us. We
                # need to track that it's being restored to avoid conflicts
                # with other restorations though.
                toRestore.append((fileInfo, fileObj))
            elif action == ACTION_CONFLICT:
                # The file exists already, we can't share it, and we're not
                # allowed to overwrite it.
                self._error(errors.FileInWayError(util.normpath(path),
                                                  troveCs.getName(),
                                                  troveCs.getNewVersion(),
                                                  troveCs.getNewFlavor()))
            else:
                assert(action == ACTION_SKIP)
                self.preservePath(path)
                self.fsJob.userRemoval(trv.getName(), trv.getVersion(),
                                       trv.getFlavor(), pathId)

        # toRestore is the list of what is going to be restored. We need to get
        # the fileObjects which will be created so we can track them in the
        # filesystem job. This lets the filesystem job look for resolveable
        # conflicts within this update. We handle newly created files first
        # and files which have changed (so we have to look up the diff)
        # a bit later.
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
                hasCapsule = trv.troveInfo.capsule.type() or False
                fullPath = self.root + path
                if not os.path.exists(fullPath):
                    continue

                if (fileObj.flags.isCapsuleAddition()):
                    # this was added to the package outside of the RPM;
                    # we don't have any responsibility for it
                    continue
                elif (fileObj.hasContents and
                      trove.conaryContents(hasCapsule, pathId, fileObj)):
                    # this content isn't part of the capsule; remember to put
                    # it back when RPM is done
                    self.preservePath(path)
                    continue

                fsFileObj = files.FileFromFilesystem(fullPath, pathId,
                                                     possibleMatch = fileObj)
                self.fsJob._remove(fsFileObj, path, fullPath,
                                   'removing rpm owned file %s',
                                   ignoreMissing = True)
