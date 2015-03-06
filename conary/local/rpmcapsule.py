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


import ctypes
import inspect, itertools, re, rpm, os, pwd, stat, tempfile
from ctypes import c_void_p, c_long, c_int

from conary import files, trove
from conary import rpmhelper
from conary import versions
from conary.conaryclient import filetypes
from conary.deps import deps
from conary.lib import elf, util, log
from conary.lib import sha1helper
from conary.lib.compat import namedtuple
from conary.local import capsules
from conary.local.capsules import SingleCapsuleOperation
from conary.local import errors
from conary.repository import changeset
from conary.repository import filecontents


try:
    BaseCapsulePlugin = capsules.BaseCapsulePlugin
except AttributeError:
    # Conary < 2.4.4 delays importing rpmcapsule until the first capsule job,
    # by which time Conary itself might have been updated already. So try to
    # be a little bit backwards-compatible, by not failing if things that are
    # only in 2.4.x are missing.
    # See https://issues.rpath.com/browse/CNY-3752
    BaseCapsulePlugin = object


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
        self.unpackFailures = []

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
        elif what == rpm.RPMCALLBACK_UNPACK_ERROR:
            hdr, path = mydata
            self.unpackFailures.append(hdr)

    def __del__(self):
        assert(not self.fdnos)
        self.flushRpmLog()
        os.close(self.rootFd)

class RpmCapsuleOperation(SingleCapsuleOperation):

    def __init__(self, *args, **kwargs):
        SingleCapsuleOperation.__init__(self, *args, **kwargs)

        self.netSharedPath = set()

        nsp = rpmExpandMacro('%_netsharedpath')
        if nsp != '%_netsharedpath':
            self.netSharedPath = set(nsp.split(':'))

    @staticmethod
    def _canonicalNvra(n, v, r, a):
        return "%s-%s-%s.%s" % (n, v, r, a)

    def _CheckRPMDBContents(self, testNvras, ts, errStr,
            unpackFailures=None, enforceUnpackFailures=None):
        mi = ts.dbMatch()
        installedNvras = set([(h['name'], h['version'], h['release'], h['arch'])
                              for h in mi])
        missingNvras = testNvras.difference(installedNvras)

        if unpackFailures and not enforceUnpackFailures:
            missingNvras -= unpackFailures
            self.callback.warning('RPM failed to unpack ' + ' '.join(
                self._canonicalNvra(*nvra) for nvra in unpackFailures))

        if missingNvras:
            missingSpecs = sorted([self._canonicalNvra(*nvra)
                                  for nvra in missingNvras])
            raise errors.UpdateError(errStr + ' '.join(missingSpecs))

    def doApply(self, fileDict, justDatabase = False, noScripts = False):
        # force the nss modules to be loaded from outside of any chroot
        pwd.getpwall()

        # Create lockdir early since RPM 4.3.3-32 won't do it.
        util.mkdirChain(os.path.join(self.root, 'var/lock/rpm'))

        ts = rpm.TransactionSet(self.root, rpm._RPMVSF_NOSIGNATURES)

        installNvras = set()
        removeNvras = set([(trv.troveInfo.capsule.rpm.name(),
                            trv.troveInfo.capsule.rpm.version(),
                            trv.troveInfo.capsule.rpm.release(),
                            trv.troveInfo.capsule.rpm.arch())
                           for trv in self.removes])
        self._CheckRPMDBContents(removeNvras, ts,
            'RPM database missing packages for removal: ')

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
            installNvras.add(
                (hdr['name'], hdr['version'], hdr['release'], hdr['arch']))
            os.close(fd)
            ts.addInstall(hdr, (hdr, localPath), "i")
            hasTransaction = True

            if (rpm.__dict__.has_key('RPMTAG_LONGARCHIVESIZE') and
                rpm.RPMTAG_LONGARCHIVESIZE in hdr.keys()):
                thisSize = hdr[rpm.RPMTAG_LONGARCHIVESIZE]
            else:
                thisSize = hdr[rpm.RPMTAG_ARCHIVESIZE]

            self.fsJob.addToRestoreSize(thisSize)

        # don't remove RPMs if we have another reference to that RPM
        # in the conary database
        #
        # by the time we get here, the erase items have already been
        # removed from the local database, so see if anything left needs
        # these nevra's

        # if we're installing the same nvra we're removing, don't remove
        # that nvra after installing it. that would be silly, and it just
        # makes our database ops more expensive anyway
        removeNvras -= installNvras
        # look for things with the same name
        afterInstall = set(self.db.findByNames(
                                [ x.getName() for x in self.removes ]))
        # but not things we're just now installing
        afterInstall -= set( x[0].getNewNameVersionFlavor()
                             for x in self.installs )
        # now find the RPMs those previously installed items need
        neededNvras = set((trv.troveInfo.capsule.rpm.name(),
                           trv.troveInfo.capsule.rpm.version(),
                           trv.troveInfo.capsule.rpm.release(),
                           trv.troveInfo.capsule.rpm.arch())
               for trv in self.db.iterTroves(afterInstall)
               if trv.troveInfo.capsule.type() == trove._TROVECAPSULE_TYPE_RPM)
        # and don't remove those things
        removeNvras -= neededNvras
        for nvra in removeNvras:
            ts.addErase("%s-%s-%s.%s" % nvra)

        ts.check()
        ts.order()

        # record RPM's chosen transaction ordering for future debugging
        orderedKeys = []
        # We must use getKeys() rather than iterating over ts to avoid
        # a refcounting bug in RPM's python bindings
        transactionKeys = ts.getKeys()
        # ts.getKeys() returns *either* a list of te's *or* None.
        if transactionKeys is not None:
            for te in transactionKeys:
                if te is not None:
                    # install has a header; erase is an entry of None
                    h = te[0]
                    orderedKeys.append("%s-%s-%s.%s" %(
                        h['name'], h['version'], h['release'], h['arch']))
        if orderedKeys:
            log.syslog('RPM install order: ' + ' '.join(orderedKeys))

        # redirect RPM messages into a temporary file; we harvest them from
        # there and send them on to the callback via the rpm callback
        tmpfd, tmpPath = tempfile.mkstemp()
        writeFile = os.fdopen(tmpfd, "w+")
        readFile = os.open(tmpPath, os.O_RDONLY)

        rpm.setLogFile(writeFile)

        cb = Callback(self.callback, self.fsJob.getRestoreSize(), readFile)
        probs = ts.run(cb, '')
        unpackFailures = set((h['name'], h['version'], h['release'], h['arch'])
                             for h in cb.unpackFailures)

        # flush the RPM log
        del cb

        writeFile.close()
        os.close(readFile)

        if probs:
            raise ValueError(str(probs))

        # CNY-3488: it's potentially harmful to enforce this here
        self._CheckRPMDBContents(installNvras, ts,
            'RPM failed to install requested packages: ',
            unpackFailures=unpackFailures,
            enforceUnpackFailures=False)

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

            if os.path.dirname(path) in self.netSharedPath:
                # we do nothing. really. nothing.
                #
                # we don't back it up. we don't mark it as removed in
                # our database. we don't look for conflicts. nothing.
                continue

            if pathId in changedByPathId:
                oldFileId = oldTrv.getFile(pathId)[1]
                fileChange = self.changeSet.getFileChange(oldFileId, fileId)
                if (oldFileId == fileId):
                    # only the version number changed; we don't need
                    # to merge anything here
                    fileObj = fileObjsByPathId[pathId]
                elif fileChange[0] == '\x01':
                    fileObj = fileObjsByPathId[pathId]
                    fileObj.twm(fileChange, fileObj)
                else:
                    fileObj = files.ThawFile(fileChange, pathId)
            elif pathId in unchangedByPathId:
                fileObj = fileObjsByPathId[pathId]
            else:
                # if it's not changed and it's not unchanged, it must be new
                fileStream = self.changeSet.getFileChange(None, fileId)
                fileObj = files.ThawFile(fileStream, pathId)

            absolutePath = util.joinPaths(self.root, path)

            if (fileObj.flags.isCapsuleAddition()):
                # this was added to the package outside of the RPM; we don't
                # have any responsibility for it
                continue
            elif (trove.conaryContents(hasCapsule, pathId, fileObj)
                  and fileObj.lsTag != 'd'):
                # this content isn't part of the capsule; remember to put
                # it back when RPM is done
                self.preservePath(path, unlink=True)
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

            if action == ACTION_CONFLICT and not existingOwners:
                # Check for "conflicts" that might just be a view across a
                # symlink.
                if self.fsJob.findAliasedRemovals(absolutePath):
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
                    elif (self._checkReplaceManagedFiles(flags, path) or
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
                    else:
                        # it's not up to us to decide if this is a true
                        # conflict; the database layer will do that for
                        # us (see checkPathConflicts)
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
                self.preservePath(path, unlink=False)
                self.fsJob.userRemoval(trv.getName(), trv.getVersion(),
                                       trv.getFlavor(), pathId)

        # toRestore is the list of what is going to be restored. We need to get
        # the fileObjects which will be created so we can track them in the
        # filesystem job. This lets the filesystem job look for resolveable
        # conflicts within this update. We handle newly created files first
        # and files which have changed (so we have to look up the diff)
        # a bit later.
        for fileInfo, fileObj in toRestore:
            fullPath = util.joinPaths(self.root, path)
            self.fsJob._restore(fileObj, fullPath, trvInfo,
                                "restoring %s from RPM",
                                restoreFile = False,
                                fileId = fileId)

    @classmethod
    def _checkReplaceManagedFiles(cls, flags, path):
        # CNY-3662 - make sure we accept old-style flags too
        if inspect.ismethod(flags.replaceManagedFiles):
            return flags.replaceManagedFiles(path)
        # Prior to conary 2.2.11, this was a simple flag, so treat it as such
        return bool(flags.replaceManagedFiles)

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
                fullPath = util.joinPaths(self.root,  path)
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
                    self.preservePath(path, unlink=True)
                    continue

                fsFileObj = files.FileFromFilesystem(fullPath, pathId,
                                                     possibleMatch = fileObj)
                self.fsJob._remove(fsFileObj, path, fullPath,
                                   'removing rpm owned file %s',
                                   ignoreMissing = True)


class RpmCapsulePlugin(BaseCapsulePlugin):
    kind = 'rpm'

    @staticmethod
    def _digest(rpmlibHeader):
        if rpmhelper.SIG_SHA1 in rpmlibHeader.keys():
            return sha1helper.sha1FromString(
                    rpmlibHeader[rpmhelper.SIG_SHA1])
        else:
            return None

    @staticmethod
    def _getCapsuleKeyFromInfo(capsuleStream):
        nevra = capsuleStream.rpm.getNevra()
        digest = capsuleStream.rpm.sha1header()
        # PartialTuple makes two keys with the same NEVRA where one is missing
        # the digest still compare equal.
        return capsules.PartialTuple((nevra, digest))

    def getCapsuleKeysFromTarget(self):
        txnSet = rpm.TransactionSet(self.root)
        matchIter = txnSet.dbMatch()
        headersByKey = {}
        for rpmlibHeader in matchIter:
            nevra = rpmhelper.NEVRA.fromHeader(rpmlibHeader)
            if nevra.name.startswith('gpg-pubkey') and not nevra.arch:
                # Skip fake packages that RPM/yum uses to hold PGP keys
                continue
            digest = self._digest(rpmlibHeader)
            key = capsules.PartialTuple((nevra, digest))
            headersByKey[key] = rpmlibHeader
        return headersByKey

    def _getPhantomNVF(self, header):
        """Choose a NVF for a new phantom package"""
        binCount = 1
        while True:
            name, _, version, release, arch = header.getNevra()
            name += ':rpm'
            verstr = '%s_%s' % (version, release)
            if arch != 'noarch':
                # This is simpler than trying to conver RPM arch to Conary iset
                verstr += '_' + arch
            verstr += '-1-%d' % binCount
            revision = versions.Revision(verstr)
            revision.resetTimeStamp()
            version = versions.Version([versions.PhantomLabel(), revision])
            flavor = deps.Flavor()
            if not self.db.hasTrove(name, version, flavor):
                return name, version, flavor
            binCount += 1

    def _addPhantomContents(self, changeSet, trv, header):
        """Fabricate files for the given RPM header"""
        for (path, owner, group, mode, size, rdev, flags, vflags, linkto,
                mtime) in itertools.izip(
                        header[rpmhelper.OLDFILENAMES],
                        header[rpmhelper.FILEUSERNAME],
                        header[rpmhelper.FILEGROUPNAME],
                        header[rpmhelper.FILEMODES],
                        header[rpmhelper.FILESIZES],
                        header[rpmhelper.FILERDEVS],
                        header[rpmhelper.FILEFLAGS],
                        header[rpmhelper.FILEVERIFYFLAGS],
                        header[rpmhelper.FILELINKTOS],
                        header[rpmhelper.FILEMTIMES],
                        ):
            fullPath = util.joinPaths(self.root, path)
            fakestat = FakeStat(mode, 0, None, 1, owner, group, size,
                    mtime, mtime, mtime, st_rdev=rdev, linkto=linkto)
            pathId = os.urandom(16)

            # Adapted from conary.build.source.addCapsule.doRPM
            kind = 'regular'
            if flags & rpmhelper.RPMFILE_GHOST:
                kind = 'initial'
            elif flags & (rpmhelper.RPMFILE_CONFIG
                    | rpmhelper.RPMFILE_MISSINGOK
                    | rpmhelper.RPMFILE_NOREPLACE):
                if size:
                    kind = 'config'
                else:
                    kind = 'initial'
            elif vflags:
                if (stat.S_ISREG(mode)
                        and not (vflags & rpmhelper.RPMVERIFY_FILEDIGEST)
                    or (stat.S_ISLNK(mode)
                        and not (vflags & rpmhelper.RPMVERIFY_LINKTO))):
                    kind = 'initial'
            # Ignore failures trying to sha1 missing/inaccessible files as long
            # as those files are flagged initial contents (ghost)
            fileStream = files.FileFromFilesystem(fullPath, pathId,
                    statBuf=fakestat, sha1FailOk=True)
            if kind == 'config':
                fileStream.flags.isConfig(set=True)
            elif kind == 'initial':
                fileStream.flags.isInitialContents(set=True)
            else:
                assert kind == 'regular'

            # From conary.build.capsulepolicy.Payload
            if (isinstance(fileStream, files.RegularFile)
                    and not fileStream.flags.isConfig()
                    and not (fileStream.flags.isInitialContents()
                        and not fileStream.contents.size())):
                fileStream.flags.isEncapsulatedContent(set=True)

            fileId = fileStream.fileId()
            trv.addFile(pathId, path, trv.getVersion(), fileId)
            changeSet.addFile(None, fileId, fileStream.freeze())
            # Config file contents have to go into the database, so snag the
            # contents from the filesystem and put them in the changeset.
            if (fileStream.hasContents
                    and not fileStream.flags.isEncapsulatedContent()):
                if fileStream.contents.sha1() == sha1helper.sha1Empty:
                    # Missing/ghost config file. Hopefully it is supposed to be
                    # empty, but even if not then the fake SHA-1 will be the
                    # SHA-1 of the empty string since there's no hint of what
                    # it was supposed to be.
                    contents = filecontents.FromString('')
                else:
                    contents = filecontents.FromFilesystem(fullPath)
                changeSet.addFileContents(pathId, fileId,
                        contType=changeset.ChangedFileTypes.file,
                        contents=contents,
                        cfgFile=fileStream.flags.isConfig(),
                        )

    def _addPhantomTrove(self, changeSet, rpmlibHeader, callback, num, total):
        header = rpmhelper.headerFromBlob(rpmlibHeader.unload())
        callback.capsuleSyncCreate(self.kind, str(header.getNevra()), num,
                total)
        name, version, flavor = self._getPhantomNVF(header)
        # Fake trove
        trv = trove.Trove(name, version, flavor)
        provides = header.getProvides()
        provides.addDep(deps.TroveDependencies, deps.Dependency(name))
        trv.setProvides(provides)
        trv.setRequires(header.getRequires(enableRPMVersionDeps=False))
        # Fake capsule file
        path = str(header.getNevra()) + '.rpm'
        fileHelper = filetypes.RegularFile(contents='')
        fileStream = fileHelper.get(pathId=trove.CAPSULE_PATHID)
        trv.addRpmCapsule(path, version, fileStream.fileId(), header)
        changeSet.addFile(None, fileStream.fileId(), fileStream.freeze())
        # Fake encapsulated files
        self._addPhantomContents(changeSet, trv, header)
        trv.computeDigests()
        changeSet.newTrove(trv.diff(None)[0])

        # Make a fake package to contain the fake component
        pkgName = name.split(':')[0]
        if self.db.hasTrove(pkgName, version, flavor):
            # It's possible to erase just the component and leave the package,
            # so don't try to create it again.
            return
        pkg = trove.Trove(pkgName, version, flavor)
        provides = deps.DependencySet()
        provides.addDep(deps.TroveDependencies, deps.Dependency(pkgName))
        pkg.setProvides(provides)
        pkg.setIsCollection(True)
        pkg.addTrove(name, version, flavor, byDefault=True)
        pkg.computeDigests()
        changeSet.newTrove(pkg.diff(None)[0])


def rpmExpandMacro(val):
    if getattr(rpm, '_rpm', ''):
        rawRpmModulePath = rpm._rpm.__file__
    else:
        rawRpmModulePath = rpm.__file__
    sonames = [ x[1] for x in elf.inspect(rawRpmModulePath)[0]
                    if x[0] == 'soname']
    rpmLibs = [ x for x in sonames if re.match('librpm[-\.].*so', x) ]
    assert(len(rpmLibs) == 1)
    librpm = ctypes.CDLL(rpmLibs[0])
    librpm.expandMacros.argtypes = (c_void_p, c_void_p, c_void_p, c_long)
    librpm.expandMacros.restype = c_int

    buf = ctypes.create_string_buffer(val, len(val) * 100)
    rc = librpm.expandMacros(None, None, buf, len(buf))
    if rc != 0:
        raise RuntimeError("failed to expand RPM macro %r" % (val,))
    return buf.value


# os.stat_result doesn't seem to be usable if you need to populate the fields
# after st_ctime, e.g. rdev
class FakeStat(namedtuple('FakeStat', 'st_mode st_ino st_dev st_nlink st_uid '
        'st_gid st_size st_atime st_mtime st_ctime st_blksize st_blocks '
        'st_rdev linkto')):
    __slots__ = ()

    def __new__(cls, *args, **kwargs):
        out = [None] * len(cls._fields)
        names = set(cls._fields)
        for n, arg in enumerate(args):
            out[n] = arg
            names.remove(cls._fields[n])
        for key, arg in kwargs.items():
            out[cls._fields.index(key)] = arg
            names.remove(key)
        return tuple.__new__(cls, out)
