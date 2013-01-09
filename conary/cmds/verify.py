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


"""
Provides the output for the "conary verify" command
"""
import itertools, os, stat, sys

from conary import trove
from conary import versions
from conary import conaryclient, files
from conary.cmds import showchangeset
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import dirset, log, sha1helper, util
from conary.local import update
from conary.repository import changeset, filecontents, trovesource
from conary import errors

DISPLAY_NONE = 0
DISPLAY_DIFF = 1
DISPLAY_CS = 2

NEW_FILES_NONE      = 0
NEW_FILES_OWNED_DIR = 1
NEW_FILES_ANY_DIR   = 2

class _FindLocalChanges(object):

    def __init__(self, db, cfg, display = True, forceHashCheck = False,
                 changeSetPath = None, allMachineChanges = False,
                 asDiff = False, repos = None, newFiles = NEW_FILES_NONE,
                 diffBinaries = False):
        self.db = db
        self.cfg = cfg
        self.display = display
        self.newFiles = newFiles
        self.forceHashCheck = forceHashCheck
        self.changeSetPath = changeSetPath
        self.allMachineChanges = allMachineChanges
        self.asDiff = asDiff or diffBinaries
        self.diffBinaries = diffBinaries
        self.repos = repos
        self.statCache = {}

        if asDiff:
            self.diffTroveSource = trovesource.SourceStack(db, self.repos)

    def _addFile(self, cs, trv, path):
        pathId = sha1helper.md5String(path)
        absPath = self.cfg.root + path
        fileObj = files.FileFromFilesystem(absPath, pathId)
        fileId = fileObj.fileId()
        trv.addFile(pathId, path, trv.getVersion(), fileId)
        cs.addFile(None, fileId, fileObj.freeze())
        if fileObj.hasContents:
            cs.addFileContents(pathId, fileId,
                               changeset.ChangedFileTypes.file,
                               filecontents.FromFilesystem(absPath),
                               False)

    def _simpleTroveList(self, troveList, newFilesByTrove):
        log.info('Verifying %s' % " ".join(x[1].getName() for x in troveList))
        changedTroves = set()

        try:
            result = update.buildLocalChanges(self.db, troveList,
                                              root=self.cfg.root,
                                              forceSha1=self.forceHashCheck,
                                              ignoreTransient=True,
                                              updateContainers=True,
                                              statCache = self.statCache)
            if not result: return
            cs = result[0]
            changed = False
            for (changed, trv) in result[1]:
                if changed:
                    changedTroves.add(trv.getNameVersionFlavor())
        except OSError, err:
            if err.errno == 13:
                log.warning("Permission denied creating local changeset for"
                            " %s " % str([ x[0].getName() for x in troveList ]))
            return

        trovesChanged = []

        for (dbTrv, srcTrv, newVer, flags), (changed, localTrv) in \
                itertools.izip(troveList, result[1]):
            if srcTrv.getNameVersionFlavor() in newFilesByTrove:
                for path in newFilesByTrove[srcTrv.getNameVersionFlavor()]:
                    self._addFile(cs, localTrv, path)

                localTrv.computeDigests()
                trvDiff = localTrv.diff(dbTrv, absolute = False)[0]
                cs.newTrove(trvDiff)
                trovesChanged.append(localTrv.getNameVersionFlavor())
            elif changed:
                trovesChanged.append(localTrv.getNameVersionFlavor())

        if trovesChanged:
            self._handleChangeSet(trovesChanged, cs)

    def _handleChangeSet(self, trovesChanged, cs):
        class NonPristineDatabaseWrapper(object):
            def __getattr__(self, n):
                return getattr(self.db, n)

            def getTroves(self, *args, **kwargs):
                kwargs['pristine'] = False
                return self.db.getTroves(*args, **kwargs)

            def __init__(self, db):
                self.db = db

        if self.display == DISPLAY_DIFF:
            for x in cs.gitDiff(self.diffTroveSource,
                                diffBinaries = self.diffBinaries):
                sys.stdout.write(x)
        elif self.display == DISPLAY_CS:
            troveSpecs = [ '%s=%s[%s]' % x for x in trovesChanged ]
            showchangeset.displayChangeSet(NonPristineDatabaseWrapper(self.db), cs, troveSpecs,
                                           self.cfg, ls=True,
                                           showChanges=True, asJob=True)

        if trovesChanged and self.finalCs:
            self.finalCs.merge(cs)

    def _verifyTroves(self, fullTroveList, newFilesByTrove):
        verifyList = []

        for troveInfo in fullTroveList:
            if verifyList and (verifyList[-1][0].getName().split(':')[0] !=
                               troveInfo[0].split(':')[0]):
                # display output as soon as we're done processing one named
                # trove; this works because walkTroveSet is guaranteed to
                # be depth first
                self._simpleTroveList(verifyList, newFilesByTrove)

                verifyList = []

            thisTrv = self.db.getTrove(pristine = False,
                                       withFileObjects = True,
                                       *troveInfo)

            self.db.getTrove(pristine = True,
                             withFileObjects = True,
                             *thisTrv.getNameVersionFlavor())

            ver = thisTrv.getVersion().createShadow(versions.LocalLabel())
            verifyList.append((thisTrv, thisTrv, ver, update.UpdateFlags()))

        self._simpleTroveList(verifyList, newFilesByTrove)

    def _scanFilesystem(self, fullTroveList, dirType = NEW_FILES_OWNED_DIR):
        dirs = list(self.db.db.getTroveFiles(fullTroveList,
                                             onlyDirectories = True))
        skipDirs = dirset.DirectorySet(self.cfg.verifyDirsNoNewFiles)
        dirOwners = dirset.DirectoryDict()
        for trvInfo, dirName, stream in dirs:
            dirOwners[dirName] = trvInfo

        newFiles = []

        if dirType == NEW_FILES_ANY_DIR and '/' not in dirOwners:
            dirsToWalk = [ '/' ]
        else:
            dirsToWalk = sorted(dirOwners.itertops())

        dbPaths = self.db.db.getTroveFiles(fullTroveList)
        fsPaths = util.walkiter(dirsToWalk, skipPathSet = skipDirs,
                                root = self.cfg.root)
        lastDbPath = None
        lastFsPath = None

        try:
            for i in itertools.count(0):
                if lastDbPath is None:
                    trvInfo, lastDbPath, lastDbStream = dbPaths.next()
                if lastFsPath is None:
                    lastFsPath, lastFsStat = fsPaths.next()

                if lastDbPath < lastFsPath:
                    # in the database, but not the filesystem. that means
                    # it's gone missing, and we don't care much
                    lastDbPath = None
                elif lastDbPath > lastFsPath:
                    # it's in the filesystem, but not the database
                    if not stat.S_ISDIR(lastFsStat.st_mode):
                        newFiles.append(lastFsPath)
                    lastFsPath = None
                else:
                    # it's in both places
                    absPath = os.path.normpath(self.cfg.root + lastFsPath)
                    self.statCache[absPath] = lastFsStat
                    lastFsPath = None
                    lastDbPath = None
        except StopIteration:
            pass

        # we don't need this, but drain the iterator
        [ x for x in dbPaths ]

        if lastFsPath and not stat.S_ISDIR(lastFsStat.st_mode):
            newFiles.append(lastFsPath)

        for lastFsPath, lastFsStat in fsPaths:
            if not stat.S_ISDIR(lastFsStat.st_mode):
                newFiles.append(lastFsPath)

        # newFiles is a list of files which have been locally added.
        # filter out ones which are owned by other troves. a bit silly
        # to do this if --all is used.
        areOwned = self.db.db.pathsOwned(newFiles)
        newFiles = [ path for path, isOwned in
                        itertools.izip(newFiles, areOwned)
                        if not isOwned ]

        # now turn newFiles into a dict which maps troves being verified to the
        # new files for that trove. byTrove[None] lists new files which no
        # trove claims ownership of
        byTrove = {}
        for path in newFiles:
            trvInfo = dirOwners.get(path, None)
            l = byTrove.setdefault(trvInfo, [])
            l.append(path)

        return byTrove

    def _addUnownedNewFiles(self, newFileList):
        if not newFileList: return

        cs = changeset.ChangeSet()
        ver = versions.VersionFromString('/localhost@local:LOCAL/1.0-1-1').copy()
        ver.resetTimeStamps()
        trv = trove.Trove("@new:files", ver, deps.Flavor())
        for path in newFileList:
            self._addFile(cs, trv, path)

        trvDiff = trv.diff(None, absolute = False)[0]
        cs.newTrove(trvDiff)

        self._handleChangeSet( [ trv.getNameVersionFlavor() ], cs)

    def generateChangeSet(self, troveNameList, all=False):
        if self.display != DISPLAY_NONE:
            # save memory by not keeping the changeset around; this is
            # particularly useful when all=True
            self.finalCs = None
        else:
            self.finalCs = changeset.ReadOnlyChangeSet()

        troveNames = [ cmdline.parseTroveSpec(x) for x in troveNameList ]
        if all:
            assert(not troveNameList)
            client = conaryclient.ConaryClient(self.cfg)
            troveInfo = client.getUpdateItemList()
            troveInfo.sort()
        else:
            troveInfo = []

            for (troveName, versionStr, flavor) in troveNames:
                try:
                    troveInfo += self.db.findTrove(None,
                                    (troveName, versionStr, flavor))
                except errors.TroveNotFound:
                    if versionStr:
                        if flavor is not None and not flavor.isEmpty():
                            flavorStr = deps.formatFlavor(flavor)
                            log.error("version %s with flavor '%s' of "
                                      "trove %s is not installed",
                                      versionStr, flavorStr, troveName)
                        else:
                            log.error("version %s of trove %s is not installed",
                                      versionStr, troveName)
                    elif flavor is not None and not flavor.isEmpty():
                        flavorStr = deps.formatFlavor(flavor)
                        log.error("flavor '%s' of trove %s is not installed",
                                  flavorStr, troveName)
                    else:
                        log.error("trove %s is not installed", troveName)

        # we need the recursive closure of the set; self.db.walkTroveSet(trv)
        # is surely not the most efficient thing to do, but it's easy. remember
        # it's depth first; keeping the order depth first helps keep the
        # output sane

        troves = self.db.getTroves(troveInfo, withDeps = False,
                                   withFileObjects = True, pristine = False)
        seen = set()
        fullTroveList = []
        for topTrv in troves:
            for nvf in self.db.walkTroveSet(topTrv, withFiles = False,
                                                asTuple = True):
                seen.add(nvf)
                fullTroveList.append(nvf)

        if self.newFiles:
            newFilesByTrove = self._scanFilesystem(fullTroveList,
                                                   dirType = self.newFiles)
        else:
            newFilesByTrove = {}

        self._verifyTroves(fullTroveList, newFilesByTrove)

        if None in newFilesByTrove:
            self._addUnownedNewFiles(newFilesByTrove[None])

        if self.finalCs:
            for trv in troves:
                self.finalCs.addPrimaryTrove(
                         trv.getName(),
                         trv.getVersion().createShadow(versions.LocalLabel()),
                         trv.getFlavor())

        return self.finalCs

    def run(self, troveNameList, all=False):
        cs = self.generateChangeSet(troveNameList, all=all)
        if self.changeSetPath:
            cs.writeToFile(self.changeSetPath)

        return cs

class DiffObject(_FindLocalChanges):

    def __init__(self, troveNameList, db, cfg, all = False,
                 changesetPath = None, forceHashCheck = False,
                 asDiff=False, repos=None, newFiles = False,
                 diffBinaries=False):
        asDiff = asDiff or diffBinaries;

        if asDiff:
            display = DISPLAY_DIFF
        elif changesetPath:
            display = DISPLAY_NONE
        else:
            display = DISPLAY_CS

        if newFiles:
            if all:
                newFiles = NEW_FILES_ANY_DIR
            else:
                newFiles = NEW_FILES_OWNED_DIR

        _FindLocalChanges.__init__(self, db, cfg,
                                   display=display,
                                   forceHashCheck=forceHashCheck,
                                   changeSetPath=changesetPath,
                                   asDiff=asDiff, repos=repos,
                                   diffBinaries = diffBinaries,
                                   newFiles=newFiles)
        self.run(troveNameList, all=all)

class verify(DiffObject):

    def generateChangeSet(self, *args, **kwargs):
        cs = DiffObject.generateChangeSet(self, *args, **kwargs)
        if cs is not None:
            # verify doesn't display changes in collections because those, by
            # definition, match the database
            for trvCs in list(cs.iterNewTroveList()):
                if trove.troveIsCollection(trvCs.getName()):
                    cs.delNewTrove(*trvCs.getNewNameVersionFlavor())

        return cs

class LocalChangeSetCommand(_FindLocalChanges):

    def __init__(self, db, cfg, item, changeSetPath = None):
        _FindLocalChanges.__init__(self, db, cfg,
                                   display=DISPLAY_NONE,
                                   allMachineChanges=True)
        cs = self.run([item])

        if not [ x for x in cs.iterNewTroveList() ]:
            log.error("there have been no local changes")
        else:
            cs.writeToFile(changeSetPath)
