# Copyright (c) 2006-2008 rPath, Inc.
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

from conary import files, trove, versions
from conary import errors as conaryerrors
from conary.build import build, source
from conary.build import errors as builderrors
from conary.build.packagerecipe import AbstractPackageRecipe
from conary.lib import log, util
from conary.repository import changeset, filecontents

class DerivedPackageRecipe(AbstractPackageRecipe):

    internalAbstractBaseClass = 1
    _isDerived = True
    parentVersion = None

    def installingTrove(self, trv):
        if self.troveFlavor is None:
            self.troveFlavor = trv.getFlavor().copy()
        else:
            assert(self.troveFlavor == trv.getFlavor())

        name = trv.getName()
        self._componentReqs[name] = trv.getRequires().copy()
        self._componentProvs[name] = trv.getProvides().copy()

        if trv.isCollection():
            # gather up existing byDefault status
            # from (component, byDefault) tuples
            self.byDefaultXX.update(dict(
                [(x[0][0], x[1]) for x in trv.iterTroveListInfo()]))

    def handleFileAttributes(self, trv, fileObj, path):
        self.troveFlavor -= fileObj.flavor()

        # Config vs. InitialContents etc. might be change in derived pkg
        # Set defaults here, and they can be overridden with
        # "exceptions = " later
        if fileObj.flags.isConfig():
            self.Config(path)
        elif fileObj.flags.isInitialContents():
            self.InitialContents(path)
        elif fileObj.flags.isTransient():
            self.Transient(path)

        # we don't restore setuid/setgid bits into the filesystem
        if fileObj.inode.perms() & 06000 != 0:
            self.SetModes(path, fileObj.inode.perms())

        if isinstance(fileObj, files.Directory):
            # remember to include this directory in the derived package even
            # if it's empty
            self.ExcludeDirectories(exceptions = path)

        if isinstance(fileObj, files.SymbolicLink):
            # mtime for symlinks is meaningless, we have to record the
            # target of the symlink instead
            self._derivedFiles[path] = fileObj.target()
        else:
            self._derivedFiles[path] = fileObj.inode.mtime()

        self._componentReqs[trv.getName()] -= fileObj.requires()
        self._componentProvs[trv.getName()] -= fileObj.requires()

    def restoreFile(self, trv, fileObj, contents, destdir, path):
        self.handleFileAttributes(trv, fileObj, path)
        if isinstance(fileObj, files.DeviceFile):
            self.MakeDevices(path, fileObj.lsTag,
                             fileObj.devt.major(), fileObj.devt.minor(),
                             fileObj.inode.owner(), fileObj.inode.group(),
                             fileObj.inode.perms())
        else:
            fileObj.restore(contents, destdir, destdir + path)

    def restoreLink(self, trv, fileObj, destdir, sourcePath, targetPath):
        self.handleFileAttributes(trv, fileObj, targetPath)
        util.createLink(destdir + sourcePath, destdir + targetPath)

    def installPath(self, path):
        return path != self.macros.buildlogpath

    def _expandChangeset(self, cs):
        destdir = self.macros.destdir

        ptrMap = {}

        fileList = []
        linkGroups = {}
        linkGroupFirstPath = {}
        self.troveFlavor = None
        self.byDefaultXX = {}
        # sort the files by pathId,fileId
        for trvCs in cs.iterNewTroveList():
            trv = trove.Trove(trvCs)
            self.installingTrove(trv)
            for pathId, path, fileId, version in trv.iterFileList():
                fileList.append((pathId, fileId, path, trv))

        fileList.sort()

        restoreList = []

        for pathId, fileId, path, trv in fileList:
            if not self.installPath(path):
                continue

            fileCs = cs.getFileChange(None, fileId)
            fileObj = files.ThawFile(fileCs, pathId)

            if fileObj.hasContents:
                restoreList.append((pathId, fileId, fileObj, path, trv))
            else:
                self.restoreFile(trv, fileObj, None, destdir, path)

        delayedRestores = {}
        for pathId, fileId, fileObj, destPath, trv in restoreList:
            (contentType, contents) = cs.getFileContents(pathId, fileId)
            if contentType == changeset.ChangedFileTypes.ptr:
                targetPtrId = contents.get().read()
                l = delayedRestores.setdefault(targetPtrId, [])
                l.append((fileObj, destPath))
                continue

            assert(contentType == changeset.ChangedFileTypes.file)

            ptrId = pathId + fileId
            if pathId in delayedRestores:
                ptrMap[pathId] = destPath
            elif ptrId in delayedRestores:
                ptrMap[ptrId] = destPath

            self.restoreFile(trv, fileObj, contents, destdir, destPath)

            linkGroup = fileObj.linkGroup()
            if linkGroup:
                linkGroups[linkGroup] = destPath

            for fileObj, targetPath in delayedRestores.get(ptrId, []):
                linkGroup = fileObj.linkGroup()
                if linkGroup in linkGroups:
                    self.restoreLink(trv, fileObj, destdir,
                                     linkGroups[linkGroup], targetPath)
                else:
                    self.restoreFile(trv, fileObj, contents, destdir,
                                     targetPath)

                    if linkGroup:
                        linkGroups[linkGroup] = targetPath

        self.useFlags = self.troveFlavor

        self.setByDefaultOn(set(x for x in self.byDefaultXX if self.byDefaultXX[x]))
        self.setByDefaultOff(set(x for x in self.byDefaultXX if not self.byDefaultXX[x]))

    def unpackSources(self, resume=None, downloadOnly=False):

        repos = self.laReposCache.repos
        if self.parentVersion:
            try:
                parentRevision = versions.Revision(self.parentVersion)
            except conaryerrors.ParseError, e:
                raise builderrors.RecipeFileError(
                            'Cannot parse parentVersion %s: %s' % \
                                    (self.parentVersion, str(e)))
        else:
            parentRevision = None

        sourceBranch = versions.VersionFromString(self.macros.buildbranch)
        if not sourceBranch.isShadow():
            raise builderrors.RecipeFileError(
                    "only shadowed sources can be derived packages")

        if parentRevision and \
                self.sourceVersion.trailingRevision().getVersion() != \
                                                parentRevision.getVersion():
            raise builderrors.RecipeFileError(
                    "parentRevision must have the same upstream version as the "
                    "derived package recipe")

        # find all the flavors of the parent
        parentBranch = sourceBranch.parentBranch()

        if parentRevision:
            parentVersion = parentBranch.createVersion(parentRevision)

            d = repos.getTroveVersionFlavors({ self.name :
                                { parentVersion : [ None ] } } )
            if self.name not in d:
                raise builderrors.RecipeFileError(
                        'Version %s of %s not found'
                                    % (parentVersion, self.name) )
        else:
            d = repos.getTroveLeavesByBranch(
                    { self.name : { parentBranch : [ None ] } } )

            if not d[self.name]:
                raise builderrors.RecipeFileError(
                    'No versions of %s found on branch %s' % 
                            (self.name, parentBranch))

            parentVersion = sorted(d[self.name].keys())[-1]

        bestFlavor = (-1, [])
        # choose which flavor to derive from
        for flavor in d[self.name][parentVersion]:
            score = self.cfg.buildFlavor.score(flavor)
            if score is False: continue
            if bestFlavor[0] < score:
                bestFlavor = (score, [ flavor ])
            elif bestFlavor[0] == score:
                bestFlavor[1].append(flavor)

        if bestFlavor[0] == -1:
            raise builderrors.RecipeFileError(
                    'No flavors of %s=%s found for build flavor %s' %
                    (self.name, parentVersion, self.cfg.buildFlavor))
        elif len(bestFlavor[1]) > 1:
            raise builderrors.RecipeFileError(
                    'Multiple flavors of %s=%s match build flavor %s',
                    self.name, parentVersion, self.cfg.buildFlavor)

        parentFlavor = bestFlavor[1][0]

        log.info('deriving from %s=%s[%s]', self.name, parentVersion,
                 parentFlavor)

        # Fetch all binaries built from this source
        v = parentVersion.getSourceVersion(removeShadows=False)
        binaries = repos.getTrovesBySource(self.name + ':source', v)

        # Filter out older ones
        binaries = [ x for x in binaries \
                        if (x[1], x[2]) == (parentVersion,
                                            parentFlavor) ]

        # Build trove spec
        troveSpec = [ (x[0], (None, None), (x[1], x[2]), True)
                        for x in binaries ]

        cs = repos.createChangeSet(troveSpec, recurse = False)
        self.addLoadedTroves([
            (x.getName(), x.getNewVersion(), x.getNewFlavor()) for x
            in cs.iterNewTroveList() ])

        self._expandChangeset(cs)
        self.cs = cs

        AbstractPackageRecipe.unpackSources(self, resume = resume,
                                             downloadOnly = downloadOnly)

    def loadPolicy(self):
        return AbstractPackageRecipe.loadPolicy(self,
                                internalPolicyModules = ( 'derivedpolicy', ) )

    def __init__(self, cfg, laReposCache, srcDirs, extraMacros={},
                 crossCompile=None, lightInstance=False):
        AbstractPackageRecipe.__init__(self, cfg, laReposCache, srcDirs,
                                        extraMacros = extraMacros,
                                        crossCompile = crossCompile,
                                        lightInstance = lightInstance)

        self._addBuildAction('Ant', build.Ant)
        self._addBuildAction('Automake', build.Automake)
        self._addBuildAction('ClassPath', build.ClassPath)
        self._addBuildAction('CompilePython', build.CompilePython)
        self._addBuildAction('Configure', build.Configure)
        self._addBuildAction('ConsoleHelper', build.ConsoleHelper)
        self._addBuildAction('Copy', build.Copy)
        self._addBuildAction('Create', build.Create)
        self._addBuildAction('Desktopfile', build.Desktopfile)
        self._addBuildAction('Doc', build.Doc)
        self._addBuildAction('Environment', build.Environment)
        self._addBuildAction('Install', build.Install)
        self._addBuildAction('JavaCompile', build.JavaCompile)
        self._addBuildAction('JavaDoc', build.JavaDoc)
        self._addBuildAction('Link', build.Link)
        self._addBuildAction('Make', build.Make)
        self._addBuildAction('MakeDirs', build.MakeDirs)
        self._addBuildAction('MakeInstall', build.MakeInstall)
        self._addBuildAction('MakeParallelSubdir', build.MakeParallelSubdir)
        self._addBuildAction('MakePathsInstall', build.MakePathsInstall)
        self._addBuildAction('ManualConfigure', build.ManualConfigure)
        self._addBuildAction('Move', build.Move)
        self._addBuildAction('PythonSetup', build.PythonSetup)
        self._addBuildAction('Remove', build.Remove)
        self._addBuildAction('Replace', build.Replace)
        self._addBuildAction('Run', build.Run)
        self._addBuildAction('SetModes', build.SetModes)
        self._addBuildAction('SGMLCatalogEntry', build.SGMLCatalogEntry)
        self._addBuildAction('Symlink', build.Symlink)
        self._addBuildAction('XInetdService', build.XInetdService)
        self._addBuildAction('XMLCatalogEntry', build.XMLCatalogEntry)

        self._addSourceAction('addArchive', source.addArchive)
        self._addSourceAction('addAction', source.addAction)
        self._addSourceAction('addPatch', source.addPatch)
        self._addSourceAction('addSource', source.addSource)
