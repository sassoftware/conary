# Copyright (c) 2006-2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.

from conary import files, trove, versions
from conary import errors as conaryerrors
from conary.build import build, source
from conary.build import errors as builderrors
from conary.build.packagerecipe import _AbstractPackageRecipe
from conary.local import update
from conary.lib import log, util
from conary.repository import changeset, filecontents

class DerivedPackageRecipe(_AbstractPackageRecipe):

    internalAbstractBaseClass = 1
    _isDerived = True
    parentVersion = None

    def _expandChangeset(self):
        destdir = self.macros.destdir

        delayedRestores = {}
        ptrMap = {}
        byDefault = {}

        fileList = []
        linkGroups = {}
        linkGroupFirstPath = {}
        # sort the files by pathId,fileId
        for trvCs in self.cs.iterNewTroveList():
            trv = trove.Trove(trvCs)

            # these should all be the same anyway
            flavor = trv.getFlavor().copy()
            name = trv.getName()
            self._componentReqs[name] = trv.getRequires().copy()
            self._componentProvs[name] = trv.getProvides().copy()

            for pathId, path, fileId, version in trv.iterFileList():
                if path != self.macros.buildlogpath:
                    fileList.append((pathId, fileId, path, name))

            if trv.isCollection():
                # gather up existing byDefault status
                # from (component, byDefault) tuples
                byDefault.update(dict(
                    [(x[0][0], x[1]) for x in trv.iterTroveListInfo()]))

        fileList.sort()

        for pathId, fileId, path, troveName in fileList:
            fileCs = self.cs.getFileChange(None, fileId)
            fileObj = files.ThawFile(fileCs, pathId)
            self._derivedFiles[path] = fileObj.inode.mtime()

            flavor -= fileObj.flavor()
            self._componentReqs[troveName] -= fileObj.requires()
            self._componentProvs[troveName] -= fileObj.requires()

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

            if isinstance(fileObj, files.DeviceFile):
                self.MakeDevices(path, fileObj.lsTag,
                                 fileObj.devt.major(), fileObj.devt.minor(),
                                 fileObj.inode.owner(), fileObj.inode.group(),
                                 fileObj.inode.perms())
            else:
                if fileObj.hasContents:
                    linkGroup = fileObj.linkGroup()
                    if linkGroup:
                        l = linkGroups.setdefault(linkGroup, [])
                        l.append(path)

                    (contentType, contents) = \
                                    self.cs.getFileContents(pathId, fileId)
                    if contentType == changeset.ChangedFileTypes.ptr:
                        targetPtrId = contents.get().read()
                        l = delayedRestores.setdefault(targetPtrId, [])
                        l.append((fileObj, path))
                        continue
                    elif linkGroup and not linkGroup in linkGroupFirstPath:
                        # only non-delayed restores can be initial target
                        linkGroupFirstPath[linkGroup] = path

                    assert(contentType == changeset.ChangedFileTypes.file)
                else:
                    contents = None

                ptrId = pathId + fileId
                if pathId in delayedRestores:
                    ptrMap[pathId] = path
                elif ptrId in delayedRestores:
                    ptrMap[ptrId] = path

                fileObj.restore(contents, destdir, destdir + path)

            if isinstance(fileObj, files.Directory):
                # remember to include this directory in the derived package
                self.ExcludeDirectories(exceptions = path)

        for targetPtrId in delayedRestores:
            for fileObj, targetPath in delayedRestores[targetPtrId]:
                sourcePath = ptrMap[targetPtrId]
                fileObj.restore(
                    filecontents.FromFilesystem(destdir + sourcePath),
                    destdir, destdir + targetPath)

        # we do not have to worry about cross-device hardlinks in destdir
        for linkGroup in linkGroups:
            for path in linkGroups[linkGroup]:
                initialPath = linkGroupFirstPath[linkGroup]
                if path == initialPath:
                    continue
                util.createLink(destdir + initialPath, destdir + path)

        self.useFlags = flavor

        self.setByDefaultOn(set(x for x in byDefault if byDefault[x]))
        self.setByDefaultOff(set(x for x in byDefault if not byDefault[x]))

    def unpackSources(self, builddir, destdir, resume=None,
                      downloadOnly=False):
        if self.parentVersion:
            try:
                parentRevision = versions.Revision(self.parentVersion)
            except conaryerrors.ParseError, e:
                raise builderrors.RecipeFileError(
                            'Cannot parse parentVersion %s: %s',
                                    self.parentRevision, str(e))
        else:
            parentRevision = None

        if not self.sourceVersion.isShadow():
            raise builderrors.RecipeFileError(
                    "only shadowed sources can be derived packages")

        if parentRevision and \
                self.sourceVersion.trailingRevision().getVersion() != \
                                                parentRevision.getVersion():
            raise builderrors.RecipeFileError(
                    "parentRevision must have the same upstream version as the "
                    "derived package recipe")

        # find all the flavors of the parent
        parentBranch = self.sourceVersion.branch().parentBranch()

        if parentRevision:
            parentVersion = parentBranch.createVersion(parentRevision)

            d = self.repos.getTroveVersionFlavors({ self.name :
                                { parentVersion : [ None ] } } )
            if self.name not in d:
                raise builderrors.RecipeFileError(
                        'Version %s of %s not found'
                                    % (parentVersion, self.name) )
        else:
            d = self.repos.getTroveLeavesByBranch(
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
                    'No flavors of %s=%s found for build flavor %s',
                    self.name, parentVersion, self.cfg.buildFlavor)
        elif len(bestFlavor[1]) > 1:
            raise builderrors.RecipeFileError(
                    'Multiple flavors of %s=%s match build flavor %s',
                    self.name, parentVersion, self.cfg.buildFlavor)

        parentFlavor = bestFlavor[1][0]

        log.info('deriving from %s=%s[%s]', self.name, parentVersion,
                 parentFlavor)

        # Fetch all binaries built from this source
        binaries = self.repos.getTrovesBySource(self.name + ':source',
                parentVersion.getSourceVersion())

        # Filter out older ones
        binaries = [ x for x in binaries \
                        if (x[1], x[2]) == (parentVersion, parentFlavor) ]

        # Build trove spec
        troveSpec = [ (x[0], (None, None), (x[1], x[2]), True)
                        for x in binaries ]

        self.cs = self.repos.createChangeSet(troveSpec, recurse = False)
        self.addLoadedTroves([
            (x.getName(), x.getNewVersion(), x.getNewFlavor()) for x
            in self.cs.iterNewTroveList() ])

        self._expandChangeset()

        _AbstractPackageRecipe.unpackSources(self, builddir, destdir,
                                             resume = resume,
                                             downloadOnly = downloadOnly)

    def loadPolicy(self):
        return _AbstractPackageRecipe.loadPolicy(self,
                                internalPolicyModules = ( 'derivedpolicy', ) )

    def __init__(self, cfg, laReposCache, srcDirs, extraMacros={},
                 crossCompile=None, lightInstance=False):
        _AbstractPackageRecipe.__init__(self, cfg, laReposCache, srcDirs,
                                        extraMacros = extraMacros,
                                        crossCompile = crossCompile,
                                        lightInstance = lightInstance)

        log.info('Warning: Derived packages are experimental and subject to change')

        self.repos = laReposCache.repos

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
