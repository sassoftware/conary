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


import os

from conary import files
from conary.build import defaultrecipes
from conary.build.packagerecipe import AbstractPackageRecipe, BaseRequiresRecipe
from conary.repository import changeset

class DerivedChangesetExploder(changeset.ChangesetExploder):

    def __init__(self, recipe, cs, destDir):
        self.byDefault = {}
        self.troveFlavor = None
        self.recipe = recipe
        changeset.ChangesetExploder.__init__(self, cs, destDir)

    def installingTrove(self, trv):
        if self.troveFlavor is None:
            self.troveFlavor = trv.getFlavor().copy()
        else:
            assert(self.troveFlavor == trv.getFlavor())

        name = trv.getName()
        self.recipe._componentReqs[name] = trv.getRequires().copy()
        self.recipe._componentProvs[name] = trv.getProvides().copy()

        if trv.isCollection():
            # gather up existing byDefault status
            # from (component, byDefault) tuples
            self.byDefault.update(dict(
                [(x[0][0], x[1]) for x in trv.iterTroveListInfo()]))

        changeset.ChangesetExploder.installingTrove(self, trv)

    def handleFileAttributes(self, trv, fileObj, path):
        self.troveFlavor -= fileObj.flavor()
        # Config vs. InitialContents etc. might be change in derived pkg
        # Set defaults here, and they can be overridden with
        # "exceptions = " later
        if fileObj.flags.isConfig():
            self.recipe.Config(path, allowUnusedFilters = True)
        elif fileObj.flags.isInitialContents():
            self.recipe.InitialContents(path, allowUnusedFilters = True)
        elif fileObj.flags.isTransient():
            self.recipe.Transient(path, allowUnusedFilters = True)

        if isinstance(fileObj, files.SymbolicLink):
            # mtime for symlinks is meaningless, we have to record the
            # target of the symlink instead
            self.recipe._derivedFiles[path] = fileObj.target()
        else:
            self.recipe._derivedFiles[path] = fileObj.inode.mtime()

        self.recipe._componentReqs[trv.getName()] -= fileObj.requires()
        self.recipe._componentProvs[trv.getName()] -= fileObj.requires()


    def handleFileMode(self, trv, fileObj, path, destdir):
        if isinstance(fileObj, files.SymbolicLink):
            return

        fullPath = '/'.join((destdir, path))
        # Do not restore setuid/setgid bits into the filesystem.
        # Call internal policy with path; do not use the SetModes
        # build action because that will override anything
        # called via setup, since setup has already been
        # invoked.  However, SetModes as invoked from setup
        # will call setModes after this call, which will
        # allow modifying the mode in the derived package.
        mode = fileObj.inode.perms()
        os.chmod(fullPath, mode & 01777)
        if fileObj.inode.perms() & 06000 != 0:
            self.recipe.setModes(path, sidbits=(mode & 06000))

        if isinstance(fileObj, files.Directory):
            if (fileObj.inode.perms() & 0700) != 0700:
                os.chmod(fullPath, (mode & 01777) | 0700)
                self.recipe.setModes(path, userbits=(mode & 0700))
            # remember to include this directory in the derived package even
            # if the directory is empty
            self.recipe.ExcludeDirectories(exceptions=path,
                allowUnusedFilters=True)

    def restoreFile(self, trv, fileObj, contents, destdir, path):
        self.handleFileAttributes(trv, fileObj, path)
        if isinstance(fileObj, files.DeviceFile):
            self.recipe.MakeDevices(path, fileObj.lsTag,
                               fileObj.devt.major(), fileObj.devt.minor(),
                               fileObj.inode.owner(), fileObj.inode.group(),
                               fileObj.inode.perms())
        else:
            changeset.ChangesetExploder.restoreFile(self, trv, fileObj,
                                                    contents, destdir, path)
            self.handleFileMode(trv, fileObj, path, destdir)

    def restoreLink(self, trv, fileObj, destdir, sourcePath, targetPath):
        self.handleFileAttributes(trv, fileObj, targetPath)
        changeset.ChangesetExploder.restoreLink(self, trv, fileObj, destdir,
                                                sourcePath, targetPath)

    def installFile(self, trv, path, fileObj):
        if path == self.recipe.macros.buildlogpath:
            return False

        return changeset.ChangesetExploder.installFile(self, trv, path, fileObj)

from conary import versions
from conary import errors as conaryerrors
from conary.build import build, source
from conary.build import errors as builderrors
from conary.lib import log

class AbstractDerivedPackageRecipe(AbstractPackageRecipe):

    internalAbstractBaseClass = 1
    _isDerived = True
    parentVersion = None

    def _expandChangeset(self, cs):
        exploder = DerivedChangesetExploder(self, cs, self.macros.destdir)

        self.useFlags = exploder.troveFlavor

        self.setByDefaultOn(set(x for x in exploder.byDefault
                                                if exploder.byDefault[x]))
        self.setByDefaultOff(set(x for x in exploder.byDefault
                                                if not exploder.byDefault[x]))

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
        elif self.sourceVersion:
            sourceRevision = self.sourceVersion.trailingRevision()
            d = repos.getTroveVersionsByLabel(
                    { self.name : { parentBranch.label() : [ None ] } } )

            if self.name not in d or not d[self.name]:
                raise builderrors.RecipeFileError(
                    'No versions of %s found on label %s' %
                            (self.name, parentBranch.label()))

            versionList = reversed(sorted(d[self.name]))
            match = False
            for version in versionList:
                # This is a really complicated way of checking that
                # version is an ancestor of sourceRevision
                sr = sourceRevision.copy()
                sr.getSourceCount().truncateShadowCount(
                        version.trailingRevision().shadowCount())

                if (version.getSourceVersion().trailingRevision() == sr):
                    match = True
                    break

            if not match:
                raise builderrors.RecipeFileError(
                    'No packages of %s of source revision %s found on label %s'
                        % (self.name, sourceRevision, parentBranch.label()))

            parentVersion = version
        else:
            parentVersion = parentBranch
        try:
            troveList = repos.findTrove(None,
                                   (self.name, parentVersion, self._buildFlavor))
        except conaryerrors.TroveNotFound, err:
            raise builderrors.RecipeFileError('Could not find package to derive from for this flavor: ' + str(err))
        if len(troveList) > 1:
            raise builderrors.RecipeFileError(
                    'Multiple flavors of %s=%s match build flavor %s' \
                    % (self.name, parentVersion, self.cfg.buildFlavor))
        parentFlavor = troveList[0][2]
        parentVersion = troveList[0][1]

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

        # Capsules are not supported in derrived packages
        for trv in cs.iterNewTroveList():
            if trv.hasCapsule():
                raise builderrors.RecipeFileError(
                    'DerivedRecipe cannot be used with %s, '
                    'it was created with a CapsuleRecipe.  Please use'
                    'a DerivedCapsuleRecipe instead.'
                    % trv.name())

        self.setDerivedFrom([
            (x.getName(), x.getNewVersion(), x.getNewFlavor()) for x
            in cs.iterNewTroveList() ])

        self._expandChangeset(cs)
        self.cs = cs

        klass = self._getParentClass('AbstractPackageRecipe')
        klass.unpackSources(self, resume = resume,
                                             downloadOnly = downloadOnly)

    def loadPolicy(self):
        klass = self._getParentClass('AbstractPackageRecipe')
        return klass.loadPolicy(self,
                                internalPolicyModules = ( 'destdirpolicy', 'packagepolicy', 'derivedpolicy', ) )

    def __init__(self, cfg, laReposCache, srcDirs, extraMacros={},
                 crossCompile=None, lightInstance=False):
        klass = self._getParentClass('AbstractPackageRecipe')
        klass.__init__(self, cfg, laReposCache, srcDirs,
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

exec defaultrecipes.DerivedPackageRecipe
