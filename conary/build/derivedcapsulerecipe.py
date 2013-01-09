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
import itertools
import shutil
from conary import versions
from conary import errors as conaryerrors
from conary import trove
from conary import files
from conary import rpmhelper
from conary.build import defaultrecipes
from conary.build import build, source
from conary.build import errors as builderrors
from conary.build.packagerecipe import BaseRequiresRecipe
from conary.build.capsulerecipe import AbstractCapsuleRecipe
from conary.build.derivedrecipe import DerivedChangesetExploder
from conary.repository import changeset
from conary.lib import log, util

class AbstractDerivedCapsuleRecipe(AbstractCapsuleRecipe):

    internalAbstractBaseClass = 1
    internalPolicyModules = ('packagepolicy', 'capsulepolicy',
                             'derivedcapsulepolicy',)
    _isDerived = True
    parentVersion = None

    def _expandChangeset(self, cs):
        exploder = DerivedChangesetExploder(self, cs, self.macros.destdir)

        self.useFlags = exploder.troveFlavor

        self.setByDefaultOn(set(x for x in exploder.byDefault
                                                if exploder.byDefault[x]))
        self.setByDefaultOff(set(x for x in exploder.byDefault
                                                if not exploder.byDefault[x]))
        return exploder

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

        self.setDerivedFrom([
            (x.getName(), x.getNewVersion(), x.getNewFlavor()) for x
            in cs.iterNewTroveList() ])

        self.exploder = self._expandChangeset(cs)
        self.cs = cs

        # register any capsules in the changeset
        for trvCs in cs.iterNewTroveList():
            assert(not trvCs.getOldVersion())
            trv = trove.Trove(trvCs)
            for pathId, path, fileId, version in trv.iterFileList(
                capsules = True, members = False):
                if (trv.troveInfo.capsule.type() ==
                    trove._TROVECAPSULE_TYPE_RPM):


                    capFileObj = self.exploder.rpmFileObj[fileId]
                    capFileObj.seek(0)
                    h = rpmhelper.RpmHeader(capFileObj)
                    capFileObj.seek(0)

                    capDir =  '/'.join((
                            os.path.dirname(self.macros.destdir),
                            '_CAPSULES_'))
                    util.mkdirChain(capDir)
                    capPath = '/'.join((capDir,
                                            h[rpmhelper.NAME] + '.rpm'))
                    outFile = open(capPath,'w')
                    ret = shutil.copyfileobj(capFileObj, outFile)
                    outFile.close()

                    self._addCapsule(capPath, trv.troveInfo.capsule.type(),
                                     trv.name())

                    fileData = list(itertools.izip(h[rpmhelper.OLDFILENAMES],
                                    h[rpmhelper.FILEUSERNAME],
                                    h[rpmhelper.FILEGROUPNAME],
                                    h[rpmhelper.FILEMODES],
                                    h[rpmhelper.FILESIZES],
                                    h[rpmhelper.FILERDEVS],
                                    h[rpmhelper.FILEFLAGS],
                                                    ))
                    self._setPathInfoForCapsule(capPath, fileData, self.name + '.'
                                                + trv.troveInfo.capsule.type())
                    for pathId, path, fileId, version in trv.iterFileList(
                        capsules = False, members = True):
                        fc = cs.getFileChange(None,fileId)
                        fileObj = files.ThawFile(fc, pathId)
                        try:
                            self.exploder.handleFileAttributes(trv, fileObj,
                                                               path)
                            self.exploder.handleFileMode(trv, fileObj, path,
                                                         self.macros.destdir)
                        except OSError, e:
                            if fileObj.flags.isInitialContents():
                                pass
                            else:
                                raise e
                elif trv.troveInfo.capsule.type():
                    raise builderrors.CookError("Derived Packages with %s type "
                        "capsule is unsupported." %trv.troveInfo.capsule.type())

        klass = self._getParentClass('AbstractCapsuleRecipe')
        klass.unpackSources(self, resume = resume,
                                             downloadOnly = downloadOnly)

    def __init__(self, cfg, laReposCache, srcDirs, extraMacros={},
                 crossCompile=None, lightInstance=False):
        klass = self._getParentClass('AbstractCapsuleRecipe')
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

        self._addBuildAction('Replace', build.Replace)
        self._addBuildAction('Run', build.Run)
        self._addBuildAction('SetModes', build.SetModes)
        self._addBuildAction('SGMLCatalogEntry', build.SGMLCatalogEntry)
        self._addBuildAction('Symlink', build.Symlink)
        self._addBuildAction('XInetdService', build.XInetdService)
        self._addBuildAction('XMLCatalogEntry', build.XMLCatalogEntry)

exec defaultrecipes.DerivedCapsuleRecipe
