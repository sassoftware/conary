# Copyright (c) 2006,2007 rPath, Inc.
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

from conary.build import action, build, macros
from conary.build import errors as builderrors
from conary.build.packagerecipe import _AbstractPackageRecipe
from conary.lib import log

class DerivedPackageRecipe(_AbstractPackageRecipe):

    internalAbstractBaseClass = 1
    parentVersion = None

    def updateTroves(self, troves):
        for trv in troves:
            rmvList = []
            for (pathId, path, fileId, version) in trv.iterFileList():
                if path in self.removedPaths:
                    rmvList.append(pathId)

            for pathId in rmvList:
                trv.removeFile(pathId)

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

        if not self.sourceVersion.hasParentVersion():
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

        import epdb
        epdb.st()
        cs = self.repos.createChangeSet(
                [ (recipeClass.name, (None, None),
                  (parentVersion, parentFlavor), True) ], recurse = True )

        _AbstractPackageRecipe.unpackSources(self, builddir, destdir,
                                             resume = resume,
                                             downloadOnly = downloadOnly)

    def __init__(self, cfg, laReposCache, srcDirs, extraMacros={},
                 crossCompile=None, lightInstance=False):
        _AbstractPackageRecipe.__init__(self, cfg, laReposCache, srcDirs,
                                        extraMacros = extraMacros,
                                        crossCompile = crossCompile,
                                        lightInstance = lightInstance)
        self.repos = laReposCache.repos

        self._addBuildAction('Remove', build.Remove)
        self._addBuildAction('Install', Install)
