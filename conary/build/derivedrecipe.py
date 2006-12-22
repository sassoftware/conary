# Copyright (c) 2006 rPath, Inc.
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

from conary.build import macros
from conary.build.recipe import Recipe, RECIPE_TYPE_DERIVEDPKG

class DerivedPackageRecipe(Recipe):

    _recipeType = RECIPE_TYPE_DERIVEDPKG
    internalAbstractBaseClass = 1

    def updateTroves(self, troves):
        for trv in troves:
            rmvList = []
            for (pathId, path, fileId, version) in trv.iterFileList():
                if path in self.removedPaths:
                    rmvList.append(pathId)

            for pathId in rmvList:
                trv.removeFile(pathId)

    def remove(self, pattern):
        self.removedPaths.add(pattern)

    def __init__(self, repos, cfg, branch, flavor, extraMacros={}):
        assert(0)
        self.repos = repos
        self.cfg = cfg
        self.branch = branch
        self.flavor = flavor
        self.macros = macros.Macros()
        self.macros.update(extraMacros)
        self.removedPaths = set()
