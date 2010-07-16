#
# Copyright (c) 2010 rPath, Inc.
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

from conary.build import defaultrecipes, macros, use
from conary.build.grouprecipe import _BaseGroupRecipe, _SingleGroup
from conary.build.recipe import loadMacros
from conary.deps import deps

class SG(_SingleGroup):

    def __init__(self, *args, **kwargs):
        _SingleGroup.__init__(self, *args, **kwargs)
        self.autoResolve = False
        self.checkPathConflicts = False
        self.depCheck = False
        self.imageGroup = False

    def iterAddSpecs(self):
        return []

    def iterAddAllSpecs(self):
        return []

    def iterReplaceSpecs(self):
        return []

    def iterRemoveSpecs(self):
        return []

    def iterDifferenceSpecs(self):
        return []

    def iterNewGroupList(self):
        return []

    def iterNewGroupDifferenceList(self):
        return []

    def iterCopiedFrom(self):
        return []

    def getComponentsToMove(self):
        return []

    def getComponentsToRemove(self):
        return []

    def getCopyComponentMap(self):
        return {}

    def getMoveComponentMap(self):
        return {}

    def getRequires(self):
        return deps.DependencySet()

    def isEmpty(self):
        return not self.troves

class _GroupSetRecipe(_BaseGroupRecipe):

    Flags = use.LocalFlags
    internalAbstractBaseClass = 1

    def __init__(self, repos, cfg, label, flavor, laReposCache, srcdirs=None,
                 extraMacros={}, lightInstance = False):

        klass = self._getParentClass('_BaseGroupRecipe')
        klass.__init__(self, laReposCache = laReposCache,
                       srcdirs = srcdirs,
                       lightInstance = lightInstance,
                       cfg = cfg)

        self.troveSource = repos

        self.labelPath = [ label ]
        self.flavor = flavor
        self.searchSource = self.troveSource
        self.macros = macros.Macros(ignoreUnknown=lightInstance)

        baseMacros = loadMacros(cfg.defaultMacros)
        self.macros.update(baseMacros)
        for key in cfg.macros:
            self.macros._override(key, cfg['macros'][key])
        self.macros.name = self.name
        self.macros.version = self.version
        if '.' in self.version:
            self.macros.major_version = '.'.join(self.version.split('.')[0:2])
        else:
            self.macros.major_version = self.version
        if extraMacros:
            self.macros.update(extraMacros)

        newGroup = SG(self.name)
        self._addGroup(self.name, newGroup)
        self._setDefaultGroup(newGroup)

    def getLabelPath(self):
        return self.labelPath

    def getSearchFlavor(self):
        return self.flavor

    def iterReplaceSpecs(self):
        return []

    def getResolveTroveSpecs(self):
        return []

    def getChildGroups(self, groupName = None):
        return []

    def getGroupMap(self, groupName = None):
        return {}

    def _getSearchSource(self):
        return self.troveSource

    def getSearchPath(self):
        return [ ]

    def add(self, troveSpec):
        from conary.updatecmd import parseTroveSpec
        ts = parseTroveSpec(troveSpec)
        troveInfo = self.troveSource.findTrove(self.labelPath, ts,
                                               defaultFlavor = self.flavor)[0]
        self.defaultGroup.addTrove(troveInfo, True, True, [])

from conary.build.packagerecipe import BaseRequiresRecipe
exec defaultrecipes.GroupSetRecipe
