#
# Copyright (c) 2005 rPath, Inc.
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
#

from conary.build.packagerecipe import _AbstractPackageRecipe, _recipeHelper
from conary.build.recipe import RECIPE_TYPE_INFO

from conary.build import buildpackage
from conary.build import usergroup
from conary.deps import deps

class UserGroupInfoRecipe(_AbstractPackageRecipe):
    _recipeType = RECIPE_TYPE_INFO
    abstraceBaseClass = 1

    def __init__(self, cfg, laReposCache, srcdirs, extraMacros={}, 
                 crossCompile=None):
        _AbstractPackageRecipe.__init__(self, cfg, laReposCache, srcdirs, extraMacros, crossCompile)
        self.requires = []
        self.infofilename = None
        self.realfilename = None

    def getPackages(self):
        # we do not package up build logs for info-* packages
        self._autoCreatedFileCount -= 1
        comp = buildpackage.BuildComponent(
            'info-%s:%s' %(self.infoname, self.type), self)
        f = comp.addFile(self.infofilename, self.realfilename)
        f.tags.set("%s-info" %self.type)
        self.addProvides(f)
        self.addRequires(f)
        comp.provides.union(f.provides())
        comp.requires.union(f.requires())
        return [comp]

    def loadPolicy(self):
        return []

    def doProcess(self, bucket):
        pass

    def addProvides(self, f):
        pass

    def addRequires(self, f):
        if not self.requires:
            return
        depSet = deps.DependencySet()
        for info, type in self.requires:
            if type == 'user':
                depClass = deps.UserInfoDependencies
            else:
                depClass = deps.GroupInfoDependencies
            depSet.addDep(depClass, deps.Dependency(info, []))
        f.requires.set(depSet)

    def requiresUser(self, user):
        self.requires.append((user, 'user'))

    def requiresGroup(self, group):
        self.requires.append((group, 'group'))

    def __getattr__(self, name):
        if not name.startswith('_'):
	    if name in usergroup.__dict__:
		return _recipeHelper(self._build, self,
                                     usergroup.__dict__[name])
        if name in self.__dict__:
            return self.__dict__[name]
        raise AttributeError, name

class UserInfoRecipe(UserGroupInfoRecipe):
    type = 'user'
    abstractBaseClass = 1

    def addProvides(self, f):
        depSet = deps.DependencySet()
        depSet.addDep(deps.UserInfoDependencies,
                      deps.Dependency(self.infoname, []))
        depSet.addDep(deps.GroupInfoDependencies,
                      deps.Dependency(self.groupname, []))
        f.provides.set(depSet)

class GroupInfoRecipe(UserGroupInfoRecipe):
    type = 'group'
    abstractBaseClass = 1

    def addProvides(self, f):
        depSet = deps.DependencySet()
        depSet.addDep(deps.GroupInfoDependencies,
                      deps.Dependency(self.infoname, []))
        f.provides.set(depSet)
