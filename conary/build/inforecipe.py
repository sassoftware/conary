#
# Copyright (c) 2005-2008 rPath, Inc.
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

from conary.build.packagerecipe import AbstractPackageRecipe, _recipeHelper
from conary.build import defaultrecipes
from conary.build.recipe import RECIPE_TYPE_INFO
from conary.build import policy
from conary.build import usergroup

class UserGroupInfoRecipe(AbstractPackageRecipe):
    _recipeType = RECIPE_TYPE_INFO
    internalAbstractBaseClass = 1
    basePolicyClass = policy.UserGroupBasePolicy

    def __getattr__(self, name):
        if not name.startswith('_'):
            if name in usergroup.__dict__:
                return _recipeHelper(self._build, self,
                                     usergroup.__dict__[name])
        if name in self.__dict__:
            return self.__dict__[name]
        klass = self._getParentClass('AbstractPackageRecipe')
        return klass.__getattr__(self, name)

    def _loadSourceActions(self, *args, **kwargs):
        pass

    def loadPolicy(self):
        klass = self._getParentClass('AbstractPackageRecipe')
        return klass.loadPolicy(self, internalPolicyModules = ('infopolicy',))

exec defaultrecipes.BaseRequiresRecipe
exec defaultrecipes.UserInfoRecipe
exec defaultrecipes.GroupInfoRecipe
