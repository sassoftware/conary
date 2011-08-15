#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


from conary.build.packagerecipe import AbstractPackageRecipe, _recipeHelper
from conary.build import defaultrecipes
from conary.build.recipe import Recipe, RECIPE_TYPE_INFO
from conary.build import policy
from conary.build import usergroup

class UserGroupInfoRecipe(AbstractPackageRecipe):
    _recipeType = RECIPE_TYPE_INFO
    internalAbstractBaseClass = 1
    basePolicyClass = policy.UserGroupBasePolicy
    abstractBaseClass = False

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
        klass = self._getParentClass('AbstractPackageRecipe')
        if self.abstractBaseClass:
            return klass._loadSourceActions(self, *args, **kwargs)

    def loadPolicy(self):
        klass = self._getParentClass('AbstractPackageRecipe')
        if self.abstractBaseClass:
            self.basePolicyClass = policy.BasePolicy
            internalPolicyModules = None
        else:
            internalPolicyModules = ('infopolicy',)
        return klass.loadPolicy(self,
                                internalPolicyModules = internalPolicyModules)

exec defaultrecipes.BaseRequiresRecipe
exec defaultrecipes.UserInfoRecipe
exec defaultrecipes.GroupInfoRecipe
