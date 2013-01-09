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
