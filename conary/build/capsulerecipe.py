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


import inspect

from conary.build import action, defaultrecipes

from conary.build.recipe import RECIPE_TYPE_CAPSULE
from conary.build.packagerecipe import BaseRequiresRecipe, AbstractPackageRecipe

class AbstractCapsuleRecipe(AbstractPackageRecipe):
    internalAbstractBaseClass = 1
    internalPolicyModules = ( 'packagepolicy', 'capsulepolicy' )
    _recipeType = RECIPE_TYPE_CAPSULE

    def __init__(self, *args, **kwargs):
        klass = self._getParentClass('AbstractPackageRecipe')
        klass.__init__(self, *args, **kwargs)

        from conary.build import build
        for name, item in build.__dict__.items():
            if inspect.isclass(item) and issubclass(item, action.Action):
                self._addBuildAction(name, item)

        self.capsuleFileSha1s = {}

    def loadSourceActions(self):
        self._loadSourceActions(lambda item: item._packageAction is True)



exec defaultrecipes.CapsuleRecipe
