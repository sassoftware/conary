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
