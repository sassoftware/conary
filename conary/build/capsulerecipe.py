#
# Copyright (c) 2009 rPath, Inc.
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

    def loadSourceActions(self):
        self._loadSourceActions(lambda item: item._packageAction is True)



exec defaultrecipes.CapsuleRecipe
