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
from conary.build.recipe import RECIPE_TYPE_INFO

from conary.build import policy
from conary.build import usergroup

class UserGroupInfoRecipe(AbstractPackageRecipe):
    _recipeType = RECIPE_TYPE_INFO
    internalAbstractBaseClass = 1
    # we need to add this line because AbstractPackageRecipe
    # isn't copied in
    buildRequires = AbstractPackageRecipe.buildRequires[:]
    basePolicyClass = policy.UserGroupBasePolicy

    def __getattr__(self, name):
        if not name.startswith('_'):
	    if name in usergroup.__dict__:
		return _recipeHelper(self._build, self,
                                     usergroup.__dict__[name])
        if name in self.__dict__:
            return self.__dict__[name]
        return AbstractPackageRecipe.__getattr__(self, name)

    def _loadSourceActions(self, *args, **kwargs):
        pass

    def loadPolicy(self):
        return AbstractPackageRecipe.loadPolicy(self,
                                internalPolicyModules = ( 'infopolicy', ) )

class UserInfoRecipe(UserGroupInfoRecipe):
    """
    NAME
    ====
    B{C{UserInfoRecipe}} - Build user info pacakges

    SYNOPSIS
    ========

    C{UserInfoRecipe} is used to create packages that define a system user

    DESCRIPTION
    ===========

    The C{UserInfoRecipe} class provides an interface to define a system
    user through the C{r.User} method.  The C{r.User} method is also
    available in the C{PackageRecipe} class.

    EXAMPLE
    =======
    A sample class that uses C{UserInfoRecipe} to define a user

        class ExamplePackage(UserInfoRecipe):
            name = 'info-example'
            version = '1.0'

            def setup(r):
                r.User('example', 500)
    """
    internalAbstractBaseClass = 1

class GroupInfoRecipe(UserGroupInfoRecipe):
    """
    NAME
    ====
    B{C{GroupInfoRecipe}} - Build group info pacakges

    SYNOPSIS
    ========

    C{GroupInfoRecipe} is used to create packages that define a system group

    DESCRIPTION
    ===========

    The C{GroupInfoRecipe} class provides an interface to define a system
    group through the C{r.Group} method.  The C{r.Group} method is also
    available in the C{PackageRecipe} class.

    The C{GroupInfoRecipe} class should be used if a system group must exist
    independently from any system users.

    EXAMPLE
    =======
    A sample class that uses C{GroupInfoRecipe} to define a group

        class ExamplePackage(GroupInfoRecipe):
            name = 'info-example'
            version = '1.0'

            def setup(r):
                r.Group('example', 500)
    """
    internalAbstractBaseClass = 1
