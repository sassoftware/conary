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


from conary.errors import ParseError
from conary.build.recipe import RECIPE_TYPE_FACTORY
from conary.build.errors import RecipeFileError

class FactoryException(RecipeFileError):

    pass

class Factory:

    internalAbstractBaseClass = True
    _recipeType = RECIPE_TYPE_FACTORY
    _trackedFlags = None

    def __init__(self, packageName, sourceFiles = [], openSourceFileFn = None):
        self.packageName = packageName
        self.sources = sourceFiles
        self._openSourceFileFn = openSourceFileFn

    @classmethod
    def getType(class_):
        return class_._recipeType

    @classmethod
    def validateClass(class_):
        if class_.version == '':
            raise ParseError("empty release string")

    def openSourceFile(self, path):
        return self._openSourceFileFn(path)

FactoryRecipe = '''
from conary.build import recipe
class FactoryRecipe(AbstractPackageRecipe, BaseRequiresRecipe):
    name = '%(name)s'
    version = '%(version)s'
    abstractBaseClass = True

    originalFactoryClass = None
    _sourcePath = None
    _trove = None

    def __init__(r, *arg, **kw):
        assert r.originalFactoryClass is not None,\
            'You must set the originalFactoryClass before creating the FactoryRecipe object'
        AbstractPackageRecipe.__init__(r, *arg, **kw)

    def setup(r):
        pass

    def setupAbstractBaseClass(r):
        AbstractPackageRecipe.setupAbstractBaseClass(r)
        ofc = r.originalFactoryClass
        # getAdditionalSourceFiles has to be a static or class
        # method
        if  hasattr(ofc, "getAdditionalSourceFiles"):
            additionalFiles = ofc.getAdditionalSourceFiles()
            for ent in additionalFiles:
                srcFile, destLoc = ent[:2]
                r.addSource(srcFile, dest = destLoc,
                            package = ':recipe')
'''


def generateFactoryRecipe(class_):
    return FactoryRecipe % dict(name=class_.name, version=class_.version)
