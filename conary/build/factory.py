#
# Copyright (c) 2008-2009 rPath, Inc.
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

from conary.build.packagerecipe import AbstractPackageRecipe
from conary.build import defaultrecipes

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

