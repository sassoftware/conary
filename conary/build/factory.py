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
