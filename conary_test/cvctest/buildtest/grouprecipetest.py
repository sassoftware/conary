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


import os
import shutil
import tempfile

from conary.build import loadrecipe, use



#test
from conary_test import recipes
from conary_test import rephelp


class GroupRecipeTest(rephelp.RepositoryHelper):
    def _loadRecipe(self, recipe=recipes.testGroup1, group_name='group-test'):
        '''Return a recipe class from the given recipe blob'''

        temp_dir = tempfile.mkdtemp(prefix='conary-test-')
        try:
            recipe_file = os.path.join(temp_dir, group_name + '.recipe')
            open(recipe_file, 'w').write(recipe)
            loader = loadrecipe.RecipeLoader(recipe_file, self.cfg)
        finally:
            shutil.rmtree(temp_dir)

        return loader.getRecipe()

    def testMacroOverrides(self):
        self.overrideBuildFlavor('is:x86(!i686,!i586,i486)')
        use.setBuildFlagsFromFlavor('group-test', self.cfg.buildFlavor)

        recipeClass = self._loadRecipe()
        dummy = recipeClass(None, self.cfg, None, None, None,
            lightInstance=True)
        self.assertEqual(dummy.macros.dummyMacro, 'right')

    def testBasicMacros(self):
        recipeClass = self._loadRecipe()

        # This should override the value from extraMacros
        self.cfg.configLine('macros thing value')
        # And this should override the default value
        self.cfg.configLine('macros bindir /binaries')

        recipeObj = recipeClass(None, self.cfg, None, None, None,
            extraMacros={'thing': 'wrong'}, lightInstance=True)
        self.assertEqual(recipeObj.macros.name, 'group-test')
        self.assertEqual(recipeObj.macros.version, '1.0')
        self.assertEqual(recipeObj.macros.bindir, '/binaries')
        self.assertEqual(recipeObj.macros.thing, 'value')

    def testGroupFlags(self):
        recipeClass = self._loadRecipe(recipe=recipes.testGroup3)
        recipeObj = recipeClass(None, self.cfg, None, None, None,
            lightInstance=True)
        recipeObj.setup()

        self.assertEqual(True,
            recipeObj.groups['group-test'].checkPathConflicts)
        self.assertEqual(False,
            recipeObj.groups['group-test2'].checkPathConflicts)
        self.assertEqual(True,
            recipeObj.groups['group-test3'].checkPathConflicts)
