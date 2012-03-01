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
