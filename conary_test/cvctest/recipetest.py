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
import tempfile

import unittest
from conary_test import rephelp

from conary.build import macros
from conary.build.loadrecipe import RecipeLoader
from conary.build.errors import RecipeFileError, CookError

class MacroTest(unittest.TestCase):
    def test1(self):
        """simple macro expansion"""
        self.macros = macros.Macros()
        self.macros.update({'foo': 'bar'})
        self.macros['foo'] = 'bar1 %(foo)s'
        self.assertEqual(self.macros['foo'], 'bar1 bar')

    def test2(self):
        """test setting a new value for a macro which references the old
        value"""
        self.macros = macros.Macros()
        self.macros.update({'foo': 'bar1',
                            'baz': 'fred'})
        self.macros['foo'] = 'bar2 %(baz)s %(foo)s'
        self.assertEqual(self.macros['foo'], 'bar2 fred bar1')
        self.macros.update({'baz': '%(baz)s asdf'})
        self.assertEqual(self.macros.baz, 'fred asdf')

    def test3(self):
        """test setting a new value for a macro, verify that only
        the macro being set is expanded, any other macros expanded
        later"""
        self.macros = macros.Macros()
        self.macros.update({'foo': 'bar1',
                            'baz': 'fred'})
        self.macros['foo'] = 'bar2 %(baz)s %(foo)s'
        self.assertEqual(self.macros['foo'], 'bar2 fred bar1')
        self.macros['baz'] = 'wilma'
        self.assertEqual(self.macros['foo'], 'bar2 wilma bar1')

    def disabletest4(self):
        """verify that a recursive macro results in a RuntimeError"""
        self.macros = macros.Macros()
        self.macros.update({'foo': 'bar1',
                            'baz': '%(foo)/fred'})
        self.macros['foo'] = 'bar2 %(baz)s'
        try:
            self.macros['baz']
        except RuntimeError, exc:
            if str(exc) != 'maximum recursion depth exceeded':
                self.fail('expected maximum recursion RuntimeError when expanding macro loop')
        else:
            self.fail('expected RuntimeError when expanding macro loop')

class RecipeTest(rephelp.RepositoryHelper):
    def testBadActionFormat(self):
        """verify that a recipe that includes an action with a bad format
        string will cause an exception before the build process begins
        (after setup)"""
        recipestr = """
class BadFormat(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Install("%(foo)/bar", "baz")
"""
        self.logFilter.add()
        self.assertRaises(CookError, self.buildRecipe, recipestr, 
                          "BadFormat", prep=True)
        self.logFilter.remove()
        self.logFilter.compare((
            'error: invalid macro substitution in "%(foo)/bar", missing "s"?',
        ))


    def testBadVersionNumber(self):
        """
        Verify that version numbers with - in them are disallowed
        """
        recipestr1 = """
class BadVersion(PackageRecipe):
    name = 'test'
    version = '0-0'
    clearBuildReqs()
    
    def setup(r):
        pass
"""

        self.assertRaises(RecipeFileError, self.buildRecipe, recipestr1, "BadVersion")

    def testNoVersion(self):
        """
        testNoVersion : Verify friendly error message when version is forgotten
        """
        recipestr1 = """
class NoVersion(PackageRecipe):
    name = 'test'
    clearBuildReqs()
    
    def setup(r):
        pass
"""
        d = tempfile.mkdtemp(dir=self.workDir)
        os.chdir(d)
        self.writeFile('test.recipe', recipestr1)
        try:
            RecipeLoader(d + '/test.recipe', self.cfg)
        except RecipeFileError, m:
            errstr = ("Recipe in file/component 'test' did not contain "
                        "both a name and a version attribute.")
            assert(m.args[0].endswith(errstr))

    def testEmptyReleaseString(self):
        recipe = """
class Empty(PackageRecipe):
    name = 'empty'
    version = ''
"""
        repos = self.openRepository()
        origDir = os.getcwd()
        try:
            os.chdir(self.workDir)
            self.newpkg('empty')
            os.chdir('empty')
            self.writeFile('empty.recipe', recipe)
            self.logFilter.add()
            try:
                self.cookItem(repos, self.cfg, 'empty.recipe')
                self.fail("CookError not raised")
            except CookError, e:
                self.assertEqual(str(e), "unable to load recipe file "
                    "%s/empty.recipe:\nempty release string" % os.getcwd())
        finally:
            os.chdir(origDir)


    def testBadNames(self):
        """
        testBadNames : Verify error message if package name starts with a non-letter
        """
        recipestr1 = """
class BadName(PackageRecipe):
    name = '+test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        pass
"""
        d = tempfile.mkdtemp(dir=self.workDir)
        os.chdir(d)
        self.writeFile('test.recipe', recipestr1)
        try:
            RecipeLoader(d + '/test.recipe', self.cfg)
        except RecipeFileError, m:
            errstr = ('Error in recipe file \"test.recipe\": package '
                      'name must start with an ascii letter or digit.')
            assert(m.args[0].endswith(errstr))

    def testNoClass(self):
        """
        testNoClass : Verify friendly error message when no class in file
        """
        recipestr1 = ""
        d = tempfile.mkdtemp(dir=self.workDir)
        os.chdir(d)
        self.writeFile('test.recipe', recipestr1)
        try:
            RecipeLoader(d + '/test.recipe', self.cfg)
        except RecipeFileError, m:
            errstr = ("file/component 'test' did not contain a valid recipe")
            assert(m.args[0].endswith(errstr))

    def testResume(self):
        # CNY-1684
        recipestr = r"""
class Test1(PackageRecipe):
    name = "test1"
    version = "1.0"
    clearBuildReqs()

    def setup(r):
        r.Create("/usr/share/foo", contents="foo\n")
        r.Create("/usr/share/bar", contents="foo\n")
        r.Create("/usr/share/baz", contents="foo\n")
"""
        self.buildRecipe(recipestr, "Test1", prep=True)
        self.buildRecipe(recipestr, "Test1", resume='7')
