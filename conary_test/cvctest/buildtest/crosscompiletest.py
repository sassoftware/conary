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


from conary_test import rephelp
from conary.build import errors, packagerecipe
from conary.deps import deps


class CrossCompileReqTest(rephelp.RepositoryHelper):

    def testDefineCrossRequirements(self):
        """
        Verify that different build requirements are found when they are 
        installed 
        """
        self.addComponent('cross:source', '1', [('cross.recipe', crossRecipe)])
        crossRecipeObj = self.loadRecipe('cross',
                                         flavor='cross is: x86 target: x86_64',
                                         init=True)
        assert(crossRecipeObj.buildRequires == 
               ['gcc:devel', 'bar:runtime', 'binutils:devel', 'bar:python'])
        assert(crossRecipeObj.crossRequires == 
               ['oldcross:devel', 'foo:devellib', 'foo:devel'])

        self.addComponent('clearcross:source', '1', [('clearcross.recipe', 
                                                       clearCrossRecipe)])
        crossRecipeObj = self.loadRecipe('clearcross',
                                         flavor='cross is: x86 target: x86_64',
                                         init=True)
        assert(crossRecipeObj.buildRequires
                == ['gcc:devel', 'bar:runtime', 'binutils:devel', 'bar:python'])
        assert(crossRecipeObj.crossRequires ==
                ['bam:runtime', 'bar:devel', 'foo:devellib'])

        self.addComponent('keepreqs:source', '1', [('keepreqs.recipe',
                                                      keepBuildreqsRecipe)])
        crossRecipeObj = self.loadRecipe('keepreqs',
                                         flavor='cross is: x86 target: x86_64',
                                         init=True)
        # foo:Devellib is kept
        assert(crossRecipeObj.buildRequires
                == ['foo:devellib', 'gcc:devel', 'bar:runtime', 
                    'binutils:devel', 'bar:python'])
        assert(crossRecipeObj.crossRequires == ['oldcross:devel', 'foo:devel'])
        self.addComponent('keepreqs:source', '2', [('keepreqs.recipe',
                                                      keepBuildreqsRecipe2)])
        crossRecipeObj = self.loadRecipe('keepreqs',
                                         flavor='cross is: x86 target: x86_64',
                                         init=True)
        assert(crossRecipeObj.buildRequires
                == ['foo:devellib', 'gcc:devel', 'bar:runtime', 
                    'binutils:devel', 'bar:python', 'foo:devel'])
        assert(crossRecipeObj.crossRequires == ['oldcross:devel'])


    def testCheckCrossRequirements(self):
        self.addComponent('cross:source', '1', [('cross.recipe',
                                                 simpleCrossRecipe)])
        crossRecipeObj = self.loadRecipe('cross',
                                         flavor='is: x86 target: x86_64',
                                         init=True)
        crossRecipeObj.macros.sysroot = '/sysroot'
        db = self.openDatabase()
        self.addDbComponent(db, 'oldcross:devel')
        self.addDbComponent(db, 'foo:devel')
        self.logFilter.add()
        self.openDatabase(self.cfg.root + crossRecipeObj.macros.sysroot)
        try:
            crossRecipeObj.checkBuildRequirements(self.cfg,
                                            crossRecipeObj._trove.getVersion())
            assert(0)
        except errors.RecipeDependencyError, err:
            assert(str(err) == "unresolved build dependencies")
            self.logFilter.compare(
            ['error: Could not find the following cross requirements (that must be installed in %s/sysroot) needed to cook this recipe:\n'
            'foo:devel\n'
            'oldcross:devel' % self.cfg.root])

        crossRecipeObj.macros.sysroot = '/opt/cross'
        db = self.openDatabase(root=self.cfg.root + crossRecipeObj.macros.sysroot)
        t1 = self.addDbComponent(db, 'oldcross:devel').getNameVersionFlavor()
        t2 = self.addDbComponent(db, 'foo:devel').getNameVersionFlavor()
        crossRecipeObj.checkBuildRequirements(self.cfg,
                                            crossRecipeObj._trove.getVersion())
        reqMap = dict((x[0], x[1].getNameVersionFlavor()) for x in 
                       crossRecipeObj.crossReqMap.items())
        assert(reqMap  == {'foo:devel' : t2,
                           'oldcross:devel' : t1})
        assert(crossRecipeObj.buildReqMap == {})

class CrossCompileTargetSet(rephelp.RepositoryHelper):

    def testGetCrossCompileSettings(self):
        def _test(flavorStr, expected):
            flavor = deps.parseFlavor(flavorStr, raiseError=True)
            result = packagerecipe.getCrossCompileSettings(flavor)
            if result is None:
                self.assertEquals(expected, None)
                return
            expected = (expected[0], deps.parseFlavor(expected[1],
                                                      raiseError=True),
                        expected[2])
            self.assertEquals(result, expected)

        _test('is:x86', None)
        _test('is:x86 target:x86_64', (None, 'is:x86_64', False))
        _test('cross is:x86 target:x86_64', (None, 'is:x86_64', True))

crossRecipe = """
class CrossRecipe(PackageRecipe):
    name = 'cross'
    version = '1'

    clearBuildReqs()
    crossRequires = ['oldcross:devel']
    buildRequires = ["foo:devel", "bar:runtime", "bar:python",
                     "foo:devellib", "gcc:devel", "binutils:devel"]

    def setup(r):
        r.Create('/tmp')
"""

clearCrossRecipe = """
loadRecipe('cross')
class ClearCrossRecipe(CrossRecipe):
    name = 'clearcross'

    clearCrossReqs()
    clearBuildReqs('foo:devel')
    crossRequires = ['bar:devel', "bam:runtime"]
"""

keepBuildreqsRecipe = """
loadRecipe('cross')
class KeepBuildReqsRecipe(CrossRecipe):
    name = 'keepreqs'
    keepBuildReqs = ["foo:devellib"]
"""

keepBuildreqsRecipe2 = """
loadRecipe('cross')
class KeepBuildReqsRecipe(CrossRecipe):
    name = 'keepreqs'
    version = '2'
    keepBuildReqs = True
"""

simpleCrossRecipe = """
class CrossRecipe(PackageRecipe):
    name = 'cross'
    version = '1'

    clearBuildReqs()
    crossRequires = ['oldcross:devel']
    buildRequires = ["foo:devel"]

    def setup(r):
        r.Create('/tmp')
"""
