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


from testrunner import testhelp

import copy, os
import sys


#conary
from conary.build import loadrecipe,packagerecipe,use,recipe
from conary.build import errors as builderrors
from conary.deps import deps
from conary.lib import log
from conary.local import database
from conary import versions
from conary.versions import Label
from conary.repository import trovesource

#test
from conary_test import recipes
from conary_test import rephelp


class SuperClassTest(rephelp.RepositoryHelper):
    def setUp(self):
        rephelp.RepositoryHelper.setUp(self)
        #Add a superclass recipe
        self.makeSourceTrove('group-appliance', """
class GroupApplianceRecipe(GroupRecipe):
    name = 'group-appliance'
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.addPackages()
""")
        self.subloaddata = loadrecipe.SubloadData(self.cfg, self.openRepository(),
            db=None, buildFlavor=None, directory=None, branch = None,
            name='group-foo', ignoreInstalled=False, overrides=None)
        self.importer = loadrecipe.Importer(subloadData = self.subloaddata)

    def testTwoSuperClasses(self):
        # You can't commit a recipe with two recipe classes (this would break
        # loadSuperClass's return value)
        self.assertRaises(builderrors.LoadRecipeError, self.makeSourceTrove, 'group-base', """
class GroupBase(GroupRecipe):
    name='group-base'
    version = '1.0'
    clearBuildRequires()

class GroupDist(GroupBase):
    name = 'group-dist'
    version = '1.0'
""")

        #Chained classes (Though probably redundant)
        self.makeSourceTrove('group-base', """
loadSuperClass('group-appliance')
class GroupBase(GroupApplianceRecipe):
    clearBuildRequires()
    name='group-base'
    version = '1.0'
""")
        for r in (
           self.importer.loadSuperClass('group-base', str(self.cfg.buildLabel)),
           self.importer.loadInstalled('group-base', str(self.cfg.buildLabel))):
            self.assertTrue(recipe.isGroupRecipe(r))
            self.assertEqual(r.__name__, 'GroupBase')


    def testLoadSuperClass(self):
        # This doesn't actually test the functionality of the load* calls, just
        # the return values.
        #Take a look at the return value
        for r in (
           self.importer.loadSuperClass('group-appliance', str(self.cfg.buildLabel)),
           self.importer.loadInstalled('group-appliance', str(self.cfg.buildLabel))):
            self.assertTrue(recipe.isGroupRecipe(r))
            self.assertEqual(r.__name__, 'GroupApplianceRecipe')

class RecipeTest(rephelp.RepositoryHelper):

    def _testSubload(self, repos, cmd, directory = None, branch = None,
                     db = None, component = 'foo:source', cfg = None,
                     overrides = None):
        if cfg is None:
            cfg = self.cfg

        return loadrecipe.RecipeLoaderFromString('''
%s

class FooRecipe(PackageRecipe):
    name = 'foo'
    version = '1.0'
''' % cmd,
                  '/test.py', cfg = cfg, repos = repos,
                  component = 'foo', directory = directory, branch = branch,
                  db = db, overrides = overrides, buildFlavor=cfg.buildFlavor)

    def testLoadRecipe(self):
        self.repos = self.openRepository()
        self.overrideBuildFlavor('foo.flag,unknownflag')

        self.addTestPkg(1, version='1.0', header='''
    if Use.readline:
        foo = '1.0-readline'
    else:
        foo = '1.0-noreadline'
''')
        self.cookTestPkg(1)
        self.updatePkg(self.cfg.root, 'test1', depCheck=False,
                       justDatabase=True)
        # make a later version available
        self.addTestPkg(1, version='2.0', header='''
    if Use.readline:
        foo = '2.0-readline'
    else:
        foo = '2.0-noreadline'
''')
        log.setVerbosity(log.INFO)
        self.logFilter.clear()
        self.logFilter.add()

        loader = self._testSubload(self.repos,
                                   'loadInstalled("test1=:linux[readline]")')

        self.logFilter.compare(['+ Loaded TestRecipe1 from test1:source=/localhost@rpl:linux/1.0-1[]'])
        log.setVerbosity(log.WARNING)
        assert(loader.module.TestRecipe1.foo == '1.0-readline')
        FooLoaded = loader.getLoadedSpecs()
        assert(FooLoaded.keys() == ['test1=:linux[readline]'])
        assert(use.LocalFlags.flag)
        assert(len(loader.getLoadedTroves()) == 1)
        v1 =  versions.VersionFromString('/localhost@rpl:linux/1.0-1')
        assert(loader.getLoadedTroves()[0] ==
                        ('test1:source', v1, deps.parseFlavor('')))

        loader = self._testSubload(self.repos,
                               'loadInstalled("test1.recipe=:linux[readline]")')
        FooLoaded = loader.getLoadedSpecs()
        assert(FooLoaded.keys() ==
                                    [ 'test1.recipe=:linux[readline]'])
        assert(loader.module.TestRecipe1.foo == '1.0-readline')
        # we don't have a version matching this installed, so just 
        # default to the latest in the repos, but make sure the flavor
        # is respected
        loader = self._testSubload(self.repos,
                           "loadInstalled('test1.recipe=:linux[!readline]')")
        assert(loader.module.TestRecipe1.foo == '2.0-noreadline')
        os.chdir(self.workDir)
        self.checkout('test1')
        os.chdir('test1')
        self.overrideBuildFlavor('!readline')

        # this is also testing that conary handles unknown flags in the build
        # flavor correctly if they're not referenced
        self.logFilter.add()
        self.cookItem(self.repos, self.cfg, 'test1.recipe')
        self.logFilter.remove()
        self.logFilter.compare(
                        ['warning: ignoring unknown Use flag unknownflag'])
        self.updatePkg(self.cfg.root, 'test1-2.0.ccs', 
                       depCheck=False, keepExisting=True, justDatabase=True,
                       replaceFiles=True)
        loader = self._testSubload(self.repos,
                        "loadInstalled('test1.recipe=:linux[readline]')")
        assert(loader.module.TestRecipe1.foo == '1.0-readline')
        loader = self._testSubload(self.repos,
                        "loadInstalled('test1.recipe=[!readline]')")
        assert(loader.module.TestRecipe1.foo == '2.0-noreadline')
        # now ensure we get the 2.0 recipe no matter what is installed
        # when we use loadSuperClass
        loader = self._testSubload(self.repos,
                        "loadSuperClass('test1.recipe=:linux[readline]')")
        assert(loader.module.TestRecipe1.foo == '2.0-readline')
        loader = self._testSubload(self.repos,
                        "loadSuperClass('test1.recipe=:linux[!readline]')")
        assert(loader.module.TestRecipe1.foo == '2.0-noreadline')

    def testUseOwnDb(self):
        # We can pass our own database to loadInstalled to cause its behavior
        # to differ from the original expected behavior a bit.
        # rMake uses this to fiddle w/ the troves that match a hostname/label
        # request.
        repos = self.openRepository()
        cfg = copy.copy(self.cfg)
        branch = versions.VersionFromString('/localhost@rpl:devel//shadow/')
        db = trovesource.SimpleTroveSource()
        self.addTestPkg(1, version='1.0')
        test2Recipe = recipes.createRecipe(2, version='1.0',
                                           header='loadInstalled("test1")')
        cfg.buildLabel = versions.Label('localhost@rpl:branch')
        cfg.installLabelPath = [cfg.buildLabel]
        self.addComponent('test2:source', '/localhost@rpl:branch/1.0',
                          [('test2.recipe', test2Recipe)])
        try:
            loader = self._testSubload(repos, "loadSuperClass('test2')",
                                       cfg = cfg, branch = branch)
            assert(0)
        except Exception, err:
            assert(str(err).find('cannot find source component test1:source:') != -1)
            pass
        db.searchAsDatabase()
        db.addTrove('test1', self._cvtVersion(':linux/1.0'),
                    deps.parseFlavor('!ssl'))
        # should work now
        loader = self._testSubload(repos, "loadSuperClass('test2')",
                                   cfg = cfg, branch = branch, db = db)

    def testUseInstallLabelPath(self):
        repos = self.openRepository()
        branch = versions.VersionFromString('/localhost@rpl:devel//shadow/')

        self.addTestPkg(1, version='1.0')
        test2Recipe = recipes.createRecipe(2, version='1.0',
                                           header='loadSuperClass("test1")')
        cfg = copy.copy(self.cfg)
        cfg.buildLabel = versions.Label('localhost@rpl:branch')
        cfg.installLabelPath = [versions.Label('localhost@rpl:branch')]
        self.addComponent('test2:source', '/localhost@rpl:branch/1.0',
                          [('test2.recipe', test2Recipe)])
        try:
            self._testSubload(repos, "loadSuperClass('test2')", branch = branch,
                              cfg = cfg)
            assert(0)
        except Exception, err:
            assert(str(err).find('cannot find source component test1:source:') != -1)
            pass
        cfg.installLabelPath = [versions.Label('localhost@rpl:linux')]
        self._testSubload(repos, "loadSuperClass('test2=:branch')",
                          branch = branch, cfg = cfg)

    def testBuildReqs(self):
        # test to see that build requirements from super classes
        # are automatically included
        os.chdir(self.workDir)
        self.writeFile('superclass.recipe', recipe1)
        self.writeFile('subclass.recipe', recipe2)
        self.writeFile('subsubclass.recipe', recipe3)
        self.writeFile('cpackage.recipe', recipe4)
        repos = self.openRepository()
        loader = self._testSubload(repos, 'loadInstalled("subclass.recipe")',
                                   directory = self.workDir)
        sc = loader.module.SubClassRecipe(self.cfg, None, '.')
        assert(set(sc.buildRequires) == set(['b', 'c']))

        # subsubclass calls clearBuildRequires and so shouldn't
        # have any of its superclasses buildreqs
        loader = self._testSubload(repos, 'loadInstalled("subsubclass.recipe")',
                                   directory = self.workDir)
        scc = loader.module.SubSubClassRecipe(self.cfg, None, '.')
        assert(set(scc.buildRequires) == set(['d']))

        loader = self._testSubload(repos, 'loadInstalled("cpackage.recipe")',
                                   directory = self.workDir)
        cp = loader.module.CPackageSubClass(self.cfg, None, '.')
        assert len(cp.buildRequires) > 1, 'missing buildreqs: %s' % cp.buildRequires

    def testLoadedTroves(self):
        # we don't check the output of this test.
        self.logFilter.add()
        repos = self.openRepository()

        header = '''
    if Use.%s:
        foo = '1.0-readline'
    else:
        foo = '1.0-noreadline'
'''
        self.addTestPkg(1, version='1.0', header=header % 'readline')
        self.addTestPkg(2, header='loadRecipe("test1")' + header % 'ssl')
        test3Recipe = self.addTestPkg(3,
                        header='loadRecipe("test2")' + header % 'bootstrap')

        use.track()
        rldep = deps.parseFlavor('readline')
        ssldep  = deps.parseFlavor('ssl')
        nobsdep  = deps.parseFlavor('~!bootstrap')
        v1 = versions.VersionFromString('/localhost@rpl:linux/1.0-1')

        loader = self._testSubload(repos, "loadInstalled('test3')")
        FooLoaded = loader.getLoadedSpecs()
        assert(FooLoaded['test3'][0] == ('test3:source', v1, nobsdep))

        Test3Loaded = FooLoaded['test3'][1]
        assert(Test3Loaded['test2'][0] == ('test2:source', v1, ssldep))

        Test2Loaded = Test3Loaded['test2'][1]
        assert(Test2Loaded['test1'][0] == ('test1:source', v1, rldep))

        Test1Loaded = Test2Loaded['test1'][1]
        assert(Test1Loaded == {})

        loadedTroves = loader.getLoadedTroves()
        assert(len(loadedTroves) == 3)
        assert(loadedTroves[0] == ('test1:source', v1, rldep))
        assert(loadedTroves[1] == ('test2:source', v1, ssldep))
        assert(loadedTroves[2] == ('test3:source', v1, nobsdep))

        # Now reset and load again w/ overrides specified
        branch = versions.VersionFromString('/localhost@rpl:foo/')
        oldLoadedSpecs = loader.getLoadedSpecs()
        # move ILP and buildLabel over to another branch, and use overrides
        # to load exactly what we want anyway.
        cfg = copy.copy(self.cfg)
        cfg.installLabelPath = [versions.Label('localhost@rpl:foo')]
        cfg.buildLabel = versions.Label('localhost@rpl:foo')
        self.overrideBuildFlavor('!readline, !ssl')
        overrides = oldLoadedSpecs
        loader = self._testSubload(repos, "loadInstalled('test3')", cfg = cfg,
                                   overrides = overrides)
        assert(loadedTroves[0] == ('test1:source', v1, rldep))
        assert(loadedTroves[1] == ('test2:source', v1, ssldep))
        assert(loadedTroves[2] == ('test3:source', v1, nobsdep))

    def testLoadRecipeWithTwoTroves(self):
        # in the case when two recipes have been shadowed onto the same
        # label via different paths, we filter which one of the two to load
        # based on the labels included in the version strings (its shadow
        # history) 
        repos = self.openRepository()
        branch = versions.VersionFromString('/localhost@rpl:devel//branch/')

        self.addTestPkg(1, version='1.0')
        self.mkbranch(self.cfg.buildLabel, "@rpl:test11", "test1:source")
        self.mkbranch(Label('localhost@rpl:test11'), "@rpl:branch", 'test1:source')
        self.addTestPkg(1, version='2.0')
        self.mkbranch(self.cfg.buildLabel, "@rpl:branch", "test1:source")

        oldLabel = self.cfg.buildLabel
        self.cfg.buildLabel = Label('localhost@rpl:branch')
        loader = self._testSubload(repos, "loadSuperClass('test1')",
                                   branch = branch)
        self.cfg.buildLabel = oldLabel
        assert(loader.module.TestRecipe1.version == '2.0')

    def testLoadInstalledScoring(self):
        # set up all of the variables that loadRecipe expects to be 
        # available at the module level
        db = database.Database(self.cfg.root, self.cfg.dbPath)
        repos = self.openRepository()
        branch = versions.VersionFromString('/localhost@rpl:linux//shadow/')

        self.addTestPkg(1, version='1.0')
        self.addTestPkg(1, version='2.0')

        self.addQuickDbTestPkg(db, 'test1', 
                               '/localhost@rpl:linux/1:1.0-1-1', '')
        self.addQuickDbTestPkg(db, 'test1', 
                               '/localhost@rpl:linux/2:2.0-1-1', '')

        loader = self._testSubload(repos, "loadInstalled('test1')",
                                   branch = branch)
        assert(loader.module.TestRecipe1.version == '2.0')
        self.resetRoot()


        db = database.Database(self.cfg.root, self.cfg.dbPath)

        self.addTestPkg(2, version='3.0')
        self.addTestPkg(2, version='4.0')
        self.mkbranch(['test2:source=3.0'], "@rpl:test2")
        self.mkbranch(['test2:source=:test2'], "@rpl:shadow", shadow=True)
        self.mkbranch(['test2:source=4.0'], "@rpl:shadow", shadow=True)

        self.addQuickDbTestPkg(db, 'test2', 
                           '/localhost@rpl:linux//test2//shadow/2:3.0-1-1', '')
        self.addQuickDbTestPkg(db, 'test2', 
                           '/localhost@rpl:linux//shadow/1:4.0-1-1', '')
        loader = self._testSubload(repos, "loadInstalled('test2')",
                                   db = db, branch = branch)
        assert(loader.module.TestRecipe2.version == '4.0')

    def testLoadRecipeFails(self):
        brokenRecipe = """\
class TestRecipe1(PackageRecipe):
    name = 'testcase'
    version = '1.0'
    a = bb # unknown variable
    def setup(self):
        pass
"""
        os.chdir(self.workDir)
        self.writeFile('testcase.recipe', brokenRecipe)
        try:
            loadrecipe.RecipeLoader(self.workDir + '/testcase.recipe', self.cfg)
        except Exception, err:
            assert('''Error in recipe file "testcase.recipe":
 Traceback (most recent call last):''' in str(err))
            assert(str(err).endswith('''\
    a = bb # unknown variable
NameError: name \'bb\' is not defined
'''))
        else:
            assert(0)

    def checkException(self, fn, *args, **kw):
        error = kw.pop('error')
        try:
            fn(*args, **kw)
        except Exception, e:
            assert(str(e) == error)
        else:
            assert(0)

    def testLoadRecipeInRecipeFails(self):
        brokenRecipe = """\
class TestRecipe1(PackageRecipe):
    loadSuperClass('foo')
    name = 'testcase'
    version = '1.0'
    def setup(self):
        pass
"""
        os.chdir(self.workDir)
        self.writeFile('testcase.recipe', brokenRecipe)
        repos = self.openRepository()
        self.checkException(loadrecipe.RecipeLoader, 
                          self.workDir + '/testcase.recipe', self.cfg,
                          repos=None, error=(
                          'unable to load recipe file %s/testcase.recipe:\n'
                          'Error in recipe file "testcase.recipe", line 1:\n'
                          ' cannot find source component foo:source: No repository access' % self.workDir))
        self.checkException(loadrecipe.RecipeLoader,
                            self.workDir + '/testcase.recipe', self.cfg,
                            repos=repos, 
                            error=(
                            'unable to load recipe file %s/testcase.recipe:\n'
                            'Error in recipe file "testcase.recipe", line 1:\n'
                            ' cannot find source component foo:source: foo:source was not found on path localhost@rpl:linux' % self.workDir))

        fooRecipe = """\
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    a = b # nameerror
    def setup(self):
        pass
"""
        self.writeFile('foo.recipe', fooRecipe)
        try:
            loadrecipe.RecipeLoader(self.workDir + '/testcase.recipe', self.cfg,
                                    repos=None)
        except Exception, err:
            if sys.version_info >= (2, 5):
                topModule = "<module>"
            else:
                topModule = "?"
            self.assertEqual(str(err), '''\
unable to load recipe file %s/testcase.recipe:
Error in recipe file "testcase.recipe", line 1:
 unable to load recipe file %s/foo.recipe:
Error in recipe file "foo.recipe":
 Traceback (most recent call last):
  File "%s/foo.recipe", line 1, in %s
    class Foo(PackageRecipe):
  File "%s/foo.recipe", line 4, in Foo
    a = b # nameerror
NameError: name 'b' is not defined
''' % (self.workDir, self.workDir, self.workDir, topModule, self.workDir))
        else:
            self.fail("Expected exception")

    def testLoadInstalledMultiArch(self):
        repos = self.openRepository()
        self.overrideBuildFlavor('is:x86')

        theRecipe = """
class TestRecipe1(PackageRecipe):
    name = 'testcase'

    if Arch.x86:
        version = '1.0'
    else:
        version = '2.0'

    def setup(self):
        pass
"""

        self.addComponent('testcase:run', '1.0', 'is: x86 x86_64')
        trv = self.addCollection('testcase', '1.0', [':run'], 
                               defaultFlavor='is: x86 x86_64')
        self.addComponent('testcase:source', '1.0', '', 
                          [('testcase.recipe', theRecipe)])
        self.updatePkg('%s=%s[%s]' % trv.getNameVersionFlavor())

        loader = self._testSubload(repos, "loadInstalled('testcase')")
        # CNY-1711: in the context where the recipe was loaded, the arch was
        # multilib, so we expect Arch.x86_64 to be set
        self.assertEqual(loader.module.TestRecipe1.version, '2.0')

    def testMultipleAvailable(self):
        self.addComponent('simple:source', '/localhost@rpl:branch//linux/1.0', 
                         [ ('simple.recipe', recipes.simpleRecipe)])
        laterVersion = self.addComponent('simple:source', 
                                         '/localhost@rpl:branch2//linux/1.0',
                                         [('simple.recipe', recipes.simpleRecipe)]).getVersion()
        repos = self.openRepository()
        self.logFilter.add()
        loader, version = loadrecipe.recipeLoaderFromSourceComponent('simple', 
                                                   self.cfg, repos,
                                                   filterVersions=True,
                                                   defaultToLatest=True)
        assert(version == laterVersion)
        # switching to getTroveLatestByLabel gets rid of this message.
        self.logFilter.compare([])

    def testMultipleAvailableLoadInstalled(self):
        repos = self.openRepository()
        self.overrideBuildFlavor('is:x86')
        simpleRecipe2 = recipes.simpleRecipe.replace("version = '1'", "version = '2'")
        simpleRecipe3 = recipes.simpleRecipe.replace("version = '1'", "version = '3'")
        self.addComponent('simple:source', ':branch1/1', 
                            [('simple.recipe', recipes.simpleRecipe)])
        self.addComponent('simple:source', ':branch2/2', 
                                [('simple.recipe', simpleRecipe2)])
        db = self.openDatabase()
        self.addDbComponent(db, 'simple', ':branch1/1', '')
        self.sleep(0.1)
        self.addDbComponent(db, 'simple', ':branch2/2', '')
        self.logFilter.add()

        loader = self._testSubload(repos, 'loadInstalled("simple")')

        self.logFilter.compare(["""\
warning: source component simple has multiple versions:
Picking latest:
      simple=/localhost@rpl:branch2/2-1-1

Not using:
      simple=/localhost@rpl:branch1/1-1-1
"""])
        self.logFilter.remove()
        assert(loader.module.SimpleRecipe.version == '2')


        #self.addComponent('simple:source', '/localhost@rpl:branch1/1-1',
        #                    [('simple.recipe', recipes.simpleRecipe)])
        self.addComponent('simple:source', '/localhost@rpl:branch2/3-1',
                           [('simple.recipe', simpleRecipe3)])

        self.addDbComponent(db, 'simple', '/localhost@rpl:branch1//linux/1-1-1', '')
        self.addDbComponent(db, 'simple', '/localhost@rpl:branch2//linux/3-1-1', '')
        self.logFilter.add()

        loader = self._testSubload(repos, 'loadInstalled("simple")')

        self.logFilter.compare(["""\
warning: source component simple has multiple versions on labelPath localhost@rpl:linux:
Picking latest:
      simple=/localhost@rpl:branch2//linux/3-1-1

Not using:
      simple=/localhost@rpl:branch1//linux/1-1-1
"""])
        self.logFilter.remove()
        assert(loader.module.SimpleRecipe.version == '3')

    def testRecipeFactory(self):
        self.addComponent('factory-simple:source',
                          factory = 'factory',
                          fileContents = [ ('factory-simple.recipe', 
                                            simpleFactoryRecipe)])

        self.addComponent('typed:source', factory = 'simple')
        repos = self.openRepository()

        loader = loadrecipe.RecipeLoaderFromRepository('typed',
                                                       self.cfg, repos,
                                                       defaultToLatest=True)
        finalRecipe = loader.getRecipe()
        self.assertEqual(finalRecipe.__name__, 'R')
        self.assertEqual(finalRecipe.name, 'typed')
        self.assertEqual(finalRecipe.version, '2.0')
        x = loader.getLoadedTroves()[0]
        self.assertEqual(
            (x[0], str(x[1]), str(x[2])),
            ('factory-simple:source', '/localhost@rpl:linux/1.0-1', ''))
        self.assertEqual(loader.getLoadedSpecs()['factory-simple'][0], x)

    def testBuiltInOverride(self):
        # Make sure we can override builtin recipes without destroying the
        # original modules.
        packageRecipeModuleId = id(packagerecipe)
        os.chdir(self.workDir)
        self.newpkg('foo')
        os.chdir('foo')
        self.writeFile('foo.recipe', fooRecipe)
        # override PackageRecipe with a derivative
        self.writeFile('packagerecipe.recipe', packageRecipe)

        i = len(sys.modules)
        repos = self.openRepository()
        loader = loadrecipe.RecipeLoader(self.workDir +
                                                '/foo/foo.recipe', self.cfg,
                                         repos = repos)
        # we don't update sys.modules for loaded modules
        assert(len(sys.modules) == i)

        recipe = loader.getRecipe()
        fooModule = recipe.__moduleObj__
        loadedPackageRecipe = fooModule.PackageRecipe
        # these should be different because we overloaded PackageRecipe, but
        # the PackageRecipe we see from the packagerecipe should be the
        # original
        assert(loadedPackageRecipe != packagerecipe.PackageRecipe)
        assert(id(packagerecipe) == packageRecipeModuleId)

        # XXX it would be nice if this were true, but it's not because we
        # make copies of the builtin base classes to prevent them from getting
        # corrupted
        #assert(loadedPackageRecipe.__base__ == packagerecipe.PackageRecipe)

    def testLoadFromGroup(self):
        self.addComponent('simplesuper:recipe=1.0',
                          fileContents = [ ('simplesuper.recipe',
                                             simpleSuperRecipe)])

        self.addCollection('simplesuper=1.0', [ ':recipe' ])
        self.addCollection('group-super=1.0', [ 'simplesuper[]' ], flavor='is:x86' )
        self.addCollection('group-super=1.0',  [ 'simplesuper[]' ], flavor='is:x86 x86_64' )

        self.addComponent('simplesuper:recipe=2.0',
                          fileContents = [ ('simplesuper.recipe', 
                                            simpleSuperRecipe.replace('1.0', '2.0')) ] )
        self.addCollection('simplesuper=2.0', [ ':recipe' ])

        repos = self.openRepository()

        # ------ Load from a single group recursively
        cfg = copy.deepcopy(self.cfg)
        if cfg.buildFlavor.stronglySatisfies(deps.parseFlavor('is: x86 x86_64')):
            cfg.buildFlavor = deps.parseFlavor('is: x86_64')
        cfg.autoLoadRecipes.append('group-super=1.0')

        importer = loadrecipe.Importer()
        loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer, cfg,
                                    repos, db = None, 
                                    buildFlavor=cfg.buildFlavor)
        assert(importer.module.SimpleSuperRecipe._loadedFromSource[0] ==
                                                'simplesuper:recipe')

        # ------ Load from a package to override the group which is later
        cfg = copy.deepcopy(self.cfg)
        cfg.autoLoadRecipes.append('simplesuper:recipe')
        cfg.autoLoadRecipes.append('group-super=1.0')

        importer = loadrecipe.Importer()
        loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer, cfg,
                                    repos, db = None, 
                                    buildFlavor=cfg.buildFlavor)
        assert(importer.module.SimpleSuperRecipe.version == '2.0')

        # ------ We should be able to load this locally instead of from
        # the repository
        self.updatePkg('group-super')
        cfg = copy.deepcopy(self.cfg)
        cfg.autoLoadRecipes.append('group-super=1.0')

        importer = loadrecipe.Importer()
        self.mock(repos, "getFileContents", None)
        loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer, cfg,
                                    repos, db = None, 
                                    buildFlavor=cfg.buildFlavor)
        self.unmock()

        self.writeFile(self.rootDir + '/simplesuper.recipe', 'foo')
        importer = loadrecipe.Importer()
        loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer, cfg,
                                    repos, db = None, 
                                    buildFlavor=cfg.buildFlavor)

        # Now make sure the repository match beats the locally installed
        # one
        repos = self.openRepository()
        self.resetRoot()
        self.updatePkg('simplesuper:recipe=1.0')
        cfg = copy.deepcopy(self.cfg)
        cfg.autoLoadRecipes.append('simplesuper:recipe')
        importer = loadrecipe.Importer()
        loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer, cfg,
                                    repos, db = None, 
                                    buildFlavor=cfg.buildFlavor)
        assert(importer.module.SimpleSuperRecipe.version == '2.0')

        # stop the repositroy and see if it falls back to the local
        # system
        self.resetRoot()
        self.updatePkg('group-super')
        self.stopRepository()
        importer = loadrecipe.Importer()
        loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer, cfg,
                                    repos, db = None, 
                                    buildFlavor=cfg.buildFlavor)

        # Make sure we handle things we can't find in the local database
        # properly
        cfg = copy.deepcopy(self.cfg)
        cfg.autoLoadRecipes.append('group-super=2.0')
        importer = loadrecipe.Importer()
        try:
            loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer,
                                        cfg, repos, db = None,
                                        buildFlavor=cfg.buildFlavor)
        except builderrors.RecipeFileError, e:
            assert(str(e) ==
                   'no match for autoLoadRecipe entry group-super=2.0')

    def testGroupLoadOrder(self):
        # Make sure we order loads of recipes from a group based on the
        # loadedTroves defined by each trove, falling back to name/version
        # sort order when those aren't sufficient to establish an ordering.

        class Importer(loadrecipe.Importer):
            # Tracks the order items are added to the module's dict
            def __init__(self, *args, **kwargs):
                self.addOrder = []
                loadrecipe.Importer.__init__(self, *args, **kwargs)

            def updateModuleDict(self, d):
                self.addOrder.append(d.keys())
                loadrecipe.Importer.updateModuleDict(self, d)

        def addRecipe(name, version, loaded):
            comp = self.addComponent('%s:recipe=%s' % (name, version),
                          fileContents = [ ('a.recipe',
                                "class %sRecipe(PackageRecipe):\n"
                                "    version = '%s'\n"
                                "    name = 'a'\n"
                                % (name.title(), version)) ] )
            self.addCollection('%s=%s' % (name, version),
                  [ comp.getNameVersionFlavor() ],
                  loadedReqs = [ x.getNameVersionFlavor() for x in loaded ])
            return comp

        repos = self.openRepository()

        a1 = addRecipe('a', '1.0', [])
        b1 = addRecipe('b', '1.0', [])

        trv = self.addCollection('group-test=1.0', [ 'a', 'b' ])
        assert([ x[0] for x in trv.iterTroveList(strongRefs = True) ]
                    == [ 'b', 'a' ])

        cfg = copy.deepcopy(self.cfg)
        cfg.autoLoadRecipes.append('group-test')

        importer = Importer()
        loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer, cfg,
                                    repos, db = None)
        assert(importer.addOrder == [ ['ARecipe'], ['BRecipe'] ])

        # Now make A depend on B and make sure that flips the order
        a2 = addRecipe('a', '2.0', [ b1 ] )
        trv = self.addCollection('group-test=2.0', [ a2, b1 ] )
        importer = Importer()
        loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer, cfg,
                                    repos, db = None)
        assert(importer.addOrder == [ ['BRecipe'], ['ARecipe'] ])

        c2 = addRecipe('c', '2.0', [ a2 ] )
        trv = self.addCollection('group-test=3.0', [ a2, b1, c2 ] )
        importer = Importer()
        loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer, cfg,
                                    repos, db = None)
        assert(importer.addOrder == [ ['BRecipe'], ['ARecipe'], ['CRecipe'] ])

        b2 = addRecipe('b', '2.0', [ a2 ])
        trv = self.addCollection('group-test=4.0', [ a2, b2 ])
        importer = Importer()
        try:
            loadrecipe.RecipeLoaderFromString._loadAutoRecipes(importer, cfg,
                                        repos, db = None)
        except Exception, err:
            assert(str(err) ==
                    'Cannot autoload recipes due to a loadedRecipes loop '
                    'involving a:recipe=/localhost@rpl:linux/2.0-1-1[] and '
                    'b:recipe=/localhost@rpl:linux/2.0-1-1[]')

    def testAutoLoadBuild(self):
        self.openRepository()
        cfg = copy.deepcopy(self.cfg)
        cfg.autoLoadRecipes.append('other:recipe')
        self.addComponent('simplesuper:source', [('simplesuper.recipe', 
                                                  simpleSuperRecipe)])
        self.addComponent('other:recipe=2.0[~ssl]', [('simplesuper.recipe', 
                                   simpleSuperRecipe.replace('1.0', '2.0'))])
        self.addCollection('other=2.0[~ssl]', [':recipe'])
        self.addComponent('other:recipe=2.0', [('simplesuper.recipe', 
                                   simpleSuperRecipe.replace('1.0', '2.0'))])
        self.addCollection('other=2.0', [':recipe'])
        self.addComponent('simplesub:source', [('simplesub.recipe', 
                                                simpleSubRecipe)])
        self.cookItem(self.openRepository(), cfg, 'simplesub')
        files = list(self.findAndGetTrove('simplesub:runtime').iterFileList())
        os.chdir(self.workDir)
        self.checkout('simplesub')
        os.chdir('simplesub')
        self.writeFile('simplesub.recipe', simpleSubRecipe  + '\n#comment\n')
        self.commit(cfg=cfg)

    def testLoadedTrovesWithAutoLoad(self):
        self.logFilter.add()
        repos = self.openRepository()
        if 'x86_64' not in str(self.cfg.buildFlavor):
            raise testhelp.SkipTestException('Skip test on x86 arch')

        self.cfg.autoLoadRecipes.append('other')
        
        self.overrideBuildFlavor('is: x86 x86_64')

        header = '''
    if Arch.x86: pass
'''
        self.addComponent('other:recipe=2.0[is:x86 x86_64]', 
                         [('simplesuper.recipe', 
                           simpleSuperRecipe.replace('1.0', '2.0'))])
        self.addCollection('other=2.0[is:x86 x86_64]', [':recipe'])
        self.addTestPkg(1, version='1.0', header=header)
        self.addTestPkg(2, header='loadRecipe("test1")')

        use.track()
        rldep = deps.parseFlavor('readline')
        ssldep  = deps.parseFlavor('ssl')
        nobsdep  = deps.parseFlavor('~!bootstrap')
        emptydep = deps.Flavor()
        x64dep = deps.parseFlavor('is:x86_64')
        v1 = versions.VersionFromString('/localhost@rpl:linux/1.0-1')

        loader = self._testSubload(repos, "loadInstalled('test2')")
        FooLoaded = loader.getLoadedSpecs()
        assertEq = self.assertEqual
        assertEq(FooLoaded['test2'][0], ('test2:source', v1, emptydep))

        Test2Loaded = FooLoaded['test2'][1]
        assertEq(Test2Loaded['test1'][0], ('test1:source', v1, x64dep))

        Test1Loaded = Test2Loaded['test1'][1]
        assertEq(Test1Loaded, {})

        loadedTroves = loader.getLoadedTroves()
        assertEq(len(loadedTroves), 2)
        assertEq(loadedTroves[0], ('test1:source', v1, x64dep))
        assertEq(loadedTroves[1], ('test2:source', v1, emptydep))

        # Now reset and load again w/ overrides specified
        branch = versions.VersionFromString('/localhost@rpl:foo/')
        oldLoadedSpecs = loader.getLoadedSpecs()
        # move ILP and buildLabel over to another branch, and use overrides
        # to load exactly what we want anyway.
        cfg = copy.copy(self.cfg)
        self.overrideBuildFlavor('!readline, !ssl')
        overrides = oldLoadedSpecs
        loader = self._testSubload(repos, "loadInstalled('test2')", cfg = cfg,
                                   overrides = overrides)
        assert(loadedTroves[0] == ('test1:source', v1, x64dep))
        assert(loadedTroves[1] == ('test2:source', v1, emptydep))

# test case ends -------------------------------

recipe1 = """
class SuperClassRecipe(PackageRecipe):
    name = 'superclass'
    version = '1.0'
    clearBuildReqs()
    buildRequires = ['a', 'b']

    def setup(self):
        pass
"""

recipe2 = """
loadRecipe("superclass.recipe")
class SubClassRecipe(SuperClassRecipe):
    name = 'subclass'
    version = '1.0'
    buildRequires = ['c']
    clearBuildReqs('a')

    def setup(r):
        r.Create('/asdf/foo')

"""
recipe3 = """
loadRecipe("subclass.recipe")
class SubSubClassRecipe(SubClassRecipe):
    name = 'subsubclass'
    version = '1.0'

    clearBuildReqs()
    buildRequires = ['d']

    def setup(r):
        r.Create('/asdf/foo')

"""
recipe4 = """
class CPackageSubClass(CPackageRecipe):
    name = 'cpackage'
    version = '1.0'

    buildRequires = ['d']

    def setup(r):
        r.Create('/asdf/foo')

"""

simpleSuperRecipe = """
class SimpleSuperRecipe(PackageRecipe):
    version = '1.0'
    name = 'simplesuper'
"""

simpleSubRecipe = """
loadSuperClass('simplesuper')
class SimpleSubRecipe(SimpleSuperRecipe):
    version = '1.0'
    name = 'simplesub'
    clearBuildReqs()

    def setup(r):
        r.Create('/foo' + SimpleSuperRecipe.version)
"""


fooRecipe = '''
loadSuperClass('packagerecipe')
class FooRecipe(PackageRecipe):
    version = '1.0'
    name = 'foo'
'''

packageRecipe = '''
class PackageRecipe(PackageRecipe):
    version = '0.0'
    name = 'packagerecipe'
'''

simpleFactoryRecipe = """
class SimpleFactory(Factory):

    name = 'factory-simple'
    version = '1.0'

    def getRecipeClass(self):

        class R(PackageRecipe):
            name = self.packageName
            version = '2.0'

        return R
"""
