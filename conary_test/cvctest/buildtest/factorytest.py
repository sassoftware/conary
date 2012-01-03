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


import bz2, copy, os, shutil
import logging

from conary import state
from conary import trove
from conary import versions
from conary.build import cook
from conary.lib import log
from conary.repository import changeset
from conary_test import rephelp
from conary_test import recipes

class FactoryTest(rephelp.RepositoryHelper):

    def testSimpleFactory(self):
        os.chdir(self.workDir)
        self.newpkg('factory-tar', factory = 'factory')
        os.chdir('factory-tar')
        self.writeFile('factory-tar.recipe',
"""
class TarFactory(Factory):

    name = 'factory-tar'
    version = '1.0'

    def getRecipeClass(self):

        class TarRecipe(PackageRecipe):

            name = self.packageName
            version = self.sources[0].split('-')[1].split('.tar')[0]
            sources = self.sources
            clearBuildReqs()

            def setup(r):
                for path in r.sources:
                    r.addArchive(path, dir = '/')

        return TarRecipe
""")
        self.addfile('factory-tar.recipe')
        self.commit()
        os.chdir("..")

        self.newpkg('sample', factory = 'tar')
        os.chdir('sample')
        os.mkdir('bin')
        self.writeFile('bin/script', '#!/bin/bash\necho hello world\n',
                       mode = 0755)
        os.system('tar czf script-1.1.tar.gz bin')
        shutil.rmtree('bin')
        self.addfile('script-1.1.tar.gz')
        self.commit()
        os.chdir("..")

        self.cookFromRepository('sample')
        self.updatePkg('sample', depCheck = False)

    def testFactoryNames(self):
        try:
            self.newpkg('factory-foo')
        except Exception, e:
            self.assertEqual(str(e),
                'Only factory troves may use "factory-" in '
                'their name. Add --factory=factory to create a factory trove.'
                )

        try:
            self.newpkg('foo', factory = 'factory')
        except Exception, e:
            assert(str(e) == 'The name of factory troves must begin with '
                             '"factory-"')

    def testFactoryWithLocalOverride(self):
        self.addComponent("factory-simple:source", "1.0",
                factory = "factory",
                fileContents = [ ( 'factory-simple.recipe',
                                recipes.simpleFactory) ] )

        self.addComponent("sample:source", "2.0",
                factory = 'simple',
                fileContents = [ ( 'sample.recipe',
"""
class SampleRecipe(FactoryRecipeClass):
    version = "2.0"
    name = "sample"
    clearBuildReqs()

    def setup(r):
        r.Create("/foo", contents = "contents\\n")
""") ] )

        self.cookFromRepository('sample')

        os.chdir(self.workDir)
        self.checkout("sample")
        os.chdir("sample")
        repos = self.openRepository()
        self.cookItem(repos, self.cfg, 'sample.recipe')
        assert(os.path.exists('sample-2.0.ccs'))
        os.unlink('sample-2.0.ccs')

        # cooking w/o specifying a filename should work as well
        cstate = state.ConaryStateFromFile('CONARY')
        self.discardOutput(cook.cookCommand, self.cfg, [ cstate ], False, {})
        assert(os.path.exists('sample-2.0.ccs'))

    def testFactorySourceFileAccess(self):
        self.addComponent("factory-simple:source", "1.0",
                factory = "factory",
                fileContents = [ ( 'factory-simple.recipe',
                                recipes.simpleFactoryWithSources) ] )

        self.addComponent("sample:source", "2.0",
                factory = 'simple',
                fileContents = [ ( 'VERSION', "2.0\n") ] )

        self.cookFromRepository('sample')
        self.updatePkg('sample=2.0')
        self.verifyFile(self.rootDir + '/foo', '2.0\n')

    def testLocalFactory(self):
        os.chdir(self.workDir)
        self.newpkg("foo", factory = "test")
        os.chdir("foo")
        self.writeFile("factory-test.recipe",
"""
class TestFactory(Factory):

    name = "factory-test"
    version = "1.0"

    def getRecipeClass(self):
        class TestSubclass(PackageRecipe):
            name = "foo"
            version = "1.0"
            clearBuildReqs()

            def setup(self):
                self.Create("/etc/foo", "foo")

        return TestSubclass
""")

        repos = self.openRepository()
        cstate = state.ConaryStateFromFile('CONARY')
        self.discardOutput(cook.cookCommand, self.cfg, [ cstate ], False, {})

    def testLocalFactoryWithLocalRecipe(self):
        os.chdir(self.workDir)
        self.newpkg("foo", factory = "test")
        os.chdir("foo")
        self.writeFile("factory-test.recipe",
"""
class TestFactory(Factory):

    name = "factory-test"
    version = "1.0"

    def getRecipeClass(self):
        class TestSubclass(PackageRecipe):
            name = "testsubclass"
            version = "1.0"
            internalAbstractBaseClass = True
            clearBuildReqs()

        return TestSubclass
""")

        self.writeFile("foo.recipe",
"""
# CNY-2813. importing log inside a recipe used to reset the loglevel
from conary.lib import log
class FooRecipe(FactoryRecipeClass):

    name = "foo"
    version = "1.1"

    def setup(self):
        self.Create("/etc/foo", "foo")
""")

        self.addfile("foo.recipe")
        repos = self.openRepository()
        cstate = state.ConaryStateFromFile('CONARY')
        level = log.getVerbosity()
        try:
            log.setVerbosity(log.INFO)
            klass = logging.getLoggerClass()
            self.discardOutput(cook.cookCommand, self.cfg, [cstate], False, {})
            self.assertEquals(klass, logging.getLoggerClass())
        finally:
            log.setVerbosity(level)
        ccs = changeset.ChangeSetFromFile(os.path.join(self.workDir,
                'foo', 'foo-1.1.ccs'))
        trvs = [trove.Trove(x) for x in ccs.iterNewTroveList()]
        trv = [x for x in trvs if x.getName() == 'foo:debuginfo'][0]
        files = [x for x in trv.iterFileList() if \
                x[1] == '/usr/src/debug/buildlogs/foo-1.1-log.bz2']
        fileId, path, pathId, ver = files[0]
        fileInfo, fileObj = ccs.getFileContents(fileId, pathId)
        decomp = bz2.BZ2Decompressor()
        data = decomp.decompress(fileObj.f.read())
        self.failIf("+ Processing" not in data,
                "build log data appears to be incomplete")

    def testCookFromNewPackage(self):
        # CNY-2661
        self.addComponent('factory-test:source',
                          factory = 'factory',
                          fileContents = [ ( 'factory-test.recipe',
"""
class FactoryTest(Factory):
    version = '1.0'
    name = "factory-test"
    def getRecipeClass(self):

        class FooRecipe(PackageRecipe):
            clearBuildReqs()
            version = '1.1'
            name = 'foo'
            def setup(r):
                r.Create('/etc/foo', 'contents')

        return FooRecipe
""") ] )

        os.chdir(self.workDir)
        self.newpkg('foo', factory = 'test')
        os.chdir('foo')

        cstate = state.ConaryStateFromFile('CONARY')
        self.discardOutput(cook.cookCommand, self.cfg, [ cstate ], False, {})
        assert(os.path.exists("foo-1.1.ccs"))

    def testSimpleFactoryOverride(self):
        # CNY-2666
        self.addComponent('factory-test:source',
                          factory = 'factory',
                          fileContents = [ ( 'factory-test.recipe',
"""
class FactoryTest(Factory):
    version = '1.0'
    name = "factory-test"
    def getRecipeClass(self):

        class FooRecipe(PackageRecipe):
            clearBuildReqs()
            version = '1.1'
            name = 'foo'
            def setup(r):
                r.Create('/etc/foo', 'contents')
        return FooRecipe
""") ] )

        self.addComponent('foo:source', '1.1',
                          factory = 'test',
                          fileContents = [ ('foo.recipe',
"""
class FooRecipe(FactoryRecipeClass):
    name = 'foo'
    version = '1.1'
""") ] )

        repos = self.openRepository()
        self.cookItem(repos, self.cfg, 'foo')
        # make sure it got built
        self.updatePkg('foo')

    def testFactoryOnAlternateLabel(self):
        self.addComponent('factory-test:source=localhost@foo:bar',
                          factory = 'factory',
                          fileContents = [ ( 'factory-test.recipe',
"""
class FactoryTest(Factory):
    version = '1.0'
    name = "factory-test"
    def getRecipeClass(self):

        class FooRecipe(PackageRecipe):
            clearBuildReqs()
            version = '1.1'
            name = 'foo'
            def setup(r):
                r.Create('/etc/foo', 'contents')
        return FooRecipe
""") ] )

        self.addComponent('foo:source', '1.1',
                          factory = 'test=localhost@foo:bar',
                          fileContents = [ ])

        repos = self.openRepository()
        self.cookItem(repos, self.cfg, 'foo')
        # make sure it got built
        self.updatePkg('foo')

    def testFactoryCooks(self):
        factory = \
"""
class FactoryTest(Factory):
    version = '1.0'
    name = "factory-test"
    def getRecipeClass(self):

        class FooRecipe(PackageRecipe):
            clearBuildReqs()
            version = '1.1'
            name = 'foo'
            def setup(r):
                r.Create('/etc/foo', 'contents')
        return FooRecipe
"""

        self.addComponent('factory-test:source',
                      factory = 'factory',
                      fileContents = [ ( 'factory-test.recipe', factory) ] )

        repos = self.openRepository()

        # XXX why does the testsuite set this in /tmp by default? that stops
        # packaging from working (which rightfully ignores things in /tmp!)
        cfg = copy.copy(self.cfg)
        cfg.baseClassDir = "/usr/share/conary/baseclasses"
        self.cookItem(repos, cfg, 'factory-test', ignoreDeps = True)
        self.updatePkg('factory-test:recipe')
        self.verifyFile(self.rootDir + cfg.baseClassDir +
                                                    '/factory-test.recipe',
                        factory)

    def testFactoryCooksBeforeCheckin(self):
        # CY-2757
        repos = self.openRepository()
        os.chdir(self.workDir)
        self.newpkg('factory-test', factory = 'factory')
        os.chdir('factory-test')
        self.writeFile('factory-test.recipe',
"""
class FactoryTest(Factory):
    version = '1.0'
    name = "factory-test"
    def getRecipeClass(self):

        return PackageRecipe
"""
)

        # this causes an error as there are no files checked in
        try:
            self.cookItem(repos, self.cfg, state.ConaryStateFromFile('CONARY'),
                          ignoreDeps = True)
        except Exception, e:
            assert(str(e) == 'version factory-test:source of @NEW@ does not '
                             'contain factory-test.recipe')

        self.addfile('factory-test.recipe')
        cfg = copy.copy(self.cfg)
        cfg.baseClassDir = "/usr/share/conary/baseclasses"
        self.cookItem(repos, cfg, state.ConaryStateFromFile('CONARY'),
                      ignoreDeps = True)
        assert(os.path.exists('factory-test-1.0.ccs'))

    def testFactoryCookWithExtraFiles(self):
        # CNY-XXX
        self.addComponent('factory-test:source',
                          factory = 'factory',
                          fileContents = [ ( 'factory-test.recipe',
"""
class FactoryTest(Factory):
    version = '1.0'
    name = "factory-test"
    clearBuildReqs()

    def getRecipeClass(self):

        class FooRecipe(PackageRecipe):
            clearBuildReqs()
            version = '1.1'
            name = 'foo'
            def setup(r):
                r.Create('/etc/foo', 'contents')

        return FooRecipe

    @classmethod
    def getAdditionalSourceFiles(kls):
        # Note that the class only processes 2-item tuples - this also tests
        # that old conary builds will be able to handle new factories
        return [ ('some-random-file', '%(datadir)s/', 'this is ignored') ]

"""),
                                ( 'some-random-file', "Some random content\n")])

        self.factoryBuildReqs()
        repos = self.openRepository()
        trvSpec = self.cookFromRepository('factory-test')
        trvSpec = [ (x[0], versions.VersionFromString(x[1]), x[2])
                    for x in trvSpec ]
        trvs = repos.getTroves(trvSpec)
        trv = trvs[0]
        self.failUnlessEqual(trv.getName(),'factory-test:recipe')
        paths = sorted([ x[1] for x in trv.iterFileList() ])
        self.failUnlessEqual(paths, ['/usr/share/some-random-file'])

    def factoryBuildReqs(self):
        db = self.openDatabase()
        for x in [
            'bash:runtime',
            'conary-build:lib',
            'conary-build:python',
            'conary-build:runtime',
            'conary:python',
            'conary:runtime',
            'coreutils:runtime',
            'dev:runtime',
            'filesystem:runtime',
            'findutils:runtime',
            'gawk:runtime',
            'grep:runtime',
            'python:lib',
            'python:runtime',
            'sed:runtime',
            'setup:runtime',
            'sqlite:lib',
            ]:
            self.addDbComponent(db, x, '/localhost@rpl:branch1//linux/1-1-1')

    def testGroupFactory(self):
        self.addComponent('foo:runtime=1',
                          fileContents = [ ('/foo', 'contents') ] )
        self.addComponent('factory-test:source',
                          factory = 'factory',
                          fileContents = [ ( 'factory-test.recipe',
"""
class FactoryTest(Factory):
    version = '1.0'
    name = "factory-test"
    clearBuildReqs()

    def getRecipeClass(self):

        class TestGroupRecipe(GroupRecipe):
            version = '1.1'
            name = 'group-test'
            clearBuildRequires()
            def setup(r):
                r.add('foo:runtime')

        return TestGroupRecipe
""") ] )

        self.factoryBuildReqs()
        trvSpec = self.cookFromRepository('factory-test')

        self.addComponent('group-test:source', '1.1',
                          factory = 'test',
                          fileContents = [ ])

        repos = self.openRepository()
        self.cookItem(repos, self.cfg, 'group-test')
        self.updatePkg('group-test')
        self.verifyFile(self.rootDir + '/foo', 'contents')
