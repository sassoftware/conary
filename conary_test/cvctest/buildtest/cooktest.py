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

import bz2
import os
import re
import shutil
import tempfile


#testsuite
from conary_test import recipes
from conary_test import rephelp

#conary
from conary import errors
from conary import state
from conary import trove
from conary import versions
from conary.build import cook, loadrecipe, macros, use, packagerecipe
from conary.build import errors as builderrors
from conary.cmds import queryrep
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import log
from conary.lib import util
from conary.local import database
from conary.repository import changeset
from conary.repository import errors as repoerrors
from conary.versions import VersionFromString as VFS

# TODO - test multiple flavors

# Decorator to change status back to original
_cwd = os.getcwd()
def protect(fn):
    def testWrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        finally:
            # Restore status
            os.chdir(_cwd)
    testWrapper.func_name = fn.func_name
    return testWrapper

class CookTest(rephelp.RepositoryHelper):

    emptyRecipe = """
class EmptyPackage(PackageRecipe):
    name = 'empty'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        pass
"""
    def testEmptyPackage(self):
        d = tempfile.mkdtemp(dir=self.workDir)

        origDir = os.getcwd()
        os.chdir(d)
        self.newpkg('empty')
        os.chdir('empty')
        self.writeFile('empty.recipe', self.emptyRecipe)
        self.addfile('empty.recipe')
        self.commit()
        repos = self.openRepository()
        self.logFilter.add()
        try:
            built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                            'empty.recipe')
            built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                            'empty.recipe', logBuild=True)
        finally:
            self.logFilter.remove()
            self.logFilter.compare([
                'error: No files were found to add to package empty']*2)
            os.chdir(origDir)
            shutil.rmtree(d)

    @testhelp.context('rollback')
    def testCookToFile(self):
        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.notByDefaultRecipe)
        self.addfile('testcase.recipe')
        self.commit()
        repos = self.openRepository()
        try:
            #built, strx = self.captureOutput(self.cookItem, repos, self.cfg,
            #                                'testcase.recipe')
            built, strx = self.cookItem(repos, self.cfg,
                                        'testcase.recipe')
            cs = changeset.ChangeSetFromFile('testcase-1.0.ccs')
            # make sure the flavor is set; for a while they were coming out None
            assert(cs.getPrimaryTroveList()[0][2] is not None)
            # make sure we can install this changeset
            self.updatePkg(self.rootDir, 'testcase-1.0.ccs')
            self.rollback(self.rootDir, 0)
        finally:
            os.chdir(origDir)

        trvcs = cs.iterNewTroveList().next()
        trv = trove.Trove(trvcs)
        self.failUnless(str(trv.getBuildFlavor()))

    def testEmerge(self):
        d = tempfile.mkdtemp(dir=self.workDir)

        origDir = os.getcwd()
        os.chdir(d)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        self.addfile('testcase.recipe')
        self.commit()
        repos = self.openRepository()
        self.discardOutput(cook.cookCommand, self.cfg,
                           ['testcase=/localhost@rpl:linux/1.0-1'],
                           prep=False, macros={},
                           emerge=True,
                           cookIds=(os.getuid(), os.getgid()))
        db = self.openDatabase()
        troveTups = db.findTrove(None, ('testcase', None, None))
        assert(len(troveTups) == 1)

    def testCookLog(self):
        d = tempfile.mkdtemp(dir=self.workDir)

        origDir = os.getcwd()
        os.chdir(d)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        self.addfile('testcase.recipe')
        self.commit()
        repos = self.openRepository()
        try:
            self.cfg.noClean = True
            (built, dummy), str = self.captureOutput(self.cookItem, 
                                            repos, self.cfg,
                                            'testcase', logBuild=True)
        finally:
            del self.cfg.noClean 
            os.chdir(origDir)
            shutil.rmtree(d)
        version = versions.VersionFromString(built[0][1])
        flavor = built[0][2]
        self.updatePkg(self.cfg.root, 'testcase:debuginfo', version)
        baseLogPath = '/usr/src/debug/buildlogs/testcase-1.0-log.bz2'
        logPath = self.cfg.root + baseLogPath

        assert(os.stat(logPath).st_size)
        logData = bz2.BZ2File(logPath).read()
        assert(logData.find('gcc -O2 -g  -static  hello.c   -o hello') != -1)
        # make sure that \r does not make it into logs
        assert('\r' not in logData)
        # make sure that foo\r shows up in log as foo\n
        # this changed in CNY-2487. foo used to not show up at all.
        assert('ComponentSpec\n' in logData)

        # make sure that macros are not expanded
        assert(logData.find('%(') != -1)

        # make sure the file got tagged
        iter = repos.iterFilesInTrove('testcase:debuginfo', version, flavor, 
                                      withFiles=True)
        # grab the file object
        logFile = [ x[4] for x in iter if x[1] == baseLogPath][0]
        assert(logFile.tags == ['buildlog'] )

        xmlLogPath = '/usr/src/debug/buildlogs/testcase-1.0-xml.bz2'
        xmlLogPath = self.cfg.root + xmlLogPath
        assert(os.stat(xmlLogPath).st_size)
        xmlLogData = bz2.BZ2File(xmlLogPath).read()
        dataLine = [x for x in xmlLogData.splitlines() \
                if 'gcc -O2 -g  -static  hello.c   -o hello' in x][0]
        desc = 'cook.build.doBuild'
        self.failIf('<descriptor>%s</descriptor>' % desc not in dataLine,
                "Expected descriptor to be present: %s" % desc)


    def testFlavors(self):
        flavorRecipe = """\
class TestRecipe1(PackageRecipe):
    name = 'testcase'
    version = '1.0'
    clearBuildReqs()
    if Use.builddocs:
        pass
    if Arch.x86.i686:
        pass
    if Arch.x86.sse2:
        pass
    if Arch.x86.threednow:
        pass

    if Arch.x86_64.nx:
        pass

    def setup(self):
        self.Create('/usr/bin/foo', contents='''#!/bin/sh
echo "Hello!"
''', mode=0755) 
        self.Run('''
cat > hello.c <<'EOF'
#include <stdio.h>

int main(void) {
    return printf("Hello, world.\\\\n");
}
EOF
        ''')
        self.Make('hello', preMake='LDFLAGS="-static"')
        self.Install('hello', '%(bindir)s/')


"""

        d = tempfile.mkdtemp(dir=self.workDir)

        origDir = os.getcwd()
        os.chdir(d)
        self.writeFile('testcase.recipe', flavorRecipe)
        repos = self.openRepository()
        try:
            built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                            'testcase.recipe[!builddocs]')
            cs = changeset.ChangeSetFromFile('testcase-1.0.ccs')
        finally:
            os.chdir(origDir)
            shutil.rmtree(d)
        flavor = cs.getPrimaryTroveList()[0][2].__str__()
        if use.Arch.x86:
            assert(flavor == \
                        '~!builddocs is: x86(~!3dnow,i486,i586,i686,~!sse2)')
        elif use.Arch.x86_64:
            assert(flavor == '~!builddocs is: x86_64(~!nx)')
        else:
            raise NotImplementedError, 'modify test for this arch'
        
    def testOverrides(self):
        flavorRecipe = """\
class TestRecipe1(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()
    x = False
    if not Use.builddocs:
        x = True

    def setup(self):
        if self.x:
            if Arch.ppc:
                pass
            if Use.readline:
                self.Create('/usr/bin/foo', contents='''#!/bin/sh
                            echo "Hello!"
                            ''', mode=0755) 
"""

        d = tempfile.mkdtemp(dir=self.workDir)
        self.cfg.buildFlavor.union(deps.parseFlavor('!builddocs'))
        origDir = os.getcwd()
        os.chdir(d)
        self.writeFile('test.recipe', flavorRecipe)
        repos = self.openRepository()
        try:
            built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                            'test.recipe')
            cs = changeset.ChangeSetFromFile('test-1.0.ccs')
        finally:
            os.chdir(origDir)
            shutil.rmtree(d)
            self.cfg.useflags = {}
        # the code will fail to get here if the overrides statement doesn't 
        # run -- ChangesetFromFile will not work
        flavor = cs.getPrimaryTroveList()[0][2]
        if use.Arch.x86:
            assert(flavor.freeze() == '1#x86|5#use:~!builddocs:readline')
        elif use.Arch.x86_64:
            assert(flavor.freeze() == '1#x86_64|5#use:~!builddocs:readline')
        else:
            raise NotImplementedError, 'modify test for this arch'
        

    def testLoadFlavors(self):
        recipe1 = """
class SuperClassRecipe(PackageRecipe):
    name = 'superclass'
    version = '1.0'
    clearBuildReqs()
    if Use.readline:
        pass

    def setup(self):
        pass
"""

        recipe2 = """
class ExtraClassRecipe(PackageRecipe):
    name = 'extraclass'
    version = '1.0'
    clearBuildReqs()
    if Use.builddocs:
        pass

    def setup(self):
        pass
"""


        recipe3 = """
loadRecipe("superclass.recipe")
loadRecipe("extraclass.recipe")
class SubClassRecipe(SuperClassRecipe):
    name = 'subclass'
    version = '1.0'
    if Use.krb:
        pass

    def setup(r):
        r.Create('/asdf/foo')
"""

        origDir = os.getcwd()
        os.chdir(self.workDir)
        d = tempfile.mkdtemp(dir=self.workDir)
        os.chdir(d)
        self.writeFile('superclass.recipe', recipe1)
        self.writeFile('extraclass.recipe', recipe2)
        self.writeFile('subclass.recipe', recipe3)
        repos = self.openRepository()
        built, out = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'subclass.recipe')
        flavor = built[0][0][2]
        # the point of this is that use.builddocs doesn't show up even though
        # it is loaded in ExtraClass
        assert(str(flavor) == 'krb,readline')

    def testLoadFollowsBranches(self):
        # makes sure that when you load a recipe that has been loaded off
        # of a branch, all future relative loadRecipes are loaded off 
        # of that new branch
        subclassRecipe = """
loadRecipe("superclass.recipe", label="localhost@rpl:branch")
class SubClassRecipe(SuperClassRecipe):
    name = 'subclass'
    version = '1.0'
    def setup(r):
        r.Create('/asdf/foo')
"""

        superclassRecipe = """
loadRecipe('otherclass.recipe')
class SuperClassRecipe(PackageRecipe):
    name = 'superclass'
    version = '1.0'
    clearBuildReqs()

    def setup(self):
        pass
"""

        otherclassRecipe = """
class OtherClassRecipe(PackageRecipe):
    name = 'otherclass'
    version = '1.0'
    clearBuildReqs()

    def setup(self):
        pass
"""
        origDir = os.getcwd()
        os.chdir(self.workDir)
        d = tempfile.mkdtemp(dir=self.workDir)
        os.chdir(d)
        repos = self.openRepository()
        # superclasses are on a different label....
        self.cfg.buildLabel = versions.Label('localhost@rpl:branch')
        self.newpkg('otherclass')
        os.chdir('otherclass')
        self.writeFile('otherclass.recipe', otherclassRecipe)
        self.addfile('otherclass.recipe')
        self.commit()
        os.chdir('..')
        self.newpkg('superclass')
        os.chdir('superclass')
        self.writeFile('superclass.recipe', superclassRecipe)
        self.addfile('superclass.recipe')
        self.captureOutput(self.commit)
        os.chdir('..')
        self.cfg.buildLabel = versions.Label('localhost@rpl:linux')
        self.newpkg('subclass')
        os.chdir('subclass')
        self.writeFile('subclass.recipe', subclassRecipe)
        self.addfile('subclass.recipe')
        self.captureOutput(self.commit)
        os.chdir('..')
        
        built, out = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'subclass')
        # if we've reached here, we've managed to load the recipes succesfully

    def testWrongName(self):
        recipe = """\
class TestRecipe1(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(self):
        pass
"""
        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipe)
        repos = self.openRepository()
        self.assertRaises(cook.CookError, self.cookItem, repos, self.cfg,
                                                            'testcase.recipe')
        self.addfile('testcase.recipe')
        self.logFilter.add()
        try:
            self.commit()
        except errors.CvcError, err:
            assert(re.match('unable to load recipe file'
                               ' .*testcase.*recipe:\n'
                               'Recipe object name \'foo\''
                               ' does not match file/component name'
                               ' \'testcase\'', str(err)))

    def testVersionNumbers(self):
        """
        verify that the correct version numbers are used when creating
        binary packages
        """
        # set up a recipe in a :source component
        origDir = os.getcwd()
        self.resetRepository()
        self.resetWork()
        srcdir = os.sep.join((self.workDir, 'src'))
        os.mkdir(srcdir)
        os.chdir(srcdir)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        self.addfile('testcase.recipe')
        self.commit()

        # verify the testcase:source component exists with the
        # correct version
        repos = self.openRepository()
        (rc, out) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       troveSpecs = [ 'testcase:source=1.0-1' ])

        # build the recipe, we should get 1.0-1-1
        built, out = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'testcase')
        x86 = use.Arch.getCurrentArch()._toDependency()
        assert(built == ((('testcase:runtime',
                           '/localhost@rpl:linux/1.0-1-1', x86),),
                         None))

        # a second build should result in 1.0-1-2
        built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'testcase')
        assert(built == ((('testcase:runtime',
                           '/localhost@rpl:linux/1.0-1-2', x86),),
                         None))

        # change the recipe file
        self.writeFile('testcase.recipe', recipes.testRecipe1 + '\n# change\n')
        self.commit()

        # verify that the testcase:source component moves to 1.0-2
        repos = self.openRepository()
        (rc, str) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       troveSpecs = [ 'testcase:source=1.0-2' ])
        
        # new build should be 1.0-2-1
        built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'testcase')
        assert(built == ((('testcase:runtime',
                           '/localhost@rpl:linux/1.0-2-1', x86),),
                         None))

        os.chdir('..')
        util.rmtree('testcase')
        self.resetRepository()

        # start over.  This time, get the testcase:source component
        # to version 1.0-2 before building any binary packages at all
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        self.addfile('testcase.recipe')
        self.commit()

        # verify 1.0-1
        (rc, str) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       [ 'testcase:source=1.0-1' ])

        # change the recipe file to get us to 1.0-2
        self.writeFile('testcase.recipe', recipes.testRecipe1 + '\n# change\n')
        self.commit()

        # verify 1.0-2
        (rc, str) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       [ 'testcase:source=1.0-2' ])

        # verify that a binary component gets 1.0-2-1
        built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'testcase')
        assert(built == ((('testcase:runtime',
                           '/localhost@rpl:linux/1.0-2-1', x86),),
                         None))

        # start over.  This time, get the testcase:source component
        # to version 1.0-2 before building any binary packages at all
        self.resetRepository()
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        self.addfile('testcase.recipe')
        self.commit()

        
        # change the recipe file to get us to 1.0-2
        self.writeFile('testcase.recipe', recipes.testRecipe1 + '\n# change\n')
        self.commit()

        # change the recipe file to get us to 1.0-3
        self.writeFile('testcase.recipe', recipes.testRecipe1 + '\n# change2\n')
        self.commit()

        # change the recipe file to get us to 1.0-4
        self.writeFile('testcase.recipe', recipes.testRecipe1 + '\n#sss\n')
        self.commit()
        # verify 1.0-4
        (rc, str) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       [ 'testcase:source=1.0-4' ])

        # verify that a binary component gets 1.0-4-1
        built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'testcase')

        assert(built == ((('testcase:runtime',
                           '/localhost@rpl:linux/1.0-4-1', x86),),
                         None))
        # create a 2.0 recipe
        newrecipe = recipes.testRecipe1.replace("version = '1.0'", 
                                                "version = '2.0'")
        self.writeFile('testcase.recipe', newrecipe)
        self.commit()
        # create 2.0-2
        self.writeFile('testcase.recipe', newrecipe + '\n#comment\n')
        self.commit()
        # verify 2.0-2
        (rc, str) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       [ 'testcase:source=2.0-2' ])

        # verify that cooked is 2.0-2-1
        built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'testcase')
        assert(built == ((('testcase:runtime',
                           '/localhost@rpl:linux/2.0-2-1', x86),),
                         None))
        os.chdir(origDir)

    def testBuildRequirements(self):
        """
        Verify that different build requirements are found when they are 
        installed .  Also test to make sure crossrequires are ignored when
        not cross compiling.
        """
        recipestr1 = """
class TestBuildReqs(PackageRecipe):
    name = 'req1'
    version = '1'
    clearBuildReqs()
    crossRequires = ['unknown']
    def setup(r):
        if Use.readline:
            pass
        r.Create('/asdf/req')
"""

        recipestr2 = """
class TestBuildReqs2(PackageRecipe):
    name = 'foo'
    version = '1'
    crossRequires = ['unknown']
    clearBuildReqs()
    buildRequires = ['req1', 'req1:runtime', 'req1[readline]', 'req1=:linux',
                     'req1=@rpl:linux']
    def setup(r):
        r.Create('/asdf/foo')
"""
        recipestr3 = """
class TestBuildReqs2(PackageRecipe):
    name = 'foo'
    version = '1'
    clearBuildReqs()
    buildRequires = ['req2', 'req1:devel', 'req1[!readline]', 'req1=:branch',
                     'req1=@spp:linux']
    def setup(r):
        r.Create('/asdf/foo')
"""
        changeBuildReqs = """\
    def _checkBuildRequirements(self, *args, **kw):
        rv = PackageRecipe._checkBuildRequirements(self, *args, **kw)
        self.buildRequirementOverrides = [ x.getNameVersionFlavor() for x in self.getBuildRequirementTroves() if x.getName() == 'req1:runtime' ]
        return rv
"""

        self.resetRepository()
        repos = self.openRepository()
        (built, d) = self.buildRecipe(recipestr1, "TestBuildReqs")
        # now shadow this package onto another label
        self.mkbranch(self.cfg.buildLabel, "@rpl:shadow", "req1", shadow=True,
                      binaryOnly=True)
        pkgname = built[0][0]
        version = VFS(built[0][1])
        version = version.createShadow(versions.Label('localhost@rpl:shadow'))
        # insall the shadowed version -- it should fulfill all of the 
        # upstream version build reqs
        self.updatePkg(self.cfg.root, 'req1', version)
        (built, d) = self.buildRecipe(recipestr2, "TestBuildReqs2")
        assert(built)
        v,f = built[0][1:]
        trv = repos.getTrove('foo', VFS(v), f)
        trvComp = repos.getTrove('foo', VFS(v), f)
        readline = deps.parseFlavor('readline')
        reqs = set([ (x, version, readline) for x in ('req1', 'req1:runtime')])
        assert(set(trv.getBuildRequirements()) == reqs)
        (built, d) = self.buildRecipe(recipestr2 + changeBuildReqs, 
                                      "TestBuildReqs2")
        v,f = built[0][1:]
        trv = repos.getTrove('foo', VFS(v), f)
        reqs = set([ (x, version, readline) for x in ('req1:runtime')])

        (built, d) = self.buildRecipe(recipestr2, "TestBuildReqs2")
        assert(built)

        trvComp = repos.getTrove('foo:runtime', VFS(v), f)
        assert(trvComp.getBuildRequirements() == [])

        self.logFilter.add()
        self.assertRaises(errors.CvcError, self.buildRecipe,
                          recipestr3, "TestBuildReqs2")
        self.logFilter.compare('error: Could not find the following troves '
                       'needed to cook this recipe:\n'
                       'req1:devel\n'
                       'req1=:branch\n'
                       'req1=@spp:linux\n'
                       'req1[!readline]\n'
                       'req2')
        (built, d) = self.buildRecipe(recipestr3, "TestBuildReqs2", 
                                      ignoreDeps=True)
        assert(built)


    def testRecursiveBuildReqs(self):
        notSsl = deps.overrideFlavor(self.cfg.flavor[0], deps.parseFlavor('!ssl'))
        self.cfg.flavor.append(notSsl)
        foo = self.addComponent('foo:run', '1', filePrimer=1)
        foo2 = self.addComponent('foo:run', '1', '!ssl', filePrimer=2)
        bar = self.addComponent('bar:run', '1', '!ssl,!readline', requires = 'trove:foo:run', 
                                filePrimer=3) # this one doesn't match
        bar2 = self.addComponent('bar:run', '1', '!ssl,~foo', requires = 'trove:foo:run', 
                                 filePrimer=4) # this one only matches the second
        bar3 = self.addComponent('bar:run', '1', '!ssl, readline', requires = 'trove:foo:run', 
                                 filePrimer=5) # this one only matches the second
        bam = self.addComponent('bam:run', '1', '!readline', 
                                requires = 'trove:bar:run abi:ELF32(SysV)', 
                                filePrimer=6)
        kernel = self.addComponent('kernel:run', '1', provides = 'abi:ELF32(SysV)', filePrimer=7)
        baz = self.addComponent('baz:run', '1', requires = 'trove:bam:run trove:blah:run', filePrimer=8)
        bazPkg = self.addCollection('baz', '1', [':run'])
        blah = self.addComponent('blah:run', '1', filePrimer=9)
        self.updatePkg(['%s[%s]' % (x.getName(), x.getFlavor())
                        for x in (foo, bar, bam, kernel, bazPkg, blah, foo2, bar2, bar3)], 
                        raiseError=True)

        recipestr = """
class TestBuildReqs1(PackageRecipe):
    name = 'final'
    version = '1'
    clearBuildReqs()
    buildRequires = ['baz']
    def setup(r):
        r.Create('/asdf/foo')
"""
        (built, d) = self.buildRecipe(recipestr, "TestBuildReqs1")
        repos = self.openRepository()
        trv = repos.getTrove(*repos.findTrove(self.cfg.installLabelPath, ('final', None, None), 
                                             self.cfg.flavor)[0])
        # all include all recursive buildreqs except for the kernel one.
        # note that foo[!ssl] is not in there because foo[''] matches on the first flavor
        # and foo[!ssl] doesn't.
        # also bar[!ssl,!readline] is not included but bar[!ssl] _and_ bar[!ssl,!readline] are.
        assert(set(trv.getBuildRequirements()) == \
               set([x.getNameVersionFlavor() for x in (foo, bar2, bar3, bam, baz, blah, bazPkg)]))
        recipestr = """
class TestBuildReqs1(GroupRecipe):
    name = 'group-foo'
    version = '1'
    clearBuildReqs()
    buildRequires = ['baz']
    def setup(r):
        r.add('final')
"""
        (built, d) = self.buildRecipe(recipestr, "TestBuildReqs1")
        trv = self.findAndGetTrove('group-foo')
        assert(set(trv.getBuildRequirements()) == \
               set([x.getNameVersionFlavor() for x in (foo, bar2, bar3, bam, baz, blah, bazPkg)]))
        recipestr = """
class basicFileset(FilesetRecipe):
    name = "fileset-test"
    version = "1.0"
    branch = "@rpl:linux"
    clearBuildRequires()
    buildRequires = ['baz']

    def setup(self):
        self.addFile('/asdf/foo', 'final:runtime', self.branch)
"""
        (built, d) = self.buildRecipe(recipestr, "basicFileset")
        trv = self.findAndGetTrove('fileset-test')
        assert(set(trv.getBuildRequirements()) == \
               set([x.getNameVersionFlavor() for x in (foo, bar2, bar3, bam, baz, blah, bazPkg)]))

        recipestr = """
class testRedirect(RedirectRecipe):
    name = 'redirect'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        r.addRedirect("final", l)
"""
        self.addComponent('redirect:runtime')
        self.addCollection('redirect', [':runtime'])
        (built, d) = self.buildRecipe(recipestr, "testRedirect")
        trv = self.findAndGetTrove('fileset-test')
        assert(set(trv.getBuildRequirements()) == \
               set([x.getNameVersionFlavor() for x in (foo, bar2, bar3, bam, baz, blah, bazPkg)]))

    def testLibDirsFlavored(self):
        recipestr1 = """
class TestFlavoredLibDir(PackageRecipe):
    name = 'foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.MakeDirs('%(libdir)s/')
        r.ExcludeDirectories(exceptions=".*")
"""
        (built, d) = self.buildRecipe(recipestr1, "TestFlavoredLibDir")
        if use.Arch.x86:
            assert(built[0][2].freeze() == '1#x86')
        elif use.Arch.x86_64:
            assert(built[0][2].freeze() == '1#x86_64')
        else:
            raise NotImplementedError, 'modify test for this arch'
        recipestr2 = """
class TestFlavoredLibDir(PackageRecipe):
    name = 'foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.MakeDirs('%(prefix)s/')
        r.ExcludeDirectories(exceptions=".*")
"""
        (built, d) = self.buildRecipe(recipestr2, "TestFlavoredLibDir")
        assert(built[0][2].freeze() == '')



    def testTestComponentDeps(self):
        """
        Verify that dependencies in :test components are not unioned into
        the package's dependencies
        """
        recipestr1 = """
class TestMultipleMainPackage(PackageRecipe):
    name = 'foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Run('mkdir test; echo -e \#\!/bin/notthere\\nhi > test/foo; chmod 755 test/foo')
        r.TestSuite('test', autoBuildMakeDependencies=False)
        r.Create('/a')
"""
        self.resetRepository()
        (built, d) = self.buildRecipe(recipestr1, "TestMultipleMainPackage")
        names = [ x[0] for x in built ]
        repos = self.openRepository()
        troves = repos.findTrove(self.cfg.buildLabel, 
                                 ('foo', None, self.cfg.flavor[0]))
        troves = repos.getTroves(troves, withFiles = False)
        assert(troves[0].getRequires().freeze() == "")

    def testTestStrangeNames(self):
        """
        Test file names that include strange file names 
        """
        recipestr1 = """
class TestStrangeNames(PackageRecipe):
    name = 'foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.MakeDirs('/usr/share/foo')
        r.MakeDirs('/usr/share/%%bar', mode=0700)
        r.Create('/usr/share/foo/[', contents='Bar')
        r.Create('/usr/share/foo/%%foo', contents='Blah', mode=0400)
        r.Run('echo "Foo" >>  %(destdir)s/usr/share/foo/%%#\@\!\ Foo\(\(\)')
        r.ComponentSpec('foo-sub:test', '/usr/share/foo/%%foo')
"""
        self.resetRepository()
        root = self.workDir + "/root"
        (built, d) = self.buildRecipe(recipestr1, "TestStrangeNames")
        versions = [ x[1] for x in built ]
        pkgnames = [ x[0] for x in built ]
        self.updatePkg(root, pkgnames[0], versions[0])
        self.updatePkg(root, pkgnames[1], versions[1])
        assert(os.path.exists('%s/usr/share/%%bar' % root))
        self.verifyFile('%s/usr/share/foo/[' %root,
                        'Bar\n')
        self.verifyFile('%s/usr/share/foo/%%#@! Foo(()' % root,
                        'Foo\n')

    def testNoFlags(self):
        use.clearFlags()
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        repos = self.openRepository()
        self.logFilter.add()
        self.assertRaises(SystemExit, self.cookItem, repos, self.cfg, 'testcase.recipe')
        self.logFilter.remove()

    def testMarch(self):
        flavorRecipe = """\
class TestCase(PackageRecipe):
    name = 'testcase'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/asdf/%(targetarch)s')
        r.Create('/asdf/unameArch/%(unamearch)s')
        r.Create('/asdf/%(target)s')
"""

        d = tempfile.mkdtemp(dir=self.workDir)
        origDir = os.getcwd()
        os.chdir(d)
        self.writeFile('testcase.recipe', flavorRecipe)
        repos = self.openRepository()
        self.overrideBuildFlavor('is: x86(i686)')
        built, d = self.cookItem(repos, self.cfg, 'testcase.recipe')
        cs = changeset.ChangeSetFromFile(d)

        versions = [ x[1] for x in built ]
        pkgname = built[0][0]
        root = self.workDir + "/root"
        self.updatePkg(root, cs)
        assert(os.path.exists('%s/asdf/i686' % root))
        assert(os.path.exists('%s/asdf/unameArch/i686' % root))
        assert(os.path.exists('%s/asdf/i686-unknown-linux' % root))
        util.rmtree(root)
        self.overrideBuildFlavor('is: x86')
        built, d = self.cookItem(repos, self.cfg, 'testcase.recipe')
        cs = changeset.ChangeSetFromFile(d)
        self.updatePkg(root, d)
        assert(os.path.exists('%s/asdf/i386' % root))
        assert(os.path.exists('%s/asdf/unameArch/i386' % root))
        assert(os.path.exists('%s/asdf/i386-unknown-linux' % root))
        util.rmtree(root)
        self.overrideBuildFlavor('is: ppc')
        built, d = self.cookItem(repos, self.cfg, 'testcase.recipe')
        cs = changeset.ChangeSetFromFile(d)
        self.updatePkg(root, d)
        assert(os.path.exists('%s/asdf/powerpc' % root))
        assert(os.path.exists('%s/asdf/unameArch/ppc' % root))
        assert(os.path.exists('%s/asdf/powerpc-unknown-linux' % root))


    def testPrimaryTroves(self):
        flavorRecipe = """\
class TestCase(PackageRecipe):
    name = 'testcase'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        for i in range(0,4):
            r.Create('/usr/bin/testcase-%s' % i, contents='''#!/bin/sh
    echo "Hello, %s!"
    ''' % i, mode=0755) 
        r.PackageSpec('testcase-1', '/usr/bin/testcase-1')
        r.ComponentSpec('testcase:devel', '/usr/bin/testcase-2')
        r.ComponentSpec('testcase-3:runtime', '/usr/bin/testcase-3')
"""

        d = tempfile.mkdtemp(dir=self.workDir)
        origDir = os.getcwd()
        os.chdir(d)
        self.writeFile('testcase.recipe', flavorRecipe)
        repos = self.openRepository()
        try:
            built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                            'testcase.recipe')
            cs = changeset.ChangeSetFromFile('testcase-1.0.ccs')
        finally:
            os.chdir(origDir)
            shutil.rmtree(d)
        primaryPackageNames = [ x[0] for x in cs.getPrimaryTroveList()]
        primaryPackageNames.sort()
        expectedNames = ['testcase', 'testcase-1', 'testcase-3' ]
        assert(primaryPackageNames == expectedNames)

    def testLotsOfFiles(self):
        """
        Test having more than 1024 files (checks for fd leaks during
        packaging)
        """
        recipestr1 = """
class LotsOfFiles(PackageRecipe):
    name = 'foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.MakeDirs('/foo')
        for i in range(0,1026):
            r.Create('/foo/%d' %i, contents='foo')
"""
        self.resetRepository()
        self.buildRecipe(recipestr1, "LotsOfFiles")

    def testDoNotCookAsRoot(self):
        self.mimicRoot()
        try:
            try:
                cook.cookCommand(self.cfg, [], False, {})
            except Exception, e:
                assert(isinstance(e, cook.CookError))
                assert(str(e) == 'Do not cook as root')
            else:
                self.fail('expected exception was not raised')
        finally:
            self.realRoot()

    def testBuildPathNotWritable(self):
        d = tempfile.mkdtemp(dir=self.workDir)

        origDir = os.getcwd()
        os.chdir(d)
        self.newpkg('empty')
        os.chdir('empty')
        self.writeFile('empty.recipe', self.emptyRecipe)
        self.addfile('empty.recipe')
        self.commit()
        repos = self.openRepository()

        # Create build path
        roDir = tempfile.mkdtemp()
        # Make it read-only
        os.chmod(roDir, 0400)

        oldBuildPath = self.cfg.buildPath
        self.cfg.buildPath = roDir

        expected = "Error creating %s/empty: Permission denied" % roDir
        try:
            try:
                built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                                'empty.recipe')
            except errors.ConaryError, e:
                self.failUnlessEqual(e.args[0], expected)
        finally:
            self.cfg.buildPath = oldBuildPath
            os.chdir(origDir)
            shutil.rmtree(d)
            os.chmod(roDir, 0600)
            util.rmtree(roDir, ignore_errors=True)

    def testBadPassword(self):
        recipe = """
class TestRecipe(PackageRecipe):
    name = "testcase"
    version = "0.1"
    clearBuildReqs()
"""
        origDir = os.getcwd()
        self.resetRepository()
        self.resetWork()
        os.chdir(self.workDir)
        self.newpkg('testcase')
        os.chdir("testcase")
        self.writeFile('testcase.recipe', recipe)
        repos = self.openRepository()

        self.addfile('testcase.recipe')

        oldUser = self.cfg.user.pop()
        self.cfg.user.addServerGlob("*", "test", "bad pw")

        try:
            self.failUnlessRaises(repoerrors.InsufficientPermission, self.commit)
        finally:
            os.chdir(origDir)
            self.cfg.user.append(oldUser)

    def testDuplicatePathIds(self):
        self.addComponent('foo:runtime=0.1-1-1',
              fileContents = [ ( '/1',
                                  rephelp.RegularFile(pathId = 'abc')) ] )
        self.addComponent('foo:runtime=0.2-1-1',
              fileContents = [ ( '/2',
                                  rephelp.RegularFile(pathId = 'abc')) ] )

        recipe = (
            "class FooRecipe(PackageRecipe):\n"
            "    name = 'foo'\n"
            "    version = '1'\n"
            "    clearBuildReqs()\n"
            "    def setup(r):\n"
            "        r.Create('/1', 'foo')\n"
            "        r.Create('/2', 'bar')\n" )

        trv = self.build(recipe, "FooRecipe")
        assert(sorted([ x[1] for x in trv.iterFileList()]) == [ '/1', '/2' ])

class MultipleMainPackageTest(rephelp.RepositoryHelper):
    def testMultipleMainPackageTest1(self):
        """
        Verify that conary builds multiple packages
        """
        recipestr1 = """
class TestMultipleMainPackage(PackageRecipe):
    name = 'foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Create('/a')
        r.Create('/aa')
        r.Create('/bb')
        r.Create('/b')
        r.Create('/foo')
        r.PackageSpec('a', '/a', '/aa')
        r.ComponentSpec('b', '/b', '/bb')
"""
        self.resetRepository()
        (built, d) = self.buildRecipe(recipestr1, "TestMultipleMainPackage")
        names = [ x[0] for x in built ]
        assert('foo:runtime' in names)
        assert('foo:b' in names)
        assert('a:runtime' in names)
        self.resetRepository()

    def testMultipleMainPackagesDifferentFlavors(self):
        self.resetWork()
        self.resetRepository()
        self.addTestPkg(1, binary=True, subPackages=['foo'])
        self.repos = self.openRepository()
        self.captureOutput(self.cookTestPkg, 1)
        versionList = self.repos.getTroveVersionList('localhost', 
                                                {'test1' : None, 
                                                  'test1-foo' : None })
        assert(len(versionList['test1'].keys()) == 1)
        assert(len(versionList['test1-foo'].keys()) == 1)
        test1v = versionList['test1'].keys()[0]
        test1Foov = versionList['test1'].keys()[0]
        flavors = self.repos.getTroveVersionFlavors(versionList)
        assert(len(flavors['test1'][test1v]) == 1)
        assert(len(flavors['test1-foo'][test1Foov]) == 1)
        test1f = flavors['test1'][test1v][0]
        assert(str(test1f).find('is: x86') != -1)
        assert(str(flavors['test1-foo'][test1Foov][0]).find('is: x86') != -1)

    def testNextVersion(self):
        origDir = os.getcwd()
        self.resetRepository()
        self.resetWork()
        repos = self.openRepository()
        cfg = self.cfg
        srcdir = os.sep.join((self.workDir, 'src'))
        os.mkdir(srcdir)
        os.chdir(srcdir)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        self.addfile('testcase.recipe')
        self.commit()
        db = database.Database(self.rootDir, self.cfg.dbPath)
        built, s = self.captureOutput(self.cookItem, repos, self.cfg,
                                      'testcase')


        # 1. using the same flavor, we bump the version #
        curArch= use.Arch.getCurrentArch()._toDependency()
        nextVer = cook.nextVersion(repos, db, 'testcase', 
                                   VFS('/localhost@rpl:linux/1.0-1'), 
                                   curArch)
        assert(str(nextVer.trailingRevision().buildCount) == '2')
        # 2. try a different flavor (none in this case) -- don't bump
        nextVer = cook.nextVersion(repos, db, 'testcase',  
                                   VFS('/localhost@rpl:linux/1.0-1'), 
                                   deps.Flavor())
        assert(str(nextVer.trailingRevision().buildCount) == '1')
        # 3. Require a build count increase - build count should increase
        #    even with different flavor
        nextVer = cook.nextVersion(repos, db, 'testcase', 
                                   VFS('/localhost@rpl:linux/1.0-1'), 
                                   deps.Flavor(), alwaysBumpCount=True)
        assert(str(nextVer.trailingRevision().buildCount) == '2')
        # now create another package, separately, that we are going to 
        # also include in our version search
        recipe2 = recipes.testRecipe1.replace("name = 'testcase'", 
                                              "name = 'testcase2'")
        # build again, so now the installed built count is 2
            (built, d) = self.buildRecipe(recipe2, "TestRecipe1")
            (built, d) = self.buildRecipe(recipe2, "TestRecipe1")
        # different version results in the troves getting the one matching
        # the highest 
        # 1. using the same flavor, we bump the version #
        nextVer = cook.nextVersion(repos, db, ['testcase', 'testcase2'], 
                                   VFS('/localhost@rpl:linux/1.0-1'), 
                                   curArch)
        assert(str(nextVer.trailingRevision().buildCount) == '3')
        # 2. try a different flavor (none in this case) -- don't bump
        nextVer = cook.nextVersion(repos, db, ['testcase', 'testcase2'],
                                   VFS('/localhost@rpl:linux/1.0-1'), 
                                   deps.Flavor())
        assert(str(nextVer.trailingRevision().buildCount) == '2')
        # 3. Require a build count increase - build count should increase
        #    even with different flavor
        nextVer = cook.nextVersion(repos, db, ['testcase', 'testcase2'],
                                   VFS('/localhost@rpl:linux/1.0-1'), 
                                   deps.Flavor(), alwaysBumpCount=True)
        assert(str(nextVer.trailingRevision().buildCount) == '3')

        # test nextVersion on a branch
        # make sure nextVersion finds the existing versions
        self.mkbranch(self.cfg.buildLabel, "@rpl:branch", "testcase:source")
        self.cfg.buildLabel = versions.Label('localhost@rpl:branch')
        built, s = self.captureOutput(self.cookItem, repos, self.cfg,
                                      'testcase')
        nextVer = cook.nextVersion(repos, db, ['testcase', 'testcase2'],
                                   VFS('/localhost@rpl:linux/1.0-1/branch/1'), 
                                   curArch)
        assert(str(nextVer.trailingRevision().buildCount) == '2')

        # the version returned should be newer than something on a different
        # branch but on the same label (in tihs case, the -3 binary count
        # should never appear on the branch)
        self.addComponent('testcase:runtime', '/localhost@rpl:branch/1.0-1-3',
                          flavor = curArch)
        self.addCollection('testcase', '/localhost@rpl:branch/1.0-1-3',
                           [ ':runtime' ], defaultFlavor = curArch )
        nextVer = cook.nextVersion(repos, db, ['testcase', 'testcase2'],
                                   nextVer.getSourceVersion(), curArch)
        assert(str(nextVer) == '/localhost@rpl:linux/1.0-1/branch/4')

    def testNextVersion2(self):
        def _create(verStr, flavorStr):
            self.addComponent('testcase:runtime', verStr,
                              flavor = flavorStr)
            self.addCollection('testcase', verStr,
                               [ ':runtime' ], defaultFlavor = flavorStr )

        _create('/localhost@rpl:1//shadow/1.0-1-0.1', 'is:x86')
        _create('/localhost@rpl:2//shadow/1.0-1-0.2', 'is:x86_64')

        repos = self.openRepository()
        db = database.Database(self.rootDir, self.cfg.dbPath)
        nextVer = cook.nextVersion(repos, db, ['testcase', 'testcase2'],
                                   VFS('/localhost@rpl:1//shadow/1.0-1'),
                                   deps.parseFlavor('is:x86'))
        assert(str(nextVer) == '/localhost@rpl:1//shadow/1.0-1-0.3')

    def testCookWithSourceDirs(self):
        # make sure that we use the srcdirs directive correctly.
        # 
        recipestr1 = """
class TestCookWithSrcDirs(PackageRecipe):
    name = 'foo'
    version = '1'
    
    clearBuildReqs()

    def setup(r):
        r.addSource('file', dest='/foo')
"""
        repos = self.openRepository()
        self.addComponent('foo:source', '1-1', '',
                          (('foo.recipe', recipestr1),
                           ('file', 'origContents')))
        self.cfg.tmpDir = self.rootDir + '/tmp'
        util.mkdirChain(self.cfg.tmpDir)
        open(self.cfg.tmpDir + '/file', 'w').write('blah')
        self.cookItem(repos, self.cfg, 'foo')
        self.updatePkg('foo')
        self.verifyFile(self.rootDir + '/foo', 'origContents')

        os.chdir(self.workDir)
        self.checkout('foo')
        os.chdir('foo')
        open('file', 'w').write('blah')
        self.cookItem(repos, self.cfg, 'foo.recipe')
        self.updatePkg('foo-1.ccs')
        self.verifyFile(self.rootDir + '/foo', 'blah')
        self.cookItem(repos, self.cfg, 'foo')
        self.updatePkg('foo=%s' % self.cfg.installLabel)
        self.verifyFile(self.rootDir + '/foo', 'origContents')

    def testCookWithCONARY(self):
        recipestr1 = """
class TestCookWithCONARY(PackageRecipe):
    name = 'foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Create('/a')
"""
        recipestr2 = """
class TestCookWithCONARY(PackageRecipe):
    name = 'foo'
    version = '2'
    clearBuildReqs()
    def setup(r):
        r.Create('/a')
"""
        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg('foo')
        os.chdir('foo')
        self.writeFile('foo.recipe', recipestr1)
        # test cooking before any commit -- this should work fine
        repos = self.openRepository()
        self.cookItem(repos, self.cfg, 'foo.recipe')
        self.addfile('foo.recipe')
        self.commit()
        os.chdir(origDir)
        self.mkbranch(self.cfg.buildLabel, "@rpl:test1", "foo:source")
        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.checkout('foo', versionStr="@rpl:test1")
        os.chdir('foo')
        self.cookItem(repos, self.cfg, 'foo.recipe')
        cs = changeset.ChangeSetFromFile('foo-1.ccs')
        assert(cs.getPrimaryTroveList()[0][1].asString() == '/localhost@rpl:linux/1-1-0/test1//local@local:COOK/0.1')
        self.writeFile('foo.recipe', recipestr2)
        self.cookItem(repos, self.cfg, 'foo.recipe')
        cs = changeset.ChangeSetFromFile('foo-2.ccs')
        assert(cs.getPrimaryTroveList()[0][1].asString() == '/localhost@rpl:linux/1-1-0/test1//local@local:COOK/2-1-0.1')


    def testCookCommand(self):
        recipestr1 = """
class TestCook(PackageRecipe):
    name = 'foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        if not Use.readline:
            r.Create('/a')
        else:
            # policy error
            r.Run('exit 1')
"""
        # recipestr2 is backwards
        recipestr2 = """
class TestCook(PackageRecipe):
    name = 'foo'
    version = '2'
    clearBuildReqs()
    if not Use.readline:
        assert(0)

    def setup(r):
        if Use.readline:
            r.Create('/a')
        else:
            r.Run('exit 1')
"""
        repos = self.openRepository()
        self.overrideBuildFlavor('!readline')
        self.makeSourceTrove('foo', recipestr1)
        self.cookItem(repos, self.cfg, 'foo[!readline]')
        self.logFilter.add()
        self.assertRaises(RuntimeError, self.cookItem, repos, 
                          self.cfg, 'foo[readline]')
        self.logFilter.remove()
        self.updateSourceTrove('foo', recipestr2)
        # make sure readline is turned on in the build flavor
        self.overrideBuildFlavor('!readline')
        # turn it off here
        self.cookItem(repos, self.cfg, 'foo[readline]')
        self.logFilter.add()
        self.assertRaises(cook.CookError, self.cookItem, repos, 
                          self.cfg, 'foo[!readline]')
        self.logFilter.remove()
        # try cooking v. 1 again
        self.cookItem(repos, self.cfg, 'foo=1-1[!readline]')

    def testTroveInfo(self):
        """testTroveInfo: Test to ensure that various pieces of troveInfo
           make it through the cook process.
        """

        recipestr1 = """
class BuildReqClass(PackageRecipe):
    name = 'buildreq'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Create('/foo')
"""

        recipestr2 = """
class ExtraClass(PackageRecipe):
    name = 'extra'
    version = '1'
    clearBuildReqs()
    if Use.readline:
        pass

    def setup(r):
        r.Create('/a')
"""
        
        recipestr3 = """
loadRecipe('extra[!readline]')
class TestClass(CPackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()

    buildRequires = ['buildreq:runtime']

    def setup(r):
        r.Create('/usr/share/installedfile')
"""

        repos = self.openRepository()
        (built, d) = self.buildRecipe(recipestr1, "BuildReqClass")
        buildReqTup = ('buildreq:runtime', VFS(built[0][1]), built[0][2])
        self.updatePkg(self.cfg.root, 'buildreq:runtime')
        d = tempfile.mkdtemp(dir=self.workDir)
        os.chdir(d)
        self.newpkg('extra')
        os.chdir('extra')
        self.writeFile('extra.recipe', recipestr2)
        self.addfile('extra.recipe')
        self.commit()
        os.chdir('..')
        self.newpkg('test')
        os.chdir('test')
        self.writeFile('test.recipe', recipestr3)
        self.addfile('test.recipe')
        self.commit()
        built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                            'test')
        extraTup = ('extra:source', VFS('/localhost@rpl:linux/1-1'), 
                    deps.parseFlavor('~!readline'))
        v, f = built[0][0][1:]
        v = VFS(v)
        trv, dataComp = repos.getTroves((('test', v, f), ('test:data', v, f)))
        assert(trv.getBuildRequirements() == [buildReqTup])
        assert(trv.getLoadedTroves() == [extraTup])
        assert(trv.isCollection())
        assert(dataComp.getBuildRequirements() == [])
        assert(dataComp.getLoadedTroves() == [])
        assert(not dataComp.isCollection())

    def testCookOntoTargetLabel(self):
        repos = self.openRepository()
        trv = self.addComponent('simple:source', '1-1', '',
                                [('simple.recipe', recipes.simpleRecipe)])
        self.addComponent('simple:runtime', 
                            '/localhost@rpl:linux//branch/1-1-0.3')
        self.addCollection('simple', '/localhost@rpl:linux//branch/1-1-0.3',
                           [':runtime'])
        loader, sourceVersion = loadrecipe.recipeLoaderFromSourceComponent(
                                    'simple:source', self.cfg, repos,
                                    versionStr=str(trv.getVersion()),
                                    labelPath = self.cfg.buildLabel)
        targetLabel = versions.Label('localhost@rpl:branch')
        built = self.discardOutput(cook.cookObject,
                                repos, self.cfg, [ loader ],
                                targetLabel = targetLabel, 
                                sourceVersion = sourceVersion, 
                                prep = False)
        assert(built[0][1] == '/localhost@rpl:linux//branch/1-1-0.4')

        self.addComponent('simple:runtime', 
                            '/localhost@rpl:linux/1-1-5')
        self.addCollection('simple', '/localhost@rpl:linux/1-1-5',
                           [':runtime'])
        targetLabel = versions.CookLabel()
        built = self.discardOutput(cook.cookObject,
                                repos, self.cfg, [ loader ],
                                targetLabel = targetLabel, 
                                sourceVersion = sourceVersion, 
                                prep = False, 
                                changeSetFile='%s/tmp.ccs' % self.workDir)
        assert(built[0][1] == '/localhost@rpl:linux//local@local:COOK/1-1-0.1')

    def testCookShowBuildreqs(self):
        repos = self.openRepository()
        origDir = os.getcwd()
        os.chdir(self.workDir)
        try:
            f = open('foo.recipe', 'w')
            f.write("""class FooRecipe(PackageRecipe):
    name='foo'
    version='1.0'

    def setup(r):
        pass
""")
            f.close()
            ret, s = self.captureOutput(cook.cookItem, repos, self.cfg,
                                        'foo.recipe', showBuildReqs=True)
            reqs = packagerecipe.PackageRecipe.buildRequires
            reqs = set()
            reqs.update(packagerecipe.PackageRecipe.buildRequires)
            reqs.update(packagerecipe.BaseRequiresRecipe.buildRequires)
            assert(s == '\n'.join(sorted(x for x in reqs)) + '\n')

            ret, s = self.captureOutput(cook.cookCommand, 
                self.cfg, ['foo.recipe'], False, {}, showBuildReqs=True)
            assert(s == '\n'.join(sorted(x for x in reqs)) + '\n')

        finally:
            os.chdir(origDir)


    def testPrep(self):
        repos = self.openRepository()
        self.makeSourceTrove('foo',recipes.buildReqTest1)
        ret, s = self.captureOutput(cook.cookItem, repos, self.cfg,
                                    'foo', downloadOnly=True)
        self.logFilter.add()
        ret, s = self.captureOutput(cook.cookItem, repos, self.cfg,
                                    'foo', prep=True)
        self.logFilter.compare(['warning: Could not find the following troves needed to cook this recipe:\nblah'])
        self.assertRaises(builderrors.RecipeDependencyError,
                          cook.cookItem, repos, self.cfg, 'foo')
        self.logFilter.compare(['error: Could not find the following troves needed to cook this recipe:\nblah'])

    def testUnknownUseFlag(self):
        repos = self.openRepository()
        util.mkdirChain(self.workDir + '/use')
        self.writeFile(self.workDir + '/use/ffff', 'sense preferred')

        self.cfg.useDirs.append(self.workDir + '/use')
        self.cfg.initializeFlavors()
        use.setBuildFlagsFromFlavor(None, self.cfg.buildFlavor, error=False)

        self.makeSourceTrove('foo', recipes.unknownFlagRecipe)
        # make sure that the checkin code switched unknown use flag checking
        # back on after the commit
        self.failUnlessRaises(use.NoSuchUseFlagError, getattr, use.Use, 'totallyUnknown')
        self.cfg.useDirs = self.cfg.useDirs[:-1]
        del use.Use['ffff']
        self.cfg.flavor = [deps.parseFlavor('')]
        self.cfg.buildFlavor = deps.parseFlavor('')
        self.cfg.initializeFlavors()
        use.setBuildFlagsFromFlavor(None, self.cfg.buildFlavor, error=False)
        self.logFilter.add()
        try:
            self.cookItem(repos, self.cfg, 'foo')
            assert 0, "should have raised exception"
        except Exception, err:
            err = re.sub('[^ ]*\.recipe', 'RECIPE', str(err))
            assert(err == '''\
unable to load recipe file RECIPE:
Error in recipe file RECIPE", line 1:
 
An unknown use flag, Use.ffff, was accessed.  The default behavior
of conary is to complain about the missing flag, since it may be
a typo.  You can add the flag /etc/conary/use/ffff, or
${HOME}/.conary/use/ffff, or use the --unknown-flags option on
the command line to make conary assume that all unknown flags are
not relevant to your system.
''')

        built = self.cookItem(repos, self.cfg, 'foo', allowUnknownFlags=True)
        self.cfg.buildFlavor = deps.overrideFlavor(self.cfg.buildFlavor,
                                                deps.parseFlavor('bar'))
        self.logFilter.add()
        built = self.cookItem(repos, self.cfg, 'foo', allowUnknownFlags=True)
        self.logFilter.compare('warning: ignoring unknown Use flag bar')

    def testPathIdLookup(self):
        recipe = """
class %(cname)sPackage(PackageRecipe):
    name = '%(name)s'
    version = '1.%(minor)s'
    clearBuildReqs()
    def setup(r):
        for i in range(%(filecount)s):
            r.Create('/usr/share/largeish/build-%(minor)s/file%(fileno)s' %% i,
                     contents="Some content for file %(minor)s-%(fileno)s" %% i)
"""
        d = tempfile.mkdtemp(dir=self.workDir)

        pname = 'largish'
        cname = 'Largish'
        recipeFile = pname + '.recipe'
        origDir = os.getcwd()
        os.chdir(d)
        self.newpkg(pname)
        os.chdir(pname)
        filecount = 10
        buildcount = 3
        for i in range(buildcount):
            dct = {
                'name'      : pname,
                'cname'     : cname,
                'fileno'    : "%06d",
                'minor'     : i,
                'filecount' : filecount,
            }
            self.writeFile(recipeFile, recipe % dct)
            self.addfile(recipeFile)
            self.commit()
            repos = self.openRepository()
            self.logFilter.add()
            self.cookItem(repos, self.cfg, pname)
        filePrefixes = ["/usr/share/largeish/build-0"]
        branch = versions.VersionFromString("/" + str(self.defLabel))

        ids = repos.getPackageBranchPathIds(pname + ':source', branch,
                                            filePrefixes)
        self.failUnlessEqual(len(ids), filecount)
        # Simulate old client against new server
        ids = repos.getPackageBranchPathIds(pname + ':source', branch)
        self.failUnlessEqual(len(ids), filecount * buildcount)

        # Confirm new clients don't send the file prefix to old servers
        currentProtocolVersion = repos.c[branch].getProtocolVersion()
        repos.c[branch].setProtocolVersion(38)
        ids = repos.getPackageBranchPathIds(pname + ':source', branch,
                                            filePrefixes)
        self.failUnlessEqual(len(ids), filecount * buildcount)
        # Restore protocol
        repos.c[branch].setProtocolVersion(currentProtocolVersion)

        # Really change the client's version (against new server)
        from conary.repository import netclient
        currentClientVersions = netclient.CLIENT_VERSIONS[:]
        del netclient.CLIENT_VERSIONS[:]
        netclient.CLIENT_VERSIONS.extend([36, 37])

        # The server will happily serve stuff to old clients if the file
        # prefixes were presented.
        try:
            ids = repos.getPackageBranchPathIds(pname + ':source', branch,
                                                filePrefixes)
        finally:
            # Restore
            del netclient.CLIENT_VERSIONS[:]
            netclient.CLIENT_VERSIONS.extend(currentClientVersions)

        self.failUnlessEqual(len(ids), filecount)

    def testPathIdDirnamesLookup(self):
        # CNY-2743
        # up until protocol 62, we were sending prefixes, after that
        # we send a full dirnames list
        branch = versions.VersionFromString("/" + str(self.defLabel))
        self.addComponent('foo:source', '1.0')
        self.addComponent('foo:foo', '1.0', '', [('/usr/foo', 'foo test\n')])
        self.addComponent('foo:runtime', '1.0', '',
                          [('/usr/bin/foo', 'foo\n'),
                           ('/usr/sbin/foo', 'foo\n'),
                           ])
        self.addComponent('foo:doc', '1.0', '',
                          [('/usr/share/man/man1/foo.1', 'foo\n'),
                           ('/usr/share/man/man3/foo.3', 'foo\n'),
                           ])
        self.addCollection("foo", "1.0",
                           [ "foo:runtime", "foo:doc", "foo:foo" ],
                           sourceName = "foo:source")
        # old style - get everything we know
        repos = self.openRepository()
        allIds = repos.getPackageBranchPathIds("foo:source", branch)
        # /usr will match strictly now
        ret1 = repos.getPackageBranchPathIds("foo:source", branch, ["/usr"])
        self.failUnlessEqual(set(ret1.keys()), set(["/usr/foo"]))
        oldVer = repos.c[branch].getProtocolVersion()
        # now /usr will be treated as a prefix and all files will be returned
        repos.c[branch].setProtocolVersion(61)
        ret1 = repos.getPackageBranchPathIds("foo:source", branch, ["/usr"])
        self.failUnlessEqual(ret1, allIds)
        
        
    @protect
    def testFlavoredPackagePathIdLookup(self):
        recipe = """
class TestCase(PackageRecipe):
    name='foo'
    version='1.0'
    clearBuildReqs()

    def setup(r):
        if Use.krb:
            r.Create('/usr/share/foo/somefile', contents='With krb\\n')
        else:
            r.Create('/usr/share/foo/somefile', contents='Without krb\\n')
"""

        branch = versions.VersionFromString("/" + str(self.defLabel))
        repos = self.openRepository()
        os.chdir(self.workDir)
        self.newpkg('foo')
        os.chdir('foo')

        self.writeFile('foo.recipe', recipe)
        self.addfile('foo.recipe')
        self.commit()

        filePrefixes = ['/usr/share/foo']

        fileIds = []
        fileVersions = []

        flavors = [ "krb", "!krb" ]
        # Build both flavors
        for f in flavors:
            tobuild = "foo[%s]" % f
            ccsfile = "foo:%s.ccs" % f.replace("!", "-")
            trvname = "foo:data[%s]" % f
            built, out = self.cookItem(repos, self.cfg, tobuild)
            self.changeset(repos, [ trvname, ], ccsfile)

            cs = changeset.ChangeSetFromFile(ccsfile)
            trv = cs.iterNewTroveList().next()
            fileobj = trv.getNewFileList()[0]
            fileIds.append(fileobj[2])
            fileVersions.append(fileobj[3])

        # Now, make sure that getPackageBranchByPathId picks up the correct
        # file

        for fileId in fileIds:
            ids = repos.getPackageBranchPathIds('foo:source', branch,
                                                filePrefixes, [fileId])
            self.failUnlessEqual(len(ids), 1)
            srvFileId = ids.values()[0][2]
            self.failUnlessEqual(fileId, srvFileId)

        # Build both flavors again
        fileIds2 = []
        fileVersions2 = []
        for f in flavors:
            tobuild = "foo[%s]" % f
            ccsfile = "foo:%s-2.ccs" % f.replace("!", "-")
            trvname = "foo:data[%s]" % f
            built, out = self.cookItem(repos, self.cfg, tobuild)
            self.changeset(repos, [ trvname, ], ccsfile)

            cs = changeset.ChangeSetFromFile(ccsfile)
            trv = cs.iterNewTroveList().next()
            fileobj = trv.getNewFileList()[0]
            fileIds2.append(fileobj[2])
            fileVersions2.append(fileobj[3])

        self.failUnlessEqual(fileIds, fileIds2)
        self.failUnlessEqual(fileVersions, fileVersions2)

    @protect
    def testLookupPathIdLotsOfFiles(self):
        raise testhelp.SkipTestException("CNY-1203: use this test to do "
              "timing of code")
        recipe = """
class %(cname)sPackage(PackageRecipe):
    name = '%(name)s'
    version = '1.%(minor)s'
    clearBuildReqs()
    def setup(r):
        for i in range(%(filecount)s):
            r.Create('/usr/share/largeish/build-%(minor)s/file%(fileno)s' %% i,
                contents="Some content for file %(minor)s-%(fileno)s" %% i)
"""
        d = tempfile.mkdtemp(dir=self.workDir)

        pname = 'largish'
        cname = 'Largish'
        recipeFile = pname + '.recipe'

        os.chdir(d)
        self.newpkg(pname)
        os.chdir(pname)
        filecount = 12000
        buildcount = 3

        repos = self.openRepository()

        for i in range(buildcount):
            dct = {
                'name'      : pname,
                'cname'     : cname,
                'fileno'    : "%06d",
                'minor'     : i,
                'filecount' : filecount,
            }
            self.writeFile(recipeFile, recipe % dct)
            if not i: self.addfile(recipeFile)
            self.commit()

            self.cookItem(repos, self.cfg, pname)

    def testLookupPathIdsFlavors(self):
        self.addComponent('simple:runtime=1[~!builddocs]')
        self.addCollection('simple=1[~!builddocs]', [':runtime'])
        simpleRecipe = recipes.simpleRecipe + '\n\tif Use.builddocs: pass'
        os.chdir(self.workDir)
        self.writeFile('simple.recipe', simpleRecipe)
        item = ('simple.recipe', None, [deps.parseFlavor('~builddocs')])
        repos = self.openRepository()
        built, d = self.cookItem(repos, self.cfg, item)
        cs = changeset.ChangeSetFromFile('simple-1.ccs')
        assert(cs.getPrimaryTroveList()[0]
            in [ x.getNewNameVersionFlavor() for x in cs.iterNewTroveList() ])
        assert(str(cs.getPrimaryTroveList()[0][2]) == '~builddocs')

    def testLookupPathIdsWithMissingComponent(self):
        # CNY-2250
        repos = self.openRepository()
        self.addComponent('foo:source', ':1')
        self.addComponent('foo:runtime', ':1', '',
                          [('/bin/foo', 'foo\n')])
        self.addComponent('foo:doc', ':1', '',
                          [('/usr/share/man/man1/foo.1', 'foo\n')])
        self.addCollection("foo", ":1/1.0-1-1",
                           [ "foo:runtime", "foo:doc" ],
                           sourceName = "foo:source")
        self.addComponent('foo:source', ':1/2')
        self.addComponent('foo:runtime', ':1/2', '',
                          [('/bin/foo', 'foo\n')])
        self.addCollection("foo", ":1/2-1-1",
                           [ "foo:runtime", "foo:doc" ],
                           sourceName = "foo:source")
        branch = versions.VersionFromString("/localhost@rpl:1")
        branchIds = repos.getPackageBranchPathIds("foo:source", branch)
        self.failUnlessEqual(branchIds, {
            '/usr/share/man/man1/foo.1':
            ('\x9f\x8a\x12\xb9/\x13`\xcbd\xab\x87\xe0Gu\x1b\x13',
             VFS('/localhost@rpl:1/1-1-1'),
             '\xad\xbeGk!\xeesP\x0e\x1b\xd2\xba\xe5\xe2\x05\xf1\xb4\xfa\xda#'),
            '/bin/foo':
            ('\x0btx\xa2\xa7Uq1\xab\x82xts\x9c|\x87',
             VFS('/localhost@rpl:1/2-1-1'),
             '\xad\xbeGk!\xeesP\x0e\x1b\xd2\xba\xe5\xe2\x05\xf1\xb4\xfa\xda#')
            })
        fileIdsPathMap = { '/blah': '1' * 20,
                           '/bin/foo': '0' * 20,
                           '/usr/share/man/man1/foo.1': '0' * 20 }
        targetVersion = versions.VersionFromString('/localhost@rpl:1/3-1-1')
        ident = cook._getPathIdGen(repos, 'foo:source', targetVersion, None,
                                   ['foo'], fileIdsPathMap)
        self.failUnlessEqual(ident.map, branchIds)
        self.failUnlessEqual(fileIdsPathMap, { '/blah': '1' * 20 })

    def testRelativePackageCommit(self):
        def check(cs, *args, **kwargs):
            for trvCs in cs.iterNewTroveList():
                if ':' not in trvCs.getName(): continue
                assert(trvCs.getOldVersion())

        (built, d) = self.buildRecipe(recipes.testTransientRecipe1,
                                      "TransientRecipe1")
        repos = self.openRepository()
        repos._commit = check
        commitRelativeChangeset = self.cfg.commitRelativeChangeset
        self.cfg.commitRelativeChangeset = True
        (built, d) = self.buildRecipe(recipes.testTransientRecipe1,
                                      "TransientRecipe1",
                                      repos = repos)
        self.cfg.commitRelativeChangeset = commitRelativeChangeset

    def testAbsolutePackageCommit(self):
        def check(cs, *args, **kwargs):
            for trvCs in cs.iterNewTroveList():
                if ':' not in trvCs.getName(): continue
                assert(not trvCs.getOldVersion())

        (built, d) = self.buildRecipe(recipes.testTransientRecipe1,
                                      "TransientRecipe1")
        repos = self.openRepository()
        repos._commit = check
        commitRelativeChangeset = self.cfg.commitRelativeChangeset
        self.cfg.commitRelativeChangeset = False
        (built, d) = self.buildRecipe(recipes.testTransientRecipe1,
                                      "TransientRecipe1",
                                      repos = repos)
        self.cfg.commitRelativeChangeset = commitRelativeChangeset

    def testCookWithPackageFlags(self):
        packageFlagRecipe = """
class MyPackage(PackageRecipe):
    name = 'packageflag'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        if PackageFlags.kernel.pae:
            r.Create('/true')
        else:
            r.Create('/false')
"""
        trv = self.build(packageFlagRecipe, "MyPackage")
        assert(set([ x[1] for x in trv.iterFileList()]) == set(['/false']))
        self.overrideBuildFlavor('kernel.pae')
        trv = self.build(packageFlagRecipe, "MyPackage")
        assert(set([ x[1] for x in trv.iterFileList()]) \
                == set(['/true']))
        self.overrideBuildFlavor('!kernel.pae')
        trv = self.build(packageFlagRecipe, "MyPackage")
        assert(set([ x[1] for x in trv.iterFileList()]) == set(['/false']))

    def testCookRemoveBadPermissions(self):
        testPackageRecipe = """
class TestPackage(PackageRecipe):
    name = 'testpkg'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        r.Create("/bar.txt", contents="some")
"""
        bdir = os.path.join(self.buildDir, 'testpkg')
        util.rmtree(bdir, ignore_errors=True)
        util.mkdirChain(bdir)
        # Create read-only dir
        rodir = os.path.join(bdir, 'read-only-dir')
        os.mkdir(rodir, 0000)

        self.logFilter.add()
        self.build(testPackageRecipe, "TestPackage")
        self.logFilter.compare("warning: working around illegal mode "
            "040000 at %s" % rodir)

        # Fail if the read only directory is still around.
        self.failIf(os.path.exists(rodir))

    def testCookMultipleGroupFlavorsWithLocalFlags(self):
        groupFoo = """
class GroupFoo(GroupRecipe):
    name = 'group-foo'
    version = '0.0.4'
    clearBuildRequires()

    if Use.ssl:
        Flags.foo = False

    def setup(r):
        if Use.ssl:
            if Flags.foo:
                pass
        r.add('setup:runtime')
"""
        self.writeFile(self.workDir + '/group-foo.recipe', groupFoo)
        self.addComponent('setup:runtime')
        os.chdir(self.workDir)
        flavors = (deps.parseFlavor('ssl,group-foo.foo'),
                   deps.parseFlavor('ssl'),
                   deps.parseFlavor('group-foo.foo,!ssl'))
        item = ('group-foo.recipe', None, flavors)
        repos = self.openRepository()
        built, d = self.cookItem(repos, self.cfg, item)
        builtFlavors = [x[2] for x in built]
        assert(builtFlavors == [deps.parseFlavor('~group-foo.foo,ssl'),
                                deps.parseFlavor('~!group-foo.foo,ssl'),
                                deps.parseFlavor('~!ssl')])

        cs = changeset.ChangeSetFromFile(d)
        trvcs = cs.iterNewTroveList().next()
        trv = trove.Trove(trvcs)
        self.failUnless(str(trv.getBuildFlavor()))

    def testCookMultiplePrefixesNonAsciiChars(self):
        # CNY-1932
        # Have several prefixes that have non-UTF8 chars in them. This will
        # call getPackageBranchPathIds with those prefies as filePrefixes,
        # which will get encoded in XMLRPC. We should now transparently handle
        # the base64 encoding/decoding

        recipe = """\
class Test(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()

    def setup(r):
        r.Create("/usr/share/dir1\x80/foo1.txt", contents = "file 1")
        r.Create("/usr/share/dir2\x80/foo1.txt", contents = "file 2")
        r.NonUTF8Filenames(exceptions = ".*")
"""
        self.makeSourceTrove('test', recipe)
        ret = self.cookFromRepository('test')

    def testCookSparcFile(self):
        # ensure that this file doesn't show up as flavored
        # sparc - unless we're on a sparc system.  (CNY-1717)
        recipe = """
class Test(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()

    def setup(r):
        r.addSource("sparc-libelf-0.97.so", dir='%(libdir)s/')
"""
        trv = self.build(recipe, "Test")
        for pathId, path, fileId, fileVer in trv.iterFileList():
            if path.endswith('sparc-libelf-0.97.so'):
                break
        else:
            assert(0)
        repos = self.openRepository()
        fileObj = repos.getFileVersion(pathId, fileId, fileVer)
        archFlavor = deps.getInstructionSetFlavor(self.cfg.buildFlavor)
        assert(fileObj.flavor() == deps.parseFlavor('is: sparc'))
        assert(trv.getFlavor().isEmpty())
        assert(trv.getRequires().isEmpty())
        self.overrideBuildFlavor('is: sparc')
        trv = self.build(recipe, "Test")
        fileObj = repos.getFileVersion(pathId, fileId, fileVer)
        archFlavor = deps.getInstructionSetFlavor(self.cfg.buildFlavor)
        assert(fileObj.flavor() == deps.parseFlavor('is: sparc'))
        assert(trv.getFlavor() == archFlavor)
        assert(not trv.getRequires().isEmpty())

    def testCallSetup(self):
        # ensure we don't get a "cleaning build tree" message
        shutil.rmtree(self.cfg.buildPath, ignore_errors = True)
        recipe = """

class Test(AutoPackageRecipe):
    name = 'test'
    version = '1.0'

    clearBuildReqs()
    def unpack(r):
        r.Create('/foo')
        r.foo()

    def foo(r):
        pass

    def bar(r):
        pass
"""
        repos = self.openRepository()
        self.makeSourceTrove('test', recipe)
        self.logFilter.add()
        log.setVerbosity(log.INFO)
        ret, s = self.captureOutput(cook.cookItem, repos, self.cfg,
                                    'test', prep=True)
        assert('\n'.join(self.logFilter.records) == """\
+ Methods called:
  AutoPackageRecipe.setup
    Test.unpack
      Test.foo
    AutoPackageRecipe.configure
    AutoPackageRecipe.make
    AutoPackageRecipe.makeinstall
    AutoPackageRecipe.policy
+ Unused methods:
  Test.bar
+ Building test=localhost@rpl:linux[]""")

    def testSetupBlacklist(self):
        # ensure we don't get a "PackageRecipe.setupAbstractBaseClass" message
        shutil.rmtree(self.cfg.buildPath, ignore_errors = True)
        recipe = """

class Test(PackageRecipe):
    name = 'test'
    version = '1.0'

    clearBuildReqs()
    def setup(r):
        r.Create('/foo')
"""
        repos = self.openRepository()
        self.makeSourceTrove('test', recipe)
        self.logFilter.add()
        log.setVerbosity(log.INFO)
        ret, s = self.captureOutput(cook.cookItem, repos, self.cfg,
                                    'test', prep=True)
        assert('\n'.join(self.logFilter.records) == """\
+ Methods called:
  Test.setup
+ Building test=localhost@rpl:linux[]""")

    def testSuperClassSetup(self):
        # ensure we don't get a "PackageRecipe.setupAbstractBaseClass" message
        shutil.rmtree(self.cfg.buildPath, ignore_errors = True)
        recipe = """

class Test(PackageRecipe):
    name = 'test'
    version = '1.0'
    abstractBaseClass = True

    clearBuildReqs()
"""
        repos = self.openRepository()
        self.makeSourceTrove('test', recipe)
        self.logFilter.add()
        log.setVerbosity(log.INFO)
        ret, s = self.captureOutput(cook.cookItem, repos, self.cfg,
                                    'test', prep=True)
        self.failUnlessEqual('\n'.join(self.logFilter.records[:5]),"""\
+ Methods called:

+ Building test=localhost@rpl:linux[]
+ adding source file test.recipe
+ found test.recipe in repository
+ test.recipe not yet cached, fetching...""")
        self.failUnless(re.match('\+ copying [^\s]*? to [^\s]*?',
                self.logFilter.records[-1]))

    def testSuperClassOverride(self):
        # ensure we use group based classes in favor of internals
        self.addComponent('packagerecipe:recipe', '0.1', fileContents = [
                    ('packagerecipe.recipe',
'''
class PackageRecipe(PackageRecipe):
    name = 'packagerecipe'
    version = '0.1'
    local = True
    clearBuildReqs()
''') ] )
        self.addCollection('packagerecipe=0.1', [ ':recipe' ])

        self.makeSourceTrove('test',
'''
class TestRecipe(PackageRecipe):

    version = '1.1';
    name = 'test'

    def setup(r):
        r.Create('/value', str(r.local))
''')

        repos = self.openRepository()

        try:
            self.cfg.autoLoadRecipes.append('packagerecipe:recipe')
            ret, s = self.captureOutput(cook.cookItem, repos, self.cfg, 'test')
            ret = ret[0][0]

            trvInfo = (ret[0].split(':')[0],
                       versions.VersionFromString(ret[1]), ret[2])
            trv = repos.getTrove(*trvInfo)
        finally:
            del self.cfg.autoLoadRecipes[:]

        (trvInfo,) = trv.getLoadedTroves()
        assert((trvInfo[0], str(trvInfo[1]), str(trvInfo[2])) ==
                    ('packagerecipe:recipe', '/localhost@rpl:linux/0.1-1-1',
                     ''))

        return

    def testSuperClassFlavor2(self):
        # ensure flavors referred to in a superclass don't cause
        # flavored binary recipe troves
        loadrecipe.RecipeLoaderFromString._defaultsLoaded = False
        recipeStr = """
class FooRecipe(PackageRecipe):
    name = 'foo'
    version = '1'

    clearBuildReqs()
    abstractBaseClass = 1

    def setupAbstractBaseClass(r):
        # override the default setup. we have no source to add, so we'll
        # create instead. the contents don't matter, we are testing flavors
        r.Create('/usr/share/conary/baseclasses/foo.recipe')
        bool(Use.readline)
        bool(Arch.x86)

        Flags.smp = True
        bool(Flags.smp)
"""
        self.resetRepository()
        self.openRepository()
        self.logFilter.add()
        loadrecipe.RecipeLoader._defaultsLoaded = False
        (built, d) = self.buildRecipe(recipeStr, 'FooRecipe')
        self.logFilter.remove()
        self.assertEquals(built[0][0], 'foo:recipe')
        self.assertEquals(built[0][-1], deps.parseFlavor(''))

    def testCookPromotesMetadata(self):
        self.addComponent('simple:runtime=1', 
                  metadata=self.createMetadataItem(licenses=['one', 'two']))
        self.addComponent('simple:data=2',
                  metadata=self.createMetadataItem(licenses=['three', 'four']))
        self.addCollection('simple=2', [':data'], 
                  metadata=self.createMetadataItem(shortDesc='simple'))
        self.addComponent('simple:source',
                          [('simple.recipe', recipes.simpleRecipe)])
        repos = self.openRepository()
        log.setVerbosity(log.INFO)
        built, str = self.captureOutput(cook.cookItem, repos, self.cfg, 'simple')
        lines = str.split('\n')
        assert('+ Copying forward metadata to newly built items...' in lines)
        assert('+ Copied metadata forward for simple[] from version /localhost@rpl:linux/2-1-1[]' in lines)
        assert('+ Copied metadata forward for simple:runtime[] from version /localhost@rpl:linux/1-1-1[]' in lines)

        assert(built[0][0])
        md = self.findAndGetTrove('simple').getMetadata()
        self.assertEquals(md['shortDesc'], 'simple')
        md = self.findAndGetTrove('simple:runtime').getMetadata()
        self.assertEquals(md['licenses'], ['one', 'two'])
        built, str = self.captureOutput(cook.cookItem, repos, self.cfg, 'simple')
        md = self.findAndGetTrove('simple:runtime').getMetadata()
        self.assertEquals(md['licenses'], ['one', 'two'])

        # next test group promoting
        groupFoo = """
class GroupFoo(GroupRecipe):
    name = 'group-foo'
    version = '1'
    clearBuildRequires()

    def setup(r):
        r.setLabelPath('localhost@rpl:linux')
        r.add('simple:runtime')
        r.createGroup('group-bar')
        r.add('simple:data', groupName='group-bar')
"""
        self.addCollection('group-foo', ['simple:runtime'],
                metadata=self.createMetadataItem(licenses=['five', 'six']))
        self.addComponent('group-foo:source', [('group-foo.recipe', groupFoo)])
        built = self.cookItem(repos, self.cfg, 'group-foo')
        assert(built[0][0])
        md = self.findAndGetTrove('group-foo').getMetadata()
        self.assertEquals(md['licenses'], ['five', 'six'])
        md = self.findAndGetTrove('group-bar').getMetadata()
        assert(set(md.values()) == set([None]))

    def testMetadataMatching(self):
        def getTroves(spec, compList):
            name, versionSpec, flavor = cmdline.parseTroveSpec(spec)
            version = self._cvtVersion(versionSpec)
            comps = [name] + [ name + ':' + x for x in compList]
            return [ trove.Trove(name, version, flavor) for x in comps ]

        def getTrove(spec):
            name, versionSpec, flavor = cmdline.parseTroveSpec(spec)
            version = self._cvtVersion(versionSpec)
            return trove.Trove(name, version, flavor)

        class RecipeObj(object):
            def __init__(self, buildBranch='/localhost@rpl:linux', 
                         skipSet = None):
                self.macros = macros.Macros()
                self.macros.buildbranch = buildBranch
                if skipSet is None:
                    skipSet = []
                self.metadataSkipSet = skipSet

        repos = self.openRepository()
        recipeObj = RecipeObj()

        foo1 = self.addComponent('foo:run', '1', 'ssl')
        foo2 = self.addComponent('foo:run', '2', '!ssl')
        trv = getTrove('foo:run=2[ssl]')
        buildBranch = versions.VersionFromString('/localhost@rpl:linux')
        matches = cook._getMetadataMatches(repos, [trv.getNameVersionFlavor()], 
                                           buildBranch)
        assert(matches == {
                    trv.getNameVersionFlavor(): foo1.getNameVersionFlavor()})
        # this time we didn't rebuilt ssl with the last version
        # so we grab the metadata from the !ssl version.
        trv = getTrove('foo:run=3[ssl]')
        matches = cook._getMetadataMatches(repos, [trv.getNameVersionFlavor()], 
                                           buildBranch)
        assert(matches == {
                    trv.getNameVersionFlavor(): foo2.getNameVersionFlavor()})

        foo2ssl = self.addComponent('foo:run', '2', '~ssl')
        matches = cook._getMetadataMatches(repos, [trv.getNameVersionFlavor()], 
                                           buildBranch)
        # prefer matching with ~ssl instead of !ssl when we need to match
        # requires ssl to something.
        assert(matches == {
                    trv.getNameVersionFlavor(): foo2ssl.getNameVersionFlavor()})

    def testCookPromotesMetadataNoRepos(self):
        # CNY-2640
        os.chdir(self.workDir)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.notByDefaultRecipe)
        self.addfile('testcase.recipe')
        self.commit()
        log.setVerbosity(log.INFO)
        # pass None in as the repos, like "cvc cook" does if it is unable
        # to open the repository
        built, str = self.captureOutput(cook.cookItem, None, self.cfg,
                                        'testcase.recipe')
        lines = str.split('\n')
        self.failUnless('+ No repository available, not copying forward metadata.' in lines)

    def testCookPromotesMetadataMissingTrove(self):
        # CNY-2611
        # runtime doesn't exist due to an error in the repository.
        # Let's make sure that we can still cook a replacement trove.
        self.addComponent('simple:data=2',
                  metadata=self.createMetadataItem(licenses=['three', 'four']))
        self.addCollection('simple=2', [':data', ':runtime'], 
                  metadata=self.createMetadataItem(shortDesc='simple'))
        self.addComponent('simple:source',
                          [('simple.recipe', recipes.simpleRecipe)])
        repos = self.openRepository()
        log.setVerbosity(log.INFO)
        built, str = self.captureOutput(cook.cookItem, repos, self.cfg, 'simple')

    def testCookFromNewPackage(self):
        # CNY-2661
        os.chdir(self.workDir)
        self.newpkg('test')
        os.chdir('test')
        self.writeFile('test.recipe',
"""
class TestRecipe(PackageRecipe):
    clearBuildReqs()
    name = "test"
    version = "1.0"
    def setup(r):
        r.Create("/ec/foo", "contents")
""")

        self.addfile('test.recipe')

        cstate = state.ConaryStateFromFile('CONARY')
        self.discardOutput(cook.cookCommand, self.cfg, [ cstate ], False, {})
        assert(os.path.exists('test-1.0.ccs'))

    def testCookFileWithPercent(self):
        os.chdir(self.workDir)

        self.newpkg('test')
        os.chdir('test')
        self.writeFile('%foo', "Contents")
        util.execute('tar -czf foo.tgz %foo')
        self.writeFile('test.recipe',
"""
class TestRecipe(PackageRecipe):
    clearBuildReqs()
    name = "test"
    version = "1.0"
    def setup(r):
        r.addArchive("foo.tgz", dir="/", preserveOwnership=True)
""")

        self.addfile('test.recipe')

        cstate = state.ConaryStateFromFile('CONARY')
        self.discardOutput(cook.cookCommand, self.cfg, [ cstate ], False, {})
        assert(os.path.exists('test-1.0.ccs'))

    def testCookItemChangeSetFile(self):
        os.chdir(self.workDir)
        self.newpkg('test')
        os.chdir('test')
        self.writeFile('test.recipe',
"""
class TestRecipe(PackageRecipe):
    clearBuildReqs()
    name = 'test'
    version = '1.0'
    def setup(r):
        r.Create('/foo', contents='bar')
""")

        self.addfile('test.recipe')
        self.commit()

        repos = self.openRepository()
        built, out = self.captureOutput(self.cookItem, repos, self.cfg, 'test',
                                        changeSetFile='test.ccs')

        # Make sure the changeset was written to the requested filename in the
        # current working directory.
        self.failUnless(os.path.exists('test.ccs'))

        # Make sure the changeset was not committed to the repository.
        self.failUnlessRaises(errors.TroveNotFound, repos.findTrove, self.cfg.buildLabel, ('test', None, None))
