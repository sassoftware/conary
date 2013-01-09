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

from conary.lib import util
from conary_test import rephelp


recipe1="""
class TestTestComponent(PackageRecipe):
    name = 'testpkg'
    version = '1.0'
    clearBuildReqs()
    
    def setup(r):
        r.Run('''mkdir TestFoo;
                 touch TestFoo/bar;
                 touch Makefile''')
        # now for creating files in destdir that are linked to 
        # builddir files
        r.Run('''mkdir bin;
                 echo "aaa" > bin/exec1;
                 mkdir doc;
                 echo "bbb" > doc/exec1''')
        r.Install('bin/exec1', '/bin/exec1', mode=0755) 
        r.Install('doc/exec1', '%(thisdocdir)s/exec1')
        # some files that should not be copied over to test component
        # (unless asked to be)
        r.Run('''touch random1;
                 touch random2;
                 touch bin/randomexec;
                 touch doc/randomexec''')
        #different tests can append policy here
        
"""

recipe2="""
class TestTestComponent(PackageRecipe):
    name = 'testpkg'
    version = '1.0'
    clearBuildReqs()
    
    def setup(r):
        r.Run('''mkdir TestFoo;
                 touch TestFoo/bar;
                 touch TestFoo/name\ with\ spaces;
                 touch Makefile''')

        # now for creating files in destdir that are linked to 
        # builddir files
        r.Run('''mkdir bin;
                 touch bin/exec1;
                 ln -s bin/exec1 execlink
              ''')

        r.Create('/usr/local/test/bin/exec1')
        r.Symlink('/usr/local/test/bin/exec1', '/usr/bin/execlink') 
"""

# XXX TODO Tests For Test Components:
# * Dest Directory contains symlinks to installed files
# * How symlinks in destdir are handled
# * Contents of conary-test-command
# * Makefile Munging

class TestCompTest(rephelp.RepositoryHelper):
    def _ddexists(self, path):
        # XXX I think this does the wrong thing with symlinks
        return os.path.exists(util.joinPaths(self.workDir, path))

    def _ddpath(self, path):
        return  util.joinPaths(self.workDir, path)

    def _testdirexists(self, path):
        return os.path.exists(self._testdirpath(path))

    def _testdirpath(self, path):
        dirpath = util.joinPaths(self.workDir, '/var/conary/tests/testpkg-1.0/')
        return util.joinPaths(dirpath, path)

    def _testdirfile(self, path):
        fullpath = self._testdirpath(path)
        if os.path.islink(fullpath):
            return False
        return os.path.exists(fullpath)

    #link that stays within testdir
    def _testdirinlink(self, path, target):
        fullpath = self._testdirpath(path)
        tpath = self._testdirpath(target)
        if os.path.islink(fullpath):
            contents = os.readlink(fullpath)
            if contents[0] == '/':
                return contents == tpath
            else:
                return util.normpath(os.path.join(os.path.dirname(fullpath), contents)) == tpath
        return False


    #link pointing to dest dir
    def _testdirlink(self, path, target):
        fullpath = self._testdirpath(path)
        tpath = self._ddpath(target)
        if os.path.islink(fullpath):
            contents = os.readlink(fullpath)
            if contents[0] == '/':
                return contents == tpath
            else:
                return util.normpath(os.path.join(os.path.dirname(fullpath), contents)) == tpath
        return False


    def testCompTestBasic(self):
        self.reset()
        recipe = recipe1 + '''
        r.TestSuite('.', 'runtest')'''
        (built, d) = self.buildRecipe(recipe, "TestTestComponent")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        assert(self._ddexists('/bin/exec1'))
        assert(self._ddexists('/usr/share/doc/testpkg-1.0/exec1'))

        # These files should be created in the test dir
        assert(self._testdirfile('Makefile'))
        assert(self._testdirfile('TestFoo/bar'))

        # These links should be created
        assert(self._testdirlink('/bin/exec1', '/bin/exec1'))

        assert(not self._testdirexists('/doc/exec1'))


    def testCompTestNoBuildTestDir(self):
        """ When there's no TestCommand it should not build test component automatically """
        recipe1b = recipe1 
        self.reset()
        (built, d) = self.buildRecipe(recipe1b, "TestTestComponent")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        assert(not self._ddexists('/var/conary/tests'))

    def testCompTestBuildTestDirWhenNoTest(self):
        """ When there's no TestCommand it should build test component when we tell it to explicitly """
        recipe1b = recipe1 + '''
        r.TestSuiteLinks(build=True)'''
        self.reset()
        (built, d) = self.buildRecipe(recipe1b, "TestTestComponent")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        assert(self._ddexists('/var/conary/tests'))

    def testCompTestIncludeExcludeFiles(self):
        """ Test including and excluding files for copying """
        recipe1b = recipe1 + '''
        r.TestSuite('.', 'random command')
        r.TestSuiteFiles(exceptions='TestFoo/bar')
        r.TestSuiteFiles('random1')'''
        self.reset()
        (built, d) = self.buildRecipe(recipe1b, "TestTestComponent")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        assert(self._testdirfile('random1'))
        assert(not self._testdirfile('TestFoo/bar'))

    def testCompTestSpecifiedSymlinks(self):
        """ Tell it to specifically link an installed file to 
            a testdir file of a specific name  """
        recipe1b = recipe1 + '''
        r.TestSuite('.', 'bogus test command')
        r.TestSuiteLinks(fileMap={'/random1' : '%(thisdocdir)s/exec1'})'''
        self.reset()
        (built, d) = self.buildRecipe(recipe1b, "TestTestComponent")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        assert(self._testdirlink('random1', '/usr/share/doc/testpkg-1.0/exec1'))

    def testCompTestAdvancedTests(self):
        """ Test more esoteric (but necessary) features """
        self.reset()
        recipe = recipe2 + '''
        r.TestSuite('.', 'foo')'''
        (built, d) = self.buildRecipe(recipe, "TestTestComponent")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        assert(self._testdirinlink('execlink', 'bin/exec1'))
        assert(self._testdirlink('bin/exec1', '/usr/local/test/bin/exec1'))
#
    def testAACompNoFileMatchWarning(self):
        """ Test behavior when no file in build dir matches dest dir file """
        recipe1b = recipe1 + '''
        #overwrite exec1 val so it no longer matches installed value
        r.Run('echo "ccc" > bin/exec1')'''

        self.reset()
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipe1b, "TestTestComponent")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        self.logFilter.remove()

        recipe1b = recipe1 + '''
        # Remove exec1s so nothing matches this file
        r.Run('rm bin/exec1')
        r.Run('rm doc/exec1')'''
        self.reset()
        self.logFilter.add()
        (built, d) = self.buildRecipe(recipe1b, "TestTestComponent")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1], depCheck=False)
        self.logFilter.remove()
        assert(not self._testdirfile('bin/exec1'))
