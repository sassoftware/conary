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


from testrunner.testhelp import context

import copy
import sys
from StringIO import StringIO
import time

from conary.build import errors as cvcerrors, packagerecipe, source
from conary import checkin
from conary import deps
from conary import errors
from conary import files, keymgmt
from conary import state
from conary.repository import netclient
import gzip
import os
from conary.cmds import queryrep
from conary_test import recipes
from conary_test import rephelp
from conary_test import resources
import shutil
import stat
import tempfile
from conary import versions
from conary.lib import log, openpgpfile, sha1helper, util

from buildtest import lookasidetest

diff0 = """\
(working version) (no log message)

test.newsource: new
--- /dev/null
+++ test.newsource
@@ -0,0 +3 @@
+some
+test contents
+final line

test.fifo: removed
"""

diff1 = """\
(working version) (no log message)

testcase.recipe: changed
Index: testcase.recipe
====================================================================
contents(size sha1)
--- testcase.recipe /localhost@rpl:linux/1.0-3
+++ testcase.recipe @NEW@
@@ -41,3 +41,4 @@
         self.Ownership(self.owner, self.group, '.*')
         self.ComponentSpec('runtime', '%(datadir)s/', '%(sysconfdir)s/')
         self.Strip(debuginfo=False)
+# some comments


"""

diff2 = """\
(working version) (no log message)

testcase.recipe: changed
Index: testcase.recipe
====================================================================
contents(size sha1)
--- testcase.recipe /localhost@rpl:linux/1.0-1/branch/1
+++ testcase.recipe @NEW@
@@ -41,3 +41,4 @@
         self.Ownership(self.owner, self.group, '.*')
         self.ComponentSpec('runtime', '%(datadir)s/', '%(sysconfdir)s/')
         self.Strip(debuginfo=False)
+# branch comment


"""

difflines = """\
(working version) (no log message)

test.source: changed
Index: test.source
====================================================================
contents(sha1)
--- test.source /localhost@rpl:linux/1.0-1
+++ test.source @NEW@
@@ -1,3 +1,3 @@
 line1
 line2
-line3
+line#


"""

autoSourceDiff = """\
(working version) (no log message)

autosource.recipe: changed
Index: autosource.recipe
====================================================================
contents(size sha1)
--- autosource.recipe /localhost@rpl:linux/1.0-1
+++ autosource.recipe @NEW@
@@ -6,3 +6,4 @@
     def setup(r):
         r.addSource('distcc-2.9.tar.bz2')
         r.addSource('localfile')
+# comment


"""

class SourceTest(rephelp.RepositoryHelper):

    #topDir = "/tmp/test"
    #cleanupDir = 0

    def testNoCONARYFile(self):
        # attempt operations without a CONARY control file
        dir = tempfile.mkdtemp()
        cwd = os.getcwd()
        os.chdir(dir)
        self.assertRaises(state.CONARYFileMissing, self.addfile, "foo")
        self.assertRaises(state.CONARYFileMissing, self.diff)
        self.assertRaises(state.CONARYFileMissing, self.commit)
        self.assertRaises(state.CONARYFileMissing, self.update)
        self.assertRaises(state.CONARYFileMissing, self.rename, 'foo', 'bar')
        self.assertRaises(state.CONARYFileMissing, self.remove, 'foo')
        os.chdir(cwd)
        os.rmdir(dir)

    def testRDiffContentChangeSamePathid(self):
        'CNY-3383'
        c1 = rephelp.RegularFile(contents='initialcontents',
                                 config=False)

        comp1 = self.addComponent('foo:source',
                    fileContents = [
                        ('name', c1) ],
                    pathIdSalt = '',
                    version = '1.0')
        c2 = rephelp.RegularFile(contents='othercontents',
                                 config=False)
        comp2 = self.addComponent('foo:source',
                    fileContents = [
                        ('name', c2) ],
                    pathIdSalt = '',
                    version = '1.1')
        # before fix for CNY-3383, this raised an exception
        rc = self.rdiff('foo', '-1', '1.1-1')

    def testRDiffAndNewStatus(self):
        rdiffOutputNew = """\
1.0-1 Test (http://bugzilla.rpath.com/)
    foo
"""

        rdiffOutputNewTestcase = """\
testcase.recipe: new
--- /dev/null
+++ testcase.recipe
@@ -0,0 +43 @@
+class TestRecipe1(PackageRecipe):
+    name = 'testcase'
+    version = '1.0'
+    clearBuildReqs()
"""
        rdiffOutputNewSource = """\
test.source: new
--- /dev/null
+++ test.source
@@ -0,0 +3 @@
+line1
+line2
+line3
"""

        rdiffOutputDiff = '''\
1.0-2 Test (http://bugzilla.rpath.com/)
    foo

test.source: changed
Index: test.source
====================================================================
contents(size sha1)
--- test.source /localhost@rpl:linux/1.0-1
+++ test.source /localhost@rpl:linux/1.0-2
@@ -1,3 +1,3 @@
 line1
 line2
-line3
+1.0-2 changes line 3


'''
        origDir = os.getcwd()
        self.resetRepository()
        self.resetWork()
        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")
        # 1.0-1
        lines = [ 'line1\n', 'line2\n', 'line3\n']
        self.writeFile("testcase.recipe", recipes.testRecipe1)
        self.writeFile("test.source", ''.join(lines))
        repos = self.openRepository()
        status = checkin.generateStatus(repos)
        self.assertEquals(status, [('?', 'test.source'), ('?', 'testcase.recipe')])
        self.addfile("testcase.recipe")
        status = checkin.generateStatus(repos)
        self.assertEquals(status, [('?', 'test.source'), ('A', 'testcase.recipe')])
        self.addfile("test.source", text = True)
        os.chdir(self.workDir)
        status = checkin.generateStatus(repos, os.sep.join((self.workDir, 'testcase')))
        self.assertEquals(status, [('A', 'test.source'), ('A', 'testcase.recipe')])
        os.chdir("testcase")
        self.commit()
        lines[2] = '1.0-2 changes line 3\n'
        self.writeFile("test.source", ''.join(lines))
        self.commit()
        rc = self.rdiff('testcase', '-1', '1.0-1')
        self.assertTrue(rc.startswith(rdiffOutputNew), rc)
        self.assertIn(rdiffOutputNewTestcase, rc)
        self.assertIn(rdiffOutputNewSource, rc)
        rc = self.rdiff('testcase', '1.0-1', '1.0-2')
        self.assertEquals(rc, rdiffOutputDiff)
        rc = self.rdiff('testcase', '-1', '1.0-2')
        self.assertEquals(rc, rdiffOutputDiff)
        #cleanup
        os.chdir("..")
        shutil.rmtree("testcase")
        os.chdir(origDir)

    def testDiffSha1(self):
        origDir = os.getcwd()
        self.resetRepository()
        self.resetWork()
        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")

        lines = [ 'line1\n', 'line2\n', 'line3\n']
        self.writeFile("testcase.recipe", recipes.testRecipe1)
        self.writeFile("test.source", ''.join(lines))
        self.addfile("testcase.recipe")
        self.addfile("test.source", text = True)
        self.commit()

        lines[2] = 'line#\n'
        self.writeFile("test.source", ''.join(lines))
        rc = self.diff()
        self.assertEquals(rc, difflines)

        #cleanup
        os.chdir("..")
        shutil.rmtree("testcase")
        os.chdir(origDir)

    def testDiffByFiles(self):
        
        mdiff2 = '''\
(working version) (no log message)

test.source2: changed
Index: test.source2
====================================================================
contents(size sha1)
--- test.source2 /localhost@rpl:linux/1.0-1
+++ test.source2 @NEW@
@@ -1,3 +1,3 @@
 line1
 line2
-line3
+linenew


'''

        mdiff1 = '''\
(working version) (no log message)

test.source1: changed
Index: test.source1
====================================================================
contents(sha1)
--- test.source1 /localhost@rpl:linux/1.0-1
+++ test.source1 @NEW@
@@ -1,3 +1,3 @@
 line1
 line2
-line3
+line#


'''

        origDir = os.getcwd()
        self.resetRepository()
        self.resetWork()
        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")

        lines = [ 'line1\n', 'line2\n', 'line3\n']
        self.writeFile("testcase.recipe", recipes.testRecipe1)
        self.writeFile("test.source1", ''.join(lines))
        self.writeFile("test.source2", ''.join(lines))
        self.addfile("testcase.recipe")
        self.addfile("test.source1", text = True)
        self.addfile("test.source2", text = True)
        self.commit()

        lines[2] = 'line#\n'
        self.writeFile("test.source1", ''.join(lines))
        lines[2] = 'linenew\n'
        self.writeFile("test.source2", ''.join(lines))
        
        rc = self.diff('test.source2')
        self.assertEquals(rc, mdiff2)

        rc = self.diff('test.source1')
        self.assertEquals(rc, mdiff1)

        #cleanup
        os.chdir("..")
        shutil.rmtree("testcase")
        os.chdir(origDir)

    def testStat(self):
        self.resetRepository()
        self.resetWork()
        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")

        lines = [ 'line1\n', 'line2\n', 'line3\n']
        self.writeFile("testcase.recipe", recipes.testRecipe1)
        self.writeFile("test.source", ''.join(lines))
        self.writeFile("not-changed.txt", ''.join(lines))
        self.addfile("testcase.recipe")
        self.addfile("test.source", text = True)
        self.addfile("not-changed.txt", text = True)
        self.commit()

        expected = []

        lines[2] = 'line#\n'
        self.writeFile("test.source", ''.join(lines))

        expected.append(('M', 'test.source'))

        for i in range(3):
            newfile = "test%02d.newfile" % i
            self.writeFile(newfile, "Some content in file %s\n" % newfile)
            self.addfile(newfile, text=True)
            expected.append(('A', newfile))

        for i in range(3):
            newfile = "test%02d.unknownfile" % i
            self.writeFile(newfile, "Some content in file %s\n" % newfile)
            expected.append(('?', newfile))

        self.remove("testcase.recipe")
        expected.append(('R', "testcase.recipe"))

        # And a directory
        os.mkdir("testdir")
        newfile = "testdir/testcase.txt"
        self.writeFile(newfile, "More content\n")
        self.addfile(newfile, text=True)
        expected.append(('A', newfile))

        # And a symlink to a file
        newfile = 'test.symlink-file'
        os.symlink('testdir/testcase.txt', newfile)
        self.addfile(newfile, text=True)
        expected.append(('A', newfile))

        # And a symlink to a directory
        newfile = 'test.symlink-dir'
        os.mkdir("testdir/testdir2")
        os.symlink('testdir/testdir2', newfile)
        self.logCheck(self.addfile, (newfile, ),
                      'error: not adding link with dereferenced file not '
                      'tracked test.symlink-dir -> testdir/testdir2',
                      kwargs={'text' : True})
        expected.append(('?', newfile))

        # And a dangling symlink
        newfile = 'test.symlink-dangling'
        os.symlink('no-such-file', newfile)
        expected.append(('?', newfile))

        rc = self.stat()

        # Sort by file name
        expected.sort(lambda x, y: cmp(x[1], y[1]))
        # Output the format and the file name
        expectedString = "\n".join([ "%s  %s" % items for items in expected ])
        expectedString += '\n'
        self.assertEqual(rc, expectedString)

        #cleanup
        os.chdir("..")

        repos = self.openRepository()
        # this is for CNY-3026
        status = checkin.generateStatus(repos, dirName = os.getcwd() + '/testcase')
        assert(sorted(status) == [('?', 'test.symlink-dangling'),
                                  ('?', 'test.symlink-dir'),
                                  ('?', 'test00.unknownfile'),
                                  ('?', 'test01.unknownfile'),
                                  ('?', 'test02.unknownfile'),
                                  ('A', 'test.symlink-file'),
                                  ('A', 'test00.newfile'),
                                  ('A', 'test01.newfile'),
                                  ('A', 'test02.newfile'),
                                  ('A', 'testdir/testcase.txt'),
                                  ('M', 'test.source'),
                                  ('R', 'testcase.recipe')])

    def testAnnotate(self):
        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")
        # 1.0-1
        lines = [ 'line1\n', 'line2\n', 'line3\n']
        self.writeFile("testcase.recipe", recipes.testRecipe1)
        self.writeFile("test.source", ''.join(lines))
        self.writeFile("test.gz", "contents")
        self.writeFile('neverchange', 'never changes\n')
        self.addfile("testcase.recipe")
        self.addfile("test.source", text = True)
        self.addfile('test.gz')
        self.addfile('neverchange', text=True)
        self.cfg.name = 'Tset'
        self.commit()
        self.cfg.name = 'Test'

        self.logCheck(self.annotate, ('missing-file', ),
                  "error: missing-file is not a member of this source trove")

        self.logCheck(self.annotate, ('test.gz', ),
                  "error: test.gz is not a text file")

        aLines = []
        for line in lines:
            aLines.append('1.0-1 (Tset): ' + line)
        rc = self.annotate('test.source')
        self.assertEquals(rc, ''.join(aLines))

        lines[2] = '1.0-2 changes line 3\n'
        self.writeFile("test.source", ''.join(lines))
        self.commit()
        aLines[2] = '1.0-2 (Test): ' + lines[2]
        rc = self.annotate('test.source')
        self.assertEquals(rc, ''.join(aLines))

        lines.append('1.0-3 adds line 4\n')
        self.writeFile("test.source", ''.join(lines))
        self.commit()
        aLines.append('1.0-3 (Test): ' + lines[3])
        rc = self.annotate('test.source')
        self.assertEquals(rc, ''.join(aLines))

        # test annotate on a file that never changed (CNY-1066)
        rc = self.annotate('neverchange')
        self.assertEqual(rc, '1.0-1 (Tset): never changes\n')

        # test annotate up a branch tree
        self.mkbranch("1.0-2", "@rpl:branch", "testcase:source")
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase", "@rpl:branch")
        os.chdir("testcase")
        lines[1] = 'branch changes line 2\n'
        del lines[3]
        self.writeFile("test.source", ''.join(lines))
        self.commit()
        aLines[1] = '1.0-2/localhost@rpl:branch/3 (Test): ' + lines[1]
        del aLines[3]
        rc = self.annotate('test.source')
        self.assertEquals(rc, ''.join(aLines))
        #cleanup
        os.chdir(origDir)

    def testDirectoryHandling(self):
        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")

        self.logCheck(self.addfile, (".",),
                    "error: cannot add special directory . to trove")
        self.logCheck(self.addfile, ("..",),
                    "error: cannot add special directory .. to trove")

        os.mkdir("foo")
        self.addfile("foo")
        self.writeFile("foo/bar", "something\n")
        self.logCheck(self.remove, ("foo",),
                    "error: cannot remove foo: Directory not empty")
        os.chdir(origDir)

    def testLogNewer(self):
        repos = self.openRepository()
        os.chdir(self.workDir)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        self.addfile('testcase.recipe')
        logMessagesUncommitted = [self.removeDateFromLogMessage(x)
                                  for x in checkin.iterLog(repos)]
        self.assertEquals(logMessagesUncommitted,
                          ['nothing has been committed'])
        self.commit()
        os.chdir(self.workDir)
        self.checkout('testcase', dir='testcasenew')
        os.chdir('testcasenew')
        self.writeFile('onenewfile.txt', 'testcontent\nmore\n')
        self.addfile('onenewfile.txt')
        self.commit(message='newcommit')
        os.chdir(self.workDir + '/testcase')
        logMessagesFull = [self.removeDateFromLogMessage(x)
                           for x in checkin.iterLog(repos)]
        os.chdir(self.workDir)
        logMessagesNewer = [self.removeDateFromLogMessage(x)
                            for x in checkin.iterLog(repos, newer=True,
                                                     dirName='testcase')]
        self.assertEquals(logMessagesFull, [
            'Name  : testcase:source',
            'Branch: /localhost@rpl:linux',
            '',
            '1.0-2 Test ',
            '    newcommit',
            '',
            '1.0-1 Test ',
            '    foo',
            ''
            ])
        self.assertEquals(logMessagesNewer, [
            'Name  : testcase:source',
            'Branch: /localhost@rpl:linux',
            '',
            '1.0-2 Test ',
            '    newcommit',
            ''
            ])
        os.chdir(self.workDir + '/testcase')
        cmdLineNewer = self.showLog(**{'newer':True})
        # cmdline has more trailing whitespace
        self.assertEquals('\n'.join(logMessagesNewer).strip(),
                          cmdLineNewer.strip())
        self.assertRaises(errors.CvcError, 
            checkin.iterLog(repos, branch='foo', newer=True).next)


    def testSource(self):
        sourceContents = "some\ntest contents\n"
        origDir = os.getcwd()
        # 1.0-1
        self.resetRepository()
        self.resetWork()
        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testRecipe1)
        self.writeFile("test.source", sourceContents)
        os.mkfifo("test.fifo")
        self.addfile("testcase.recipe")
        self.addfile("test.source", text = True)
        self.addfile("test.fifo")
        self.logCheck(self.diff, (),
                      "error: no versions have been committed",
                      kwargs = { 'rc' : 2  })
        self.commit()

        # verify that a commit with no changes results in a log message
        self.logCheck(self.commit, (),
                      '+ no changes have been made to commit')

        os.chdir("..")
        shutil.rmtree("testcase")

        # make sure all the files in the source component are flagged
        # properly
        repos = self.openRepository()
        for (pathId, path, fileId, version, fObj) in repos.iterFilesInTrove(
                    'testcase:source', 
                    versions.VersionFromString('/localhost@rpl:linux/1.0-1'), 
                    deps.deps.Flavor(), withFiles = True):
            assert(fObj.flags.isSource())
            if path in ('test.source', 'testcase.recipe'):
                assert(fObj.flags.isConfig())
            else:
                assert(not fObj.flags.isConfig())
        del repos

        self.checkout("testcase", dir = "foo")
        os.chdir("foo")
        self.verifySrcDirectory(["testcase.recipe", "test.source", 
                                 "test.fifo" ])
        os.chdir("..")

        # 1.0-2
        self.checkout("testcase")
        os.chdir("testcase")
        self.verifyFile("testcase.recipe", recipes.testRecipe1)
        self.verifyFile("test.source", sourceContents)
        self.verifyFifo("test.fifo")
        newSourceContents = sourceContents + "final line\n"
        self.writeFile("test.newsource", newSourceContents)
        self.addfile("test.newsource", text = True)
        self.remove("test.fifo")
        rc = self.diff(rc = 1)
        self.assertEquals(rc, diff0)

        self.commit()

        s = self.showLog()
        lines = [ x.strip() for x in s.split('\n') if x.strip() != '' ] 
        self.assertEquals(lines, ['Name  : testcase:source',
                         'Branch: /localhost@rpl:linux',
                         '1.0-2 Test', 'foo', '1.0-1 Test', 'foo' ] )
        os.chdir("..")
        shutil.rmtree("testcase")

        # foo has an out of date source dir
        os.rename('foo', 'testcase')
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testRecipe1 + "# change")
        self.logCheck(self.commit, (),
                      ("error: contents of working directory are not all "
                       "from the head of the branch; use update"))
        self.update()
        self.verifyFile("testcase.recipe", recipes.testRecipe1 + "# change")
        os.chdir('..')
        shutil.rmtree("testcase")

        self.checkout("testcase")
        os.chdir("testcase")
        self.verifySrcDirectory(["testcase.recipe", "test.source", 
                                 "test.newsource" ])
        self.verifyFile("test.newsource", newSourceContents)
        os.chdir("..")
        shutil.rmtree("testcase")

        # 1.0-3
        self.checkout("testcase", "1.0-1")
        os.chdir("testcase")
        self.verifySrcDirectory(["testcase.recipe", "test.source", 
                                 "test.fifo" ])
        self.update()
        self.verifySrcDirectory(["testcase.recipe", "test.source", 
                                 "test.newsource" ])
        self.rename("test.source", "test.renamed")
        os.remove("test.newsource")
        self.logCheck(self.diff, (),
                'error: test.newsource is missing (use remove if this '
                'is intentional)',
                kwargs = { 'rc' : 2 } )
        # test attempting to commit with a file missing
        self.logCheck(self.commit, (),
                'error: test.newsource is missing (use remove if this '
                'is intentional)')
        # after removing the file from the package, the commit should
        # work
        self.remove("test.newsource")
        rc = self.diff(rc = 1)
        self.assertEquals(rc, "(working version) (no log message)\n\n"
                      "test.renamed (aka test.source)\n"
                      "test.newsource: removed\n")
        self.commit()
        rc = self.diff(rc = 0)
        assert(not rc)
        os.chdir("..")
        shutil.rmtree("testcase")

        self.checkout("testcase", "1.0-3")
        os.chdir("testcase")
        self.verifySrcDirectory(["testcase.recipe", "test.renamed"])
        os.chdir("..")
        shutil.rmtree("testcase")

        # 1.0-4
        self.checkout("testcase", "1.0-2")
        os.chdir("testcase")
        self.update()
        self.verifySrcDirectory(["testcase.recipe", "test.renamed"])
        self.verifyFile("testcase.recipe", recipes.testRecipe1)
        self.verifyFile("test.renamed", sourceContents)
        sourceContents2 = recipes.testRecipe1 + "# some comments\n" 
        self.writeFile("testcase.recipe", sourceContents2)
        rc = self.diff()
        self.assertEquals(rc, diff1)
        rc = self.diff(revision='1.0-3')
        self.assertEquals(rc, diff1)
        self.commit()
        os.chdir("..")
        shutil.rmtree("testcase")

        self.checkout("testcase", "1.0-3")
        os.chdir("testcase")
        sourceContents3 = "# top comment\n" + recipes.testRecipe1
        self.writeFile("testcase.recipe", sourceContents3)
        self.update()
        self.verifyFile("testcase.recipe", sourceContents3 + \
                        "# some comments\n")

        # test renaming and modifying a file in a single commit (CNY-944)
        # 1.0.5
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase")
        os.chdir("testcase")
        self.rename("test.renamed", "test.renamedagain")
        renamedContents = "this file has been renamed\n"
        self.writeFile("test.renamedagain", renamedContents)
        self.commit()
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase", "1.0-4")
        os.chdir("testcase")
        self.update()

        # branch tests
        path = self.workDir + '/shadow.ccs'
        self.mkbranch("1.0-1", "@rpl:branch", "testcase:source",
                      targetFile = path)
        assert(os.path.exists(path))
        os.unlink(path)
        del path

        self.mkbranch("1.0-1", "@rpl:branch", "testcase:source")
        self.logCheck(self.mkbranch,
               ("1.0-1", "@rpl:branch", "testcase:source") ,
               "warning: testcase:source already has branch /localhost@rpl:linux/1.0-1/branch")
        self.checkout("testcase", "@rpl:branch")
        os.chdir("testcase")
        branchContents = recipes.testRecipe1 + "# branch comment\n"
        self.writeFile("testcase.recipe", branchContents)
        rc = self.diff()
        self.assertEquals(rc, diff2)
        self.commit()
        os.chdir("..")

        self.checkout("testcase", "@rpl:branch")
        os.chdir("testcase")
        self.verifyFile("testcase.recipe", branchContents)
        os.chdir("..")
        shutil.rmtree("testcase")

        self.checkout("testcase", "@rpl:linux")
        self.verifyFile("testcase/testcase.recipe", sourceContents2)

        # check to make sure that an update to a version that does
        # not exist results in the correct error message
        os.chdir("testcase")
        err = "error: cannot find source trove: revision badVersion of testcase:source was not found on label(s) localhost@rpl:linux"
        self.logCheck(self.update, ("badVersion",), err)
        os.chdir(origDir)
    
    def testSourceFlag(self):
        self.resetWork()
        origDir = os.getcwd()

        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testRecipe1)
        f = gzip.open("test.source.gz", "w")
        f.write("some text")
        f.close()
        file("Makefile", "w").write('ignore this\n')
        file("asdf", "w").write('#!/bin/bash\n')
        file("blah.patch", "w").write('ignore this too\n')
        self.addfile("testcase.recipe")
        # test non-config extension
        self.addfile("test.source.gz")
        # test full filename
        self.addfile("Makefile")
        # test magic.script
        self.addfile("asdf")
        # test config extension
        self.addfile("blah.patch")
        self.commit()

        # it's a shame there isn't a better way to do this
        repos = self.openRepository()
        trv = repos.getTrove("testcase:source", 
             versions.VersionFromString("/localhost@rpl:linux/1.0-1"), 
             deps.deps.Flavor())

        for (pathId, path, fileId, version) in trv.iterFileList():
            fileObj = repos.getFileVersion(pathId, fileId, version)
            assert(fileObj.flags.isSource())
            if path in ('testcase.recipe', 'Makefile', 'asdf', 'blah.patch'):
                assert(fileObj.flags.isConfig())
            else:
                assert(not fileObj.flags.isConfig())

        os.chdir(origDir)

    def testCheckout(self):
        os.chdir(self.workDir)
        buildLabel = str(self.cfg.buildLabel)
        self.addQuickTestComponent('foo:source', '1.0-1')
        self.cfg.buildLabel = None
        self.checkout('foo', buildLabel)
        assert(os.path.exists('foo'))
        util.rmtree('foo')
        self.checkout('foo:source', buildLabel)
        assert(os.path.exists('foo'))
        self.assertRaises(errors.CvcError, 
                          self.checkout, 'foo:source')
        # no build label set and we don't specify a label
        self.assertRaises(errors.LabelPathNeeded, 
                          self.checkout, 'foo:source', '1.0')

    def testNewpkg(self):
        os.chdir(self.workDir)
        buildLabel = str(self.cfg.buildLabel)
        self.cfg.buildLabel = None
        self.newpkg('foo=%s' % buildLabel)
        assert(os.path.exists('foo'))

        templateDir = os.path.join(resources.get_archive(), 'recipeTemplates')
        self.cfg.recipeTemplateDirs = [templateDir]
        self.cfg.recipeTemplate = 'test'
        self.newpkg('foo=%s' % buildLabel)
        assert(os.path.exists(os.path.join('foo', 'foo.recipe')))
        util.rmtree('foo')

        self.cfg.recipeTemplate = 'default'
        self.newpkg('foo=%s' % buildLabel)
        assert(os.path.exists(os.path.join('foo', 'foo.recipe')))
        util.rmtree('foo')

        self.newpkg('foo=%s --template rpath' % buildLabel)
        assert(os.path.exists(os.path.join('foo', 'foo.recipe')))
        util.rmtree('foo')

        self.cfg.recipeTemplate = 'rpath'
        self.newpkg('foo=%s' % buildLabel)
        assert(os.path.exists(os.path.join('foo', 'foo.recipe')))

        util.remove(os.path.join('foo', 'CONARY'))
        self.newpkg('foo=%s' % buildLabel)
        conaryState = state.ConaryStateFromFile(os.path.join('foo', 'CONARY'),
                                                None)
        sourceState = conaryState.getSourceState()
        self.assertEquals([ x[1] for x in sourceState.iterFileList() if x[1].endswith('.recipe') ], ['foo.recipe'])

        self.cfg.recipeTemplate = None

    def testCommitErrors(self):
        # let's do evil things with commit
        # like try to commit a file that we removed
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', recipes.simpleRecipe)
        self.writeFile('foo', '')
        self.addfile('simple.recipe')
        self.addfile('foo', binary = True)
        os.remove('foo')
        self.assertRaises(errors.CvcError, self.commit)
        # or we could try to reference a non-existant source in the recipe
        myRecipe = recipes.simpleRecipe + '\tr.addSource("foo.baz")\n'
        self.writeFile('simple.recipe', myRecipe)
        self.writeFile('foo', '')
        self.assertRaises(errors.CvcError, self.commit)

    def testUpdateErrors(self):
        # cvc update can fail in some neat ways
        # eg CNY-715 - run cvc update on a new package
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.logCheck(self.update, ['someversion'], 
            "error: cannot update source directory for package 'simple:source' - it was created with newpkg and has never been checked in.")

    def testNewCommit(self):
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', recipes.simpleRecipe)
        self.addfile('simple.recipe')

        self.addQuickTestComponent('simple:source', '1.0')

        self.logCheck2([ 'error: simple:source is marked as a new package '
                         'but it already exists' ], self.commit)

    def testBasicLabelMultiplicity(self):
        os.chdir(self.workDir)
        b1 = '/%s/1.0-1' % self.cfg.buildLabel
        b2 = '/localhost@rpl:bar//%s/1.0-1' % self.cfg.buildLabel
        b3 = '/localhost@rpl:foo//%s/1.0-1' % self.cfg.buildLabel
        self.addQuickTestComponent('bash:source', b1)
        self.addQuickTestComponent('bash:source', b2)
        # note - the b3 bash:source should have a later timestamp than b2 or b1.
        self.addQuickTestComponent('bash:source', b3,
                    fileContents = [ ( 'bash.recipe', recipes.bashRecipe) ] )
        self.checkout('bash')
        trvState = state.ConaryStateFromFile('bash/CONARY')
        self.assertEquals(trvState.getSourceState().getVersion(),
                    versions.VersionFromString(b3) )

        shutil.rmtree('bash')
        self.checkout('bash=%s' % b1)
        os.chdir('bash')
        self.logCheck2([ '+ switching directory %s/bash to branch '
                         '/localhost@rpl:foo//linux' % self.workDir ],
                       self.update, verbosity = log.INFO)
        trvState = state.ConaryStateFromFile('CONARY')
        self.assertEquals(trvState.getSourceState().getVersion(),
                    versions.VersionFromString(b3) )

        # make sure a commit doesn't switch branches
        self.writeFile('test.txt', 'some text\n')
        self.addfile('test.txt')
        self.commit()
        trvState = state.ConaryStateFromFile('CONARY')
        self.assertEquals(trvState.getSourceState().getVersion().branch(),
                    versions.VersionFromString(b3).branch() )

    def testCookLabelMultiplicity(self):
        os.chdir(self.workDir)
        b1 = '/%s/1.0-1' % self.cfg.buildLabel
        b2 = '/localhost@rpl:bar//%s/1.0-1' % self.cfg.buildLabel
        b3 = '/localhost@rpl:foo//%s/1.0-1' % self.cfg.buildLabel
        self.addQuickTestComponent('bash:source', b1)
        self.addQuickTestComponent('bash:source', b2)
        # note - the b3 bash:source should have a later timestamp than b2 or b1.
        self.addQuickTestComponent('bash:source', b3,
                                   fileContents = [ ('bash.recipe',
                                                     recipes.bashRecipe) ] )
        # this cook would fail for versions b1 and b2 as they don't contain
        # recipes
        self.cookFromRepository('bash')

    def testMetaDataConflicts(self):
        self.resetRepository()
        self.resetWork()
        # set up wrappers to fake stat, so we can masquerade as two users
        # other than the current user
        origstat = os.stat
        origlstat = os.lstat
        binuser = StatWrapper(origstat, origlstat, 1, 1)
        daemonuser = StatWrapper(origstat, origlstat, 2, 2)

        os.chdir(self.workDir)
        # create version 1.0-1 of the source component
        self.newpkg("testcase")
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testRecipe1)
        self.addfile("testcase.recipe")

        # use the binuser for the first commit
        os.stat = binuser.stat
        os.lstat = binuser.lstat
        self.commit()

        repos = self.openRepository()

        # verify that the source component has the correct (fake) ownerships
        (rc, str) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       [ "testcase:source=1.0-1" ],
                                       lsl = True)
        assert("bin      bin" in str)

        # make a change
        sourceContents2 = recipes.testRecipe1 + "# some comments\n" 
        self.writeFile("testcase.recipe", sourceContents2)

        # user the daemon user to commit this change (1.0-2)
        os.stat = daemonuser.stat
        os.lstat = daemonuser.lstat
        self.commit()

        # verify that the source component once more
        (rc, str) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       [ "testcase:source=1.0-2" ], lsl = True)
        assert("daemon   daemon" in str)

        # restore the system stat
        os.stat = origstat
        os.lstat = origlstat

        # check out version 1.0-1 of the source component (this will be
        # owned by the current user)
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase", "1.0-1")
        os.chdir("testcase")

        # verify that an update causes no error
        self.logCheck(self.update, (), [])

        # verify the update was successful
        self.verifyFile("testcase.recipe", sourceContents2)

    def testAutoSource(self):
        def _checkFile(repos, target, version):
            trv = repos.getTrove("autosource:source", 
                 versions.VersionFromString("/localhost@rpl:linux/%s" % 
                                                    version), 
                 deps.deps.Flavor())

            for (pathId, path, fileId, version) in trv.iterFileList():
                if path == target: 
                    return version.asString()

            return None

        def _checkCONARY(f1, f2):
            l1 = open(f1).readlines()
            l1.sort()
            l2 = open(f2).readlines()
            l2.sort()
            # the version lines could have different timestamps, thanks to
            # timestamps changing on commit
            v1 = versions.ThawVersion(l1[-1].split()[1])
            v2 = versions.ThawVersion(l2[-1].split()[1])
            self.assertEquals(v1, v2)
            self.assertEquals(l1[:-1], l2[:-1])

        def _checkCONARYFiles(l):
            f = open('CONARY', 'r')
            lines = f.readlines()[5:]
            files = [ line.split()[1] for line in lines ]
            self.assertEqual(sorted(files), sorted(l))

        sourceDir = os.path.join(self.workDir, 'sourceSearch')
        self.cfg.sourceSearchDir = sourceDir
        dir = os.path.join(self.cfg.lookaside, 'autosource')
        util.mkdirChain(dir)
        util.mkdirChain(sourceDir)
        # copy autosource files into new source search dir so we can
        # modify them safely
        for fileName in [ 'distcc-2.9.tar.bz2', 'multilib-sample.tar.bz2' ]:
            shutil.copy2(os.path.join(resources.get_archive(), fileName), 
                         sourceDir)

        repos = self.openRepository()

        os.chdir(self.workDir)
        self.newpkg("autosource")
        os.chdir("autosource")
        self.writeFile("autosource.recipe", recipes.autoSource1)
        self.writeFile('localfile', 'test contents\n')
        self.logCheck(self.commit, (),
                  'error: recipe not in CONARY state file, please run cvc add')
        self.addfile("autosource.recipe")
        self.logCheck(self.commit, (), 
                      'error: localfile (in current directory) must '
                      'be added with cvc add')
        self.addfile("localfile", binary = True)
        self.commit()
        _checkCONARYFiles(('distcc-2.9.tar.bz2', 'localfile',
                           'autosource.recipe'))

        assert(_checkFile(repos, 'distcc-2.9.tar.bz2', '1.0-1'))

        os.chdir("..")
        shutil.rmtree("autosource")
        self.checkout("autosource")
        os.chdir("autosource")
        _checkCONARYFiles(('distcc-2.9.tar.bz2', 'localfile',
                           'autosource.recipe'))
        self.verifySrcDirectory(["autosource.recipe", 'localfile'])
        open("autosource.recipe", "a").write("# comment\n")
        rc = self.diff()
        # ignore mtime changes
        rc = rc.replace('inode(mtime)\n', '')
        self.assertEquals(rc, autoSourceDiff)
        self.commit()
        _checkCONARYFiles(('distcc-2.9.tar.bz2', 'localfile',
                           'autosource.recipe'))

        open("localfile", "a").write("new contents\n")
        self.commit()
        _checkCONARYFiles(('distcc-2.9.tar.bz2', 'localfile',
                           'autosource.recipe'))

        self.writeFile("autosource.recipe", recipes.autoSource2)
        self.commit()
        _checkCONARYFiles(('multilib-sample.tar.bz2', 'localfile',
                           'autosource.recipe'))
        assert(_checkFile(repos, 'distcc-2.9.tar.bz2', '2.0-1') is None)
        assert(_checkFile(repos, 'multilib-sample.tar.bz2', '2.0-1'))

        # now, copy our current CONARY state and do a fresh checkout
        os.chdir("..")
        shutil.copy('autosource/CONARY', 'CONARY')
        shutil.rmtree("autosource")
        self.checkout("autosource")
        # make sure that the CONARY file we get matches the CONARY file
        # we had before
        _checkCONARY('CONARY', 'autosource/CONARY')
        shutil.rmtree("autosource")

        # now check out the first version we checked in
        self.checkout("autosource=1.0-1")
        os.chdir("autosource")
        self.update()
        # and make sure that the state we get taking the update path
        # matches the final state we checked in
        _checkCONARY('../CONARY', 'CONARY')

        self.writeFile("autosource.recipe", recipes.autoSource3)
        self.remove('localfile')
        f = open(os.path.join(sourceDir, 'multilib-sample.tar.bz2'), 'w')
        f.write('new contents\n')
        f.close()
        self.refresh('multilib-sample.tar.bz2')
        # make sure the refresh shows up in the diff and stat
        rc = self.diff()
        assert('multilib-sample.tar.bz2: changed' in rc)
        rc = self.stat()
        assert('M  multilib-sample.tar.bz2' in rc)
        self.logFilter.add()
        self.commit()
        self.logFilter.compare(['+ found localfile in repository', 
                                '+ localfile not yet cached, fetching...'])
        os.chdir("..")
        v = _checkFile(repos, 'multilib-sample.tar.bz2', '3.0-1').split('/')[-1]
        self.assertEquals(v, '3.0-1')
        # copy 3.0 conary up a dir
        self.diff()
        shutil.copy('autosource/CONARY', 'CONARY')

        shutil.rmtree("autosource")
        self.checkout('autosource=2.0')
        os.chdir("autosource")
        self.update()
        _checkCONARY('../CONARY', 'CONARY')

        # now see if we can change the autosource file to a normal file
        # and back again (CNY-946)
        os.unlink(sourceDir + '/multilib-sample.tar.bz2')
        open("multilib-sample.tar.bz2", "w").write("local contents")
        self.addfile("multilib-sample.tar.bz2")

        self.commit()

        os.chdir("..")
        shutil.rmtree("autosource")
        self.checkout('autosource=3.0-2')
        os.chdir("autosource")
        self.verifyFile('multilib-sample.tar.bz2', 'local contents')

        self.update("3.0-1")
        assert(not util.exists('multilib-sample.tar.bz2'))
        self.update("3.0-2")
        self.verifyFile('multilib-sample.tar.bz2', 'local contents')

        # touch the recipe and make sure we don't refetch sources in order
        # to commit
        self.update()
        util.rmtree(self.cfg.lookaside)
        util.mkdirChain(self.cfg.lookaside)
        open("autosource.recipe", "a").write("#comment\n")
        self.commit()
        assert(not os.listdir(self.cfg.lookaside))

    def testDownloadingAutosource(self):
        self.logFilter.add()
        sourceDir = os.path.join(self.workDir, 'sourceSearch')
        archivePath = resources.get_archive()

        self.cfg.sourceSearchDir = sourceDir
        dir = os.path.join(self.cfg.lookaside, 'autosource')
        util.mkdirChain(dir)
        util.mkdirChain(sourceDir)
        # copy autosource files into new source search dir so we can
        # modify them and have it have some effect
        rpm = 'distcache-1.4.5-2.src.rpm'
        tarfile = 'distcache-1.4.5.tar.bz2'
        open(sourceDir + '/' + tarfile, 'w').write('blam\n')

        self.openRepository()
        repos = self.openRepository(1)

        os.chdir(self.workDir)
        self.newpkg("autosource")
        os.chdir("autosource")
        self.writeFile("autosource.recipe", recipes.autoSource4)
        self.addfile("autosource.recipe")
        self.commit()

        self.mkbranch(['autosource:source'], 'localhost1@rpl:branch',
                      shadow=True)

        os.chdir("..")

        shutil.copyfile(archivePath + '/' + rpm,
                        sourceDir + '/' + rpm)
        os.unlink(sourceDir + '/' + tarfile)

        shutil.rmtree("autosource")
        self.checkout("autosource", 'localhost1@rpl:branch')
        os.chdir("autosource")
        self.writeFile("autosource.recipe", recipes.autoSource5)
        self.commit()
        os.chdir("..")
        shutil.rmtree("autosource")

        # ensure that createChangeSet excludeAutoSource is not downloading
        # auto source parts from other repositories.
        self.checkout("autosource", 'localhost1@rpl:branch')
        trvTup = repos.findTrove(self.cfg.installLabelPath, 
                                 ('autosource:source', 
                                  'localhost1@rpl:branch', 
                                  deps.deps.Flavor()), None)[0]
        changeList = [(trvTup[0], (None, None), trvTup[1:], False)]
        cs = repos.createChangeSet(changeList, excludeAutoSource=True)
        troveCs = cs.iterNewTroveList().next()

        found = False
        for (pathId, path, fileId, version) in troveCs.getNewFileList():
            fileObj = files.ThawFile(cs.getFileChange(None, fileId), pathId)
            if fileObj.flags.isAutoSource():
                try:
                    cs.getFileContents(pathId, fileId)
                except KeyError:
                    found = True
                    break
                else:
                    assert(0)
            else:
                cs.getFileContents(pathId, fileId)
        if not found:
            assert 0, 'changeset contained autosource components!'

        cs = repos.createChangeSet(changeList, excludeAutoSource=True)
        troveCs = cs.iterNewTroveList().next()
        for (pathId, path, fileId, version) in troveCs.getNewFileList():
            fileObj = files.ThawFile(cs.getFileChange(None, fileId), pathId)
            if fileObj.flags.isAutoSource():
                continue
            cs.getFileContents(pathId, fileId)

    def testAddSourceInSubClass(self):
        # subclass is an exact duplicate of superclass -
        # it has no setup() method, and so just uses the superclass one.
        # superclass gains a source file 'newsource' in its second 
        # variation.  When then try cooking subclass and it fails
        # because newsource is not available.

        # NOTE: I'm turning this test off because being able to do this
        # is not part of the design of course sources atm.  I'm leaving
        # the code here for historical record and to document this 
        # behavior.
        return 
        os.chdir(self.workDir)
        self.newpkg("superclass")
        os.chdir("superclass")
        self.writeFile("superclass.recipe", recipes.sourceSuperClass1)
        self.addfile("superclass.recipe")
        self.commit()

        self.newpkg("subclass")
        os.chdir("subclass")
        self.writeFile("subclass.recipe", recipes.sourceSubClass1)
        self.addfile("subclass.recipe")
        self.commit()
        self.cookFromRepository('subclass')

        os.chdir(self.workDir)
        self.checkout("superclass")
        os.chdir("superclass")
        self.writeFile("superclass.recipe", recipes.sourceSuperClass2)
        self.writeFile('newsource', 'some text\nsome more text\n')
        self.addfile("newsource", binary = True)
        self.commit()

        self.cookFromRepository('subclass')

    def testStateFile(self):
        self.writeFile(self.workDir + '/CONARY',
                       "context 1\ncontext 2\n")
        conaryState = state.ConaryStateFromFile(self.workDir + '/CONARY')
        self.assertEquals(conaryState.context, '2')
        self.writeFile(self.workDir + '/CONARY',
                       "nonesuch 1\ncontext 2\n")
        try:
            state.ConaryStateFromFile(self.workDir + '/CONARY')
        except state.ConaryStateError, err:
            self.assertEquals(str(err), 'Cannot parse state file ' + self.workDir +
                   '/CONARY: Invalid field "nonesuch"')
        else:
            assert(0)

    def testSubDirectorySourcesWithSameName(self):
        # Testing for CNY-617, cvc doesn't support subdirectories with the
        # same name.

        dir = os.path.join(self.cfg.lookaside, 'simple', 'localhost')
        util.mkdirChain(dir)

        # copy file into autosource directory
        shutil.copyfile(os.path.join(resources.get_archive(),
                        'distcc-2.9.tar.bz2'), 
                         dir + '/foo')
        simpleRecipe = (recipes.simpleRecipe 
                     + '\tr.addSource("a/foo", dir="/foo1")\n'
                     + '\tr.addSource("b/foo", dir="/foo2")\n'
                     + '\tr.addSource("http://localhost/foo", dir="/foo3")\n'
                     )
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', simpleRecipe)
        os.mkdir('a')
        os.mkdir('b')
        self.writeFile('a/foo', 'blarg\n')
        self.writeFile('b/foo', 'glarb\n')
        self.addfile('simple.recipe')
        self.addfile('a/foo', binary = True)
        self.addfile('b/foo', binary = True)
        self.commit()
        repos = self.openRepository()
        self.cookItem(repos, self.cfg, 'simple')
        self.updatePkg('simple')
        self.verifyFile(self.rootDir + '/foo1/foo', 'blarg\n')
        self.verifyFile(self.rootDir + '/foo2/foo', 'glarb\n')

    def testSubDirectorySourcesWithSameName2(self):
        # same test as above, except an autoSource package
        sourceDir = os.path.join(self.workDir, 'sourceSearch')
        archivePath = resources.get_archive()

        self.logFilter.add()

        self.cfg.sourceSearchDir = sourceDir
        dir = os.path.join(self.cfg.lookaside, 'simple')
        util.mkdirChain(dir)
        util.mkdirChain(sourceDir)
        # copy autosource files into new source search dir so we can
        # modify them and have it have some effect
        rpm = 'distcache-1.4.5-2.src.rpm'
        tarfile = 'distcache-1.4.5.tar.bz2'
        shutil.copyfile(archivePath + '/' + rpm,
                        sourceDir + '/' + rpm)

        simpleRecipe = ((recipes.simpleRecipe 
                     + '\tr.addSource("a/%(tarfile)s", dir="/foo1")\n'
                     + '\tr.addSource("b/%(tarfile)s", dir="/foo2")\n')
                      % dict(tarfile=tarfile, rpm=rpm))

        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', simpleRecipe)
        os.mkdir('a')
        os.mkdir('b')
        self.writeFile('a/%s' % tarfile, 'blarg\n')
        self.writeFile('b/%s' % tarfile, 'glarb\n')

        self.addfile('simple.recipe')
        self.addfile('a/%s' % tarfile)
        self.addfile('b/%s' % tarfile)
        self.commit()

        repos = self.openRepository()
        self.cookItem(repos, self.cfg, 'simple', requireCleanSources=False)
        self.updatePkg('simple', depCheck=False)

        self.verifyFile(self.rootDir + '/foo1/%s' % tarfile, 'blarg\n')
        self.verifyFile(self.rootDir + '/foo2/%s' % tarfile, 'glarb\n')

    def testSourceFileConflict(self):
        dir = os.path.join(self.cfg.lookaside, 'simple', 'localhost')
        util.mkdirChain(dir)

        # copy file into cache, for finding when using http
        shutil.copyfile(os.path.join(resources.get_archive(),
                        'distcc-2.9.tar.bz2'), 
                         dir + '/foo')
        simpleRecipe = (recipes.simpleRecipe 
                     + '\tr.addSource("foo", dir="/foo1")\n'
                     + '\tr.addSource("a/foo", dir="/foo2")\n'
                     + '\tr.addSource("http://localhost/foo", dir="/foo3")\n'
                     )
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', simpleRecipe)
        self.writeFile('foo', 'glarb\n')
        os.mkdir('a')
        self.writeFile('a/foo', 'blarg\n')
        self.addfile('simple.recipe')
        self.addfile('a/foo', binary = True)
        self.addfile('foo', binary = True)
        try:
            self.commit()
        except cvcerrors.RecipeFileError, err:
            self.assertEquals(str(err), 'The following file names conflict (cvc does not currently support multiple files with the same name from different locations):\n   http://localhost/foo\n   foo')
        else:
            assert(0)

    def testTrailingNewline(self):
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')

        self.writeFile('foo.txt', 'text without a trailing newline')
        self.logCheck(self.addfile, ("foo.txt",),
                    "error: foo.txt does not end with a trailing new line")

        self.writeFile('foo.gz', 'text without a trailing newline')
        self.addfile('foo.gz')

    def testStateVersion1Migration(self):
        class FakeRepos:

            def getFileVersions(self, l):
                r = []
                for (pathId, fileId, version) in l:
                    pathIdStr = sha1helper.md5ToString(pathId)
                    if pathIdStr[0:2] == '10':
                        config = 0
                        autoSource = 0
                    elif pathIdStr[0:2] == '11':
                        config = 1
                        autoSource = 1
                    else:
                        assert(0)

                    f = files.RegularFile(pathId)
                    f.flags.isConfig(set = config)
                    f.flags.isAutoSource(set = autoSource)
                    r.append(f)

                return r

        open(self.workDir + "/CONARY", "w").write("""\
stateversion 1
name foo:source
version /conary.rpath.com@rpl:linux/1.000:1.0-1
branch /conary.rpath.com@rpl:linux
2
10000000000000000000000000000000 foo 0000000000000000000000000000000000000000 0 0 /conary.rpath.com@rpl:linux/1.0-1
11111111111111111111111111111111 bar 1111111111111111111111111111111111111111 1 0 /conary.rpath.com@rpl:linux/1.0-1
""")

        try:
            st = state.ConaryStateFromFile(self.workDir + '/CONARY', None)
        except Exception, err:
            self.assertEquals(str(err), 'Cannot parse state file %s/CONARY: CONARY file has version 1, but this application cannot convert - please run a cvc command, e.g. cvc diff, to convert.' % self.workDir)
        else:
            assert(0)

        st = state.ConaryStateFromFile(self.workDir + '/CONARY', FakeRepos())
        st.write(self.workDir + "/CONARY")

        self.verifyFile(self.workDir + "/CONARY", """\
stateversion 2
name foo:source
version /conary.rpath.com@rpl:linux/1.000:1.0-1
branch /conary.rpath.com@rpl:linux
2
10000000000000000000000000000000 foo 0000000000000000000000000000000000000000 _ /conary.rpath.com@rpl:linux/1.0-1
11111111111111111111111111111111 bar 1111111111111111111111111111111111111111 config/autosource /conary.rpath.com@rpl:linux/1.0-1
""")

    def testStateVersion0Migration(self):
        class FakeRepos:

            def getFileVersions(self, l):
                r = []
                for (pathId, fileId, version) in l:
                    pathIdStr = sha1helper.md5ToString(pathId)
                    if pathIdStr[0:2] == '10':
                        config = 0
                    elif pathIdStr[0:2] == '11':
                        config = 1
                    else:
                        assert(0)

                    f = files.RegularFile(pathId)
                    f.flags.isConfig(set = config)
                    r.append(f)

                return r

        open(self.workDir + "/CONARY", "w").write("""\
name foo:source
version /conary.rpath.com@rpl:linux/1.000:1.0-1
branch /conary.rpath.com@rpl:linux
2
10000000000000000000000000000000 foo 0000000000000000000000000000000000000000 /conary.rpath.com@rpl:linux/1.0-1
11111111111111111111111111111111 bar 1111111111111111111111111111111111111111 /conary.rpath.com@rpl:linux/1.0-1
""")

        try:
            st = state.ConaryStateFromFile(self.workDir + '/CONARY', None)
        except Exception, err:
            self.assertEquals(str(err), 'Cannot parse state file %s/CONARY: CONARY file has version 0, but this application cannot convert - please run a cvc command, e.g. cvc diff, to convert.' % self.workDir)
        else:
            assert(0)

        st = state.ConaryStateFromFile(self.workDir + '/CONARY', FakeRepos())
        st.write(self.workDir + "/CONARY")

        self.verifyFile(self.workDir + "/CONARY", """\
stateversion 2
name foo:source
version /conary.rpath.com@rpl:linux/1.000:1.0-1
branch /conary.rpath.com@rpl:linux
2
10000000000000000000000000000000 foo 0000000000000000000000000000000000000000 _ /conary.rpath.com@rpl:linux/1.0-1
11111111111111111111111111111111 bar 1111111111111111111111111111111111111111 config /conary.rpath.com@rpl:linux/1.0-1
""")

    def testSetConfig(self):
        def verifyConfigFlag(filePath, setting):
            st = state.ConaryStateFromFile(self.workDir + '/simple/CONARY',
                                           None).getSourceState()
            for (pathId, path, fileId, version) in st.iterFileList():
                if path != filePath: continue
                isConfig = st.fileIsConfig(pathId)
                assert((setting and isConfig) or (not setting and not isConfig))

        os.chdir(self.workDir)

        self.newpkg('simple')
        os.chdir('simple')
        simpleRecipe = (recipes.simpleRecipe 
                     + '\tr.addSource("foo.txt", dir="/foo1")\n'
                     )
        self.writeFile('simple.recipe', simpleRecipe)
        self.writeFile('foo.txt', 'lines\n')
        self.addfile('simple.recipe')
        self.addfile('foo.txt')
        self.commit()

        os.chdir('..')
        shutil.rmtree("simple")
        self.checkout('simple')
        os.chdir('simple')
        verifyConfigFlag('simple.recipe', 1)
        self.setSourceFlag('simple.recipe', binary = True)
        self.commit()

        os.chdir('..')
        shutil.rmtree("simple")
        self.checkout('simple')
        os.chdir('simple')
        verifyConfigFlag('simple.recipe', 0)
        self.setSourceFlag('simple.recipe', text = True)
        self.commit()

        os.chdir('..')
        shutil.rmtree("simple")
        self.checkout('simple')
        os.chdir('simple')
        verifyConfigFlag('simple.recipe', 1)
        self.setSourceFlag('simple.recipe')
        verifyConfigFlag('simple.recipe', 1)

    def testAdd(self):
        os.chdir(self.workDir)

        self.newpkg('simple')
        os.chdir('simple')

        self.writeFile('simple.recipe', 'recipe\n')

        self.writeFile('SomeFile', 'text\n')
        self.logCheck(self.addfile, ('SomeFile',),
            "error: cannot determine if SomeFile is binary or text. please "
            "add --binary or --text and rerun cvc add for SomeFile")

        self.addfile('SomeFile', binary = True)

    def testBinaryConflict(self):
        os.chdir(self.workDir)

        self.newpkg('simple')
        os.chdir('simple')
        simpleRecipe = (recipes.simpleRecipe
                        + '\tr.addSource("foo.tar")\n')
        self.writeFile('simple.recipe', simpleRecipe)

        self.writeFile('simple.recipe', simpleRecipe)
        self.writeFile('foo.tar', 'first\n')
        self.addfile('simple.recipe')
        self.addfile('foo.tar')
        self.commit()

        self.writeFile('foo.tar', 'second\n')
        self.commit()

        self.update('1-1')
        self.writeFile('foo.tar', 'third\n')
        self.logCheck(self.update, (),
                      "error: file contents conflict for "
                      "%s/simple/foo.tar" % self.workDir)

    def testCvcContext(self):
        os.chdir(self.workDir)
        self.writeFile('conaryrc', '''\
[context]
installLabelPath localhost@rpl:new
repositoryMap foo http://foo.com
''')
        cfg = copy.deepcopy(self.cfg)
        cfg.includeConfigFile('conaryrc')
        rc, txt = self.captureOutput(self.context, cfg=cfg)
        self.assertEquals(txt, 'No context set.\n')
        assert(not os.path.exists('CONARY'))
        self.context('context', cfg=cfg)
        assert(os.path.exists('CONARY'))
        assert('context context\n' in open('CONARY').readlines())
        cfg.setContext('context')
        rc, txt = self.captureOutput(self.context, cfg=cfg)
        self.assertEquals(txt, '''\
[context]
installLabelPath          localhost@rpl:new
repositoryMap             foo                       http://foo.com
''')

    @context('labelmultiplicity')
    def testCommitLabelMultiplicity(self):
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.addComponent('simple:source', '/localhost@rpl:foo//linux/1-1')
        self.writeFile('simple.recipe', recipes.simpleRecipe)
        self.addfile('simple.recipe')
        rc, txt = self.captureOutput(self.commit)
        self.assertEquals(txt, '''\
WARNING: performing this commit will switch the active branch:

New version simple:source=/localhost@rpl:linux/1-1
   obsoletes existing simple:source=/localhost@rpl:foo//linux/1-1
error: interactive mode is required when changing active branch
''')
        self.cfg.interactive = True
        oldStdin = sys.stdin
        try:
            sys.stdin = StringIO('y\n')
            rc, txt = self.captureOutput(self.commit)
        finally:
            self.cfg.interactive = False
            sys.stdin = oldStdin

        # just want to make sure the commit actually worked
        repos = self.openRepository()
        repos.findTrove(None, ('simple:source', '/localhost@rpl:linux', None))


    def testCvcLogWithLabelMultiplicity(self):
        # test CNY-706
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', recipes.simpleRecipe)
        self.addfile('simple.recipe')
        rc, txt = self.captureOutput(self.commit)
        self.writeFile('simple.recipe', recipes.simpleRecipe + '\n#\n')
        rc, txt = self.captureOutput(self.commit)

        # add another source component on the same label
        self.addComponent('simple:source', '/localhost@rpl:foo//linux/1-1')

        s = self.showLog()
        self.assertEquals(s, 'Name  : simple:source\nBranch: /localhost@rpl:linux\n\n1-2 Test \n    foo\n\n1-1 Test \n    foo\n\n')

    def testNewPackageBadName(self):
        self.assertRaises(errors.CvcError, self.newpkg, 'foo/bar')

    def testDoubleAdd(self):
        # test a file being added both locally and in the repository before
        # an update
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', recipes.simpleRecipe)
        self.addfile('simple.recipe')
        self.commit()
        self.writeFile('another', "contents\n")
        self.addfile('another', text = True)
        self.commit()
        self.update('1-1')
        self.writeFile('another', "other contents\n")
        self.addfile('another', text = True)
        self.logCheck(self.update, (),
                  "error: path another added both locally and in repository")
        self.remove('another')
        self.update()

    def testLotsOfFiles(self):
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', recipes.simpleRecipe)
        self.addfile("simple.recipe")
        fcount = 2000

        def addFiles(start, count):
            # Start adding a bunch of files
            fnames = []
            for i in range(start, start + count):
                fname = 'file-%05d.txt' % i
                fcont = 'Content for %s\n' % fname
                self.writeFile(fname, fcont)
                fnames.append(fname)
            # Add all files
            t0 = time.time()
            self.addfile(*fnames)
            t1 = time.time()
            return t1 - t0

        elapsed = []
        for i in range(4):
            elapsed.append(addFiles((i + 1) * fcount, fcount))
        for i in range(1, 4):
            # Trying to catch quadratic behavior
            if elapsed[i] > 2 * elapsed[i - 1]:
                sys.stderr.write("\nWarning: testLotsOfFiles: Iteration %d: "
                                 "times: previous: %.3f; current: %.3f\n\n" %
                                     (i, elapsed[i-1], elapsed[i]))
                break

    def testSymlinks(self):
        os.chdir(self.workDir)
        self.newpkg('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', recipes.simpleRecipe)
        self.addfile("simple.recipe")

        os.mkdir("dir1")
        flist = []
        self.writeFile('target1.txt', recipes.simpleRecipe)
        os.symlink('target1.txt', 'link1')
        # Make sure the link appears before the target
        flist.append('link1')
        flist.append('target1.txt')
        self.addfile(*flist)

        # Adding CONARY should fail
        self.logCheck(self.addfile, ('./././CONARY', ),
            'error: refusing to add CONARY to the list of managed sources')

        # Adding the same file multipe times should fail
        self.logCheck(self.addfile, ('./././target1.txt', ),
             'error: file target1.txt is already part of this source component')

        # Absolute symlink
        os.symlink('/foo', 'link2')
        self.logCheck(self.addfile, ('link2', ),
            'error: not adding absolute symlink link2 -> /foo')

        # Invalid components
        os.symlink('a/../b/../c', 'link3')
        self.logCheck(self.addfile, ('link3', ),
            'error: not adding symlink with bad destination '
            'link3 -> a/../b/../c')

        # Dangling symlink
        os.symlink('dangling', 'link4')
        self.logCheck(self.addfile, ('link4', ),
            'error: not adding broken symlink link4 -> dangling')

        # Multiple symlinks chained
        os.symlink('target1.txt', 'link5')
        os.symlink('link5', 'link6')
        os.symlink('link6', 'link7')
        os.symlink('link6', 'link8')
        self.addfile('link5', 'link6', 'link6', 'link8')

        # Symlink pointing to a file not tracked (link9)
        os.symlink('target1.txt', 'link9')
        os.symlink('link9', 'link10')
        self.logCheck(self.addfile, ('link10', ),
            'error: not adding link with dereferenced file not tracked '
            'link10 -> link9')

        # Symlink loop - will appear as a broken symlink
        os.symlink('link11', 'link12')
        os.symlink('link12', 'link13')
        os.symlink('link13', 'link11')
        self.logCheck(self.addfile, ('link11', 'link12', 'link13'),
            ('error: not adding broken symlink link11 -> link13',
             'error: not adding broken symlink link12 -> link11',
             'error: not adding broken symlink link13 -> link12',))

    def testAddSourceUrlQuote(self):
        # CNY-2389
        contentServer = rephelp.HTTPServerController(lookasidetest.getRequester())
        try:
            url = contentServer.url()
            myRecipe = """
class SimpleRecipe(PackageRecipe):
    clearBuildReqs()
    name = 'simple'
    version = '1'

    def setup(r):
        r.addSource('%s/foo%%%%20bar/baz', dir='/asdf')
        r.addSource('%s/foo bar/boop', dir='/asdf')
""" % (url, url)
            (built, d) = self.buildRecipe(myRecipe, "SimpleRecipe")
            self.updatePkg('simple')
            file1 = os.path.join(self.rootDir, 'asdf/baz')
            file2 = os.path.join(self.rootDir, 'asdf/boop')
            self.assertEqual(file(file1).read(), '//foo%20bar/baz:1\n')
            self.assertEqual(file(file2).read(), '//foo%20bar/boop:1\n')
        finally:
            contentServer.kill()

    def testAddSourceTwice(self):
        contentServer = rephelp.HTTPServerController(lookasidetest.getRequester())
        try:
            url = contentServer.url()
            myRecipe = """\
class SimpleRecipe(PackageRecipe):
    clearBuildReqs()
    name = 'simple'
    version = '1'

    def setup(r):
        r.addSource('%s/foo', dir='asdf')
        r.addSource('%s/foo', dir='asdf2')
        r.Create('/asdf/foo')
""" % (url, url)
            os.chdir(self.workDir)
            self.newpkg('simple')
            os.chdir('simple')
            self.writeFile('simple.recipe', myRecipe)
            self.addfile("simple.recipe")
            # used to fail bc of the double-add of foo
            self.captureOutput(self.commit)
        finally:
            contentServer.kill()

    def testAddSourceDir(self):
        contentServer = rephelp.HTTPServerController(lookasidetest.getRequester())
        try:
            url = contentServer.url()
            myRecipe = """\
class SimpleRecipe(PackageRecipe):
    clearBuildReqs()
    name = 'simple'
    version = '1'

    def setup(r):
        r.Create('/foo.patch')
        r.addPatch('foo.patch', sourceDir = '/')
"""
            os.chdir(self.workDir)
            self.newpkg('simple')
            os.chdir('simple')
            self.writeFile('simple.recipe', myRecipe)
            self.addfile("simple.recipe")
            # used to fail bc useage of sourceDir did not disable lookaside
            self.captureOutput(self.commit)
        finally:
            contentServer.kill()

    def testAddSourceDir2(self):
        contentServer = rephelp.HTTPServerController(lookasidetest.getRequester())
        try:
            url = contentServer.url()
            myRecipe = """\
class SimpleRecipe(PackageRecipe):
    clearBuildReqs()
    name = 'simple'
    version = '1'

    def setup(r):
        r.Create('/foo.patch')
        r.addPatch('foo.patch', sourceDir = '/')
        r.addPatch('foo.patch')
"""
            os.chdir(self.workDir)
            self.newpkg('simple')
            os.chdir('simple')
            self.writeFile('simple.recipe', myRecipe)
            self.addfile("simple.recipe")
            # one addPatch should fail.
            self.assertRaises(errors.CvcError, self.captureOutput, self.commit)
        finally:
            contentServer.kill()

    def testSourceDirExclusion(self):
        class DummyPackageRecipe(packagerecipe.PackageRecipe):
            def __init__(x, cfg):
                x.name = 'package'
                x.version = '1.0'
                packagerecipe.PackageRecipe.__init__(x, cfg, None, None)
                x._loadSourceActions(lambda x: True)
                x.loadPolicy()
            def _makeSources(x):
                res = []
                count = 0
                for sourceDir in ('test', None):
                    for klass in (source.addSource, source.addPatch,
                            source.addArchive):
                        count += 1
                        obj = klass(x, 'foo%d' % count, sourceDir = sourceDir)
                        obj.fetchLocal = obj.getPath
                        res.append(obj)
                return res

        recipe = DummyPackageRecipe(self.cfg)
        recipe._sources = recipe._makeSources()
        srcPathList = recipe.getSourcePathList()
        self.assertEquals(len(recipe._sources), 6)
        self.assertEquals([x.getPath() for x in srcPathList], ['foo4', 'foo5',
                'foo6'])
        self.assertEquals(recipe.fetchLocalSources(), ['foo4', 'foo5', 'foo6'])

    def testUnknownUser(self):
        self.addComponent('foo:source', '1.0',
                          fileContents = [ ( 'foo.recipe',
                            rephelp.RegularFile( owner = 'unknownuser2',
                                                group = 'unknowngroup2') ) ] )
        self.addComponent('foo:source', '2.0',
                          fileContents = [ 'foo.recipe', ( 'new.file',
                            rephelp.RegularFile(perms = 0600,
                                                contents = '2',
                                                owner = 'unknownuser2',
                                                group = 'unknowngroup2') ) ] )
        os.chdir(self.workDir)
        self.logCheck2([], self.checkout, 'foo:source=1.0')
        os.chdir('foo')
        self.logCheck2([], self.update)

    def testGroupSources(self):
        os.chdir(self.workDir)
        self.newpkg('group-basic')
        os.chdir('group-basic')
        self.writeFile('group-basic.recipe',
                "class basicGroup(GroupRecipe):\n"
                "   name = 'group-basic'\n"
                "   version = '1.0'\n"
                "   clearBuildRequires()\n"
                "\n"
                "   def setup(self):\n"
                "       self.addPreUpdateScript('script-file')\n")
        self.writeFile('script-file', 'a script file\n')
        self.addfile("group-basic.recipe")
        self.addfile("script-file", text = True)
        self.commit()
        conaryState = state.ConaryStateFromFile('CONARY', None)
        sourceState = conaryState.getSourceState()
        files = sorted([ x[1] for x in sourceState.iterFileList() ])
        self.assertEquals(files, [ 'group-basic.recipe', 'script-file' ] )

    def testRevert(self):
        os.chdir(self.workDir)
        self.newpkg('foo')
        os.chdir('foo')

        r1 = ("class fooRecipe(PackageRecipe):\n"
              "   name = 'foo'\n"
              "   version = '1.0'\n"
              "\n"
              "   def setup(r):\n"
              "      r.addArchive('distcc-2.9.tar.bz2')\n"
              "\n")
        self.writeFile('foo.recipe', r1)
        perms = os.stat('foo.recipe').st_mode & 0777;
        assert(perms != 0200)
        self.addfile('foo.recipe')

        self.writeFile('other', 'some contents\n')
        self.addfile('other', text = True)

        os.symlink('other', 'symlink')
        self.addfile('symlink')

        self.commit()
        self.writeFile('foo.recipe', '')
        self.revertSource()
        self.verifyFile('foo.recipe', r1)

        os.unlink('foo.recipe')
        self.revertSource()
        self.verifyFile('foo.recipe', r1)

        os.chmod('foo.recipe', 0200)
        self.revertSource()
        self.assertEqual(perms, os.stat('foo.recipe').st_mode & 0777)

        self.logCheck(self.commit, (),
                      '+ no changes have been made to commit')

        os.unlink('foo.recipe')
        os.unlink('other')
        self.revertSource('other')
        self.verifyFile('other', 'some contents\n')
        assert(not os.path.exists('foo.recipe'))
        self.writeFile('other', '')
        self.revertSource('foo.recipe')
        self.verifyFile('foo.recipe', r1)
        self.verifyFile('other', '')
        self.revertSource()
        self.verifyFile('other', 'some contents\n')

        try:
            self.revertSource('distcc-2.9.tar.bz2')
        except cvcerrors.CvcError, e:
            self.assertEqual(str(e), 'autosource files cannot be reverted')
        else:
            assert(0)

        self.logCheck2([ 'error: file unknown-file not found in source '
                         'component' ], self.revertSource,  'unknown-file')

        self.writeFile('foo', '')
        self.addfile('foo', text = True)
        self.logCheck2([ 'error: file foo was newly added; use cvc remove '
                         'to remove it' ], self.revertSource,  'foo')

        os.unlink('symlink')
        self.revertSource()
        self.assertEqual(os.readlink('symlink'), 'other')

        # make sure removing something from the source component, not just
        # from the filesystem, gets reverted
        oldState = open('CONARY').read()
        self.remove('other')
        self.revertSource('other')
        self.verifyFile('other', 'some contents\n')
        self.diff()
        newState = open('CONARY').read()
        self.assertEqual(oldState, newState)

    def testMarkRemovedSource(self):
        self.addComponent('simple:source', '1', '', 
                         [('simple.recipe', recipes.simpleRecipe)])
        self.markRemoved('simple:source')
        os.chdir(self.workDir)
        self.assertRaises(errors.TroveNotFound, self.checkout, 'simple')
        self.newpkg('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', recipes.simpleRecipe)
        self.addfile('simple.recipe')
        self.commit()
        trv = self.findAndGetTrove('simple:source')
        self.assertEquals(str(trv.getVersion().trailingRevision()), '1-2')
        self.writeFile('simple.recipe', recipes.simpleRecipe + '#comment\n')
        self.commit()
        self.markRemoved('simple:source=1-3')
        os.chdir('..')
        util.rmtree('simple')
        self.checkout('simple')
        os.chdir('simple')
        self.writeFile('simple.recipe', recipes.simpleRecipe + '#comment\n')
        self.commit()
        trv = self.findAndGetTrove('simple:source')
        self.assertEquals(str(trv.getVersion().trailingRevision()), '1-4')

    def testRedundantAdd(self):
        # CNY-1428
        self.addComponent('simple:source', '1', '',
                         [('simple.recipe', recipes.simpleRecipe) ] )

        self.addComponent('simple:source', '2', '',
                         [('simple.recipe', recipes.simpleRecipe),
                          ('somefile', 'some text\n' )])

        os.chdir(self.workDir)
        self.checkout('simple=1')
        os.chdir('simple')
        self.writeFile('somefile', 'some text\n')
        self.addfile('somefile', binary = True)
        self.update()
        self.verifyFile('somefile', 'some text\n')
        self.logCheck(self.commit, (),
                      '+ no changes have been made to commit')

    def testCommitWithDynamicSearchPath(self):
        # CNY-1740
        os.chdir(self.workDir)
        self.newpkg('group-basic')
        os.chdir('group-basic')
        self.writeFile('group-basic.recipe',
                "class basicGroup(GroupRecipe):\n"
                "   name = 'group-basic'\n"
                "   version = '1.0'\n"
                "   clearBuildRequires()\n"
                "\n"
                "   def setup(r):\n"
                "       r.setSearchPath(r.labelPath[0])\n")
        self.addfile('group-basic.recipe')
        self.commit()
        f = open('group-basic.recipe', 'a')
        f.write('\n')
        f.close()
        self.commit()

    def testBadCONARYFile(self):
        os.chdir(self.workDir)
        self.assertRaises(state.CONARYFileMissing, self.update)
        os.mkdir('CONARY')
        self.assertRaises(state.CONARYNotFile, self.update)

    def testUsingBuildlabel(self):
        # buildlabel is not defined at checkin time.  But that
        # should be ok because unknown macros are allowed at checkin 
        # time.
        os.chdir(self.workDir)
        self.newpkg('group-test')
        os.chdir('group-test')
        self.writeFile('group-test.recipe', """\
class GroupTest(GroupRecipe):
    name = 'group-test'
    version = '1'
    clearBuildRequires()
    def setup(r):
        r.setSearchPath(r.macros.buildlabel)
        r.add('foo:run')
""")
        self.addfile('group-test.recipe')
        self.commit()
        self.addComponent('foo:run=1')
        repos = self.openRepository()
        cfg = self.cfg
        self.cookItem(repos, cfg, 'group-test')
        self.promote('--:branch', 'group-test:source')
        self.addComponent('foo:run=:branch/1')
        self.cookItem(repos, cfg, 'group-test=:branch')
        trv = self.findAndGetTrove('group-test=:branch')
        self.assertEquals(str(trv.iterTroveList(strongRefs=True).next()[1].trailingLabel()), 'localhost@rpl:branch')

    def testUpdate(self):
        os.chdir(self.workDir)

        self.addComponent('foo:source', '1-1',
                          fileContents = [ ('a', '1') ] )
        self.addComponent('foo:source', '2-2',
                          fileContents = [ ('a', '2') ] )

        self.addComponent('bar:source', '1-1',
                          fileContents = [ ('b', '1') ] )
        self.addComponent('bar:source', '2-2',
                          fileContents = [ ('b', '2') ] )

        self.checkout('foo=1-1')
        self.verifyFile('foo/a', '1')

        os.chdir('foo')
        self.update()
        self.verifyFile('a', '2')
        os.chdir('..')

        self.update('foo=1-1')
        self.verifyFile('foo/a', '1')

        self.checkout('bar')
        self.verifyFile('bar/b', '2')

        self.update('foo=2-2', 'bar=1-1')
        self.verifyFile('foo/a', '2')
        self.verifyFile('bar/b', '1')

        self.update('foo=1', 'bar=1-1')
        self.verifyFile('foo/a', '1')
        self.verifyFile('bar/b', '1')

        self.update('foo', 'bar')
        self.verifyFile('foo/a', '2')
        self.verifyFile('bar/b', '2')

        self.logCheck2([], self.update, 'foo', 'bar')

    def testAddRecipe(self):
        # CNY-3200
        os.chdir(self.workDir)
        self.newpkg('foo')
        os.chdir('foo')
        self.writeFile('foo.recipe',
            'class Foo(PackageRecipe)\n'
            '    version = "1"\n'
            '    name = "foo"\n')
        self.addfile('foo.recipe', binary = True)
        trv = state.ConaryStateFromFile('CONARY').getSourceState()

        pathId = [ x[0] for x in trv.iterFileList() ][0]
        assert(trv.fileIsConfig(pathId))

    def testCheckout2(self):
        def _checkState(path, fileList):
            trv = state.ConaryStateFromFile(path).getSourceState()
            filesInState = [ x[1] for x in trv.iterFileList() ]
            self.assertEquals(set(filesInState), set(fileList))

        self.addComponent('foo:source', '1-1',
                          fileContents = [ ('a', '1') ] )
        self.addComponent('foo:source', '2-2',
                          fileContents = [ ('a', '2') ] )

        self.addComponent('bar:source', '1-1',
                          fileContents = [ ('b', '1') ] )
        self.addComponent('bar:source', '2-2',
                          fileContents = [ ('b', '2') ] )

        os.chdir(self.workDir)

        # this fails with a usage message; can't check out two things into
        # the same directory
        (rc, str) = self.captureOutput(self.checkout, ['foo', 'bar'],
                                       dir = 'something')
        assert(str.lower().startswith('usage'))

        self.checkout(['foo', 'bar'])
        self.verifyFile('foo/a', '2')
        self.verifyFile('bar/b', '2')
        # make sure the CONARY file looks right
        _checkState("foo/CONARY", [ "a" ])
        _checkState("bar/CONARY", [ "b" ])

        shutil.rmtree('foo')
        shutil.rmtree('bar')

        self.checkout(['foo=1-1', 'bar'])
        self.verifyFile('foo/a', '1')
        self.verifyFile('bar/b', '2')

    def testLoadInstalledUsesReposOnCheckin(self):
        fooRecipe = """
loadInstalled('bar')
class FooRecipe(PackageRecipe):
    name = 'foo'
    version = '1'
    def setup(r):
       r.macros.bar = BarRecipe.bar
       r.Create('%(bar)s')
"""
        barRecipe = """
class BarRecipe(PackageRecipe):
    name = 'bar'
    version = '1'
    def setup(r):
        r.Create('/blah')
"""
        # don't use loadInstalled when running "checkin"
        self.addComponent('bar:source=1',  [('bar.recipe', barRecipe)])
        self.addComponent('bar:runtime=1')
        self.addCollection('bar=1', [':runtime'])
        self.updatePkg('bar')

        # add in bar = /bar in newest version of bar recipe - not installed
        barRecipe = barRecipe.replace('version', 'bar = "/bar"; version')
        self.addComponent('bar:source=2',  [('bar.recipe', barRecipe)])
        os.chdir(self.workDir)
        self.newpkg('foo')
        os.chdir('foo')
        self.writeFile('foo.recipe', fooRecipe)
        self.addfile('foo.recipe')
        self.commit()
        # if this succeeds, we've found the right bar to load.

    def testCvcKeyManagement(self):
        def _checkAdd(repos, server, user, key, check = []):
            self.assertEquals([server, user, key], check)

        def expect(server, user, asciiKey):
            self.mock(sys, 'stdin', StringIO(asciiKey))
            key = openpgpfile.parseAsciiArmorKey(asciiKey)
            self.mock(netclient.NetworkRepositoryClient, 'addNewPGPKey',
                      lambda r, s, u, k : _checkAdd(r, s, u, k,
                                            check = [ server, user, key ] ) )

        asciiKey = open('%s/key.asc' % resources.get_archive()).read()
        binaryKey = openpgpfile.parseAsciiArmorKey(asciiKey)

        self.openRepository()
        self.openRepository(1)
        s = self.captureOutput(keymgmt.displayKeys, self.cfg, None, None)[1]
        self.assertEquals(s, 'Public key fingerprints for user test on server '
                'localhost:\n    F94E405E\n')
        s = self.captureOutput(keymgmt.displayKeys, self.cfg, None, None,
                               showFingerprints = True)[1]
        self.assertEquals(s, 'Public key fingerprints for user test on server '
                'localhost:\n    95B457D16843B21EA3FC73BBC7C32FC1F94E405E\n')

        # make sure the servername arg works by stopping the first server
        self.stopRepository()
        s = self.captureOutput(keymgmt.displayKeys, self.cfg, 'localhost1',
                               None)[1]
        self.assertEquals(s, 'Public key fingerprints for user test on server '
                    'localhost1:\n    F94E405E\n')

        s = self.captureOutput(keymgmt.displayKeys, self.cfg, 'localhost1',
                               'missing')[1]
        self.assertEquals(s, 'No keys found for user missing on server localhost1.\n')

        s = self.captureOutput(keymgmt.showKey, self.cfg, 'localhost1',
                               'F94E405E')[1]
        fromServer = openpgpfile.parseAsciiArmorKey(s)
        self.assertEquals(fromServer, binaryKey)

        # restart localhost repository
        self.openRepository()
        s2 = self.captureOutput(keymgmt.showKey, self.cfg, None, 'F94E405E')[1]
        self.assertEquals(s, s2)

        expect('localhost', 'test', asciiKey)
        keymgmt.addKey(self.cfg, None, None)

        expect('localhost', 'user', asciiKey)
        keymgmt.addKey(self.cfg, None, 'user')

        expect('localhost1', 'test', asciiKey)
        keymgmt.addKey(self.cfg, 'localhost1', None)

    def testDuplicateSourceFile(self):
        # CNY-2543
        now = time.time()

        self.addComponent('simple:source', '/localhost@rpl:foo//linux/1-1',
                fileContents = [ ( 'simple.recipe', recipes.simpleRecipe),
                         ( 'a', rephelp.RegularFile(contents= 'hello\n',
                                                    config = False) ),
                         ( 'b', rephelp.RegularFile(contents= 'hello\n',
                                                    config = False) ) ] )
        os.chdir(self.workDir)
        self.checkout("simple")
        self.verifyFile("simple/a", "hello\n")
        self.verifyFile("simple/b", "hello\n")
        repos = self.openRepository()
        self.changeset(repos, ['simple:source'], 'foo.ccs')

    def testFactoryCommand(self):
        os.chdir(self.workDir)
        self.newpkg("foo", factory = 'bar')
        os.chdir('foo')
        (rc, str) = self.captureOutput(checkin.factory)
        self.assertEquals(str, 'bar\n')
        checkin.factory('baz')
        (rc, str) = self.captureOutput(checkin.factory)
        self.assertEquals(str, 'baz\n')
        checkin.factory('')
        (rc, str) = self.captureOutput(checkin.factory)
        self.assertEquals(str, '(none)\n')

    def testCvcUpdateToFactory(self):
        self.addComponent('simple:source', 
                          [('simple.recipe', recipes.simpleRecipe)])
        os.chdir(self.workDir)
        self.checkout('simple')
        self.addComponent('simple:source=2', 
                          [('simple.recipe', recipes.simpleRecipe)],
                          factory='foo')
        os.chdir('simple')
        self.update()
        assert('factory' in open('CONARY').read())


    def testFilePermChangeCache(self):
        repos = self.openRepository()
        self.addComponent('autosource:source', 
                          [('autosource.recipe', recipes.autoSource0 +
                          '\tr.Install("localfile", "/foo")\n\n'),
                           ('localfile', 'contents\n')])
        self.cookItem(repos, self.cfg, 'autosource')
        trv = self.findAndGetTrove('autosource:runtime')
        for pathId, path, fileId, fileVer in trv.iterFileList():
            fileObj = repos.getFileVersion(pathId, fileId, fileVer)
            mode = fileObj.inode.perms()
            assert(mode == 0644)

        os.chdir(self.workDir)
        self.checkout('autosource')
        os.chdir('autosource')
        os.chmod('localfile', 0755)
        self.commit()
        self.cookItem(repos, self.cfg, 'autosource')
        trv = self.findAndGetTrove('autosource:runtime')
        for pathId, path, fileId, fileVer in trv.iterFileList():
            fileObj = repos.getFileVersion(pathId, fileId, fileVer)
            mode = fileObj.inode.perms()
            assert(mode == 0755)


    def testGpgKeyRetrievalFailure(self):
        # Clear PGP build keyring
        try:
            file(os.path.join(self.buildDir, "pubring.pgp"), "w")
        except IOError:
            pass

        def fakeDoDownloadPublicKey(slf, keyServer):
            import socket
            raise socket.error("aa")

        self.mock(source._Source, '_doDownloadPublicKey', fakeDoDownloadPublicKey)

        recipestr = """
class R(PackageRecipe):
    name = 'testcase'
    version = '1'
    clearBuildReqs()

    def setup(r):
        r.addArchive("distcc-2.9.tar.bz2", keyid = "3C63CA3FA0B3E88B")
        r.Create("%(datadir)s/some-file", contents = "aaa")
"""
        e = self.assertRaises(source.SourceError,
            self.buildRecipe, recipestr, "R")
        self.assertEqual(str(e),
            'Failed to retrieve PGP key 3C63CA3FA0B3E88B')

    def testRedundantUpdate(self):
        os.chdir(self.workDir)
        self.newpkg("foo")
        os.chdir('foo')
        self.writeFile('foo.recipe',
            "class fooRecipe(PackageRecipe):\n"
              "   name = 'foo'\n"
              "   version = '1.0'\n"
              "\n"
              "   def setup(r):\n"
              "      r.addArchive('distcc-2.9.tar.bz2')\n"
              "\n")
        self.addfile("foo.recipe")
        self.commit()
        self.writeFile('test', 'hello\n')
        self.addfile('test', text = True)
        self.commit()
        self.update('1.0-1')
        self.writeFile('test', 'hello\n')
        self.update()




class SbWrapper:
    def __getattr__(self, attr):
        if attr == 'st_uid':
            return self.uid
        if attr == 'st_gid':
            return self.gid
        return self.sb.__getattribute__(attr)

    def __getitem__(self, item):
        if item == stat.ST_UID:
            return self.uid
        if item == stat.ST_GID:
            return self.gid
        return self.sb.__getitem__(item)
    
    def __init__(self, sb, uid, gid):
        self.sb = sb
        self.uid = uid
        self.gid = gid

class StatWrapper:
    def stat(self, *args):
        sb = self.realstat(*args)
        return SbWrapper(sb, self.uid, self.gid)

    def lstat(self, *args):
        sb = self.reallstat(*args)
        return SbWrapper(sb, self.uid, self.gid)

    def __init__(self, realstat, reallstat, uid, gid):
        self.realstat = realstat
        self.reallstat = reallstat
        self.uid = uid
        self.gid = gid
