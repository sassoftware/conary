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

#conary
from conary import errors
from conary.deps import deps
from conary.local import database
from conary.cmds import cscmd
from conary.repository import changeset
from conary.cmds.showchangeset import displayChangeSet
from conary import versions

#test
from conary_test import recipes
from conary_test import rephelp


class FileFromLine:
    def __init__(self, line):
        args = line.split()
        # XXX eventually we may want to handle the optional args here
        assert(len(args) in (9, 10))
        if len(args) == 9:
            (self.mode, self.nodes, self.owner, 
             self.grp, self.size, mo, day, tm, self.path) = args[0:9]
            self.change = 'New'
        else:
            (self.change, self.mode, self.nodes, self.owner, 
             self.grp, self.size, mo, day, tm, self.path) = args[0:10]


    def __repr__(self):
        return "<%s %s>" % (self.change, self.path)

class ShowChangesetTest(rephelp.RepositoryHelper):

    def _parseFileList(self, lines):
        """ Takes the output of a file listing and returns tuples w/ time
            removed """
        files = {}
        for line in lines:
            if not line:
                continue
            newFile = FileFromLine(line)
            files[newFile.path] = newFile
        return files
            

    def testAbsoluteChangeSet(self):
        self.resetRepository()
        d = tempfile.mkdtemp()

        origDir = os.getcwd()
        os.chdir(d)
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        repos = self.openRepository()
        try:
            built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                            'testcase.recipe')
            cs = changeset.ChangeSetFromFile('testcase-1.0.ccs')
        finally:
            os.chdir(origDir)
            shutil.rmtree(d)
        rc, res = self.captureOutput(displayChangeSet, None, cs, None, 
                                                                    self.cfg)
        assert(res == 'testcase=1.0-1-0.1\n')
        rc, res = self.captureOutput(displayChangeSet, None, cs, 
                                            ['testcase=1.0-1-0.1'], self.cfg,
                                            showTroves=True)
        assert(res == '''\
testcase=1.0-1-0.1
  testcase:runtime=1.0-1-0.1
''')
        rc, res = self.captureOutput(displayChangeSet, None, cs, 
                                            ['testcase:runtime=1.0-1-0.1'], 
                                            self.cfg, fileVersions=True,
                                            alwaysDisplayHeaders=True)
        assert(res == '''\
testcase:runtime=1.0-1-0.1
  /etc/changedconfig    1.0-1-0.1
  /etc/unchangedconfig    1.0-1-0.1
  /usr/bin/hello    1.0-1-0.1
  /usr/share/changed    1.0-1-0.1
  /usr/share/unchanged    1.0-1-0.1
''')
        rc, res = self.captureOutput(displayChangeSet, None, cs, 
                                            ['testcase:runtime=1.0-1-0.1'], 
                                            self.cfg, lsl=True)
        lines = res.split('\n')
        files = self._parseFileList(lines)
        assert(len(files) == 5)
        paths  = files.keys()
        paths.sort()
        assert(paths == [ '/etc/changedconfig', '/etc/unchangedconfig',
                        '/usr/bin/hello', '/usr/share/changed', 
                        '/usr/share/unchanged'])
        for f in files.values():
            assert f.change == 'New'
        rc, res = self.captureOutput(displayChangeSet, None, cs, 
                                    ['testcase:runtime=1.0-1-0.1'], 
                                    self.cfg, info=True)
        # we don't test the output format of --info too closely, mostly just
        # want to make sure it runs.
        assert('testcase:runtime' in res)
        assert('1.0-1-0.1' in res)
        assert('is: x86' in res)

        rc, res = self.captureOutput(displayChangeSet, None, cs, 
                                    ['testcase:runtime=1.0-1-0.1'], 
                                    self.cfg, asDiff=True)

    def testGitDiff(self):
        # very simple test of scs --diff
        t = self.addComponent('foo:run=1',
                              fileContents = [ ( '/foo', 'contents\n') ])
        repos = self.openRepository()
        cs = repos.createChangeSet([ ('foo:run', (None, None),
                                      t.getNameVersionFlavor()[1:], False) ])
        rc, res = self.captureOutput(displayChangeSet, None, cs, [],
                                    self.cfg, asDiff=True)
        self.assertEquals(res,
            "diff --git a/foo b/foo\n"
            "new user root\n"
            "new group root\n"
            "new mode 100644\n"
            "--- a/dev/null\n"
            "+++ b/foo\n"
            "@@ -1,0 +1,1 @@\n"
            "+contents\n")

    def testPartialChangeSet(self):
        db = database.Database(self.rootDir, self.cfg.dbPath)
        self.repos = self.openRepository()
        self.addTestPkg(1, content='r.Create("%(thisdocdir)s/README")')
        self.cookTestPkg(1)
        self.cfg.configLine('excludeTroves .*:runtime') 
        os.chdir(self.workDir)
        cscmd.ChangeSetCommand(self.cfg, ['test1'], 'test.ccs') 
        cs = changeset.ChangeSetFromFile('test.ccs')
        rc, res = self.captureOutput(displayChangeSet, None, cs,
                                     ['test1=1.0-1-1'],   self.cfg,
                                     showTroves=True)

        assert(res == '''\
test1=1.0-1-1
  test1:runtime=1.0-1-1
''')

    def testEraseChangeSet(self):
        db = database.Database(self.rootDir, self.cfg.dbPath)
        self.repos = self.openRepository()
        self.addTestPkg(1, content='r.Create("%(thisdocdir)s/README")')
        self.cookTestPkg(1)
        os.chdir(self.workDir)
        cscmd.ChangeSetCommand(self.cfg, ['test1=1.0--'], 'test.ccs') 
        cs = changeset.ChangeSetFromFile('test.ccs')
        rc, res = self.captureOutput(displayChangeSet, db, 
                                     cs, ['test1=1.0'], self.cfg)
        assert(res == 'Erase   test1=1.0-1-1\n')

    def testChangedFiles(self):
        # set up a recipe in a :source component
        self.repos = self.openRepository()
        self.addTestPkg(1, content='r.Create("/etc/foo", contents="A\\n"*1000)',
                            tag='myTag')
        self.cookTestPkg(1)
        res = self.addTestPkg(1, fileContents='a change in the file', 
                                                                tag='myTag2')
        self.cookTestPkg(1)
        v1 = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        v2 = versions.VersionFromString('/localhost@rpl:linux/1.0-2-1')

        cs = self.repos.createChangeSet([('test1:runtime', 
                                         (v1, deps.Flavor()), 
                                         (v2, deps.Flavor()), False)])

        rc, res = self.captureOutput(displayChangeSet, None, cs, None, 
                                            self.cfg, lsl=True)
        lines = res.split('\n')
        files = self._parseFileList(lines)
        assert(files['/usr/bin/test1'].change == 'Mod')
        assert(files['/etc/foo'].change == 'Del')
        rc, res = self.captureOutput(displayChangeSet, None, cs, 
                                    None, self.cfg, lsl=True, showChanges=True)
        lines = res.split('\n')
        self.assertEquals(lines[3].split()[1], '52')
        rc, res = self.captureOutput(displayChangeSet, None, cs, 
                                    None, self.cfg, lsl=True, showChanges=True,
                                    tags=True)
        lines = res.split('\n')
        assert(lines[2].find('{myTag}') != -1)
        assert(lines[3].find('{myTag2}') != -1)
        rc, res = self.captureOutput(displayChangeSet, None, cs, 
                                    None, self.cfg, lsl=True, showChanges=True,
                                    ids=True, sha1s=True)
        lines = res.split('\n')
        oldFile = lines[2].split()
        newFile = lines[3].split()
        # two changes btw the files -- sha1 and size -- possibly date
        assert(len(newFile) in (3, 6))
        assert(oldFile[2] != newFile[1])
        assert(len(newFile[1]) == 40)

    def testGroupChangeSet(self):
        repos = self.openRepository()
        os.chdir(self.workDir)
        self.addQuickTestComponent('test:runtime', '1.0', 
                                    fileContents=['/usr/run1'])
        self.addQuickTestComponent('test:doc', '1.0', 
                                   fileContents=['/usr/doc1'])
        self.addQuickTestCollection('test', '1.0', ['test:runtime', 'test:doc'])
        self.addQuickTestCollection('group-test', '1.0', ['test'])
        self.changeset(repos, ['group-test'], 'group-test.ccs', recurse=False)
        cs = changeset.ChangeSetFromFile('group-test.ccs')
        rc, res = self.captureOutput(displayChangeSet, None, cs, [], self.cfg)
        assert(res == 'group-test=1.0-1-1\n')
        rc, res = self.captureOutput(displayChangeSet, None, cs, [], self.cfg,
                                     ls=True, alwaysDisplayHeaders=True,
                                     recurseRepos=True)
        assert(res == '''\
group-test=1.0-1-1
  test=1.0-1-1
    test:doc=1.0-1-1
      /usr/doc1
    test:runtime=1.0-1-1
      /usr/run1
''')

    def testExactFlavor(self):
        self.addComponent('foo:run[~ssl]')
        repos = self.openRepository()
        csPath = self.workDir + '/foo.ccs'
        self.changeset(repos, ['foo:run'], csPath)
        cs = changeset.ChangeSetFromFile(csPath)
        self.assertRaises(errors.TroveNotFound,
                        displayChangeSet, None, cs, ['foo:run[ssl]'], self.cfg,
                        exactFlavors=True)
        self.assertRaises(errors.TroveNotFound,
                        displayChangeSet, None, cs, ['foo:run'], self.cfg,
                        exactFlavors=True)
        self.captureOutput(displayChangeSet, None, cs, ['foo:run[~ssl]'], 
                           self.cfg,exactFlavors=True)
