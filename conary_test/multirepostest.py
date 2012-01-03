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

from conary_test import recipes
from conary_test import rephelp

from conary import trove
from conary import versions
from conary.build import use
from conary.deps import deps
from conary.lib import util
from conary.local import database
from conary.repository import changeset, filecontainer
from conary.versions import Label

class MultipleRepositoryTest(rephelp.RepositoryHelper):
    def testDistributedBranch(self):
        origDir = os.getcwd()
        try:
            self.resetAllRepositories()
            self.resetWork()

            self.openRepository(0)
            repos = self.openRepository(1)

            repos.deleteUserByName(Label('localhost@foo:bar'), 'anonymous')
            repos.deleteUserByName(Label('localhost1@foo:bar'), 'anonymous')

            # create initial source component in the first repository
            srcdir = os.sep.join((self.workDir, 'src'))
            os.mkdir(srcdir)
            os.chdir(srcdir)
            self.newpkg('testcase')
            os.chdir('testcase')
            self.writeFile('testcase.recipe', recipes.testRecipe1)
            self.addfile('testcase.recipe')
            self.commit()

            # build testcase binary components
            built, out = self.captureOutput(self.cookItem, repos, self.cfg,
                                            'testcase')
            flavor = use.Arch.getCurrentArch()._toDependency()
            assert(built == ((('testcase:runtime',
                               '/localhost@rpl:linux/1.0-1-1', flavor),), None))

            # branch testcase:source on second repository
            newLabel = versions.Label("localhost1@rpl:linux")
            self.mkbranch(self.cfg.buildLabel, newLabel, 'testcase:source')

            # check out branched source component
            origBuildLabel = self.cfg.buildLabel
            self.cfg.buildLabel = newLabel
            os.chdir(srcdir)
            shutil.rmtree('testcase')
            self.checkout('testcase')
            os.chdir('testcase')

            # make a modification on the branch
            f = open('testcase.recipe', 'a')
            f.write('        self.Create("/newfile", contents="hello, world")\n')
            f.close()
            os.system("sed -i 's/fileText = .*/initialFileText = initialFileText.replace(\"4\", \"5\"); &/' testcase.recipe")
            self.commit()

            # build testcase binary components
            built, out = self.captureOutput(self.cookItem, repos, self.cfg,
                                            'testcase')
            branchedVersion = versions.VersionFromString(
                      '/localhost@rpl:linux/1.0-1-0/localhost1@rpl:linux/2-1')
            assert(built ==
                   ((('testcase:runtime',
                      '/localhost@rpl:linux/1.0-1-0/localhost1@rpl:linux/2-1',
                      flavor),),
                    None))

            # see if we can create a changeset
            repos = self.openRepository()
            (fd, path) = tempfile.mkstemp()
            os.close(fd)
            repos.createChangeSetFile(
                [('testcase', (None, None), (branchedVersion, flavor), 0)], path)
            os.unlink(path)

            # install the branched testcase:runtime component
            self.updatePkg(self.cfg.root,
                           'testcase:runtime', 'localhost1@rpl:linux')
            self.verifyFile(os.sep.join((self.cfg.root, 'newfile')),
                            "hello, world\n")


            # try flipping from one version to another while changing the config
            # file
            self.resetRoot()
            self.updatePkg(self.cfg.root, 'testcase', 'localhost@rpl:linux')

            f = open(self.cfg.root + "/etc/changedconfig", "a")
            f.write("new line\n")
            f.close()

            self.updatePkg(self.cfg.root, 'testcase', 'localhost1@rpl:linux')

            # try flipping from one version to another
            self.resetRoot()
            self.updatePkg(self.cfg.root, 'testcase', 'localhost@rpl:linux')
            self.updatePkg(self.cfg.root, 'testcase', 'localhost1@rpl:linux')
            self.verifyFile(os.sep.join((self.cfg.root, 'newfile')),
                            "hello, world\n")
            # and back again, though this doesn even need repository 1 anymore
            self.servers.stopServer(1)
            self.updatePkg(self.cfg.root, 'testcase', 'localhost@rpl:linux')
            assert(not os.path.exists(os.sep.join((self.cfg.root, 'newfile'))))
        finally:
            os.chdir(origDir)

    def testInstallPath(self):
        # make sure that an initial install picks the trove from a single
        # repository
        self.resetRepository()
        self.resetRoot()

        rc = self.buildRecipe(recipes.simpleConfig1, 'SimpleConfig1')
        trunkVersion, trunkFlavor = rc[0][0][1:3]

        repos1 = self.openRepository(1)
        newLabel = versions.Label("localhost1@rpl:linux")
        oldLabel = self.cfg.buildLabel
        self.cfg.buildLabel = newLabel

        rc = self.buildRecipe(recipes.simpleConfig1, 'SimpleConfig1')
        self.cfg.buildLabel = oldLabel
        branchVersion, branchFlavor = rc[0][0][1:3]

        oldPath = self.cfg.installLabelPath
        self.cfg.installLabelPath = [ oldLabel, newLabel ]

        self.updatePkg(self.rootDir, 'simpleconfig')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        l = db.getTroveVersionList('simpleconfig')
        assert([ x.asString() for x in l ] == 
                        [ '/localhost@rpl:linux/1.0-1-1' ])

        # try and make a changeset between the two versions, just to see
        # if it can be done (this is a test of client-side changeset
        # generation)
        repos = self.openRepository()
        repos.createChangeSet([('simpleconfig', 
                                (versions.VersionFromString(trunkVersion),
                                 trunkFlavor),
                                (versions.VersionFromString(branchVersion),
                                 branchFlavor),
                                False)])
            
    def testSimpleConfig(self):
        self.resetRepository()
        self.resetRoot()

        vList = [ None, None ]
        (built, d) = self.buildRecipe(recipes.simpleConfig1, 'SimpleConfig1')
        vList[0] = built[0][1]
        newLabel = versions.Label("localhost1@rpl:branch")

        repos1 = self.openRepository(1)

        self.mkbranch(self.cfg.buildLabel, newLabel, 'simpleconfig', 
                      binaryOnly=True)

        sourceVersion = versions.VersionFromString(vList[0]).getSourceVersion()
        (built, d) = self.buildRecipe(recipes.simpleConfig2, 'SimpleConfig2',
                                      serverIdx = 1, 
                                      sourceVersion = sourceVersion)
        vList[1] = built[0][1]

        self.updatePkg(self.rootDir, 'simpleconfig', vList[0])
        self.verifyFile(self.rootDir + '/etc/foo', 'text 1\n')
        self.updatePkg(self.rootDir, 'simpleconfig', vList[1])
        self.verifyFile(self.rootDir + '/etc/foo', 'text 2\n')

    def testDuplicateFileIds(self):
        # make sure that streams get filled in on branches when they were
        # originally None
        os.chdir(self.workDir)
        self.newpkg('branchedFileId')
        os.chdir('branchedFileId')
        self.writeFile('branchedFileId.recipe', recipes.branchedFileIdTest1)
        self.addfile('branchedFileId.recipe')
        self.commit()
        os.chdir("..")
        shutil.rmtree('branchedFileId')

        repos0 = self.openRepository(0)
        repos1 = self.openRepository(1)

        self.cookItem(repos0, self.cfg, 'branchedFileId')

        self.mkbranch("1.0-1-1", "localhost1@rpl:shadow", 
                      "branchedFileId", shadow = True)

        self.checkout("branchedFileId", "localhost1@rpl:shadow")
        os.chdir('branchedFileId')
        self.writeFile('branchedFileId.recipe', recipes.branchedFileIdTest2)
        self.commit()

        self.cfg.buildLabel = versions.Label("localhost1@rpl:shadow")
        self.cookItem(repos1, self.cfg, 'branchedFileId')

        self.updatePkg(self.cfg.root,
                       'branchedFileId', 'localhost1@rpl:shadow')

    def testDistributedUpdateWithUninstalledComponents(self):
        # this test verifies that when updating a package that was
        # installed from one repository to a version that resides on
        # a different repository, and that package has uninstalled
        # components on the system (a !byDefault component that isn't
        # installed, for example) updates properly.  The netclient wasn't
        # requesting pristine troves from the database when creating
        # the changeset in this situation.
        self.resetAllRepositories()
        self.resetWork()

        repos0 = self.openRepository(0)
        repos1 = self.openRepository(1)

        # removing the anonymous user makes sure no repository cross talk
        # is needed

        def _createCs(version):
            # create an absolute changeset
            flavor = deps.parseFlavor('')
            cs = changeset.ChangeSet()
            # add a pkg diff
            v = versions.VersionFromString(version, timeStamps=[1.000])
            old = trove.Trove('test', v, flavor, None)
            old.setIsCollection(True)
            old.addTrove('test:foo', v, flavor, byDefault=True)
            old.addTrove('test:bar', v, flavor, byDefault=False)
            old.computeDigests()

            # add the 'test' package
            diff = old.diff(None)[0]
            cs.newTrove(diff)
            cs.addPrimaryTrove('test', v, flavor)

            # add the test:foo component
            oldfoo = trove.Trove('test:foo', v, flavor, None)
            oldfoo.computeDigests()
            diff = oldfoo.diff(None)[0]
            cs.newTrove(diff)

            # add the test:bar component
            oldbar = trove.Trove('test:bar', v, flavor, None)
            oldbar.computeDigests()
            diff = oldbar.diff(None)[0]
            cs.newTrove(diff)

            return cs

        # create the first version on repos0
        cs = _createCs('/localhost@rpl:devel/1.0-1-1')
        repos0.commitChangeSet(cs)
        # install it
        self.updatePkg(self.cfg.root, 'test', 'localhost@rpl:devel')

        # create the second version on repos1
        cs = _createCs('/localhost@rpl:devel//localhost1@rpl:devel/1.0-1-1')
        repos1.commitChangeSet(cs)
        # update to it
        self.updatePkg(self.cfg.root, 'test', 'localhost1@rpl:devel')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTroveByName("test"))
        assert(db.hasTroveByName("test:foo"))
        assert(not db.hasTroveByName("test:bar"))

    def testDistributedGroup(self):
        # Build a group which includes a trove from a different repository,
        # then a new version of that group w/o that trove, and finally
        # create a changeset between those two versions. This ensures 
        # removed packages from a remote repository get passed back to the
        # client for removal.
        trv = self.build(recipes.testTransientRecipe1, 'TransientRecipe1')
        self.addQuickTestComponent("foo:runtime", "1.0-1-1")

        newLabel = versions.Label("localhost1@rpl:linux")
        oldLabel = self.cfg.buildLabel
        self.cfg.buildLabel = newLabel
        repos1 = self.openRepository(1)

        grpRecipe = """
class TestGroup(GroupRecipe):
            name = 'group-test'
            version = '1.0'
            clearBuildRequires()
            def setup(self):
                self.addTrove('testcase', 'localhost@rpl:linux')
                self.addTrove('foo:runtime', 'localhost@rpl:linux')
"""
        trv1 = self.build(grpRecipe, 'TestGroup')

        smallerGroupRecipe = """
class TestGroup(GroupRecipe):
            name = 'group-test'
            version = '1.0'
            clearBuildRequires()
            def setup(self):
                self.addTrove('foo:runtime', 'localhost@rpl:linux')
"""
        trv2 = self.build(smallerGroupRecipe, 'TestGroup')

        self.cfg.buildLabel = oldLabel

        cs = repos1.createChangeSet([('group-test', 
                        (trv1.getVersion(), trv1.getFlavor()),
                        (trv2.getVersion(), trv2.getFlavor()), False)])
        assert(['testcase'] == [ x[0] for x in cs.getOldTroveList()])

    def testFileAdded(self):
        self.openRepository(1)
        contents = [ ( 'first', 'first', '/localhost@rpl:linux/1.0-1-1') ]
        self.addQuickTestComponent('test:runtime', 
                 '/localhost@rpl:linux/1.0-1-1',
                 fileContents = contents)
        self.addQuickTestComponent('test:runtime', 
                 '/localhost@rpl:linux/1.0-1-1/localhost1@rpl:branch/1.5-1-1',
                 fileContents = contents)
        self.updatePkg(self.cfg.root, 'test:runtime', 'localhost1@rpl:branch')

        contents.append(('second', 'second', '/localhost@rpl:linux/2.0-1-1'))
        self.addQuickTestComponent('test:runtime', 
                 '/localhost@rpl:linux/2.0-1-1',
                 fileContents = contents)

        self.addQuickTestComponent('test:runtime', 
                 '/localhost@rpl:linux/1.0-1-1/localhost1@rpl:branch/3.0-1-1',
                 fileContents = contents)

        self.updatePkg(self.cfg.root, 'test:runtime', 'localhost1@rpl:branch')

    def testFileChanges(self):
        self.openRepository(1)
        linuxContents = [ ('/etc/first', 'first=a'),
                          ('/etc/secnd', 'secnd=a') ] 
        branchContents = [ ('/etc/first', 'first=b'),
                           ('/etc/secnd', 'secnd=b') ] 
        self.addQuickTestComponent('test:foo', '/localhost@rpl:linux/1.0-1-1',
                            fileContents = linuxContents)
        self.addQuickTestComponent('test:foo', 
                '/localhost@rpl:branch/1.0-1-1/localhost1@rpl:branch/2.0-1-1',
                            fileContents = branchContents)
        
        self.updatePkg(self.rootDir, 'test:foo', 
                       version = 'localhost@rpl:linux')
        self.updatePkg(self.rootDir, 'test:foo', 
                       version = 'localhost1@rpl:branch')

        for x in branchContents:
            self.verifyFile(self.rootDir + x[0], x[1])

    def testFileBecomesConfig(self):
        self.openRepository(1)
        self.addQuickTestComponent('test:runtime', '1.0-1-1',
                 fileContents = [ ( '/etc/foo', 'contents' ) ],
                 setConfigFlags = False)
        self.updatePkg(self.rootDir, "test:runtime")
        self.addQuickTestComponent('test:runtime', 
                                   '/localhost1@rpl:branch/2.0-1-1',
                 fileContents = [ ( '/etc/foo', 'contents' ) ],
                 setConfigFlags = True)
        self.updatePkg(self.rootDir, "test:runtime", 
                       version = '/localhost1@rpl:branch/2.0-1-1')

    def _testMirrorModeChangesets(self, singleRepos = True, protocol = None):
        # When mirroring, contents need to be included whenever a file version
        # changes because it could be a cross-repository change. mirrorMode
        # makes this happen CNY-1570.
        #
        # Similarly, mirrorMode doesn' allow file diffs across repositories,
        # since that would require crosstalk on commit. CNY-2210
        if singleRepos:
            repos = self.openRepository(serverName = [ 'localhost',
                                                       'localhost1' ])
        else:
            repos = self.openRepository(0)
            repos = self.openRepository(1)

        if protocol:
            repos.c['localhost'].setProtocolVersion(protocol)
            repos.c['localhost1'].setProtocolVersion(protocol)

        orig = self.addQuickTestComponent('test:runtime',
                 '/localhost@rpl:linux/1.0-1-1',
                 fileContents = [
                    ( '/bin/foo', rephelp.RegularFile(
                       version = '/localhost@rpl:linux/1.0-1-1',
                       contents = 'foo' ) ),
                    ('/usr/foo', rephelp.Directory(perms = 0755,
                       version = '/localhost@rpl:linux/1.0-1-1',
                    ) ),
                    ] )

        old = self.addQuickTestComponent('test:runtime',
                 '/localhost@rpl:linux//localhost1@rpl:linux/1.0-1-1',
                 fileContents = [
                    ( '/bin/foo', rephelp.RegularFile(
                       version = '/localhost@rpl:linux/1.0-1-1',
                       contents = 'foo' ) ),
                    ('/usr/foo', rephelp.Directory(perms = 0755,
                       version = '/localhost@rpl:linux/1.0-1-1',
                    ) ),
                    ] )

        new = self.addQuickTestComponent('test:runtime',
             '/localhost@rpl:linux//localhost1@rpl:linux/1.0-1.1-1',
             fileContents = [
                ( '/bin/foo', rephelp.RegularFile(
                   version = '/localhost@rpl:linux//localhost1@rpl:linux/1.0-1.1-1',
                   contents = 'foo' ) ),
                ('/usr/foo', rephelp.Directory(perms = 0755,
                   version = '/localhost@rpl:linux//localhost1@rpl:linux/1.0-1.1-1',
                ) ),
                ] )

        veryNew = self.addQuickTestComponent('test:runtime',
             '/localhost@rpl:linux//localhost1@rpl:linux/1.0-1.2-1',
             fileContents = [
                ( '/bin/foo', rephelp.RegularFile(
                   version = '/localhost@rpl:linux//localhost1@rpl:linux/1.0-1.1-1',
                   contents = 'foo' ) ),
                ('/usr/foo', rephelp.Directory(perms = 0755,
                   version = '/localhost@rpl:linux//localhost1@rpl:linux/1.0-1.1-1',
                ) ),
                ] )

        csPath = self.workDir + '/test.ccs'
        #repos.createChangeSetFile( [
            #( 'test:runtime', ( old.getVersion(), old.getFlavor() ),
                              #( new.getVersion(), new.getFlavor() ), False ),
            #], csPath)
        #fc = filecontainer.FileContainer(
                                #util.ExtendedFile(csPath, buffering = False))
        #fc.getNextFile()
        #assert(not fc.getNextFile())

        repos.createChangeSetFile( [
            ( 'test:runtime', ( old.getVersion(), old.getFlavor() ),
                              ( new.getVersion(), new.getFlavor() ), False ),
            ], csPath, mirrorMode = True)
        fc = filecontainer.FileContainer(
                                util.ExtendedFile(csPath, buffering = False))
        fc.getNextFile()
        assert(fc.getNextFile())

        repos.createChangeSetFile( [
            ( 'test:runtime', ( orig.getVersion(), orig.getFlavor() ),
                              ( new.getVersion(),  new.getFlavor() ), False ),
            ], csPath, mirrorMode = True)
        cs = changeset.ChangeSetFromFile(csPath)
        assert( [ x[0] != '\x01' for x in cs.files.values() ] == 
                    [ True, True ] )

    def testMirrorModeChangesets1(self):
        self._testMirrorModeChangesets(singleRepos = True)

    def testMirrorModeChangesets2(self):
        self.resetAllRepositories()
        self._testMirrorModeChangesets(singleRepos = False)

    def testMirrorModeChangesets3(self):
        self._testMirrorModeChangesets(singleRepos = False,
                                       protocol = 48)

    def setUp(self, *args):
        rephelp.RepositoryHelper.setUp(self, *args)
        self.cfg.threaded = False
