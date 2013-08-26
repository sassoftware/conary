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


from testrunner import testhelp
from testrunner.testhelp import context

import os
from conary_test import rephelp
from conary_test import recipes
import shutil

from conary import conaryclient, state
from conary.cmds import cvccmd
from conary.deps import deps
from conary import errors, conarycfg
from conary.versions import Label, VersionFromString as VFS
from conary.checkin import ConaryStateFromFile, CheckinCallback
from conary import repository
from sourcetest import StatWrapper
from conary.build import use
from conary.conaryclient import branch
from conary.lib import log


class ShadowTest(rephelp.RepositoryHelper):
    def _checkLatest(self, trove, verType, ver):
        repos = self.openRepository()
        if verType[0] == '/':
            verDict = repos.getTroveLeavesByBranch(
                                { trove : { VFS(verType) : None } } )
        else:
            verDict = repos.getTroveLeavesByLabel(
                                { trove : { Label(verType) : None } } )
        assert(len(verDict) == 1)
        assert(verDict[trove].keys()[0].asString() == ver)

    def _shadowPrefix(self, shadow):
        shadowHost, label = shadow.split('@')
        shadowNamespace, shadowName = label.split(':')
        # if the shadow is on the same repository, then just the shadow
        # name will show up in the version string
        if shadowHost == 'localhost':
            shadowPart = shadowName
        else:
            # otherwise use the whole shadow label
            shadowPart = shadow
        return '/localhost@rpl:linux//%s/' % shadowPart
                    
    def _testSourceShadow(self, shadow):
        self.resetWork()
        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")
        # the origional version, 1.0-1
        self.writeFile("testcase.recipe", recipes.testTransientRecipe1)
        self.addfile("testcase.recipe")
        self.commit()

        shadowPrefix = self._shadowPrefix(shadow)
        
        # create a shadow of 1.0-1
        self.mkbranch("1.0-1", shadow, "testcase:source", shadow = True)
        self._checkLatest('testcase:source', shadow, shadowPrefix + '1.0-1')


        rc = self.rdiff('testcase', '-1', shadowPrefix +'1.0-1')
        rdiffShadowOutput = '\n'.join((
            'New shadow:',
            '  %s1.0-1' %shadowPrefix,
            '  of',
            '  /localhost@rpl:linux/1.0-1',
            '',
        ))
        assert(rc == rdiffShadowOutput)

        # build directly off the shadowed sources, the build count should
        # be 0.1
        self.cfg.buildLabel = Label(shadow)
        built = self.cookFromRepository('testcase')
        assert(built[0][1] == shadowPrefix + '1.0-1-0.1')

        # check out the shadowed source and make a local change.  source
        # count should become 1.1
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase", shadow)
        os.chdir("testcase")
        f = open("testcase.recipe", "a")
        f.write("\n\n")
        del f
        self.commit()
        self._checkLatest('testcase:source', shadow, shadowPrefix + '1.0-1.1')
        # check build count, should start at 1
        built = self.cookFromRepository('testcase')
        assert(built[0][1] == shadowPrefix + '1.0-1.1-1')

        # test that changing the upstream version to 1.1 (from 1.0) results
        # in the correct source count (0.1)
        self.writeFile("testcase.recipe", recipes.testTransientRecipe2)
        self.commit()
        self._checkLatest('testcase:source', shadow, shadowPrefix + '1.1-0.1')
        # check build count, should start at 1
        built = self.cookFromRepository('testcase')
        assert(built[0][1] == shadowPrefix + '1.1-0.1-1')
        # build again, build count should be 2
        built = self.cookFromRepository('testcase')
        assert(built[0][1] == shadowPrefix + '1.1-0.1-2')

        # test that changing the upstream version back to 1.0 results in the
        # correct source count (1.2)
        self.writeFile("testcase.recipe", recipes.testTransientRecipe1)
        self.commit()
        self._checkLatest('testcase:source', shadow, shadowPrefix + '1.0-1.2')
        # and check that the build count becomes 1
        built = self.cookFromRepository('testcase')
        assert(built[0][1] == shadowPrefix + '1.0-1.2-1')

    def testSourceShadow(self):
        self.repos = self.openRepository()
        self._testSourceShadow('localhost@rpl:shadow')
        
    def testDistributedSourceShadow(self):
        self.openRepository(0)
        self.resetRepository(1)
        self.openRepository(1)
        self._testSourceShadow('localhost1@rpl:shadow')

    def _testMerge(self, shadow, switchUsers=False):
        def _assertExists(repos, ver):
            repos.findTrove(None, ('testcase:source', ver, None))

        def checkMetadata(ver, shortDesc):
            md = self.findAndGetTrove('testcase:source=%s' % ver).getMetadata()
            self.assertEquals(md['shortDesc'], shortDesc)

        def updateMetadata(ver, shortDesc):
            repos = self.openRepository()
            mi = self.createMetadataItem(shortDesc=shortDesc)
            trv = repos.findTrove(self.cfg.buildLabel, 
                                    ('testcase:source', ver, None))[0]
            repos.addMetadataItems([(trv, mi)])

        def verifyCONARY(ver, verMap, lastMerged = None):
            conaryState = ConaryStateFromFile("CONARY", None)
            sourceState = conaryState.getSourceState()
            assert(sourceState.getVersion().asString() == ver)

            if lastMerged:
                assert(sourceState.getLastMerged().asString() == lastMerged)
            else:
                assert(sourceState.getLastMerged() is None)

            for pathId, path, fileId, version in sourceState.iterFileList():
                foo = verMap.pop(path)
                assert(version.asString() == foo)

            # there should be no paths remaining in the vermap
            assert(not verMap)
        
        repos = self.openRepository()
        shadowPrefix = self._shadowPrefix(shadow)
        parentPrefix = '/%s/' %(self.cfg.buildLabel.asString())

        self.resetWork()
        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")
        # 1.0-1
        self.writeFile("testcase.recipe", recipes.testTransientRecipe1)
        self.writeFile('notChangedOnShadow', 'hello, world\n')
        self.writeFile('removedOnShadow', 'goodbye, world\n')
        self.writeFile('converging', '1\n2\n3\n4\n')
        self.addfile("testcase.recipe")
        self.addfile("notChangedOnShadow", text = True)
        self.addfile("removedOnShadow", text = True)
        self.addfile("converging", text = True)
        self.commit()
        updateMetadata('1.0', 'foo')

        self.mkbranch("1.0-1", shadow, "testcase:source", shadow = True)
        self._checkLatest('testcase:source', shadow, shadowPrefix + '1.0-1')

        # metadata should have transfered with the shadow
        checkMetadata(shadow, 'foo')

        # do shadow operations as the daemon user, so there is always
        if switchUsers:
            origstat = os.stat
            origlstat = os.lstat
            otheruser = StatWrapper(origstat, origlstat, 1, 1)

        # 1.0-1.1
        if switchUsers:
            os.stat = otheruser.stat
            os.lstat = otheruser.lstat
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase", shadow)
        os.chdir("testcase")
        f = open("testcase.recipe", "a")
        f.write("\n# extra comment\n")
        f.close()
        self.writeFile('addedOnShadow', 'hello, world\n')
        self.writeFile('converging', '1\n2\n3\n4\n5\n')
        self.addfile('addedOnShadow', text = True)
        self.remove('removedOnShadow')
        self.commit()

        # 1.1 (upstream change)
        if switchUsers:
            os.stat = origstat
            os.lstat = origlstat
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase")
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testTransientRecipe2)
        self.writeFile('notChangedOnShadow', 'hello 2\n')
        self.writeFile('removedOnShadow', 'changed\n')
        self.writeFile('converging', '1\n2\n3\n4\n5\n')
        self.commit()
        checkMetadata('1.1', 'foo')
        updateMetadata('1.1', 'upstream change')

        # 1.1-1.1 (merge)
        if switchUsers:
            os.stat = otheruser.stat
            os.lstat = otheruser.lstat
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase", shadow)
        os.chdir("testcase")
        self.merge()
        assert(not os.path.exists('removedOnShadow'))
        # make sure that all the versions are correct
        verifyCONARY(shadowPrefix + '1.0-1.1',
                     {'testcase.recipe': parentPrefix + '1.1-1',
                      'notChangedOnShadow': parentPrefix + '1.1-1',
                      'addedOnShadow': shadowPrefix + '1.0-1.1',
                      'converging': parentPrefix + '1.1-1' },
                     lastMerged = parentPrefix + '1.1-1')
        self.commit()
        # the merge doesn't override the metadata, metadata doesn't get
        # transferred via merge.
        checkMetadata(shadow, 'foo')
        # after the commit, the source count should be 1.1
        verifyCONARY(shadowPrefix + '1.1-1.1',
                     {'testcase.recipe': shadowPrefix + '1.1-1.1',
                      # XXX it would be nice if this was reset to be from
                      # the parent instead of a change on the shadow
                      'notChangedOnShadow': shadowPrefix + '1.1-1.1',
                      'addedOnShadow': shadowPrefix + '1.0-1.1',
                      'converging': parentPrefix + '1.1-1' })
        self._checkLatest('testcase:source', shadow, shadowPrefix + '1.1-1.1')

        # make sure the intermediate version appears shadowed
        assert(repos.hasTrove('testcase:source', VFS(shadowPrefix + '1.1-1'), deps.Flavor()))

        # check out the latest version on the shadow
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase", shadow)
        os.chdir('testcase')
        verifyCONARY(shadowPrefix + '1.1-1.1',
                     {'testcase.recipe': shadowPrefix + '1.1-1.1',
                      # XXX it would be nice if this was reset to be from
                      # the parent instead of a change on the shadow
                      'notChangedOnShadow': shadowPrefix + '1.1-1.1',
                      'addedOnShadow': shadowPrefix + '1.0-1.1',
                      'converging': parentPrefix + '1.1-1' })

        # change shadowed version to 1.1-1.2
        recipeText = open('testcase.recipe', 'r').read()
        newText = recipeText + '\n#comment bottom\n'
        self.writeFile("testcase.recipe", newText)
        self.commit()
        verifyCONARY(shadowPrefix + '1.1-1.2',
                     {'testcase.recipe': shadowPrefix + '1.1-1.2',
                      'notChangedOnShadow': shadowPrefix + '1.1-1.1',
                      'addedOnShadow': shadowPrefix + '1.0-1.1',
                      'converging': parentPrefix + '1.1-1' })


        # change shadowed version to 2.0-0.1
        newText = recipeText.replace("version = '1.1'", "version = '2.0'")

        self.writeFile("testcase.recipe", newText)
        self.commit()
        verifyCONARY(shadowPrefix + '2.0-0.1',
                     {'testcase.recipe': shadowPrefix + '2.0-0.1',
                      'notChangedOnShadow': shadowPrefix + '1.1-1.1',
                      'addedOnShadow': shadowPrefix + '1.0-1.1',
                      'converging': parentPrefix + '1.1-1' })

        # make two minor revisions upstream to get 1.1-3
        if switchUsers:
            os.stat = origstat
            os.lstat = origlstat
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase")
        os.chdir("testcase")

        newText = open('testcase.recipe', 'r').read() + '\n#minor comment\n'
        self.writeFile("testcase.recipe", newText)
        self.commit()

        newText = '\n#minor comment \n' + newText
        self.writeFile("testcase.recipe", newText)
        self.commit()

        # merge to get 2.0-0.2 
        if switchUsers:
            os.stat = otheruser.stat
            os.lstat = otheruser.lstat
        os.chdir("..")
        shutil.rmtree("testcase")
        self.checkout("testcase", shadow)

        os.chdir("testcase")
        verifyCONARY(shadowPrefix + '2.0-0.1',
                     {'testcase.recipe': shadowPrefix + '2.0-0.1',
                      'notChangedOnShadow': shadowPrefix + '1.1-1.1',
                      'addedOnShadow': shadowPrefix + '1.0-1.1',
                      'converging': parentPrefix + '1.1-1' },
                     lastMerged = None)

        self.merge()
        assert(not os.path.exists('removedOnShadow'))
        # make sure that all the versions are correct
        verifyCONARY(shadowPrefix + '2.0-0.1',
                     {'testcase.recipe': parentPrefix + '1.1-3',
                      'notChangedOnShadow': shadowPrefix + '1.1-1.1',
                      'addedOnShadow': shadowPrefix + '1.0-1.1',
                      'converging': parentPrefix + '1.1-1' },
                     lastMerged = parentPrefix + '1.1-3')
        self.commit()
        # after the commit, the source count should be 2.0-0.2
        verifyCONARY(shadowPrefix + '2.0-0.2',
                     {'testcase.recipe': shadowPrefix + '2.0-0.2',
                      'notChangedOnShadow': shadowPrefix + '1.1-1.1',
                      'addedOnShadow': shadowPrefix + '1.0-1.1',
                      'converging': parentPrefix + '1.1-1' })
        self._checkLatest('testcase:source', shadow, shadowPrefix + '2.0-0.2')
        assert(not repos.hasTrove('testcase:source', VFS(shadowPrefix + '1.1-2'), deps.Flavor()))
        assert(repos.hasTrove('testcase:source', VFS(shadowPrefix + '1.1-3'), deps.Flavor()))

        if switchUsers:
            os.stat = origstat
            os.lstat = origlstat

    def testMerge(self):
        self.openRepository()
        self._testMerge('localhost@rpl:shadow', False)

    def testMergeDifferentUser(self):
        self.openRepository()
        self._testMerge('localhost@rpl:shadow', True)

    def testDistributedMerge(self):
        self.openRepository()
        self.resetRepository(1)
        self.openRepository(1)
        self._testMerge('localhost1@rpl:shadow', False)

    def testDistributedMergeDifferentUser(self):
        self.openRepository()
        self.resetRepository(1)
        self.openRepository(1)
        self._testMerge('localhost1@rpl:shadow', True)

    def testMergeAndDiffAutoSource(self):
        os.chdir(self.workDir)
        self.newpkg('autosource')
        os.chdir('autosource')
        self.writeFile("autosource.recipe", recipes.autoSource1)
        self.writeFile('localfile', 'test contents\n')
        self.addfile("autosource.recipe")
        self.addfile("localfile", text = True)
        self.commit()

        shadow = 'localhost@rpl:shadow'
        shadowPrefix = self._shadowPrefix(shadow)
        # create a shadow of 1.0-1
        self.mkbranch("1.0-1", shadow, "autosource:source", shadow = True)

        # make changes on the parent branch
        self.writeFile("autosource.recipe", recipes.autoSource2)
        open("localfile", "a").write("new contents\n")
        self.commit()

        # merge and diff calls buildLocalChanges
        os.chdir('..')
        self.checkout('autosource=%s' % shadow, dir='autosource-shadow')
        os.chdir('autosource-shadow')
        self.merge()
        self.diff()

    def testUnneededMerge(self):
        # CNY-968
        os.chdir(self.workDir)

        self.newpkg("testcase")
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testTransientRecipe1)
        self.addfile("testcase.recipe")
        self.writeFile("source", contents = "middle\n")
        self.addfile("source", text = True)
        self.commit()

        # create a shadow of 1.0-1
        shadow = 'localhost@test:shadow'
        self.mkbranch("1.0-1", shadow, "testcase:source", shadow = True)

        self.update(shadow)
        self.writeFile("source", contents = "middle\nlast\n")
        self.commit()

        # nothing to merge
        self.logCheck(self.merge, [],
                      'error: No changes have been made on the parent branch; '
                      'nothing to merge.')
        self.logCheck(self.commit, [],
                      '+ no changes have been made to commit')

    def testMergeRename(self):
        # CNY-967
        os.chdir(self.workDir)

        self.newpkg("testcase")
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testTransientRecipe1)
        self.addfile("testcase.recipe")
        self.writeFile("firstname", contents = "first\n")
        self.addfile("firstname", text = True)
        self.commit()

        # create a shadow of 1.0-1
        shadow = 'localhost@test:shadow'
        self.mkbranch("1.0-1", shadow, "testcase:source", shadow = True)

        self.rename("firstname", "secondname")
        self.writeFile("secondname", contents = "second\n")
        self.commit()

        self.update(shadow)
        self.verifyFile("firstname", "first\n")
        assert(not os.path.exists("secondname"))
        self.merge()
        self.verifyFile("secondname", "second\n")
        assert(not os.path.exists("firstname"))

    def testMergeDuplicateAddAutosource(self):
        # CNY-1856
        os.chdir(self.workDir)

        self.newpkg('autosource')

        os.chdir('autosource')
        self.writeFile('autosource.recipe', recipes.autoSource0)
        self.addfile('autosource.recipe')
        self.writeFile('localfile', contents='blah\n')
        self.addfile('localfile', text=True)
        self.commit()

        # create a shadow of 1.0-1
        shadow = 'localhost@test:shadow'
        self.mkbranch('1.0-1', shadow, 'autosource:source', shadow = True)

        # add an auto-source file to the original branch
        self.writeFile('autosource.recipe', recipes.autoSource5)
        self.commit()

        # update to the shadow version
        os.chdir("..")
        self.checkout('autosource=%s' % shadow, dir='autosource-shadow')
        # self.update(str(shadow))
        os.chdir('autosource-shadow')
        # add the same file, same contents
        self.writeFile('autosource.recipe', recipes.autoSource5)
        self.commit()

        # now merge changes from parent
        self.merge()
        self.commit()

    def testShadowBinaryGroup(self):

        basicSplitGroup = """
class splitGroup(GroupRecipe):
    name = 'group-first'
    version = '1.0'
    checkPathConflicts = False
    clearBuildRequires()
    def setup(self):
        self.addTrove("testcase", ":linux", byDefault=False)
        self.createGroup('group-second')
        self.addTrove("testcase", ":test1",
                      groupName = 'group-second')
        # add group-second to group-first
        self.addNewGroup('group-second')
"""
        repos = self.openRepository()

        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        origBuildLabel = self.cfg.buildLabel
        self.cfg.buildLabel = Label('localhost@rpl:test1')
        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        self.cfg.buildLabel = origBuildLabel
        (built, d) = self.buildRecipe(basicSplitGroup, "splitGroup")
        n,v,f = [ x for x in built if x[0] == 'group-first' ][0]
        v = VFS(v)
        shadowVerStr = '/localhost@rpl:linux//shadow/1.0-1-1'
        shadowVer = VFS(shadowVerStr)
        flavor = use.Arch.getCurrentArch()._toDependency()
        group = repos.getTrove(n,v,f)
        self.verifyTroves(group, [('testcase', 
                                   '/localhost@rpl:linux/1.0-1-1',
                                   flavor),
                                  ('group-second', 
                                   '/localhost@rpl:linux/1.0-1-1',
                                   flavor)])
        self.mkbranch("1.0-1-1", 'localhost@rpl:shadow', "group-first", 
                      shadow=True, binaryOnly=True)
        group = repos.getTrove('group-first', shadowVer, flavor)
        assert(not group.includeTroveByDefault('testcase', shadowVer, flavor))
        self.verifyTroves(group, [('testcase', 
                                   shadowVerStr,
                                    flavor),
                                  ('group-second', 
                                   shadowVerStr,
                                   flavor)])
        group = repos.getTrove('group-second', 
                               shadowVer,
                               flavor)
        self.verifyTroves(group, [('testcase', 
                                   '/localhost@rpl:test1//shadow/1.0-1-1',
                                   flavor)])


    def testReShadowBinaryGroupWithVersions(self):
        basicGroup = """
class TestGroup(GroupRecipe):
    name = 'group-test'
    version = '2.0'
    clearBuildRequires()
    def setup(self):
        self.addTrove("testcase", "1.0")
"""
        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        (built, d) = self.buildRecipe(basicGroup, "TestGroup")
        self.mkbranch("2.0-1-1", 'localhost@rpl:shadow', "group-test", 
                      shadow=True, binaryOnly=True)
        self.logFilter.add()
        self.mkbranch("2.0-1-1", 'localhost@rpl:shadow', "group-test", 
                      shadow=True, binaryOnly=True)
        self.logFilter.remove()
        self.logFilter.compare(['warning: group-test already has branch /localhost@rpl:linux//shadow',
        'warning: testcase already has branch /localhost@rpl:linux//shadow',
        'warning: testcase:runtime already has branch /localhost@rpl:linux//shadow'])

    def testShadowOpts(self):
        self.repos = self.openRepository()
        cfg = self.cfg

        self.addTestPkg(1)
        self.cookTestPkg(1)
        self.addTestPkg(2)
        self.cookTestPkg(2)
        oldLabel = cfg.installLabelPath
        shadowLabel = Label('localhost@rpl:shadow')
        try:
            self.mkbranch(["test1", "test2:source"], 'localhost@rpl:shadow', 
                           shadow=True, sourceOnly=True)
            cfg.installLabelPath = conarycfg.CfgLabelList([shadowLabel])
            query = [('test1:source', None, None), ('test1', None, None),
                     ('test2:source', None, None), ('test2', None, None)]
            results = self.repos.findTroves(cfg.installLabelPath, query,
                                            allowMissing=True)
            assert(('test1:source', None, None) in results)
            assert(('test2:source', None, None) in results)
            assert(('test1', None, None) not in results)

            self.mkbranch(['test1:source', 'test2'],
                           'localhost@rpl:shadow', 
                           shadow=True, binaryOnly=True)

            results = self.repos.findTroves(cfg.installLabelPath, query,
                                             allowMissing=True)
            assert(('test1:source', None, None) in results)
            assert(('test2:source', None, None) in results)
            assert(('test2', None, None) in results)
            assert(('test1', None, None) not in results)

            # now shadow both binary and source
            shadowLabel2 = Label('localhost@rpl:shadow2')
            self.mkbranch("1.0-1-1", 'localhost@rpl:shadow2', "test1", 
                           shadow=True)

            cfg.installLabelPath = conarycfg.CfgLabelList([shadowLabel2])

            results = self.repos.findTroves(cfg.installLabelPath, query,
                                             allowMissing=True)
            assert(('test1:source', None, None) in results)
            assert(('test1', None, None) in results)
        finally:
            cfg.installLabelPath = oldLabel

    def testMerge2(self):
        # a file is added on both the source and the shadow, then a merge
        # is attempted
        # create the original and the shadow
        shadowLabel = Label('localhost@rpl:branch')
        self.makeSourceTrove('test', simpleRecipe)
        self.mkbranch(['test:source'], shadowLabel, shadow = True)

        # update the original with the new file 'extra'
        os.chdir(self.workDir)
        self.checkout('test')
        self.checkout('test', str(shadowLabel), dir='test-shadow')
        os.chdir('test')
        self.writeFile('extra', 'Contents1\nContents2\n')
        self.addfile('extra', text = True)
        self.commit()

        # update the shadow with a conflicting version of the new file 'extra'
        # (but by adding it, not by merging)
        os.chdir('../test-shadow')
        self.writeFile('extra', 'Contents1\nContents3\n')
        self.addfile('extra', text = True)
        self.commit()

        # Now try to merge, there's a file conflict
        self.logCheck(self.merge, [],
                'error: path extra added both locally and in repository')

        self.writeFile('extra', 'Contents1\nContents2\n')
        self.commit()

        self.logCheck(self.merge, [],
                'error: path extra added both locally and in repository')

    def testMergeNonShadow(self):
        self.makeSourceTrove('test', simpleRecipe)
        os.chdir(self.workDir)
        self.checkout('test')
        os.chdir('test')
        self.logCheck(self.merge, [], 'error: test:source=/localhost@rpl:linux is not a shadow')

    def testMergeNotAtTip(self):
        self.addComponent('test:source', '/localhost@rpl:linux/1.0-1')
        self.addComponent('test:source', '/localhost@rpl:linux//shadow/1.0-1')
        self.addComponent('test:source', '/localhost@rpl:linux//shadow/1.0-1.1')
        os.chdir(self.workDir)
        self.checkout('test=localhost@rpl:shadow/1.0-1')
        os.chdir('test')
        self.logCheck2([ '+ working directory is not the latest on label '
                         'localhost@rpl:shadow'  ], self.merge,
                         verbosity = log.INFO)

    def testMergeWithRevision(self):
        recipe1 = simpleRecipe
        recipe2 = (simpleRecipe + '\n\t#extra line\n').replace('1.0', '2.0')
        recipe3 = (simpleRecipe + '\n\t#extra line\n\t#extra line 2\n').replace('1.0', '3.0')

        self.addComponent('test:source', '1.0-1', '',
                          [('test.recipe', recipe1)])
        self.addComponent('test:source', '2.0-1', '',
                          [('test.recipe', recipe2 )])
        self.addComponent('test:source', '3.0-1', '',
                          [('test.recipe', recipe3)])
        self.mkbranch(['test:source=1.0'], 'localhost@rpl:shadow', shadow=True)
        os.chdir(self.workDir)
        self.cfg.buildLabel = Label('localhost@rpl:shadow')
        self.checkout('test')
        os.chdir('test')
        self.merge('2.0')
        self.verifyFile('test.recipe', recipe2)
        self.commit()
        self.merge('3.0-1')
        self.verifyFile('test.recipe', recipe3)
        self.commit()
        self.logFilter.add()
        self.merge('2.0') # raise an error
        self.logFilter.compare(['error: Cannot merge: version specified is before the last merge point, would be merging backwards'])
        self.logFilter.add()
        self.merge('localhost@rpl:linux') # raise an error
        self.logFilter.compare(['error: Can only specify upstream version, upstream version + source count or full versions to merge'])

    def testShadowComponent(self):
        self.makeSourceTrove('test', simpleRecipe)
        self.cookFromRepository('test')
        try:
            self.mkbranch(['test:runtime'], 'localhost@rpl:shadow', shadow=True)
        except errors.ParseError, err:
            assert(str(err) == 'Cannot branch or shadow individual components:'
                               ' test:runtime')

    def testThreeLevelShadowWithMerge(self):
        self.addComponent('test:source', '/localhost@rpl:1/1.0-1',
                          fileContents = [ ("sourcefile", "source 1.0\n") ] )
        self.addComponent('test:source', '/localhost@rpl:1/2.0-1',
                          fileContents = [ ("sourcefile", "source 2.0\n") ] )
        self.mkbranch(['test:source=/localhost@rpl:1/2.0-1'],
                      'localhost@rpl:shadow1', shadow=True)
        self.addComponent('test:source',
                          '/localhost@rpl:1//shadow1/2.0-1.1',
                          fileContents = [ ("sourcefile", "source 2.0\n") ] )
        self.mkbranch(['test:source=/localhost@rpl:1//shadow1/2.0-1'],
                      'localhost@rpl:shadow2', shadow=True)
        self.addComponent('test:source',
                          '/localhost@rpl:1//shadow1//shadow2/2.0-1.0.1',
                          fileContents = [ ("sourcefile", "source 2.0\n") ] )
        os.chdir(self.workDir)
        self.checkout('test=localhost@rpl:shadow2')
        os.chdir('test')
        self.merge()

    def testShadowBackwards(self):
        # Shadowing an earlier trove to a later point on the child
        # causes havoc with our merge algorithms.
        self.addComponent('test:source', '1.0-1')
        self.addComponent('test:source', '2.0-1')
        self.mkbranch(['test:source=2.0'], 'localhost@rpl:shadow', shadow=True)
        try:
            self.mkbranch(['test:source=1.0'], 'localhost@rpl:shadow', 
                          shadow=True)
        except branch.BranchError, err:
            assert(str(err) == '''\
Cannot shadow backwards - already shadowed
    test:source=/localhost@rpl:linux/2.0-1[]
cannot shadow earlier trove
    test:source=/localhost@rpl:linux/1.0-1[]
''')

    def testMissingFiles(self):
        self.openRepository(1)
        self.addComponent('test:source', '1.0-1')
        self.mkbranch(['test:source=1.0'], 'localhost1@rpl:shadow', shadow=True)
        self.stopRepository(0)
        os.chdir(self.workDir)
        raise testhelp.SkipTestException('CNY-462 Temporarily disabling until we figure out how to pass the OpenError exception')
        self.assertRaises(repository.errors.OpenError, self.checkout,
                          "test", 'localhost1@rpl:shadow')

        # this open resets repository 0, which means that files from the
        # shadow are missing
        self.openRepository(0)
        try:
            self.checkout("test", 'localhost1@rpl:shadow')
        except repository.errors.FileStreamMissing, e:
            j = """File Stream Missing
    The following file stream was not found on the server:
    fileId: 1602c79ea7aeb2cc64c6f11b45bc1be141f610d2
    This could be due to an incomplete mirror, insufficient permissions,
    or the troves using this filestream having been removed from the server."""
            assert(str(e) == j)

    def testMergeDuplicateAdd(self):
        # CNY-1021
        # test cvc merge's handling of a file added locally and in 
        # repository
        self.addComponent('test:source', '1.0-1',  ['bar'])
        self.mkbranch(['test:source=1.0'], 'localhost@rpl:shadow', shadow=True)
        os.chdir(self.workDir)
        self.checkout('test=localhost@rpl:shadow')
        os.chdir('test')
        self.writeFile('foo', 'bar!\n')
        self.addfile('foo', text=True)
        self.addComponent('test:source', '2.0-1',  ['bar', 'foo'])
        self.logCheck2('error: path foo added both locally and in repository',
                       self.merge)


    @context('shadow')
    @context('labelmultiplicity')
    def testShadowCreatesLabelMultiplicity(self):
        shadow = 'localhost@rpl:shadow'
        self.addComponent('foo:source', '/localhost@rpl:linux/1-1')
        self.addComponent('foo:source', '/%s/1-1' % shadow)
        self.logFilter.add()
        rc, txt = self.captureOutput(self.mkbranch, "1-1", shadow,
                                    "foo:source", shadow = True,
                                    ignoreConflicts = False)
        assert(not txt)

    def testShadowMetadata(self):
        self.addComponent('foo:source=1-1',
                            metadata=self.createMetadataItem(shortDesc='foo'))
        rc, txt = self.captureOutput(self.mkbranch, "1-1", 
                                    'localhost@rpl:shadow',
                                    "foo:source", shadow = True)
        metadata = self.findAndGetTrove('foo:source=:shadow').getMetadata()
        assert(metadata['shortDesc'] == 'foo')

    @context('shadow')
    @context('labelmultiplicity')
    def testShadowSourceDisappears(self):
        # CNY-462
        class CustomError(Exception):
            errorIsUncatchable = True

        class MFCheckinCallbackFailure(CheckinCallback):
            def missingFiles(self, files):
                raise CustomError(files)

        class MFCheckinCallbackFailure2(CheckinCallback):
            def missingFiles(self, files):
                return False

        class MFCheckinCallbackSuccess(CheckinCallback):
            def missingFiles(self, files):
                return True

        shadow = 'localhost1@rpl:shadow'
        shadowPrefix = self._shadowPrefix(shadow)

        self.openRepository(0)
        self.resetRepository(1)
        repos1 = self.openRepository(1)

        self.resetWork()
        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")
        # the origional version, 1.0-1
        self.writeFile("testcase.recipe", recipes.testTransientRecipe1)
        self.addfile("testcase.recipe")
        self.commit()

        # create a shadow of 1.0-1
        self.mkbranch("1.0-1", shadow, "testcase:source", shadow = True)
        self._checkLatest('testcase:source', shadow, shadowPrefix + '1.0-1')

        self.resetWork()
        os.chdir(self.workDir)
        # Source goes away
        self.stopRepository(0)

        # This should be in a different test case: repository is not even
        # available
        self.openRepository(0)

        callbackF = MFCheckinCallbackFailure()
        callbackF2 = MFCheckinCallbackFailure2()
        callbackS = MFCheckinCallbackSuccess()

        os.chdir(self.workDir)
        # This should fail period. (no callback)
        self.assertRaises(repository.errors.FileStreamMissing,
                              self.checkout, "testcase",
                              versionStr=shadow)
        # This call will fail because the callback throws an exception
        self.assertRaises(CustomError, self.checkout, "testcase",
                              versionStr=shadow, callback=callbackF)
        # This call will fail because the callback returns False
        self.assertRaises(repository.errors.FileStreamMissing,
                              self.checkout, "testcase",
                              versionStr=shadow, callback=callbackF2)

        self.stopRepository(0)
        # This should fail period. (no callback)
        self.assertRaises(repository.errors.OpenError,
                              self.checkout, "testcase",
                              versionStr=shadow)

        # In passing, test CNY-1415 (missing files while writing a changeset to
        # a file)
        jobList = [
            ('testcase:source',
                (None, None),
                (VFS(shadowPrefix + '1.0-1'), deps.parseFlavor('')),
                True),
        ]

        csfile = os.path.join(self.workDir, "changeset-file.ccs")
        # fix up the proxy cfg
        repos1 = self.openRepository(1)
        repos1.createChangeSetFile(jobList, csfile, callback=callbackS)
        cs = repository.changeset.ChangeSetFromFile(csfile)
        self.assertEqual([ 'testcase:source' ],
                             [ t.getName() for t in cs.iterNewTroveList()])

        # This call succeeds
        self.checkout("testcase", versionStr=shadow, callback=callbackS)

        # Test the callback
        shutil.rmtree('testcase')

        c = cvccmd.CvcMain()
        cmd = c._supportedCommands['checkout']

        expOutput = """\
Warning: The following files are missing:
testcase.recipe
"""

        (ret, strng) = self.captureOutput(
            c.runCommand, cmd, self.cfg, {}, 
            ['cvc', 'checkout', 'testcase=' + shadow])
        self.assertEqual(strng, expOutput)

        os.chdir("testcase")

        dest = "localhost2@rpl:shadow"
        open("testcase.recipe", "w+").write(simpleRedirectRecipe % dest)
        self.addfile("testcase.recipe")
        self.commit(callback=callbackS)

        os.chdir(self.workDir)
        shutil.rmtree('testcase')

    def testMergeWithConflicts(self):
        # CNY-1278
        common = "line1\nline2\nline3\nline4\nline5\nline6\nline7\n"
        orig =                  common + "ORIG BOTTOM\n"
        newOnParent = "TOP\n" + common + "PARENT BOTTOM\n"
        newOnShadow =           common + "SHADOW BOTTOM\n"

        for version in [ '/localhost@rpl:linux/1.0-1',
                         '/localhost@rpl:linux//shadow/1.0-1' ]:
            self.addComponent('test:source', version,
                              fileContents =
            [ ('test.source', simpleRecipe),
              ('other', rephelp.RegularFile(contents = orig,
                                version = '/localhost@rpl:linux/1.0-1',
                                config = True) ) ] )

        self.addComponent('test:source', '/localhost@rpl:linux/1.0-2',
                          fileContents = [ ('test.source', simpleRecipe),
                                           ('other', newOnParent) ] )

        self.addComponent('test:source',
                          '/localhost@rpl:linux//shadow/1.0-1.1',
                          fileContents = [ ('test.source', simpleRecipe),
                                           ('other', newOnShadow) ] )

        os.chdir(self.workDir)
        self.checkout("test", 'localhost@rpl:shadow')
        os.chdir("test")

        self.logCheck(self.merge, [],
                      'warning: conflicts from merging changes from head '
                      'into %s/test/other saved as %s/test/other.conflicts'
                      % (self.workDir, self.workDir))

        self.logCheck(self.merge, [],
                      'error: outstanding merge must be committed before '
                      'merging again')

        self.verifyFile(self.workDir + '/test/other',
                        "TOP\n" + common + "SHADOW BOTTOM\n")

    def testCircularShadow(self):
        # CNY-847
        repos = self.openRepository()

        branch1 = "/localhost@rpl:1"
        branch2 = "/localhost@rpl:1//2"
        branch3 = "/localhost@rpl:1//2//3"

        shadow1 = "localhost@rpl:1"
        shadow2 = "localhost@rpl:2"
        shadow3 = "localhost@rpl:3"

        ver1 = branch1 + '/1.0-1'
        self.addComponent("foo:source", ver1)
        self.addComponent("foo:data", ver1 + '-1')
        self.addCollection('foo', ver1 + '-1', ['foo:data'])

        trvspec = "foo=%s/1.0-1-1" % branch1
        self.mkbranch([ trvspec ], shadow2, shadow=True)

        # Try to shadow back to @rpl:1
        self.assertRaises(errors.VersionStringError,
            self.mkbranch, [ trvspec ], shadow1, shadow=True)

        # Create a deeper shadow hierarchy

        trvspec = "foo=%s/1.0-1-1" % branch2
        self.mkbranch([ trvspec ], shadow3, shadow=True)

        # Shadow in the middle of the hierarchy
        # (from @rpl:1//2//3 to @rpl:1//2)
        trvspec = "foo=%s/1.0-1-1" % branch3
        self.assertRaises(errors.VersionStringError,
            self.mkbranch, [ trvspec ], shadow2, shadow=True)

        # Shadow to the top parent
        # (from @rpl:1//2//3 to @rpl:1)
        self.assertRaises(errors.VersionStringError,
            self.mkbranch, [ trvspec ], shadow1, shadow=True)

    def testCrossMerge(self):
        self.addComponent('test:source', '/localhost@rpl:r1//a/1.0-1',
                      fileContents = [ ('test.recipe', simpleRecipe) ] )
        self.addComponent('test:source', '/localhost@rpl:r2//a/2.0-1',
                      fileContents = [ ('test.recipe', simpleRecipe2) ] )

        self.addComponent('test:source', '/localhost@rpl:r1//a//b/1.0-1',
                      fileContents = [ ('test.recipe', simpleRecipe) ] )

        os.chdir(self.workDir)
        self.checkout('test=localhost@rpl:b')
        os.chdir('test')
        self.logCheck2(
            [ '+ Merging from /localhost@rpl:r1//a onto new shadow '
                    '/localhost@rpl:r2//a',
              '+ patching %s/test/test.recipe' % self.workDir,
              '+ patch: applying hunk 1 of 1' ], self.merge,
              verbosity = log.INFO)

        self.verifyFile('test.recipe', simpleRecipe2)
        self.commit()
        trvState = state.ConaryStateFromFile('CONARY')
        assert(str(trvState.getSourceState().getVersion()) ==
                    '/localhost@rpl:r2//a//b/2.0-1.0.1')

        self.addComponent('test:source', '/localhost@rpl:r1//a/3.0-1',
                      fileContents = [ ('test.recipe', simpleRecipe3) ] )
        self.logCheck2(
            [ '+ Merging from /localhost@rpl:r2//a onto /localhost@rpl:r1//a',
              '+ patching %s/test/test.recipe' % self.workDir,
              '+ patch: applying hunk 1 of 1' ], self.merge,
              verbosity = log.INFO)

        self.verifyFile('test.recipe', simpleRecipe3)
        self.commit()
        trvState = state.ConaryStateFromFile('CONARY')
        assert(str(trvState.getSourceState().getVersion()) ==
                    '/localhost@rpl:r1//a//b/3.0-1.0.1')

    def testPathIdLookupPermissions(self):
        # CNY-1911
        label2 = Label('localhost2@rpl:devel')
        label1 = Label('localhost1@rpl:devel')
        shadowLabel = Label('localhost@rpl:shadow')

        self.openRepository(0)
        self.openRepository(1)
        # Remove anonymous user
        repos = self.openRepository(2)
        repos.deleteUserByName(label2.asString(), "anonymous")

        # Add a file that disappears
        recipe1 = simpleRecipe + "        r.Create('/usr/blip', contents='abc\\n')\n"
        self.makeSourceTrove('test', recipe1, buildLabel = label2)
        built = self.cookFromRepository('test', buildLabel = label2)

        # Extra file goes away
        self.updateSourceTrove('test', simpleRecipe,
                               versionStr = label2.asString())
        built = self.cookFromRepository('test', buildLabel = label2)

        self.assertEqual(built[0][1], '/localhost2@rpl:devel/1.0-2-1')

        # Now shadow
        self.mkbranch(['test:source=' + label2.asString()], label1, shadow=True)

        # Noop change
        newRecipe = simpleRecipe.replace("mode=0755",
                                         "contents='foobar\\n', mode=0755")
        self.updateSourceTrove('test', newRecipe, versionStr = label1.asString())
        # And build in the repo
        built = self.cookFromRepository('test', buildLabel = label1)
        self.assertEqual(built[0][1], '/localhost2@rpl:devel//localhost1@rpl:devel/1.0-2.1-1')

        # Now shadow again
        self.mkbranch(['test:source=' + label1.asString()], shadowLabel, shadow=True)

        # Add the original file back
        self.updateSourceTrove('test', recipe1,
                               versionStr = shadowLabel.asString())

        # Reset users, client-side
        self.cfg.user.addServerGlob('localhost2', ('test', 'wrongpass'))

        client = conaryclient.ConaryClient(self.cfg)
        repos = client.getRepos()

        # And build in the repo
        built = self.cookFromRepository('test', buildLabel = shadowLabel,
                                        repos = repos)
        self.assertEqual(built[0][1], '/localhost2@rpl:devel//localhost1@rpl:devel//localhost@rpl:shadow/1.0-2.1.1-1')

        trvList = repos.getTroves([ (x[0], VFS(x[1]), x[2])
                                  for x in built ])
        trv = trvList[0]
        # Iterate over all files
        for _, _, _, vr in trv.iterFileList():
            # Make sure the file version is the same as the trove version. If
            # the originating repo didn't reject the request (and the client
            # didn't ignore the reject), then we'd see the file version be on
            # the /localhost2 branch.
            self.assertEqual(vr.asString(), built[0][1])

    def testPathIdLookupShortcut(self):
        # CNY-1911
        self.openRepository(1)
        self.openRepository(2)

        label2 = Label('localhost2@rpl:devel')
        label1 = Label('localhost1@rpl:devel')
        shadowLabel = Label('localhost@rpl:shadow')
        self.makeSourceTrove('test', simpleRecipe, buildLabel = label2)
        built = self.cookFromRepository('test', buildLabel = label2)
        self.assertEqual(built[0][1], '/localhost2@rpl:devel/1.0-1-1')

        # Now shadow
        self.mkbranch(['test:source=' + label2.asString()], label1, shadow=True)

        # Noop change
        newRecipe = simpleRecipe.replace("mode=0755",
                                         "contents='foobar\\n', mode=0755")
        self.updateSourceTrove('test', newRecipe, versionStr = label1.asString())
        # And build in the repo
        built = self.cookFromRepository('test', buildLabel = label1)
        self.assertEqual(built[0][1], '/localhost2@rpl:devel//localhost1@rpl:devel/1.0-1.1-1')

        # Now shadow again
        self.mkbranch(['test:source=' + label1.asString()], shadowLabel, shadow=True)
        # And build in the repo
        built = self.cookFromRepository('test', buildLabel = shadowLabel)

        # Stop the ancestor repo
        self.stopRepository(2)

        # We should still be able to cook
        built = self.cookFromRepository('test', buildLabel = shadowLabel)


simpleRecipe = '''
class SimpleRecipe(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        r.Create('/usr/bin/foo', mode=0755)
'''

simpleRecipe2 = '''
class SimpleRecipe(PackageRecipe):
    name = 'test'
    version = '2.0'
    clearBuildReqs()
    def setup(r):
        r.Create('/usr/bin/foo', mode=0755)
        r.Create('/usr/bin/bar', mode=0755)
'''

simpleRecipe3 = '''
class SimpleRecipe(PackageRecipe):
    name = 'test'
    version = '3.0'
    clearBuildReqs()
    def setup(r):
        r.Create('/usr/bin/foo', mode=0755)
        r.Create('/usr/bin/bar', mode=0755)
        r.Create('/usr/bin/baz', mode=0755)
'''

simpleRedirectRecipe = r"""\
class SimpleRedirectRecipe(RedirectRecipe):
    name = 'testcase'
    version = '0'
    def setup(r):
        r.addRedirect('testcase', '%s')
"""
