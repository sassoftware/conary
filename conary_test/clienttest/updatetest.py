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
import signal
import os

#testsuite
from conary_test import recipes
from conary_test import rephelp

from testrunner.testhelp import context

#conary
from conary.conarycfg import RegularExpressionList
from conary.deps import deps
from conary import conaryclient, sqlite3
from conary.conaryclient import cmdline, update
from conary.lib import util
from conary.local import database
from conary import callbacks, errors, versions
from conary.cmds import query
from conary.repository import changeset
"""
This code tests several areas of conaryclient.update.

Currently it's divided into the following sections:
 1. Branch affinity tests
 3. Erasure tests
 4. Uninstalled/installed Reference tests
 5. General _mergeCollections tests
 6. Diff trove tests
 7. Synctrove tests
 8. Pin tests
 9. Relative update tests
10. Local update tests
11. updateAll tests
12. migrate tests
13. path hash match tests
14. searchPath tests
15. job restart tests

Each section is prefaced by a section header that starts with
# Section <#> for searching.
"""

class FailureUpdateCallback(callbacks.UpdateCallback):

    def _failMethod(self, *args, **kwargs):
        e = Exception(self.failMethodName)
        e.errorIsUncatchable = True
        raise e

    def __init__(self, failMethod):
        callbacks.UpdateCallback.__init__(self)
        self.failMethodName = failMethod
        self.__dict__[failMethod] = self._failMethod

class ClientUpdateTest(rephelp.RepositoryHelper):

#############################################
#
# Section 1. Branch affinity tests
#
#############################################
    

    def testLocalChangeUpdatedWithBranchChange(self):
        # 1. install test=1.0, and group-test=2.0, (group-test v2 should have
        #    a reference to test=1.0 as a local change)

        # 2. update to group-test=:branch, which references test=:branch. 
        #    Conary should update test=1.0 -> test=:branch, even though you've
        #    made a local change to test.

        self.addComponent('test:runtime',   '1.0-1-1') 

        branchVer = '/localhost@rpl:branch/3.0-1-1'

        self.addComponent('test:runtime',   '2.0-1-1')
        self.addCollection("group-test", "2.0-1-1", ["test:runtime"])
        self.addComponent('test:runtime',   branchVer)
        self.addCollection("group-test", branchVer, ['test:runtime'])

        self.updatePkg(self.rootDir, 'test:runtime', '1.0-1-1')
        self.updatePkg(self.rootDir, 'group-test', '2.0-1-1', recurse=False)

        self.checkUpdate('group-test=' + branchVer, 
                         ['test:runtime=:linux/1.0--:branch/3.0',
                          'group-test=:linux/2.0--:branch/3.0'])

    def testLocalChangesBranchThenGroup(self):
        # install test:runtime=:branch/2.0
        # install group-test=:devel/1.0
        # upgrade group-test to :branch=3.0,
        # test:runtime should be updated to 3.0, since it's on the target branch
        branchVer1 = '/localhost@rpl:branch/2.0-1-1'
        branchVer2 = '/localhost@rpl:branch/3.0-1-1'

        self.addComponent('test:runtime',   '1.0-1-1') 
        self.addComponent('test:runtime',   branchVer1)
        self.addComponent('test:runtime',   branchVer2)

        self.addCollection("group-test", '1.0-1-1', ['test:runtime'])
        self.addCollection("group-test", branchVer2, ['test:runtime'])

        self.updatePkg(self.rootDir, 'group-test', '1.0-1-1', recurse=False)
        self.updatePkg(self.rootDir, 'test:runtime', branchVer1)

        self.checkUpdate('group-test=' + branchVer2, 
                         ['test:runtime=:branch/2.0--:branch/3.0',
                          'group-test=:linux/1.0--:branch/3.0'])

    def testLocalChangesBranchThenGroup2(self):
        # okay, we're getting a little more complicated here.
        # group-os contains group-dist.
        # group-dist used to contain test, but on :branch, it contains 
        # group-core which contains test.
        # 1. Install old group-os, group-dist
        # 2. update test to an old version on :branch 
        # 3. update group-os to :branch, make sure that test knows to 
        #    update its version, not try to install a new version
        v1 = '1.0-1-1'
        branchVer1 = '/localhost@rpl:branch/2.0-1-1'
        branchVer2 = '/localhost@rpl:branch/3.0-1-1'

        for ver in v1, branchVer1, branchVer2:
            self.addComponent('test:runtime',   ver)
            self.addCollection('test', ver, ['test:runtime'])

        self.addCollection("group-dist", '1.0-1-1', ['test'])
        self.addCollection("group-os", '1.0-1-1', ['group-dist'])

        self.addCollection("group-core", branchVer2, ['test'])
        self.addCollection("group-dist", branchVer2, ['group-core'])
        self.addCollection("group-os", branchVer2, ['group-dist'])

        self.updatePkg(self.rootDir, 'group-os', '1.0-1-1', recurse=False)
        self.updatePkg(self.rootDir, 'group-dist', '1.0-1-1', recurse=False)
        self.updatePkg(self.rootDir, 'test', branchVer1)

        self.checkUpdate('group-os=' + branchVer2,
                         ['test:runtime=:branch/2.0--:branch/3.0',
                          'test=:branch/2.0--:branch/3.0',
                          'group-core=--:branch/3.0',
                          'group-dist=:linux/1.0--:branch/3.0',
                          'group-os=:linux/1.0--:branch/3.0'])

    def testPinnedCollectionBranchUpdate(self):
        # install test collection 2.0 and group-dist 1.0
        # pin test collection
        # update group-dist to another branch, causing the update to 
        # test 2.0 to be treated as an absolute update...
        # (strangely, this test works differently if 3.0 is on the same branch)
        # since v2 is pinned, we install a fresh v3 (that has only the 
        # components in the already installed v2)

        for v in ('1.0-1-1', '2.0-1-1', '/localhost@rpl:branch/3.0-1-1'):
            self.addComponent('test:runtime', v)
            self.addCollection('test', v, ['test:runtime'])
            self.addCollection('group-dist', v, ['test'])

        self.updatePkg(self.rootDir, 'test', '2.0', recurse=False)
        self.updatePkg(self.rootDir, 'group-dist', '1.0', recurse=False)
        self.pin('test')
        self.checkUpdate('group-dist=:branch/3.0',
                         ['group-dist=1.0--:branch',
                          'test=--:branch/3.0', 
                          ])

    def testPinnedInstallExcludeTroves(self):
        self.addCollection('test', '1', [':run'])
        self.addComponent('test:run', '1')
        self.addCollection('test', '2', [':run'])
        self.addComponent('test:run', '2', filePrimer=1)
        self.addCollection('group-dist', '1', ['test'])
        self.addCollection('group-dist', '2', ['test'])

        self.updatePkg('group-dist=1')
        self.pin('test')

        self.checkUpdate(['group-dist'], ['group-dist=1--2', 
                                          'test=--2', 'test:run=--2'])
        self.cfg.excludeTroves.addExp("test")
        self.checkUpdate(['group-dist'], ['group-dist=1--2'])
        self.cfg.excludeTroves = RegularExpressionList()

    def testMaintainBranchSwitch(self):
        b1 = '/localhost@rpl:branch/'
        b2 = '/localhost@rpl:rpl1/'

        for v in ('1.0-1-1', b1 + '2.0-1-1', b2 + '3.0-1-1'):
            self.addComponent('test:runtime', v)
            self.addCollection('test', v, ['test:runtime'])

        self.addCollection('group-test', '1.0-1-1', ['test'])
        self.addCollection('group-test', b2 + '3.0-1-1', ['test'])

        self.updatePkg(self.rootDir, 'group-test', '1.0-1-1')
        self.updatePkg(self.rootDir, 'test', b1 + '2.0-1-1')

        self.checkUpdate(['group-test='+ b2+'3.0-1-1'], 
                         ['group-test=1.0-1-1--3.0-1-1'])

    def testConflictingAdds(self):
        # this test tickled a bug where the cross-branch update of 
        # gnome-vfs:lib and :data resulted (incorrectly) in those updates
        # looking like they were orphaned branch updates and needed to try 
        # to find a system trove to match against.  They would then find the 
        # x86_64 trove which had been moved onto the new branch, and an
        # error would arise because two troves were updating to the same
        # version.
        orig = '1.0-1-1'
        b2 = '/localhost@rpl:branch/2.0-1-1'
        b3 = '/localhost@rpl:branch/3.0-1-1'
        for v in orig, b3:
            self.addComponent('gnome-vfs:lib', v, flavor='is:x86')
            self.addComponent('gnome-vfs:data', v, flavor='is:x86',
                                       filePrimer=2)
            self.addCollection('gnome-vfs', v, 
                                        [('gnome-vfs:lib',  v, 'is:x86'),
                                         ('gnome-vfs:data', v, 'is:x86'),
                                         ])
        for v in orig, b2, b3:
            self.addComponent('gnome-vfs:lib', v, flavor='is:x86_64',
                                       filePrimer=3)
            self.addComponent('gnome-vfs:data', v, flavor='is:x86_64',
                                       filePrimer=4)

        self.checkUpdate(['gnome-vfs[is:x86]', 
                          'gnome-vfs:lib=:branch/2.0[is:x86_64]',
                          'gnome-vfs:data=:branch/2.0[is:x86_64]', ],
                         ['gnome-vfs=--:linux/1.0[is:x86]',
                          'gnome-vfs:lib=--:linux/1.0[is:x86]',
                          'gnome-vfs:data=--:linux/1.0[is:x86]',
                          'gnome-vfs:lib=--:branch/2.0[is:x86_64]', 
                          'gnome-vfs:data=--:branch/2.0[is:x86_64]'], 
                          apply=True)
        self.checkUpdate('gnome-vfs=' + b3, 
                         ['gnome-vfs{,:lib,:data}=:linux[is:x86]--:branch/3.0[is:x86]'])

    def testUpdateTroveMissingOneOfTwoComponents(self):
        # in this case, conary was counting the installed foo:runtime=:b2
        # as a user-made change from the uninstalled version, even though
        # the foo:runtime=:b2's parent trove was installed.

        for v in (':branch1/1.0', ':branch2/1.0', ':branch3/1.0'):
            self.addComponent('foo:runtime', v)
            self.addCollection('foo', v, ['foo:runtime'])

        self.updatePkg(self.rootDir, ['foo=:branch1',
                                      'foo=:branch2',
                                      'foo:runtime=:branch2', 
                                      ], recurse=False)
        self.checkUpdate(['foo=:branch3'], 
                         ['foo=:branch2--:branch3', 
                          'foo:runtime=:branch2--:branch3'])


    def testSwitchBackBranches(self):
        # 1. Install group-foo=:b1, foo=:b2, foo:runtime=:b2
        # 2. Try to switch foo back to :b1, where it originated.
        # The switch back should succeed.

        for v in (':branch1/1.0', ':branch1/2.0', ':branch2/2.0'):
            self.addComponent('foo:runtime', v)
            self.addCollection('foo', v, ['foo:runtime'])

        self.addCollection('group-foo', ':branch1/1.0',
                                    #note explicit reference to foo:runtime
                                    ['foo', 'foo:runtime'])
        self.updatePkg(self.rootDir, ['group-foo=:branch1', 
                                      'foo=:branch2', 
                                      'foo:runtime=:branch2'], recurse=False)

        self.checkUpdate(['foo=:branch1'], 
                         ['foo=:branch2--:branch1', 
                          'foo:runtime=:branch2--:branch1'])

    def testFollowLocalChangesBreak(self):
        # group-a included group-b includes foo includes foo:runtime

        # installed troves (this is complicated):
        # group-a is on :b2, group-b is on :b1, foo is on :b2, and foo:run 
        # is on :b1.

        # This test shows that when we switch group-b back to :b2, 
        # it will see that foo has already been switched back and therefore
        # it won't try to switch foo:runtime.

        for v in (':b1/1', ':b2/1', ':b2/2'):
            self.addComponent('foo:runtime', v)
            self.addCollection('foo', v, ['foo:runtime'])
            self.addCollection('group-b', v, ['foo'])

        self.addCollection('group-a', ':b2/1', 
                                    ['group-b', 'foo', 'foo:runtime'])

        self.updatePkg(self.rootDir, ['group-a=:b2', 'group-b=:b1', 
                                      'foo=:b2/1', 'foo:runtime=:b1'],
                                      recurse=False)
        self.checkUpdate(['group-b=:b2'], ['group-b=:b1--:b2', 'foo=:b2--:b2'])

    def testRespectBranchAffinityRecurse(self):        
        # group-a includes group-b includes foo includes foo:runtime.

        # installed troves:
        # group-a is on :b1, group-b is on :b2, foo is on :b3, and 
        # foo:runtime is on :b2.

        # This test shows that when we switch group-b from :b2 to :b3, 
        # foo:runtime doesn't get switched because foo is alreay on :b3 
        # (because foo isn't making a branch switch, we
        # respect branch affinity for its children)

        # NOTE: this test relies the troves on :b2 having a later timestamp
        # than those on :b1

        for v in (':b1/1', ':b2/2', ':b3/3', ':b3/3.1'):
            self.addComponent('foo:runtime', v)
            self.addCollection('foo', v, ['foo:runtime'])
            self.addCollection('group-b', v, ['foo'])

        self.addCollection('group-a', ':b1/1', ['group-b', 'foo', 
                                                         'foo:runtime'])
        self.updatePkg(self.rootDir, ['group-a=:b1', 'group-b=:b2', 
                                      'foo=:b3/3', 'foo:runtime=:b2'], 
                                      recurse=False)
        self.checkUpdate(['group-b=:b3'],
                         ['group-b=:b2--:b3',
                          'foo=:b3--:b3'])

    def testNewReferenceRespectsAffinity(self):        
        # 1. Install a local version of foo:lib.
        # 2. An update or install of group-dist adds a reference
        #    to foo:lib when none-existed before.  The installed
        #    foo:lib should not be touched.
        localVer = '/localhost@rpl:linux/2.0-1-1/LOCAL:COOK/1'
        self.addComponent('foo:lib', localVer)
        self.addComponent('foo:lib', '1.0')
        self.addComponent('bar:runtime', '1.0', filePrimer=2)

        self.addCollection('group-dist', '1.0', ['bar:runtime'])
        self.addCollection('group-dist', '2.0', [('bar:runtime', '1.0'), ('foo:lib', '1.0')])
        self.updatePkg('foo:lib=%s' % localVer)
        
        # try a fresh install of group-dist 
        self.checkUpdate('group-dist', ['group-dist', 'bar:runtime'])

        # now try an update of group-dist
        self.updatePkg('group-dist=1.0')
        self.checkUpdate('group-dist', ['group-dist'])

    def testNewReferenceRespectsAffinity2(self):        
        # 1. Install a local version of foo:lib.
        # 2. An update or install of group-dist adds a reference
        #    to foo:lib when none-existed before.  The installed
        #    foo:lib should not be touched.
        localVer = '/localhost@rpl:linux/2.0-1-1/LOCAL:COOK/1'
        self.addComponent('foo:lib', localVer)
        self.addCollection('foo', localVer, [':lib'])
        self.addComponent('foo:lib', '1.0')
        self.addCollection('foo', '1.0', [':lib'])
        self.addComponent('bar:runtime', '1.0', filePrimer=2)

        self.addCollection('group-dist', '1.0', ['bar:runtime'])
        self.addCollection('group-dist', '2.0', [('bar:runtime', '1.0'), ('foo', '1.0')])
        self.updatePkg('foo=%s' % localVer)
        
        # try a fresh install of group-dist 
        self.checkUpdate('group-dist', ['group-dist', 'bar:runtime'])

        # now try an update of group-dist
        self.updatePkg('group-dist=1.0')
        self.checkUpdate('group-dist', ['group-dist'])



    def testNewReferenceSwitchesBranch(self):        
        # 1. Install a local version of foo:lib from :devel.
        # 2. An update of group-dist from :devel -> :1 adds a reference
        #    to foo:lib.  Foo should follow.
        self.addComponent('foo:lib', ':devel/1.0')
        self.addComponent('bar:runtime', ':devel/1.0', filePrimer=2)
        self.addCollection('group-dist', ':devel/1.0', ['bar:runtime']) # no reference to foo:lib

        self.addComponent('foo:lib', ':new/2.0')
        self.addComponent('bar:runtime', ':new/2.0', filePrimer=2)
        self.addCollection('group-dist', ':new/2.0', ['bar:runtime', 'foo:lib']) 

        self.updatePkg('foo:lib=:devel/1.0')
        self.updatePkg('group-dist=:devel/1.0')
        self.checkUpdate('group-dist=:new/2.0', ['group-dist', 'bar:runtime', 'foo:lib'])


    def testGroupIncludedInGroupMovedToAnotherBranch(self):
        def _buildStuff(version):
            self.addComponent('test:runtime', version)
            self.addCollection('test', version,
                                        [ 'test:runtime' ])
            self.addCollection('group-media', version,
                                        [ ('test', version) ])
            self.addCollection('group-dist', version,
                                        [ ('group-media', version) ])

        db = database.Database(self.rootDir, self.cfg.dbPath)
        # first create group-dist which includes group-media which
        # includes test
        _buildStuff('1.0-1-1')

        # install group-dist
        self.checkUpdate(['group-dist'], 
                         ['group-dist=--1.0', 
                          'group-media=--1.0',
                          'test=--1.0',
                          'test:runtime=--1.0'], apply=True)

        # now shadow group-media and move to that branch
        self.mkbranch('1.0-1-1', 'localhost@rpl:shadow',
                      'group-media', shadow = True, binaryOnly = True)

        self.checkUpdate(['group-media=localhost@rpl:shadow'], 
                         ['group-media=:linux--:shadow',
                          'test=:linux--:shadow',
                          'test:runtime=:linux--:shadow'], apply=True)

        # build a new version of group-dist, group-media, etc
        _buildStuff('2.0-1-1')
        # update group-dist, 
        # verify that group-media and the packages that are contained
        # in group-media are still from the shadow
        self.checkUpdate(['group-dist'], 
                         ['group-dist=1.0--2.0'])


    def testNewGroupContainsComponentOnTwoBranches(self):
        # create test:runtime /localhost@rpl:linux/1.0-1-1
        # create group-test=1.0-1-1 which contains test:runtime=1.0-1-1
        # install group-test
        # create test:runtime /localhost@rpl:linux/1.1-1-1
        # update to test:runtime=1.1-1-1
        # create test:runtime /localhost@rpl:linux/2.0-1-1
        # create test:runtime /localhost@rpl:linux/1.1-1-1/branch/1.2-1-1
        # create group-test=2.0-1-1 which contains test:runtime=2.0-1-1 and
        #       test:runtime=:branch
        # update group-test
        # should end up with both installed (as per group-test=2.0-1-1)

        self.addComponent('test:runtime', '1.0-1-1')
        self.addCollection('group-test', '1.0-1-1',
                                    [ ('test:runtime', '1.0-1-1') ])
        self.updatePkg(self.rootDir, 'group-test')
        self.addComponent('test:runtime', '1.1-1-1')
        self.updatePkg(self.rootDir, 'test:runtime')
        self.addComponent('test:runtime', '2.0-1-1')
        self.addComponent('test:runtime',
                                   '/localhost@rpl:linux/1.1-1-1/branch/1.2-1-1',
                                   fileContents = [ ( 'bar', 'contents2' )])

        self.addCollection('group-test', '2.0-1-1',
                                    [ ('test:runtime', '2.0-1-1'),
             ('test:runtime', '/localhost@rpl:linux/1.1-1-1/branch/1.2-1-1') ])
        self.updatePkg(self.rootDir, 'group-test')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        vlist = db.getTroveVersionList('test:runtime')
        assert(len(vlist) == 2)
        assert(sorted([ x.asString() for x in vlist]) ==
               ['/localhost@rpl:linux/1.1-1-1/branch/1.2-1-1',
                '/localhost@rpl:linux/2.0-1-1'])

    def testNewGroupContainsComponentOnTwoBranches2(self):
        # same as above, except:
        # create group-test=2.0-1-1 which contains test:runtime=1.0-1-1
        #       (we use test:runtime=2.0 above) and test:runtime=:branch
        # update group-test
        # should end up with both installed (as per group-test=2.0-1-1)

        self.addComponent('test:runtime', '1.0-1-1')
        self.addCollection('group-test', '1.0-1-1',
                                    [ ('test:runtime', '1.0-1-1') ])
        self.updatePkg(self.rootDir, 'group-test')
        self.addComponent('test:runtime', '1.1-1-1')
        self.updatePkg(self.rootDir, 'test:runtime')
        self.addComponent('test:runtime',
                               '/localhost@rpl:linux/1.1-1-1/branch/1.2-1-1',
                               fileContents = [ ( 'bar', 'contents2' )])

        self.addCollection('group-test', '2.0-1-1',
                                    [ ('test:runtime', '1.0-1-1'),
             ('test:runtime', '/localhost@rpl:linux/1.1-1-1/branch/1.2-1-1') ])

        self.checkUpdate('group-test',
             [ 'group-test=1.0-1-1--2.0-1-1',
               'test:runtime=/localhost@rpl:linux/1.1-1-1/branch/1.2-1-1'
               ])

    def testNewGroupContainsComponentOnTwoBranches3(self):
        # this is similar to the first testNewGroup...OnTwoBranches, except
        # that it adds only a component from the branch to the group.
        v1 = '1.0-1-1'
        self.addComponent('test:runtime', v1)
        self.addCollection('test', v1, [ 'test:runtime' ])
        v11 = '1.1-1-1'
        self.addComponent('test:runtime', v11)
        self.addCollection('test', v11, [ 'test:runtime' ])
        self.addCollection('group-test', '1.0-1-1',
                                    [ ('test', v11) ])
        self.updatePkg(self.rootDir, 'group-test')
        # downgrade the test package
        self.updatePkg(self.rootDir, 'test', v1)

        v2 = '/localhost@rpl:linux/2.0-1-1'
        self.addComponent('test:runtime', v2)
        self.addCollection('test', v2, [ 'test:runtime' ])

        vbranch = '/localhost@rpl:linux/1.1-1-1/branch/1.2-1-1'
        self.addComponent('test:runtime',
                                   vbranch,
                                   fileContents = [ ( 'bar', 'contents2' )])

        self.addCollection('group-test', '2.0-1-1',
                                    [ ('test', v2),
                                      ('test:runtime', vbranch) ])
        self.updatePkg(self.rootDir, 'group-test')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        vlist = db.getTroveVersionList('test:runtime')
        assert(len(vlist) == 2)
        assert(sorted([ x.asString() for x in vlist]) == [vbranch, v2])

    def testSwitchLabel(self):
        # create test:runtime :linux/1.0-1-1 
        # create test:runtime :linux/branch/1.0-1-1 
        # create test:runtime :linux/2.0-1-1
        # create group-dist which contains test:runtime=2.0, 
        #    test:runtime=:branch
        # create group-dist :linux/branch/1.0-1-1
        # install group-dist from head
        # update to group-dist from branch

        # install group-dist
        self.addComponent('test:runtime', '1.0-1-1')
        self.addCollection('test', '1.0-1-1', [':runtime'])
        self.mkbranch(self.cfg.buildLabel, "@rpl:branch", "test",
                      binaryOnly=True)
        self.addComponent('test:runtime', '2.0-1-1', filePrimer=2)
        self.addCollection('group-dist', '3.0-1-1',
                        [ ('test:runtime', '2.0-1-1'),
                          ('test:runtime', '/localhost@rpl:linux/1.0-1-1/branch/1.0-1-1')
                                    ])
        self.mkbranch(self.cfg.buildLabel, "@rpl:rpl1", "group-dist",
                      binaryOnly=True)
        self.updatePkg(self.rootDir, 'group-dist')
        self.updatePkg(self.rootDir, 'group-dist', ':rpl1')

    def testBranchAffinityWhenTroveMovesGroups(self):
        # make sure that branch affinity is honored when a package
        # moves between groups
        #
        # create foo:runtime foo and group-a
        # group-a=1.0-1-1
        #  `- foo=1.0-1-1
        #     `- foo:runtime=1.0-1-1
        self.addComponent('foo:runtime', '1.0-1-1')
        self.addCollection("foo", "1.0-1-1",
                                    [ ("foo:runtime", "1.0-1-1") ])
        self.addCollection("group-a", "1.0-1-1",
                                    [ ("foo", "1.0-1-1") ])

        # install group-a
        self.updatePkg(self.rootDir, "group-a", '1.0-1-1')

        # create foo and foo:runtime on a different branch
        branch = '/localhost@rpl:branch/1.0-1-1'
        self.addComponent('foo:runtime', branch)
        self.addCollection("foo", branch,
                                    [ ("foo:runtime", branch) ])
        # change the installed version of foo to the version on
        # the branch.
        self.updatePkg(self.rootDir, 'foo', branch)

        # create
        # group-a=2.0-1-1
        #   `- group-b=2.0-1-1
        #      `- foo=1.0-1-1
        self.addCollection("group-b", "2.0-1-1",
                                    [ ("foo", "1.0-1-1") ])
        self.addCollection("group-a", "2.0-1-1",
                                    [ ("group-b", "2.0-1-1") ])

        self.checkUpdate('group-a',
                         [ 'group-b=2.0-1-1',
                           'group-a=1.0-1-1--2.0-1-1',
                           ])

        # update group-a.  we should keep foo from rpl:branch due to
        # branch affinity.
        self.addCollection("group-b", "3.0-1-1",
                                    [ ("foo:runtime", "1.0-1-1") ])
        self.addCollection("group-a", "3.0-1-1",
                                    [ ("group-b", "3.0-1-1") ])

        self.checkUpdate('group-a',
                         [ 'group-b=3.0-1-1',
                           'group-a=1.0-1-1--3.0-1-1',
                           ])

    @testhelp.context('branchaffinity')
    def testBranchSwitchAsPartOfUpdate(self):
        # the group being updated stays on the same label, but the package
        # switches.  That's okay, because the trove that is being switched
        # is not part of a local update.
        self.addComponent('foo:run', '1')
        self.addCollection('group-foo', '1', ['foo:run'])
        self.addComponent('foo:run', ':b1/1')
        self.addCollection('group-foo', '2', [('foo:run', ':b1/1')])

        self.updatePkg('group-foo=1')
        self.checkUpdate('group-foo', ['group-foo', 'foo:run=1--:b1/1'])

        self.addComponent('foo:run', '2')
        self.updatePkg('foo:run')

        # if we've made some minor change, this update should still
        # be allowed.
        self.checkUpdate('group-foo', ['group-foo', 'foo:run=2--:b1/1'])

    @testhelp.context('branchaffinity')
    def testUpdateMatchesBothSidesOfLocalUpdate(self):
        self.addComponent('foo:run', '1')
        self.addComponent('foo:run', '2')
        self.addComponent('foo:run', ':branch/1')
        self.addComponent('foo:run', '3', filePrimer=2)
        self.addCollection('foo', '1', [':run'])

        self.updatePkg(['foo=1', 'foo:run=:branch'], recurse=False)

        # :linux/3 matches up with the (missing) :linux/1, leaving
        # :linux/2 to match up with :branch.
        self.checkUpdate(['foo:run=:linux/2', 'foo:run=:linux/3'],
                         ['foo:run=--3', 'foo:run=:branch--2'])

    @testhelp.context('branchaffinity')
    def testUpdateMatchesBothSidesOfLocalUpdate2(self):
        for idx, v in enumerate(['1', '2', ':branch/1', ':branch/2']):
            self.addComponent('foo:run', v, filePrimer=idx)
            self.addCollection('foo', v, [':run'])

        self.updatePkg(['foo=1', 'foo:run=:branch/1'], recurse=False)

        self.checkUpdate(['foo=2', 'foo:run=:branch'],
                         ['foo=:linux/1--:linux/2',
                          'foo:run=:branch/1--:branch/2'])


############################################
#
# 3. Erasure tests
#
############################################

    @context('erase')
    def testEraseWithNoDeps(self):
        self.addComponent('foo:runtime', '1.0-1-1')
        self.addCollection('foo', '1.0-1-1', ['foo:runtime'])
        self.addComponent('bar:runtime', '1.0-1-1', 
                                    requires='trove: foo:runtime', 
                                    filePrimer=1)
        self.updatePkg(self.rootDir, ['foo:runtime', 'foo', 'bar:runtime'])
        self.checkUpdate(['-foo'], 
                         ['foo=1.0--', 
                          'foo:runtime=1.0--'], depCheck=False)

    @context('erase')
    def testErase(self):
        self.addComponent("test:runtime", "1.0-1-1")
        self.addCollection("test", "1.0-1-1",
                                    [ "test:runtime" ])
        self.addCollection("group-foo", "1.0-1-1",
                                    [ ("test:runtime", "1.0-1-1") ])
        self.updatePkg(self.rootDir, 'test')
        self.updatePkg(self.rootDir, 'group-foo')
        self.erasePkg(self.rootDir, 'test')
        # test:runtime should still be there because the group needs it
        self.erasePkg(self.rootDir, 'test:runtime')

    @context('erase')
    def testEraseGroup(self):
        self.addComponent("test:runtime", "1.0-1-1")
        self.addCollection("test", "1.0-1-1",
                                    [ "test:runtime" ])
        self.addCollection("group-foo", "1.0-1-1",
                                    [ ("test", "1.0-1-1") ])
        self.updatePkg(self.rootDir, 'group-foo')
        self.erasePkg(self.rootDir, 'group-foo')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert([ x for x in db.iterAllTroveNames() ] == [])

    @context('erase')
    def testEraseOverridesUpdate(self):
        for v in '1', '2','3':
            self.addComponent('foo:runtime', v)
            self.addCollection('foo', v, [':runtime'])
            self.addCollection('group-foo', v, ['foo'])

        self.updatePkg(['group-foo=1', 'foo=2', 'foo:runtime=2'], recurse=False)
        self.checkUpdate(['foo', '-foo:runtime'],
                         ['foo=2--3', 'foo:runtime=2--'])

    @context('erase')
    def testEraseWeakRef(self):
        raise testhelp.SkipTestException(
                        'CNY-5 (closed) erasing weak ref troves is a bad idea')
        self.addComponent("test:runtime", "1.0-1-1")
        self.addCollection("test", "1.0-1-1",
                                    [ "test:runtime" ])
        self.addCollection("group-foo", "1.0-1-1",
                                    [ ("test", "1.0-1-1") ])
        self.updatePkg(['test:runtime', 'group-foo'], recurse=False)
        self.checkUpdate(['-group-foo'], 
                         ['group-foo=1.0--', 'test:runtime=1.0--'])

############################################
#
# 4. Uninstalled/installed Reference tests
#
#############################################

    def testUpdateWouldInstallNewComponentForUninstalledPkg(self):
        self.addComponent('foo:lib', '1')
        self.addCollection('foo', '1', [':lib'])
        self.addCollection('group-foo', '1', ['foo'])
        self.updatePkg('group-foo', recurse=False)

        self.addComponent('foo:lib', '2')
        self.addComponent('foo:runtime', '2')
        self.addCollection('foo', '2', [':lib', ':runtime'])
        self.addCollection('group-foo', '2', ['foo'])

        self.checkUpdate('group-foo', ['group-foo'])
        # don't install foo:runtime even though it's new


    def testRemovedComponentMovesGroups(self):
        # create foo:runtime foo and group-a
        # group-a=1.0-1-1
        #  `- foo=1.0-1-1
        #     `- foo:runtime=1.0-1-1
        self.addComponent('foo:runtime', '1.0-1-1')
        self.addCollection("foo", "1.0-1-1",
                                    [ ("foo:runtime", "1.0-1-1") ])
        self.addCollection("group-a", "1.0-1-1",
                                    [ ("foo", "1.0-1-1") ])

        # install group-a
        self.updatePkg(self.rootDir, "group-a", '1.0-1-1')
        # erase foo
        self.checkUpdate('-foo', ['foo=1.0--', 'foo:runtime=1.0--'], 
                         apply=True)

        # create
        # group-a=2.0-1-1
        #   `- group-b=2.0-1-1
        #      `- foo=1.0-1-1
        self.addCollection("group-b", "2.0-1-1",
                                    [ ("foo", "1.0-1-1") ])
        self.addCollection("group-a", "2.0-1-1",
                                    [ ("group-b", "2.0-1-1") ])

        # update group-a.  Since we erased foo before, it should not
        # show up now.
        self.checkUpdate('group-a',
                         [ 'group-b=2.0-1-1',
                           'group-a=1.0-1-1--2.0-1-1',
                           ])

        # now add a version where group-b has a direct reference to foo:runtime
        # instead of foo.

        self.addCollection("group-b", "3.0-1-1",
                                    [ ("foo:runtime", "1.0-1-1") ])
        self.addCollection("group-a", "3.0-1-1",
                                    [ ("group-b", "3.0-1-1") ])

        # update group-a.  You might think that because foo
        # was erased, we should not install foo:runtime.
        # But we only have a _weak_ reference to the missing foo:runtime,
        # and we are installing a strong reference to foo:runtime here.
        # We let the presence of the new, strong parent trove 
        # override the implicit erasure.
        self.checkUpdate('group-a',
                         [ 'group-b=3.0-1-1',
                           'group-a=1.0-1-1--3.0-1-1',
                           'foo:runtime=--1.0'
                         ])

    def testComponentMovesBetweenContainers(self):
        self.addComponent("test:runtime", "1.0-1-1")
        self.addComponent("test:foo", "1.0-1-1",
                                   fileContents = [ ("/foo", "foo") ] )
        self.addComponent("test:bar", "1.0-1-1",
                                   fileContents = [ ("/bar", "bar") ] )

        self.addCollection("group-foo", "1.0-1-1",
                                    [ ("test:foo", "1.0-1-1"),
                                      ("test:runtime", "1.0-1-1") ])
        self.addCollection("group-bar", "1.0-1-1",
                                    [ ("test:bar", "1.0-1-1") ] )
        self.addCollection("group-uber", "1.0-1-1",
                                    [ ("group-foo", "1.0-1-1" ),
                                      ("group-bar", "1.0-1-1" ) ] )
        self.updatePkg(self.rootDir, 'group-uber')

        self.addCollection("group-foo", "2.0-1-1",
                                    [ ("test:foo", "1.0-1-1") ] )
        self.addCollection("group-bar", "2.0-1-1",
                                    [ ("test:bar", "1.0-1-1"),
                                      ("test:runtime", "1.0-1-1") ])
        self.addCollection("group-uber", "2.0-1-1",
                                    [ ("group-foo", "2.0-1-1" ),
                                      ("group-bar", "2.0-1-1" ) ] )
        self.updatePkg(self.rootDir, 'group-uber')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTroveByName('test:runtime'))

 
    def testComponentMovesFromToplevel(self):
        self.addComponent("test:runtime", "1.0-1-1")
        self.addComponent("test:foo", "1.0-1-1",
                                   fileContents = [ ("/foo", "foo") ] )

        self.addCollection("group-foo", "1.0-1-1",
                                    [ ("test:foo", "1.0-1-1") ] )
        self.addCollection("group-uber", "1.0-1-1",
                                    [ ("group-foo", "1.0-1-1" ),
                                      ("test:runtime", "1.0-1-1" ) ] )
        self.updatePkg(self.rootDir, 'group-uber')

        self.addCollection("group-foo", "2.0-1-1",
                                    [ ("test:foo", "1.0-1-1"),
                                      ("test:runtime", "1.0-1-1") ])
        self.addCollection("group-uber", "2.0-1-1",
                                    [ ("group-foo", "2.0-1-1" ) ])
        self.updatePkg(self.rootDir, 'group-uber')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTroveByName('test:runtime'))


    def testAddComponentMovesGroupAndPackageIsErased(self):
        # create foo:runtime foo and group-a
        # group-a=1.0-1-1
        #  `- foo=1.0-1-1
        #     `- foo:runtime=1.0-1-1
        self.addComponent('foo:runtime', '1.0-1-1')
        self.addCollection("foo", "1.0-1-1",
                                    [ ("foo:runtime", "1.0-1-1") ])
        self.addCollection("group-a", "1.0-1-1",
                                    [ ("foo", "1.0-1-1") ])

        # install group-a
        self.updatePkg(self.rootDir, "group-a", '1.0-1-1')

        # create
        # group-a=2.0-1-1
        #   `- group-b=2.0-1-1
        #      `- foo:runtime=1.0-1-1
        self.addCollection("group-b", "2.0-1-1",
                                    [ ("foo:runtime", "1.0-1-1") ])
        self.addCollection("group-a", "2.0-1-1",
                                    [ ("group-b", "2.0-1-1") ])

        # update group-a.  we should remove foo (but not foo:runtime!)
        # install group-b, and update group-a
        self.checkUpdate('group-a',
                         [ 'foo=1.0-1-1--',
                           'group-b=2.0-1-1',
                           'group-a=1.0-1-1--2.0-1-1',
                           ])

    def testInstallMissingStrongRefs(self):
        # group-dist is old-style and has a strong reference to foo:runtime - 
        # but that shouldn't affect whether it gets installed  since we're 
        # installing foo:runtime's parent.
        for v in '1.0', '2.0':
            self.addComponent('foo:runtime', v)
            self.addCollection('foo', v, [':runtime'])

        self.addCollection('group-dist', '1.0', ['foo', 'foo:runtime'])

        self.updatePkg('group-dist', recurse=False)
        self.checkUpdate('foo', ['foo', 'foo:runtime'])

    def testMultilibPackageUpdates(self):
        for v in '1.0', '2.0':
            self.addComponent('foo:lib', v, 'is:x86')
            self.addComponent('foo:data', v, 'is:x86', filePrimer=1)
            self.addCollection('foo', v, [':lib', ':data'],
                               defaultFlavor='is:x86')

            self.addComponent('foo:lib', v, 'is:x86_64', filePrimer=2)
            self.addComponent('foo:data', v, 'is:x86_64', filePrimer=1)
            self.addCollection('foo', v, [':lib', ':data'],
                               defaultFlavor='is:x86_64')

        self.updatePkg('foo=1.0[is:x86_64]')
        self.updatePkg(['foo=1.0[is:x86]', 'foo:lib=1.0[is:x86]'], 
                        recurse=False, keepExisting=True)
        self.checkUpdate('foo[is:x86]', ['foo[is:x86]--[is:x86]',
                                         'foo:lib[is:x86]--[is:x86]'])

        # what if foo:data[x86_64] is off a version?

        self.checkUpdate('foo:data[is:x86_64]',
                         ['foo:data[is:x86_64]--[is:x86_64]'],
                         apply=True)
        self.checkUpdate('foo[is:x86]', ['foo[is:x86]--[is:x86]',
                                         'foo:lib[is:x86]--[is:x86]'])

    def testKernelSmpNoSmpUpdates(self):
        count = 0
        for v in '1.0', '2.0', '3.0':
            count += 2
            self.addComponent('kernel:runtime', v, '~kernel.smp', 
                             filePrimer=count+1)
            self.addCollection('kernel', v, [':runtime'],
                               defaultFlavor='~kernel.smp')

            self.addComponent('kernel:runtime', v, '~!kernel.smp',
                              filePrimer=count+2)
            self.addCollection('kernel', v, [':runtime'],
                               defaultFlavor='~!kernel.smp')

            self.addCollection('group-core', v, [('kernel', v, '~!kernel.smp'),
                                                 ('kernel', v, '~kernel.smp')])

        self.updatePkg('group-core=1.0', recurse=False)
        self.updatePkg('kernel=1.0[!kernel.smp]',
                       'kernel=2.0[!kernel.smp]')
        self.pin('kernel')

        self.checkUpdate('group-core', 
                         ['group-core=3.0',
                          'kernel=--3.0[!smp]',
                          'kernel:runtime=--3.0[!smp]'])


    def testRespectNotByDefaultOnBranchSwitch(self):
        # switch foo from one branch to another where there's a reference
        # to a third version of foo already existing on the target branch
        # make sure the debuginfo component goes through the required 
        # checks to see if it should be installed.
        for v in '1.0', ':branch/1.0', ':branch/2.0':
            self.addComponent('foo:runtime', v)
            self.addComponent('foo:debuginfo', v)
            self.addCollection('foo', v, [':runtime', (':debuginfo', False)])
            self.addCollection('group-foo', v, ['foo'])

        self.updatePkg('foo')
        self.updatePkg('group-foo=:branch/1.0')
        self.checkUpdate('foo=:branch', ['foo', 'foo:runtime'])

    def testInstallingGroupThatIsReferenced(self):
        # we're group-perl is a new install, therefore
        # we should install paz, even though it is
        # referenced and not installed in group-dist.
        for v in '1.0', ':b2/2.0':
            self.addComponent('paz:run', v)
            self.addCollection('paz', v, [':run'])

        self.addCollection('group-dist', '1.0', ['paz'])
        self.addCollection('group-perl', ':b2/2.0', ['paz'])
        self.addCollection('group-ws', ':b2/2.0', ['group-perl'])

        self.updatePkg('group-dist', '1.0', recurse=False)
        self.checkUpdate('group-ws=:b2/2.0', ['group-ws', 'group-perl', 
                                              'paz=:b2', 'paz:run=:b2'])

    def testReferencedComponentNowByDefault(self):
        self.addComponent('perl:perl=1')
        self.addComponent('perl:runtime=1', filePrimer=1)
        self.addCollection('perl=1', [':perl', ':runtime'])
        self.addCollection('group-dist=1', ['perl'], 
                           weakRefList=[('perl:perl', False), 
                                     ('perl:runtime', True)])

        self.addComponent('perl:perl=2')
        self.addComponent('perl:runtime=2', filePrimer=1)
        self.addCollection('perl=2', [':perl', ':runtime'])
        # this will have perl:perl byDefault True
        self.addCollection('group-dist=2', ['perl'])

        self.updatePkg('group-dist=1')
        self.checkUpdate('group-dist',
                         ['group-dist=2', 'perl=2', 'perl:runtime=2', 
                          'perl:perl=2'], apply=True)
        self.updatePkg('-perl:perl', raiseError=True)

        self.addComponent('perl:perl=3')
        self.addComponent('perl:runtime=3', filePrimer=1)
        self.addCollection('perl=3', [':perl', ':runtime'])
        self.addCollection('group-dist=3', ['perl'])
        self.checkUpdate('group-dist',
                         ['group-dist=3', 'perl=3', 'perl:runtime=3'])
        self.checkUpdate('perl=3', ['perl', 'perl:runtime'], apply=True)


        self.addComponent('perl:perl=4')
        self.addComponent('perl:runtime=4', filePrimer=1)
        self.addCollection('perl=4', [':perl', ':runtime'])
        self.addCollection('group-dist=4', ['perl'])
        self.checkUpdate('group-dist',
                         ['group-dist=4', 'perl=4', 'perl:runtime=4'])



#############################################
#
# 5. General _mergeCollections tests
#
#############################################

    # FIXME: add a test section about byDefault handling
    def testUpdateGroupDevel(self):
        # create two groups one with runtime components turned on
        # and the other with devel components turned on.  
        # Installing the devel group should install the devel components!
        self.addComponent('foo:runtime', '1')
        self.addComponent('foo:devel', '1')
        self.addCollection('foo', '1', ['foo:runtime', 'foo:devel'])

        self.addCollection('group-run', '1', ['foo'],
                           weakRefList=[('foo:runtime', True),
                                        ('foo:devel', False)])

        self.addCollection('group-devel', '1', ['foo'],
                           weakRefList=[('foo:runtime', False),
                                        ('foo:devel', True)])

        self.checkUpdate('group-run', ['group-run', 'foo', 'foo:runtime'],
                         apply=True)
        self.checkUpdate('group-devel', ['group-devel', 'foo:devel'])

    def testUpdateWouldDowngrade(self):
        self.addComponent('foo:runtime', '1.0')
        self.addCollection('foo', '1.0', [':runtime'])
        self.addComponent('foo:runtime', '2.0')
        self.addCollection('foo', '2.0', [':runtime'])
        self.updatePkg(['foo=2.0', 'foo:runtime=2.0'], recurse=False)
        self.checkUpdate('foo=1.0', ['foo=1.0', 'foo:runtime=1.0'])

        self.addComponent('foo:runtime', '3.0')
        self.addCollection('foo', '3.0', [':runtime'])

        self.resetRoot()
        self.updatePkg(['foo=2.0', 'foo:runtime=3.0'], recurse=False)
        self.checkUpdate('foo=1.0', ['foo=1.0'])

        self.addCollection('group-foo', '1.0', ['foo'])
        self.addCollection('group-foo', '2.0', [('foo', '2.0')])

        self.resetRoot()


#############################################
#
# 5. General _mergeCollections tests
#
#############################################

    def testSubGroupsHaveDifferingDefaults(self):
        # foo:devellib is not in current versions of foo. In a new version,
        # foo:devellib is byDefault True in group-devel, byDefault False in 
        # group-core.  We only have group-core installed, so we shouldn't
        # get foo:devellib

        # to be fixed for 1.1
        raise testhelp.SkipTestException('CNY-494 - need for no by-default setting for weak troves')
        self.addComponent('foo:runtime', '1.0')
        self.addComponent('foo:devel', '1.0', filePrimer=1)
        self.addCollection('foo', '1.0', [':devel', ':runtime'])
        self.addCollection('group-devel', '1.0', ['foo'],
                            weakRefList=[('foo:runtime', False),
                                         ('foo:devel', True)])
        self.addCollection('group-core', '1.0', ['foo'],
                            weakRefList=[('foo:runtime', True),
                                         ('foo:devel', False)])
        self.addCollection('group-dist', '1.0', ['group-core', 'group-devel'])


        self.addComponent('foo:runtime', '2.0')
        self.addComponent('foo:devel', '2.0', filePrimer=1)
        self.addComponent('foo:devellib', '2.0', filePrimer=2)
        self.addCollection('foo', '2.0', [':devel', ':devellib', ':runtime'])

        self.addCollection('group-devel', '2.0', ['foo'],
                            weakRefList=[('foo:runtime', False),
                                         ('foo:devel', True),
                                         ('foo:devellib', True)])
        self.addCollection('group-core', '2.0', ['foo'],
                            weakRefList=[('foo:runtime', True),
                                         ('foo:devel', False),
                                         ('foo:devellib', False)])
        self.addCollection('group-dist', '2.0', ['group-core', 'group-devel'])

        self.updatePkg('group-dist=1.0', recurse=False)
        self.updatePkg('group-core=1.0')

        self.checkUpdate('group-dist', ['group-dist=2.0',
                                        'group-core=2.0',
                                        'foo=2.0',
                                        'foo:runtime=2.0'])


    def testUpdateOneOfTwo(self):
        # create test:runtime /localhost@rpl:linux/1.0-1-1
        # create test:runtime /localhost@rpl:linux/1.0-1-1/branch/0.5-1-1
        # update test:runtime=:branch
        # update test:runtime=:branch test:runtime
        # should end up with both installed

        self.addComponent('test:runtime', '1.0-1-1',
                                   fileContents = [( 'foo', 'contents1' )])
        self.addComponent('test:runtime',
                                 '/localhost@rpl:linux/1.0-1-1/branch/0.5-1-1',
                                   fileContents = [ ( 'bar', 'contents1' )])
        self.updatePkg(self.rootDir, 'test:runtime', version=':branch')
        self.updatePkg(self.rootDir, 
                ['test:runtime=/localhost@rpl:linux/1.0-1-1/branch/0.5-1-1', 
                 'test:runtime=/localhost@rpl:linux/1.0-1-1'])
        db = database.Database(self.rootDir, self.cfg.dbPath)
        vlist = db.getTroveVersionList('test:runtime')
        assert(len(vlist) == 2)
        assert(sorted([ x.asString() for x in vlist]) ==
               ['/localhost@rpl:linux/1.0-1-1',
                '/localhost@rpl:linux/1.0-1-1/branch/0.5-1-1',
                ])

    def testDuplicateUpdate(self):
        self.addComponent('test:runtime', '1.0-1-1')
        self.addCollection("test", "1.0-1-1", ["test:runtime"])
        self.updatePkg(self.rootDir, ['test', 'test:runtime'])
        #make sure package actually got installed
        db = database.Database(self.rootDir, self.cfg.dbPath)
        db.getTrove(*db.trovesByName("test")[0])

    def testFindOutdatedTrovesRecursively(self):
        # 1. install group-test=1.0, and test=1.0 (group-test v1 doesn't
        #    reference test)
        # 2. update to group-test=2.0, which references test=2 
        #    conary should note that you have test=1 installed and mark
        #    the update as being from test=1 to test=2, not a new test=2

        self.addComponent('test:runtime',   '1.0-1-1') 
        self.addComponent('test:runtime',   '2.0-1-1') 

        # NOTE: unused trove is just added bc group-test v1 needs to have 
        # something in it and it can't have test:runtime in it.
        self.addComponent('unused:runtime', '1.0-1-1') 

        self.addCollection("group-test", "1.0-1-1", ["unused:runtime"])
        self.addCollection("group-test", "2.0-1-1", ['test:runtime'])

        #setup    
        self.updatePkg(self.rootDir, 'test:runtime', '1.0-1-1')
        self.updatePkg(self.rootDir, 'group-test', '1.0-1-1', recurse=False)
        # the test
        self.checkUpdate('group-test', 
                          ['test:runtime=1.0--2.0',
                           'group-test=1.0--2.0'])


    def testUpdateTroveTwoWays(self):
        # install test:runtime=1 and test:lib=1
        # install group-test, which does not reference test:runtime
        # but does reference test:lib
        # upgrade group-test and test:runtime together, where the new 
        # group-test adds in test.
        # Thus, conary is told to _update_ test:runtime as a primary command
        # and told to install a new test:runtime as part of the group-test
        # update.  It should just update test:runtime

        for v in ('1.0-1-1', '2.0-1-1'):
            self.addComponent('test:runtime', v)
            self.addComponent('test:lib', v, filePrimer=2)
            self.addCollection("test", v, ['test:runtime', 'test:lib'])

        self.addCollection("group-dist", '1.0-1-1', ['test:lib'])
        self.addCollection("group-dist", '2.0-1-1', ['test'])

        self.updatePkg(self.rootDir, 'test:runtime', '1.0-1-1')
        self.updatePkg(self.rootDir, 'test:lib', '1.0-1-1')
        self.updatePkg(self.rootDir, 'group-dist', '1.0-1-1', recurse=False)

        self.checkUpdate(['group-dist', 'test:runtime', 'test:lib'],
                         ['test:runtime=1.0--2.0',
                          'test:lib=1.0--2.0',
                          'test=--2.0', 
                          'group-dist=1.0--2.0'])

    def testUpdateGroupIncludesInstalled(self):
        # foo v1, and foo:runtime v1 and v2 are installed.
        # updateall says to update foo and foo:runtime to v2,
        # because foo:runtime v2 is already installed, only
        # foo should get updated.
        # For a while, a bug where v2 would be considered ineligible for 
        # a removal from the system but not for matching up as an install
        # would result in conary thinking that the update foo:runtime=1--2 
        # should occur.

        self.addComponent('foo:runtime', '1.0-1-1')
        self.addComponent('foo:runtime', '2.0-1-1', filePrimer=2)

        self.addCollection('foo', '1.0-1-1', ['foo:runtime'])
        self.addCollection('foo', '2.0-1-1', [('foo:runtime', '2.0-1-1')])
        self.updatePkg(self.rootDir, ['foo=1.0', 'foo:runtime=1.0', 
                                     'foo:runtime=2.0'], recurse=False)
        self.pin('foo:runtime=1.0')
        #replicates an updateAll
        self.checkUpdate(['foo', 'foo:runtime'],
                         ['foo=1.0--2.0'])


    def testMatchingUpdateEraseDoesNothing(self):        
        self.addComponent('foo:runtime', '1')
        self.updatePkg(self.rootDir, 'foo:runtime')
        self.assertRaises(update.NoNewTrovesError,
                          self.checkUpdate, 
                          ['+foo:runtime', '-foo:runtime'], [])




    def testUnlinkedComponent(self):
        self.addComponent('test:runtime', '1.0-1-1')
        self.updatePkg(self.rootDir, 'test:runtime')

        # set up group-test that includes a test:runtime slightly newer
        # than the version we just installed
        self.addComponent('test:runtime', '1.0-2-1')
        self.addCollection('group-test', '1.0-1-1',
                                    [ ('test:runtime', '1.0-2-1') ])
        # install it with no recurse, so that we still hve test:runtime=1.01-1-1
        self.updatePkg(self.rootDir, 'group-test', recurse=False)

        # make a new version of test:runtime and group-test
        self.addComponent('test:runtime', '2.0-1-1')
        self.addCollection('group-test', '2.0-1-1',
                                    [ ('test:runtime', '2.0-1-1') ])

        # now try updating both at the same time.  This makes sure that
        # the group-test update of test:runtime=1.0-2-1--2.0-1-1 does not
        # override the changeset request of 1.0-1-1--2.0-1-1 needed by the
        # system
        self.updatePkg(self.rootDir, ['group-test', 'test:runtime'])

        # check versions of stuff
        db = database.Database(self.rootDir, self.cfg.dbPath)
        vlist = db.getTroveVersionList('test:runtime')
        assert(len(vlist) == 1)
        assert([ x.asString() for x in vlist] == [ '/localhost@rpl:linux/2.0-1-1'])

        vlist = db.getTroveVersionList('group-test')
        assert(len(vlist) == 1)
        assert([ x.asString() for x in vlist] == [ '/localhost@rpl:linux/2.0-1-1'])

    def testUnlinkedComponentInPackage(self):
        v1 = '1.0-1-1'
        self.addComponent('test:runtime', v1)
        self.addCollection('test', v1, [ 'test:runtime' ])
        self.updatePkg(self.rootDir, 'test')

        # set up group-test that includes a test:runtime slightly newer
        # than the version we just installed
        v2 = '1.0-2-1'
        self.addComponent('test:runtime', v2)
        self.addCollection('test', v2, [ 'test:runtime' ])
        self.addCollection('group-test', '1.0-1-1',
                                    [ ('test', v2) ])
        # install it with no recurse, so that we still have test:runtime=1.0-1-1
        self.updatePkg(self.rootDir, 'group-test', recurse=False)

        # make a new version of test:runtime and group-test
        v3 = '2.0-1-1'
        self.addComponent('test:runtime', v3)
        self.addCollection('test', v3, [ 'test:runtime' ])
        self.addCollection('group-test', '2.0-1-1',
                                    [ ('test', v3) ])

        # now try updating both at the same time.  This makes sure that
        # the group-test update of test:runtime=1.0-2-1--2.0-1-1 does not
        # override the changeset request of 1.0-1-1--2.0-1-1 needed by the
        # system
        self.updatePkg(self.rootDir, ['test', 'group-test'])

        # check versions of stuff
        db = database.Database(self.rootDir, self.cfg.dbPath)
        vlist = db.getTroveVersionList('test:runtime')
        assert(len(vlist) == 1)
        assert([ x.asString() for x in vlist] == [ '/localhost@rpl:linux/2.0-1-1'])

        vlist = db.getTroveVersionList('test')
        assert(len(vlist) == 1)
        assert([ x.asString() for x in vlist] == [ '/localhost@rpl:linux/2.0-1-1'])

        vlist = db.getTroveVersionList('group-test')
        assert(len(vlist) == 1)
        assert([ x.asString() for x in vlist] == [ '/localhost@rpl:linux/2.0-1-1'])

    def testUpdateByDefault(self):
        # create test (+:runtime) :linux/1.0-1-1 
        # create group-test=1.0-1-1 which contains test:runtime=1
        # create group-machine=1.0-1-1 which contains test:runtime=1
        # but is byDefault False
        # create group-os=1.0-1-1 which contains group-test and 
        #    group-machine


        # install group-os, should not have group-machine installed

        # create test (+:runtime) :linux/2.0-1-1, upgrade 

        # create test (+:runtime) :linux/3.0-1-1 
        # create group-machine=3.0-1-1 which contains test:runtime=2
        # create group-dist=3.0-1-1 which contains test:runtime=2

        # create group-os=3.0-1-1 which contains group-test and 
        #    group-machine

        # upgrade group-os, should not have group-machine installed

        #test=1
        self.addComponent('test:runtime', '1.0-1-1')
        self.addCollection('test', '1.0-1-1',
                                    [ ('test:runtime', '1.0-1-1') ])
        self.addCollection('group-test', '1.0-1-1',
                                    [ ('test', '1.0-1-1') ])
        self.addCollection('group-machine', '1.0-1-1',
                                    [ ('test', '1.0-1-1') ])
        self.addCollection('group-os', '1.0-1-1',
                        [ ('group-test', '1.0-1-1'),
                          ('group-machine', '1.0-1-1', '', False),
                        ])

        # install group-os - should not have group-machine installed
        self.updatePkg(self.rootDir, 'group-os')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(not db.getTroveVersionList('group-machine'))

        #test=2
        self.addComponent('test:runtime', '2.0-1-1')
        self.addCollection('test', '2.0-1-1',
                                    [ ('test:runtime', '2.0-1-1') ])

        
        #update to test=2
        self.updatePkg(self.rootDir, 'test')


        # test=3
        self.addComponent('test:runtime', '3.0-1-1')
        self.addCollection('test', '3.0-1-1',
                                    [ ('test:runtime', '3.0-1-1') ])

        self.addCollection('group-test', '3.0-1-1',
                                    [ ('test', '3.0-1-1') ])
        self.addCollection('group-machine', '3.0-1-1',
                                    [ ('test', '3.0-1-1') ])
        self.addCollection('group-os', '3.0-1-1',
                        [ ('group-test', '3.0-1-1'),
                          ('group-machine', '3.0-1-1', '', False),
                        ])
        self.updatePkg(self.rootDir, 'group-os')
        assert(not db.getTroveVersionList('group-machine'))

    def testUpdateByDefault1(self):
        # create test (+:runtime) :linux/1.0-1-1 
        # create group-test=1.0-1-1 which contains test:runtime=1
        # create group-machine=1.0-1-1 which contains test:runtime=1
        # but is byDefault False
        # create group-os=1.0-1-1 which contains group-test and 
        #    group-machine


        # install group-os, should not have group-machine installed

        # create test (+:runtime) :linux/2.0-1-1, upgrade 

        # create test (+:runtime) :linux/3.0-1-1 
        # create group-machine=3.0-1-1 which contains test:runtime=2
        # create group-dist=3.0-1-1 which contains test:runtime=2

        # create group-os=3.0-1-1 which contains group-test and 
        #    group-machine

        # upgrade group-os, should not have group-machine installed

        #test=1
        self.addComponent('test:runtime', '1.0-1-1')
        self.addCollection('test', '1.0-1-1',
                                    [ ('test:runtime', '1.0-1-1') ])
        self.addCollection('group-test', '1.0-1-1',
                                    [ ('test', '1.0-1-1') ])
        self.addCollection('group-machine', '1.0-1-1',
                                    [ ('test:runtime', '1.0-1-1') ])
        self.addCollection('group-os', '1.0-1-1',
                        [ ('group-test', '1.0-1-1'),
                          ('group-machine', '1.0-1-1', '', False),
                        ])

        # install group-os - should not have group-machine installed
        self.updatePkg(self.rootDir, 'group-os')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(not db.getTroveVersionList('group-machine'))

        #test=2
        self.addComponent('test:runtime', '2.0-1-1')
        self.addCollection('test', '2.0-1-1',
                                    [ ('test:runtime', '2.0-1-1') ])

        
        #update to test=2
        self.updatePkg(self.rootDir, 'test')


        # test=3
        self.addComponent('test:runtime', '3.0-1-1')
        self.addCollection('test', '3.0-1-1',
                                    [ ('test:runtime', '3.0-1-1') ])

        self.addCollection('group-test', '3.0-1-1',
                                    [ ('test', '3.0-1-1') ])
        self.addCollection('group-machine', '3.0-1-1',
                                    [ ('test', '3.0-1-1') ])
        self.addCollection('group-os', '3.0-1-1',
                        [ ('group-test', '3.0-1-1'),
                          ('group-machine', '3.0-1-1', '', False),
                        ])
        self.updatePkg(self.rootDir, 'group-os')

    def testDoubleComponents(self):
        t1 = self.addComponent('test:runtime', '1.0-1-1', 
                                        flavor = 'flv1')
        t2 = self.addComponent('test:runtime', '1.0-1-1', 
                                        flavor = 'flv2', filePrimer = 1)
        self.addCollection('group-package', '1.0-1-1',
                                    [ ('test:runtime', '1.0-1-1',
                                       'flv2') ])
        self.updatePkg(self.rootDir, 'group-package[flv2]')
        self.addCollection('group-all', '1.0-1-1',
                                    [ ('test:runtime', '1.0-1-1', 'flv1'),
                                      ('group-package', '1.0-1-1', 'flv2') ])
        self.updatePkg(self.rootDir, 'group-all[flv1,flv2]')

        db = database.Database(self.rootDir, self.cfg.dbPath)
        l = db.findTrove([], ('test:runtime', None, None))
        assert(len(l) == 2)


    def testDroppedMember(self):
        # * group-a=1 contains test:runtime=1, group-a=2 contains test:runtime=2
        # 
        # * the system has group-a=1, test:runtime=1, test:runtime=2 installed
        #
        # * updating to group-a=2 should remove test:runtime=1

        self.addQuickTestComponent('test:runtime', '1.0-1-1', filePrimer = 0)
        self.addQuickTestCollection("group-a", '1.0-1-1',
                                    [ ("test:runtime", '1.0-1-1') ])

        self.updatePkg(self.rootDir, 'group-a')

        self.addQuickTestComponent('test:runtime', '2.0-1-1', filePrimer = 1)
        self.updatePkg(self.rootDir, 'test:runtime', keepExisting = True)

        self.addQuickTestCollection("group-a", '2.0-1-1',
                                    [ ("test:runtime", '2.0-1-1') ])

        self.updatePkg(self.rootDir, 'group-a')

        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(len(db.trovesByName('test:runtime')) == 1)

    def testPartialUpdates(self):
        # This tests updating components without collections
        version1, flavor1 = self.buildRecipe(recipes.libhelloRecipe, 
                                             'Libhello')[0][0][1:3]
        version1 = versions.VersionFromString(version1)
        version2, flavor2 = self.buildRecipe(recipes.libhelloRecipe, 
                                             'Libhello')[0][0][1:3]
        version2 = versions.VersionFromString(version2)
        version3, flavor3 = self.buildRecipe(recipes.libhelloRecipe, 
                                             'Libhello')[0][0][1:3]
        version3 = versions.VersionFromString(version3)

        self.updatePkg(self.rootDir, "libhello", version1,  
                       tagScript = "/dev/null")
        self.erasePkg(self.rootDir, "libhello:runtime",
                       tagScript = "/dev/null", depCheck = False)
        self.updatePkg(self.rootDir, "libhello", version2,
                       tagScript = "/dev/null", depCheck = False)

        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(not db.hasTrove('libhello:runtime', version1, flavor1))

        self.resetRoot()
        self.updatePkg(self.rootDir, "libhello:runtime", version1,  
                       tagScript = "/dev/null")
        self.updatePkg(self.rootDir, "libhello", version1,  
                       tagScript = "/dev/null")
        self.updatePkg(self.rootDir, "libhello", 
                       tagScript = "/dev/null")
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(not db.hasTrove('libhello', version2, flavor2))
        assert(not db.hasTrove('libhello:runtime', version2, flavor2))
        assert(db.hasTrove('libhello', version3, flavor1))
        assert(db.hasTrove('libhello:runtime', version3, flavor3))

        self.resetRoot()
        self.updatePkg(self.rootDir, "libhello", version1,  
                       tagScript = "/dev/null")
        self.updatePkg(self.rootDir, "libhello:runtime", version2,  
                       tagScript = "/dev/null")
        self.updatePkg(self.rootDir, "libhello", version3,  
                       tagScript = "/dev/null")
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTrove('libhello', version3, flavor3))
        assert(db.hasTrove('libhello:user', version3, flavor3))
        assert(db.hasTrove('libhello:runtime', version3, flavor2))

        self.resetRoot()
        self.cfg.excludeTroves.addExp(".*:runtime")
        self.updatePkg(self.rootDir, "libhello", version1,  
                       tagScript = "/dev/null", depCheck = False)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTrove('libhello', version1, flavor1))
        assert(not db.hasTrove('libhello:runtime', version1, flavor1))
        self.updatePkg(self.rootDir, "libhello:runtime", version1,  
                       tagScript = "/dev/null")
        assert(db.hasTrove('libhello:runtime', version1, flavor1))
        self.cfg.excludeTroves = RegularExpressionList()

        self.resetRoot()
        self.updatePkg(self.rootDir, "libhello:runtime", version1,
                       tagScript = "/dev/null", depCheck = False)
        self.updatePkg(self.rootDir, "libhello", version2,
                       tagScript = "/dev/null", depCheck = False)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTrove('libhello', version2, flavor2))
        assert(db.hasTrove('libhello:user', version2, flavor2))
        assert(db.hasTrove('libhello:runtime', version2, flavor2))

        self.resetRoot()
        self.updatePkg(self.rootDir, "libhello", version1,
                       tagScript = "/dev/null", depCheck = False)
        self.updatePkg(self.rootDir, "libhello:runtime", version2,
                       tagScript = "/dev/null", depCheck = False)
        self.erasePkg(self.rootDir, "libhello")
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert([ x for x in db.iterAllTroveNames() ] == [])

        self.resetRoot()
        self.updatePkg(self.rootDir, "libhello:runtime", version1,
                       tagScript = "/dev/null", depCheck = False)
        self.pin('libhello:runtime')
        self.updatePkg(self.rootDir, "libhello", version2,
                       tagScript = "/dev/null", depCheck = False)
        self.erasePkg(self.rootDir, "libhello")
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert([ x for x in db.iterAllTroveNames() ] == [ 'libhello:runtime' ])
        assert(db.hasTrove('libhello:runtime', version1, flavor2))

        self.resetRoot()
        self.updatePkg(self.rootDir, "libhello", version1,
                       tagScript = "/dev/null", depCheck = False)
        self.pin("libhello:runtime")
        self.updatePkg(self.rootDir, "libhello", version2,
                       tagScript = "/dev/null", depCheck = False)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTrove('libhello', version2, flavor2))
        assert(db.hasTrove('libhello:runtime', version1, flavor2))
        assert(db.hasTrove('libhello:user', version2, flavor2))

        self.resetRoot()
        # update libhello:runtime to version 3, then libhello to version 2
        # libhello:runtime should stay at version 3 - we don't downgrade
        # user-made changes
        self.updatePkg(self.rootDir, "libhello", version1,
                       tagScript = "/dev/null", depCheck = False)
        self.updatePkg(self.rootDir, "libhello:runtime", version3,
                       tagScript = "/dev/null", depCheck = False)
        self.updatePkg(self.rootDir, "libhello", version2,
                       tagScript = "/dev/null", depCheck = False)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTrove('libhello', version2, flavor2))
        assert(db.hasTrove('libhello:runtime', version3, flavor2))
        assert(db.hasTrove('libhello:user', version2, flavor2))

    def testUpdateWithLocalUpdateParent(self):
        # we've made a change to the system that affects our update
        # but is not a part of our update - the switch of group-foo from
        # 1 to 2.
        for v in '1', '2':
            self.addComponent('foo:runtime', v)
            self.addCollection('foo', v, [':runtime'])
            self.addCollection('group-foo', v, [('foo', '1')])
            self.addCollection('group-bar', v, ['group-foo'])
        self.updatePkg(['group-bar=1', 'group-foo=2', 'foo=1', 'foo:runtime=1'],
                       recurse=False)
        self.checkUpdate('foo', ['foo=1--2', 'foo:runtime=1--2'])


    def testUpdatePinnedKernelsWouldInstallSmp(self):
        for v in range(1, 4):
            vStr = str(v)
            self.addComponent('kernel:lib', vStr, '~!smp', filePrimer=v*2)
            self.addCollection('kernel', vStr, [':lib'], defaultFlavor='~!smp')
            self.addComponent('kernel:lib', vStr, '~smp', filePrimer=(v*2 + 1))
            self.addCollection('kernel', vStr, [':lib'], defaultFlavor='~smp')

            self.addCollection('group-dist', vStr, [('kernel', vStr, '~smp'),
                                                   ('kernel', vStr, '~!smp')])

        self.updatePkg(['group-dist=2'], recurse=False)
        self.updatePkg(['kernel=1[~!smp]', 'kernel=2[~!smp]'])

        self.checkUpdate('group-dist', 
                         ['group-dist=2--3', 'kernel=2--3[~!smp]',
                          'kernel:lib=2--3[~!smp]'])

    def testLocalFlavorSwitchOverride(self):
        self.addComponent('foo:run', '1', 'f1')
        self.addComponent('foo:run', '1', '!f1')
        self.addCollection('foo', '1', [':run'], defaultFlavor='f1')
        self.addCollection('foo', '1', [':run'], defaultFlavor='!f1')
        self.addCollection('group-foo', '1', ['foo'], defaultFlavor='f1')

        self.addComponent('foo:run', '2', 'f1')
        self.addCollection('foo', '2', [':run'], defaultFlavor='f1')

        self.updatePkg('foo[!f1]')
        self.updatePkg('group-foo[f1]', recurse=False)
        self.checkUpdate('foo[f1]', ['foo=[!f1]--2[f1]', 'foo:run[!f1]--2[f1]'])

    def testUpdateIneligibleCausedErase(self):
        # CNY-748 - As of 8/8/06 - because the update group-dist line causes 
        # foo to be "ineligible" - no job is created for it, which means that 
        # when foo is considered for erasure as part of the group-media update,
        # conary looks for containers that have foo in them.  But the only
        # such container is group-dist which is a weak reference, which isn't
        # counted.
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [':run'])
        self.addCollection('group-core', '1', ['foo'])
        self.addCollection('group-media', '1', ['foo'])
        self.addCollection('group-dist', '1', ['group-core'])
        self.addCollection('group-media', '2', [('foo', '1')])

        self.updatePkg(['foo', 'group-dist', 'group-media=1'], recurse=False)
        self.checkUpdate(['group-dist', 'group-media'], ['group-media'])

    def testDowngradeAlreadyDowngradedPackage(self):
        # this package has been already downgraded once (changing how
        # conary treats it internally, since it's a local modification)
        # make sure that downgrading it again also downgrades components.
        # (CNY-836).
        for v in ('1', '2', '3'):
            self.addComponent('bar:run', v, filePrimer=0)
            self.addCollection('bar', v, [':run'])
            self.addCollection('group-dist', v, ['bar'])

        self.updatePkg(['group-dist=3', 'bar=2', 'bar:run=2'], recurse=False)
        self.checkUpdate('bar=1', ['bar=2--1', 'bar:run=2--1'])

    def testTheKernelProblem(self):
        # this is an example of how adding more and more kernels
        # causes our update mechanism to fail to do the right thing.
        # eventually, an update will cause there to be an update that 
        # moves from ~!smp - smp, where the ~!smp is not part of any local 
        # update and cannot match against the ~!smp trove in the update.
        # in that case, it's like you just had a ~!smp trove installed and
        # told conary to update it to a ~smp trove.

        # NOTE: this case now passes.  The reason is a fix to the trove.diff
        # mechanism that causes newer packages to match up to newer packages
        # (this was always supposed to be the case but was not happening
        # when matching on the same branch).  It's not clear that this
        # solves "The Kernel Problem" in general but it's not immediately 
        # clear to me how to reproduce.
        for i in range(0, 4):
            v = str(i+1)
            self.addComponent('kernel:runtime', v, '~smp', filePrimer=i * 2)
            self.addCollection('kernel', v, [':runtime'], defaultFlavor='~smp')
            self.addComponent('kernel:runtime', v, '~!smp', 
                              filePrimer=i * 2 + 1)
            self.addCollection('kernel', v, [':runtime'], defaultFlavor='~!smp')
            self.addCollection('group-dist', v, [('kernel', v, '~smp', False),
                                                 ('kernel', v, '~!smp', False)])

        self.updatePkg(['kernel=1[~!smp]',
                        'kernel=2[!smp]',
                        'kernel=3[~!smp]',
                        'group-dist=3[~!smp]'])
        self.checkLocalUpdates(['group-dist=--3',
                                'kernel=3[smp]--2[!smp]',
                                'kernel=--1[!smp]'])
        self.checkUpdate('group-dist', ['group-dist=3--4',
                                        'kernel=3[!smp]--4[!smp]',
                                        'kernel:runtime=3[!smp]--4[!smp]',
                                        ])



###########################################
#
# 6. Diff trove tests
#
##########################################

    def testCannotDetermineReplacedTrove(self):
        self.addComponent('test:runtime', '1.0-1-1',
                                   fileContents = [( 'foo', 'contents1' )])
        self.addComponent('test:runtime', '1.1-1-1',
                                   fileContents = [( 'bar', 'contents1' )])
        self.addComponent('test:runtime', '/localhost@rpl:devel/1.2-1-1/branch/0.5-1-1',
                                   fileContents = [( 'foo', 'contents2' )])
        
        self.updatePkg(self.rootDir, 'test:runtime', version = '1.0-1-1')
        self.updatePkg(self.rootDir, 'test:runtime', version = '1.1-1-1',
                       keepExisting=True)

        self.checkUpdate('test:runtime=:branch',
                         [ 'test:runtime=1.0--0.5' ])


    def testOutdateRightTroveOnGroupMove(self):
        # create foo:runtime foo and group-a
        # group-a=1.0-1-1
        #  `- foo=1.0-1-1
        #     `- foo:runtime=1.0-1-1
        # foo=:branch/3.0

        v1 = '/localhost@rpl:devel//rpl1/1.0-1-1'
        v2 = '/localhost@rpl:devel//1/2.0-1-1'
        v3 = '/localhost@rpl:devel/4.0-1-1/branch/3.0-1-1'

        # upgrade to group-a on new branch, foo=:branch should be left alone
        self.addComponent('foo:runtime', v1)
        self.addComponent('foo:runtime', v3, filePrimer=3)

        self.addCollection("foo", v1, [ "foo:runtime" ])
        self.addCollection("group-a", v1, [ "foo" ])

        self.updatePkg("group-a=" + v1)
        # install second foo:runtime
        self.updatePkg("foo:runtime=%s" % v3, keepExisting=True)
        
        # create
        # group-a=:branch2/2.0-1-1
        #   `- group-b=:branch2/2.0-1-1
        #      `- foo=:branch2/2.0-1-1
        self.addComponent('foo:runtime', v2)
        self.addCollection("foo", v2, [ "foo:runtime" ])
        self.addCollection("group-b", v2, [ "foo" ])
        self.addCollection("group-a", v2, [ "group-b" ])

        # update group-a.  foo should just update from v1 to v2, since that's
        # the cleanest update.
        self.checkUpdate('group-a=%s' % v2,
                         ['foo:runtime=:rpl1/1.0--:1/2.0',
                          'foo=:rpl1/1.0--:1/2.0',
                          'group-b=--:1/2.0-1-1',
                          'group-a=:rpl1/1.0-1-1--:1/2.0-1-1',
                         ])

    def testTwoFlavorsOfTroveWithUpdates(self):
        # This test exercizes the flavor matching code of
        # trove.diff.  Before, for each flavor that was being updated
        # for kernel, trove.diff would take the first old kernel flavor
        # and link the two.  ~!kernel.smp satisfies ~kernel.smp, trove.diff
        # would in certain circumstances say that the user was updating
        # a trove from ~!kernel.smp v1 to ~kernel.smp v2.  That caused
        # three way merging to fail.
        db = database.Database(self.rootDir, self.cfg.dbPath)
        nosmp = '~!kernel.smp'
        nosmp2 = '~!kernel.smp, ~!kernel.debugdata'
        smp = '~kernel.smp'
        smp2 = '~kernel.smp, ~!kernel.debugdata'
        v = '1.0-1-1'
        self.addComponent('kernel:runtime', v, smp,
                                   fileContents=[('/smpv1', 'foo')])
        self.addComponent('kernel:runtime', v, nosmp,
                                   fileContents=[('/nosmpv1', 'foo')])
        self.addCollection('group-dist', v,
                                    [ ('kernel:runtime', v, smp),
                                      ('kernel:runtime', v, nosmp)])
        # install group-dist
        self.updatePkg(self.rootDir, 'group-dist')
        v2 = '2.0-1-1'
        # version 2 of the kernel has a slightly changed flavor 
        # - adds flag ~!kernel.debugdata to smp and not smp.
        # The extra flag causes the 'exact flavor match' short circuiting
        # in trove.diff to be passed over
        self.addComponent('kernel:runtime', v2, smp2,
                                   fileContents=[('/smpv2', 'foo')])
        self.addComponent('kernel:runtime', v2, nosmp2,
                                   fileContents=[('/nosmpv2', 'foo')])

        # update kernel.smp to the later version - this means
        # that while pristine trove diff still has to match up 
        # two flavors of kernel:runtime, the local one only
        # has to match up !kernel.smp.  When the local 
        # trove diff matches the kernel updates right and 
        # and the pristine diff doesn't, the bug is caught
        # in the twm by assert(newOverlap is None)
        self.updatePkg(self.rootDir, 'kernel:runtime', flavor='kernel.smp')

        self.addCollection('group-dist', v2,
                                    [ ('kernel:runtime', v2, smp2),
                                      ('kernel:runtime', v2, nosmp2)])
        self.updatePkg(self.rootDir, 'group-dist')

    def testTooManyUpdateChoices(self):
        # create test:runtime :linux/1.0-1-1 
        # create group-test=1.0-1-1 which contains test:runtime=1
        # create group-machine=1.0-1-1 which contains test:runtime=1
        # create group-os=1.0-1-1 which contains group-test and 
        #    group-machine

        # install group-os

        # create test:runtime :linux/2.0-1-1 
        # create group-machine=2.0-1-1 which contains test:runtime=2
        # create test:runtime :linux/3.0-1-1 
        # create group-dist=2.0-1-1 which contains test:runtime=3

        # create group-os=2.0-1-1 which contains group-test and 
        #    group-machine

        # upgrade group-os, get friendly error message

        # XXX
        self.addComponent('test:runtime', '1.0-1-1')
        self.addCollection('group-test', '1.0-1-1',
                                    [ ('test:runtime', '1.0-1-1') ])
        self.addCollection('group-machine', '1.0-1-1',
                                    [ ('test:runtime', '1.0-1-1') ])
        self.addCollection('group-os', '1.0-1-1',
                            [ ('group-test', '1.0-1-1'),
                              ('group-machine', '1.0-1-1'),
                            ])
        self.updatePkg(self.rootDir, 'group-os')

        self.addComponent('test:runtime', '2.0-1-1')
        self.addComponent('test:runtime', '3.0-1-1')
        self.addCollection('group-test', '2.0-1-1',
                                    [ ('test:runtime', '2.0-1-1') ])
        self.addCollection('group-machine', '2.0-1-1',
                                    [ ('test:runtime', '3.0-1-1') ])
        self.addCollection('group-os', '2.0-1-1',
                            [ ('group-test', '2.0-1-1'),
                              ('group-machine', '2.0-1-1'),
                            ])

        self.logCheck(self.updatePkg, (self.rootDir, 'group-os'),
                   'error: Troves being installed appear to conflict:\n'
                   '   test:runtime -> /localhost@rpl:linux/3.0-1-1[]->/localhost@rpl:linux/2.0-1-1[]')

    def testOneUpdatesOneDoesntOnSameBranch(self):
        # 1 version of libgtop installed, one referenced
        # 1 version of libgtop:doc installed, one referenced
        # we ask to update both versions of libgtop, but we should end up 
        # with only one version of foo:doc installed, since 
        # we only had one version installed before.
        self.addComponent('libgtop:doc', '2.12')
        self.addComponent('libgtop:lib', '2.12', filePrimer=1)
        self.addCollection('libgtop', '2.12', [':doc', ':lib'])


        self.addComponent('libgtop:doc', '2.13', filePrimer=2)
        self.addComponent('libgtop:lib', '2.13', filePrimer=3)
        self.addCollection('libgtop', '2.13', [':doc', ':lib'])
        self.addComponent('libgtop:doc', '2.13-1-2', filePrimer=2)
        self.addComponent('libgtop:lib', '2.13-1-2', filePrimer=3)
        self.addCollection('libgtop', '2.13-1-2', [':doc', ':lib'])

        self.addCollection('group-dist', '1.0', [('libgtop', '2.12') ])
        self.addCollection('group-dist', '2.0', 
                                [('libgtop', '2.12'),
                                 ('libgtop', '2.13-1-2')],
                             weakRefList=[('libgtop:lib', '2.13-1-2'),
                                          ('libgtop:doc', '2.13-1-2'),
                                          ('libgtop:lib', '2.12'),
                                          ('libgtop:doc', '2.12', '', False)])
        self.updatePkg('group-dist=1.0', recurse=False)
        self.updatePkg('libgtop=2.13-1-1')

        self.checkUpdate('group-dist', ['group-dist',
                                       'libgtop=2.13--2.13',
                                       'libgtop:doc=2.13--2.13',
                                       'libgtop:lib=2.13--2.13'])



#############################################
#
# 7. Synctrove tests
#
############################################

    def testUpdateSync(self):
        v1Data = '''\
synctrove:data=1-1-1
'''
        v1All = '''\
synctrove=1-1-1
'''
        v2Data = v1Data.replace('1-1-1', '2-1-1')
        v2All = v1All.replace('1-1-1', '2-1-1')
        allTroves = '''\
synctrove=1-1-1
synctrove=2-1-1
'''


        self.buildRecipe(recipes.syncTroveRecipe1, 'SyncTrove')
        self.buildRecipe(recipes.syncTroveRecipe2, 'SyncTrove')
        self.buildRecipe(recipes.syncGroupRecipe1, 'SyncGroup')
        self.buildRecipe(recipes.syncGroupRecipe2, 'SyncGroup')
        # 1. test installing trove with --sync with no reference - 
        # should get no op.

        self.logFilter.add()
        self.updatePkg(self.rootDir, 'synctrove', self.cfg.buildLabel, 
                       sync=True)
        self.logFilter.remove()
        self.logFilter.compare('error: synctrove was not found on path localhost@rpl:linux')

        # 2. test installing trove and installing :data and :debug components
        self.updatePkg(self.rootDir, 'synctrove', '1', recurse=False)
        self.updatePkg(self.rootDir, 'synctrove:data', sync=True)
        self.updatePkg(self.rootDir, 'synctrove:debuginfo', sync=True)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        (rc, s) = self.captureOutput(query.displayTroves, db, self.cfg)
        assert(s == v1All)
        # make sure updating w/o sync works
        self.updatePkg(self.rootDir, 'synctrove:data')
        (rc, s) = self.captureOutput(query.displayTroves, db, self.cfg,
                                     ['synctrove:data'])
        assert(s == v2Data)
        # now make sure updating from some later version to syncing works
        self.updatePkg(self.rootDir, 'synctrove:data', sync=True)
        (rc, s) = self.captureOutput(query.displayTroves, db, self.cfg,
                                     ['synctrove:data'])
        assert(s == v1Data)
        # 3. erase :data and :debuginfo, install two versions of 
        # synctrove, and try updating :data, should update both copies
        self.erasePkg(self.rootDir, 'synctrove:data')
        self.erasePkg(self.rootDir, 'synctrove:debuginfo')
        self.updatePkg(self.rootDir, 'synctrove', '2', recurse=False,
                       keepExisting=True)
        self.updatePkg(self.rootDir, 'synctrove:data', sync=True)
        self.updatePkg(self.rootDir, 'synctrove:debuginfo', sync=True)
        (rc, s) = self.captureOutput(query.displayTroves, db, self.cfg)
        assert(s == allTroves)

        # 4. erase v2 of trove  and v1 components, install group1, 
        # run sync, should be null op.
        self.erasePkg(self.rootDir, 'synctrove', '2')
        self.updatePkg(self.rootDir, 'group-sync', '1')
        self.logFilter.add()
        self.updatePkg(self.rootDir, 'synctrove', sync=True)
        self.logFilter.remove()
        self.logFilter.compare('error: no new troves were found')
        (rc, s) = self.captureOutput(query.displayTroves, db, self.cfg)
        assert(s == '''\
group-sync=1-1-1
synctrove=1-1-1
''')
        # 5. move to group2, run sync, should upgrade trove and all 
        # comps.
        self.updatePkg(self.rootDir, 'group-sync', recurse=False)
        self.updatePkg(self.rootDir, 'synctrove', sync=True)
        (rc, s) = self.captureOutput(query.displayTroves, db, self.cfg)
        assert(s == '''\
group-sync=2-1-1
synctrove=2-1-1
''')
        # 6. reinstall group1 w/ keepexisting, run sync, should install
        # old version.
        self.updatePkg(self.rootDir, 'group-sync', '1', keepExisting=True, 
                        recurse=False)
        self.updatePkg(self.rootDir, 'synctrove', sync=True)
        (rc, s) = self.captureOutput(query.displayTroves, db, self.cfg)
        assert(s == '''\
group-sync=1-1-1
group-sync=2-1-1
synctrove=1-1-1
synctrove=2-1-1
''')

        # 7. run sync on group, null op.
        self.logFilter.add()
        self.updatePkg(self.rootDir, 'group-sync', sync=True)
        self.logFilter.remove()
        self.logFilter.compare('error: group-sync was not found')

    def testUpdateSyncMustResolve(self):
        dep = 'trove: prov:lib'
        self.addComponent('prov:lib', '1')
        self.addComponent('prov:lib', '2')
        self.addComponent('req:lib', '1', requires=dep)
        self.addComponent('req:lib', '2')

        self.addCollection('group-foo', '1', ['prov:lib', 'req:lib'])

        self.updatePkg('group-foo', recurse=False)
        self.checkUpdate('req:lib', ['req:lib=1', 'prov:lib=2'], 
                         sync=True, resolve=True)

    def testSyncChildrenMustResolve(self):
        self.addComponent('oldreq:runtime', '1', requires='trove: prov:lib(1)',
                          filePrimer=1)
        self.addCollection('oldreq', '1', [':runtime'])

        self.addComponent('req:runtime', '1', requires='trove: prov:lib(2)',
                          filePrimer=2)
        self.addCollection('req', '1', [':runtime'])

        self.addComponent('prov:lib', '1', provides='trove: prov:lib(1)',
                          filePrimer=3)
        self.addCollection('prov', '1', [':lib'])
        self.addComponent('prov:lib', '2', provides='trove: prov:lib(2)',
                          filePrimer=4)
        self.addCollection('prov', '2', [':lib'])


        self.updatePkg(['prov:lib=1', 'oldreq:runtime'])
        self.updatePkg(['prov', 'req'], recurse=False)

        self.logFilter.add()
        self.checkUpdate(['req', 'prov'],
                         ['req:runtime', 'prov:lib=--2'],
                         syncChildren=True, resolve=True, keepRequired = True)
        self.logFilter.remove()

    def testSyncChildrenMustResolve2(self):
        self.addComponent('req:runtime', '0')
        self.addComponent('req:runtime', '1', requires='trove: prov:lib')
        self.addCollection('req', '1', [':runtime'])

        self.addComponent('prov:lib', '1')

        self.updatePkg('req:runtime=0')
        self.updatePkg('req', recurse=False)

        self.checkUpdate(['req'],
                         ['req:runtime', 'prov:lib'],
                         syncChildren=True, resolve=True, updateOnly=True)

    def testSyncAlreadyReferenced(self):
        # We have updated foo, but not it's group, and have 
        # older components we now wish to update.  Before 
        # conary would not treat the switch from foo:run=2--1
        # as a local update, meaning foo:run would get installed
        # side-by-side.
        self.addComponent('foo:run', '1')
        self.addComponent('foo:run', '2')
        self.addCollection('foo', '1', [':run'])
        self.addCollection('foo', '2', [':run'])
        self.addCollection('group-foo', '1', ['foo'])
        self.updatePkg(['group-foo', 'foo', 'foo:run=1'], recurse=False)
        self.checkUpdate(['foo:run'], ['foo:run=1--2'])


##################################################
#
# 8. Pin tests
#
#################################################3

    def testAutoPin(self):
        unlocked = self.addComponent('test:runtime', '1.0')

        lockedrun1 = self.addComponent('test2:runtime', '1.0',
                                               filePrimer = 2)
        lockeddoc1 = self.addComponent('test2:doc', '1.0', 
                                               filePrimer = 3)
        locked1 = self.addCollection('test2', '1.0', 
                                             [':runtime', ':doc'])

        lockedrun2 = self.addComponent('test2:runtime', '2.0',
                                               filePrimer = 2)
        lockeddoc2 = self.addComponent('test2:doc', '2.0', 
                                               filePrimer = 3)
        locked2 = self.addCollection('test2', '2.0', 
                                             [':runtime', ':doc'])

        self.updatePkg(self.rootDir, "test:runtime")
        self.updatePkg(self.rootDir, "test2=1.0")
        db = database.Database(self.rootDir, self.cfg.dbPath)

        lockedVer, lockedFla = locked1.getVersion(), locked1.getFlavor()

        l = db.trovesArePinned([ ("test:runtime", unlocked.getVersion(),
                                    unlocked.getFlavor()),
                                 ("test2",         lockedVer, lockedFla),
                                 ("test2:runtime", lockedVer, lockedFla),
                                 ("test2:doc",     lockedVer, lockedFla)])

        assert(l == [ False, False, False, False ])


        self.cfg.pinTroves.addExp('test2.*')

        self.updatePkg(self.rootDir, "test2=2.0")

        lockedVer, lockedFla = locked2.getVersion(), locked2.getFlavor()

        l = db.trovesArePinned([ ("test:runtime", unlocked.getVersion(),
                                    unlocked.getFlavor()),
                                 ("test2",         lockedVer, lockedFla),
                                 ("test2:runtime", lockedVer, lockedFla),
                                 ("test2:doc",     lockedVer, lockedFla)])

                                    
        assert(l == [ False, True, True, True ])

        self.cfg.pinTroves.addExp('test.*')
        self.resetRoot()
        db = database.Database(self.rootDir, self.cfg.dbPath)

        self.updatePkg(self.rootDir, "test:runtime")

        l = db.trovesArePinned([ ("test:runtime", unlocked.getVersion(),
                                    unlocked.getFlavor())])
        assert(l == [ True ])


    def testPinCollection(self):
        # install test and test:runtime, lock test=1, then update to test=2
        # test and test:runtime should be updated to 2
        db = database.Database(self.rootDir, self.cfg.dbPath)
        t1run = self.addComponent('test:runtime', '1.0-1-1')
        t1 = self.addCollection("test", "1.0-1-1", [ "test:runtime" ])
        self.updatePkg(self.rootDir, "test")

        self.pin("test")
        l = db.trovesArePinned([(t1run.getName(), t1run.getVersion(),
                                  t1run.getFlavor()),
                                 (t1.getName(), t1.getVersion(),
                                  t1.getFlavor())])
        assert(l == [True, True])

        t2run = self.addComponent('test:runtime', '2.0-1-1')
        t2 = self.addCollection("test", "2.0-1-1", [ "test:runtime" ])
        self.logCheck(self.updatePkg, [self.rootDir, 'test'],
"""\
error: Not removing old test as part of update - it is pinned.
Therefore, the new version cannot be installed.

To upgrade test, run:
conary unpin 'test=/localhost@rpl:linux/1.0-1-1[]'
and then repeat your update command
""")

        self.resetRoot()
        self.updatePkg(self.rootDir, "test")
        self.pin("test")
        self.logCheck(self.erasePkg, [self.rootDir, 'test'], 
"""\
error: Not erasing test - it is pinned.

To erase this test, run:
conary unpin 'test=/localhost@rpl:linux/2.0-1-1[]'
conary erase 'test=/localhost@rpl:linux/2.0-1-1[]'
""")
        db = database.Database(self.rootDir, self.cfg.dbPath)
        l = db.trovesArePinned([(t2run.getName(), t2run.getVersion(),
                                  t2run.getFlavor())])
        assert(l == [True])

        # add a test:runtime that can be installed side-by-side the pinned one
        self.addComponent('test:runtime', '3.0', filePrimer=3)
        self.logCheck(self.updatePkg, [self.rootDir, 'test:runtime'], 
"""\
warning: 
Not removing old test:runtime as part of update - it is pinned.
Installing new version of test:runtime side-by-side instead.

To remove the old test:runtime, run:
conary unpin 'test:runtime=/localhost@rpl:linux/2.0-1-1[]'
conary erase 'test:runtime=/localhost@rpl:linux/2.0-1-1[]'
""")
        l = db.trovesArePinned([(t2run.getName(), t2run.getVersion(),
                                  t2run.getFlavor())])
        assert(l == [True])
        assert(len(db.trovesByName('test:runtime')) == 2)

    def testPinnedRelativeUpdate(self):
        # Pin a trove, and then do an update where not all components
        # can even possibly be part of the job.  The changeset source 
        # won't have those missing components, and thus they can't 
        # be used to determine whether the pinned trove conflicts.
        self.addCollection('foo', '1', [':run'], createComps=True)
        self.addCollection('foo', '2', [':run'], createComps=True)
        self.updatePkg(['foo=1'], recurse=False)
        self.pin('foo')
        self.logFilter.add()
        self.checkUpdate('foo', ['foo=--2'], recurse=False)
        self.logFilter.remove()

    @context('pin')
    def testAlreadyInstalledWithPinned(self):
        self.addComponent('foo:run', '1', filePrimer=1)
        self.addCollection('foo', '1', [':run'])

        for v in ('1', '2'):
            self.addComponent('bar:run', v, filePrimer=0)
            self.addCollection('bar', v, [':run'])
            self.addCollection('group-dist', v, [('bar', v), ('foo', '1')])

        self.updatePkg('group-dist=1')
        self.pin('bar')
        self.checkUpdate('group-dist', ['group-dist'])

    @context('pin')
    def testAlreadyInstalledPinned(self):
        self.addComponent('foo:run', '1', filePrimer=1)
        self.addCollection('foo', '1', [':run'])
        self.addComponent('foo:run', '2', filePrimer=1)
        self.addCollection('foo', '2', [':run'])
        self.addCollection('group-foo', '2', ['foo'])

        self.updatePkg(['group-foo=2', 'foo=1', 'foo:run=1'], recurse=False)
        self.pin('foo')
        self.assertRaises(update.UpdatePinnedTroveError, self.checkUpdate, 
                          'foo', ['foo', 'foo:run'])



##############################################
#
# 9. Relative update tests
#
##############################################

    def testRelativeUpdates(self):
        for v in (':branch1/1.0', ':branch2/1.0'):
            self.addComponent('foo:runtime', v)
            self.addComponent('foo:debuginfo', v, filePrimer=2)
            self.addCollection('foo', v, ['foo:runtime',
                                                   ('foo:debuginfo', None, None,  False)])

        self.updatePkg(self.rootDir, ['foo=:branch1'])

        self.checkUpdate(['foo=:branch1--:branch2'],
                         ['foo=:branch1--:branch2', 
                          'foo:runtime=:branch1--:branch2'])


    def testRelativeUpdateWouldInstallExisting(self):        
        # install foo:lib, then install foo --keep-existing.  
        # should skip foo:lib
        self.addComponent('foo:lib', '1')
        self.addCollection('foo', '1', [':lib'])
        self.updatePkg('foo:lib') 
        self.checkUpdate('foo=--1', ['foo'])

        # now install a second foo and update from foo=2--1
        self.addComponent('foo:lib', '2', filePrimer=2)
        self.addCollection('foo', '2', [':lib'])
        self.updatePkg('foo', keepExisting=True) 
        self.checkUpdate('foo=2--1', ['foo=2--1', 'foo:lib=2--'])

    def testUpdateAllWithLocalVer(self):
        # test an update where we're switching from a version without
        # weak references to one with - branch affinity shouldn't be 
        # ignored!
        branchedVer = '/localhost@rpl:branch/1.0-1-1'
        for ver in '1', '2', branchedVer:
            self.addComponent('foo:lib', ver)
            self.addCollection('foo', ver, [':lib'])
        self.addCollection('group-dist', '1', ['foo'], weakRefList=[])
        self.addCollection('group-dist', '2', ['foo'])

        db = self.openDatabase()
        self.updatePkg(['group-dist=1', 'foo=:branch', 'foo:lib=:branch'],
                        recurse=False)
        self.checkUpdate('group-dist', ['group-dist'])

    def testCrossFlavorUpdate(self):
        # we've installed a version of firefox of the wrong flavor, 
        # and now we're updating.  Make sure the update is successful.
        self.addComponent('firefox:runtime', '1.5.0', 'is:x86_64')
        self.addComponent('firefox:runtime', '1.5.0', 'is:x86')
        self.addCollection('firefox', '1.5.0', [':runtime'], 
                           defaultFlavor='is:x86')
        self.addCollection('firefox', '1.5.0', [':runtime'], 
                            defaultFlavor='is:x86_64')
        self.addCollection('group-dist', '1',
                            [('firefox', '1.5.0', 'is:x86_64')])
        self.addCollection('group-dist', '2',
                            [('firefox', '1.5.0', 'is:x86_64')])
        self.updatePkg(['group-dist=1[is:x86_64]',
                        'firefox=1.5.0[is:x86]',
                        'firefox:runtime=1.5.0[is:x86]'], recurse=False)
        self.checkUpdate('group-dist', ['group-dist=1--2'])

    def testCrossFlavorUpdate2(self):
        # we switched the branch and flavor of gvm, and now we're switching
        # back.  Make sure conary recognizes this as a branch switch,
        # not a fresh install!
        self.addComponent('gvm:runtime', '1', '~builddocs')
        self.addComponent('gvm:runtime', ':branch/1', '~!builddocs')
        self.addCollection('gvm', '1', [':runtime'], defaultFlavor='~builddocs')
        self.addCollection('gvm', ':branch/1', [':runtime'],
                           defaultFlavor='~!builddocs')
        self.addCollection('group-dist', '1', [('gvm', '1', '~builddocs')])
        self.updatePkg(['group-dist', 'gvm=:branch/1',
                        'gvm:runtime=:branch/1'], recurse=False)
        self.checkUpdate('gvm=:linux', ['gvm=:branch--:linux', 
                                        'gvm:runtime=:branch--:linux'])

######################################################
#
# 10. Local update tests
#
#    Test whether conary has an accurate understanding 
#    of how the user modified their system.
#
######################################################


    @context('localupdate')
    def testLocalUpdates1(self):
        # user moved from foo=:a -> foo=:b, then from foo:lib=:b -> foo:lib=:c
        for v in ':a/1', ':b/1', ':c/1':
            self.addComponent('foo:lib', v)
            self.addCollection('foo', v, [':lib'])
            self.addCollection('group-foo', v, ['foo'])

        self.updatePkg(['group-foo=:a', 'foo=:b', 'foo:lib=:c'], recurse=False)
        self.checkLocalUpdates(['group-foo=--:a', 'foo=:a--:b',
                                'foo:lib=:b--:c'])
        self.checkLocalUpdates(['group-foo=--:a', 'foo=:a--:b',
                                'foo:lib=:b--:c',
                                'foo:lib=:a--'], getImplied=True)

    @context('localupdate')
    def testLocalUpdates2(self):
        # group references two versions of kernel, smp and !smp,
        # smp is not installed
        for v in range(1, 4):
            vStr = str(v)
            self.addComponent('kernel:lib', vStr, '!smp', filePrimer=v)
            self.addCollection('kernel', vStr, [':lib'], defaultFlavor='!smp')

        self.addComponent('kernel:lib', '1', 'smp')
        self.addCollection('kernel', '1', [':lib'], defaultFlavor='smp')
        self.addCollection('group-dist', '1', [('kernel', '1', 'smp'),
                                               ('kernel', '1', '!smp')])
        self.updatePkg('group-dist', recurse=False)
        self.updatePkg('kernel=1[!smp]')
        self.checkLocalUpdates(['group-dist=--1'])

        # install a second !smp kernel
        self.updatePkg('kernel=--2')
        self.checkLocalUpdates(['group-dist=--1', 'kernel=1[smp]--2[!smp]'])

        # install a third !smp kernel
        self.updatePkg('kernel=--3')
        self.checkLocalUpdates(['group-dist=--1', 'kernel=1[smp]--3[!smp]',
                                'kernel=2[!smp]' ])

        # uninstall the expected !smp kernel, so now we have two kernel updates
        self.updatePkg('-kernel=1')
        self.checkLocalUpdates(['group-dist=--1', 'kernel=1[smp]--2[!smp]',
                                'kernel=1[!smp]--3[!smp]' ])


    @context('localupdate')
    def testLocalUpdates3(self):
        # we're only updating foo:lib
        # but foo:lib was part of a larger local update
        for v in '1', '2':
            self.addComponent('foo:lib', v)
            self.addCollection('foo', v, [':lib'])
            self.addCollection('group-foo', v, ['foo'])
            self.addCollection('group-bar', v, ['group-foo'])
        self.updatePkg('group-bar=1', recurse=False)
        self.checkUpdate('group-foo=2',
                         ['group-foo=--2', 'foo=--2', 'foo:lib=--2'], 
                         apply=True)

        self.checkLocalUpdates(['group-bar=--1', 'group-foo=1--2'], 
                                troveNames=['foo:lib'])
        self.checkLocalUpdates(['group-bar=--1', 'group-foo=2',
                                'foo=1--2', 'foo:lib=1--2'],
                                troveNames=['foo:lib'], getImplied=True)

        # make another collection reference foo:lib too
        self.addCollection('group-bam', '1', ['foo:lib'])
        self.updatePkg('group-bam', recurse=False)
        self.checkLocalUpdates(['group-bar=--1', 'group-foo=2', 'foo=1--2',
                                'foo:lib=1--2', 'group-bam=--1'],
                                troveNames=['foo:lib'], getImplied=True)

        # a different version of foo:lib
        # it's questionable whether foo:lib=2 should be an update from
        # foo:lib=1 or foo:lib=3.  group-bam says one and foo says the 
        # other.
        self.addComponent('foo:lib', '3')
        self.addCollection('group-bam', '3', ['foo:lib'])
        self.updatePkg('group-bam', recurse=False)
        self.checkLocalUpdates(['group-bar=--1', 'group-foo=2', 'foo=1--2',
                                'foo:lib=1--2', 'group-bam=--3', 'foo:lib=3--'],
                                troveNames=['foo:lib'], getImplied=True)

        # what if we've erased foo:lib?
        self.updatePkg('-foo:lib')
        self.checkLocalUpdates(['group-bar=--1', 'group-bam=--3', 
                                'group-foo=2', 'foo=1--2',
                                'foo:lib=1--', 'foo:lib=2--', 'foo:lib=3--'],
                                troveNames=['foo:lib'], getImplied=True)


    @context('localupdate')
    def testLocalUpdates4(self):
        # foo v2. is installed, foo v1. is referenced
        # foo:lib v1 and v2 are installed.  Make sure
        # we don't get a local update of foo:lib v1 -> v2
        self.addComponent('foo:lib', '1')
        self.addComponent('foo:lib', '2', filePrimer=1)
        self.addCollection('foo', '1', [':lib'])
        self.addCollection('foo', '2', [':lib'])
        self.addCollection('group-foo', '1', ['foo'])
        self.updatePkg(['group-foo', 'foo:lib=1', 'foo=2', 'foo:lib=2'],
                        recurse=False)
        self.checkLocalUpdates(['group-foo=--1', 'foo=1--2'],
                                troveNames=['foo:lib'], getImplied=True)

    @context('localupdate')
    def testLocalUpdates5(self):
        # two copies foo:lib have been installed, neither of them
        # the one expected by the parent group-foo.
        self.addComponent('foo:lib', '1')
        self.addComponent('foo:lib', '2')
        self.addComponent('foo:lib', '3', filePrimer=1)
        self.addCollection('foo', '1', [':lib'])
        self.addCollection('foo', '2', [':lib'])
        self.addCollection('foo', '3', [':lib'])
        self.addCollection('group-foo', '2', ['foo'])
        self.updatePkg(['group-foo=2', 'foo=3', 'foo:lib=3', 'foo:lib=1'],
                       recurse=False)

        self.checkLocalUpdates(['group-foo=--2', 'foo=2--3',
                                'foo:lib=2--1'], getImplied=True)
        # FIXME: this could do better - I think the optimal
        # returned value would be:
        # group-foo=--2 foo=2--3 foo:lib=--1
        # therefore, I'm skipping this test to mark it as needing improvement
        raise testhelp.SkipTestException('CNY-752 - local update determination gets it wrong here - missing foo:lib=--3 at least')
        self.checkLocalUpdates(['group-foo=--2', 'foo:lib=--1', 
                                'foo=2--3', 'foo:lib=2--3'], getImplied=True)

    @context('localupdate')
    def testLocalUpdates6(self):
        # a kernel smp/!smp arragement - make sure conary knows not to 
        # consider a local update from smp -> !smp
        for v in '1', '2':
            self.addComponent('kernel:run', v, 'smp')
            self.addComponent('kernel:run', v, '!smp')
            self.addCollection('kernel', v, [':run'], defaultFlavor='smp')
            self.addCollection('kernel', v, [':run'], defaultFlavor='!smp')

        trv = self.addCollection('group-foo', '1', [('kernel', '1', 'smp'),
                                                    ('kernel', '1', '!smp')])
        self.addCollection('group-foo', '2', [('kernel', '1', 'smp'),
                                              ('kernel', '1', '!smp')])
        self.addCollection('group-foo', '3', [('kernel', '2', 'smp'),
                                              ('kernel', '2', '!smp')])
        self.addCollection('group-bar', '1', ['group-foo'])
        self.addCollection('group-bar', '2', ['group-foo'])
        self.addCollection('group-bar', '3', ['group-foo'])

        self.addCollection('group-bam', '1', ['group-bar'])
        self.addCollection('group-bam', '2', ['group-bar'])
        self.addCollection('group-bam', '3', ['group-bar'])

        self.updatePkg(['group-foo=3', 'group-bar=2',
                        'group-bam=1', 'kernel=1[!smp]',
                        'kernel:run=1[!smp]'], recurse=False)

        self.checkLocalUpdates(['group-bam=--1', 'group-bar=1--2',
                                'group-foo=2--3', 'group-foo=1--', 
                                'kernel=1--', 'kernel:run=1--',
                                'kernel=2[!smp]--1[!smp]', 
                                'kernel:run=2[smp]--',
                                'kernel:run=2[!smp]--',
                                'kernel=2[smp]--',
                                ], 
                                getImplied=True)

    @context('localupdate')
    def testLocalUpdates7(self):
        # test when we 'switch back' a trove to an earlier version
        # we should still count that as a local update, giving the
        # strong reference
        for v in '1', '2':
            self.addComponent('foo:run', v)
            self.addCollection('foo', v, [':run'])
        self.addCollection('group-foo', '1', ['foo'])
        self.updatePkg(['group-foo', 'foo=2', 'foo:run=1'], recurse=False)
        self.checkLocalUpdates(['group-foo=--1', 'foo=1--2',
                                'foo:run=2--1' ], getImplied=True)

    @context('localupdate')
    def testLocalUpdatesWithLocalTrove(self):
        db = self.openDatabase()
        path = '%s/simpleconfig.recipe' % self.workDir
        self.writeFile(path, recipes.simpleConfig1)
        os.chdir(self.workDir)
        trv = self.cookFromRepository(path)
        self.updatePkg(self.rootDir, 'simpleconfig-1.0.ccs', recurse=False)

        self.addComponent('simpleconfig:runtime', '2')
        self.updatePkg('simpleconfig:runtime', '2')
        self.checkLocalUpdates(['simpleconfig=--1.0',
                                'simpleconfig:runtime=1.0--2' ],
                                getImplied=True)

    @context('localupdate')
    def testLocalUpdatesMultiArch(self):
        # multiarch localupdate calculation was broken, especially when
        # some troves w/ the same name were referenced only weakly, and
        # others both strongly and weakly.
        self.addComponent('foo:lib', '1', 'is:x86', filePrimer=3)
        self.addComponent('foo:data', '1', 'is:x86', filePrimer=4)
        self.addCollection('foo', '1', [':lib', ':data'],
                           defaultFlavor='is:x86')
        self.addCollection('foo', '1', [':lib', ':data'],
                           defaultFlavor='is:x86_64', createComps=True)
        self.addCollection('group-compat', '1', ['foo'],
                           defaultFlavor='is:x86',
                           weakRefList=[('foo:lib', True),
                                        ('foo:data', False)])
        self.addCollection('group-dist', '1', [('foo', '1', 'is:x86_64'),
                                               ('group-compat', '1', 'is:x86')])
        oldFlavor = self.cfg.flavor
        self.updatePkg(['group-dist[is:x86 x86_64]', 
                        'foo[is:x86_64]', 'foo:lib[is:x86_64]',
                        'foo:data[is:x86_64]', 'foo[is:x86]',
                        'foo:lib[is:x86]'], recurse=False)
        self.checkLocalUpdates(['group-dist'])

    def testUpdatingFromChangeSetWhenReposNotAvailable(self):
        for ver in (1,2,3):
            self.addComponent('foo:runtime=%s' % ver, filePrimer=ver)
            self.addCollection('foo=%s' % ver, [':runtime'])
            self.addCollection('group-foo=%s' % ver, ['foo'])
        self.updatePkg('group-foo=1')
        self.updatePkg('foo=2')
        self.changeset(self.openRepository(), 'group-foo=3', 
                       self.workDir + 'group-foo.ccs')
        self.stopRepository()
        self.updatePkg([self.workDir + 'group-foo.ccs'])

    def testUpdateFromLocalWithTwoContainingGroups(self):
        self.addComponent('foo:runtime=1')
        self.addCollection('foo=1', [':runtime'])
        self.addComponent('foo:runtime=:branch')
        self.addCollection('foo=:branch', [':runtime'])
        self.addCollection('group-dist=1', ['foo'])
        self.addCollection('group-dist-2=1', ['foo'])
        self.updatePkg(['group-dist', 'group-dist-2', 'foo=:branch', 
                         'foo:runtime=:branch'],
                        recurse=False)
        self.checkUpdate('foo=:linux', 
                        ['foo=:branch--:linux', 'foo:runtime=:branch--:linux'])

######################################################
#
# 11. Update All tests
#
#    Tests of what conary thinks should be updated
#    when someone updates their system
#
######################################################

    @context('updateall')
    def testUpdateAll(self):
        # install foo v.1 and foo:lib v2.
        # foo:lib should not show up in the update list because it 
        # is not a branch switch or a flavor change.
        self.addComponent('foo:lib', '1')
        trv = self.addCollection('foo', '1', [':lib'])
        self.addComponent('foo:lib', '2')
        self.checkUpdate(['foo=1', 'foo:lib=2'],
                         ['foo=--1', 'foo:lib=--2'],
                         recurse=False, apply=True)
        self.checkLocalUpdates(['foo=--1', 'foo:lib=1--2'])
        client = conaryclient.ConaryClient(self.cfg)
        items = client.fullUpdateItemList()
        assert(items == [('foo', None, None)])
        items = client.getUpdateItemList()
        assert(items == [trv.getNameVersionFlavor()])

    @context('updateall')
    def testUpdateAllX86_64Withx86Flavors(self):
        # make sure that updateall info for x86 troves on an x86_64 system 
        # doesn't include the x86_64 flavor (CNY-628)
        base = self.cfg.flavor[0]
        override = deps.overrideFlavor
        parseFlavor = deps.parseFlavor
        newFlavor = [override(base, parseFlavor('is:x86_64(~cmov)')), 
                     override(base, parseFlavor('is:x86_64(~cmov) x86(~cmov)'))]
        oldFlavor = self.cfg.flavor
        try:
            self.cfg.flavor = newFlavor
            self.addComponent('foo:run', '1', 'is:x86_64')
            self.addCollection('group-dist', '1', ['foo:run'],
                               defaultFlavor='is:x86_64')
            self.addComponent('foo:run', '1', 'is:x86(~foo)', filePrimer=1)
            self.updatePkg(['foo:run[is:x86(foo)]', 'group-dist[is:x86_64]'])
            client = conaryclient.ConaryClient(self.cfg)
            items = client.fullUpdateItemList()
            fooFlavor = [ x for x in items if x[0] == 'foo:run'][0][2]
            assert(fooFlavor.stronglySatisfies(deps.parseFlavor('is:x86(foo)')))
            assert(not fooFlavor.satisfies(deps.parseFlavor('is:x86_64')))
        finally:
            self.cfg.flavor = oldFlavor

    @context('updateall')
    @context('pin')
    def testUpdateAllPinned(self):
        self.addComponent('foo:lib', '1')
        self.updatePkg('foo:lib')
        self.pin('foo:lib')
        client = conaryclient.ConaryClient(self.cfg)
        assert(client.fullUpdateItemList() == [])

    @context('pin')
    def testUpdateAllTrovePinnedAndUnpinned(self):
        # See CNY-661 - the foo:lib hits both the checkPrimaryPins=False
        # check for updateall and is also part of a recursive update,
        # so checkPrimaryPins=True
        self.addComponent('foo:lib', '1')
        self.addComponent('foo:lib', '2', filePrimer=1)
        self.addComponent('foo:lib', '3', filePrimer=2)
        self.addComponent('foo:lib', '4', filePrimer=3)
        self.addCollection('foo', '3', [':lib'])
        self.addCollection('foo', '4', [':lib'])

        self.checkUpdate(['foo=3', 'foo:lib=1', 'foo:lib=2'],
                         ['foo=3', 'foo:lib=1', 'foo:lib=2', 'foo:lib=3'],
                         apply=True)
        self.checkLocalUpdates([], ['foo:lib=--1', 'foo:lib=--2', 'foo=--3'])
        self.pin('foo:lib=3')
        raise testhelp.SkipTestException('CNY-661 - Test Fails due to double update')
        self.checkUpdate(['foo', 'foo:lib'], ['foo:lib=3--4', 'foo=3--4'],
                         checkPrimaryPins=False)


    @context('pin')
    def testPinnedAndWeakrefedGroupUpdate(self):
        # CNY-682
        for primer, v in enumerate(('1.0-1-1', '2.0-1-1')):
            self.addComponent('test:runtime', v, filePrimer=primer)
            self.addComponent('test:devel', v, filePrimer=primer)
            self.addCollection('test', v, ['test:runtime', 'test:devel'])
            # add test:runtime to group-core byDefault=False
            self.addCollection('group-core', v, [('test', False)],
                               weakRefList=[('test:runtime', True),
                                            ('test:devel', False)])
            self.addCollection('group-base', v, ['test'],
                               weakRefList=[('test:runtime', True),
                                            ('test:devel', True)])
            self.addCollection('group-dist', v, ['group-core', 'group-base'],
                               weakRefList=[('test', True),
                                            ('test:runtime', True),
                                            ('test:devel', True)])
        self.updatePkg(self.rootDir, 'group-core', version='1.0-1-1')
        self.updatePkg(self.rootDir, 'test:runtime', version='1.0-1-1')
        self.updatePkg(self.rootDir, 'test', version='1.0-1-1',
                       recurse=False)
        self.updatePkg(self.rootDir, 'group-dist', version='1.0-1-1',
                       recurse=False)
        self.pin('test')
        self.pin('test:runtime')
        # currently this is installing test:devel
        self.checkUpdate('group-dist', ['test:runtime=2.0-1-1',
                                        'test=2.0-1-1',
                                        'group-core=1.0-1-1--2.0-1-1',
                                        'group-dist=1.0-1-1--2.0-1-1',
                                        ])
        self.updatePkg(self.rootDir, 'group-dist')

#############################################
#
# 12. Migrate tests
#
#############################################


    @context('migrate')
    def testMigrate(self):
        # this to test:
        # multiple versions of something installed, one update avail
        self.addCollection('foo', '1', [':data',
                                        (':debuginfo', False),
                                        ':lib',
                                        ':java',
                                        (':perl', False),
                                        (':python', False),
                                        ':toerase', # erased in new version
                                        ':toerase2', # erased in new version
                                        ],
                           createComps=True)
        self.addCollection('foo', '2', [(':data', False), # switched to false
                                        (':debuginfo', False),
                                        ':lib',
                                        ':perl', # switched to true
                                        ':java',
                                        (':python', False), # stays false, but
                                                            # is installed.
                                        ':runtime', # new
                                        ],
                                        createComps=True)
        self.addComponent('foo:runtime', ':branch/1', filePrimer=99)
        self.addComponent('foo:toerase', ':branch/1', filePrimer=100)

        self.updatePkg(['foo=1', 
                        'foo:data=1',
                        'foo:runtime=:branch',
                        'foo:python=1',
                        'foo:toerase=:branch',
                        'foo:toerase2=1',
                        'foo:lib=1',
                        ], recurse=False)

        oldMigrate = {'removeNotByDefault' : True,
                      'syncUpdate'         : True }

        # old migrate behavior:
        # note that we don't erase foo:toerase, because the linkage
        # was broken because the update was across a branch.  Sad.
        self.checkUpdate('foo', ['foo=1--2',
                                 'foo:data=1--',
                                 'foo:lib=1--2',
                                 'foo:perl=--2',
                                 'foo:java=--2',
                                 'foo:python=1--',  # trove is
                                                    # byDefault false
                                 'foo:runtime=1--2',
                                 'foo:toerase2=1--',
                                 #'foo:toerase=1--'
                                 ],
                                 **oldMigrate)


        # with the new migrate.  We erase the toerase troves,
        # but we _keep_ foo:python because it was byDefault False
        # in the old version and is byDefault False in the new version
        self.checkUpdate('foo', ['foo=1--2',
                                 'foo:data=1--',
                                 'foo:lib=1--2',
                                 'foo:perl=--2',
                                 'foo:java=--2',
                                 'foo:python=1--', # trove has
                                                   # byDefault false
                                 'foo:runtime=:branch--2',
                                 'foo:toerase2=1--',
                                 'foo:toerase=1--'],
                                 migrate=True)

        # old migrate behavior
        # now let's do an update where we're foo is already at 2.
        # in this case we don't erase foo:toerase{1,2} - how could we,
        # we have no links to them!
        self.updatePkg(['foo=2'], recurse=False)
        self.checkUpdate('foo', ['foo:data=1--',
                                 'foo:lib=1--2',
                                 'foo:perl=--2',
                                 'foo:java=--2',
                                 'foo:python=1--',
                                 'foo:runtime=:branch--2'],
                                 **oldMigrate)

        # new migrate behavior removes the offending 
        # erase troves.
        self.checkUpdate('foo', ['foo:data=1--',
                                 'foo:lib=1--2',
                                 'foo:perl=--2',
                                 'foo:java=--2',
                                 'foo:python=1--',
                                 'foo:runtime=:branch--2',
                                 'foo:toerase=1--',
                                 'foo:toerase2=1--'],
                                 migrate=True, apply=True)

    @context('migrate')
    def testMigrateKernelGroup(self):
        self.addComponent('bash:run')
        self.addComponent('bash:debuginfo', filePrimer=1)
        self.addCollection('bash', [':run', (':debuginfo', False)])
        self.addComponent('foo:devel', filePrimer=2)
        self.addComponent('kernel:runtime', filePrimer=3)
        self.addCollection('kernel', [':runtime'])
        self.addCollection('group-core', ['bash'])
        self.addCollection('group-devel', ['foo:devel'])
        self.addCollection('group-dist', [('kernel', False),
                                          ('group-devel', False),
                                          'group-core'])
        self.updatePkg(['group-dist', 'group-devel', 'kernel', 
                        'bash:debuginfo'], raiseError=True)
        self.checkUpdate('group-dist', ['bash:debuginfo=1.0--'],
                         migrate=True)

    @context('migrate')
    def testMigrateMultipleVersions(self):
        # we have version 1 and version 2 of foo installed.
        # migrate in this case will just 
        for i, ver in enumerate(['1', '2', '3', ':branch/1']):
            self.addComponent('foo:lib', ver, filePrimer=i)
            self.addComponent('foo:data', ver, filePrimer=i)
            self.addCollection('foo', ver, [':lib', ':data'])

        self.updatePkg(['foo=1', 'foo:lib=2', 'foo=2'], recurse=False)
        # old migrate doesn't touch 
        self.checkUpdate('foo', ['foo=2--3', 
                                 'foo:lib=2--3',
                                 'foo:data=--3'], oldMigrate=True)
        self.checkUpdate('foo', ['foo=2--3', 
                                 'foo:lib=2--3',
                                 'foo:data=--3',
                                 'foo=1--'], migrate=True)

        self.resetRoot()

        self.updatePkg(['foo=2', 'foo:lib=2', 
                        'foo=:branch'], recurse=False)

        # old migrate behavior (now sync --full)
        self.checkUpdate(['foo'], ['foo=2--3',
                                   'foo:lib=2--3',
                                   'foo:data=--3',
                                   'foo:lib=--:branch',
                                   'foo:data=--:branch',
                                   ], oldMigrate=True)
        # new migrate uses branch affinity, and discovers both the
        # :branch and :linux one.
        self.checkUpdate(['foo'], ['foo=2--3',
                                   'foo:lib=2--3',
                                   'foo:data=--3',
                                   'foo:lib=--:branch',
                                   'foo:data=--:branch'], migrate=True)


    @context('migrate')
    def testMigrateGroups(self):
        # make sure weak references win when migrating a group.
        self.addComponent('foo:lib', '1')
        self.addComponent('foo:debuginfo', '1')
        self.addCollection('foo', '1', [':lib', (':debuginfo', False)])
        self.addCollection('group-foo', '1', ['foo'],
                           weakRefList=[('foo:lib', False),
                                        ('foo:debuginfo', True)])
        self.updatePkg(['group-foo', 'foo:lib'], recurse=False)
        self.checkUpdate(['group-foo'], ['foo', 'foo:debuginfo',
                                         'foo:lib=1--'], migrate=True)
        self.checkUpdate(['group-foo'], ['foo', 'foo:debuginfo',
                                         'foo:lib=1--'], oldMigrate=True)
        self.addCollection('group-foo', '2', [('foo', '1')],
                           weakRefList=[('foo:lib', '1', '', True),
                                        ('foo:debuginfo', '1', '', True)])
        self.checkUpdate(['group-foo'], ['group-foo', 'foo',
                                         'foo:debuginfo'], migrate=True,
                                         apply=True)
        self.addCollection('group-foo', '3', [('foo', '1')],
                           weakRefList=[('foo:lib', '1', '', False),
                                        ('foo:debuginfo', '1', '', True)])
        self.checkUpdate(['group-foo'], ['group-foo',
                                         'foo:lib=1--'], migrate=True,
                          apply=True)


    @context('migrate')
    def testMigrateInstalledSwitchesToNotByDefault(self):
        self.addComponent('foo:lib', '1')
        self.addComponent('foo:doc', '1', filePrimer=2)
        self.addCollection('foo', '1', [':lib', ':doc'])

        self.addCollection('group-foo', '1', ['foo'])

        self.addCollection('group-bar', '1', ['foo'],
                            weakRefList=[('foo:lib', True),
                                         ('foo:doc', False)])
        self.addCollection('group-bar2', '1', ['foo'],
                            weakRefList=[('foo:lib', True),
                                         ('foo:doc', False)])
        self.addCollection('group-os2', '1', ['group-bar', 'group-bar2'])

        self.checkUpdate('group-foo=1', 
                         ['group-foo', 'foo', 'foo:lib', 'foo:doc'], apply=True)

        #old behavior: you had to specify -group-foo
        self.checkUpdate(['-group-foo', 'group-os2'], 
                         ['group-os2', 'group-bar', 'group-bar2', 
                          'group-foo=1--', 'foo:doc=1--'],
                           oldMigrate=True)

        #new behavior: -group-foo is implicit
        self.checkUpdate(['group-os2'], 
                         ['group-os2', 'group-bar', 'group-bar2', 
                          'group-foo=1--', 'foo:doc=1--'],
                           migrate=True)

    @context('migrate')
    @context('pin')
    def testMigratePinned(self):
        # new test for new behavior, make sure it workes correctly
        # with pins.
        for i in range(2):
            primer = i * 4
            ver = str(i+1)
            self.addComponent('kernel:lib', ver, '~smp', 
                              filePrimer=primer + 0)
            self.addComponent('kernel:doc', ver, '~smp',  
                              filePrimer=primer + 1)
            self.addCollection('kernel', ver, [':lib', ':doc'], 
                               defaultFlavor='~smp')

            self.addComponent('kernel:lib', ver, '~!smp', 
                              filePrimer=primer + 2)
            self.addComponent('kernel:doc', ver, '~!smp', 
                              filePrimer=primer + 3)
            self.addCollection('kernel', ver, [':lib', ':doc'], 
                               defaultFlavor='~!smp')

        self.addCollection('group-foo', '1', [('kernel', '1', '~smp', False),
                                              ('kernel', '1', '~!smp', False)])
        self.addCollection('group-foo', '2', [('kernel', '2', '~smp', False),
                                              ('kernel', '2', '~!smp', False)])
        self.updatePkg(['group-foo=1', 'kernel=1[!smp]', 
                                       'kernel:lib=1[!smp]'], recurse=False,
                                                              raiseError=True)
        self.checkUpdate('group-foo', ['group-foo=1--2', 
                                       'kernel=1[!smp]--2',
                                       'kernel:lib=1[!smp]--2'], 
                                       migrate=True)
        self.pin('kernel')
        self.checkUpdate('group-foo', ['group-foo=1--2', 
                                       'kernel=--2[!smp]',
                                       'kernel:lib=--2[!smp]'], migrate=True)

        self.resetRoot()
        self.updatePkg(['group-foo=1', 'kernel=1[!smp]', 
                                       'kernel:lib=1[!smp]'], recurse=False,
                                                              raiseError=True)

        self.checkUpdate('group-foo', ['group-foo=1--2', 
                                       'kernel=1[!smp]--2',
                                       'kernel:lib=1[!smp]--2'], 
                                       migrate=True, apply=True)
        self.assertRaises(update.NoNewTrovesError,
            self.checkUpdate, 'group-foo', [], migrate=True, apply=True)


    @context('migrate')
    @context('pin')
    def testMigratePinnedErase(self):
        # CNY-680 - Pinned erases were not being respected by migrate
        self.addComponent('foo:run', '1')
        self.addComponent('foo:lib', '1', filePrimer=1)
        self.addCollection('foo', '1', [':run', ':lib'])

        self.addComponent('foo:run', '2')
        self.addCollection('foo', '2', [':run'])

        self.updatePkg('foo=1')
        self.pin('foo:lib')
        self.checkUpdate('foo', ['foo=1--2', 'foo:run=1--2'], migrate=True)

    @context('migrate')
    @context('redirect')
    def testMigrateRedirects(self):
        # CNY-722 - Make redirects work with groups
        self.addComponent('foo:run', '1')
        self.addComponent('foo:run', '2', redirect=['bar:run'])
        self.addComponent('bar:run', '1')

        self.updatePkg('foo:run=1')
        self.checkUpdate(['foo:run'], ['bar:run', 'foo:run=1--'], migrate=True)

    @context('migrate')
    @context('erase')
    @context('redirect')
    def testMigrateEraseRedirects(self):
        self.addComponent('foo:run', '1')
        self.addComponent('foo:run', '2', redirect=[])
        self.addComponent('bar:run', '1')
        self.updatePkg('foo:run=1')
        try:
            self.checkUpdate(['foo:run'], [], migrate=True)
        except conaryclient.UpdateError, err:
            assert(str(err) == 'Cannot migrate to redirect(s), as they are all erases - \nfoo:run=/localhost@rpl:linux/2-1-1[]')

        self.checkUpdate(['foo:run', 'bar:run'], ['bar:run', 'foo:run=--'], 
                         migrate=True)

    @context('migrate')
    @context('redirect')
    def testMigrateRecursiveRedirect(self):
        self.addComponent('foo:run', '1')
        self.addComponent('foo:run', '2', redirect=['bar:run'])
        self.addComponent('bar:run', '1', redirect=['baz:run', 'bam:run'])
        self.addComponent('baz:run', '1',  filePrimer=1)
        self.addComponent('bam:run', '1',  filePrimer=2)

        self.updatePkg('foo:run=1')
        self.checkUpdate(['foo:run'], ['baz:run', 'bam:run', 'foo:run=1--'],
                         migrate=True)
        self.addComponent('baz:run', '2', redirect=['bam:run'])

        self.checkUpdate(['foo:run'], ['bam:run', 'foo:run=1--'], migrate=True)

        self.addComponent('baz:run', '3', redirect=['foo:run'])

        try:
            self.checkUpdate(['foo:run'], ['foo:run=1--'], migrate=True)
        except conaryclient.UpdateError, err:
            assert(str(err) == 'Redirect Loop detected - includes foo:run=/localhost@rpl:linux/2-1-1[] and bar:run=/localhost@rpl:linux/1-1-1[]')

        self.addComponent('baz:run', '4', redirect=['bar:run'])
        try:
            self.checkUpdate(['foo:run'], ['foo:run=1--'], migrate=True)
        except conaryclient.UpdateError, err:
            assert(str(err) == 'Redirect Loop detected - includes bar:run=/localhost@rpl:linux/1-1-1[] and baz:run=/localhost@rpl:linux/4-1-1[]')

        self.addComponent('foo:run', '3', redirect=['foo:run'])
        try:
            self.checkUpdate(['foo:run'], ['foo:run=1--'], migrate=True)
        except conaryclient.UpdateError, err:
            assert(str(err) == 'Redirect Loop detected - trove foo:run=/localhost@rpl:linux/3-1-1[] redirects to itself')

    @context('migrate')
    @context('redirect')
    def testMigrateRedirectNotByDefaultBehavior(self):
        # we have a redirect where a component once was not by default.
        # in the new trove it should remain installed.
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [(':run', False)])
        self.addComponent('foo:run', '2')
        self.addCollection('foo', '2', ['foo:run'], redirect=['group-bar'])

        self.addComponent('bar:run', '1')
        self.addCollection('group-bar', '1', [('foo:run', False)])

        self.updatePkg(['foo:run=1', 'foo=1'])
        self.checkUpdate(['foo'], ['foo=1--', 'group-bar']) # foo:run stays installed

    @context('migrate')
    def testMigrateConfigFiles(self):
        self.addComponent('foo:run', '1', '', [('/etc/config', 'config1\n')])
        self.addComponent('foo:run', '2', '', [('/etc/config', 'config2\n')])
        f = self.cfg.root + '/etc/config'
        self.updatePkg('foo:run=1')
        self.verifyFile(f, 'config1\n')
        self.writeFile(f, 'config1\nconfig3\n')
        self.checkUpdate(['foo:run'], ['foo:run'], migrate=True, apply=True)
        self.verifyFile(f, 'config2\nconfig3\n')

    @context('migrate')
    def testMigrateFromChangesets(self):
        # Test a migrate with the trove present both in the repository and in
        # the supplied changeset

        # Paths to the changesets we create
        changesets = []

        repos = self.openRepository()
        groupTroveName = "group-dummy"

        for v in range(2):
            v = str(v + 1)

            self.addComponent('footrove:runtime', v)
            self.addCollection('footrove', v, [':runtime'])
            self.addCollection(groupTroveName, v, ['footrove'])

            csPath = self.workDir + '/gd%s.ccs' % v
            self.changeset(repos, [groupTroveName], csPath)
            changesets.append(csPath)

        # Expected troves
        expTroves = [groupTroveName, 'footrove', 'footrove:runtime']

        # May not need to specify the version
        trvset = [ "%s=1" % groupTroveName ]
        expected = [ x + '=1' for x in expTroves ]

        # Install the first group
        self.checkUpdate(trvset, expected, apply=True)

        trvset = [ "%s=2" % groupTroveName ]
        expected = [ x + '=2' for x in expTroves ]

        # Migrate to the second group from a changeset
        cs = changeset.ChangeSetFromFile(changesets[1])
        # This should not fail with a NameError: DuplicateTrove not defined
        # error after CNY-1039 is fixed
        self.checkUpdate(trvset, expected, migrate=True, apply=True,
                         fromChangesets=[cs])

    @context('migrate')
    def testMigrateNothing(self):
        # CNY-1246
        self.addComponent('foo:runtime', '1')
        self.updatePkg('foo:runtime')
        self.assertRaises(update.NoNewTrovesError,
            self.checkUpdate, ['foo:runtime'], ['foo:runtime'], migrate = True)

    @context('migrate')
    def testMigrateNoRecurse(self):
        # Test a migrate when we specify exactly what is to be migrated to.
        self.addComponent('foo:runtime', '1')
        self.addComponent('foo:debuginfo', '1', filePrimer=1)
        self.addComponent('foo:lib', '1', filePrimer=2)
        self.addCollection('foo', '1', [':runtime', (':debuginfo', False),
                                        ':lib'])

        self.checkUpdate(['foo', 'foo:lib', 'foo:debuginfo'],
                         ['foo', 'foo:lib', 'foo:debuginfo'],
                         migrate=True, apply=True, recurse=False)


#######################################################
#
# 13. Path hash match tests
#
#    Test of path hash match's effect on updates.
#
######################################################


    @context('pathhashmatch')
    def testPathHashDiffMatching(self):
        # functional tests of path hash matching on diff
        gnome1Branch = '/localhost@rpl:linux//gnome1'
        relBranch = '/localhost@rpl:linux//1'
        devBranch = '/localhost@rpl:linux'
        self.addComponent('glib:lib', relBranch + '/1.0-1-1', ['/glib2'])
        self.addCollection('glib', relBranch + '/1.0-1-1', [':lib'])
        self.addComponent('glib:lib', gnome1Branch + '/1.0-1-1', ['/glib1'])
        self.addCollection('glib', gnome1Branch + '/1.0-1-1', [':lib'])
        self.addComponent('glib:lib', devBranch + '/1.0-1-1', ['/glib2'])
        self.addCollection('glib', devBranch + '/1.0-1-1', [':lib'])

        self.checkUpdate(['glib=' + relBranch, 'glib='  + gnome1Branch],
                         ['glib{,:lib}=--' + relBranch,
                          'glib{,:lib}=--' + gnome1Branch],
                          apply=True)

        self.checkUpdate('glib=:linux', ['glib{,:lib}=:1--:linux'])

        # branch affinity overrides path matching, (branch distance + flavor
        # don't)
        self.addComponent('glib:lib', relBranch + '/2.0-1-1', ['/glib3'])
        self.addCollection('glib', relBranch + '/2.0-1-1', [':lib'])
        self.checkUpdate(['glib=:linux', 'glib=:1', 'glib=:gnome1'],
                         ['glib{,:lib}=:1--:1',
                          'glib{,:lib}=--:linux'])

    @context('pathhashmatch')
    def testPathHashDiffPackagesRemain(self):
        # if two components from the same package go to different places
        # or come from different places, we leave the packages around
        gnome1Branch = '/localhost@rpl:linux//gnome1'
        relBranch = '/localhost@rpl:linux//1'
        devBranch = '/localhost@rpl:linux'
        for branch in gnome1Branch, relBranch, devBranch:
            self.addComponent('glib:lib', branch + '/1.0-1-1', [('/lib', branch)])
            self.addComponent('glib:runtime', branch + '/1.0-1-1', [('/runtime', branch)])
            self.addCollection('glib', branch + '/1.0-1-1', [':lib', ':runtime'])
        # test 1: move from two installed components from different packages
        # to one package.
        self.updatePkg(['glib:lib=' + relBranch, 'glib=' + relBranch,
                        'glib:runtime=' + gnome1Branch, 'glib=' + gnome1Branch,
                       ], recurse=False, raiseError=True)
        # note the old packages are left installed
        self.checkUpdate('glib=' + devBranch,
                         ['glib{,:lib}=:1--:linux', 
                          'glib:runtime=:gnome1--:linux' ])

        self.resetRoot()
        self.updatePkg(['glib=' + devBranch])

        # XXX this leaves an empty shell of glib=:linux around.

        self.checkUpdate(['glib:lib=' + relBranch, 'glib=' + relBranch,
                          'glib:runtime=' + gnome1Branch, 
                          'glib=' + gnome1Branch, ],
                         ['glib{,:lib}=:linux--:1',
                          'glib=--:gnome1',
                          'glib:runtime=:linux--:gnome1',],
                          recurse=False, apply=True)
        raise testhelp.SkipTestException, 'Should leave old glib around'

    @context('pathhashmatch')
    def testPathHashDiffPackagesRemain2(self):
        # if two components from the same package go to different places
        # or come from different places, we leave the packages around
        # - note that in this case that results in glib:data=relBranch 
        # remaining behind, even though there is a potential branch switch
        # for glib.
        gnome1Branch = '/localhost@rpl:linux//gnome1'
        relBranch = '/localhost@rpl:linux//1'
        devBranch = '/localhost@rpl:linux'
        for branch in gnome1Branch, relBranch, devBranch:
            self.addComponent('glib:lib', branch + '/1.0-1-1', ['/lib'])
            self.addComponent('glib:runtime', branch + '/1.0-1-1', ['/runtime'])
            if branch is devBranch:
                self.addComponent('glib:data', branch + '/1.0-1-1', ['/data'])
                self.addCollection('glib', branch + '/1.0-1-1', [':lib',
                                                                ':runtime',
                                                                 ':data'])
            else:
                self.addCollection('glib', branch + '/1.0-1-1', [':lib',
                                                                ':runtime'])

        self.updatePkg('glib=' + devBranch)
        self.checkUpdate(['glib:lib=' + relBranch, 'glib=' + relBranch,
                        'glib:runtime=' + gnome1Branch, 'glib=' + gnome1Branch,
                         ],
                         ['glib{,:lib}=:linux--:1',
                          'glib:runtime=:linux--:gnome1',
                          'glib=--:gnome1' ],
                         recurse=False)
        raise testhelp.SkipTestException, 'Should leave old glib around'

    @context('pathhashmatch')
    def testBranchAffinityAndPathHash(self):
        # an upgrade on the main branch switches all paths (from 2.4 to 2.5)
        # an uninstalled version on another branch has the old paths.
        self.addComponent('python:lib', '2.4', ['/usr/bin/python2.4'])
        self.addComponent('python:lib', '2.5', ['/usr/bin/python2.5'])
        self.addComponent('python:lib', ':compat/2.4-1-1',
                                                     ['/usr/bin/python2.4'])
        self.addComponent('python:lib', ':compat/2.4-1-2',
                                                     ['/usr/bin/python2.4'])

        self.addCollection('python', '2.4', [':lib'])
        self.addCollection('python', '2.5', [':lib'])
        self.addCollection('python', ':compat/2.4-1-1', [':lib'])
        self.addCollection('python', ':compat/2.4-1-2', [':lib'])

        self.addCollection('group-python', '1.0', [('python', '2.4')])
        self.addCollection('group-python', '2.0', [('python', '2.5')])
        self.addCollection('group-python', ':compat/1.0',
                           [('python', ':compat/2.4-1-1')])
        self.addCollection('group-python', ':compat/1.1',
                           [('python', ':compat/2.4-1-2')])

        self.updatePkg(['group-python=:compat/1.0',
                        'group-python=1.0', 'python=2.4', 'python:lib=2.4'],
                        raiseError=True, recurse=False)
        self.checkUpdate('group-python', ['python{,:lib}=2.4--2.5',
                                          'group-python=:compat--:compat',
                                          'group-python=:linux--:linux'])
 
    @context('pathhashmatch')
    def testBranchAffinityAndPathHash2(self):
        # uninstalled foo on the main label
        # foo on the other label is installed.
        # a conary update of foo on the main label will cause a path conflict.
        # (This is probably okay).
        self.addComponent('foo:lib', '1', ['/foo0'])
        self.addComponent('foo:lib', '2', [('/foo1', 'a')])
        self.addComponent('foo:lib', ':other/1', [('/foo1', 'b')])

        self.updatePkg(['foo:lib=1', 'foo:lib=:other'], raiseError=True)
        # applying this causes a conflict - even though we _could_ do
        # an update from foo:lib=:other --> :linux
        self.assertRaises(errors.ConaryError, self.checkUpdate,
                          'foo:lib=2', ['foo:lib=:linux/1--:linux/2'],
                          apply=True)

    @context('pathhashmatch')
    def testBranchAffinityAndPathHash3(self):
        # 2 foos installed on the main label
        # two updates done, one on the main label one on another label.
        # foo:lib=2--2.1 is an obious match, but at the time we're deciding
        # whether to match troves by branch affinity or path hashes, 
        # foo:lib=1 _could_ have been a branch affinity update, it just 
        # happens not to be.  Because of that we put it back in the pool
        # of general, non-branch affinity updates and find that its path
        # matches foo:lib=/compat1.1.
        self.addComponent('foo:lib', '1', [('/foo1', 'a')])
        self.addComponent('foo:lib', '2', ['/foo2'])
        self.addComponent('foo:lib', ':compat2/0', ['/foo0'])
        self.addComponent('foo:lib', ':compat/1.1', [('/foo1', 'b')])

        self.updatePkg(['foo:lib=1', 'foo:lib=2', 'foo:lib=:compat2'],
                        raiseError=True)

        self.addComponent('foo:lib', '2.1', ['/foo2'])

        # this apply get it right - foo:lib=1 couldn't possibly match up
        # with anything else on its branch (all matches on its
        # branch are already taken), so it searches via paths against
        # the other things that don't have path hash matches
        self.checkUpdate(['foo:lib=:linux', 'foo:lib=:compat/1.1'],
                         ['foo:lib=2--2.1',
                          'foo:lib=1--:compat/1.1'],
                          apply=True)

    @context('pathhashmatch')
    def testBranchAffinityAndPathHash4(self):
        # this is like case 3 except that this time the match up between
        # 2 and 2.2 isn't obvious, because they don't share any paths.
        # Instead, we _delay_ the matchup of 1, because it has a potential
        # matchup with :compat/1.1.  This only works because there's not
        # another potential matchup 1 on its branch.
        self.addComponent('foo:lib', '1', [('/foo1', 'a')])
        self.addComponent('foo:lib', '2', ['/foo2'])
        self.addComponent('foo:lib', ':compat2/0', ['/foo0'])
        self.addComponent('foo:lib', ':compat/1.1', [('/foo1', 'b')])

        self.updatePkg(['foo:lib=1', 'foo:lib=2', 'foo:lib=:compat2'],
                        raiseError=True)

        self.addComponent('foo:lib', '2.1', ['/foo2.1']) # the change from 2 to
                                                         # foo 2.1 has all new
                                                         # paths.

        self.checkUpdate(['foo:lib=:linux', 'foo:lib=:compat/1.1'],
                         ['foo:lib=2--2.1',
                          'foo:lib=:linux/1--:compat/1.1'],
                          apply=True)

###########################################################
#
# 14. SearchPath test
#
##########################################################

    def testSearchPath(self):
        self.addComponent('foo:run', ':branch/1', requires='trove:bar:run')
        self.addComponent('bar:run', ':branch/1', requires='trove:bam:run')
        self.addComponent('bam:run', ':branch2/1', requires='trove:bam:run')
        self.cfg.searchPath = ['foo:run=:branch', 'bar:run=:branch',
                               'localhost@rpl:branch2' ]
        self.checkUpdate('foo:run', ['foo:run', 'bar:run', 'bam:run'],
                         resolve=True)

    def testUpdateWithSearchPathFindsOneOfTwoAvailable(self):
        # CNY-1881 - update found two troves because of the searchFlavor
        self.addComponent('foo:run[is:x86]')
        self.addComponent('foo:run[is:x86_64]')
        self.cfg.flavor = [deps.parseFlavor('is:x86'),
                           deps.parseFlavor('is:x86_64')]
        self.addCollection('group-foo', ['foo:run[is:x86]',
                                         'foo:run[is:x86_64]'])
        self.cfg.searchPath = ['group-foo[is:x86 x86_64]']
        self.checkUpdate('foo:run', ['foo:run[is:x86]'])

#######################################################
#
# N. Miscellaneous
#
#    Put your test here if you don't want to sort it
#
######################################################

    def testCallbackFailure(self):
        self.addComponent('foo:run', '1', filePrimer = 0)
        self.addComponent('bar:run', '1', filePrimer = 1)
        self.cfg.updateThreshold = 1
        self.logFilter.add()
        try:
            self.updatePkg([ 'foo:run', 'bar:run' ],
                       callback = FailureUpdateCallback('restoreFiles'))
        except Exception, e:
            self.assertEqual(e.args[0], 'restoreFiles')
        else:
            self.fail("Exception expected but not raised")
        self.logFilter.compare('error: a critical error occured -- '
                               'reverting filesystem changes')

        # After fixing CNY-1264, if the download fails, then nothing gets
        # installed.
        db = self.openDatabase()
        assert(len([ x for x in db.iterAllTroveNames() ]) == 0)

        # if the failure is in the download thread, it's handled the same
        # way (the current job finishes but then we abort)
        self.resetRoot()
        try:
            self.updatePkg([ 'foo:run', 'bar:run' ],
                       callback = FailureUpdateCallback('downloadingChangeSet'))
        except Exception, e:
            self.assertEqual(e.args[0], 'downloadingChangeSet')
        else:
            self.fail("Exception expected but not raised")

        db = self.openDatabase()
        assert(len([ x for x in db.iterAllTroveNames() ]) == 0)

    def testLockedDatabase(self):
        # CNY-1292 - Database locked exception not caught
        self.addComponent('foo:run', '1', filePrimer = 0)
        self.addComponent('bar:run', '1', filePrimer = 1)
        self.updatePkg('foo:run')
        db = sqlite3.connect(self.rootDir + self.cfg.dbPath + '/conarydb')
        cu = db.cursor()
        cu.execute("begin immediate")

        oldTimeout = database.sqldb.Database.timeout
        try:
            # Speed up the test - time out in 2000 milliseconds (instead of
            # the default 30 seconds)
            database.sqldb.Database.timeout = 2000
            try:
                self.updatePkg('bar:run', raiseError=True)
            except errors.ConaryError, e:
                self.assertEqual(str(e), "Database error: database is locked")
            else:
                self.fail("ConaryError not raised")
        finally:
            database.sqldb.Database.timeout = oldTimeout

    @context('trovescripts')
    def testRollbackInvalidationStatus(self):
        # tests using script defined rollback invalidation
        client = conaryclient.ConaryClient(self.cfg)

        self.addComponent('foo:runtime', '1.0')

        self.addCollection('group-foo', '1.0', [ ('foo:runtime', '1.0' ) ],
                           postUpdateScript = 'postupdate')

        applyList = cmdline.parseChangeList(['group-foo'])

        # these aren't updates, they're installs, so they don't invalidate
        # rollbacks
        updJob, suggMap = client.updateChangeSet(applyList)
        assert(not updJob.updateInvalidatesRollbacks())
        updJob, suggMap = client.updateChangeSet(applyList, migrate = True)
        assert(not updJob.updateInvalidatesRollbacks())

        self.updatePkg('group-foo', justDatabase = True)

        # tests of invalidation based on compatibility class checks. first,
        # make sure updates are invalidated if the compat class changes at all
        self.addCollection('group-foo', '1.1', [ ('foo:runtime', '1.0' ) ],
                           compatClass = 1)
        updJob, suggMap = client.updateChangeSet(applyList)
        assert(updJob.updateInvalidatesRollbacks())
        updJob, suggMap = client.updateChangeSet(applyList, migrate = True)
        assert(updJob.updateInvalidatesRollbacks())
        self.updatePkg('group-foo', justDatabase = True)

        # test the same thing (apparently) again; the difference is the old
        # trove has a compatibilityClass now while it used to be None
        self.addCollection('group-foo', '1.2', [ ('foo:runtime', '1.0' ) ],
                           compatClass = 2)
        updJob, suggMap = client.updateChangeSet(applyList)
        assert(updJob.updateInvalidatesRollbacks())
        updJob, suggMap = client.updateChangeSet(applyList, migrate = True)
        assert(updJob.updateInvalidatesRollbacks())

        # now create a group which has a valid rollback script
        self.addCollection('group-foo', '1.2.1', [ ('foo:runtime', '1.0' ) ],
                           compatClass = 2,
                           postRollbackScript =
                                rephelp.RollbackScript(script= 'prerollback',
                                                       conversions = [ 1 ] ) )
        updJob, suggMap = client.updateChangeSet(applyList)
        assert(not updJob.updateInvalidatesRollbacks())
        updJob, suggMap = client.updateChangeSet(applyList, migrate = True)
        assert(not updJob.updateInvalidatesRollbacks())

        grp = self.addCollection('group-basic', '1.0',
                         [ ('foo:runtime', '1.0' ) ],
                           compatClass = 3,
                           postRollbackScript =
                                rephelp.RollbackScript(script= 'postrollback',
                                                       conversions = [ 2 ] ) )
        grpCs = grp.diff(None, absolute=True)[0]
        self.assertEquals(False, grpCs.isRollbackFence(3))
        self.assertEquals(False, grpCs.isRollbackFence(2))
        self.assertEquals(True, grpCs.isRollbackFence(1))

    def testJobSizes(self):
        self.addComponent('first:run', '1')
        self.addCollection('first', '1', [ 'first:run' ] )
        self.addComponent('second:run', '1')
        self.addCollection('second', '1', [ 'second:run' ] )

        client = conaryclient.ConaryClient(self.cfg)

        applyList = cmdline.parseChangeList(['first:run', 'second:run'])
        updJob, suggMap = client.updateChangeSet(applyList)
        assert(len(client.getDownloadSizes(updJob)) == 1)

        self.cfg.updateThreshold = 2
        applyList = cmdline.parseChangeList(['first', 'second'])
        updJob, suggMap = client.updateChangeSet(applyList)
        jobSizes = client.getDownloadSizes(updJob)
        assert(len(jobSizes) == 2)

        size = 0
        for trv in [ 'first', 'first:run', 'second', 'second:run' ]:
            applyList = cmdline.parseChangeList([ trv ])
            updJob, suggMap = client.updateChangeSet(applyList, recurse = False)
            size += client.getDownloadSizes(updJob)[0]

        assert(size == sum(jobSizes))


    def testDatabaseLocking(self):

        class Blocker:

            def __init__(self, fn, w, pid):
                self.fn = fn
                self.w = w
                self.pid = pid

            def __call__(self, realSelf, *args, **kwargs):
                os.write(self.w, 'a')
                pid, status = os.waitpid(self.pid, 0)
                assert(status == 0)

                return 

        self.addComponent('foo:runtime', '1.0')
        self.addComponent('bar:runtime', '1.0')
        self.updatePkg('foo:runtime')

        # primary process starts an update and tells the child process to
        # start a rollback (which will fail with DatabaseLocked)
        r, w = os.pipe()
        childpid = os.fork()
        if childpid == 0:
            # this ensures the child terminates sooner or later
            signal.alarm(2)
            client = conaryclient.ConaryClient(self.cfg)
            # wait for the parent to start the update
            os.read(r, 1)
            # try and rollback
            try:
                self.rollback(self.rootDir, 0)
            except errors.DatabaseLockedError:
                os._exit(0)

            os._exit(1)

        client = conaryclient.ConaryClient(self.cfg)
        self.mock(database.Database, 'commitChangeSet',
                  Blocker(database.Database.commitChangeSet, w,
                          childpid))
        self.updatePkg('bar:runtime')

    def testLabelAffinity(self):
        self.addComponent('foo:runtime', '/localhost@rpl:1//shadow/1.0-1-1',
                          fileContents = [ '/1' ]  )
        self.addComponent('foo:runtime', '/localhost@rpl:2//shadow/2.0-1-1',
                          fileContents = [ '/2' ]  )
        self.addComponent('foo:runtime', '/localhost@rpl:1//shadow/3.0-1-1',
                          fileContents = [ '/3' ]  )

        # Conary 2.0 (when label affinity replaced branch affinity) replaces
        # the latest version on the same label with the new version being
        # intalled (2->3). When branch affinity was used, the change was
        # made based on full branches; the shared label was irrelevant.
        self.updatePkg('foo:runtime=localhost@rpl:shadow/1.0-1-1')
        self.updatePkg('foo:runtime=localhost@rpl:shadow/2.0-1-1',
                       keepExisting = True)
        self.updatePkg('foo:runtime=/localhost@rpl:1//shadow/3.0-1-1')

        assert(os.path.exists(self.rootDir + '/1'))
        assert(os.path.exists(self.rootDir + '/3'))

        #raise testhelp.SkipTestException('Waiting for dugan')
        # With label affinity (instead of branch affinity) we find version 3
        # for the update
        self.resetRoot()
        self.updatePkg('foo:runtime=localhost@rpl:shadow/2.0-1-1')
        self.updatePkg('foo:runtime')
        assert(os.path.exists(self.rootDir + '/3'))

    def testManifest(self):
        def _checkManifest(*trvs):
            manifestPath = self.cfg.root + self.cfg.dbPath + '/manifest'
            trvs = ['%s=%s[%s]\n' % x.getNameVersionFlavor() for x in trvs ]
            assert(trvs == open(manifestPath).readlines())
        foo = self.addComponent('foo:runtime=1[ssl]')
        bar = self.addComponent('bar:runtime=1', filePrimer=1)
        self.updatePkg('foo:runtime')
        _checkManifest(foo)
        self.updatePkg('-foo:runtime')
        _checkManifest()
        self.updatePkg(['foo:runtime', 'bar:runtime'])
        _checkManifest(bar, foo)

#############################################
#
# Section 15. job restart tests
#
#############################################

    def testPreviousVersion(self):
        client = conaryclient.ConaryClient(self.cfg)
        updJob = database.UpdateJob(client.db)
        restartDir = update._storeJobInfo([], updJob)

        # Change the version
        vFilePath = os.path.join(restartDir, "__version__")
        file(vFilePath, "w").write("version 1001.1plus\n")

        updJob = database.UpdateJob(client.db)
        self.assertEqual(updJob.getPreviousVersion(), None)
        update._loadRestartInfo(restartDir, updJob)
        self.assertEqual(updJob.getPreviousVersion(), "1001.1plus")

        # Unreadable file
        os.chmod(vFilePath, 0)
        updJob = database.UpdateJob(client.db)
        update._loadRestartInfo(restartDir, updJob)
        self.assertEqual(updJob.getPreviousVersion(), None)

        # Readable file, junk at the beginning
        os.chmod(vFilePath, 0644)
        file(vFilePath, "w").write("junk 1\nversion -1\n")
        updJob = database.UpdateJob(client.db)
        update._loadRestartInfo(restartDir, updJob)
        self.assertEqual(updJob.getPreviousVersion(), '-1')

        # Just junk
        file(vFilePath, "w").write("junk 1")
        updJob = database.UpdateJob(client.db)
        update._loadRestartInfo(restartDir, updJob)
        self.assertEqual(updJob.getPreviousVersion(), None)

        # Empty file
        file(vFilePath, "w").write("")
        updJob = database.UpdateJob(client.db)
        update._loadRestartInfo(restartDir, updJob)
        self.assertEqual(updJob.getPreviousVersion(), None)

        util.rmtree(restartDir)

    def testParseAlreadyRunScripts(self):
        # CNY-3219
        # if you have two groups with pre scripts, the first one will have an
        # additional line termination that was braeking
        # cmdline.parseTroveSpec
        ver0 = (versions.VersionFromString("/localhost@test:1/1-2-3",
                                           timeStamps = [ 1234567890.0 ]),
                deps.parseFlavor("is: x86"))
        ver1 = (versions.VersionFromString("/localhost@test:1/1-2-4",
                                           timeStamps = [ 1234567890.1 ]),
                deps.parseFlavor("is: x86_64"))
        job0 = ('group-foo', ver0, ver1)
        job1 = ('group-bar', ver0, ver1)

        preScripts = [('preinstall', job0), ('preupdate', job1)]

        data = update._serializePreScripts(preScripts)
        # Add a leading whitespace to the group name, just to make sure we are
        # indeed stripping whitespaces around frumptos
        # And some trailing whitespaces after the closing ]
        data = data.replace("group-foo", " group-foo").replace(']', '] ')

        import StringIO
        io = StringIO.StringIO(data)
        io.seek(0)
        ret = update._unserializePreScripts(io)
        def _cvtJob(job):
            return (job[0], (versions.ThawVersion(job[1][0]), job[1][1]),
                (versions.ThawVersion(job[2][0]), job[2][1]))
        self.assertEqual([ (sname, _cvtJob(job)) for (sname, job) in ret ],
            preScripts)

# NOTE Don't put your tests here if there's some section they could go into
# By keeping all of the tests for a particular feature together it makes
# it easier to see if there's a test that's doing something similar to 
# what you want.
