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


import inspect
import os
import sys
import tempfile

from conary_test import rephelp
import policytest

from conary.build import cook, policy, trovefilter
from conary.lib import util
from conary import versions
from conary.deps import deps

simpleGroupRecipe = """\
class GroupSimpleAdd(GroupRecipe):
    name = 'group-simple'
    version = '1.0.0'
    imageGroup = False

    clearBuildReqs()

    def setup(r):
        r.add('foo')
"""

class FakeCache(dict):

    troveIsCached = dict.__contains__

    def getTrove(self, n, v, f, withFiles = False):
        return self[(n, v, f)]

class GroupPolicyTest(rephelp.RepositoryHelper):
    def testTroveVersionsOnBranch(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'

    clearBuildReqs()

    def setup(r):
        r.add('foo', '1')
        r.add('foo:data', '2')
"""
        self.addComponent('foo:data', '1', filePrimer = 1)
        self.addCollection('foo', '1', [':data'])
        self.addComponent('foo:data', '2', filePrimer = 2)
        self.addCollection('foo', '2', [':data'])
        try:
            self.build(groupRecipe, 'GroupConflict')
        except policy.PolicyError, e:
            self.assertEquals(str(e), '\n'.join((
                "Group Policy errors found:",
                "Multiple versions of foo from localhost@rpl:linux were found:",
                "",
                "foo:data=/localhost@rpl:linux/2-1-1[]: (Added directly)",
                "group-conflict=/localhost@rpl:linux/1.0.0-1-1[]",
                "  foo=/localhost@rpl:linux/2-1-1[]",
                "    foo:data=/localhost@rpl:linux/2-1-1[]",
                "",
                "foo:data=/localhost@rpl:linux/1-1-1[]: (Included by adding foo=/localhost@rpl:linux/1-1-1[])",
                "group-conflict=/localhost@rpl:linux/1.0.0-1-1[]",
                "  foo=/localhost@rpl:linux/1-1-1[]",
                "    foo:data=/localhost@rpl:linux/1-1-1[]",
                "",
                "Multiple versions of these troves were found:",
                "foo:data")))
        else:
            self.fail("build should have raised PolicyError")

    def testPathFormatting(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'
    autoResolve = True

    clearBuildReqs()

    def setup(r):
        r.createGroup('group-test')
        r.addNewGroup('group-test')
        r.setDefaultGroup('group-test')
        r.add('bar', '1')
        r.add('foo')
"""
        self.addComponent('foo:data', '1', filePrimer = 1,
                requires = 'file: /bin/sh')
        self.addCollection('foo', '1', [':data'])
        self.addComponent('bar:runtime', '1', filePrimer = 2,
                provides = 'file: /bin/bash')
        self.addCollection('bar', '1', [':runtime'])
        self.addComponent('bar:runtime', '2', filePrimer = 3,
                provides = 'file: /bin/sh')
        self.addCollection('bar', '2', [':runtime'])
        try:
            self.build(groupRecipe, 'GroupConflict')
        except policy.PolicyError, e:
            self.assertEquals(str(e), '\n'.join((
                "Group Policy errors found:",
                "Multiple versions of bar from localhost@rpl:linux were found:",
                "",
                "bar:runtime=/localhost@rpl:linux/2-1-1[]: (Added to satisfy dep(s): ('file: /bin/sh') required by foo:data=/localhost@rpl:linux/1-1-1[])",
                "group-conflict=/localhost@rpl:linux/1.0.0-1-1[]",
                "  bar=/localhost@rpl:linux/2-1-1[]",
                "    bar:runtime=/localhost@rpl:linux/2-1-1[]",
                "",
                "bar:runtime=/localhost@rpl:linux/1-1-1[]: (Included by adding bar=/localhost@rpl:linux/1-1-1[])",
                "group-conflict=/localhost@rpl:linux/1.0.0-1-1[]",
                "  group-test=/localhost@rpl:linux/1.0.0-1-1[]",
                "    bar=/localhost@rpl:linux/1-1-1[]",
                "      bar:runtime=/localhost@rpl:linux/1-1-1[]",
                "",
                "Multiple versions of these troves were found:",
                "bar:runtime"
                )))
        else:
            self.fail("build should have raised PolicyError")

    def testPathFormatting2(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'
    autoResolve = True

    clearBuildReqs()

    def setup(r):
        r.createGroup('group-test')
        r.addNewGroup('group-test')
        r.setDefaultGroup('group-test')
        r.add('bar', '1')
        r.add('bar', '2')
        r.add('foo', '1')
        r.add('foo', '2')
"""
        self.addComponent('foo:data', '1', filePrimer = 1)
        self.addCollection('foo', '1', [':data'])
        self.addComponent('foo:data', '2', filePrimer = 2)
        self.addCollection('foo', '2', [':data'])
        self.addComponent('bar:runtime', '1', filePrimer = 3)
        self.addCollection('bar', '1', [':runtime'])
        self.addComponent('bar:runtime', '2', filePrimer = 4)
        self.addCollection('bar', '2', [':runtime'])
        try:
            self.build(groupRecipe, 'GroupConflict')
        except policy.PolicyError, e:
            self.assertEquals(str(e).splitlines()[-4:],
                    ['', 'Multiple versions of these troves were found:',
                    'bar:runtime', 'foo:data'])
            self.assertEquals(str(e).count("Multiple versions of these " \
                    "troves were found"), 1)
        else:
            self.fail("build should have raised PolicyError")

    def testTroveVersionsOnBranchExceptions(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'

    clearBuildReqs()

    def setup(r):
        r.add('foo', '1')
        r.add('foo:data', '2')
        r.VersionConflicts(exceptions = "foo.*")
"""
        self.addComponent('foo:data', '1', filePrimer = 1)
        self.addCollection('foo', '1', [':data'])
        self.addComponent('foo:data', '2', filePrimer = 2)
        self.addCollection('foo', '2', [':data'])
        grp = self.build(groupRecipe, 'GroupConflict')

    def testTroveVersionsOnBranchExceptions2(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'

    clearBuildReqs()

    def setup(r):
        r.add('foo', '1')
        r.add('foo:lib', '2')
"""
        self.addComponent('foo:lib', '1', filePrimer = 1)
        self.addComponent('foo:runtime', '1', filePrimer = 2)
        self.addCollection('foo', '1', [':lib', ':runtime'])
        self.addComponent('foo:lib', '2', filePrimer = 3)
        self.addComponent('foo:runtime', '2', filePrimer = 4)
        self.addCollection('foo', '2', [':lib', ':runtime'])
        # this should trigger a VersionConflict on :lib
        self.assertRaises(policy.PolicyError, self.build,
                groupRecipe, 'GroupConflict')

    def testTroveVersionsOnBranchPath(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'

    clearBuildReqs()

    def setup(r):
        r.createGroup('group-1')
        r.addNewGroup('group-1', groupName = 'group-conflict')
        r.setDefaultGroup('group-1')
        r.add('foo:runtime', '1')
        r.createGroup('group-2')
        r.addNewGroup('group-2', groupName = 'group-conflict')
        r.setDefaultGroup('group-2')
        r.add('foo:lib', '2')
"""
        self.addComponent('foo:lib', '1', filePrimer = 1)
        self.addComponent('foo:runtime', '1', filePrimer = 2)
        self.addCollection('foo', '1', [':lib', ':runtime'])
        self.addComponent('foo:lib', '2', filePrimer = 3)
        self.addComponent('foo:runtime', '2', filePrimer = 4)
        self.addCollection('foo', '2', [':lib', ':runtime'])
        # this should trigger a VersionConflicts because of :lib conflicts
        self.assertRaises(policy.PolicyError, self.build,
                groupRecipe, 'GroupConflict')

    def testTroveConflictOnNonLeafGroup(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'

    clearBuildReqs()

    def setup(r):
        r.add('foo:doc', '2')
        r.createGroup('group-1')
        r.addNewGroup('group-1', groupName = 'group-conflict')
        r.setDefaultGroup('group-1')
        r.add('foo:runtime', '1')
"""
        self.addComponent('foo:doc', '1', filePrimer = 1)
        self.addComponent('foo:runtime', '1', filePrimer = 2)
        self.addCollection('foo', '1', [':doc', ':runtime'])
        self.addComponent('foo:doc', '2', filePrimer = 3)
        self.addComponent('foo:runtime', '2', filePrimer = 4)
        self.addCollection('foo', '2', [':doc', ':runtime'])
        # this should trigger a VersionConflict. We're specifically checking
        # to see if a conflict in the main group was detected
        self.assertRaises(policy.PolicyError, self.build,
                groupRecipe, 'GroupConflict')

    def testOrFilters(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'

    clearBuildReqs()

    def setup(r):
        r.add('foo', '2')
        r.createGroup('group-2')
        r.addNewGroup('group-2', groupName = 'group-conflict')
        r.setDefaultGroup('group-2')
        r.add('bar', '1')
        r.createGroup('group-1')
        r.addNewGroup('group-1', groupName = 'group-conflict')
        r.setDefaultGroup('group-1')
        r.add('foo', '1')
        r.add('bar', '2')

        fooFilter = r.troveFilter("foo:runtime")
        group2 = r.troveFilter("group-2")

        r.VersionConflicts(exceptions = fooFilter | group2)
"""
        self.addComponent('foo:doc', '1', filePrimer = 1)
        self.addComponent('foo:runtime', '1', filePrimer = 2)
        self.addCollection('foo', '1', [':doc', ':runtime'])
        self.addComponent('foo:doc', '2', filePrimer = 3)
        self.addComponent('foo:runtime', '2', filePrimer = 4)
        self.addCollection('foo', '2', [':doc', ':runtime'])
        self.addComponent('bar:doc', '1', filePrimer = 5)
        self.addComponent('bar:runtime', '1', filePrimer = 6)
        self.addCollection('bar', '1', [':doc', ':runtime'])
        self.addComponent('bar:doc', '2', filePrimer = 7)
        self.addComponent('bar:runtime', '2', filePrimer = 8)
        self.addCollection('bar', '2', [':doc', ':runtime'])

        try:
            self.build(groupRecipe, 'GroupConflict')
        except policy.PolicyError, e:
            self.assertFalse("foo:runtime" in str(e))
            self.assertFalse("bar" in str(e))
            self.assertFalse("group-2" in str(e))
        else:
            self.fail("expected policy error")
        self.assertRaises(policy.PolicyError, self.build,
                groupRecipe, 'GroupConflict')

    def testAndFilters(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'

    clearBuildReqs()

    def setup(r):
        r.add('foo', '2')
        r.createGroup('group-2')
        r.addNewGroup('group-2', groupName = 'group-conflict')
        r.setDefaultGroup('group-2')
        r.add('bar', '1')
        r.createGroup('group-1')
        r.addNewGroup('group-1', groupName = 'group-conflict')
        r.setDefaultGroup('group-1')
        r.add('foo', '1')
        r.add('bar', '2')

        fooFilter = r.troveFilter("foo.*")
        group1 = r.troveFilter("group-1")

        r.VersionConflicts(exceptions = fooFilter & group1)
"""
        self.addComponent('foo:doc', '1', filePrimer = 1)
        self.addComponent('foo:runtime', '1', filePrimer = 2)
        self.addCollection('foo', '1', [':doc', ':runtime'])
        self.addComponent('foo:doc', '2', filePrimer = 3)
        self.addComponent('foo:runtime', '2', filePrimer = 4)
        self.addCollection('foo', '2', [':doc', ':runtime'])
        self.addComponent('bar:doc', '1', filePrimer = 5)
        self.addComponent('bar:runtime', '1', filePrimer = 6)
        self.addCollection('bar', '1', [':doc', ':runtime'])
        self.addComponent('bar:doc', '2', filePrimer = 7)
        self.addComponent('bar:runtime', '2', filePrimer = 8)
        self.addCollection('bar', '2', [':doc', ':runtime'])

        try:
            self.build(groupRecipe, 'GroupConflict')
        except policy.PolicyError, e:
            self.assertFalse("foo" in str(e))
            self.assertFalse("bar" not in str(e))
            self.assertFalse("bar:runtime" not in str(e))
            self.assertFalse("bar:doc" not in str(e))
            self.assertFalse("group-1" not in str(e))
            self.assertFalse("group-2" not in str(e))
        else:
            self.fail("expected policy error")
        self.assertRaises(policy.PolicyError, self.build,
                groupRecipe, 'GroupConflict')

    def testNotFilters(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'

    clearBuildReqs()

    def setup(r):
        r.add('foo', '2')
        r.createGroup('group-2')
        r.addNewGroup('group-2', groupName = 'group-conflict')
        r.setDefaultGroup('group-2')
        r.add('bar', '1')
        r.createGroup('group-1')
        r.addNewGroup('group-1', groupName = 'group-conflict')
        r.setDefaultGroup('group-1')
        r.add('foo', '1')
        r.add('bar', '2')
        r.add('foo', '2')

        group1 = r.troveFilter("group-1")

        r.VersionConflicts(exceptions = ~group1)
"""
        self.addComponent('foo:doc', '1', filePrimer = 1)
        self.addComponent('foo:runtime', '1', filePrimer = 2)
        self.addCollection('foo', '1', [':doc', ':runtime'])
        self.addComponent('foo:doc', '2', filePrimer = 3)
        self.addComponent('foo:runtime', '2', filePrimer = 4)
        self.addCollection('foo', '2', [':doc', ':runtime'])
        self.addComponent('bar:doc', '1', filePrimer = 5)
        self.addComponent('bar:runtime', '1', filePrimer = 6)
        self.addCollection('bar', '1', [':doc', ':runtime'])
        self.addComponent('bar:doc', '2', filePrimer = 7)
        self.addComponent('bar:runtime', '2', filePrimer = 8)
        self.addCollection('bar', '2', [':doc', ':runtime'])

        try:
            self.build(groupRecipe, 'GroupConflict')
        except policy.PolicyError, e:
            self.assertFalse("foo" not in str(e))
            self.assertFalse("foo:runtime" not in str(e))
            self.assertFalse("foo:doc" not in str(e))
            self.assertFalse("bar" in str(e))
            self.assertFalse("group-1" not in str(e))
            self.assertFalse("group-2" in str(e))
        else:
            self.fail("expected policy error")
        self.assertRaises(policy.PolicyError, self.build,
                groupRecipe, 'GroupConflict')

    def testTroveConflictInUncookedGroup(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'

    clearBuildReqs()

    def setup(r):
        r.add('group-uncooked')
        r.createGroup('group-1')
        r.addNewGroup('group-1', groupName = 'group-conflict')
        r.setDefaultGroup('group-1')
        r.add('foo:runtime', '2.0.0')
"""
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        self.addCollection('group-uncooked', '1.0.0',
                strongList = ['foo'], weakRefList = ['foo:doc'])

        self.addComponent('foo:doc', '2.0.0', filePrimer = 3)
        self.addComponent('foo:runtime', '2.0.0', filePrimer = 4)
        self.addCollection('foo', '2.0.0', [':doc', ':runtime'])
        self.assertRaises(policy.PolicyError, self.build,
                groupRecipe, 'GroupConflict')

    def testTroveConflictInUncookedGroup2(self):
        groupRecipe = """
class GroupConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'
    checkOnlyByDefaultDeps = True

    clearBuildReqs()

    def setup(r):
        r.add('group-uncooked')
        r.createGroup('group-1')
        r.addNewGroup('group-1', groupName = 'group-conflict')
        r.setDefaultGroup('group-1')
        r.add('foo:runtime', '2.0.0')
        r.VersionConflicts(exceptions = r.troveFilter(name = 'foo:runtime'))
"""
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        self.addCollection('group-uncooked', '1.0.0', ['foo:doc'])

        self.addComponent('foo:doc', '2.0.0', filePrimer = 3)
        self.addComponent('foo:runtime', '2.0.0', filePrimer = 4)
        self.addCollection('foo', '2.0.0', [':doc', ':runtime'])
        # this shouldn't trigger an error. foo:runtime was excluded
        self.build(groupRecipe, 'GroupConflict')

    def testNoConflictInImageGroups(self):
        groupRecipe = """
class GroupNoConflict(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'
    imageGroup = False

    clearBuildReqs()

    def setup(r):
        r.createGroup('group-1', imageGroup = True)
        r.addNewGroup('group-1', groupName = 'group-conflict')
        r.setDefaultGroup('group-1')
        r.add('foo', '1.0.0')
        r.createGroup('group-2', imageGroup = True)
        r.addNewGroup('group-2', groupName = 'group-conflict')
        r.setDefaultGroup('group-2')
        r.add('foo', '2.0.0')
"""
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        self.addComponent('foo:doc', '2.0.0', filePrimer = 3)
        self.addComponent('foo:runtime', '2.0.0', filePrimer = 4)
        self.addCollection('foo', '2.0.0', [':doc', ':runtime'])
        # this shouldn't trigger an error. group-1 and group-2 are considered
        # separately
        self.build(groupRecipe, 'GroupNoConflict')

    def testUnusedFilters(self):
        groupRecipe = """
class GroupUnusedFilter(GroupRecipe):
    name = 'group-conflict'
    version = '1.0.0'
    imageGroup = True

    clearBuildReqs()

    def setup(r):
        r.add('foo', '1.0.0')
        r.VersionConflicts(exceptions = 'bar')
"""
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])
        self.logFilter.add()
        self.build(groupRecipe, 'GroupUnusedFilter')
        self.logFilter.remove()
        self.assertEquals(self.logFilter.records,
                ["warning: VersionConflicts: Exception <TroveFilter: 'bar'> "
                 "for VersionConflicts was not used"])

class ManagedPolicyTest(rephelp.RepositoryHelper):
    def setUp(self):
        self.policyCount = 0
        self.registeredPolicies = []
        rephelp.RepositoryHelper.setUp(self)

    def tearDown(self):
        for modname in self.registeredPolicies:
            mod = sys.modules.get(modname)
            if not mod:
                continue
            for klass in [x[0] for x in mod.__dict__.iteritems() \
                    if inspect.isclass(x[1])]:
                del mod.__dict__[klass]
        rephelp.RepositoryHelper.tearDown(self)

    def registerPolicy(self, dirName, contents):
        self.policyCount += 1
        modname = "testpolicy%d" % self.policyCount
        self.registeredPolicies.append(modname)
        fn = modname + '.py'
        f = open(os.path.join(dirName, fn), 'w')
        f.write(contents)
        f.close()

    def testUnmanagedPolicy1(self):
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        policyStr = """
from conary.build import policy
class AlwaysError(policy.GroupEnforcementPolicy):
    def doProcess(self, recipe):
        self.recipe.reportErrors("Automatic error", groupError = False)
"""
        tmpDir = tempfile.mkdtemp()
        try:
            policyDirs = self.cfg.policyDirs
            self.cfg.policyDirs = [tmpDir]
            self.registerPolicy(tmpDir, policyStr)
            try:
                self.build(simpleGroupRecipe, 'GroupSimpleAdd')
            except policy.PolicyError, e:
                self.assertEquals(e.args,
                        ('Package Policy errors found:\nAutomatic error',))
            else:
                self.fail("expected PolicyError to be raised")
        finally:
            self.cfg.policyDirs = policyDirs
            util.rmtree(tmpDir)

    def testUnmanagedPolicy2(self):
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        policyStr = """
from conary.build import policy
class AlwaysError(policy.GroupEnforcementPolicy):
    def doProcess(self, recipe):
        self.recipe.reportErrors("Automatic error")
"""
        tmpDir = tempfile.mkdtemp()
        try:
            policyDirs = self.cfg.policyDirs
            self.cfg.policyDirs = [tmpDir]
            enforceManagedPolicy = self.cfg.enforceManagedPolicy
            self.cfg.enforceManagedPolicy = True
            self.registerPolicy(tmpDir, policyStr)
            # this will fail with CookError because we shouldn't be allowed
            # to use a policy that's not managed by conary
            self.assertRaises(cook.CookError, self.build,
                    simpleGroupRecipe, 'GroupSimpleAdd')
        finally:
            self.cfg.enforceManagedPolicy = enforceManagedPolicy
            self.cfg.policyDirs = policyDirs
            util.rmtree(tmpDir)

    def testManagedPolicy(self):
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        policyStr = """
from conary.build import policy
class AlwaysError(policy.GroupEnforcementPolicy):
    def doProcess(self, recipe):
        self.recipe.reportErrors("Automatic error")
"""

        policyPath = os.path.join(self.cfg.root, 'policy', 'errpolicy.py')
        # we're effectively creating /tmp/_/root/tmp/_/root/policy/...
        # we're doing this so that the system db in /tmp/_/root will
        # match the absolute path of the actual policy file.
        self.addComponent('errorpolicy:runtime',
                fileContents = [(policyPath, policyStr)])
        self.updatePkg('errorpolicy:runtime')

        try:
            policyDirs = self.cfg.policyDirs
            self.cfg.policyDirs = [os.path.dirname(policyPath)]
            enforceManagedPolicy = self.cfg.enforceManagedPolicy
            self.cfg.enforceManagedPolicy = True
            util.mkdirChain(os.path.dirname(policyPath))
            f = open(policyPath, 'w')
            f.write(policyStr)
            f.close()
            self.assertRaises(policy.PolicyError, self.build,
                    simpleGroupRecipe, 'GroupSimpleAdd')
        finally:
            self.cfg.enforceManagedPolicy = enforceManagedPolicy
            self.cfg.policyDirs = policyDirs
            util.rmtree(os.path.dirname(policyPath))

    def testNoInclusions(self):
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        policyStr = """
from conary.build import policy
class AlwaysError(policy.GroupEnforcementPolicy):
    invariantexceptions = ['.*']
    def doTroveSet(self, troveSet):
        self.recipe.reportErrors("Automatic error")
"""
        tmpDir = tempfile.mkdtemp()
        try:
            policyDirs = self.cfg.policyDirs
            self.cfg.policyDirs = [tmpDir]
            self.registerPolicy(tmpDir, policyStr)
            self.build(simpleGroupRecipe, 'GroupSimpleAdd')
        finally:
            self.cfg.policyDirs = policyDirs
            util.rmtree(tmpDir)

    def testCompileExpression1(self):
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        policyStr = """
from conary.build import policy
class ValidateFilters(policy.GroupEnforcementPolicy):
    def do(self):
        assert self.exceptions is None, \
                "exceptions: '%s' is not None"  % self.exceptions
        assert self.inclusions is None, \
                "inclusions: '%s' is not None"  % self.inclusions
        assert self.exceptionFilters == [], \
                "exceptionFilters: '%s' is not []" % self.exceptionFilters
        assert self.inclusionFilters == [], \
                "inclusionFilters: '%s' is not []" % self.inclusionFilters
"""
        tmpDir = tempfile.mkdtemp()
        try:
            policyDirs = self.cfg.policyDirs
            self.cfg.policyDirs = [tmpDir]
            self.registerPolicy(tmpDir, policyStr)
            self.build(simpleGroupRecipe, 'GroupSimpleAdd')
        finally:
            self.cfg.policyDirs = policyDirs
            util.rmtree(tmpDir)

    def testCompileExpression2(self):
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        policyStr = """
from conary.build import policy
class ValidateFilters(policy.GroupEnforcementPolicy):
    allowUnusedFilters = True
    def do(self):
        assert len(self.exceptions) == 5, "len(self.exceptions) != 5"
        assert len(self.exceptionFilters) == 1, \
                "len(self.exceptionFilters) != 1"
        assert len(self.inclusions) == 5, "len(self.inclusions) != 5"
        assert len(self.inclusionFilters) == 1, \
                "len(self.inclusionFilters) != 1"
"""
        recipeStr = """
class FilterGroup(GroupRecipe):
    name = 'group-fitlers'
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.add('foo')
        r.ValidateFilters(exceptions = '.*')
        r.ValidateFilters(exceptions = '.*')
        trvFilter = r.troveFilter(name = '.*')
        r.ValidateFilters(exceptions = trvFilter)
        r.ValidateFilters(exceptions = [trvFilter, '.*'])
        r.ValidateFilters(inclusions = '.*')
        r.ValidateFilters('.*')
        r.ValidateFilters(inclusions = trvFilter)
        r.ValidateFilters(inclusions = ['.*', trvFilter])
"""
        tmpDir = tempfile.mkdtemp()
        try:
            policyDirs = self.cfg.policyDirs
            self.cfg.policyDirs = [tmpDir]
            self.registerPolicy(tmpDir, policyStr)
            self.build(recipeStr, 'FilterGroup')
        finally:
            self.cfg.policyDirs = policyDirs
            util.rmtree(tmpDir)

    def testPolicyAttributes(self):
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        policyStr = """
from conary.build import policy
class ValidateFilters(policy.GroupEnforcementPolicy):
    def preProcess(self):
        self.preprocess = True
    def test(self):
        assert 'preprocess' in self.__dict__ and self.preprocess
        # returning False indicates test failed
        return False
    def do(self):
        raise RuntimeError, "self.test() should have aborted doProcess"
"""
        tmpDir = tempfile.mkdtemp()
        try:
            policyDirs = self.cfg.policyDirs
            self.cfg.policyDirs = [tmpDir]
            self.registerPolicy(tmpDir, policyStr)
            self.build(simpleGroupRecipe, 'GroupSimpleAdd')
        finally:
            self.cfg.policyDirs = policyDirs
            util.rmtree(tmpDir)

    def testImageGroupPolicyRun(self):
        self.addComponent('foo:doc', '1.0.0', filePrimer = 1)
        self.addComponent('foo:runtime', '1.0.0', filePrimer = 2)
        self.addCollection('foo', '1.0.0', [':doc', ':runtime'])

        policyStr = """
from conary.build import policy
class AlwaysFails(policy.ImageGroupEnforcementPolicy):
    def doTroveSet(self, troveSet):
        raise RuntimeError, "doTroveSet should not have been called"
"""
        recipeStr = """
class ImageGroup(GroupRecipe):
    name = 'group-fitlers'
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.add('foo')
"""
        tmpDir = tempfile.mkdtemp()
        try:
            policyDirs = self.cfg.policyDirs
            self.cfg.policyDirs = [tmpDir]
            self.registerPolicy(tmpDir, policyStr)
            # a non-image group will pass
            self.build(simpleGroupRecipe, 'GroupSimpleAdd')
            # a image group will fail
            self.assertRaises(RuntimeError, self.build,
                    recipeStr, 'ImageGroup')
        finally:
            self.cfg.policyDirs = policyDirs
            util.rmtree(tmpDir)

    def testDontWalkReferencedImageGroups(self):
        self.addComponent('foo:runtime')
        self.addCollection('foo', strongList = ['foo:runtime'])
        self.addCollection('group-foo', strongList = ['foo'],
                imageGroup = True)
        recipeStr = """
class GroupUncooked(GroupRecipe):
    name = 'group-uncooked'
    version = '1'

    clearBuildRequires()
    imageGroup = True

    def setup(r):
        r.add('foo')
        r.add('group-foo')
        r.RecordPolicy(exceptions = 'group-foo')
"""
        policyStr = """
from conary.build import policy
class RecordPolicy(policy.ImageGroupEnforcementPolicy):
    def __init__(self, *args, **kwargs):
        self.outputFile = open('%s', 'w')
        policy.ImageGroupEnforcementPolicy.__init__(self, *args, **kwargs)

    def doTroveSet(self, troveSet):
        self.outputFile.write(str(troveSet) + '\\n')
        self.outputFile.flush()

    def __del__(self):
        self.outputFile.close()
"""
        outputDir = tempfile.mkdtemp()
        tmpDir = tempfile.mkdtemp()
        try:
            policyDirs = self.cfg.policyDirs
            self.cfg.policyDirs = [tmpDir]
            outputFile = os.path.join(outputDir, 'log.txt')
            self.registerPolicy(tmpDir, policyStr % outputFile)
            grp = self.build(recipeStr, "GroupUncooked")
            data = open(outputFile).read()
            self.assertFalse("group-foo" in data,
                    "this trove should not have been mentioned")
        finally:
            util.rmtree(tmpDir)
            util.rmtree(outputDir)
            self.cfg.policyDirs = policyDirs

class TestTroveWalk(rephelp.RepositoryHelper):
    def getRecipe(self):
        return policytest.DummyRecipe(self.cfg)

    def getTrove(self, name, version = None, flavor = None,
            byDefault = True, troveList = [], cache = {}):
        class DummyTrove(object):
            def __init__(x, name, version, flavor, byDefault,
                    troveList, cache):
                x.name = name
                x.troveList = troveList
                x.byDefault = byDefault
                x.cache = cache
                if version == None:
                    version = versions.VersionFromString( \
                            '/test.rpath.local@rpl:linux//1.0.0-1-1')
                if flavor == None:
                    flavor = deps.parseFlavor('')
                x.version = version
                x.flavor = flavor
            def getNameVersionFlavor(x):
                return (x.name, x.version, x.flavor)
            getName = lambda x: x.name
            def iterTroveList(x, strongRefs = False, weakRefs = False):
                assert strongRefs or weakRefs
                for trv in x.troveList:
                    if strongRefs:
                        yield trv.getNameVersionFlavor()
                    if weakRefs:
                        for x in trv.iterTroveList(strongRefs, weakRefs):
                            yield x
            def iterTroveListInfo(x):
                for trv in x.troveList:
                    yield (trv.getNameVersionFlavor(), trv.byDefault, True)
                    for x in trv.iterTroveListInfo():
                        yield (x[0], x[1], False)
        return DummyTrove(name, version, flavor, byDefault, troveList, cache)

    def testNoTroveWalk(self):
        recipe = self.getRecipe()
        dummyGroup = self.getTrove('group-test')
        dummyGroup.imageGroup = False
        recipe.groups = {dummyGroup.getName(): dummyGroup}
        recipe.troveMap = {dummyGroup.getNameVersionFlavor(): dummyGroup}
        class RecordPolicy(policy.GroupEnforcementPolicy):
            def __init__(x, *args, **kwargs):
                x.troveSets = []
                policy.GroupEnforcementPolicy.__init__(x, *args, **kwargs)
            def doTroveSet(x, troveSet):
                x.troveSets.append(troveSet)
        pol = RecordPolicy(recipe)
        pol.inclusionFilters = []
        pol.exceptionFilters = []
        pol.do()
        self.assertEquals(pol.troveSets, [])
        pol.checkImageGroups = True
        pol.do()
        self.assertEquals(pol.troveSets, [])

    def testTroveWalk(self):
        recipe = self.getRecipe()
        fooData = self.getTrove('foo:data')
        fooDebuginfo = self.getTrove('foo:debuginfo', byDefault = False)
        foo = self.getTrove('foo', troveList = [fooData, fooDebuginfo])
        cache = FakeCache({fooData.getNameVersionFlavor(): fooData,
                fooDebuginfo.getNameVersionFlavor(): fooDebuginfo,
                foo.getNameVersionFlavor(): foo})
        dummyGroup = self.getTrove('group-test', troveList = [foo],
                cache = cache)
        dummyGroup.imageGroup = False
        recipe.groups = {dummyGroup.getName(): dummyGroup}
        recipe.troveMap = {dummyGroup.getNameVersionFlavor(): dummyGroup}
        class RecordPolicy(policy.GroupEnforcementPolicy):
            def __init__(x, *args, **kwargs):
                x.troveSets = []
                policy.GroupEnforcementPolicy.__init__(x, *args, **kwargs)
            def doTroveSet(x, troveSet):
                x.troveSets.append(troveSet)
        pol = RecordPolicy(recipe)
        pol.inclusionFilters = [trovefilter.TroveFilter(recipe, name = '.*')]
        pol.exceptionFilters = []
        pol.checkImageGroups = True
        pol.do()
        self.assertEquals(pol.troveSets, [])
        pol.checkImageGroups = False
        pol.do()
        fooPath = ([dummyGroup.getNameVersionFlavor(),
                foo.getNameVersionFlavor()], True, True)
        fooDataPath = ([dummyGroup.getNameVersionFlavor(),
                foo.getNameVersionFlavor(),
                fooData.getNameVersionFlavor()], True, False)
        fooDebuginfoPath = ([dummyGroup.getNameVersionFlavor(),
                foo.getNameVersionFlavor(),
                fooDebuginfo.getNameVersionFlavor()], False, False)
        troveSets = [[fooPath, fooDataPath, fooDebuginfoPath]]
        self.assertEquals(pol.troveSets, troveSets)
        self.assertEquals([x[0] for x in troveSets[0]],
                [x for x in pol.walkTrove([], cache, dummyGroup)])

    def testMultiTroveWalk(self):
        recipe = self.getRecipe()
        fooData = self.getTrove('foo:data')
        fooDebuginfo = self.getTrove('foo:debuginfo', byDefault = False)
        foo = self.getTrove('foo', troveList = [fooData, fooDebuginfo])
        barData = self.getTrove('bar:data')
        barDebuginfo = self.getTrove('bar:debuginfo', byDefault = False)
        bar = self.getTrove('bar', troveList = [barData, barDebuginfo])
        cache = FakeCache({fooData.getNameVersionFlavor(): fooData,
                fooDebuginfo.getNameVersionFlavor(): fooDebuginfo,
                foo.getNameVersionFlavor(): foo})
        subGroupFoo = self.getTrove('group-foo', troveList = [foo],
                cache = cache)
        subGroupFoo.imageGroup = True
        cache = FakeCache({barData.getNameVersionFlavor(): barData,
                barDebuginfo.getNameVersionFlavor(): barDebuginfo,
                bar.getNameVersionFlavor(): bar})
        subGroupBar = self.getTrove('group-bar', troveList = [bar],
                cache = cache)
        subGroupBar.imageGroup = True
        cache = FakeCache({fooData.getNameVersionFlavor(): fooData,
                fooDebuginfo.getNameVersionFlavor(): fooDebuginfo,
                foo.getNameVersionFlavor(): foo,
                barData.getNameVersionFlavor(): barData,
                barDebuginfo.getNameVersionFlavor(): barDebuginfo,
                bar.getNameVersionFlavor(): bar})

        dummyGroup = self.getTrove('group-test',
                troveList = [subGroupFoo, subGroupBar], cache = cache)
        dummyGroup.imageGroup = False

        recipe.groups = FakeCache({dummyGroup.getName(): dummyGroup,
                        subGroupFoo.getName():subGroupFoo,
                        subGroupBar.getName():subGroupBar})
        recipe.troveMap = {dummyGroup.getNameVersionFlavor(): dummyGroup,
                           subGroupFoo.getNameVersionFlavor():subGroupFoo,
                           subGroupBar.getNameVersionFlavor():subGroupBar,}
        class RecordPolicy(policy.GroupEnforcementPolicy):
            def __init__(x, *args, **kwargs):
                x.troveSets = []
                policy.GroupEnforcementPolicy.__init__(x, *args, **kwargs)
            def doTroveSet(x, troveSet):
                x.troveSets.append(troveSet)
        pol = RecordPolicy(recipe)
        pol.inclusionFilters = [trovefilter.TroveFilter(recipe, name = '.*')]
        pol.exceptionFilters = []
        pol.checkImageGroups = True
        pol.do()

        fooPath = ([subGroupFoo.getNameVersionFlavor(),
                foo.getNameVersionFlavor()], True, True)
        fooDataPath = ([subGroupFoo.getNameVersionFlavor(),
                foo.getNameVersionFlavor(),
                fooData.getNameVersionFlavor()], True, False)
        fooDebuginfoPath = ([subGroupFoo.getNameVersionFlavor(),
                foo.getNameVersionFlavor(),
                fooDebuginfo.getNameVersionFlavor()], False, False)
        barPath = ([subGroupBar.getNameVersionFlavor(),
                bar.getNameVersionFlavor()], True, True)
        barDataPath = ([subGroupBar.getNameVersionFlavor(),
                bar.getNameVersionFlavor(),
                barData.getNameVersionFlavor()], True, False)
        barDebuginfoPath = ([subGroupBar.getNameVersionFlavor(),
                bar.getNameVersionFlavor(),
                barDebuginfo.getNameVersionFlavor()], False, False)
        troveSets = sorted([[fooPath, fooDataPath, fooDebuginfoPath],
                    [barPath, barDataPath, barDebuginfoPath]])
        self.assertEquals(sorted(pol.troveSets), troveSets)
