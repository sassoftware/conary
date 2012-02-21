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

import os
import shutil
import tempfile
import sys

from conary_test import recipes
from conary_test import rephelp

from conary import versions
from conary.versions import VersionFromString as VFS
from conary.build import cook, errors, grouprecipe, loadrecipe, use
from conary.conarycfg import RegularExpressionList
from conary import conaryclient
from conary.deps import deps
from conary.lib import log
from conary.local import database
from conary.repository import changeset, netclient
from conary_test import resources


packageRecipe = """
class testRecipe(PackageRecipe):
    name = "test"
    version = "1.0"
    clearBuildReqs()

    def setup(self):
        self.Create("/bin/ls")
        self.Create("/bin/cat")
        self.SetModes("/bin/{ls,cat}", 0755)
        self.Create("/bin/dd")
        self.NonBinariesInBindirs(exceptions='/bin/dd')
        self.Create("/usr/bin/vi")
        self.Create("/usr/bin/vim")
        self.Create("/usr/bin/vile")
        self.Create("/usr/bin/emacs")
        self.Create("/usr/bin/pico")
        self.Create("/usr/bin/nano")
        self.NonBinariesInBindirs(exceptions='/usr/bin/')
        self.ExcludeDirectories(exceptions='/usr/bin')
"""

packageRecipe2 = packageRecipe.replace("1.0", "1.1")

basicGroup = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    checkPathConflicts = False
    imageGroup = False

    clearBuildRequires()

    def setup(self):
        self.add("test", "@rpl:linux")
        self.add("test", "@rpl:test1")
"""

pkgGroup = """
class pkgGroup(GroupRecipe):
    name = 'group-pkg'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.add("test")
"""

compGroup = """
class compGroup(GroupRecipe):
    name = 'group-comp'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.add("foo:devel")
        self.add("bar:devel")
"""

basicSplitGroup = """
class splitGroup(GroupRecipe):
    name = 'group-first'
    version = '1.0'
    checkPathConflicts = False
    clearBuildRequires()

    def setup(self):
        self.add("test", "@rpl:linux")
        self.createGroup('group-second')
        self.createGroup('group-third')
        self.add("test", "@rpl:test1",
                      groupName = ['group-second', 'group-third'])
        # add group-second to group-first
        self.addNewGroup('group-second')
"""

cyclicGroup = """
class cyclicGroup(GroupRecipe):
    name = 'group-first'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.createGroup('group-second')
        self.addNewGroup('group-second')
        self.addNewGroup('group-first', groupName = 'group-second')
"""

cyclicGroup2 = """
class cyclicGroup(GroupRecipe):
    name = 'group-first'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.addNewGroup('group-first')
"""



primariesGroup1 = """
class primariesGroup1(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.createGroup('group-second')
        self.addNewGroup('group-test', groupName='group-second')
        self.addTrove('test:runtime') 
"""

primariesGroup2 = """
class primariesGroup1(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        # two independent groups, both should be primary
        self.addTrove('test:runtime') 
        self.createGroup('group-second')
        self.addTrove('test:runtime', groupName='group-second') 
"""


selectiveGroup = """
class SelectiveGroup(GroupRecipe):
    name = 'group-selective'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.add("testcase", byDefault = False)
        self.add("double")
"""

flavoredGroup1 = """
class flavoredGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()

    Flags.bar = True
    imageGroup = False
    def setup(self):
        PackageFlags['group-test'].bar.setPlatform()
        Use.bootstrap.setPlatform()
        PackageFlags.test1.foo.setPlatform()
        Use.readline.setPlatform()
        if Flags.bar:
            self.add("test1", "@rpl:linux", 'test1.foo')
        if not Use.bootstrap:
            self.add("test2", "@rpl:linux")
"""

flavoredGroup2 = """
class flavoredGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    Flags.test1 = True
    imageGroup = False
    clearBuildRequires()

    def setup(self):
        PackageFlags['group-test'].test1.setPlatform()
        Use.bootstrap.setPlatform()
        PackageFlags.test1.foo.setPlatform()
        Use.readline.setPlatform()
        if Flags.test1:
            self.add("test1", "@rpl:linux", '!test1.foo')
        if not Use.bootstrap:
            self.add("test2", "@rpl:linux")
"""

flavoredGroup3 = """
class flavoredGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    Flags.test1 = True
    checkPathConflicts = False
    imageGroup = False
    clearBuildRequires()

    def setup(self):
        Flags.test1.setPlatform()
        Use.bootstrap.setPlatform()
        PackageFlags.test1.foo.setPlatform()
        Use.readline.setPlatform()
        self.add("test1", "@rpl:linux", '!test1.foo')
        self.add("test1", "@rpl:linux", 'test1.foo')
"""

almostFlavoredGroup = """
class almostFlavoredGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    Flags.test1 = True
    checkPathConflicts = False
    imageGroup = False
    clearBuildRequires()

    def setup(r):
        Use.readline.setPlatform(False)
        if Use.bootstrap:
            pass
        r.createGroup('group-second')
        r.addNewGroup('group-second')
        r.setDefaultGroup('group-second')
        r.add("test1", "@rpl:linux")
"""


archFlavoredGroup = """
class flavoredGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    requireLatest = False
    imageGroup = False
    clearBuildRequires()


    def setup(self):
        self.add("test1", "@rpl:linux", '%s')
"""

archFlavoredRecipe = """
class testRecipe(PackageRecipe):
    name = "test1"
    version = "1.0"
    clearBuildReqs()

    def setup(self):
        self.addArchive('multilib-sample.tar.bz2', dir='/asdf/')
"""

errorGroup = """
class errorGroup(GroupRecipe):
    name = "fileset-error-test"
    version = "1.0"

    imageGroup = False
    clearBuildRequires()


    def setup(self):
        # add a trove that does not exist
        self.add("foo", "@rpl:linux")
"""

multiLabelGroup = """
class multiLabelGroup(GroupRecipe):
    name = "group-multi-label-test"
    version = "1.0"

    imageGroup = False
    clearBuildRequires()


    def setup(self):
        # add troves from two different labels on different repositories
        self.add("testcase")
        self.add("bash", "localhost1@rpl:linux")
"""

labelPathGroup = """
class labelPathGroup(GroupRecipe):
    name = "group-label-path-test"
    version = "1.0"

    imageGroup = False
    clearBuildRequires()

    def setup(self):
        # add troves from two different labels on different repositories
        self.setLabelPath("localhost1@rpl:linux", "localhost@rpl:linux")
        self.add("testcase")
        self.add("bash")
"""

memberGoneMissingGroup = """
class memberGoneMissingGroup(GroupRecipe):
    name = "group-member-gone-missing"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.add("bash")
"""

referrerGroup1 = """
class ReferrerGroup(GroupRecipe):
    name = "group-referrer1"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        groupRef = self.addReference('group-ref')
        self.createGroup('group-second')
        self.add("foo:runtime", groupName='group-second', ref=groupRef)
        self.addNewGroup('group-second')
"""

addAllGroupSimple = """
class AddAllGroup(GroupRecipe):
    name = "group-addall"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.addAll('group-os', 'localhost@rpl:linux/1')
"""


addByDefaultFalseGroup = """
class AddByDefaultFalseGroup(GroupRecipe):
    name = "group-byd"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.add('foo', byDefault=False)
"""


addAllGroup = """
class AddAllGroup(GroupRecipe):
    name = "group-addall"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.addAll('group-os', 'localhost@rpl:linux/1')
        self.replace('foo')
        self.replace('foo')
        self.replace('bam')
        self.createGroup('group-dist')
        self.remove('foo:data', groupName='group-dist')
"""


addAllGroup2 = """
class AddAllGroup(GroupRecipe):
    name = "group-addall"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.addAll('group-os', 'localhost@rpl:linux/1', recurse=False)
        self.remove('group-dist')
"""

addAllGroupNameGroup = """
class AddAllGroupNameGroup(GroupRecipe):
    name = "group-addall-groupname"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.addAll('group-os', 'localhost@rpl:linux/1')
        self.createGroup('group-dist')
        self.createGroup('group-os-extras')
        self.replace('foo', groupName='group-dist')
        self.replace('foo', groupName='group-os-extras')
"""


addAllGroupNoRecurse = """
class AddAllGroup(GroupRecipe):
    name = "group-addall-no-recurse"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.addAll('group-os', 'localhost@rpl:linux/1', recurse=False)
        self.remove('foo:data')
"""


addAllGroupError = """
class AddAllGroupError(GroupRecipe):
    name = "group-addall"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.addAll('group-os', 'localhost@rpl:linux/1')
"""

addCopyGroup = """
class AddCopyGroup(GroupRecipe):
    name = "group-addcopy"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.addCopy('group-os', 'localhost@rpl:linux/1')
        self.VersionConflicts(exceptions = '.*', allowUnusedFilters = True)
"""
addCopyGroup2 = """
class AddCopyGroup(GroupRecipe):
    name = "group-addcopy"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.createGroup('group-foo')
        self.addNewGroup('group-foo') # adds group-foo to group-addcopy
        self.addCopy('group-os', 'localhost@rpl:linux/1', groupName='group-foo')
"""




removeWeakTrovesGroup = """
class RemoveWeakTrovesGroup(GroupRecipe):
    name = "group-removeWeakTroves"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.add("foo")
        self.add("bar", components=['debuginfo'])
        self.removeComponents('devel')
        self.remove('foo:runtime')
"""

removeWeakTrovesRecursiveGroup = """
class RemoveWeakTrovesRecursiveGroup(GroupRecipe):
    name = "group-removeWeakTrovesRecursive"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.add("foo")
        self.createGroup('group-second')
        self.setDefaultGroup('group-second')
        self.addNewGroup('group-removeWeakTrovesRecursive')
        self.removeComponents('devel')
        self.remove('foo:runtime')
"""

removeWeakTrovesGroup2 = """
class RemoveWeakTrovesGroup2(GroupRecipe):
    name = "group-removeWeakTroves2"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.add("group-a")
        self.remove('group-b')
        self.remove('group-c')
"""

removeWeakTrovesGroup3 = """
class RemoveWeakTrovesGroup3(GroupRecipe):
    name = "group-removeWeakTroves3"
    version = "1.0"
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.createGroup('group-a')
        self.add("foo", groupName='group-a')

        self.addNewGroup('group-a')
        self.remove('foo')
"""

replaceWeakTrovesGroup = """
class ReplaceWeakTrovesGroup(GroupRecipe):
    name = "group-replaceWeakTroves"
    version = "1.0"
    clearBuildRequires()

    def setup(self):
        self.add("group-foo")
        self.replace('foo')
"""

replaceNothingGroup = """
class ReplaceNothingGroup(GroupRecipe):
    name = "group-replaceNothing"
    version = "1.0"
    clearBuildRequires()

    def setup(self):
        self.add("foo")
"""


resolveByGroupRecipe = """
class ResolveByGroup(GroupRecipe):
    name = "group-resolveByGroup"
    version = "1.0"
    clearBuildRequires()

    autoResolve = True

    def setup(self):
        self.addResolveSource('group-foo', '1')
        self.addResolveSource('bam:run', ':branch')
        self.add("foo:run")
"""

searchTipFirst = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()

    autoResolve = True

    def setup(self):
        self.setSearchPath('localhost@rpl:linux', 'localhost@rpl:branch')
        self.add("foo:run")
"""


imageAutoResolveGroup1 = """
class ImageAutoResolveGroup(GroupRecipe):
    name = 'group-image'
    version = '2.0.5'
    clearBuildRequires()

    # unspecified autoResolve matches default of True

    def setup(self):
        self.setSearchPath('localhost@rpl:linux', 'localhost@rpl:branch')
        self.add("foo:run")
"""

imageAutoResolveGroup2 = """
class ImageAutoResolveGroup(GroupRecipe):
    name = 'group-image'
    version = '2.0.5'
    clearBuildRequires()

    # unspecified autoResolve matches default of False
    imageGroup = False

    def setup(self):
        self.setSearchPath('localhost@rpl:linux', 'localhost@rpl:branch')
        self.add("foo:run")
"""

imageAutoResolveGroup3 = """
class ImageAutoResolveGroup(GroupRecipe):
    name = 'group-image'
    version = '2.0.5'
    clearBuildRequires()

    # autoResolve True overrides default of False
    imageGroup = False
    autoResolve = True

    def setup(self):
        self.setSearchPath('localhost@rpl:linux', 'localhost@rpl:branch')
        self.add("foo:run")
"""

imageAutoResolveGroup4 = """
class ImageAutoResolveGroup(GroupRecipe):
    name = 'group-image'
    version = '2.0.5'
    clearBuildRequires()

    # autoResolve False overrides default of True
    imageGroup = True
    autoResolve = False

    def setup(self):
        self.setSearchPath('localhost@rpl:linux', 'localhost@rpl:branch')
        self.add("foo:run")
"""

allowMissing1 = """
class AllowMissing(GroupRecipe):
    name = 'group-missing-trove'
    version = '2.0.5'
    clearBuildRequires()

    def setup(r):
        r.setSearchPath(r.cfg.buildLabel)
        r.add('foo:runtime')
        r.add('bar:runtime', allowMissing=True)
"""

allowMissing2 = """
class AllowMissing(GroupRecipe):
    name = 'group-missing-trove'
    version = '2.0.5'
    clearBuildRequires()

    def setup(r):
        r.setSearchPath(r.cfg.buildLabel)
        r.addAll('group-foo')
        r.addAll('group-bar', allowMissing=True)
"""

allowMissing3 = """
class AllowMissing(GroupRecipe):
    name = 'group-missing-trove'
    version = '2.0.5'
    clearBuildRequires()

    def setup(r):
        r.setSearchPath(r.cfg.buildLabel)
        r.add('foo:runtime')
        r.replace('foo:runtime', allowMissing=True)
"""

class GroupTest(rephelp.RepositoryHelper):

    emptyFlavor = deps.Flavor()

    def _get(self, repos, name, version=None, flavor=None):
        
        info = repos.findTrove(self.cfg.installLabelPath, 
                                (name, version, flavor))
        assert(len(info) == 1)
        return repos.getTrove(withFiles=False, *info[0])

    
    def build(self, str, name, dict = {}, serverIdx = 0, returnName = None,
              groupOptions=None, logLevel=None):
        (built, d) = self.buildRecipe(str, name, dict,
                                      groupOptions=groupOptions,
                                      logLevel=logLevel)
        if returnName:
            name, verStr, flavor = [x for x in built if x[0] == returnName][0]
        else:
            name, verStr, flavor = built[0]
        repos = self.openRepository(serverIdx)
        version = VFS(verStr)
        pkg = repos.getTrove(name, version, flavor)
        return pkg

    def _verifyGroup(self, trv, troveList):
        troveList.sort()
        thisList = [ (x[0], x[1].asString()) for x in trv.iterTroveList(
                                                            strongRefs=True) ]
        thisList.sort()
        assert(troveList == thisList)

    def checkFailure(self, recipeStr, recipeName, msg):
        try:
            self.build(recipeStr, recipeName)
        except cook.CookError, e:
            self.assertFalse(str(e) != msg, "incorrect exception: %s" % str(e))
        else:
            self.fail("exception expected")

    def testError(self):
        try:
            self.build(errorGroup, 'errorGroup')
        except errors.RecipeFileError, e:
            # XXX should really fix this error message
            pass
        else:
            self.fail("exception expected")

    def testBasic(self):
        self.makeSourceTrove('test', packageRecipe)
        self.buildRecipe(packageRecipe, "testRecipe")
        self.mkbranch(self.cfg.buildLabel, "@rpl:test1", "test:source")

        origBuildLabel = self.cfg.buildLabel
        self.cfg.buildLabel = versions.Label("localhost@rpl:test1")
        self.buildRecipe(packageRecipe2, "testRecipe")
        self.cfg.buildLabel = origBuildLabel

        group = self.build(basicGroup, "basicGroup")
        ver = "/" + self.cfg.buildLabel.asString() + '/1.0-1-1'
        ver2 = "/" + self.cfg.buildLabel.asString() + '/1.0-1-0/test1/1.1-1-1'
        self.verifyTroves(group, [('test', ver2, self.emptyFlavor),
                                  ('test', ver, self.emptyFlavor)])
        assert(str(group.getProvides()) == 'trove: group-test')
        assert(group.isCollection())
        assert(set(group.iterTroveList(weakRefs=True, strongRefs=False))
               == set([('test:runtime', VFS(ver), self.emptyFlavor),
                       ('test:runtime', VFS(ver2), self.emptyFlavor)]))
        self.assertTrue(str(group.getBuildFlavor()))

        primary = self.build(basicSplitGroup, "splitGroup", 
                             returnName='group-first')
        repos = self.openRepository()
        secondary = repos.getTrove('group-second', primary.getVersion(),
                                   primary.getFlavor())
        tertiary = repos.getTrove('group-third', primary.getVersion(),
                                  primary.getFlavor())
        self.verifyTroves(primary, [('test', ver, self.emptyFlavor),
                                    ('group-second', ver, self.emptyFlavor) ])
        self.verifyTroves(secondary, [('test', ver2, self.emptyFlavor) ])
        self.verifyTroves(tertiary, [('test', ver2, self.emptyFlavor) ])

        try:
            self.build(cyclicGroup, "cyclicGroup")
        except cook.CookError, msg:
            assert(str(msg) == 
                   "cycle in groups:"
                   "\n  ['group-first', 'group-second']")
        else:
            assert(0)

        try:
            self.build(cyclicGroup2, "cyclicGroup")
        except errors.CookError, msg:
            assert(str(msg).endswith("group group-first cannot contain itself"))
        else:
            assert(0)

    def testUpdates(self):
        pkgVer1 = self.buildRecipe(packageRecipe, "testRecipe")[0][0][1]
        grpVer1 = self.buildRecipe(pkgGroup, "pkgGroup")[0][0][1]
        pkgVer2 = self.buildRecipe(packageRecipe, "testRecipe")[0][0][1]
        grpVer2 = self.buildRecipe(pkgGroup, "pkgGroup")[0][0][1]
        pkgVer3 = self.buildRecipe(packageRecipe, "testRecipe")[0][0][1]
        grpVer3 = self.buildRecipe(pkgGroup, "pkgGroup")[0][0][1]
        grpVer4 = self.buildRecipe(pkgGroup, "pkgGroup")[0][0][1]

        self.updatePkg(self.rootDir, 'group-pkg', version = grpVer1)
        self.updatePkg(self.rootDir, 'test', version = grpVer2)
        self.updatePkg(self.rootDir, 'group-pkg', version = grpVer3)
        # we should verify the versions of test, test:runtime, and group-pkg
        # XXX

        self.resetRoot()
        self.updatePkg(self.rootDir, 'group-pkg', version = grpVer1)
        self.updatePkg(self.rootDir, 'test', version = grpVer2)
        self.updatePkg(self.rootDir, 'group-pkg', version = grpVer2)

        self.resetRoot()
        self.updatePkg(self.rootDir, 'group-pkg', version = grpVer3)
        self.updatePkg(self.rootDir, 'group-pkg', version = grpVer4)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTroveByName('test'))

    def testSelective(self):
        self.buildRecipe(recipes.doubleRecipe1, "Double")
        self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        self.buildRecipe(selectiveGroup, "SelectiveGroup")
        self.updatePkg(self.rootDir, 'group-selective')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTroveByName('double'))
        assert(not db.hasTroveByName('testcase'))

    # known broken test
    def testMultipleLabels(self):
        self.buildRecipe(recipes.testRecipe1, "TestRecipe1")

        newLabel = versions.Label("localhost1@rpl:linux")
        repos1 = self.openRepository(1)
        oldLabel = self.cfg.buildLabel
        self.cfg.buildLabel = newLabel
        self.buildRecipe(recipes.bashRecipe, "Bash")
        self.cfg.buildLabel = oldLabel
    
        group = self.build(multiLabelGroup, "multiLabelGroup", serverIdx=0)
        self._verifyGroup(group,
                    [ ( 'testcase', '/localhost@rpl:linux/1.0-1-1' ),
                      ( 'bash', '/localhost1@rpl:linux/0-1-1' ) ] )
        self.updatePkg(self.cfg.root, "group-multi-label-test", "localhost@rpl:linux")

        group = self.build(labelPathGroup, "labelPathGroup", serverIdx=0)
        self._verifyGroup(group,
                    [ ( 'testcase', '/localhost@rpl:linux/1.0-1-1' ),
                      ( 'bash', '/localhost1@rpl:linux/0-1-1' ) ] )

        self.cfg.buildLabel = newLabel
        self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        self.cfg.buildLabel = oldLabel

        group = self.build(labelPathGroup, "labelPathGroup", serverIdx=0)
        self._verifyGroup(group,
                    [ ( 'testcase', '/localhost1@rpl:linux/1.0-1-1' ),
                      ( 'bash', '/localhost1@rpl:linux/0-1-1' ) ] )

    def testFlavors(self):
        self.resetRepository()
        self.resetWork()
        self.repos = self.openRepository()

        recipe = self.addTestPkg(1, binary=True, flags='Use.readline', 
                                                      localflags='foo')
        rc, result = self.captureOutput(self.cookTestPkg, 1)
        flavor = rc[0][0][2]
        if use.Arch.x86:
            flavorBase = '1#x86|'
        elif use.Arch.x86_64:
            flavorBase = '1#x86_64|'
        else:
            raise NotImplementedError, 'modify test for this arch'
            
        assert(flavor.freeze() == flavorBase + '5#use:readline:~test1.foo')
        #rc, result = self.captureOutput(self.cookTestPkg, 1)
        # now cook a second version where foo is false...this one
        # will not get into the group...
        oldBuildFlavor = self.cfg.buildFlavor
        self.overrideBuildFlavor('!test1.foo')
        rc, result = self.captureOutput(self.cookTestPkg, 1)
        flavor = rc[0][0][2]
        assert(flavor.freeze() == flavorBase + '5#use:readline:~!test1.foo')
        self.cfg.buildFlavor = oldBuildFlavor
        recipe = self.addTestPkg(2)
        rc = self.cookTestPkg(2)
        flavor = rc[0][0][2]
        assert(flavor.freeze() == '')
        group = self.build(flavoredGroup1, 'flavoredGroup')
        flavor = group.getFlavor()
        self.assertEquals(flavor.freeze(), flavorBase + '5#use:~!bootstrap:~group-test.bar:readline:~test1.foo')
        use.LocalFlags._clear()
        group = self.build(flavoredGroup2, 'flavoredGroup')
        flavor = group.getFlavor()
        self.assertEquals(flavor.freeze(), flavorBase + '5#use:~!bootstrap:~group-test.test1:readline:~!test1.foo')
        troves = [ x for x in group.iterTroveList(strongRefs=True) ] 
        assert(len(troves) == 2)
        test1 = [ x for x in troves if x[0] == 'test1'][0]
        test1flavor = test1[2]
        self.assertEquals(test1flavor.freeze(), flavorBase + '5#use:readline:~!test1.foo')
        # now test when both flavors are included...in this case,
        # test1.foo should be dropped from the final group's flavor
        group = self.build(flavoredGroup3, 'flavoredGroup')
        flavor = group.getFlavor()
        assert(flavor.freeze() == flavorBase + '5#use:readline')
        troves = [ x for x in group.iterTroveList(strongRefs=True) ] 
        assert(len(troves) == 2)
        flavors = set([ x[2].freeze() for x in troves if x[0] == 'test1'])
        assert(flavors == set((flavorBase + '5#use:readline:~!test1.foo', 
                               flavorBase + '5#use:readline:~test1.foo'))) 

    def testIdenticalFlavors(self):
        # CNY-3401
        self.repos = self.openRepository()

        recipe = self.addTestPkg(1, binary=True, localflags='foo')
        rc, result = self.captureOutput(self.cookTestPkg, 1)
        flavor = rc[0][0][2]
        if use.Arch.x86:
            flavorBase = '1#x86|'
        elif use.Arch.x86_64:
            flavorBase = '1#x86_64|'
        else:
            raise NotImplementedError, 'modify test for this arch'
            
        self.addComponent('group-test:source', '1.0',
                          [('group-test.recipe', almostFlavoredGroup)])
        log.setVerbosity(log.INFO)
        groupOptions = cook.GroupCookOptions(alwaysBumpCount=True,
                                             errorOnFlavorChange=False,
                                             shortenFlavors=True)
        self.logFilter.add()
        self.captureOutput(self.cookItem, self.repos, self.cfg,
            ('group-test', None, [
                 deps.parseFlavor('readline'),
                 deps.parseFlavor('!readline'),
             ]), groupOptions=groupOptions)
        msg = ('+ Removed duplicate flavor of group '
               'group-test[~!bootstrap,~test1.foo is: ')
        if use.Arch.x86_64:
            msg += 'x86_64]'
        elif use.Arch.x86:
            msg += 'x86]'
        else:
            assert(0) # better message above already; can't get here

        assert(msg in self.logFilter.records)

    def testArchFlavors(self):
        # add an x86 version
        self.addTestPkg(1, binary=True)
        self.repos = self.openRepository()
        built, str = self.captureOutput(self.cookTestPkg, 1)
        if use.Arch.x86:
            assert(built[0][0][2].freeze() == '1#x86')
            arch = 'x86'
        elif use.Arch.x86_64:
            assert(built[0][0][2].freeze() == '1#x86_64')
            arch = 'x86_64'
        else:
            raise NotImplementedError, 'modify test for this arch'
        os.chdir(self.workDir)
        self.checkout("test1")
        os.chdir("test1")
        shutil.copy2(os.path.join(resources.get_archive(), 
                                  'multilib-sample.tar.bz2'), '.')
        self.writeFile("test1.recipe", archFlavoredRecipe)
        self.addfile("multilib-sample.tar.bz2")
        self.commit()
        kwargs={'macros': {'lib': 'lib64'}}
        self.logFilter.add()
        self.overrideBuildFlavor('is: x86 x86_64')
        built, str = self.captureOutput(self.cookTestPkg, 1, **kwargs)
        self.logFilter.clear()
        assert(built[0][0][2].freeze() == '1#x86|1#x86_64')
        self.overrideBuildFlavor('is:x86_64')
        # if flavor merging for add is working correctly, we'll get the
        # plain x86 trove.  If not, we'll get the multilib version

        group = self.build(archFlavoredGroup % ('is: '+arch), 'flavoredGroup')
        flavor = group.getFlavor()
        assert(flavor.freeze() == '1#'+arch)

    def testGroupPrimaries(self):
        repos = self.openRepository()
        self.addComponent('test:runtime', '1.0')
        dir = tempfile.mkdtemp(dir=self.workDir)
        os.chdir(dir)
        self.writeFile('group-test.recipe', primariesGroup1)
        self.captureOutput(self.cookItem, repos, self.cfg, 'group-test.recipe')
        cs = changeset.ChangeSetFromFile('group-test-1.0.ccs')
        primaryTroves = set([x[0] for x in cs.getPrimaryTroveList()])
        assert(primaryTroves == set(['group-second']))

        self.writeFile('group-test.recipe', primariesGroup2)
        self.captureOutput(self.cookItem, repos, self.cfg, 'group-test.recipe')
        cs = changeset.ChangeSetFromFile('group-test-1.0.ccs')
        primaryTroves = set([x[0] for x in cs.getPrimaryTroveList()])
        assert(primaryTroves == set(['group-second', 'group-test']))

    def testDepCheck(self):
        depCheckRecipe = """
class DepCheckRecipe(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    depCheck = True
    imageGroup = False
    clearBuildRequires()

    def setup(self):
        self.add('test:runtime')
"""
        
        self.addComponent('test:runtime', '1.0-1-1',
                 fileContents = [ ( 'file', 'contents', None,
                                    deps.parseDep('trove: other:runtime') ) ] )
        self.addComponent('other:runtime', '1.0-1-1')
        try:
            self.build(depCheckRecipe, 'DepCheckRecipe')
        except errors.CookError, msg:
            self.assertEqual(str(msg), '''\
Dependency failure
Group group-test has unresolved dependencies:
test:runtime
\ttrove: other:runtime''')
        else:
            assert(0)
        depCheckRecipe += '        self.add("other:runtime")'
        self.build(depCheckRecipe, 'DepCheckRecipe')



    def testAutoResolve(self):
        autoResolveRecipe = """
class AutoResolveRecipe(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    autoResolve = True
    clearBuildRequires()
    def setup(self):
        self.add('test:runtime')
"""
        
        self.addComponent('test:runtime', '1.0',
                 fileContents = [ ( 'file', 'contents', None,
                                    deps.parseDep('trove: other:runtime') ) ] )
        self.addComponent('test:devel', '1.0')
        self.addComponent('other:runtime', '1.0',
                 fileContents = [ ( 'file2', 'contents', None,
                                    deps.parseDep('trove: other2:runtime') ) ] )
        self.addComponent('other2:runtime', '1.0')
        self.addCollection('other', '1.0', [':runtime', ':devel'])

        group = self.build(autoResolveRecipe, 'AutoResolveRecipe')

        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        d = deps.parseFlavor('')
        expectedTroves = [('other', v, d), ('test:runtime', v, d),
                          ('other2:runtime', v, d)]
        expectedTroves.sort()
        actualTroves = [ x for x in group.iterTroveList(strongRefs=True) ]
        actualTroves.sort()
        assert(actualTroves == expectedTroves)

        byDefault = dict((x[0], group.includeTroveByDefault(*x)) \
                    for x in group.iterTroveList(weakRefs=True, 
                                                 strongRefs=False))
        assert(byDefault.pop('other:runtime'))
        assert(not byDefault.pop('other:devel'))

        actualTroves = set(group.iterTroveList(weakRefs=True))

    def testAutoResolveOutput(self):
        autoResolveRecipe = """
class AutoResolveRecipe(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    checkOnlyByDefaultDeps = True
    autoResolve = True
    clearBuildRequires()
    def setup(self):
        self.add('test:runtime', byDefault=True)
"""

        self.addComponent('test:runtime', '1.0',
                 fileContents = [ ( 'file', 'contents', None,
                                deps.parseDep('trove: other:runtime') ) ] )
        self.addComponent('test:devel', '1.0')
        self.addComponent('other:runtime', '1.0')
        self.addCollection('other', '1.0', [':runtime', ':devel'])


        def testOutput(recipe, byDefault=True, checkOnlyByDefaultDeps=True):
            defaultStr = byDefault and 'byDefault' or 'not byDefault'
            if not byDefault:
                recipe = recipe.replace('byDefault=True', 'byDefault=False')
            if not checkOnlyByDefaultDeps:
                recipe = recipe.replace('checkOnlyByDefaultDeps = True',
                                        'checkOnlyByDefaultDeps = False')

            self.logFilter.add()
            self.captureOutput(rephelp.RepositoryHelper.build, self, 
                                recipe, 'AutoResolveRecipe', 
                                logLevel=log.INFO)

            assert('+ test:runtime=1.0-1-1 resolves deps by including:'
                   in self.logFilter.records)
            assert('+ \tother:runtime=1.0-1-1 [%s]' % defaultStr
                   in self.logFilter.records)

            self.cfg.fullFlavors = self.cfg.fullVersions = True
            self.logFilter.clear()
            self.logFilter.add()

            self.captureOutput(rephelp.RepositoryHelper.build, self, 
                                recipe, 'AutoResolveRecipe', 
                                logLevel=log.INFO)
            assert('+ test:runtime=/localhost@rpl:linux/1.0-1-1[] resolves '
                   'deps by including:' in self.logFilter.records)
            assert('+ \tother:runtime=/localhost@rpl:linux/1.0-1-1[] [%s]'
                   % defaultStr in self.logFilter.records)

            self.cfg.fullFlavors = self.cfg.fullVersions = False

        testOutput(autoResolveRecipe, byDefault=True,
                   checkOnlyByDefaultDeps=True)
        testOutput(autoResolveRecipe, byDefault=False,
                   checkOnlyByDefaultDeps=False)

    def testAutoResolveLabelPath(self):
        autoResolveRecipe = """
class AutoResolveRecipe(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    autoResolve = True
    clearBuildRequires()
    def setup(self):
        self.setLabelPath('localhost@rpl:linux', 'localhost@rpl:other')
        self.add('test:runtime')
"""

        self.addComponent('test:runtime', '1.0-1-1',
                 fileContents = [ ( 'file', 'contents', None,
                                    deps.parseDep('trove: other:runtime') ) ] )
        self.addComponent('other:runtime', '/localhost@rpl:other/1.0-1-1')
        group = self.build(autoResolveRecipe, 'AutoResolveRecipe')
        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        v2 = versions.VersionFromString('/localhost@rpl:other/1.0-1-1')
        d = deps.parseFlavor('')
        expectedTroves = [('other:runtime', v2, d), ('test:runtime', v, d)]
        expectedTroves.sort()
        actualTroves = [ x for x in group.iterTroveList(strongRefs=True) ]
        actualTroves.sort()
        assert(actualTroves == expectedTroves)

    def testResolveByGroup(self):
        for v in '1', '2':
            self.addComponent('prov1:run', v, filePrimer=1,
                               provides='trove:prov1:run(foo)')
            self.addCollection('prov1', v, [':run'])
            self.addCollection('group-foo', v, ['prov1'])
        self.addComponent('bam:run', ':branch/1', filePrimer=2)

        self.addComponent('foo:run', filePrimer=3,
                          requires='trove: prov1:run(foo) trove: bam:run')

        group = self.build(resolveByGroupRecipe, 'ResolveByGroup')
        childTups = list(group.iterTroveList(strongRefs=True))
        prov1 = [ x for x in childTups if x[0] == 'prov1'][0]
        assert(str(prov1[1].trailingRevision().getVersion()) == '1')
        assert('bam:run' in [ x[0] for x in childTups])

        # now add a version of prov1 that does _not_ provide foo.
        # this should cause dep resolution to fail, even though there
        # are solutions out there
        v = '1-1-2'
        self.addComponent('prov1:run', v, filePrimer=1)
        self.addCollection('prov1', v, [':run'])
        self.addCollection('group-foo', v, ['prov1'])
        try:
            group = self.build(resolveByGroupRecipe, 'ResolveByGroup')
            assert(0)
        except conaryclient.DependencyFailure, e:
            assert(str(e) == 'The following dependencies could not be resolved:\n    foo:run=1.0-1-1:\n\ttrove: prov1:run(foo)')


    def testSwitchToNotByDefault(self):
        groupTest1 = """
class GroupTest(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    imageGroup = False
    clearBuildRequires()
    def setup(self):
        self.add('test:runtime', '1.0')
        self.add('test2:runtime', '1.1')
"""
        groupTest2 = """
class GroupTest(GroupRecipe):
    name = 'group-test'
    version = '2.0'
    imageGroup = False
    clearBuildRequires()
    def setup(self):
        self.add('test:runtime', '2.0', byDefault=False)
        self.add('test2:runtime', '1.1', byDefault=False)
"""
        self.addComponent('test:runtime', '1.0-1-1')
        self.addComponent('test:runtime', '2.0-1-1')
        self.addComponent('test2:runtime', '1.1-1-1', filePrimer=1)
        group = self.build(groupTest1, 'GroupTest')
        group2 = self.build(groupTest2, 'GroupTest')
        self.updatePkg(self.rootDir,'group-test=1.0')
        self.updatePkg(self.rootDir,'test:runtime=2.0')
        self.updatePkg(self.rootDir,'group-test=2.0')
        db = database.Database(self.rootDir, self.cfg.dbPath)

        n,v,f = db.trovesByName('group-test')[0]
        testTup = db.trovesByName('test:runtime')[0]
        test2Tup = db.trovesByName('test2:runtime')[0]
        trv = db.getTrove(n,v,f, pristine=False)
        assert(not trv.includeTroveByDefault(*testTup))
        assert(not trv.includeTroveByDefault(*test2Tup))
        trv = db.getTrove(n,v,f, pristine=True)
        assert(not trv.includeTroveByDefault(*testTup))
        assert(not trv.includeTroveByDefault(*test2Tup))

    def testDepCheckRecurse(self):
        depCheckRecipe = """
class DepCheckRecipe(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.createGroup('group-os', depCheck=True, 
                         checkOnlyByDefaultDeps=True)

        self.add('prov:runtime')

        self.setDefaultGroup('group-os')
        self.addNewGroup('group-dist')
        self.add('req')
        self.add('other:runtime', byDefault=False)
"""
        
        myDep = deps.parseDep('trove: prov:runtime')
        otherReqDep = deps.parseDep('trove: other2:runtime')
        noflavor = deps.Flavor()
        self.addComponent('req:runtime', '1.0-1-1', requires=myDep)
        self.addComponent('req:test', '1.0-1-1', requires=otherReqDep)
        self.addCollection("req", "1.0-1-1",
                                    [ ("req:runtime", "1.0-1-1"),
                                      ('req:test', '1.0-1-1', noflavor, False)])
        self.addComponent('prov:runtime', '1.0-1-1', provides=myDep,
                                                              filePrimer=1)
        self.addComponent('other:runtime', '1.0-1-1', 
                                   requires=otherReqDep, filePrimer=2)
        self.build(depCheckRecipe, 'DepCheckRecipe')

    def testDepCheckNotByDefault(self):
        depCheckRecipe = """
class DepCheckRecipe(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    imageGroup = False
    clearBuildRequires()
    def setup(self):
        self.createGroup('group-os', depCheck=True, 
                         checkOnlyByDefaultDeps=False)
        self.addNewGroup('group-dist', groupName='group-os')

        self.add('req:runtime', groupName='group-os')

        self.setDefaultGroup('group-dist')
        self.add('prov:runtime')
        self.add('other:runtime', byDefault=False)
"""
        myDep = deps.parseDep('trove: prov:runtime')
        otherReqDep = deps.parseDep('trove: other2:runtime')
        noflavor = deps.Flavor()
        self.addComponent('req:runtime', '1.0-1-1', requires=myDep)
        self.addComponent('req:test', '1.0-1-1', requires=otherReqDep)
        self.addCollection("req", "1.0-1-1",
                                    [ ("req:runtime", "1.0-1-1"),
                                      ('req:test', '1.0-1-1', noflavor, False)])
        self.addComponent('prov:runtime', '1.0-1-1', provides=myDep,
                                    filePrimer=1)
        self.addComponent('other:runtime', '1.0-1-1', 
                                   requires=otherReqDep, filePrimer=2)
        errMsg = ('Dependency failure\n'
                  'Group group-os has unresolved dependencies:\n'
                  'other:runtime\n'
                  '\ttrove: other2:runtime')
        try:
            self.build(depCheckRecipe, 'DepCheckRecipe')
        except cook.CookError, msg:
            assert(str(msg) == errMsg)
        else:
            assert 0
        depCheckRecipe = depCheckRecipe.replace('checkOnlyByDefaultDeps=False',
                                                'checkOnlyByDefaultDeps=True')
        self.build(depCheckRecipe, 'DepCheckRecipe')

        depCheckRecipe2 = """
class DepCheckRecipe(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.createGroup('group-os', depCheck=True, 
                         checkOnlyByDefaultDeps=False)
        self.setDefaultGroup('group-os')
        self.addNewGroup('group-dist')
        self.add('req')

        self.setDefaultGroup('group-dist')
        self.add('prov:runtime')
"""
        errMsg = ('Dependency failure\n'
                  'Group group-os has unresolved dependencies:\n'
                  'req:test\n'
                  '\ttrove: other2:runtime')
        try:
            self.build(depCheckRecipe2, 'DepCheckRecipe')
        except cook.CookError, msg:
            assert(str(msg) == errMsg)
        else:
            assert(0)

        self.addComponent('other2:runtime', '1.0-1-1', provides=otherReqDep, filePrimer=3)
        self.build(depCheckRecipe, 'DepCheckRecipe')

    def testAutoResolveRecurse(self):
        autoResolveRecipe = """
class AutoResolveRecipe(GroupRecipe):
    # tests to ensure auto resolve leaves out by default=False comps as well 
    name = 'group-dist'
    version = '1.0'
    checkOnlyByDefaultDeps = True
    clearBuildRequires()

    imageGroup = False

    def setup(self):
        self.createGroup('group-os', autoResolve=True)

        self.add('prov:runtime', groupName='group-dist')

        self.setDefaultGroup('group-os')
        self.addNewGroup('group-dist')
        self.add('req:runtime')
        self.add('other:runtime', byDefault=False)
"""
        
        myDep = deps.parseDep('trove: prov:runtime')
        otherReqDep = deps.parseDep('trove: other2:runtime')
        self.addComponent('req:runtime', '1.0-1-1', requires=myDep, 
                                                             filePrimer=1)
        self.addComponent('prov:runtime', '1.0-1-1', provides=myDep,
                                                              filePrimer=2)
        self.addComponent('other:runtime', '1.0-1-1',  
                                   requires=otherReqDep, filePrimer=3)
        self.build(autoResolveRecipe, 'AutoResolveRecipe')
        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        d = deps.parseFlavor('')
        repos = self.openRepository()
        group = repos.getTrove('group-os', v, d)
        expectedTroves = [('req:runtime', v, d), ('group-dist', v, d),
                          ('other:runtime', v,d)]
        expectedTroves.sort()
        actualTroves = [ x for x in group.iterTroveList(strongRefs=True) ]
        actualTroves.sort()
        assert(actualTroves == expectedTroves)

    def testExcludeTroves(self):
        self.addComponent('foo:runtime', '1.0-1-1')
        self.addCollection("foo", "1.0-1-1",
                                    [ ("foo:runtime", "1.0-1-1") ])
        self.addCollection("group-a", "1.0-1-1",
                                    [ ("foo", "1.0-1-1") ])
        
        self.addComponent('foo:runtime', '2.0-1-1')
        self.addComponent('foo:lib', '2.0-1-1')
        self.addCollection("foo", "2.0-1-1",
                                    [ ("foo:runtime", "2.0-1-1"),
                                      ("foo:lib", "2.0-1-1") ])
        self.addCollection("group-a", "2.0-1-1",
                                    [ ("foo", "2.0-1-1") ])

        self.resetRoot()
        self.cfg.excludeTroves.addExp("foo:lib")
        self.updatePkg(self.rootDir, "group-a", '1.0-1-1')
        self.updatePkg(self.rootDir, "group-a", '2.0-1-1')
        db = database.Database(self.cfg.root, self.cfg.dbPath)
        assert([ x for x in sorted(db.iterAllTroveNames()) ] == 
                                ['foo', 'foo:runtime', 'group-a'] )
        self.cfg.excludeTroves = RegularExpressionList()


    def testCookEmptyGroup(self):
        emptyGroup = """
class EmptyGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    resolveDependencies = True

    imageGroup = False
    clearBuildRequires()

    def setup(self):
        self.add("test:runtime")
        self.remove("test:runtime")
"""
        self.addComponent('test:runtime', '1.0-1-1')
        try:
            self.build(emptyGroup, 'EmptyGroup')
        except cook.CookError, msg:
            assert(str(msg) == 'group-test has no troves in it')
        else:
            assert(0)

    def testGroupOrdering(self):
        groupOrderingRecipe = """
class GroupOrdering(GroupRecipe):
    # tests to ensure auto resolve leaves out by default=False comps as well 
    name = 'group-dist'
    version = '1.0'
    autoResolve = True
    clearBuildRequires()

    def setup(r):
        inner = 'group-inner'
        outer = 'group-outer'

        r.createGroup(inner)
        r.addNewGroup(inner)
        r.add('inner:runtime', groupName=inner)

        r.createGroup(outer)
        r.addNewGroup(outer)
        r.addNewGroup(inner, groupName=outer)
"""
        
        self.addComponent('inner:runtime', '1.0-1-1')
        self.addComponent('outer:runtime', '1.0-1-1')
        self.build(groupOrderingRecipe, 'GroupOrdering')
        # if it builds, that means that the groups were expanded in the right
        # order

    def testRemoveWeak(self):
        grpSize = 0
        self.addComponent('foo:runtime', '1.0')
        self.addComponent('foo:devel', '1.0', filePrimer=1)
        trv = self.addComponent('foo:lib', '1.0', filePrimer=2)
        grpSize += trv.getSize()

        self.addCollection('foo', '1.0', [':devel', ':runtime', 
                                                   ':lib'], calcSize=True)

        trv = self.addComponent('bar:runtime', '1.0', filePrimer=3)
        grpSize += trv.getSize()

        self.addComponent('bar:debuginfo', '1.0', filePrimer=4)
        self.addCollection('bar', '1.0', 
                                    [':runtime', (':debuginfo', False)],
                                    calcSize=True)
        (n,v,f) = self.buildRecipe(removeWeakTrovesGroup, 
                                      'RemoveWeakTrovesGroup')[0][0]
        v = VFS(v)

        repos = self.openRepository()

        trv = repos.getTrove(n, v, f)
        assert(grpSize == trv.getSize())

        byDefault = dict((x[0], trv.includeTroveByDefault(*x)) \
                    for x in trv.iterTroveList(weakRefs=True, strongRefs=True))

        assert(byDefault.pop('foo:devel') == False)
        assert(byDefault.pop('foo:runtime') == False)
        assert(byDefault.pop('foo:lib') == True)
        assert(byDefault.pop('foo') == True)
        assert(byDefault.pop('bar') == True)
        assert(byDefault.pop('bar:runtime') == False)
        assert(byDefault.pop('bar:debuginfo') == True)
        assert(not byDefault)
        self.checkUpdate([n], [n, 'foo', 'foo:lib', 'bar', 'bar:debuginfo'])

        (n, v, f) = self.buildRecipe(removeWeakTrovesRecursiveGroup, 
                                        'RemoveWeakTrovesRecursiveGroup')[0][0]
        v = VFS(v)
        trv = repos.getTrove('group-removeWeakTrovesRecursive', v, f)

        byDefault = dict((x[0], trv.includeTroveByDefault(*x)) \
                    for x in trv.iterTroveList(weakRefs=True, strongRefs=True))
        assert(byDefault.pop('foo:devel') == True)
        assert(byDefault.pop('foo:runtime') == True)
        assert(byDefault.pop('foo:lib') == True)
        assert(byDefault.pop('foo') == True)
        assert(not byDefault)

        trv = repos.getTrove('group-second', v, f)
        byDefault = dict((x[0], trv.includeTroveByDefault(*x)) \
                    for x in trv.iterTroveList(weakRefs=True, strongRefs=True))

        assert(byDefault.pop('group-removeWeakTrovesRecursive') == True)
        assert(byDefault.pop('foo:devel') == False)
        assert(byDefault.pop('foo:runtime') == False)
        assert(byDefault.pop('foo:lib') == True)
        assert(byDefault.pop('foo') == True)
        assert(not byDefault)

    def testRemoveWeak2(self):
        # reference the troves foo and bar by two different groups, 
        # we remove group-b and group-c, but leave group-d.  So 
        # foo should be byDefault=False (all refs are removed) while
        # bar should be byDefault=True.
        self.addComponent('foo:runtime', '1')
        self.addCollection('foo', '1', [':runtime'])
        self.addComponent('bar:runtime', '1', filePrimer=2)
        self.addCollection('bar', '1', [':runtime'])
        self.addCollection('group-b', '1', ['foo']) #removed
        self.addCollection('group-c', '1', ['foo', 'bar']) #removed
        self.addCollection('group-d', '1', ['bar']) #kept
        self.addCollection('group-a', '1', ['group-b', 'group-c', 'group-d'])

        trv = self.build(removeWeakTrovesGroup2, 'RemoveWeakTrovesGroup2')
        byDefault = dict((x[0], trv.includeTroveByDefault(*x)) \
                    for x in trv.iterTroveList(weakRefs=True, strongRefs=True))

        assert(byDefault.pop('group-a') == True)
        assert(byDefault.pop('group-b') == False)
        assert(byDefault.pop('group-c') == False)
        assert(byDefault.pop('foo') == False)
        assert(byDefault.pop('foo:runtime') == False)
        assert(byDefault.pop('group-d') == True)
        assert(byDefault.pop('bar') == True)
        assert(byDefault.pop('bar:runtime') == True)
        assert(not byDefault)


    def testRemoveWeak3(self):
        # we include a trove by addNewGroup and then we remove it.
        self.addComponent('foo:runtime', '1')
        self.addCollection('foo', '1', [':runtime'])
        trv = self.build(removeWeakTrovesGroup3, 'RemoveWeakTrovesGroup3')
        (n,v,f) = trv.getNameVersionFlavor()
        repos = self.openRepository()
        trv = repos.getTrove('group-removeWeakTroves3', v, f)
        byDefault = dict((x[0], trv.includeTroveByDefault(*x)) \
                    for x in trv.iterTroveList(weakRefs=True, strongRefs=True))
        assert(byDefault.pop('group-a') == True)
        assert(byDefault.pop('foo') == False)
        assert(byDefault.pop('foo:runtime') == False)

    def testReplaceWeak(self):
        # reference the troves foo and bar by two different groups, 
        # we remove group-b and group-c, but leave group-d.  So 
        # foo should be byDefault=False (all refs are removed) while
        # bar should be byDefault=True.
        self.addComponent('foo:runtime', '1')
        self.addCollection('foo', '1', [':runtime'])
        self.addComponent('foo:runtime', '2')
        self.addCollection('foo', '2', [':runtime'])
        self.addCollection('group-foo', '1', ['foo'])
        self.logFilter.add()
        self.build(replaceWeakTrovesGroup, 'ReplaceWeakTrovesGroup')
        self.logFilter.compare(['''\
warning: Cannot replace the following troves in group-replaceWeakTroves:

   foo=/localhost@rpl:linux/1-1-1[]
   (Included by adding group-foo=/localhost@rpl:linux/1-1-1[])

You are not building the containing group, so conary does not know where to add the replacement.
To resolve this problem, use r.addCopy for the containing group instead of r.add.
'''])

    def testReplaceMatchesNothing(self):
        self.addComponent('foo:runtime', '1')
        self.addCollection('foo', '1', [':runtime'])
        self.addComponent('nothing:runtime', '1')
        self.addCollection('nothing', '1', [':runtime'])
        self.logFilter.add()
        self.build(replaceNothingGroup + '\tself.replace("nothing")\n',
                   'ReplaceNothingGroup')
        self.build(replaceNothingGroup + '\tself.replace("nothing", groupName="group-replaceNothing")\n', 'ReplaceNothingGroup')
        self.logFilter.compare(['''\
warning: Could not find troves to replace in any group:
    nothing
''','''\
warning: Could not find troves to replace in group-replaceNothing:
    nothing
'''])

        self.build(replaceNothingGroup + '\tself.replace("nothing",\n'
                                         '\t\t           allowNoMatch=True)\n', 
                   'ReplaceNothingGroup')
        self.build(replaceNothingGroup + '\tself.replace("nothing",\n'
                         '\t             allowNoMatch=True,\n'
                         '\t             groupName="group-replaceNothing")\n',
                   'ReplaceNothingGroup')


    def testRemoveMatchesNothing(self):
        self.addComponent('foo:runtime', '1')
        self.addCollection('foo', '1', [':runtime'])
        self.addComponent('nothing:runtime', '1')
        self.addCollection('nothing', '1', [':runtime'])
        self.logFilter.add()
        self.build(replaceNothingGroup + '\tself.remove("nothing", "1.0", '
                                                        'flavor="foo")\n',
                   'ReplaceNothingGroup')
        self.logFilter.compare(['''\
warning: Could not find troves to remove in group-replaceNothing:
    nothing=1.0[foo]
'''])
        self.build(replaceNothingGroup + '\tself.remove("nothing",\n'
                                         '\t\t           allowNoMatch=True)\n', 
                   'ReplaceNothingGroup')



    def testAddByDefaultFalse(self):
        fooRun = self.addComponent('foo:runtime', '1')
        foo = self.addCollection('foo', '1', [':runtime'])
        trv = self.build(addByDefaultFalseGroup, 'AddByDefaultFalseGroup')
        assert(not trv.includeTroveByDefault(*foo.getNameVersionFlavor()))
        assert(not trv.includeTroveByDefault(*fooRun.getNameVersionFlavor()))

    def testIncludePackage(self):
        noFlavor = deps.Flavor()

        self.addComponent('foo:devel', '1.0')
        self.addComponent('foo:runtime', '1.0')
        self.addComponent('bar:devel', '1.0', filePrimer=1)
        self.addCollection('foo', '1.0', [':devel', ':runtime'])
        # NOTE no package bar

        (n, v, f) = self.buildRecipe(compGroup, 'compGroup')[0][0]
        v = VFS(v)

        repos = self.openRepository()
        trv = repos.getTrove(n, v, f, withFiles=False)

        strongRefs = set(x[0] for x in trv.iterTroveList(strongRefs=True))
        assert(strongRefs == set(['foo', 'bar:devel']))

        byDefault = dict((x[0], trv.includeTroveByDefault(*x)) \
                    for x in trv.iterTroveList(weakRefs=True, strongRefs=False))
        assert(byDefault.pop('foo:devel') == True)
        assert(byDefault.pop('foo:runtime') == False)
        assert(not byDefault)

    def testReferences(self):
        for v in ('1', '2'):
            self.addComponent('foo:runtime', v, )
            self.addComponent('foo:data',  v) 

        self.addCollection('foo', '1', [ 'foo:runtime' ])
        self.addCollection('group-ref', '1', [ 'foo' ])

        self.build(referrerGroup1, 'ReferrerGroup')


    def testAddAll(self):
        for idx, v in enumerate(('1', '2', '/localhost@rpl:branch/3')):
            self.addComponent('foo:runtime', v, filePrimer=idx * 3)
            self.addComponent('foo:data',  v, filePrimer=idx * 3 + 1) 
            self.addCollection('foo', v, [ 'foo:runtime', 'foo:data' ])

            self.addComponent('bam:runtime', v, filePrimer=idx * 3 + 2)
            self.addCollection('bam', v, [ ':runtime' ])



        self.addComponent('bar:runtime', '1') 
        self.addCollection('group-dist-extras', '1', ['bar:runtime'])

        self.addCollection('group-dist', '1', [('foo', True),
                                               ('bam', True),
                                               ('group-dist-extras', False)])
        self.addCollection('group-os-extras', '1', [('foo', '2')])
        self.addCollection('group-os', '1', [('group-dist', True),
                                     ('group-os-extras', '1', None, False)])

        origLabel = self.cfg.buildLabel
        self.cfg.buildLabel = versions.Label('localhost@rpl:branch')
        self.build(addAllGroup, 'AddAllGroup')

        repos = self.openRepository()
        self.cfg.installLabelPath = [versions.Label('localhost@rpl:branch')]
        trv = self._get(repos, 'group-addall', None, None)

        byDefault = dict((x[0][0], (x[0][1].branch().label(), x[1], x[2]))
                                            for x in trv.iterTroveListInfo())

        # ensure there's only one foo in this trove, even though there
        # is a foo v1 and foo v2 in the original trove.

        assert(len(byDefault) == len(list(trv.iterTroveListInfo())))
        # dict = name -> (label, byDefault, strongRef)

        newLabel = self.cfg.buildLabel
        assert(byDefault == {'group-os-extras'   : (newLabel,  False, True),
          # through group-dist we should include the new foo, and it should 
          # by default true (even though it's not-bydefault through 
          # group-os-extras)
                             'group-dist'        : (newLabel,  True,  True),
                             'foo'               : (newLabel,  True,  False),
                             'foo:runtime'       : (newLabel,  True,  False),
                             # foo:data is 'removed' from group-dist
                             'foo:data'          : (newLabel,  False, False),

                             'bam'               : (newLabel,  True,  False),
                             'bam:runtime'       : (newLabel,  True,  False),


                             # included in group-dist-extras
                             'group-dist-extras' : (newLabel,  False,  False),
                             'bar:runtime'       : (origLabel, False,  False)})

        trv = self._get(repos, 'group-dist', None, None)
            
        byDefault = dict((x[0][0], (x[0][1].branch().label(), x[1], x[2]))
                                            for x in trv.iterTroveListInfo())
        assert(byDefault == {'foo'               : (newLabel,  True,  True),
                             'foo:runtime'       : (newLabel,  True,  False),
                              # foo:data is 'removed' from group-dist
                              'foo:data'          : (newLabel,  False, False),

                              'bam'               : (newLabel,  True,  True),
                              'bam:runtime'       : (newLabel,  True,  False),
                              'group-dist-extras'   : (newLabel,  False, True),
                              # included in group-dist-extras
                              'bar:runtime'       : (origLabel, False,  False)})

        self.build(addAllGroup2, 'AddAllGroup')
        trv = self._get(repos, 'group-addall', None, None)
        byDefault = dict((x[0][0], (x[0][1].branch().label(), x[1], x[2]))
                                            for x in trv.iterTroveListInfo())
        assert('group-dist' not in byDefault)

        self.build(addAllGroupNoRecurse, 'AddAllGroup')
        trv = self._get(repos, 'group-addall-no-recurse', None, None)
        byDefault = dict((x[0][0], (x[0][1].branch().label(), x[1], x[2]))
                                            for x in trv.iterTroveListInfo())
        assert(byDefault['group-dist'] == (origLabel, True, True))
        assert(byDefault['foo:data'] == (origLabel, False, False))

        self.build(addAllGroupNameGroup, 'AddAllGroupNameGroup')
        trv = self._get(repos, 'group-addall-groupname', None, None)
        byDefault = dict((x[0][0], (x[0][1].branch().label(), x[1], x[2]))
                                            for x in trv.iterTroveListInfo())
        assert(byDefault['foo'] == (newLabel,  True,  False))
        trv = self._get(repos, 'group-dist', None, None)
        byDefault = dict((x[0][0], (x[0][1].branch().label(), x[1], x[2]))
                                            for x in trv.iterTroveListInfo())
        assert(byDefault['foo'] == (newLabel,  True,  True))

    def testAddAll2(self):
        repos = self.openRepository()
        # test addAll that includes 1. a group that overrides default weak
        # ref settings for troves and 2. a parent group with different 
        # weak ref settings than the child 
        for idx, v in enumerate(('1', '2')):
            self.addComponent('foo:runtime', v, filePrimer=idx)
            self.addComponent('foo:data',  v, filePrimer=idx + 2) 
            self.addCollection('foo', v, [ 'foo:runtime', 'foo:data' ])

        self.addComponent('bar:runtime', '1') 
        self.addCollection('group-dist-extras', '1', ['bar:runtime'])

        self.addCollection('group-dist', '1', [('foo', True),
                                               ('group-dist-extras', False)],
                                   weakRefList=[('foo:runtime', True),
                                                ('foo:data',    False),
                                                ('bar:runtime', False)])
        self.addCollection('group-os', '1', [('group-dist', True)],
                                    weakRefList=[('foo:runtime', False),
                                                ('foo:data',    False),
                                                ('foo',         False),
                                                ('group-dist-extras',  False),
                                                ('bar:runtime', False)])

        origLabel = self.cfg.buildLabel
        self.cfg.buildLabel = versions.Label('localhost@rpl:branch')

        self.build(addAllGroupSimple, 'AddAllGroup')
        self.cfg.installLabelPath = [versions.Label('localhost@rpl:branch')]
        trv = self._get(repos, 'group-addall', None, None)

        byDefault = dict((x[0][0], (x[0][1].branch().label(), x[1], x[2]))
                                            for x in trv.iterTroveListInfo())

        assert(len(byDefault) == len(list(trv.iterTroveListInfo())))
        # dict = name -> (label, byDefault, strongRef)

        newLabel = self.cfg.buildLabel
        assert(byDefault == {'group-dist'        : (newLabel,  True,  True),
                             'foo'               : (origLabel,  False,  False),
                             'foo:runtime'       : (origLabel,  False,  False),
                             'foo:data'          : (origLabel,  False,  False),

                             # included in group-dist-extras
                             'group-dist-extras' : (newLabel,  False,  False),
                             'bar:runtime'       : (origLabel, False,  False)})

        trv = self._get(repos, 'group-dist', None, None)

        byDefault = dict((x[0][0], (x[0][1].branch().label(), x[1], x[2]))
                                            for x in trv.iterTroveListInfo())

        assert(len(byDefault) == len(list(trv.iterTroveListInfo())))
        # dict = name -> (label, byDefault, strongRef)
        assert(byDefault == {'foo'               : (origLabel,  True,  True),
                             'foo:runtime'       : (origLabel,  True,  False),
                             'foo:data'          : (origLabel,  False,  False),

                             # included in group-dist-extras
                             'group-dist-extras' : (newLabel,  False,  True),
                             'bar:runtime'       : (origLabel, False,  False)})

    def testAddAllErrors(self):
        for v in ('1', '2'):
            self.addComponent('foo:runtime', v)
            self.addComponent('foo:data',  v, filePrimer=2) 
            self.addCollection('foo', v, [ 'foo:runtime', 'foo:data' ])
            self.addComponent('bam:runtime', v, filePrimer=3)
            self.addComponent('bam:data',  v, filePrimer=4) 
            self.addCollection('bam', v, [ 'bam:runtime', 'bam:data' ])

            self.addCollection('group-foo', v, [ 'foo' ])

        self.addCollection('group-os', '1', [('group-foo', '1'),
                                             ('group-foo', '2')])

        try:
            self.build(addAllGroup, 'AddAllGroup')
        except errors.CookError, msg:
            assert(str(msg) == '''\
Cannot recursively addAll from group "group-os":
  Multiple groups with the same name(s) 'group-foo'
  are included.''')

    def testAddAllReporting(self):
        self.addComponent('foo:runtime=1[ssl]', filePrimer=2)
        self.addCollection('group-os=1[ssl]', ['foo:runtime'])

        self.logFilter.add()
        self.captureOutput(rephelp.RepositoryHelper.build, self, 
                    addAllGroupSimple, 'AddAllGroup',
                    logLevel=log.INFO)
        assert('+ Adding all from group-os=/localhost@rpl:linux/1-1-1[ssl]' in self.logFilter.records)

    def testDepResolveWithRemovedWeakTroves(self):
        """ If you remove foo:runtime from group-dist, its dependencies
            should not be resolved in.
        """
        self.addComponent('known:runtime', '1.0', filePrimer=1)
        self.addComponent('foo:runtime', '1.0', requires='trove:unknown')
        self.addComponent('foo:devel', '1.0', requires='trove:known:runtime',
                                              filePrimer=2)
        self.addCollection('foo', '1.0', ['foo:runtime', 'foo:devel'])


        groupAutoResolve = """
class GroupAutoResolve(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    autoResolve = True
    depCheck = True
    imageGroup = False
    clearBuildRequires()

    def setup(r):
        r.add('foo')
        r.remove('foo:runtime')
"""
        self.build(groupAutoResolve, 'GroupAutoResolve')
        repos = self.openRepository()
        trv = self._get(repos, 'group-dist')
        byDefault = dict((x[0][0], x[1]) for x in trv.iterTroveListInfo())

        assert(byDefault.pop('foo') == True)
        assert(byDefault.pop('foo:runtime') == False)
        assert(byDefault.pop('foo:devel') == True)
        assert(byDefault.pop('known:runtime') == True)
        assert(not byDefault)

    def testAddCopy(self):
        for idx, v in enumerate(('1', '2', '/localhost@rpl:branch/3')):
            self.addComponent('foo:runtime', v, filePrimer=idx * 3)
            self.addComponent('foo:data',  v, filePrimer=idx * 3 + 1) 
            self.addCollection('foo', v, [ 'foo:runtime', 'foo:data' ])

            self.addComponent('bam:runtime', v, filePrimer=idx * 3 + 2)
            self.addCollection('bam', v, [ ':runtime' ])



        self.addComponent('bar:runtime', '1') 
        self.addCollection('group-dist-extras', '1', ['bar:runtime'])

        self.addCollection('group-dist', '1', [('foo', True),
                                               ('bam', True),
                                               ('group-dist-extras', False)])
        self.addCollection('group-os-extras', '1', [('foo', '2')])
        self.addCollection('group-os', '1', [('group-dist', True),
                                     ('group-os-extras', '1', None, False)])

        self.cfg.buildLabel = versions.Label('localhost@rpl:branch')
        self.build(addCopyGroup, 'AddCopyGroup')
        repos = self.openRepository()
        trv = self._get(repos, 'group-addcopy', 'localhost@rpl:branch')
        assert([x[0] for x in trv.iterTroveList(strongRefs=True
                                                    )][0] == 'group-os')
        self.assertEqual(trv.getTroveCopiedFrom(), [])

        trv = self._get(repos, 'group-os', 'localhost@rpl:branch')
        labels = set(str(x[1].trailingLabel()) 
                     for x in trv.iterTroveList(strongRefs=True))
        assert(labels == set(['localhost@rpl:branch']))
        copiedFrom = [(n, str(v), str(f)) for (n, v, f) in trv.getTroveCopiedFrom()]
        self.assertEqual(copiedFrom,
                             [('group-os', '/localhost@rpl:linux/1-1-1', '')])
        # Check that subgroups have their copiedFrom properly set
        trv = self._get(repos, 'group-dist', 'localhost@rpl:branch')
        copiedFrom = [(n, str(v), str(f)) for (n, v, f) in trv.getTroveCopiedFrom()]
        self.assertEqual(copiedFrom,
                             [('group-dist', '/localhost@rpl:linux/1-1-1', '')])

    def testAddCopyNonDefaultGroup(self):
        self.addComponent('foo:runtime=1')
        self.addCollection('group-os=1', ['foo:runtime'])
        self.build(addCopyGroup2, 'AddCopyGroup')
        repos = self.openRepository()
        trv = self.findAndGetTrove('group-addcopy')
        assert([x[0] for x in trv.iterTroveList(strongRefs=True
                                                    )][0] == 'group-foo')
        trv = self.findAndGetTrove('group-foo')
        assert([x[0] for x in trv.iterTroveList(strongRefs=True
                                                    )][0] == 'group-os')
        self.assertEqual(trv.getTroveCopiedFrom(), [])

        trv = self.findAndGetTrove('group-os')
        copiedFrom = [(n, str(v), str(f)) for (n, v, f) in trv.getTroveCopiedFrom()]
        self.assertEqual(copiedFrom,
                             [('group-os', '/localhost@rpl:linux/1-1-1', '')])

    def testSharedFiles(self):
        self.addComponent('foo:runtime', '1.0', '', [ ('/usr/bin/foo', 'a') ])
        self.addComponent('bar:runtime', '1.0', '', ['/usr/bin/bar',
                                                      ('/usr/bin/foo', 'a') ])
        groupConflicts = """
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.add('foo:runtime') 
        r.add('bar:runtime') 
"""
        self.build(groupConflicts, 'GroupConflicts')

    def testPathHashConflicts(self):
        # foo, bar, and baz conflict
        # baz and bam conflict
        # boo doesn't conflict
        groupConflicts = """
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.add('foo:runtime') 
        r.add('bar:runtime') 
        r.add('baz:runtime')
        r.add('bam:runtime')
        r.add('boo:runtime')
"""

        bazList = [ '/usr/bin/baz%s' % i for i in range(0,20)]

        self.addComponent('foo:runtime', '1.0', '', [ ('/usr/bin/foo', 'a') ])
        self.addComponent('bar:runtime', '1.0', '', ['/usr/bin/bar',
                                                      ('/usr/bin/foo', 'b') ])
        self.addComponent('baz:runtime', '1.0', '', [ ('/usr/bin/foo', 'c'),
                                                      ('/usr/bin/baz', 'c')]
                                             + [ (x, 'a') for x in bazList ] )
        self.addComponent('bam:runtime', '1.0', '', [('/usr/bin/baz', 'd'),
                                                     '/usr/bin/bam']
                                             + [ (x, 'b') for x in bazList ] )
        self.addComponent('boo:runtime', '1.0', '', ['/usr/bin/boo'])

        try:
            self.build(groupConflicts, 'GroupConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            self.assertEquals(str(err), '''\

The following troves in the following groups have conflicts:

group-dist:
  The following 3 troves share 1 conflicting paths:

    Troves:
     foo:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)
     bar:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)
     baz:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)

    Conflicting Files:
      /usr/bin/foo

  The following 2 troves share 21 conflicting paths:

    Troves:
     bam:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)
     baz:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)

    Conflicting Files:
      /usr/bin/baz
      /usr/bin/baz0
      /usr/bin/baz1
      /usr/bin/baz10
      /usr/bin/baz11
      /usr/bin/baz12
      /usr/bin/baz13
      /usr/bin/baz14
      /usr/bin/baz15
      /usr/bin/baz16
      /usr/bin/baz17
      ... (11 more)
''')

    def testPathConflictExceptions(self):
        # foo, bar, and baz conflict
        # baz and bam conflict
        # boo doesn't conflict
        bazList = [ '/usr/bin/baz%s' % i for i in range(0,20)]

        self.addComponent('foo:runtime', '1.0', '', [ ('/usr/bin/foo', 'a') ])
        self.addCollection('foo', '1.0', ['foo:runtime'])
        self.addComponent('bar:runtime', '1.0', '', ['/usr/bin/bar',
                                                      ('/usr/bin/foo', 'b') ])
        self.addCollection('bar', '1.0', ['bar:runtime'])
        self.addComponent('baz:runtime', '1.0', '', [ ('/usr/bin/foo', 'c'),
                                                      ('/usr/bin/baz', 'c')]
                                             + [ (x, 'a') for x in bazList ] )
        self.addCollection('baz', '1.0', ['baz:runtime'])
        self.addComponent('bam:runtime', '1.0', '', [('/usr/bin/baz', 'd'),
                                                     '/usr/bin/bam']
                                             + [ (x, 'b') for x in bazList ] )
        self.addCollection('bam', '1.0', ['bam:runtime'])
        self.addComponent('boo:runtime', '1.0', '', ['/usr/bin/boo'])
        self.addCollection('boo', '1.0', ['boo:runtime'])

        groupConflicts1 = """
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.add('foo:runtime') 
        r.add('bar:runtime') 
        r.add('baz:runtime')
        r.add('bam:runtime')
        r.add('boo:runtime')
        r.PathConflicts(exceptions=['/usr/bin/foo'])
"""

        try:
            self.build(groupConflicts1, 'GroupConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            self.assertEquals(str(err), '''\

The following troves in the following groups have conflicts:

group-dist:
  The following 2 troves share 21 conflicting paths:

    Troves:
     bam:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)
     baz:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)

    Conflicting Files:
      /usr/bin/baz
      /usr/bin/baz0
      /usr/bin/baz1
      /usr/bin/baz10
      /usr/bin/baz11
      /usr/bin/baz12
      /usr/bin/baz13
      /usr/bin/baz14
      /usr/bin/baz15
      /usr/bin/baz16
      /usr/bin/baz17
      ... (11 more)
''')

        groupConflicts2 = """
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.add('foo:runtime') 
        r.add('bar:runtime') 
        r.add('baz:runtime')
        r.add('bam:runtime')
        r.add('boo:runtime')
        r.PathConflicts(exceptions='/usr/bin/baz.*')
"""
        try:
            self.build(groupConflicts2, 'GroupConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            self.assertEquals(str(err), '''\

The following troves in the following groups have conflicts:

group-dist:
  The following 3 troves share 1 conflicting paths:

    Troves:
     foo:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)
     bar:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)
     baz:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)

    Conflicting Files:
      /usr/bin/foo
''')

        groupConflicts3 = """
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.add('foo:runtime') 
        r.add('bar:runtime') 
        r.add('baz:runtime')
        r.add('bam:runtime')
        r.add('boo:runtime')
        r.PathConflicts(exceptions=r.troveFilter('baz:runtime'))
"""
        try:
            self.build(groupConflicts3, 'GroupConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            self.assertEquals(str(err), '''\

The following troves in the following groups have conflicts:

group-dist:
  The following 2 troves share 1 conflicting paths:

    Troves:
     foo:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)
     bar:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)

    Conflicting Files:
      /usr/bin/foo
''')

        groupConflicts4 = """
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.add('foo:runtime') 
        r.add('bar:runtime') 
        r.add('baz:runtime')
        r.add('bam:runtime')
        r.add('boo:runtime')
        r.PathConflicts(exceptions=['/usr/bin/baz..*', '/usr/bin/foo'])
"""

        try:
            self.build(groupConflicts4, 'GroupConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            self.assertEquals(str(err), '''\

The following troves in the following groups have conflicts:

group-dist:
  The following 2 troves share 1 conflicting paths:

    Troves:
     bam:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)
     baz:runtime=/localhost@rpl:linux/1.0-1-1[]
       (Added directly)

    Conflicting Files:
      /usr/bin/baz
''')

        groupConflicts5 = """
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.startGroup('group-inner')
        r.add('foo') 
        r.add('bar') 
        r.add('baz')
        r.add('bam')
        r.add('boo')
        r.PathConflicts(exceptions='.*')
"""

        grp = self.build(groupConflicts5, 'GroupConflicts')
        assert [ x for x in grp.iterTroveList(weakRefs = True) ], 'there must be weakrefs'
        assert sorted([x for x in grp.troveInfo.pathConflicts]) == [
            '/usr/bin/baz', '/usr/bin/baz0', '/usr/bin/baz1',
            '/usr/bin/baz10', '/usr/bin/baz11', '/usr/bin/baz12',
            '/usr/bin/baz13', '/usr/bin/baz14', '/usr/bin/baz15',
            '/usr/bin/baz16', '/usr/bin/baz17', '/usr/bin/baz18',
            '/usr/bin/baz19', '/usr/bin/baz2', '/usr/bin/baz3',
            '/usr/bin/baz4', '/usr/bin/baz5', '/usr/bin/baz6',
            '/usr/bin/baz7', '/usr/bin/baz8', '/usr/bin/baz9',
            '/usr/bin/foo'], 'path conflicts must be reported in troveInfo'


    def testDepResolveAddsBackRemovedWeakTroves(self):
        """ You remove :devel components, but foo:runtime requires
            foo:devel.  Make sure it just gets turned back to 
            byDefault true instead of added at the top level.
        """
        self.addComponent('foo:runtime', '1.0', filePrimer=1,
                          requires='trove:foo:devel')
        self.addComponent('foo:devel', '1.0', filePrimer=2)
        self.addCollection('foo', '1.0', ['foo:runtime', 'foo:devel'])

        groupAutoResolve = """
class GroupAutoResolve(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    autoResolve = True
    depCheck = True
    clearBuildRequires()

    def setup(r):
        r.add('foo')
        r.removeComponents(['devel'])
"""
        self.build(groupAutoResolve, 'GroupAutoResolve')
        repos = self.openRepository()
        trv = self._get(repos, 'group-dist')
        troveNames = [ x[0] for x in trv.iterTroveList(strongRefs=True) ]
        assert('foo:devel' not in troveNames)
        byDefault = dict((x[0][0], (x[0][1].branch().label(), x[1], x[2]))
                                            for x in trv.iterTroveListInfo())
        assert(byDefault['foo:devel'])


    def testAddGroupCreatesConflict(self):
        self.addComponent('foo:runtime', '1', [ ('/p', 'this') ] )
        self.addCollection('foo', '1', [':runtime'])
        self.addCollection('group-foo', '1', ['foo'])

        self.addComponent('foo:runtime', '2', [ ('/p', 'that') ] )
        self.addCollection('foo', '2', [':runtime'])

        groupAddGroupConflicts = """
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    autoResolve = True
    depCheck = True
    clearBuildRequires()

    def setup(r):
        r.add('foo')
        r.add('group-foo')
"""
        try:
            self.build(groupAddGroupConflicts, 'GroupConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            self.assertEqual(str(err), '''\

The following troves in the following groups have conflicts:

group-dist:
  The following 2 troves share 1 conflicting paths:

    Troves:
     foo:runtime=/localhost@rpl:linux/1-1-1[]
       (Included by adding group-foo=/localhost@rpl:linux/1-1-1[])
     foo:runtime=/localhost@rpl:linux/2-1-1[]
       (Included by adding foo=/localhost@rpl:linux/2-1-1[])

    Conflicting Files:
      /p
''')
            # CNY-3079
            self.assertTrue(isinstance(err.args, tuple), type(err.args))


    def testDepResolveCreatesConflict(self):
        self.addComponent('foo:runtime', '1', [ ('/foo', 'this') ],
                          provides='trove:foo:runtime(1)')
        self.addCollection('foo', '1', [':runtime'], )

        self.addComponent('foo:runtime', '2', [ ('/foo', 'that') ])
        self.addCollection('foo', '2', [':runtime'])
        self.addComponent('bar:runtime', '1', requires='trove:foo:runtime(1)',
                          filePrimer=1)
        self.addCollection('bar', '1', [':runtime'])
        groupAddResolveConflicts = """
class GroupResolveConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    autoResolve = True
    clearBuildRequires()

    def setup(r):
        r.add('foo')
        r.add('bar')
"""
        try:
            self.build(groupAddResolveConflicts, 'GroupResolveConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            assert(str(err) == '''
The following troves in the following groups have conflicts:

group-dist:
  The following 2 troves share 1 conflicting paths:

    Troves:
     foo:runtime=/localhost@rpl:linux/1-1-1[]
       (Added to satisfy dep(s): ('trove: foo:runtime(1)') required by bar:runtime=/localhost@rpl:linux/1-1-1[])
     foo:runtime=/localhost@rpl:linux/2-1-1[]
       (Included by adding foo=/localhost@rpl:linux/2-1-1[])

    Conflicting Files:
      /foo
''')

    def testDepResolveCreatesConflictMultiDeps(self):
        myDep = deps.parseDep( \
                ' '.join(['trove:foo%d:runtime' % x for x in range(7)]))
        self.addComponent('foo:runtime', '1', [ ('/foo', 'a') ],
                          provides = myDep)
        self.addCollection('foo', '1', [':runtime'], )

        self.addComponent('foo:runtime', '2', [ ('/foo', 'b') ])
        self.addCollection('foo', '2', [':runtime'])
        self.addComponent('bar:runtime', '1', requires = myDep, filePrimer=1)
        self.addCollection('bar', '1', [':runtime'])
        groupAddResolveConflicts = """
class GroupResolveConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    autoResolve = True
    clearBuildRequires()

    def setup(r):
        r.add('foo')
        r.add('bar')
"""
        try:
            self.build(groupAddResolveConflicts, 'GroupResolveConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            assert(str(err) == '''
The following troves in the following groups have conflicts:

group-dist:
  The following 2 troves share 1 conflicting paths:

    Troves:
     foo:runtime=/localhost@rpl:linux/1-1-1[]
       (Added to satisfy dep(s): ('trove: foo0:runtime', 'trove: foo1:runtime', 'trove: foo2:runtime', 'trove: foo3:runtime', 'trove: foo4:runtime', ... 2 more) required by bar:runtime=/localhost@rpl:linux/1-1-1[])
     foo:runtime=/localhost@rpl:linux/2-1-1[]
       (Included by adding foo=/localhost@rpl:linux/2-1-1[])

    Conflicting Files:
      /foo
''')

    def testDepResolveCreatesConflictMultiDepsDebug(self):
        myDep = deps.parseDep( \
                ' '.join(['trove:foo%d:runtime' % x for x in range(7)]))
        self.addComponent('foo:runtime', '1', [ ('/foo', 'this') ],
                          provides = myDep)
        self.addCollection('foo', '1', [':runtime'], )

        self.addComponent('foo:runtime', '2', [ ('/foo', 'that') ])
        self.addCollection('foo', '2', [':runtime'])
        self.addComponent('bar:runtime', '1', requires = myDep, filePrimer=1)
        self.addCollection('bar', '1', [':runtime'])
        groupAddResolveConflicts = """
class GroupResolveConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    autoResolve = True
    clearBuildRequires()

    def setup(r):
        r.add('foo')
        r.add('bar')
"""
        try:
            self.build(groupAddResolveConflicts, 'GroupResolveConflicts',
                    logLevel = log.DEBUG)
            assert(0)
        except errors.GroupPathConflicts, err:
            assert(str(err) == '''
The following troves in the following groups have conflicts:

group-dist:
  The following 2 troves share 1 conflicting paths:

    Troves:
     foo:runtime=/localhost@rpl:linux/1-1-1[]
       (Added to satisfy dep(s): ('trove: foo0:runtime', 'trove: foo1:runtime', 'trove: foo2:runtime', 'trove: foo3:runtime', 'trove: foo4:runtime', 'trove: foo5:runtime', 'trove: foo6:runtime') required by bar:runtime=/localhost@rpl:linux/1-1-1[])
     foo:runtime=/localhost@rpl:linux/2-1-1[]
       (Included by adding foo=/localhost@rpl:linux/2-1-1[])

    Conflicting Files:
      /foo
''')

    def testDegenerateGetReasonString(self):
        class DummyGroup(grouprecipe.SingleGroup):
            #__getattr__ = object.__getattribute__
            def __init__(obj):
                obj.cache = None
                obj.reasons = {}
                for i in range(1, 8):
                    obj.reasons[('name%d' % i, 'version', 'flavor')] = \
                            (grouprecipe.ADD_REASON_DEP,
                            (('n', 'v', 'f'), ('pn', 'pv', 'pf')))

        grp = DummyGroup()
        reason = grp.getReasonString('name1', 'version', 'flavor')
        ref = "Added to satisfy dep of n=v[f]"
        self.assertEquals(reason, ref)
        reason = grp.getReasonString('name7', 'version', 'flavor')
        self.assertEquals(reason, ref)

    def testAddAllCreatesConflict(self):
        self.addComponent('foo:runtime', '1', [ ('/foo', 'this') ],
                          provides='trove:foo:runtime(1)')
        self.addCollection('foo', '1', [':runtime'], )
        self.addCollection('group-foo', '1', ['foo'])

        self.addComponent('foo:runtime', '2', [ ('/foo', 'that') ])
        self.addCollection('foo', '2', [':runtime'])
        groupAddAllConflicts = """
class GroupResolveConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    autoResolve = True
    clearBuildRequires()

    def setup(r):
        r.add('foo')
        r.addAll('group-foo')
"""
        try:
            self.build(groupAddAllConflicts, 'GroupResolveConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            assert(str(err) == '''\

The following troves in the following groups have conflicts:

group-dist:
  The following 2 troves share 1 conflicting paths:

    Troves:
     foo:runtime=/localhost@rpl:linux/1-1-1[]
       (Included by adding all from group-foo=/localhost@rpl:linux/1-1-1[])
     foo:runtime=/localhost@rpl:linux/2-1-1[]
       (Included by adding foo=/localhost@rpl:linux/2-1-1[])

    Conflicting Files:
      /foo
''')

    def testReplaceCreatesConflict(self):
        self.addComponent('foo:runtime', '1', [ ('/foo', 'a') ])
        self.addCollection('foo', '1', [':runtime'], )
        self.addComponent('bar:runtime', '1', ['/bar'])
        self.addCollection('bar', '1', [':runtime'], )
        self.addCollection('group-foo', '1', ['foo', 'bar'])

        self.addComponent('bar:runtime', '2', [ ('/foo', 'b') ])
        self.addCollection('bar', '2', [':runtime'], )

        groupReplaceConflicts = """
class GroupReplaceConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    autoResolve = True
    clearBuildRequires()

    def setup(r):
        r.addAll('group-foo')
        r.replace('bar')
"""
        try:
            self.build(groupReplaceConflicts, 'GroupReplaceConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            assert(str(err) == '''\

The following troves in the following groups have conflicts:

group-dist:
  The following 2 troves share 1 conflicting paths:

    Troves:
     bar:runtime=/localhost@rpl:linux/2-1-1[]
       (Included by replace of bar=/localhost@rpl:linux/2-1-1[])
     foo:runtime=/localhost@rpl:linux/1-1-1[]
       (Included by adding all from group-foo=/localhost@rpl:linux/1-1-1[])

    Conflicting Files:
      /foo
''')

    def testAddGroupConflicts(self):
        self.addComponent('foo:runtime', '1', [('/foo', 'here')])
        self.addCollection('foo', '1', [':runtime'], )
        self.addComponent('foo:runtime', '2', [('/foo', 'there')])
        self.addCollection('foo', '2', [':runtime'], )

        groupAddGroupConflicts = """
class GroupAddGroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    autoResolve = True
    clearBuildRequires()

    def setup(r):
        r.createGroup('group-foo')
        r.addNewGroup('group-foo')
        r.add('foo', '1', groupName='group-foo')
        r.add('foo', '2', groupName='group-dist')
"""
        try:
            self.build(groupAddGroupConflicts, 'GroupAddGroupConflicts')
            assert(0)
        except errors.GroupPathConflicts, err:
            assert(str(err) == '''\

The following troves in the following groups have conflicts:

group-dist:
  The following 2 troves share 1 conflicting paths:

    Troves:
     foo:runtime=/localhost@rpl:linux/1-1-1[]
       (Included by adding new group group-foo)
     foo:runtime=/localhost@rpl:linux/2-1-1[]
       (Included by adding foo=/localhost@rpl:linux/2-1-1[])

    Conflicting Files:
      /foo
''')

    def testRemoveLastComponentFromPackage(self):
        # CNY-774 - When building groups, removing the last byDefault component
        # will make the package byDefault False.
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [':run'])
        groupRemoveAllComponents = """\
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'

    imageGroup = False
    clearBuildRequires()

    def setup(self):
        self.add("foo", "@rpl:linux")
        self.remove("foo:run")
"""
        grp = self.build(groupRemoveAllComponents, 'basicGroup')
        # byDefault for all the troves in the group is False.
        assert(set(x[1] for x in grp.iterTroveListInfo()) == set([False]))

    def testSetByDefault(self):

        groupSetDefaultFalse = """\
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'

    imageGroup = False
    clearBuildRequires()

    def setup(self):
        self.setByDefault(False)
        self.add("foo:run")
"""
        self.addComponent('foo:run')
        grp = self.build(groupSetDefaultFalse, 'basicGroup')
        # byDefault for all the troves in the group is False.
        assert(set(x[1] for x in grp.iterTroveListInfo()) == set([False]))

    def testParallelGroupCook(self):
        groupParallel = """\
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'

    imageGroup = False
    clearBuildRequires()

    def setup(self):
        if Use.ssl:
            self.add("foo:run")
        else:
            self.add("bar:run")
"""
        self.addComponent('foo:run', '1', ['/foo'])
        self.addComponent('bar:run', '1', ['/bar'])
        repos = self.openRepository()
        os.chdir(self.workDir)
        self.writeFile('group-test.recipe', groupParallel)
        built, csPath = self.cookItem(repos, self.cfg, 
                       ('group-test.recipe', None, 
                        [deps.parseFlavor('ssl'), deps.parseFlavor('!ssl')]))
        assert(len(built) == 2)
        self.updatePkg([csPath])
        db = self.openDatabase()
        assert(len(db.trovesByName('foo:run')) == 1)
        assert(len(db.trovesByName('bar:run')) == 1)
        assert(len(db.trovesByName('group-test')) == 2)

    def testAddAllYourself(self):
        self.addComponent('foo:run')
        groupOne = """\
class basicGroup(GroupRecipe):
    name = 'group-one'
    version = '1.0'

    imageGroup = False
    clearBuildRequires()

    def setup(self):
        self.createGroup('group-two')
        self.addNewGroup('group-two')
        self.add('foo:run', groupName='group-two')
"""
        groupTwo = """\
class basicGroup(GroupRecipe):
    name = 'group-two'
    version = '1.0'

    imageGroup = False
    clearBuildRequires()

    def setup(self):
        self.addAll('group-one')
"""
        self.build(groupOne, 'basicGroup')
        self.checkFailure(groupTwo, 'basicGroup', """Tried to addAll "group-one=/localhost@rpl:linux/1.0-1-1" into group-two - which resulted in trying to add group-two to itself.  This is not allowed.  You may wish to pass recurse=False to addAll.""")

    def testDepResolveWeakComponent(self):
        # You've already got openssl included as a weak
        # reference.  Now you need to switch it to be a strong reference
        # because it's needed for dep resolution.  Make sure that 
        # openssl is switched to a strong reference and that openssl:runtime
        # and openssl are both byDefault True.
        self.addComponent('openssl:runtime', 1, filePrimer=1)
        self.addComponent('openssl:lib', 1, filePrimer=2)
        self.addCollection('openssl', '1', [(':lib', True), 
                                            (':runtime', False)])
        self.addComponent('foo:runtime', '1', requires='trove:openssl:runtime')
        self.addCollection('foo', '1', [':runtime'])
        self.addCollection('group-ssl', '1', ['openssl'])
        basicGroup = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    
    autoResolve = True
    clearBuildRequires()

    def setup(self):
        self.addCopy('group-ssl')
        self.add('foo')
"""
        group = self.build(basicGroup, 'basicGroup')
        byDefault = dict((x[0], group.includeTroveByDefault(*x)) \
                    for x in group.iterTroveList(weakRefs=False,
                                                 strongRefs=True))
        assert(byDefault.pop('openssl'))
        assert(byDefault.pop('foo'))
        assert(byDefault.pop('group-ssl'))
        assert(not byDefault)

        byDefault = dict((x[0], group.includeTroveByDefault(*x)) \
                    for x in group.iterTroveList(weakRefs=True, 
                                                 strongRefs=False))
        assert(byDefault.pop('openssl:runtime'))
        assert(byDefault.pop('openssl:lib'))
        assert(byDefault.pop('foo:runtime'))
        assert(not byDefault)

    def testAddWithLabelPath(self):
        # CNY-1227 - add a labelPath argument for adding troves
        t1 = self.addComponent('foo:runtime', ':branch/1')
        t2 = self.addComponent('bar:runtime', filePrimer=1)
        basicGroup = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'

    imageGroup = False
    clearBuildRequires()

    def setup(self):
        self.add('foo:runtime', labelPath=['localhost@rpl:foo', 
                                           'localhost@rpl:branch'])
        self.add('bar:runtime')
"""
        group = self.build(basicGroup, 'basicGroup')
        troveList = set(group.iterTroveList(strongRefs=True))
        assert(troveList == 
               set([t1.getNameVersionFlavor(), t2.getNameVersionFlavor()]))

    def testAddWithLabelPathErrors(self):
        group1 = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.add('foo:runtime', labelPath=['localhost@rpl']) 
"""
        group2 = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        ref = self.addReference('group-dist')
        self.add('foo:runtime', labelPath=['localhost@rpl:devel'], ref=ref)
"""
        try:
            self.build(group1, 'basicGroup')
            assert(0)
        except Exception, err:
            assert(str(err).endswith('ParseError: Error parsing label "localhost@rpl": @ sign can only be used with a colon'))

    def testSearchPath(self):
        group1 = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'

    autoResolve = True
    clearBuildRequires()

    def setup(self):
        self.setSearchPath('localhost@rpl:branch', 'group-foo')
        self.add('foo:runtime') # from group-foo
        self.add('bar:runtime', searchPath=['foo:runtime=:branch3',
                                            'localhost@rpl:branch2'])
        self.add('bam:runtime') # from :branch
"""
        self.addComponent('foo:runtime', ':branch3/1',
                           requires='trove:foo2:runtime trove:foo3:runtime')
        self.addComponent('bar:runtime', '/localhost@rpl:branch2/2',
                          filePrimer=1)
        self.addComponent('bam:runtime', '/localhost@rpl:branch/1',
                          filePrimer=2)
        self.addComponent('foo2:runtime', '1')
        self.addComponent('foo2:runtime', ':branch/1', filePrimer=3)
        self.addComponent('foo3:runtime', ':branch4/1', filePrimer=4)
        self.addCollection('group-foo', '1', [('foo:runtime', ':branch3/1'),
                                              'foo2:runtime',
                                              ('foo3:runtime', ':branch4/1')])
        grp = self.build(group1, 'basicGroup')
        lst = grp.iterTroveList(strongRefs=True, weakRefs=True)
        lst = set('%s=:%s' % (x[0], x[1].trailingLabel().branch) for x in lst)
        assert(lst == set(['foo:runtime=:branch3', 
                           'bar:runtime=:branch2',
                           'bam:runtime=:branch',
                           'foo2:runtime=:branch',
                           'foo3:runtime=:branch4']))

    def testFlatten(self):
        fooRun = self.addComponent('foo:runtime', '1')
        fooRun = self.addComponent('foo:data', '1', filePrimer=1)
        fooBar = self.addCollection('foo', '1', [':runtime', (':data', False)])
        self.addCollection('group-foo', '1', [('foo', False)])
        grp = self.addCollection('group-bar', '1', ['group-foo'],
                           weakRefList=[('foo', True), ('foo:runtime', False),
                                        ('foo:data', True)])

        d = {}
        for troveTup, byDefault, isStrong in grp.iterTroveListInfo():
            d[troveTup[0]] = byDefault, isStrong
        assert(d.pop('foo') == (True, False))
        assert(d.pop('foo:runtime') == (False, False))
        assert(d.pop('foo:data') == (True, False))
        assert(len(d) == 1)

        group1 = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.addAll('group-bar', flatten=True)
"""
        grp = self.build(group1, 'basicGroup')
        d = {}
        for troveTup, byDefault, isStrong in grp.iterTroveListInfo():
            d[troveTup[0]] = byDefault, isStrong
        assert(d.pop('foo') == (True, True))
        assert(d.pop('foo:runtime') == (False, False))
        assert(d.pop('foo:data') == (True, False))
        assert(not d)

        copiedFrom = [(n, str(v), str(f)) for (n, v, f) in grp.getTroveCopiedFrom()]
        self.assertEqual(copiedFrom,
                             [('group-bar', '/localhost@rpl:linux/1-1-1', '')])


    def testRemoveTrovesIncludedInNewGroup(self):
        # CNY-1380
        group1 = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.setSearchPath('localhost@rpl:branch', 'localhost@rpl:linux')
        self.createGroup('group-my-core')
        self.addNewGroup('group-my-core')
        self.setDefaultGroup('group-my-core')
        self.addAll('group-core')
        self.replace('bash', searchPath=['localhost@rpl:branch'])
        self.add('python')
        self.add('foo', 'localhost@rpl:branch2')

        self.createGroup('group-extras')
        self.addNewGroup('group-extras', groupName='group-basic')
        self.setDefaultGroup('group-extras')
        self.addAll('group-os', flatten=True)
        self.removeItemsAlsoInNewGroup('group-my-core')
        self.removeItemsAlsoInGroup('group-core')
        # we have to explicitly remove this because we added a different
        # version to group-my-core.
        self.remove('python')
"""
        for (idx, name) in enumerate(['bash', 'setup', 'python', 'extra']):
            self.addComponent('%s:run' % name, '1', filePrimer=idx)
            self.addCollection('%s' % name, '1', [':run'])
        self.addComponent('bash:run', ':branch/1', filePrimer=0)
        self.addCollection('bash', ':branch/1', [':run'])
        self.addComponent('python:run', ':branch/1', filePrimer=2)
        self.addCollection('python', ':branch/1', [':run'])
        self.addComponent('foo:run', ':branch2/1', filePrimer=10)
        self.addCollection('foo', ':branch2/1', [':run'])

        self.addCollection('group-core', '1',  [ 'bash', 'setup'])
        self.addCollection('group-devel', '1', ['python', 'extra'])
        self.addCollection('group-dist', '1', ['group-core', 'group-devel'])
        self.addCollection('group-os', '1', ['group-dist'])

        grp = self.build(group1, 'basicGroup')
        assert(set(x[0] for x in grp.iterTroveList(strongRefs=True)) 
                == set(['group-my-core', 'group-extras']))
        ss = self.getSearchSource()
        groupCore = ss.getTrove(*ss.findTrove(('group-my-core', None, None))[0])
        groupExtras = ss.getTrove(*ss.findTrove(('group-extras', None, None))[0])
        assert(set(x[0] for x in groupExtras.iterTroveList(strongRefs=True,
                                                           weakRefs=True))
                == set(['extra', 'extra:run']))
        assert(set('%s=:%s' % (x[0], x[1].trailingLabel().branch)
                    for x in groupCore.iterTroveList(strongRefs=True,
                                                     weakRefs=True))
                == set(['bash=:branch', 'bash:run=:branch', 
                        'setup=:linux', 'setup:run=:linux',
                        'python=:branch', 'python:run=:branch',
                        'foo=:branch2', 'foo:run=:branch2']))

        copiedFrom = [(n, str(v), str(f))
                        for (n, v, f) in groupCore.getTroveCopiedFrom()]
        self.assertEqual(copiedFrom,
                             [('group-core', '/localhost@rpl:linux/1-1-1', '')])

    def testSearchTipFirst(self):
        self.addComponent('foo:run', '1', filePrimer=0,
                            requires='trove:bar:run(1.0)')
        self.addComponent('bar:run', '1-1-1', provides='trove:bar:run(1.0)', 
                          filePrimer=1)
        self.addComponent('bar:run', '1-1-2', provides='trove:bar:run', 
                          filePrimer=1)
        branchBar = self.addComponent('bar:run', ':branch/1',
                                      provides='trove:bar:run(1.0)', 
                                      filePrimer=1)
        grp = self.build(searchTipFirst, 'basicGroup')
        barTup = [ x for x in grp.iterTroveList(strongRefs=True) if x[0] == 'bar:run'][0]
        assert(barTup == branchBar.getNameVersionFlavor())

    def testMoveComponents(self):
        group1 = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        for group in 'group-first', 'group-second', 'group-third':
            self.createGroup(group)
            self.addNewGroup(group)
        self.add('foo', groupName='group-first')
        self.add('bar', groupName='group-first')
        self.moveComponents(':devel', 'group-first', 'group-second')
        self.copyComponents(':devel', 'group-first', 'group-third',
                            byDefault=False)
        self.copyComponents(':debuginfo', 'group-first', 'group-third',
                            byDefault=True)
"""

        repos = self.openRepository()
        self.addComponent('foo:run', '1')
        self.addComponent('foo:debuginfo', '1', filePrimer=1)
        self.addComponent('foo:devel', '1', filePrimer=2)
        self.addCollection('foo', '1', [':run', ':devel', 
                                        (':debuginfo', False)])
        self.addComponent('bar:devel', '1', filePrimer=3)
        self.addCollection('bar', '1', [':devel'])

        grp = self.build(group1, 'basicGroup')
        # check first only has :run True
        first = repos.getTrove('group-first', grp.getVersion(), grp.getFlavor())
        byDefaultDict = dict((x[0][0], x[1]) for x in first.iterTroveListInfo())
        assert(byDefaultDict == {'foo:run': True, 'foo': True,
                                 'foo:devel': False,
                                 'foo:debuginfo': False, 
                                 'bar' : False, 'bar:devel' : False})
        # check second only has :devel True
        second = repos.getTrove('group-second', grp.getVersion(),
                                grp.getFlavor())
        byDefaultDict = dict((x[0][0], x[1]) 
                             for x in second.iterTroveListInfo())
        assert(byDefaultDict == {'foo:run': False, 'foo': True,
                                 'foo:devel': True,
                                 'foo:debuginfo': False,
                                 'bar:devel' : True,
                                 'bar' : True})
        # third should have debuginfo True and devel False
        third = repos.getTrove('group-third', grp.getVersion(), grp.getFlavor())
        byDefaultDict = dict((x[0][0], x[1]) 
                             for x in third.iterTroveListInfo())
        assert(byDefaultDict == {'foo:run': False, 'foo': True,
                                 'foo:devel': False,
                                 'foo:debuginfo': True,
                                 'bar:devel' : False,
                                 'bar' : False})

    @testhelp.context('trovescripts')
    def testGroupScripts(self):
        def _checkGroupBasic(trv, compatClass = 1):
            assert(trv.troveInfo.scripts.preUpdate.script() == 'prescript')
            assert(trv.troveInfo.scripts.postInstall.script() == 'postscript')
            assert(trv.troveInfo.scripts.postUpdate.script() == 'postupscript')
            assert(trv.troveInfo.scripts.postRollback.script() ==
                                                            'postrollback')
            assert(trv.getCompatibilityClass() == compatClass)

        def _checkGroupFoo(trv):
            assert(trv.troveInfo.scripts.preUpdate.script() ==
                            'prescript-other')
            assert(trv.troveInfo.scripts.postInstall.script() ==
                            'postscript-other')
            assert(not trv.troveInfo.scripts.postRollback.script())
            assert(trv.getCompatibilityClass() == 0)

        self.addComponent('foo:run', '1')
        group1 = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.createGroup('group-other')
        self.addNewGroup('group-other')
        self.add('foo:run', groupName = 'group-other')
        self.setCompatibilityClass(1)

        self.addPreUpdateScript('prescript-file')
        self.addPostInstallScript(contents = 'postscript')
        self.addPreRollbackScript(contents = 'prerollback')
        self.addPostRollbackScript(contents = 'postrollback')
        self.addPostUpdateScript(contents = 'postupscript')

        self.addPreUpdateScript(contents = 'prescript-other',
                                 groupName = 'group-other')
        self.addPostInstallScript(contents = 'postscript-other',
                                  groupName = 'group-other')
        self.addPostUpdateScript(contents = 'postupscript-other',
                                 groupName = 'group-other')
"""

        self.addComponent('group-basic:source', '1.0',
                          fileContents =
                [ ('group-basic.recipe', group1),
                  ('prescript-file', 'prescript' ) ] )
        repos = self.openRepository()
        groupsBuilt = self.cookFromRepository('group-basic')
        # put group-basic first in the list
        groupsBuilt = [ (x[0], VFS(x[1]), x[2]) for x in sorted(groupsBuilt) ]
        grp, grpFoo = repos.getTroves(groupsBuilt)
        _checkGroupBasic(grp)
        assert(grp.troveInfo.scripts.preUpdate.script() == 'prescript')
        assert(grp.troveInfo.scripts.postInstall.script() == 'postscript')
        assert(grp.troveInfo.scripts.postUpdate.script() == 'postupscript')
        assert(grp.troveInfo.scripts.preRollback.script() == 'prerollback')
        assert(grp.troveInfo.scripts.postRollback.script() == 'postrollback')
        assert(grp.getCompatibilityClass() == 1)

        grpFoo = repos.getTrove('group-other', grp.getVersion(),
                                grp.getFlavor() )
        _checkGroupFoo(grpFoo)

        # test script copying
        groupBeta = """\
class betaGroup(GroupRecipe):
    name = 'group-beta'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.addCopy('group-basic', copyScripts = True,
                     copyCompatibilityClass = False)
"""
        grp = self.build(groupBeta, 'betaGroup')
        grpBasic = repos.getTrove('group-basic', grp.getVersion(),
                                  grp.getFlavor())
        _checkGroupBasic(grpBasic, compatClass = 0)
        grpFoo = repos.getTrove('group-other', grp.getVersion(),
                                grp.getFlavor() )
        _checkGroupFoo(grpFoo)

        unknownGroup = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.add('foo:run')
        self.addPreUpdateScript(contents = 'prescript', groupName = 'baz')
"""

        try:
            self.build(unknownGroup, 'basicGroup')
        except errors.RecipeFileError, e:
            assert(str(e) == "No such group 'baz'")
        else:
            assert(0)

        noContents = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.add('foo:run')
        self.addPreUpdateScript()
"""
        try:
            self.build(noContents, 'basicGroup')
        except errors.CookError, e:
            assert(str(e).endswith('RecipeFileError: no contents given for '
                                   'group script'))
        else:
            assert(0)

        doubleContents = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.add('foo:run')
        self.addPreUpdateScript(contents = 'a')
        self.addPreUpdateScript(contents = 'a')
"""

        try:
            self.build(doubleContents, 'basicGroup')
        except errors.RecipeFileError, e:
            assert(str(e).endswith('script already set for group group-basic'))
        else:
            assert(0)

        multipleContentSpec = """
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.add('foo:run')
        self.addPreUpdateScript('path', contents = 'a')
"""
        try:
            self.build(multipleContentSpec, 'basicGroup')
        except errors.CookError, e:
            assert(str(e).endswith('both contents and filename given for '
                                   'group script'))
        else:
            assert(0)

        missingFile = """
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.add('foo:run')
        self.addPreUpdateScript('path')
"""
        try:
            self.build(missingFile, 'basicGroup')
        except errors.RecipeFileError, e:
            assert(str(e).endswith('file "path" not found for group script'))
        else:
            assert(0)

    def testGroupCookFlavorCheck(self):
        repos = self.openRepository()
        myPkgGroup = pkgGroup + '\n\tUse.readline.setPlatform();Use.ssl.setPlatform()\n'
        self.addComponent('group-pkg:source', '1.0',
                          [('group-pkg.recipe', myPkgGroup)])
        self.addComponent('test:run', '1.0', 'readline,ssl')
        self.addCollection('test', '1.0', ['test:run'], 
                            defaultFlavor='readline,ssl')
        self.addComponent('test:run', '1.0', 'readline,!ssl')
        self.addCollection('test', '1.0', ['test:run'], 
                           defaultFlavor='readline,!ssl')
        self.addCollection('group-pkg', '1.0', ['test:run'], 
                           defaultFlavor='readline,ssl')
        groupOptions = cook.GroupCookOptions(alwaysBumpCount=True,
                                             errorOnFlavorChange=True,
                                             shortenFlavors=True)
        try:
            self.captureOutput(self.cookItem, repos, self.cfg, 'group-pkg[!ssl]', groupOptions=groupOptions)
            assert(0)
        except Exception, err:
            self.assertEquals(str(err), '''The group flavors that were cooked changed from the previous cook.
The following flavors were newly cooked:
    
     readline,!ssl

The following flavors were not cooked this time:
    
     readline,ssl

With the latest conary, you must now cook all versions of a group at the same time.  This prevents potential race conditions when clients are selecting the version of a group to install.''')
        groupOptions2 = cook.GroupCookOptions(alwaysBumpCount=True,
                                             errorOnFlavorChange=False,
                                             shortenFlavors=True)
        self.captureOutput(self.cookItem, repos, self.cfg, 
                          'group-pkg[!ssl]', groupOptions=groupOptions2)
        trv = self.findAndGetTrove('group-pkg[!ssl]')
        assert(str(trv.getVersion().trailingRevision()) == '1.0-1-2')

        self.addCollection('group-pkg', '1.0-1-2', ['test:run'],
                           defaultFlavor='readline,ssl')
        self.captureOutput(self.cookItem, repos, self.cfg,
                           ('group-pkg', None, [deps.parseFlavor('!ssl'),
                                                deps.parseFlavor('ssl')]),
                           groupOptions=groupOptions)

    def testPlatformFlavors(self):
        groupOptions = cook.GroupCookOptions(alwaysBumpCount=True,
                                             errorOnFlavorChange=False,
                                             shortenFlavors=True)
        group1 = """
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.add('kernel:runtime')
"""
        self.addComponent('kernel:runtime', '1', '~kernel.bar')
        grp = self.build(group1, 'basicGroup', returnName = 'group-basic',
                         groupOptions=groupOptions)
        assert(grp.getFlavor().isEmpty())
        # kernel.smp is a platform flag - it never gets dropped
        self.addComponent('kernel:runtime', '2', '~kernel.smp')
        grp = self.build(group1, 'basicGroup', returnName = 'group-basic',
                         groupOptions=groupOptions)
        assert(str(grp.getFlavor()) == '~kernel.smp')

    def testMatchingFlavors(self):
        # foo should not show up in this flavor because it is not
        # needed to distinguish the two x86 troves, it only distinguishes
        # x86 from x86_64, which is already done by the architecture
        repos = self.openRepository()
        groupOptions = cook.GroupCookOptions(alwaysBumpCount=True,
                                             errorOnFlavorChange=False,
                                             shortenFlavors=True)
        self.addComponent('kernel:runtime', '1', '~ssl,~readline is: x86')
        self.addComponent('kernel:runtime', '1', '~ssl,~!readline is: x86')
        self.addComponent('kernel:runtime', '1', '~!ssl,~readline is: x86_64')
        group1 = """
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.add('kernel:runtime')
"""
        self.addComponent('group-basic:source', '1.0',
                          [('group-basic.recipe', group1)])
        self.captureOutput(self.cookItem, repos, self.cfg,
                           ('group-basic', None, 
                            [deps.parseFlavor('ssl,readline is:x86'),
                             deps.parseFlavor('ssl,!readline is:x86'),
                             deps.parseFlavor('!ssl,readline is:x86_64')]),
                           groupOptions=groupOptions)
        self.assertEquals(
            str(self.findAndGetTrove('group-basic[ssl,readline is:x86]').getFlavor()),
             '~readline is: x86')
        self.assertEquals(
            str(self.findAndGetTrove('group-basic[ssl,!readline is:x86]').getFlavor()),
             '~!readline is: x86')
        self.assertEquals(
            str(self.findAndGetTrove('group-basic[!ssl,readline is:x86_64]').getFlavor()),
             '~readline is: x86_64')

    def testCompatibilityClasses(self):
        def _conversions(item):
            return list(sorted(str(x) for x in item.conversions.iter()))

        self.addComponent('foo:run', '1')
        group1 = """\
class basicGroup(GroupRecipe):
    name = 'group-basic'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.createGroup('group-other')
        self.setCompatibilityClass(3)
        self.setCompatibilityClass(7, groupName = 'group-other')
        self.add('foo:run')
        self.add('foo:run', groupName = 'group-other')
        self.addNewGroup('group-other')

        self.addPostRollbackScript(contents = 'a', toClass = 2)
        self.addPostRollbackScript(contents = 'a', toClass = [ 6, 9 ],
                                   groupName = 'group-other')
"""

        grp = self.build(group1, 'basicGroup', returnName = 'group-basic')
        grpFoo = self.openRepository().getTrove('group-other',
                        grp.getVersion(), grp.getFlavor() )

        assert(grp.getCompatibilityClass() == 3)
        assert(_conversions(grp.troveInfo.scripts.postRollback) == [ '3->2' ])
        assert(grpFoo.getCompatibilityClass() == 7)
        assert(_conversions(grpFoo.troveInfo.scripts.postRollback) ==
                        [ '7->6', '7->9' ])

        # test addCopy for comapatibility classes
        groupCopy = """
class copiedGroup(GroupRecipe):
    name = "group-copy"
    version = "1"
    clearBuildRequires()

    def setup(r):
        r.addCopy('group-basic', recurse = True, copyScripts = False,
                  copyCompatibilityClass = True)
"""
        grp = self.build(groupCopy, 'copiedGroup', returnName = 'group-copy')
        assert(grp.getCompatibilityClass() is 0)
        grpBasic = self.openRepository().getTrove('group-basic',
                        grp.getVersion(), grp.getFlavor() )
        assert(grpBasic.getCompatibilityClass() == 3)
        assert(grpBasic.troveInfo.scripts.postRollback.script() == '')
        grpFoo = self.openRepository().getTrove('group-other',
                        grp.getVersion(), grp.getFlavor() )
        assert(grpFoo.getCompatibilityClass() == 7)
        assert(grpFoo.troveInfo.scripts.postRollback.script() == '')

        # test default group handling
        groupText = """
class GroupDist(GroupRecipe):
    name = "group-dist"
    version = "3"
    clearBuildRequires()

    def setup(r):
        r.add('foo:run')

        r.createGroup('group-foo', autoResolve=False, depCheck=False)
        r.addNewGroup('group-foo', groupName='group-dist', byDefault=True)
        r.setDefaultGroup('group-foo')
        r.add('foo:run')

        r.setCompatibilityClass(2)
        r.addPostRollbackScript(contents='a', toClass=1)
"""
        grpDist = self.build(groupText, 'GroupDist')
        assert(grpDist.getCompatibilityClass() == 0)
        grpFoo = self.openRepository().getTrove('group-foo',
                        grpDist.getVersion(), grpDist.getFlavor() )
        assert(grpFoo.getCompatibilityClass() == 2)
        assert(_conversions(grpFoo.troveInfo.scripts.postRollback) ==
                        [ '2->1' ])

        # test broken compatibility classes
        brokenGroup = """
class GroupDist(GroupRecipe):
    name = "group-dist"
    version = "3"
    clearBuildRequires()

    def setup(r):
        r.setCompatibilityClass('a')
"""

        try:
            grpDist = self.build(brokenGroup, 'GroupDist')
        except errors.CookError, e:
            assert(str(e).endswith('RecipeFileError: group compatibility classes must be integers'))
        else:
            assert(0)

        # test more brokenness
        brokenGroup = """
class GroupDist(GroupRecipe):
    name = "group-dist"
    version = "3"
    clearBuildRequires()

    def setup(r):
        r.addPostRollbackScript(contents='a', toClass='a')
"""

        try:
            grpDist = self.build(brokenGroup, 'GroupDist')
        except errors.RecipeFileError, e:
            assert(str(e).endswith('group compatibility classes must be integers'))
        else:
            assert(0)

    def _getSourcesForGroup(self, name, versionStr=''):
        repos = self.openRepository()
        cfg = self.cfg
        loader, sourceVersion = loadrecipe.recipeLoaderFromSourceComponent(
                                            name, cfg, repos,
                                            versionStr=versionStr,
                                            labelPath = cfg.buildLabel,
                                            buildFlavor=cfg.buildFlavor)[0:2]
        recipeClass = loader.getRecipe()
        groupRecipe = recipeClass(repos, cfg, sourceVersion.branch().label(),
                                  cfg.buildFlavor, None, None, {})
        cook._callSetup(cfg, groupRecipe)
        return grouprecipe.findSourcesForGroup(repos, groupRecipe)

    def testGetSourceComponents(self):
        flavoredGroup = """
class flavoredGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    Flags.bar = True
    clearBuildRequires()
    def setup(self):
        self.add("test", "@rpl:linux", 'test.foo')
"""
        trv1 = self.addComponent('test:source', '1').getNameVersionFlavor()
        trv2 = self.addComponent('test:source', ':test1/1').getNameVersionFlavor()
        self.addComponent('group-test:source', '1.0', [('group-test.recipe',
                                                        basicGroup)])
        troveTups = set(self._getSourcesForGroup('group-test'))
        assert(troveTups == set([(trv1[0], trv1[1], None),
                                 (trv2[0], trv2[1], None)]))
        self.addComponent('group-test:source', '1.0-2', [('group-test.recipe',
                                                          flavoredGroup)])
        troveTups = set(self._getSourcesForGroup('group-test'))
        assert(troveTups == set([(trv1[0], trv1[1],
                                  deps.parseFlavor('test.foo'))]))

    def testGetReplaceComponents(self):
        '''
        Ensure that sources included via r.replace are picked up in
        findSourcesForGroup.

        @tests: CNY-2605
        @tests: CNY-2606
        '''

        replaceGroup = """
class replaceGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    imageGroup = False
    clearBuildRequires()
    def setup(self):
        self.replace("test1")
        self.replace("test2", groupName=self.name)
"""
        trv1 = self.addComponent('test1:source', '1').getNameVersionFlavor()
        trv2 = self.addComponent('test2:source', '1').getNameVersionFlavor()
        self.addComponent('group-test:source', '1.0', [('group-test.recipe',
                                                        replaceGroup)])
        troveTups = set(self._getSourcesForGroup('group-test'))
        self.assertEqual(troveTups, set([
            (trv1[0], trv1[1], None),
            (trv2[0], trv2[1], None),
          ]))

    def testAddBackComponent(self):
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    Flags.bar = True
    clearBuildRequires()
    def setup(self):
        self.addAll("group-core")
        self.add('foo')
"""
        self.addComponent('foo:lib')
        self.addComponent('foo:data', filePrimer=1)
        self.addComponent('foo:debuginfo', filePrimer=2)
        self.addCollection('foo', [':lib', ':data', (':debuginfo', False)])
        self.addCollection('group-core', ['foo'], 
                            weakRefList=[('foo:lib', False), 
                                         ('foo:data', True),
                                         ('foo:debuginfo', True)])
        self.addComponent('group-test:source', '1.0', [('group-test.recipe',
                                                          groupRecipe)])
        grp = self.build(groupRecipe, 'basicGroup')
        fooLib = [ x for x in grp.iterTroveListInfo() if x[0][0] == 'foo:lib'][0]
        fooDebug = [ x for x in grp.iterTroveListInfo() if x[0][0] == 'foo:debuginfo'][0]
        byDefault = fooLib[1]
        assert(byDefault)
        byDefault = fooDebug[1]
        assert(byDefault)

    def testGetSourceComponentsWithSearchPath(self):
        # test to make sure getting the sources to build works
        # correctly with a searchPath
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    imageGroup = False
    clearBuildRequires()
    def setup(self):
        self.setSearchPath('group-bar')
        self.add('bam:run', '1-1-1')
        self.add('bam:run')
"""
        self.addComponent('group-test:source', [('group-test.recipe', 
                                                  groupRecipe)])
        self.addComponent('bam:run=:branch/1-1-1')
        self.addComponent('bam:run=:branch/2-1-1', filePrimer=1)
        self.addComponent('bam:source=:branch/1-1')
        self.addComponent('bam:source=:branch/2-1')
        self.addCollection('group-bar', [('bam:run=:branch/1-1-1'),
                                         ('bam:run=:branch/2-1-1')])
        troveTups = set(self._getSourcesForGroup('group-test'))
        assert(len(troveTups) == 2)
        assert(sorted(str(x[1].trailingRevision()) for x in troveTups) == ['1-1', '2-1'])

    def testGetQualifiedSourceComponents(self):
        '''
        Check that r.add with label+revision and with full version is
        handled correctly.

        @tests: CNY-2768
        '''
        buildRep = self.cfg.buildLabel.getHost()
        buildNs = self.cfg.buildLabel.getNamespace()
        buildLabel = '%s@%s:branch' % (buildRep, buildNs)
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    imageGroup = False
    clearBuildRequires()
    def setup(self):
        self.setSearchPath('group-bar')
        self.add('bam:run', '%(label)s/1')
        self.add('bam:run', '%(label)s/1-1')
        self.add('bam:run', '%(label)s/1-1-1')
        self.add('bam:run', '/%(label)s/1-1-1')
""" % dict(label=buildLabel)
        self.addComponent('group-test:source', [('group-test.recipe', 
                                                  groupRecipe)])
        self.addComponent('bam:run=:branch/1-1-1')
        self.addComponent('bam:run=:branch/2-1-1', filePrimer=1)
        self.addComponent('bam:source=:branch/1-1')
        self.addComponent('bam:source=:branch/2-1')
        self.addCollection('group-bar', [('bam:run=:branch/1-1-1'),
                                         ('bam:run=:branch/2-1-1')])
        troveTups = set(self._getSourcesForGroup('group-test'))
        self.assertEqual(len(troveTups), 1)
        self.assertEqual(str(list(troveTups)[0][1].trailingRevision()), '1-1')


    def testAddAllRedirect(self):
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    imageGroup = False
    clearBuildRequires()
    def setup(self):
        self.addAll("group-foo")
"""
        self.addComponent('group-test:source', '1.0',
                          [('group-test.recipe', groupRecipe)])
        self.addComponent('foo:run=1')
        foo = self.addCollection('foo=1', [':run'])
        self.addCollection('group-foo', '1', [],
                           redirect=[('group-foo', '/localhost@rpl:branch')])
        self.addCollection('group-foo', ':branch', ['foo=:linux'])
        self.logFilter.add()
        grp = self.build(groupRecipe, 'basicGroup', logLevel=log.INFO)
        self.logFilter.compare(['+ Following redirects for trove group-foo=/localhost@rpl:linux/1-1-1[]',
        '+ Found group-foo=/localhost@rpl:branch/1-1-1[] following redirect'],
        allowMissing=True)
        assert(grp.iterTroveList(strongRefs=True).next() == foo.getNameVersionFlavor())

    def testAddRedirect(self):
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.add("foo", 'localhost@rpl:branch')
"""
        self.addComponent('group-test:source', '1.0',
                          [('group-test.recipe', groupRecipe)])
        self.addComponent('foo:run=1')
        foo = self.addCollection('foo=1', [':run'])
        self.addComponent('foo:run=:branch/1', 
                          redirect=[('foo:run', '/localhost@rpl:linux')])
        self.addCollection('foo=:branch/1', [':run'],
                                 redirect=[('foo', '/localhost@rpl:linux')])
        self.logFilter.add()
        grp = self.build(groupRecipe, 'basicGroup', logLevel=log.DEBUG)
        assert(grp.iterTroveList(strongRefs=True).next()
                                    == foo.getNameVersionFlavor())
        self.logFilter.compare(['+ Following redirects for trove foo=/localhost@rpl:branch/1-1-1[]', '+ Found foo=/localhost@rpl:linux/1-1-1[] following redirect'], allowMissing=True)

    def testAddRedirectToNothing(self):
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.add("foo:run", 'localhost@rpl:linux')
        self.add('bar:run', 'localhost@rpl:linux')
"""
        self.addComponent('foo:run', redirect=[None])
        bar = self.addComponent('bar:run')
        self.logFilter.add()
        grp = self.build(groupRecipe, 'basicGroup', logLevel=log.DEBUG)
        self.logFilter.compare([
            '+ Following redirects for trove foo:run=/localhost@rpl:linux/1.0-1-1[]', 
            '+ Redirect is to nothing'], allowMissing=True)
        assert(grp.iterTroveList(strongRefs=True).next()
                                    == bar.getNameVersionFlavor())
        assert(len(list(grp.iterTroveList(strongRefs=True))) == 1)

    def testAddRedirectFailsBecauseOfSearchPath(self):
        raise testhelp.SkipTestException('Test is no longer valid after CNY-1993 - but maybe we will change this behavior?')
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.setSearchPath('group-foo', 'foo:run')
        self.add("foo:run", 'localhost@rpl:linux')
"""
        self.addComponent('foo:run', redirect=['bar:run'])
        bar = self.addComponent('bar:run')
        self.addComponent('bam:run')
        self.addCollection('group-foo', ['bam:run'])
        try:
            grp = self.build(groupRecipe, 'basicGroup')
            assert 0, "Should have gotten error"
        except cook.CookError, msg:
            self.assertEquals(str(msg), 'Could not find redirect target for foo:run=/localhost@rpl:linux/1.0-1-1[].  Check your search path or remove redirect from recipe: bar:run was not found on path localhost@rpl:linux')

    def testAddRedirectToBranch(self):
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.add("foo:run", 'localhost@rpl:linux')
"""
        foo = self.addComponent('foo:run=/localhost@rpl:linux/1-1')
        self.addComponent('foo:run=/localhost@rpl:branch//linux/1-1', 
                          redirect=[('foo:run', '/localhost@rpl:linux', None)])
        self.logFilter.add()
        grp = self.build(groupRecipe, 'basicGroup', logLevel=log.DEBUG)
        self.logFilter.compare([
           '+ Following redirects for trove foo:run=/localhost@rpl:branch//linux/1-1[]',
           '+ Found foo:run=/localhost@rpl:linux/1-1[] following redirect'], 
             allowMissing=True)
        assert(grp.iterTroveList(strongRefs=True).next()
                                    == foo.getNameVersionFlavor())
        assert(len(list(grp.iterTroveList(strongRefs=True))) == 1)

    def testSearchSourceSearchesTroveSourceFirst(self):
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.setSearchPath('localhost@rpl:linux', 'group-foo=:1')
        self.add("foo:run", 'localhost@rpl:1')
"""
        correctFoo = self.addComponent('foo:run=:1/1')
        self.addComponent('foo:run=:1/2')
        self.addCollection('group-foo=:1/1', ['foo:run'])
        grp = self.build(groupRecipe, 'basicGroup')
        assert(grp.iterTroveList(strongRefs=True).next()
                                    == correctFoo.getNameVersionFlavor())
        assert([repr(x) for x in grp.getSearchPath()] == ["Label('localhost@rpl:linux')", "('group-foo', VFS('/localhost@rpl:1/1-1-1'), Flavor(''))"])

    def testSearchSourceSearchesTroveSourceFirstWithOnlyTroveSource(self):
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.setSearchPath('group-foo=:1')
        self.add("foo:run", 'localhost@rpl:linux')
"""
        correctFoo = self.addComponent('foo:run=1')
        self.addComponent('foo:run=:1/1')
        self.addCollection('group-foo=:1/1', ['foo:run'])
        grp = self.build(groupRecipe, 'basicGroup')
        assert(grp.iterTroveList(strongRefs=True).next()
                                    == correctFoo.getNameVersionFlavor())

        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.setSearchPath('group-foo=:1')
        self.add("bar:run")
"""
        try:
            grp = self.build(groupRecipe, 'basicGroup')
        except cook.CookError, msg:
            assert(str(msg) == 'No search label path given and no label specified for trove bar:run - set the installLabelPath')


    def testRemoveFromCopiedGroup(self):

        TestGroup = """
class TestGroup(GroupRecipe):
    name = 'group-dist'
    version = '1'
    clearBuildRequires()

    def setup(r):
        r.addCopy('group-bar')
        r.remove('test')
"""

        # Create a group.
        self.addComponent('test:runtime', '1', filePrimer=1)
        self.addCollection('test', '1', [':runtime'])
        self.addCollection('group-foo', '1', ['test'])
        self.addCollection('group-bar', '1', ['group-foo'])
        trove = self.build(TestGroup, 'TestGroup')

    def testReplaceFromAddAll(self):
        raise testhelp.SkipTestException('CNY-2162 - replace + addAll interaction')
        groupRecipe = """
class basicGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.addAll("group-foo")
        self.replace('foo')
"""
        self.addComponent('foo:lib')
        self.addComponent('foo:runtime', filePrimer=1)
        self.addCollection('foo', [':lib', ':runtime'])
        self.addCollection('group-foo', ['foo'], weakRefList=[('foo:lib', True),
                                                   ('foo:runtime', False)])

        self.addComponent('foo:lib', '2')
        self.addComponent('foo:runtime', '2', filePrimer=1)
        self.addCollection('foo', '2', [':lib', ':runtime'])
        grp = self.build(groupRecipe, 'basicGroup')
        byDefault = dict((x[0], grp.includeTroveByDefault(*x)) \
                    for x in grp.iterTroveList(weakRefs=True, 
                                               strongRefs=False))
        self.assertEquals({'foo:lib': True, 'foo:runtime': False},
                          byDefault)

    def testFlavorPreferencesAllowBiarchDepResolutionOnX86System(self):
        TestGroup = """
class TestGroup(GroupRecipe):
    name = 'group-dist'
    version = '1'

    autoResolve = True
    requireLatest = False
    clearBuildRequires()

    def setup(r):
        r.add('foo:run') # gets older x86_64 trove since we're cooking
                         # for x86_64
        r.add('bar:run') # falls back to x86 since no x86_64 package is 
                         # available
"""
        foo64 = self.addComponent('foo:run=1[is:x86_64]', filePrimer=1)
        self.addComponent('foo:run=2[is:x86]')
        bar = self.addComponent('bar:run=1[is:x86]', requires='trove: bam:run',
                          filePrimer=2)
        bam = self.addComponent('bam:run=1[is:x86]', filePrimer=3)
        self.addComponent('group-dist:source', '1',
                            [('group-dist.recipe', TestGroup)])
        self.cookFromRepository('group-dist[is:x86 x86_64]')
        trv = self.findAndGetTrove('group-dist[is:x86 x86_64]')
        assert(set(trv.iterTroveList(strongRefs=True))
                == set(x.getNameVersionFlavor() for x in (foo64, bam, bar)))

    def testFlavorPreferencesWithSearchPath(self):
        TestGroup = """
class TestGroup(GroupRecipe):
    name = 'group-dist'
    version = '1'
    clearBuildRequires()

    autoResolve = True
    requireLatest = False

    def setup(r):
        r.setSearchPath('group-foo')
        r.add('foo:run') # gets older x86_64 trove since we're cooking
                         # for x86_64
        r.add('bar:run') # falls back to x86 since no x86_64 package is 
                         # available
"""
        foo64 = self.addComponent('foo:run=1[is:x86_64]', filePrimer=1,
                                   requires='trove: baz:run')
        self.addComponent('foo:run=2[is:x86]')
        bar = self.addComponent('bar:run=1[is:x86]', requires='trove: bam:run',
                                filePrimer=2)
        bam = self.addComponent('bam:run=1[is:x86]', filePrimer=3)
        baz64 = self.addComponent('baz:run=1[is:x86_64]', filePrimer=4)
        self.addComponent('baz:run=2[is:x86]')
        self.addCollection('group-foo[is:x86 x86_64]',
                          ['foo:run=1[is:x86_64]', 'foo:run=2[is:x86]',
                           'bar:run=1[is:x86]', 'bam:run=1[is:x86]',
                           'baz:run=1[is:x86_64]', 'baz:run=2[is:x86]'])
        self.addComponent('group-dist:source', '1',
                            [('group-dist.recipe', TestGroup)])
        self.cookFromRepository('group-dist[is:x86 x86_64]')
        trv = self.findAndGetTrove('group-dist[is:x86 x86_64]')
        assert(set(trv.iterTroveList(strongRefs=True))
                == set(x.getNameVersionFlavor() for x in (foo64, bam, bar, 
                                                          baz64)))

    @testhelp.context('requireLatest')
    def testRequireLatestNoVersion(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'
    clearBuildRequires()

    requireLatest = %(LATEST)s

    def setup(r):
        r.add('foo', flavor = 'old')
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestLabel(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        # different branch. same label
        self.addComponent('foo:lib', version = '/localhost@rpl:dev//linux/2',
                flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'
    clearBuildRequires()

    requireLatest = %(LATEST)s

    def setup(r):
        r.add('foo', versionStr = 'localhost@rpl:linux', flavor = 'old')
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestBranch(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        # different branch. same label
        self.addComponent('foo:lib', version = '/localhost@rpl:dev//linux/2',
                flavor = newFlavor)
        self.addCollection('foo', version = '/localhost@rpl:dev//linux/2',
                strongList = ['foo:lib'], defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    requireLatest = %(LATEST)s
    clearBuildRequires()

    def setup(r):
        r.add('foo', versionStr = '/localhost@rpl:linux', flavor = 'old')
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        # the group should build just fine. there's only the old flavor on
        # the /localhost@rpl:linux branch
        grp = self.build(recipeStr % {'LATEST': 'True'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        self.addComponent('foo:lib', version = '3', flavor = newFlavor)
        self.addCollection('foo', version = '3', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestRevision(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'
    clearBuildRequires()

    requireLatest = %(LATEST)s

    def setup(r):
        r.add('foo', versionStr = '1', flavor = 'old')
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        # the group should build just fine. there's only the old flavor
        # with that upstream version
        grp = self.build(recipeStr % {'LATEST': 'True'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        self.addComponent('foo:lib', version = '1-1-2', flavor = newFlavor)
        self.addCollection('foo', version = '1-1-2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestFullVersion(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = %(LATEST)s

    def setup(r):
        r.add('foo', versionStr = '/localhost@rpl:linux/1-1-1', flavor = 'old')
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        # the group should build just fine. an exact version was specified
        # with that upstream version
        grp = self.build(recipeStr % {'LATEST': 'True'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        self.addComponent('foo:lib', version = '1', flavor = newFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)
        grp = self.build(recipeStr % {'LATEST': 'True'}, 'OldFlavor')
        # group will continue to build
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])

    @testhelp.context('requireLatest')
    def testRequireLatestNoVersion2(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'
    clearBuildRequires()

    requireLatest = True

    def setup(r):
        r.add('foo', flavor = 'old', requireLatest = %(LATEST)s)
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestLabel2(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        # different branch. same label
        self.addComponent('foo:lib', version = '/localhost@rpl:dev//linux/2',
                flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = True

    def setup(r):
        r.add('foo', versionStr = 'localhost@rpl:linux', flavor = 'old',
                requireLatest = %(LATEST)s)
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestBranch2(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        # different branch. same label
        self.addComponent('foo:lib', version = '/localhost@rpl:dev//linux/2',
                flavor = newFlavor)
        self.addCollection('foo', version = '/localhost@rpl:dev//linux/2',
                strongList = ['foo:lib'], defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = True

    def setup(r):
        r.add('foo', versionStr = '/localhost@rpl:linux', flavor = 'old',
                requireLatest = %(LATEST)s)
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        # the group should build just fine. there's only the old flavor on
        # the /localhost@rpl:linux branch
        grp = self.build(recipeStr % {'LATEST': 'True'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        self.addComponent('foo:lib', version = '3', flavor = newFlavor)
        self.addCollection('foo', version = '3', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestRevision2(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = True

    def setup(r):
        r.add('foo', versionStr = '1', flavor = 'old',
                requireLatest = %(LATEST)s)
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        # the group should build just fine. there's only the old flavor
        # with that upstream version
        grp = self.build(recipeStr % {'LATEST': 'True'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        self.addComponent('foo:lib', version = '1-1-2', flavor = newFlavor)
        self.addCollection('foo', version = '1-1-2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestFullVersion2(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = True

    def setup(r):
        r.add('foo', versionStr = '/localhost@rpl:linux/1-1-1', flavor = 'old',
                requireLatest = %(LATEST)s)
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        # the group should build just fine. an exact version was specified
        # with that upstream version
        grp = self.build(recipeStr % {'LATEST': 'True'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])
        self.addComponent('foo:lib', version = '1', flavor = newFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)
        grp = self.build(recipeStr % {'LATEST': 'True'}, 'OldFlavor')
        # group will continue to build
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])

    @testhelp.context('requireLatest')
    def testRequireLatestAddCopy(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addCollection('group-foo', version = '1', strongList = ['foo'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)
        self.addCollection('group-foo', version = '2', strongList = ['foo'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = True

    def setup(r):
        # note we're making a new group-foo on the exact same label. inane,
        # but it tests the right stuff.
        # be careful of the order of cooks because a successful build will
        # alter the setup for the test.
        r.addCopy('group-foo', flavor = 'old',
                requireLatest = %(LATEST)s)
"""
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True, True])

    @testhelp.context('requireLatest')
    def testRequireLatestAddAll(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addCollection('group-foo', version = '1', strongList = ['foo'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)
        self.addCollection('group-foo', version = '2', strongList = ['foo'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = True

    def setup(r):
        r.addAll('group-foo', flavor = 'old',
                requireLatest = %(LATEST)s)
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])

        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestReplace(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)
        self.addCollection('group-foo', version = '2', strongList = ['foo'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = True

    def setup(r):
        r.addCopy('group-foo', versionStr = '2', flavor = 'new')
        r.replace('foo', groupName = 'group-foo', newFlavor = 'old',
                requireLatest = %(LATEST)s)
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True, True])

        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestReplace2(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = True

    def setup(r):
        r.add('foo', versionStr = '2', flavor = 'new')
        r.replace('foo', newFlavor = 'old', requireLatest = %(LATEST)s)
"""
        grp = self.build(recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertEquals([x[2] == oldFlavor for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                [True, True])

        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestOverride(self):
        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    def setup(r):
        r.%(COMMAND)s('group-foo', requireLatest = %(CMDLATEST)s)
"""
        class StopExecution(Exception):
            pass
        realFindTroves = netclient.NetworkRepositoryClient.findTroves
        def mockFindTroves(x, *args, **kwargs):
            if sys._getframe(3).f_code.co_name != 'findTrovesForGroups':
                return realFindTroves(x, *args, **kwargs)
            self.assertEquals(kwargs.get('requireLatest'), self.expected)
            raise StopExecution
        self.mock(netclient.NetworkRepositoryClient, 'findTroves',
                mockFindTroves)

        for command in ('add', 'addAll', 'addCopy', 'replace'):
            for recipeLatest in ('True', 'False'):
                for commandLatest in ('True', 'False'):
                    self.expected = commandLatest == 'True'
                    macros = {'RECIPELATEST': recipeLatest,
                            'CMDLATEST': commandLatest,
                            'COMMAND': command}
                    self.assertRaises(StopExecution, self.build,
                            recipeStr % macros, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestOverrideDefault(self):
        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    def setup(r):
        r.%(COMMAND)s('group-foo', requireLatest = %(CMDLATEST)s)
"""
        class StopExecution(Exception):
            pass
        realFindTroves = netclient.NetworkRepositoryClient.findTroves
        def mockFindTroves(x, *args, **kwargs):
            if sys._getframe(3).f_code.co_name != 'findTrovesForGroups':
                return realFindTroves(x, *args, **kwargs)
            self.assertEquals(kwargs.get('requireLatest'), self.expected)
            raise StopExecution
        self.mock(netclient.NetworkRepositoryClient, 'findTroves',
                mockFindTroves)

        for command in ('add', 'addAll', 'addCopy', 'replace'):
            for commandLatest in ('True', 'False'):
                self.expected = commandLatest == 'True'
                macros = {'CMDLATEST': commandLatest,
                        'COMMAND': command}
                self.assertRaises(StopExecution, self.build,
                        recipeStr % macros, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestNoMatch(self):
        oldFlavor = deps.parseFlavor('old')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = %(LATEST)s

    def setup(r):
        r.add('foo', flavor = 'new')
"""
        # this will fail regardless of requireLatest settings. the trove
        # we're requesting simply isn't there. this test is to ensure the
        # requireLatest pathway handles empty results sanely.
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestNoMatch2(self):
        oldFlavor = deps.parseFlavor('old')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    requireLatest = %(LATEST)s

    def setup(r):
        r.add('foo', flavor = 'totallyrandom')
"""
        # this will fail regardless of requireLatest settings. the trove
        # we're requesting simply isn't there. this test is to ensure the
        # requireLatest pathway handles empty results sanely.
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'False'}, 'OldFlavor')
        self.assertRaises(errors.CookError, self.build,
                recipeStr % {'LATEST': 'True'}, 'OldFlavor')

    @testhelp.context('requireLatest')
    def testRequireLatestMessage(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        newFlavor2 = deps.parseFlavor('new2')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor,
                filePrimer = 1)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor,
                filePrimer = 2)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'

    clearBuildRequires()
    def setup(r):
        r.add('foo', flavor = 'old')
"""
        try:
            self.build(recipeStr, 'OldFlavor')
        except errors.CookError, e:
            self.assertEquals(str(e), 'foo=/localhost@rpl:linux/1-1-1[old] was found, but newer troves exist:\nfoo=/localhost@rpl:linux/2-1-1[new]\n\nThis error indicates that conary selected older versions of the troves mentioned above due to flavor preference. You should probably select one of the current flavors listed. If you meant to select an older trove, you can pass requireLatest=False as a parameter to r.add, r.replace or related calls. To disable requireLatest checking entirely, declare requireLatest=False as a recipe attribute.')
        else:
            self.fail('Expected CookError to be raised')
        self.addComponent('foo:lib', version = '2', flavor = newFlavor2)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor2)
        try:
            self.build(recipeStr, 'OldFlavor')
        except errors.CookError, e:
            self.assertEquals(str(e), 'foo=/localhost@rpl:linux/1-1-1[old] was found, but newer troves exist:\nfoo=/localhost@rpl:linux/2-1-1[new]\nfoo=/localhost@rpl:linux/2-1-1[new2]\n\nThis error indicates that conary selected older versions of the troves mentioned above due to flavor preference. You should probably select one of the current flavors listed. If you meant to select an older trove, you can pass requireLatest=False as a parameter to r.add, r.replace or related calls. To disable requireLatest checking entirely, declare requireLatest=False as a recipe attribute.')
        else:
            self.fail('Expected CookError to be raised')

    @testhelp.context('requireLatest')
    def testRequireLatestMessage2(self):
        oldFlavor = deps.parseFlavor('old')
        newFlavor = deps.parseFlavor('new')
        newFlavor2 = deps.parseFlavor('new2')
        self.addComponent('foo:lib', version = '1', flavor = oldFlavor,
                filePrimer = 1)
        self.addCollection('foo', version = '1', strongList = ['foo:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('foo:lib', version = '2', flavor = newFlavor,
                filePrimer = 2)
        self.addCollection('foo', version = '2', strongList = ['foo:lib'],
                defaultFlavor = newFlavor)
        self.addComponent('bar:lib', version = '1', flavor = oldFlavor,
                filePrimer = 3)
        self.addCollection('bar', version = '1', strongList = ['bar:lib'],
                defaultFlavor = oldFlavor)
        self.addComponent('bar:lib', version = '2', flavor = newFlavor,
                filePrimer = 4)
        self.addCollection('bar', version = '2', strongList = ['bar:lib'],
                defaultFlavor = newFlavor)

        recipeStr = """
class OldFlavor(GroupRecipe):
    name = 'group-old'
    version = '1'
    clearBuildRequires()

    def setup(r):
        r.add('foo', flavor = 'old')
        r.add('bar', flavor = 'old')
"""
        try:
            self.build(recipeStr, 'OldFlavor')
        except errors.CookError, e:
            message = str(e)
            self.assertFalse('foo' not in message,
                    "Error should have referenced foo")
            self.assertFalse('bar' not in message,
                    "Error should have referenced bar")
        else:
            self.fail('Expected CookError to be raised')


    def testImageGroupPropogation(self):
        recipeStr = """
class ImageGroupTest(GroupRecipe):
    name = 'group-sys'
    version = '1'
    clearBuildRequires()

    # verify that the default is True

    clearBuildRequires()

    def setup(r):
        # check that no specification is false
        r.createGroup('group-1')
        r.addNewGroup('group-1')
        r.add('foo', groupName = 'group-1')
        # check that conary honors an explicit False
        r.createGroup('group-2', imageGroup = False)
        r.addNewGroup('group-2')
        # check that conary honors an explicit True
        r.createGroup('group-3', imageGroup = True)
        r.addNewGroup('group-3')

        #check that no spec in a subgroup is false
        r.createGroup('group-2-1')
        r.addNewGroup('group-2-1', groupName = 'group-2')
        r.add('foo', groupName = 'group-2-1')

        #check that no spec in a subgroup is false
        r.createGroup('group-3-1')
        r.addNewGroup('group-3-1', groupName = 'group-3')
        r.add('foo', groupName = 'group-3-1')
"""
        self.addComponent('foo:runtime')
        self.addCollection('foo', strongList = ['foo:runtime'])

        grp = self.build(recipeStr, 'ImageGroupTest', returnName = 'group-sys')
        ref = {'group-sys'  : 1,
                'group-1'   : 0,
                'group-2'   : 0,
                'group-3'   : 1,
                'group-2-1' : 0,
                'group-3-1' : 0,
                'foo'       : None,
                'foo:runtime': None}
        repos = self.openRepository()
        for nvf in grp.iterTroveList(strongRefs = True, weakRefs = True):
            trv = repos.getTrove(*nvf)
            self.assertEquals(trv.troveInfo.imageGroup(),
                    ref[trv.getName()])

    def testStartGroup(self):
        startGroup = """
class StartGroup(GroupRecipe):
    name = 'group-start'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.startGroup('group-foo')
        self.add('foo:runtime')
"""

        self.addComponent('foo:runtime')
        self.addCollection('foo', strongList = ['foo:runtime'])
        trv = self.build(startGroup, 'StartGroup', returnName = 'group-start')
        # we're testing the setDefaultGroup behavior of startGroup here
        self.assertEquals(['group-foo'],
                [x[0] for x in trv.iterTroveList(strongRefs = True,
                                                 weakRefs = False)])
        self.assertEquals(['foo', 'foo:runtime', 'group-foo'],
                sorted([x[0] for x in trv.iterTroveList(strongRefs = True,
                                                        weakRefs = True)]))

    def testStartGroup2(self):
        startGroup = """
class StartGroup(GroupRecipe):
    name = 'group-start'
    version = '1.0'
    clearBuildRequires()

    def setup(self):
        self.startGroup('group-foo')
        self.startGroup('group-bar', groupName = 'group-foo',
                imageGroup = True)
        self.add('foo:runtime')
"""

        self.addComponent('foo:runtime')
        self.addCollection('foo', strongList = ['foo:runtime'])
        trv = self.build(startGroup, 'StartGroup', returnName = 'group-start')
        # we're testing the setDefaultGroup behavior of startGroup here
        self.assertEquals(['group-foo'],
                [x[0] for x in trv.iterTroveList(strongRefs = True,
                                                 weakRefs = False)])
        self.assertEquals(['foo', 'foo:runtime', 'group-bar', 'group-foo'],
                sorted([x[0] for x in trv.iterTroveList(strongRefs = True,
                                                        weakRefs = True)]))
        self.assertEquals(trv.troveInfo.imageGroup(), 1)
        repos = self.openRepository()
        nvf = list(trv.iterTroveList(strongRefs = True))[0]
        groupFoo = repos.getTrove(*nvf)
        self.assertEquals(groupFoo.troveInfo.imageGroup(), 0)
        nvf = [x for x in groupFoo.iterTroveList(strongRefs = True) \
                if x[0] == 'group-bar'][0]
        groupBar = repos.getTrove(*nvf)
        self.assertEquals(groupBar.troveInfo.imageGroup(), 1)

    def testStartGroupByDefault(self):
        startGroup = """
class StartGroup(GroupRecipe):
    name = 'group-start'
    version = '1.0'
    clearBuildRequires()

    checkOnlyByDefaultDeps = False

    def setup(self):
        self.startGroup('group-foo', byDefault = False)
        self.add('foo:runtime')
"""

        self.addComponent('foo:runtime')
        self.addCollection('foo', strongList = ['foo:runtime'])
        trv = self.build(startGroup, 'StartGroup', returnName = 'group-start')
        # we're testing the setDefaultGroup behavior of startGroup here
        self.assertEquals(['group-foo'],
                [x[0] for x in trv.iterTroveList(strongRefs = True,
                                                 weakRefs = False)])
        self.assertEquals(['foo', 'foo:runtime', 'group-foo'],
                sorted([x[0] for x in trv.iterTroveList(strongRefs = True,
                                                        weakRefs = True)]))
        repos = self.openRepository()
        nvf = repos.findTrove(self.cfg.buildLabel, ('group-foo', None, None))[0]
        trv = repos.getTrove(*nvf)

        # prove that foo was added byDefault = True to group-foo
        self.assertEquals([(x[0][0], x[1], x[2]) for x in \
                trv.iterTroveListInfo()], [('foo', True, True),
                ('foo:runtime', True, False)])

    def testStartGroupByDefault2(self):
        startGroup = """
class StartGroup(GroupRecipe):
    name = 'group-start'
    version = '1.0'
    clearBuildRequires()

    checkOnlyByDefaultDeps = False
    byDefault = False

    def setup(self):
        # create a group which has a byDefault = False setting
        self.createGroup('group-bar', byDefault = False)
        self.addNewGroup('group-bar', byDefault = True,
                groupName = 'group-start')
        self.setDefaultGroup('group-bar')

        self.startGroup('group-foo')
        self.add('foo:runtime')
"""

        self.addComponent('foo:runtime')
        self.addCollection('foo', strongList = ['foo:runtime'])
        trv = self.build(startGroup, 'StartGroup', returnName = 'group-bar')
        # we're testing the setDefaultGroup behavior of startGroup here
        self.assertEquals(['group-foo'],
                [x[0] for x in trv.iterTroveList(strongRefs = True,
                                                 weakRefs = False)])
        self.assertEquals(['foo', 'foo:runtime', 'group-foo'],
                sorted([x[0] for x in trv.iterTroveList(strongRefs = True,
                                                        weakRefs = True)]))
        repos = self.openRepository()
        nvf = repos.findTrove(self.cfg.buildLabel, ('group-foo', None, None))[0]
        trv = repos.getTrove(*nvf)

        # prove that foo was added byDefault = True to group-foo
        # note that this defies the convention of "inherit setting from parent"
        self.assertEquals([(x[0][0], x[1], x[2]) for x in \
                trv.iterTroveListInfo()], [('foo', True, True),
                ('foo:runtime', True, False)])


    def testRemoveFromCopiedGroup2(self):
        TestGroup = """
class TestGroup(GroupRecipe):
    name = 'group-dist'
    version = '1'
    clearBuildRequires()

    imageGroup = False

    def setup(r):
        r.addCopy('group-bar')
        r.remove('test')
"""

        # Create a group.
        self.addComponent('test:runtime', '1', filePrimer=1)
        self.addCollection('test', '1', [':runtime'])
        self.addCollection('group-foo', '1', ['test'])
        self.addCollection('group-bar', '1', ['group-foo'])
        trove = self.build(TestGroup, 'TestGroup')

    def testImageAutoResolve1(self):
        self.addComponent('bar:info')
        self.addCollection('bar', strongList = ['bar:info'])
        self.addComponent('foo:run', '1.0',
                 fileContents = [ ( 'file', 'contents', None,
                     deps.parseDep('trove: bar:info') ) ] )
        self.addCollection('foo', strongList = ['foo:run'])

        # autoResolve is unspecified but this is a image group
        grp = self.build(imageAutoResolveGroup1, "ImageAutoResolveGroup")
        self.assertFalse('bar:info' not in [x[0] for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                "expected bar:info to be present due to autoResolve")

    def testImageAutoResolve2(self):
        self.addComponent('bar:info')
        self.addCollection('bar', strongList = ['bar:info'])
        self.addComponent('foo:run', '1.0',
                 fileContents = [ ( 'file', 'contents', None,
                     deps.parseDep('trove: bar:info') ) ] )
        self.addCollection('foo', strongList = ['foo:run'])

        # autoResolve is unspecified but this is a image group
        grp = self.build(imageAutoResolveGroup2, "ImageAutoResolveGroup")
        self.assertFalse('bar:info' in [x[0] for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                "expected bar:info to be absent due to autoResolve")

    def testImageAutoResolve3(self):
        self.addComponent('bar:info')
        self.addCollection('bar', strongList = ['bar:info'])
        self.addComponent('foo:run', '1.0',
                 fileContents = [ ( 'file', 'contents', None,
                     deps.parseDep('trove: bar:info') ) ] )
        self.addCollection('foo', strongList = ['foo:run'])

        # autoResolve is unspecified but this is a image group
        grp = self.build(imageAutoResolveGroup3, "ImageAutoResolveGroup")
        self.assertFalse('bar:info' not in [x[0] for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                "expected bar:info to be present due to autoResolve")

    def testImageAutoResolve4(self):
        self.addComponent('bar:info')
        self.addCollection('bar', strongList = ['bar:info'])
        self.addComponent('foo:run', '1.0',
                 fileContents = [ ( 'file', 'contents', None,
                     deps.parseDep('trove: bar:info') ) ] )
        self.addCollection('foo', strongList = ['foo:run'])

        # autoResolve is unspecified but this is a image group
        grp = self.build(imageAutoResolveGroup4, "ImageAutoResolveGroup")
        self.assertFalse('bar:info' in [x[0] for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                "expected bar:info to be absent due to autoResolve")

    def testImageAutoResolve5(self):
        self.addComponent('bar:info')
        self.addCollection('bar', strongList = ['bar:info'])
        self.addComponent('foo:run', '1.0',
                 fileContents = [ ( 'file', 'contents', None,
                     deps.parseDep('trove: bar:info') ) ] )
        self.addCollection('foo', strongList = ['foo:run'])

        imageGroup = """
class ImageAutoResolveGroup(GroupRecipe):
    name = 'group-image'
    version = '1.90'
    clearBuildRequires()

    imageGroup = False

    def setup(r):
        r.setSearchPath('localhost@rpl:linux', 'localhost@rpl:branch')
        r.createGroup('group-foo', imageGroup = True)
        r.addNewGroup('group-foo', groupName = 'group-image')
        r.add('foo:run', groupName = 'group-foo')
"""

        # autoResolve is unspecified but this is a image group
        grp = self.build(imageGroup, "ImageAutoResolveGroup")
        self.assertFalse('bar:info' not in [x[0] for x in \
                grp.iterTroveList(strongRefs = True, weakRefs = True)],
                "expected bar:info to be present due to autoResolve")

    def testAllowMissingAdd(self):
        self.addComponent('foo:runtime')
        self.addCollection('foo', strongList = ['foo:runtime', ])

        grp = self.build(allowMissing1, 'AllowMissing', logLevel=log.INFO)

        trvs = [ x[0] for x in grp.iterTroveList(strongRefs=True) ]
        self.assertEqual(len(trvs), 1)
        self.assertTrue('foo' in trvs)

    def testAllowMissingAddAll(self):
        self.addComponent('foo:runtime')
        self.addCollection('foo', strongList=['foo:runtime', ])
        self.addCollection('group-foo', strongList=['foo', ])

        grp = self.build(allowMissing2, 'AllowMissing', logLevel=log.INFO)

        trvs = [ x[0] for x in grp.iterTroveList(strongRefs=True) ]
        self.assertEqual(len(trvs), 1)
        self.assertTrue('foo' in trvs)

    def testAllowMissingReplace(self):
        self.addComponent('foo:runtime')
        self.addCollection('foo', strongList=['foo:runtime', ])

        grp = self.build(allowMissing3, 'AllowMissing', logLevel=log.INFO)

        trvs = [ x[0] for x in grp.iterTroveList(strongRefs=True) ]
        self.assertEqual(len(trvs), 1)
        self.assertTrue('foo' in trvs)
