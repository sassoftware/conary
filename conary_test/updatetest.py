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
import copy
import errno
import grp, itertools
import os
import pwd
import shutil
import stat
import tempfile
import StringIO
import sys
import time

from conary_test import recipes
from conary_test import rephelp

from conary import callbacks
from conary import conarycfg, conaryclient
from conary import errors
from conary import trove
from conary import versions
from conary.build import use
from conary.cmds import commit
from conary.cmds import conarycmd
from conary.cmds import cscmd
from conary.cmds import query
from conary.cmds import queryrep
from conary.cmds import showchangeset
from conary.cmds import updatecmd
from conary.conaryclient import cmdline, cml, resolve, update, systemmodel
from conary.deps import deps
from conary.lib import util, log
from conary.local import database
from conary.repository import changeset
from conary.versions import VersionFromString as VFS
import conary.local.update

rollbackListOutput = [
    '\t  updated: testcase:runtime 1.0-1-1%(flavor)s -> 1.1-1-1%(flavor)s',
    '\tinstalled: testcase:runtime 1.0-1-1%(flavor)s',
]

display1Output = """\
testcase:runtime=1.0-1-1
  /etc/changedconfig    1.0-1-1
  /etc/unchangedconfig    1.0-1-1
  /usr/bin/hello    1.0-1-1
  /usr/share/changed    1.0-1-1
  /usr/share/unchanged    1.0-1-1
"""

display2Output = """\
testcase:runtime=1.0-1-1
  -rw-r--r--    1 root     root           20 Apr 23 10:56 /etc/changedconfig
  -rw-r--r--    1 root     root           20 Apr 23 10:56 /etc/unchangedconfig
  -rwxr-xr-x    1 root     root         4615 Apr 23 10:56 /usr/bin/hello
  -rw-r--r--    1 root     root           20 Apr 23 10:56 /usr/share/changed
  -rw-r--r--    1 root     root           20 Apr 23 10:56 /usr/share/unchanged
"""

display3Output = """\
testcase=1.2-1-1
"""

display4Output = """\
testcase=1.0-1-1[%(is)s]
testcase=1.1-1-1[%(is)s]
testcase=1.2-1-1[%(is)s]
"""

display1OutputNoFlavor = """\
testcase:runtime=1.0-1-1
  /etc/changedconfig    1.0-1-1
  /etc/unchangedconfig    1.0-1-1
  /usr/share/changed    1.0-1-1
  /usr/share/unchanged    1.0-1-1
"""

display2OutputNoFlavor = """\
testcase:runtime=1.0-1-1
-rw-r--r--    1 root     root           20 Apr 23 10:56 /etc/changedconfig
-rw-r--r--    1 root     root           20 Apr 23 10:56 /etc/unchangedconfig
-rw-r--r--    1 root     root           20 Apr 23 10:56 /usr/share/changed
-rw-r--r--    1 root     root           20 Apr 23 10:56 /usr/share/unchanged
"""

display4OutputNoFlavor = """\
testcase=1.0-1-1
testcase=1.1-1-1
testcase=1.2-1-1
"""

changedConfigConflicts = """\
@@ -2,7 +2,7 @@
 2
 3
 4
-5
+1
 6
 7
 8
"""

MIXED = 2

class UpdateTest(rephelp.RepositoryHelper):
    def setTroveVersion(self, val):
        self.OLD_TROVE_VERSION = trove.TROVE_VERSION
        self.OLD_TROVE_VERSION_1_1 = trove.TROVE_VERSION_1_1
        trove.TROVE_VERSION = val
        trove.TROVE_VERSION_1_1 = val

    def restoreTroveVersion(self):
        trove.TROVE_VERSION = self.OLD_TROVE_VERSION
        trove.TROVE_VERSION_1_1 = self.OLD_TROVE_VERSION_1_1

    def _compareRoots(self, fcn, root1, root2):
        for root, dirs, files in os.walk(root1):
            for f in files:
                if f.find('changed') == -1:
                    continue
                dirname = root[len(root1) + 1:]
                if fcn(os.path.join(root1, dirname, f),
                       os.path.join(root2, dirname, f)) == False:
                    return False

        return True

    def rootContentsSame(self, root1, root2):
        return self._compareRoots(self.compareFileContents, root1, root2)

    def rootPermissionsSame(self, root1, root2):
        return self._compareRoots(self.compareFileModes, root1, root2)

    def verifyPermissions(self, path, expected):
        return (os.lstat(path).st_mode & 07777) == expected

    def applyLocalChangeSet(self, root, fileName):
        db = database.Database(root, self.cfg.dbPath)
        commit.doLocalCommit(db, fileName)
        db.close()

    def setupRecipes(self, owner = "root", group = "root", withBinary = True,
                     withUse = False):
        repos = self.openRepository()
        startDict = { }
        built = []

        for (text, name) in [ (recipes.testRecipe1, "TestRecipe1"),
                              (recipes.testRecipe2, "TestRecipe2"),
                              (recipes.testRecipe3, "TestRecipe3") ]:
            loader = rephelp.LoaderFromString(text, "/path",
                                              objDict = startDict,
                                              cfg = self.cfg,
                                              repos = repos,
                                              component = name)
            d = loader.getModuleDict()

            d[name].filename = "test"
            d[name].owner = owner
            d[name].group = group
            d[name]._sourcePath = None

            if withBinary == MIXED:
                if name == "TestRecipe3":
                    d[name].withBinary = True
                else:
                    d[name].withBinary = False
            elif withBinary:
                d[name].withBinary = True
            else:
                d[name].withBinary = False

            if withUse:
                d[name].withUse = True

            built += self.cookObject(loader)
            startDict[name] = loader.getRecipe()

            # This is awful, but it stops conary from being upset that we're
            # leaving a recipe in the namespace
            del startDict[name].version
            startDict[name].internalAbstractBaseClass = True

        return (built, d)

    def testConflicts(self):
        root = self.workDir + "/root"

        (built, d) = self.setupRecipes()

        vers = [ x[1] for x in built ]
        pkgname = built[0][0]

        self.updatePkg(root, pkgname, vers[0])
        localchange = '\n'.join('1' * 10)
        f = open('%s/etc/changedconfig' %root, 'w+')
        f.write(localchange)
        f.close()
        self.logFilter.add()
        self.logFilter.ignore('change "implements self" to "implements handler".*')
        self.updatePkg(root, pkgname, vers[1])
        self.verifyFile('%s/etc/changedconfig.conflicts' %root,
                        changedConfigConflicts)
        self.verifyFile('%s/etc/changedconfig' %root, localchange)
        self.logFilter.remove()
        self.logFilter.compare('warning: conflicts from merging changes '
                               'from head into %s/etc/changedconfig saved '
                               'as %s/etc/changedconfig.conflicts'
                               %(root, root))
        # XXX verify that the version in the db is vers[1]
        
    def basicTest(self, owner = "root", group = "root", flavor = True):

        root1 = self.workDir + "/root1"
        root2 = self.workDir + "/root2"

        if owner != 'root':
            # we'll need to satisfy this dependency
            userrecipe = """
class User(UserInfoRecipe):
    name = 'info-%(owner)s'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.User('%(owner)s', 1000, group='%(group)s')
""" % {'owner': owner, 'group': group}
            (userbuilt, d) = self.buildRecipe(userrecipe, "User")
            userbuilt = ['%s=%s' % x[:2] for x in userbuilt]
            self.updatePkg(root1, userbuilt)
            self.updatePkg(root2, userbuilt)
            rbbase = 1
        else:
            rbbase = 0

        (built, d) = self.setupRecipes(owner = owner, group = group, withBinary = flavor)
        vers = [ x[1] for x in built ]
        pkgname = built[0][0]

        _time = time.time
        try:
            time.time = lambda: 1111111111.0
            self.updatePkg(root1, pkgname, vers[0])
            self.updatePkg(root2, pkgname, vers[0])
        finally:
            time.time = _time

        for n in ( 'changed', 'unchanged' ):
            self.verifyFile(root1 + "/etc/" + n + "config",
                            d['TestRecipe1'].fileText)
            self.verifyFile(root1 + "/usr/share/" + n,
                            d['TestRecipe1'].fileText)

        if self.rootContentsSame(root1, root2) != True:
            self.fail('installing the same package in two different roots '
                      'did not result in the same files')
        if self.rootPermissionsSame(root1, root2) != True:
            self.fail('installing the same package in two different roots '
                      'did not result in identical permissions')

        self.mock(time, 'localtime', time.gmtime)
        db = database.Database(root1, self.cfg.dbPath)
        (rc, s) = self.captureOutput(query.displayTroves, db, self.cfg,
                [pkgname], info=True)
        db.close()
        self.assertIn('Installed : Fri Mar 18 01:58:31 2005\n', s)

        os.chmod(os.path.join(root2, 'etc/changedconfig'), 0777)
        self.updatePkg(root1, pkgname, vers[1])
        self.updatePkg(root2, pkgname, vers[1])

        self.verifyFile(root1 + "/etc/unchangedconfig",
                        d['TestRecipe1'].fileText)
        self.verifyFile(root1 + "/usr/share/unchanged",
                        d['TestRecipe1'].fileText)
        self.verifyFile(root1 + "/etc/changedconfig",
                        d['TestRecipe2'].fileText)
        self.verifyFile(root1 + "/usr/share/changed",
                        d['TestRecipe2'].fileText)

        if self.rootContentsSame(root1, root2) != True:
            self.fail('updating to a newer version of a package with a '
                      'local permission change and a contents change in '
                      'the changeset did not result in the new contents '
                      'being written')
        if not self.verifyPermissions(
                    os.path.join(root2, 'etc/changedconfig'), 0777):
            self.fail('permission change in second root did not survive '
                      'an update to a newer version of the package')
        shutil.rmtree(root1)
        shutil.rmtree(root2)
        if owner != 'root':
            self.updatePkg(root1, userbuilt)
            self.updatePkg(root2, userbuilt)


        self.updatePkg(root1, pkgname, vers[0])
        changedText = d['TestRecipe1'].fileText + "new line\n"
        changedText2 = d['TestRecipe2'].fileText + "new line\n"

        for n in ( 'changed', 'unchanged' ):
            self.writeFile(root1 + "/etc/" + n + "config",
                            changedText)
            if n != "changed":
                self.writeFile(root1 + "/usr/share/" + n,
                                changedText)

        self.updatePkg(root1, pkgname, vers[1])

        self.verifyFile(root1 + "/etc/unchangedconfig",
                        changedText)
        # Conary won't touch this file as it didn't change between
        # versions
        self.verifyFile(root1 + "/usr/share/unchanged",
                        changedText)
        self.verifyFile(root1 + "/etc/changedconfig",
                        changedText2)
        # Conary will leave the modified version of this file in
        # place; a warning would be nice though!
        #self.verifyFile(root1 + "/usr/share/changed", changedText)
        

        # check that we can display things properly
        repos = self.openRepository()
        (rc, stg) = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                       [ "%s=%s" % (pkgname, vers[0]) ],
                                       ls=True, fileVersions=True,
                                       alwaysDisplayHeaders=True)
        if flavor:
            assert(stg == display1Output)
        else:
            assert(stg == display1OutputNoFlavor)
            
        (rc, stg) = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                       [ "%s=%s" % (pkgname, vers[0]) ],
                                       lsl = True, alwaysDisplayHeaders=True)
        if flavor:
            ourLines = display2Output.split("\n")
        else:
            ourLines = display2OutputNoFlavor.split("\n")
        theLines = stg.split("\n")
        assert(len(ourLines) == len(theLines))
        for (i, ourLine) in enumerate(ourLines):
            theFields = theLines[i].split()
            ourFields = ourLine.split()

            for (j, ourField) in enumerate(ourFields):
                # skip the date and time
                if (j >= 4 and j <= 7): 
                    continue
                elif j == 2:
                    assert theFields[j] == owner
                elif j == 3:
                    assert theFields[j] == group
                else:
                    assert(ourField == theFields[j])

        (rc, stg) = self.captureOutput(queryrep.displayTroves, self.cfg)
        stg = '\n'.join([ x for x in stg.split('\n') if 'info-' not in x ])
        assert(stg == display3Output)

        if flavor:
            self.cfg.fullFlavors = True
        (rc, stg) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       [], 
                                  versionFilter = queryrep.VERSION_FILTER_ALL, 
                                  flavorFilter =  queryrep.FLAVOR_FILTER_ALL)
        stgList = stg.split('\n')
        stgList = [ x for x in stgList if 'info-' not in x]
        stg = '\n'.join(stgList)

        if flavor:
            assert(stg == display4Output % self.buildIs)
        else:
            assert(stg == display4OutputNoFlavor % self.buildIs)

        del repos

        rblist = self.rollbackList(root1)
        rblist = [ x for x in rblist.split('\n')
                   if x.startswith('\t') and 'info-' not in x ]
        expected = ''
        if self.cfg.fullFlavors:
            expected = '[%(is)s]' % self.buildIs
        expectedMsg = [ x % {'flavor' : expected} for x in rollbackListOutput ]
        self.assertEqual(rblist, expectedMsg)

        self.logFilter.add()
        try:
            self.rollback(root1, 7)
        except database.RollbackDoesNotExist, e:
            self.assertEqual(str(e), "rollback r.7 does not exist")
            self.logFilter.compare("error: rollback 'r.7' not present")
        else:
            self.fail("Expected an exception")

        assert(not self.rollback(root1, rbbase+1))
        self.verifyFile(root1 + "/etc/unchangedconfig", changedText)
        self.verifyFile(root1 + "/usr/share/unchanged", changedText)
        self.verifyFile(root1 + "/etc/changedconfig", changedText)
        self.verifyFile(root1 + "/usr/share/changed", 
                        d['TestRecipe1'].fileText)

        assert(not self.rollback(root1, rbbase+0))
        # var sticks around as it contains the database
        remains = [ 'var' ]
        if owner != 'root':
            remains.append('etc')
        self.verifyDirectory(remains, dir = root1)

        self.resetRoot()
        self.updatePkg(self.rootDir, 'testcase', recurse = False)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert([ x for x in db.iterAllTroveNames()
                 if not x.startswith('info-') ] == [ 'testcase' ])
        self.cfg.fullFlavors = False

    @testhelp.context('rollback')
    def testBasic1(self):
        self.basicTest()

    @testhelp.context('rollback')
    def testBasic2(self):
        self.basicTest(owner = self.owner, group = self.group)

    @testhelp.context('rollback')
    def testBasic3(self):
        self.basicTest(flavor = False)

    @testhelp.context('rollback')
    def testBasicAsRoot(self):
        self.mimicRoot()
        self.basicTest(flavor = False)
        self.realRoot()

    @testhelp.context('rollback')
    def testLocalRollbacks(self):
        self.cfg.localRollbacks = True
        try:
            self.basicTest()
        finally:
            self.cfg.localRollbacks = False

    def testMerges(self):
        repos = self.openRepository()
        double1 = self.build(recipes.doubleRecipe1, "Double")
        testcase1 = self.build(recipes.testRecipe1, "TestRecipe1")

        repos = self.openRepository()
        cs1 = repos.createChangeSet([(double1.getName(), (None, None), 
                                     (double1.getVersion(), 
                                      double1.getFlavor()), 
                                     1)])
        
        cs2 = repos.createChangeSet([(testcase1.getName(), (None, None), 
                                     (testcase1.getVersion(), 
                                      testcase1.getFlavor()), 
                                     1)])
        
        cs1.merge(cs2)
        self.updatePkg(self.rootDir, cs1)

        self.verifyDirectory(["foo1", "changedconfig", "unchangedconfig" ], 
                             dir = self.rootDir + "/etc")

    def testWithAndWithoutFlavor(self):
        """
        test updating from a trove without a flavor to one with a flavor
        and updating from a trove with a flavor to one without a flavor
        """
        (built, d) = self.setupRecipes(withBinary = MIXED)
        vers = [ x[1] for x in built ]
        pkgname = built[0][0]

        # test going from a trove without flavor to a trove with flavor
        self.updatePkg(self.rootDir, pkgname, vers[0])
        self.updatePkg(self.rootDir, pkgname, vers[2])
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.getTroveVersionList(pkgname)[0].asString() == vers[2])
        db.close()

        # test going from a trove with flavor to a trove without flavor
        self.resetRoot()
        self.updatePkg(self.rootDir, pkgname, vers[2])
        self.updatePkg(self.rootDir, pkgname, vers[0])
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.getTroveVersionList(pkgname)[0].asString() == vers[0])
        db.close()

    def testFlavorChanges(self):
        """
        test updating from a trove where the flavor has a use flag
        and an instruction set, to one with a use flag and no instruction
        set.  test the inverse as well
        """
        (built, d) = self.setupRecipes(withBinary=MIXED, withUse=True)
        vers = [ x[1] for x in built ]
        flavors = [ deps.formatFlavor(x[2]) for x in built ]
        pkgname = built[0][0]

        # test going from a trove without instruction set to a trove with one
        self.updatePkg(self.rootDir, pkgname, vers[0], flavor = flavors[0])
        self.updatePkg(self.rootDir, pkgname, vers[2], flavor = flavors[2])
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.getTroveVersionList(pkgname)[0].asString() == vers[2])
        db.close()

        # test going from a trove with instrunction set to a trove without one
        self.resetRoot()
        self.updatePkg(self.rootDir, pkgname, vers[2], flavor = flavors[2])
        self.updatePkg(self.rootDir, pkgname, vers[0], flavor = flavors[0])
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.getTroveVersionList(pkgname)[0].asString() == vers[0])
        db.close()

    def testNonFlavorFileChange(self):
        """
        Test handling of a file which changes between flavors of a trove
        even though the file itself is not executable.
        """
        repos = self.openRepository()
        d = { 'cfg' : self.cfg, 'repos' : repos }
        built = []
        text = []

        loader = rephelp.LoaderFromString(recipes.testRecipe1, "/path",
                                          cfg = self.cfg,
                                          repos = repos,
                                          component = 'TestRecipe1')
        recipe = loader.getRecipe()

        recipe.withUse = False
        text.append(recipe.fileText)
        built += self.cookObject(loader)

        # change the flavor and the file
        recipe.withUse = True
        recipe.initialFileText = "1\n2\n3\n4\n5\n"
        text.append(recipe.initialFileText)
        built += self.cookObject(loader)

        # it's a shame built doesn't return the flavors; this makes sure
        # the version wasn't updated though
        assert(built[0][0:2] == built[1][0:2])

        repos = self.openRepository()
        version = versions.VersionFromString(built[0][1])

        # make sure all of the pathIds's are the same in the two components
        troves = [ None, None ]
        troves[0] = (repos.getTrove(built[0][0], version, built[0][2]))
        troves[1] = (repos.getTrove(built[1][0], version, built[1][2]))

        pathIds = {}
        for (pathId, path, fileId, fileVersion) in troves[0].iterFileList():
            pathIds[path] = pathId
        for (pathId, path, fileId, fileVersion) in troves[1].iterFileList():
            assert(pathIds[path] == pathId)
            del pathIds[path]
        assert(not pathIds)

        # XXX
        # if we could specify a flavor for updates we could just do that.
        # we can't, so make changesets and install those
        cs = repos.createChangeSet([('testcase:runtime', (None, None), 
                                     (version, troves[0].getFlavor()), 1)])
        self.updatePkg(self.rootDir, cs)
        assert(open(os.path.join(self.rootDir, "usr/share/changed")).read() ==
                    text[0])

        cs = repos.createChangeSet([('testcase:runtime', 
                                     (None, None),
                                     (version, troves[1].getFlavor()), 1)])
        self.updatePkg(self.rootDir, cs)
        assert(open(os.path.join(self.rootDir, "usr/share/changed")).read() ==
                    text[1])

    @testhelp.context('rollback', 'fileoverlap')
    def testLocalNonRegularFileChange(self):
        """
        test to ensure that a local file that changes from a
        regular file to a non-regular file updates properly
        """
        # build recipes
        (built, d) = self.setupRecipes()
        vers = [ x[1] for x in built ]
        pkgname = built[0][0]

        # test that a non-config file that switches from a regular file
        # to a non-regular file updates properly fails with an error
        self.updatePkg(self.rootDir, pkgname, vers[0])

        target = os.path.join(self.rootDir, 'usr/share/changed')
        os.unlink(target)
        os.symlink('danglingSymlink', target)

        self.logCheck(self.updatePkg, (self.rootDir, pkgname, vers[1]), 
                      ("error: changeset cannot be applied:\n"
                       'applying update would cause errors:\n'
                       "file type of /usr/share/changed changed"))
        
        # verify that the update did not succeed
        assert(os.readlink(target) == 'danglingSymlink')

        # it should all work fine if we --replace-files
        self.updatePkg(self.rootDir, pkgname, vers[1], 
                       replaceModifiedFiles = True)
        self.verifyFile(target, d['TestRecipe2'].fileText)

        self.rollback(self.rootDir, 1)
        assert(os.readlink(target) == 'danglingSymlink')

        # test that a config file that switches from a regular file
        # to a non-regular file updates properly fails with an error
        self.resetRoot()
        
        self.updatePkg(self.rootDir, pkgname, vers[0])
        
        target = os.path.join(self.rootDir, 'etc/changedconfig')
        os.unlink(target)
        os.symlink('danglingSymlink', target)

        self.logCheck(self.updatePkg, (self.rootDir, pkgname, vers[1]),
                      ("error: changeset cannot be applied:\n"
                       'applying update would cause errors:\n'
                       "file type of /etc/changedconfig changed",))

        # verify that the update did not succeed
        assert(os.readlink(target) == 'danglingSymlink')

        # it should all work fine if we --replace-files
        self.updatePkg(self.rootDir, pkgname, vers[1], 
                       replaceModifiedConfigFiles = True)
        self.verifyFile(target, d['TestRecipe2'].fileText)

        self.rollback(self.rootDir, 1)
        assert(os.readlink(target) == 'danglingSymlink')

    def testConfigFileChange(self):
        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        vers = [ built[0][1] ]
        (built, d) = self.buildRecipe(recipes.testRecipe4, "TestRecipe4",
                                      d = d)
        vers.append(built[0][1])
        (built, d) = self.buildRecipe(recipes.testRecipe2, "TestRecipe2",
                                      d = d)
        vers.append(built[0][1])

        self.updatePkg(self.rootDir, "testcase", vers[1])
        self.verifyFile(self.rootDir + "/etc/changedconfig",
                        d['TestRecipe4'].fileText)
        self.updatePkg(self.rootDir, "testcase", vers[0])
        self.verifyFile(self.rootDir + "/etc/changedconfig",
                        d['TestRecipe4'].fileText)

        self.resetRoot()
        self.updatePkg(self.rootDir, "testcase", vers[1])
        self.verifyFile(self.rootDir + "/etc/changedconfig",
                        d['TestRecipe4'].fileText)
        self.writeFile(self.rootDir + "/etc/changedconfig", "new text")
        self.logCheck(self.updatePkg, (self.rootDir, "testcase", vers[2]),
                    "warning: preserving contents of /etc/changedconfig "
                    "(now a config file)")
        self.verifyFile(self.rootDir + "/etc/changedconfig",
                        "new text")

        self.resetRoot()
        self.updatePkg(self.rootDir, "testcase", vers[1])
        self.verifyFile(self.rootDir + "/etc/changedconfig",
                        d['TestRecipe4'].fileText)
        self.writeFile(self.rootDir + "/etc/changedconfig", "new text")
        self.updatePkg(self.rootDir, "testcase", vers[0])

        self.verifyFile(self.rootDir + "/etc/changedconfig",
                        "new text")

    def testTransientFileChange(self):
        (built, d) = self.buildRecipe(recipes.testTransientRecipe1,
                                      "TransientRecipe1")
        vers = [ built[0][1] ]
        (built, d) = self.buildRecipe(recipes.testTransientRecipe2,
                                      "TransientRecipe2", d = d)
        vers.append(built[0][1])
        (built, d) = self.buildRecipe(recipes.testTransientRecipe3,
                                      "TransientRecipe3", d = d)
        vers.append(built[0][1])


        self.updatePkg(self.rootDir, "testcase", vers[0])
        self.verifyFile(self.rootDir + "/foo",
                        d['TransientRecipe1'].fileText)
        self.updatePkg(self.rootDir, "testcase", vers[1])
        self.verifyFile(self.rootDir + "/foo",
                        d['TransientRecipe2'].fileText)

        self.resetRoot()
        self.updatePkg(self.rootDir, "testcase", vers[0])
        self.verifyFile(self.rootDir + "/foo",
                        d['TransientRecipe1'].fileText)
        self.writeFile(self.rootDir + "/foo", "new text")
        self.updatePkg(self.rootDir, "testcase", vers[1])
        self.verifyFile(self.rootDir + "/foo",
                        d['TransientRecipe2'].fileText)
        self.writeFile(self.rootDir + "/foo", "new text")
        self.updatePkg(self.rootDir, "testcase", vers[2])
        self.verifyFile(self.rootDir + "/foo2",
                        d['TransientRecipe3'].fileText)
        assert(not os.path.exists(self.rootDir + '/foo'))

    def testTransientFileDeleted(self):
        self.addComponent('test:runtime=1.0',
                          fileContents = [
                          ('/foo', rephelp.RegularFile(contents = 'bar\n',
                                                       transient = True,
                                                       mode = 0644)) ])
        self.addComponent('test:runtime=2.0',
                          fileContents = [
                          ('/foo', rephelp.RegularFile(contents = 'blah\n',
                                                       transient = True,
                                                       mode = 0644)) ])

        self.addComponent('test:runtime=2.1',
                          fileContents = [
                          ('/foo', rephelp.RegularFile(contents = 'blah\n',
                                                       transient = True,
                                                       mode = 0600)) ])

        path = self.rootDir + "/foo"

        self.updatePkg("test:runtime=1.0")
        self.verifyFile(path, "bar\n")
        os.unlink(path)
        (rc, str) = self.captureOutput(self.updatePkg, "test:runtime=2.0")
        assert(str ==
            'warning: /foo is missing (use remove if this is intentional)\n')
        self.verifyFile(path, "blah\n")
        os.unlink(path)
        (rc, str) = self.captureOutput(self.updatePkg, "test:runtime=2.1")
        assert(str ==
            'warning: /foo is missing (use remove if this is intentional)\n')
        assert(not os.path.exists(path))

    def testFileMovesAndBecomesTransientButWasRemoved(self):
        self.addComponent('test:old=1.0',
                          fileContents = [
                          ('/foo', rephelp.RegularFile(contents = 'bar\n',
                                                       transient = False)) ])
        self.addComponent('test:new=1.0',
                          fileContents = [
                          ('/foo', rephelp.RegularFile(contents = 'blah\n',
                                                       transient = True)) ])
        path = self.rootDir + "/foo"
        self.updatePkg("test:old=1.0")
        self.verifyFile(path, "bar\n")
        os.unlink(path)
        (rc, str) = self.captureOutput(self.updatePkg,
                                       [ '-test:old', '+test:new' ])
        self.verifyFile(path, "blah\n")
        self.assertEquals(str,
                'warning: cannot remove /foo: No such file or directory\n')

    def testNewTransients(self):
        # CNY-1841
        self.addComponent('foo:runtime',
              fileContents = [ ('/foo', rephelp.RegularFile(transient = True,
                                                            contents = 'foo') )
                             ] )

        self.addComponent('bar:runtime',
              fileContents = [ ('/foo', rephelp.RegularFile(contents = 'bar') )
                             ] )

        f = file(self.rootDir + '/foo', "w")
        f.write('something')
        f.close()
        self.updatePkg('foo:runtime')
        self.verifyFile(self.rootDir + '/foo', 'foo')

        util.rmtree(self.rootDir)
        self.updatePkg('bar:runtime')
        self.logCheck(self.updatePkg, ([ 'foo:runtime' ],),
            ('error: changeset cannot be applied:\n'
             'applying update would cause errors:\n'
             '%s/foo conflicts with a file owned by '
                    'bar:runtime=/localhost@rpl:linux/1.0-1-1[]' %
                    self.rootDir))

    @testhelp.context('rollback')
    def testMissingFile(self):
        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        version = built[0][1]

        (built, d) = self.buildRecipe(recipes.testRecipe2, "TestRecipe2",
                                      d = d)
        nextVersion = built[0][1]

        self.updatePkg(self.rootDir, "testcase", version)
        assert(os.path.exists(self.rootDir + "/etc/changedconfig"))
        os.unlink(os.path.join(self.rootDir, 'etc/changedconfig'))
        self.logCheck(self.erasePkg, (self.rootDir, "testcase", version),
           [ 'warning: cannot remove /etc/changedconfig: No such '
             'file or directory',
             'warning: /etc/changedconfig has already been removed' ])
        self.rollback(self.rootDir, 1)
        assert(not os.path.exists(self.rootDir + "/etc/changedconfig"))

        self.resetRoot()
        self.updatePkg(self.rootDir, "testcase", version)
        assert(os.path.exists(self.rootDir + "/usr/share/unchanged"))
        os.unlink(os.path.join(self.rootDir, "usr/share/unchanged"))
        self.logCheck(self.updatePkg, (self.rootDir, "testcase", nextVersion),
                      "warning: /usr/share/unchanged is missing (use remove "
                      "if this is intentional)")
        assert(not os.path.exists(self.rootDir + "/usr/share/unchanged"))
        self.logCheck(self.rollback, (self.rootDir, 1), [ ])
        assert(not os.path.exists(self.rootDir + "/usr/share/unchanged"))

    @testhelp.context('rollback')
    def testMissingDirectory(self):
        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        version = built[0][1]

        self.updatePkg(self.rootDir, "testcase", version)
        assert(os.path.exists(self.rootDir + "/etc/changedconfig"))
        util.rmtree(os.path.join(self.rootDir, 'etc'))
        self.logCheck(self.erasePkg, (self.rootDir, "testcase", version),
            [ 'warning: cannot remove /etc/changedconfig: '
                    'No such file or directory',
              'warning: cannot remove /etc/unchangedconfig: '
                    'No such file or directory',
              'warning: /etc/unchangedconfig has already been removed',
              'warning: /etc/changedconfig has already been removed' ])
        self.rollback(self.rootDir, 1)
        assert(not os.path.exists(self.rootDir + "/etc"))

    def testAbsoluteChangeSetUpdate(self):
        (built, d) = self.setupRecipes()
        versionList = [ x[1] for x in built ]
        pkgname = built[0][0]

        csList = []
        for (i, (name, version, flavor)) in enumerate(built):
            v = versions.VersionFromString(version)
            repos = self.openRepository()
            (fd, path) = tempfile.mkstemp()

            repos.createChangeSetFile(
                [(name, (None, None), (v, flavor), True)], path, 
                    recurse = False)
            repos.close()
            os.close(fd)
            csList.append((path, v))

        for (path, v) in csList:
            self.updatePkg(self.rootDir, path)
            os.unlink(path)

    def testChangeset(self):
        (built, d) = self.setupRecipes()
        versionList = [ x[1] for x in built ]
        pkgname = built[0][0]

        csList = []
        for (i, (name, version, flavor)) in enumerate(built):
            v = versions.VersionFromString(version)
            repos = self.openRepository()
            cs = repos.createChangeSet([(name, (None, flavor), (v, flavor), 1)])
            repos.close()
            self.updatePkg(self.rootDir, cs)
            self.verifyFile(self.rootDir + "/etc/changedconfig",
                            d['TestRecipe%d' % (i + 1)].fileText)

        self.resetRoot()
        v = None
        repos = self.openRepository()
        for (i, (name, version, flavor)) in enumerate(built):
            lastVer = v
            v = versions.VersionFromString(version)
            if lastVer:
                self.changeset(repos, [ '%s=%s[%s]--%s[%s]'
                                        % (name, lastVer, flavor, v, flavor) ],
                               self.workDir + '/foo.ccs')
            else:
                self.changeset(repos, [ '%s=%s[%s]'
                                        % (name, v, flavor) ],
                               self.workDir + '/foo.ccs')

            cs = changeset.ChangeSetFromFile(self.workDir + '/foo.ccs')
            self.updatePkg(self.rootDir, cs)

            self.verifyFile(self.rootDir + "/etc/changedconfig",
                            d['TestRecipe%d' % (i + 1)].fileText)

        self.resetRoot()
        csList = []
        against = None
        for (i, (name, version, flavor)) in enumerate(built):
            v = versions.VersionFromString(version)
            repos = self.openRepository()
            (fd, path) = tempfile.mkstemp()
            repos.createChangeSetFile(
                [(name, (against, flavor), (v, flavor), 0)], path)
            repos.close()
            os.close(fd)
            if not against:
                against = v
            csList.append((path, v))

        self.resetRepository()
        for (path, v) in csList:
            repos = self.openRepository()
            repos.commitChangeSetFile(path)
            os.unlink(path)
            v2 = repos.getTroveLeavesByLabel(
                            { pkgname : { self.cfg.buildLabel : None } } )
            v2 = v2[pkgname].keys()
            repos.close()
            assert(len(v2) == 1 and v2[0] == v)

        self.resetRoot()
        self.updatePkg(self.rootDir, pkgname, versionList[0])
        (fd, file) = tempfile.mkstemp()
        try:
            os.close(fd)
            os.chmod(os.path.join(self.rootDir, 'etc/changedconfig'), 0777)
            self.localChangeset(self.rootDir, pkgname, file)
            self.resetRoot()
            self.updatePkg(self.rootDir, pkgname, versionList[0])
            self.applyLocalChangeSet(self.rootDir, file)
        finally:
            os.unlink(file)

        if not self.verifyPermissions(
                    os.path.join(self.rootDir, 'etc/changedconfig'), 0777):
            self.fail("local change set didn't fix permission change")

    def testLdconfig(self):
        (built, d) = self.buildRecipe(recipes.libhelloRecipe, "Libhello")
        version = built[0][1]
        self.logFilter.add()
        self.mimicRoot()
        self.updatePkg(self.rootDir, "libhello:runtime", version)
        self.realRoot()
        self.logFilter.remove()
        self.logFilter.compare(())
        ldsoname = util.joinPaths(self.rootDir, '/etc/ld.so.conf')
        assert(util.isregular(ldsoname))
        ldsoContents = file(ldsoname).read()
        if use.Arch.x86:
            self.assertEquals(ldsoContents, '/lib\n/usr/lib\n')
        elif use.Arch.x86_64:
            self.assertEquals(ldsoContents, '/lib64\n/usr/lib64\n')
        else:
            raise NotImplementedError, 'edit test for this arch'

    def testLdconfigNoSubRoot(self):
        'CNY-2982: no double leading / on entries'
        templdsoconf = StringIO.StringIO()
        def mkstemp(*args, **kwargs):
            return templdsoconf, 'ignoreme'
        def true(*args, **kwargs):
            return True
        def false(*args, **kwargs):
            return False
        def fdopen(*args, **kwargs):
            return args[0]
        def waitpid(*args, **kwargs):
            return True, 0
        templdsoconf.close = true

        self.mock(os.path, "isdir", true)
        self.mock(tempfile, "mkstemp", mkstemp)
        self.mock(os, "fdopen", fdopen)
        self.mock(os, "chmod", true)
        self.mock(os, "rename", true)
        self.mock(os, "fork", true)
        self.mock(os, "waitpid", waitpid)
        self.mock(util, "exists", false)
        self.mimicRoot()
        conary.local.update.shlibAction('/',
            ['/sir/not/appearing/on/this/system/libfoo.a'])
        self.realRoot()
        templdsoconf.seek(0)
        data = templdsoconf.read()
        self.assertEquals('/sir/not/appearing/on/this/system\n', data)

    def testLdconfigSubRoot(self):
        'CNY-2982: handle trailing / on root'
        (built, d) = self.buildRecipe(recipes.libhelloRecipe, "Libhello")
        version = built[0][1]
        self.logFilter.add()
        self.updatePkg(self.rootDir+'/', "libhello:runtime", version)
        self.logFilter.remove()
        self.logFilter.compare(
            ['warning: ldconfig skipped (insufficient permissions)'])
        ldsoname = util.joinPaths(self.rootDir, '/etc/ld.so.conf')
        assert(util.isregular(ldsoname))
        ldsoContents = file(ldsoname).read()
        if use.Arch.x86:
            assert ldsoContents == '/lib\n/usr/lib\n'
        elif use.Arch.x86_64:
            assert ldsoContents == '/lib64\n/usr/lib64\n'
        else:
            raise NotImplementedError, 'edit test for this arch'

    def testLdconfigConfD(self):
        (built, d) = self.buildRecipe(recipes.libhelloRecipeLdConfD, "Libhello")
        version = built[0][1]
        self.logFilter.add()
        self.mimicRoot()
        self.updatePkg(self.rootDir, "libhello:runtime", version)
        self.realRoot()
        self.logFilter.remove()
        self.logFilter.compare(())
        ldsoname = util.joinPaths(self.rootDir, '/etc/ld.so.conf')
        ldsoDname = util.joinPaths(self.rootDir, '/etc/ld.so.conf.d/first.conf')
        assert(util.isregular(ldsoname))
        assert(util.isregular(ldsoDname))
        ldsoContents = file(ldsoname).read()
        if use.Arch.x86:
            assert ldsoContents == 'include /etc/ld.so.conf.d/*.conf\n/opt/foo\n/usr/lib\n'
        elif use.Arch.x86_64:
            assert ldsoContents == 'include /etc/ld.so.conf.d/*.conf\n/opt/foo\n/usr/lib64\n'
        else:
            raise NotImplementedError, 'edit test for this arch'
        ldsoDContents = file(ldsoDname).read()
        if use.Arch.x86:
            assert ldsoDContents == '/lib\n'
        elif use.Arch.x86_64:
            assert ldsoDContents == '/lib64\n'
        else:
            raise NotImplementedError, 'edit test for this arch'

    def testErase(self):
        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        version = built[0][1]

        db = database.Database(self.rootDir, self.cfg.dbPath)
        self.updatePkg(self.rootDir, "testcase", version)
        assert(db.hasTroveByName("testcase"))
        assert(db.hasTroveByName("testcase:runtime"))
        del db

        self.erasePkg(self.rootDir, "testcase:runtime", version)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(not db.hasTroveByName("testcase:runtime"))
        self.erasePkg(self.rootDir, "testcase", version)
        assert(not db.hasTroveByName("testcase"))
        del db

        db = database.Database(self.rootDir, self.cfg.dbPath)
        self.updatePkg(self.rootDir, "testcase", version)
        assert(db.hasTroveByName("testcase"))
        assert(db.hasTroveByName("testcase:runtime"))
        del db

        db = database.Database(self.rootDir, self.cfg.dbPath)
        self.erasePkg(self.rootDir, "testcase", version)
        assert(not db.hasTroveByName("testcase"))
        assert(not db.hasTroveByName("testcase:runtime"))
        del db

    @testhelp.context('rollback')
    def testLocalRollbackChangedMetadata(self):
        self.addComponent('foo:runtime=1', fileContents = [ ( '/foo', '1') ])
        fooPath = self.rootDir + '/foo'
        self.addComponent('foo:runtime=2', fileContents = [ ( '/bar', '2') ])
        client = conaryclient.ConaryClient(self.cfg)
        try:
            client.cfg.localRollbacks = True
            self.updatePkg('foo:runtime=1')
            os.chmod(fooPath, 0600)             # change the inode
            self.verifyFile(fooPath, '1')
            self.updatePkg('foo:runtime=2')
            self.rollback(1)
            self.verifyFile(fooPath, '1')
        finally:
            client.cfg.localRollbacks = False

    @testhelp.context('rollback')
    def testRemove(self):
        self.addComponent('test:runtime', '1.0-1-1',
                 fileContents = [ ( '/etc/changedconfig', '1.0' ),
                                  ( '/etc/unchangedconfig', '1.0') ] )
        self.addCollection('test', '1.0', [ 'test:runtime' ])
        self.addComponent('test:runtime', '2.0-1-1',
                 fileContents = [ ( '/etc/changedconfig', '2.0' ),
                                  ( '/etc/unchangedconfig', '1.0') ] )
        self.addCollection('test', '2.0', [ 'test:runtime' ])

        p = os.path.join(self.rootDir, 'etc/changedconfig')

        self.updatePkg("test:runtime=1.0")
        assert(os.path.exists(p))
        self.removeFile(self.rootDir, "/etc/changedconfig")
        assert(not os.path.exists(p))
        self.updatePkg(self.rootDir, "test:runtime=2.0")
        assert(not os.path.exists(p))

        # q --ls should exclude files which have been removed
        db = database.Database(self.rootDir, self.cfg.dbPath)
        (rc, s) = self.captureOutput(query.displayTroves, db, self.cfg,
                                     ["test:runtime"], lsl=True)
        db.close()
        d = {}
        for line in s.split("\n")[:-1]:
            path = line.split()[8]
            d[path] = 1

        assert(d == {'/etc/unchangedconfig': 1})

        # create the removed file and make sure erase leaves it alone
        open(p, "w")
        self.erasePkg(self.rootDir, 'test:runtime')
        assert(os.path.exists(p))

        # check a relative path, changed contents, and restore
        self.resetRoot()
        self.updatePkg("test=1.0")
        self.writeFile(p, "local")
        curDir = os.open(".", os.O_RDONLY)
        os.chdir(self.rootDir + '/etc')
        try:
            # This warning isn't ideal
            self.logCheck(self.removeFile, (self.rootDir, "changedconfig"),
                          'warning: /etc/changedconfig has changed but has '
                          'been removed on head')
        finally:
            os.fchdir(curDir)
        assert(not os.path.exists(p))

        self.restoreTrove(self.rootDir, "test")
        self.verifyFile(p, '1.0')

        # rollback the restore
        self.rollback(self.rootDir, 2)
        assert(not os.path.exists(p))

        # rollback the remove
        self.rollback(self.rootDir, 1)
        assert(os.path.exists(p))
        self.verifyFile(p, 'local')

        # and an unowned file
        self.resetRoot()
        self.updatePkg("test:runtime=1.0")
        self.logCheck(self.removeFile, (self.rootDir, "/etc/something"),
              "error: no trove owns /etc/something")
        assert(self.rollbackCount() == 0)

        # and a file which has already been removed
        os.unlink(p)
        self.logCheck(self.removeFile, (self.rootDir, "/etc/changedconfig"),
              'warning: /etc/changedconfig is missing (use remove if this is '
              'intentional)')
        assert(self.rollbackCount() == 1)

        # a file we don't have permission to remove
        try:
            os.chmod(self.rootDir + "/etc", 0444)
            self.logCheck(self.removeFile,
                          (self.rootDir, "/etc/unchangedconfig"),
                          "warning: cannot remove /etc/unchangedconfig: "
                          "Permission denied")
            assert(self.rollbackCount() == 2)
        finally:
            os.chmod(self.rootDir + "/etc", 0755)

        # multiple files
        self.resetRoot()
        self.updatePkg("test:runtime=1.0")
        self.removeFile(self.rootDir, "/etc/changedconfig", 
                        "/etc/unchangedconfig")
        assert(not os.path.exists(self.rootDir + '/etc/changedconfig'))
        assert(not os.path.exists(self.rootDir + '/etc/unchangedconfig'))

        db = database.Database(self.rootDir, self.cfg.dbPath)
        (rc, s) = self.captureOutput(query.displayTroves, db, self.cfg,
                                     ["test:runtime"], lsl=True)
        assert(not s)

        self.rollback(self.rootDir, 1)
        assert(os.path.exists(self.rootDir + '/etc/changedconfig'))
        assert(os.path.exists(self.rootDir + '/etc/unchangedconfig'))

    @testhelp.context('rollback')
    def testRestore(self):
        # this is tested as part of testRemove, but this tests some of
        # the outlier conditions
        self.addComponent('foo:runtime=1',
                          fileContents = [ ('/foo', 'f1') ])
        self.addComponent('foo:runtime=2',
                          fileContents = [ ('/foo', 'f2') ])
        self.addComponent('bar:runtime=1',
                          fileContents = [ ('/foo', 'b1') ])
        self.addComponent('bar:runtime=2',
                          fileContents = [ ('/foo', 'b2') ])

        absPath = self.rootDir + '/foo'

        self.updatePkg('foo:runtime=1')
        self.verifyFile(absPath, "f1")                      # 1
        self.removeFile(self.rootDir, '/foo')
        assert(not os.path.exists(absPath))                 # 2
        self.updatePkg('bar:runtime=1')
        self.verifyFile(absPath, "b1")                      # 3
        self.updatePkg('foo:runtime=2')
        self.verifyFile(absPath, "b1")                      # 4

        self.logCheck(self.restoreTrove, (self.rootDir, "foo:runtime"),
              "error: applying update would cause errors:\n"
              "%s/foo is in the way of a newly "
              "created file in "
              "foo:runtime=/localhost@rpl:linux//local@local:LOCAL/2-1-1[]"
              % self.rootDir)
        self.verifyFile(absPath, "b1")

        # this is the same as using --replace-files on an install; we don't
        # support it for restore (though maybe we should)
        self.removeFile(self.rootDir, '/foo')               # 5
        self.restoreTrove(self.rootDir, "foo:runtime")
        self.verifyFile(absPath, "f2")                      # 6
        self.erasePkg(self.rootDir, 'bar:runtime')

        self.logCheck(self.updatePkg, ("bar:runtime=2",),
            'error: changeset cannot be applied:\napplying update would cause errors:\n%s/foo conflicts with a file owned by foo:runtime=/localhost@rpl:linux/2-1-1[]' % self.rootDir)

        self.rollback(self.rootDir, 5)
        assert(not os.path.exists(absPath))                 # 5
        self.rollback(self.rootDir, 4)
        self.verifyFile(absPath, "b1")                      # 4
        self.rollback(self.rootDir, 3)
        self.verifyFile(absPath, "b1")                      # 3
        self.rollback(self.rootDir, 2)
        assert(not os.path.exists(absPath))                 # 2
        self.rollback(self.rootDir, 1)
        self.verifyFile(absPath, "f1")                      # 1
        self.rollback(self.rootDir, 0)
        assert(not os.path.exists(absPath))                 # 0

    def testRemovedFileGetsRemovedFromTrove(self):
        # install 1.0 of trove which has a file '/etc/config'.
        # remove it using 'conary remove /etc/config'
        # update to 2.0 of the trove, in which '/etc/config' has been removed
        (built, d) = self.buildRecipe(recipes.testRemove1, "Remove")
        vers = [ built[0][1] ]
        (built, d) = self.buildRecipe(recipes.testRemove2, "Remove")
        vers.append(built[0][1])

        self.updatePkg(self.rootDir, "remove", vers[0])
        p = os.path.join(self.rootDir, 'etc/config')
        assert(os.path.exists(p))
        # remove the file from the system, and from the database
        self.removeFile(self.rootDir, '/etc/config')
        self.updatePkg(self.rootDir, 'remove', vers[1])
        assert(not os.path.exists(p))

    @testhelp.context('rollback')
    def testModifiedFileRemoved(self):
        (built, d) = self.buildRecipe(recipes.testRecipe1,
                                      "TestRecipe1")
        vers = [ built[0][1] ]
        (built, d) = self.buildRecipe(recipes.testRecipe5,
                                      "TestRecipe5", d = d)
        vers.append(built[0][1])

        self.updatePkg(self.rootDir, "testcase", vers[0])
        self.writeFile(self.rootDir + "/usr/share/unchanged", "new text")
        self.logCheck(self.updatePkg, (self.rootDir, "testcase", vers[1]),
                    "warning: /usr/share/unchanged has changed but has been "
                    "removed on head")
        assert(not os.path.exists(self.rootDir + "/usr/share/unchanged"))

        self.rollback(self.rootDir, 1)
        self.verifyFile(self.rootDir + "/usr/share/unchanged", "new text")

    def testMultipleInstalls(self):
        # this sets up the following structure for trove 'double':
        # 
        #                              root: localhost@rpl:linux
        #
        #                                         * 1.0-1-1
        #                                        /|
        #  localhost@rpl:branch ------>         / |
        #                                      /  |
        #                                     /   |
        #                                    /    |
        #                                   /     |
        #                          1.1-1-1 *      * 2.0-1-1
        #                                 /       |
        #                                /        |
        #                               /         |
        #                              /          |
        #                             /           |
        #                  1.2-1-1   *            * 2.1-1-1
        double1 = self.makeSourceTrove('double', recipes.doubleRecipe1)
        double1 = self.build(recipes.doubleRecipe1, "Double")

        branch = versions.Label("localhost@rpl:branch")
        self.mkbranch(self.cfg.buildLabel, "@rpl:branch", "double:source")

        double2 = self.build(recipes.doubleRecipe2, "Double")
        double2_1 = self.build(recipes.doubleRecipe2_1, "Double")

        oldLabel = self.cfg.buildLabel
        self.cfg.buildLabel = branch
        double1_1 = self.build(recipes.doubleRecipe1_1, "Double")
        double1_2 = self.build(recipes.doubleRecipe1_2, "Double")
        self.cfg.buildLabel = oldLabel

        double2_1 = self.build(recipes.doubleRecipe2_1, "Double")

        self.updatePkg(self.rootDir, 'double', version = "1.0-1-1")
        self.updatePkg(self.rootDir, 'double', keepExisting = True, version = 
            "/localhost@rpl:linux/1.0-1-0/localhost@rpl:branch/1.1-1-1")
        self.verifyDirectory(["foo1", "foo1.1" ], dir = self.rootDir + "/etc")

        self.logFilter.add()
        self.updatePkg(self.rootDir, 'double', version = "1.1-1-1")
        self.logFilter.remove()
        self.logFilter.compare('error: no new troves were found')
        self.verifyDirectory(["foo1", "foo1.1" ], dir = self.rootDir + "/etc")

        self.updatePkg(self.rootDir, 'double')
        self.verifyDirectory(["foo1.2", "foo2.1" ], dir = self.rootDir + "/etc")

        # test that using keep existing ignores branch affinity and simply
        # follows the label path
        self.resetRoot()
        self.updatePkg(self.rootDir, 'double', version = "1.0-1-1")
        oldLabelPath = self.cfg.installLabelPath
        self.cfg.installLabelPath = \
            conarycfg.CfgLabelList([versions.Label('localhost@rpl:branch')])
        self.updatePkg(self.rootDir, 'double', keepExisting = True, version = 
            "1.1-1-1")
        self.verifyDirectory(["foo1", "foo1.1" ], dir = self.rootDir + "/etc")
        self.cfg.installLabelPath = oldLabelPath
 
        self.resetRoot()
        self.updatePkg(self.rootDir, 'double', version = "1.0-1-1")
        repos = self.openRepository()
        
        cs = repos.createChangeSet([('double',
                    (None, None),
                    (double1_1.getVersion(), double1_1.getFlavor()), True) ])
        self.updatePkg(self.rootDir, cs, keepExisting = True)
        self.verifyDirectory(["foo1", "foo1.1" ], dir = self.rootDir + "/etc")

    def testParallelBranches(self):
        # this sets up the following structure for trove 'double':
        # 
        #                              root: localhost@rpl:linux
        #
        #                                         * 1.0-1-1
        #                                        /|
        #  localhost@rpl:branch ------>         / |
        #                                      /  |
        #                                     /   |
        #                                    /    |
        #                                   /     |
        #                          1.1-1-1 *      * 2.0-1-1
        #                                  |       \
        #  localhost@rpl:release ------>   |        \ <- localhost@rpl:release
        #                                  |         \
        #                                  |          \
        #                        1.2-1-1   *           * 2.1-1-1
        #                                 /
        #                                /
        #                               /
        #                              /
        #                   1.3-1-1   *
        self.resetRepository()
        self.resetRoot()

        branch = versions.Label("localhost@rpl:branch")
        release = versions.Label("localhost@rpl:release")
        self.makeSourceTrove('double', recipes.doubleRecipe1)
        double1 = self.build(recipes.doubleRecipe1, "Double")
        self.mkbranch(self.cfg.buildLabel, "@rpl:branch", "double:source")

        self.updateSourceTrove('double', recipes.doubleRecipe2)
        double2 = self.build(recipes.doubleRecipe2, "Double")

        oldLabel = self.cfg.buildLabel
        self.cfg.buildLabel = branch
        self.updateSourceTrove('double', recipes.doubleRecipe1_1)
        double1_1 = self.build(recipes.doubleRecipe1_1, "Double")
        self.cfg.buildLabel = oldLabel
            
        repos = self.openRepository()
        l = [ x.asString() for x in 
                repos.getAllTroveLeaves('localhost', 
                                        { "double" :None })["double"] ]
        l.sort()
        assert(l == ['/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1', 
                     '/localhost@rpl:linux/2.0-1-1'])
        
        l = [ x.asString() for x in 
                repos.getTroveLeavesByLabel(     
                            { "double" : { branch : None }})["double"] ]
        assert(l == ['/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1'])

        
        self.mkbranch(branch, "@rpl:release", "double:source")
        self.cookFromRepository(
             'double=/localhost@rpl:linux/1.0-1/branch/1.1-1/release/1',
             repos=repos)
        self.mkbranch(self.cfg.buildLabel, "@rpl:release", "double:source")
        self.cookFromRepository(
              'double=/localhost@rpl:linux/2.0-1/release/1', repos=repos)

        l = [ x.asString() for x in 
                repos.getTroveLeavesByLabel(
                        { "double" : { release : None } })["double"] ]
        l.sort()
        assert(l == 
          ['/localhost@rpl:linux/1.0-1-0/branch/1.1-1-0/release/1', 
           '/localhost@rpl:linux/2.0-1-0/release/1'])

        self.checkUpdate('double', ['double{,:runtime}=--:linux/2.0-1-1'], 
                        apply=True)
        self.verifyDirectory(["foo2" ], dir = self.rootDir + "/etc")

        self.resetRoot()
        self.cfg.installLabelPath = conarycfg.CfgLabelList([ release ])
        self.checkUpdate('double', ['double{,:runtime}=--/localhost@rpl:linux/2.0-1-0/release/1'], apply=True)
        self.verifyDirectory(["foo2" ], dir = self.rootDir + "/etc")

        self.cfg.buildLabel = release
        self.updateSourceTrove('double', recipes.doubleRecipe1_2,
                 versionStr='/localhost@rpl:linux/1.0-1/branch/1.1-1/release/1')
        self.cfg.buildLabel = oldLabel
        self.cookFromRepository(
             'double=/localhost@rpl:linux/1.0-1/branch/1.1-1/release/1.2-1',
             repos=repos)
        l = [ x.asString() for x in 
                repos.getTroveLeavesByLabel(
                    { "double" : { release : None } })[ "double" ] ]
        l.sort()
        assert(l == 
      ['/localhost@rpl:linux/1.0-1-0/branch/1.1-1-0/release/1.2-1-1', 
           '/localhost@rpl:linux/2.0-1-0/release/1'])

        self.checkUpdate('double', ['double{,:runtime}=/localhost@rpl:linux/2.0-1-0/release/1--/localhost@rpl:linux/1.0-1-0/branch/1.1-1-0/release/1.2-1-1'],
            apply=True)
        self.verifyDirectory(["foo1.2" ], dir = self.rootDir + "/etc")
        self.cfg.buildLabel = release
        self.updateSourceTrove('double', recipes.doubleRecipe1_3,
            versionStr='/localhost@rpl:linux/1.0-1/branch/1.1-1/release/1.2-1')
        self.cfg.buildLabel = oldLabel

        self.cookFromRepository(
             'double=/localhost@rpl:linux/1.0-1/branch/1.1-1/release/1.3-1',
             repos=repos)
        self.updateSourceTrove('double', recipes.doubleRecipe2_1,
            versionStr='/localhost@rpl:linux/2.0-1/release/1')
        self.cookFromRepository(
             'double=/localhost@rpl:linux/2.0-1/release/2.1-1', repos=repos)
        l = [ x.asString() for x in 
                repos.getTroveLeavesByLabel(
                    { "double" : { release : None } } )["double"] ]
        l.sort()
        assert(l == 
          ['/localhost@rpl:linux/1.0-1-0/branch/1.1-1-0/release/1.3-1-1', 
           '/localhost@rpl:linux/2.0-1-0/release/2.1-1-1'])

        self.checkUpdate('double', 'double{,:runtime}=1.2--2.1-1-1', apply=True)
        self.verifyDirectory(["foo2.1" ], dir = self.rootDir + "/etc")

        self.resetRoot()
        self.checkUpdate('double=localhost@rpl:branch',
                         'double{,:runtime}=--:branch/1.1-1-1', apply=True)
        self.verifyDirectory(["foo1.1" ], dir = self.rootDir + "/etc")
        self.checkUpdate('double=localhost@rpl:release',
                         'double{,:runtime}=:branch/1.1-1-1--:release/2.1-1-1',
                         apply=True)
        self.verifyDirectory(["foo2.1" ], dir = self.rootDir + "/etc")
        self.checkUpdate('double=localhost@rpl:branch',
                         'double{,:runtime}=:release/2.1-1-1--:branch/1.1-1-1',
                         apply=True)
        self.verifyDirectory(["foo1.1" ], dir = self.rootDir + "/etc")

        self.resetRoot()
        self.checkUpdate('double=/localhost@rpl:linux/1.0-1-0/branch/1.1-1-0/release/1.3-1-1', 'double{,:runtime}=--:release/1.3-1-1', apply=True)
        self.checkUpdate('double=/localhost@rpl:linux/2.0-1-0/release/2.1-1-1',
                         ['double{,:runtime}=--:release/2.1-1-1'],
                         keepExisting = True, apply=True)
        self.verifyDirectory(["foo1.3", "foo2.1" ], dir = self.rootDir + "/etc")

    def testLinks(self):
        builtList, buildD = self.buildRecipe(recipes.linkRecipe1, "LinkRecipe")
        trove1Info = (builtList[0][0], VFS(builtList[0][1]), builtList[0][2])

        self.updatePkg(self.rootDir, 'linktest')
        assert(os.stat(self.rootDir + "/usr/share/foo") ==
               os.stat(self.rootDir + "/usr/share/bar"))

        trove2 = self.build(recipes.linkRecipe2, "LinkRecipe2",
                            buildDict = buildD)
        # there shouldn't be any files changed between these versions
        repos = self.openRepository()

        self.changeset(repos, ['%s=%s[%s]--%s[%s]' %
            (trove1Info[0], trove1Info[1], trove1Info[2],
                               trove2.getVersion(), trove2.getFlavor() ) ],
            self.workDir + '/foo.ccs')
        cs = changeset.ChangeSetFromFile(self.workDir + '/foo.ccs')

        for trvCs in cs.iterNewTroveList():
            assert(not trvCs.getChangedFileList())

        trove3 = self.build(recipes.linkRecipe3, "LinkRecipe3",
                            buildDict = buildD)
        # the files end up in new link groups this way
        self.changeset(repos, ['%s=%s[%s]--%s[%s]' %
            (trove1Info[0], trove1Info[1], trove1Info[2],
                               trove3.getVersion(), trove3.getFlavor() ) ],
            self.workDir + '/foo.ccs')
        cs = changeset.ChangeSetFromFile(self.workDir + '/foo.ccs')

        for trvCs in cs.iterNewTroveList():
            if trvCs.getName() != "linktest:runtime": continue
            assert(len(trvCs.getChangedFileList()) == 2)
            assert(len(trvCs.getNewFileList()) == 1)

        self.updatePkg(self.rootDir, 'linktest')
        assert(os.stat(self.rootDir + "/usr/share/foo") ==
               os.stat(self.rootDir + "/usr/share/bar") and
               os.stat(self.rootDir + "/usr/share/foo") ==
               os.stat(self.rootDir + "/usr/share/foobar"))

    def testParallelBranches2(self):
        # Just a quick check to make sure that when presented with 
        # parallel branches conary now picks the later of the two.
        self.addComponent('foo:run=localhost@rpl:linux')
        self.addComponent('foo:run=/localhost@rpl:linux//branch/1-1-1')
        self.addComponent('foo:run=/localhost@rpl:branch/1-1-1')
        self.updatePkg('foo:run')
        self.checkUpdate('foo:run=:branch',
                         ['foo:run=:linux--/localhost@rpl:branch'])

    def testLinks2(self):
        # two link groups, both linkgroups have the same contents sha1
        self.build(recipes.linkRecipe4, "LinkRecipe")
        self.updatePkg(self.rootDir, 'linktest')
        assert(os.stat(self.rootDir + "/usr/share/lg1-1") ==
               os.stat(self.rootDir + "/usr/share/lg1-2"))
        assert(os.stat(self.rootDir + "/usr/share/lg2-1") ==
               os.stat(self.rootDir + "/usr/share/lg2-2"))
        assert(os.stat(self.rootDir + "/usr/share/lg1-1") !=
               os.stat(self.rootDir + "/usr/share/lg2-1"))
        assert(os.stat(self.rootDir + "/usr/share/lg1-2") !=
               os.stat(self.rootDir + "/usr/share/lg2-2"))

    def testSymLinkChangedToRegularFile(self):
        # component 'foo' version 1.0-1-1 contains a symlink and is intalled
        # the user changes the symlink to a regular file
        # component 'foo' version 1.0-1-2 still has a symlink and is applied
        # the local change is expected to be retained.
        self.resetRepository()
        self.resetRoot()

        built = []
        vars = { 'hard': 0 }
        for i in range(2):
            (b, d) = self.buildRecipe(recipes.linkRecipe1, "LinkRecipe",
                                      vars=vars)
            built.extend(b)
        vers = [ x[1] for x in built ]

        self.updatePkg(self.rootDir, 'linktest', vers[0])
        target = self.rootDir + '/usr/share/bar'
        os.remove(target)
        f = open(target, 'w')
        f.write('hello\n')
        f.close()
        self.updatePkg(self.rootDir, 'linktest', vers[1])
        sb = os.lstat(target)
        assert(stat.S_ISREG(sb.st_mode))
        f = open(target, 'r')
        assert(f.read() == 'hello\n')

    def testFileTypeChange(self):
        (b, d) = self.buildRecipe(recipes.fileTypeChangeRecipe1, 
                                  "FileTypeChange")
        built1 = b[0]
            

        (b, d) = self.buildRecipe(recipes.fileTypeChangeRecipe2, 
                                  "FileTypeChange")
        built2 = b[0]

        self.updatePkg(self.rootDir, built1[0], built1[1])
        assert(stat.S_ISREG(os.lstat(self.rootDir + "/bin/foo").st_mode))
        self.updatePkg(self.rootDir, built2[0], built2[1])
        assert(stat.S_ISLNK(os.lstat(self.rootDir + "/bin/foo").st_mode))

    @testhelp.context('fileoverlap')
    def testReplaceFilesOnDirectory(self):
        self.addComponent('foo:run', '1.0', 
                          fileContents = [ ('/foo', rephelp.Directory() ) ])
        self.addComponent('foo:run', '1.1', 
                          fileContents = [ ('/foo', 'contents\n') ])

        errMsg = ("error: changeset cannot be applied:\n"
             "applying update would cause errors:\n"
             "directory %s/foo is in the way of a newly created file"
               " in foo:run=/localhost@rpl:linux/1.1-1-1[]") % self.rootDir

        # test applying a changeset when a directory is in the way of a
        # new file
        util.mkdirChain(self.rootDir + '/foo')

        self.logCheck(self.updatePkg, ('foo:run',), errMsg)

        self.updatePkg('foo:run', replaceUnmanagedFiles = True)
        self.verifyFile(self.rootDir + '/foo', 'contents\n')

        # if the directory that's in the way is not empty, the update
        # should bail
        self.resetRoot()
        util.mkdirChain(self.rootDir + '/foo')
        f = open(self.rootDir + '/foo/foo', 'w')
        f.write('hi')
        f.close()

        self.logCheck(self.updatePkg, ('foo:run',), errMsg)

        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(not db.hasTroveByName('foo:run'))

        self.resetRoot()
        self.updatePkg('foo:run=1.1')
        self.updatePkg('foo:run=1.0')
        assert(stat.S_ISDIR(os.stat(self.rootDir + '/foo').st_mode))

    def testIdChange(self):
        # this test depends on fileids not being generated properly when
        # they go missing in intermediate troves. this will have to be
        # more complicated when we fix that (though divergent branches
        # will probably stay this way, giving a way to preserve this test)

        d = self.buildRecipe(recipes.idChange1, "IdChange1")[1]
        d = self.buildRecipe(recipes.idChange2, "IdChange2", d = d)[1]
        d = self.buildRecipe(recipes.idChange3, "IdChange3", d = d)[1]

        self.updatePkg(self.rootDir, 'idchange', version = "1.0-1-1")
        # there have been bugs which make this traceback
        self.updatePkg(self.rootDir, 'idchange', version = "1.2-1-1")
                                    
    def testUpdateToNonExistingTrove(self):
        self.logCheck(self.updatePkg, (self.rootDir, 'testcase',
                                       '/localhost@rpl:linux/1.0-1-1'),
                     'error: version /localhost@rpl:linux/1.0-1-1 of testcase'
                     ' was not found')

    def testSimpleConfig(self):
        vers = [ None, None ]
        (built, d) = self.buildRecipe(recipes.simpleConfig1, 'SimpleConfig1')
        vers[0] = built[0][1]
        (built, d) = self.buildRecipe(recipes.simpleConfig2, 'SimpleConfig2')
        vers[1] = built[0][1]

        self.updatePkg(self.rootDir, 'simpleconfig', vers[0])
        self.verifyFile(self.rootDir + '/etc/foo', 'text 1\n')
        self.updatePkg(self.rootDir, 'simpleconfig', vers[1])
        self.verifyFile(self.rootDir + '/etc/foo', 'text 2\n')

    def testDoubleCookToFile(self):
        root = self.workDir + "/root"

        origDir = os.getcwd()
        d = tempfile.mkdtemp()
        os.chdir(d)

        self.writeFile('testcase.recipe', recipes.testRecipe1)
        repos = self.openRepository()

        built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'testcase.recipe')
        cs = changeset.ChangeSetFromFile('testcase-1.0.ccs')

        self.updatePkg(root, 'testcase-1.0.ccs')
        self.logCheck(self.updatePkg, (root, 'testcase-1.0.ccs'),
              ( "error: no new troves were found" ))

    def testFileMovesComponents(self):
        self.repos = self.openRepository()

        self.addTestPkg(1,
            packageSpecs=[
                "r.Create('/usr/foo/test1')",
                "r.ComponentSpec('runtime', '/usr/foo/test1')",
                "r.Create('/usr/foo/test2')",
                "r.ComponentSpec('lib', '/usr/foo/test2')",
                ])
        self.cookTestPkg(1)
        self.addTestPkg(1, version='1.1',
            packageSpecs=[
                "r.Create('/usr/foo/test1')",
                "r.ComponentSpec('lib', '/usr/foo/test1')",
                "r.Create('/usr/foo/test2')",
                "r.ComponentSpec('lib', '/usr/foo/test2')",
                ])
        self.cookTestPkg(1)
        self.addTestPkg(1, version='1.2',
            packageSpecs=[
                "r.Create('/usr/foo/test1')",
                "r.ComponentSpec('runtime', '/usr/foo/test1')",
                "r.Create('/usr/foo/test2')",
                "r.ComponentSpec('runtime', '/usr/foo/test2')",
                ])
        self.cookTestPkg(1)

        # initial version
        self.logCheck(self.updatePkg, (self.rootDir, 'test1', '1.0-1-1'),
                      (), {'depCheck':False})
        # file moves from runtime to lib
        self.logCheck(self.updatePkg, (self.rootDir, 'test1', '1.1-1-1'),
                      (), {'depCheck':False})
        # lib component disappears -- test1 no longer includes a
        # test1:lib component, so updating test1 should make the
        # test1:lib component no longer be installed and should
        # allow the /usr/foo/test* files to migrate to runtime
        self.logCheck(self.updatePkg, (self.rootDir, 'test1', '1.2-1-1'),
                      (), {'depCheck':False})

    def testJustDatabase(self):
        vers = [ None, None ]
        (built, d) = self.buildRecipe(recipes.testRecipe1, 'TestRecipe1')
        vers[0] = built[0][1]

        self.updatePkg(self.rootDir, 'testcase', vers[0],
                       justDatabase=True)
        self.verifyNoFile(self.rootDir + '/usr/share/unchanged')
        (rc, str) = self.captureOutput(
                self.erasePkg, self.rootDir, 'testcase', justDatabase=True)
        self.assertEquals(str, """\
warning: cannot remove /usr/share/changed: No such file or directory
warning: cannot remove /usr/share/unchanged: No such file or directory
warning: cannot remove /usr/bin/hello: No such file or directory
warning: cannot remove /etc/changedconfig: No such file or directory
warning: cannot remove /etc/unchangedconfig: No such file or directory
""")

        self.updatePkg(self.rootDir, 'testcase', vers[0])
        self.erasePkg(self.rootDir, 'testcase', justDatabase=True)
        self.verifyFile(self.rootDir + '/usr/share/unchanged',
                        d['TestRecipe1'].fileText)

    def testDiffForceSha1(self):
        self.resetRepository()
        self.resetWork()
        self.repos = self.openRepository()

        self.addTestPkg(1, version='1.0',
            packageSpecs=[
                "r.Create('/etc/test1', contents='test1')",
                ])
        self.cookTestPkg(1)
        self.addTestPkg(1, version='1.1',
            packageSpecs=[
                "r.Create('/etc/test1', contents='test2')",
                ])
        self.cookTestPkg(1)

        # initial version
        self.updatePkg(self.rootDir, 'test1', '1.0-1-1', depCheck=False)
        # same size recipe, but changed contents, so should build
        self.updatePkg(self.rootDir, 'test1', '1.1-1-1', depCheck=False)

    def testTroveInfo(self):
        trv = self.build(recipes.testTransientRecipe1, 'TransientRecipe1')
        assert(trv.getSize() == 4)
        assert(trv.getSourceName() == 'testcase:source')

        self.updatePkg(self.rootDir, 'testcase')

        db = self.openRepository()
        dbTrv = db.getTrove(trv.getName(), trv.getVersion(), trv.getFlavor())
        assert(dbTrv.getSize() == 4)
        assert(dbTrv.getSourceName() == 'testcase:source')
        dbTrv = db.getTrove(trv.getName().split(':')[0] , trv.getVersion(), 
                            trv.getFlavor())
        assert(dbTrv.getSize() == 4)
        assert(dbTrv.getSourceName() == 'testcase:source')

    def testConfigFileGoesEmpty(self):
        # there was a bug in fileContentsDiff() that made it not return
        # a diff when one file or the other was empty.  This exercises that
        # case
        self.resetRepository()
        self.resetRoot()

        (built, d) = self.buildRecipe(recipes.configFileGoesEmpty1,
                                      "Config")
        vers = [ built[0][1] ]
        (built, d) = self.buildRecipe(recipes.configFileGoesEmpty2,
                                      "Config")
        vers.append(built[0][1])

        self.updatePkg(self.rootDir, "config", vers[0])
        # a local change forces a three way merge, which will fail
        self.writeFile(self.rootDir + "/etc/config", "# local modification")
        # the line has already been erased from the file, so we should
        # see the patch as already applied and leave the package alone
        self.updatePkg(self.rootDir, "config", vers[1])
        assert(open(self.rootDir + "/etc/config").readlines() ==
                    [ "# local modification" ])

        # XXX test the reverse direction: empty file -> config with contents

    def testConfigFileBecomesSymlink(self):
        (built, d) = self.buildRecipe(recipes.configFileBecomesSymlink1,
                                      "Config")
        self.updatePkg(self.rootDir, "config", built[0][1])
        (built, d) = self.buildRecipe(recipes.configFileBecomesSymlink2,
                                      "Config")
        self.updatePkg(self.rootDir, "config", built[0][1])

    def testSymlinkBecomesConfigFileConfigFile(self):
        (built, d) = self.buildRecipe(recipes.configFileBecomesSymlink2,
                                      "Config")
        self.updatePkg(self.rootDir, "config", built[0][1])
        (built, d) = self.buildRecipe(recipes.configFileBecomesSymlink1,
                                      "Config")
        self.updatePkg(self.rootDir, "config", built[0][1])

    def testSymlinkBecomesFile(self):
        (built, d) = self.buildRecipe(recipes.symlinkBecomesFile1, "Test")
        self.updatePkg(self.rootDir, "test", built[0][1])
        (built, d) = self.buildRecipe(recipes.symlinkBecomesFile2, "Test")
        self.updatePkg(self.rootDir, "test", built[0][1])

    def testDirectoryBecomesSymlink(self):
        # replacing a directory with a symlink is hard.  we want
        # to make sure that conary handles this correctly.
        self.addComponent('test:runtime', '1.0', fileContents =
                [ ('/var/spool/mail', rephelp.Directory()) ] )

        self.addComponent('test:runtime', '2.0', fileContents =
                [ ('/var/spool/mail', rephelp.Symlink('../../srv/spool/mail') ),
                  ('/srv/spool/mail', rephelp.Directory() ) ] )

        self.updatePkg("test:runtime=1.0")
        self.logCheck(self.updatePkg, ("test:runtime=2.0",),
                      ('error: changeset cannot be applied:\n'
                       'applying update would cause errors:\n'
                       '/var/spool/mail changed from a '
                       'directory to a symbolic link.  To apply this '
                       'changeset, first manually move /var/spool/mail '
                       'to /srv/spool/mail, then run "ln -s '
                       '../../srv/spool/mail /var/spool/mail".',))

        # but if the user fixes the problem, the changeset should apply fine
        util.mkdirChain(self.rootDir + '/srv/spool')
        os.rename(self.rootDir + '/var/spool/mail',
                  self.rootDir + '/srv/spool/mail')
        os.symlink('../../srv/spool/mail', self.rootDir + '/var/spool/mail')
        self.logCheck(self.updatePkg, ("test:runtime=2.0",), ())
        assert(os.readlink(self.rootDir + '/var/spool/mail') ==
                                '../../srv/spool/mail')

    def testTestSuite(self):
        version1, flavor1 =self.buildRecipe(recipes.testSuiteRecipe, 
                                            'TestSuiteRecipe')[0][0][1:3]
        version1 = versions.VersionFromString(version1)
        version2, flavor2 =self.buildRecipe(recipes.testSuiteRecipe, 
                                            'TestSuiteRecipe')[0][0][1:3]
        version2 = versions.VersionFromString(version2)

        self.updatePkg(self.rootDir, "testcase", version = version1)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTrove('testcase:runtime', version1, flavor1))
        assert(not db.hasTroveByName('testcase:test'))
        self.updatePkg(self.rootDir, "testcase")
        assert(db.hasTrove('testcase:runtime', version2, flavor2))
        assert(not db.hasTroveByName('testcase:test'))

        self.resetRoot()
        db = database.Database(self.rootDir, self.cfg.dbPath)
        self.updatePkg(self.rootDir, "testcase", version = version1)
        self.updatePkg(self.rootDir, "testcase:test", version = version1,
                       depCheck = False)
        assert(db.hasTrove('testcase:runtime', version1, flavor1))
        assert(db.hasTrove('testcase:test', version1, flavor1))
        self.updatePkg(self.rootDir, "testcase", depCheck = False)
        assert(db.hasTrove('testcase:runtime', version2, flavor2))
        assert(db.hasTrove('testcase:test', version2, flavor2))

        # we'll take the opportunity to do some random tests of changesets
        # as well
        repos = self.openRepository()
        (fd, path) = tempfile.mkstemp()
        os.close(fd)
        cscmd.ChangeSetCommand(self.cfg, [ 'testcase' ], path)
        cs = changeset.ChangeSetFromFile(path)
        troves = set([ x.getName() for x in cs.iterNewTroveList() ])
        assert(troves == set([ 'testcase', 'testcase:runtime' ]) )
        assert([ x[0] for x in cs.getPrimaryTroveList()] == [ 'testcase' ])

        cscmd.ChangeSetCommand(self.cfg, [ 'testcase' ], path, 
                               recurse = False)
        cs = changeset.ChangeSetFromFile(path)
        troves = [ x.getName() for x in cs.iterNewTroveList() ]
        assert(troves == [ 'testcase' ])

        cscmd.ChangeSetCommand(self.cfg, [ 'testcase', 
                                                  'testcase:test' ], path, 
                               recurse = False)
        cs = changeset.ChangeSetFromFile(path)
        troves = set([ x.getName() for x in cs.iterNewTroveList() ])
        assert(troves == set([ 'testcase', 'testcase:test' ]))
        assert(set([ x[0] for x in cs.getPrimaryTroveList()]) == 
                            set([ 'testcase', 'testcase:test' ]))

        self.cfg.excludeTroves.addExp(".*:runtime")
        cscmd.ChangeSetCommand(self.cfg, [ 'testcase' ], path)
        cs = changeset.ChangeSetFromFile(path)
        troves = [ x.getName() for x in cs.iterNewTroveList() ]
        assert(troves == [ 'testcase'] )

        cscmd.ChangeSetCommand(self.cfg, [ 'testcase', 
                                                  'testcase:runtime' ], path)
        cs = changeset.ChangeSetFromFile(path)
        troves = set([ x.getName() for x in cs.iterNewTroveList() ])
        assert(troves == set([ 'testcase', 'testcase:runtime']) )
        assert(set([ x[0] for x in cs.getPrimaryTroveList()]) == 
                            set([ 'testcase', 'testcase:runtime' ]))

        os.unlink(path)

    @testhelp.context('rollback', 'initialcontents')
    def testInitialContents(self):
        version0, flavor0 = self.buildRecipe(recipes.initialContentsRecipe0,
                                             'InitialContentsTest')[0][0][1:3]
        version0 = versions.VersionFromString(version0)
        version01, flavor01 = self.buildRecipe(recipes.initialContentsRecipe01,
                                             'InitialContentsTest')[0][0][1:3]
        version01 = versions.VersionFromString(version01)
        version02, flavor02 = self.buildRecipe(recipes.initialContentsRecipe02,
                                             'InitialContentsTest')[0][0][1:3]
        version02 = versions.VersionFromString(version02)
        version1, flavor1 = self.buildRecipe(recipes.initialContentsRecipe1,
                                             'InitialContentsTest')[0][0][1:3]
        version1 = versions.VersionFromString(version1)
        version2, flavor2 = self.buildRecipe(recipes.initialContentsRecipe2,
                                             'InitialContentsTest')[0][0][1:3]
        version2 = versions.VersionFromString(version2)

        target = self.rootDir + '/foo'

        # First, test update when file exists already on system
        self.writeFile(target, 'previouscontents\n')
        self.updatePkg(self.rootDir, "initcontents", version = version1)
        self.verifyFile(target, 'previouscontents\n')
        self.writeFile(target, 'mynextcontents\n')
        self.updatePkg(self.rootDir, "initcontents", version = version2)
        self.verifyFile(target, 'mynextcontents\n')
        self.erasePkg(self.rootDir, "initcontents")
        self.rollback(self.rootDir, 2)
        self.verifyFile(target, 'mynextcontents\n')
        self.rollback(self.rootDir, 1)
        self.verifyFile(target, 'mynextcontents\n')

        self.resetRoot()
        # now test update when file didn't previously exist on system
        self.updatePkg(self.rootDir, "initcontents", version = version1)
        self.verifyFile(target, 'initialrecipecontents\n')
        self.writeFile(target, 'mynextcontents\n')
        self.updatePkg(self.rootDir, "initcontents", version = version2)
        self.verifyFile(target, 'mynextcontents\n')

        self.resetRoot()
        # test update when no changes are made
        self.updatePkg(self.rootDir, "initcontents", version = version1)
        self.verifyFile(target, 'initialrecipecontents\n')
        self.updatePkg(self.rootDir, "initcontents", version = version2)
        self.verifyFile(target, 'initialrecipecontents\n')

        self.resetRoot()
        # test update when file moves from regular to initialcontents
        self.updatePkg(self.rootDir, "initcontents", version = version01)
        self.verifyFile(target, 'initialregularcontents\n')
        self.updatePkg(self.rootDir, "initcontents", version = version2)
        self.verifyFile(target, 'initialregularcontents\n')

        self.resetRoot()
        # test update when file moves from transient to initialcontents
        self.updatePkg(self.rootDir, "initcontents", version = version0)
        self.verifyFile(target, 'initialtransientcontents\n')
        self.updatePkg(self.rootDir, "initcontents", version = version2)
        self.verifyFile(target, 'initialtransientcontents\n')

        self.resetRoot()
        # test update when file moves from initialcontents to transient
        self.updatePkg(self.rootDir, "initcontents", version = version2)
        self.verifyFile(target, 'secondrecipecontents\n')
        self.writeFile(target, 'mustbeignoredcontents\n')
        self.updatePkg(self.rootDir, "initcontents", version = version0)
        self.verifyFile(target, 'initialtransientcontents\n')

        self.resetRoot()
        # test update when file moves from config to initialcontents
        self.updatePkg(self.rootDir, "initcontents", version = version02)
        self.verifyFile(target, 'initialconfigcontents\n')
        self.updatePkg(self.rootDir, "initcontents", version = version2)
        self.verifyFile(target, 'initialconfigcontents\n')

        self.resetRoot()
        # test update when file moves from initialcontents to config
        self.updatePkg(self.rootDir, "initcontents", version = version1)
        self.verifyFile(target, 'initialrecipecontents\n')
        self.writeFile(target, 'mynextcontents\n')
        self.logFilter.add()
        self.updatePkg(self.rootDir, "initcontents", version = version02)
        self.logFilter.remove()
        self.logFilter.compare('warning: preserving contents of /foo (now a config file)')
        self.verifyFile(target, 'mynextcontents\n')

    def testMovedFiles(self):
        self.addComponent('test:runtime', '1.0-1-1',
                 fileContents = [ ( 'foo', 'contents' ) ] )
        self.addComponent('test:runtime', '2.0-1-1',
                 fileContents = [ ( 'new', 'contents', None,
                                  deps.parseDep('trove: other:runtime')) ] )
        self.addComponent('test:config', '2.0-1-1',
                 fileContents = [ ( 'foo', 'new contents' ) ] )

        self.buildRecipe(recipes.otherRecipe, 'Other')

        self.updatePkg(self.rootDir, 'test:runtime', version = '1.0-1-1')
        self.updatePkg(self.rootDir, [ 'test:runtime', 'other', 'test:config' ])

    @testhelp.context('rollback')
    def testSwitchFromLocalToRepos(self):
        d = tempfile.mkdtemp(dir=self.workDir)

        origDir = os.getcwd()
        os.chdir(d)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', recipes.testTransientRecipe1)
        self.addfile('testcase.recipe')
        self.commit()
        repos = self.openRepository()
        try:
            # emerge testcase onto the system
            built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                            'testcase', emerge=True)
            self.updatePkg(self.rootDir, built[1])
            os.unlink(built[1])
        finally:
            os.chdir(origDir)
            shutil.rmtree(d)

        # cook testcase into the repository
        built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'testcase')
        # update from the emerged version on the system to the version
        # cooked into the repository
        self.updatePkg(self.rootDir, 'testcase', self.cfg.buildLabel)
        self.erasePkg(self.rootDir, 'testcase')
        for rb in (2, 1, 0):
            rc = self.rollback(self.rootDir, rb, replaceFiles=False)
            assert rc == 0

    def testMultipleBranches(self):
        # create test:runtime /localhost@rpl:linux/1.0-1-1
        # create test:runtime /localhost@rpl:linux/1.0-1-1/branch/0.5-1-1
        # update test:runtime=:branch
        # update test:runtime=:linux --keep-existing
        # create test:runtime /localhost@rpl:linux/2.0-1-1
        # update test:runtime

        self.addComponent('test:runtime', '1.0-1-1',
                                   fileContents = [( 'foo', 'contents1' )])
        self.addComponent('test:runtime',
                                 '/localhost@rpl:linux/1.0-1-1/branch/0.5-1-1',
                                   fileContents = [ ( 'bar', 'contents1' )])
        self.updatePkg(self.rootDir, 'test:runtime', version=':branch')
        self.updatePkg(self.rootDir, 'test:runtime', version='1.0-1-1',
                       keepExisting=True)
        self.addComponent('test:runtime', '2.0-1-1',
                                   fileContents = [( 'foo', 'contents2' )])
        self.updatePkg(self.rootDir, 'test:runtime')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        vlist = db.getTroveVersionList('test:runtime')
        assert(len(vlist) == 2)
        assert(sorted([ x.asString() for x in vlist]) ==
               ['/localhost@rpl:linux/1.0-1-1/branch/0.5-1-1',
                '/localhost@rpl:linux/2.0-1-1'])

    def testLinkages(self):
        self.addComponent('test:runtime', '1.0-1-1')
        self.addComponent('test:runtime', '2.0-1-1', filePrimer = 2)
        self.addComponent('test:runtime', '3.0-1-1', filePrimer = 3)
        self.addCollection("test", "1.0-1-1", ["test:runtime"])
        self.addComponent('test:runtime', 
                                   '/localhost@rpl:test2/1.0-1-1')
        self.addCollection("test", "2.0-1-1", ["test:runtime"])

        # update to different branch, link should be broken
        self.updatePkg(self.rootDir, "test", version = '1.0')
        self.updatePkg(self.rootDir, "test:runtime", 
                       version = "localhost@rpl:test2")

        db = database.Database(self.rootDir, self.cfg.dbPath)
        trv = db.getTrove(pristine=False, *db.trovesByName("test")[0])
        assert([ x for x in trv.iterTroveList(strongRefs=True) ] == [] )

        # return to the current branch, link should be restored
        self.updatePkg(self.rootDir, "test:runtime", 
                       version = "/localhost@rpl:linux/2.0-1-1")
        trv = db.getTrove(pristine=False, *db.trovesByName("test")[0])
        assert([ x[1].asString() \
                        for x in trv.iterTroveList(strongRefs=True) ] == 
                [ "/localhost@rpl:linux/2.0-1-1" ])

        self.resetRoot()
        # install component and then install the collection, the link should be
        # created
        self.updatePkg(self.rootDir, "test:runtime", version = '2.0')
        self.updatePkg(self.rootDir, "test", version = '1.0', recurse = False)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        trv = db.getTrove(pristine=False, *db.trovesByName("test")[0])
        assert([ x[1].asString() \
                    for x in trv.iterTroveList(strongRefs=True) ] == 
                                        [ "/localhost@rpl:linux/2.0-1-1" ])

        self.resetRoot()
        # install collection and then upgrade the component, the link should
        # be kept
        self.updatePkg(self.rootDir, "test", '1.0')
        self.updatePkg(self.rootDir, "test:runtime", '2.0')

        db = database.Database(self.rootDir, self.cfg.dbPath)
        trv = db.getTrove(pristine=False, *db.trovesByName("test")[0])
        assert([ x[1].asString() for x in trv.iterTroveList(strongRefs=True) ]\
                    == [ "/localhost@rpl:linux/2.0-1-1" ])

        self.resetRoot()
        # install collection with no recurse and then install the component
        # the link should be added
        self.updatePkg(self.rootDir, "test", '1.0', recurse=False)
        self.updatePkg(self.rootDir, "test:runtime", '2.0')

        db = database.Database(self.rootDir, self.cfg.dbPath)
        trv = db.getTrove(pristine=False, *db.trovesByName("test")[0])
        assert([ x[1].asString() for x in trv.iterTroveList(strongRefs=True) ] \
                    == [ "/localhost@rpl:linux/2.0-1-1" ])



        # don't let things get overlinked
        self.resetRoot()
        self.updatePkg(self.rootDir, "test:runtime", '1.0')
        self.updatePkg(self.rootDir, "test:runtime", '2.0', keepExisting = True)
        self.updatePkg(self.rootDir, "test", '1.0')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        trv = db.getTrove(pristine=False, *db.trovesByName("test")[0])
        assert([ x[1].asString() for x in trv.iterTroveList(strongRefs=True) ]                  == [ "/localhost@rpl:linux/1.0-1-1" ])

        self.resetRoot()
        self.updatePkg(self.rootDir, "test", '1.0')
        self.updatePkg(self.rootDir, "test:runtime", '2.0')
        self.updatePkg(self.rootDir, "test:runtime", '1.0', keepExisting = True)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        trv = db.getTrove(pristine=False, *db.trovesByName("test")[0])
        assert([ x[1].asString() for x in trv.iterTroveList(strongRefs=True) ] \
                                        == [ "/localhost@rpl:linux/1.0-1-1" ])

        self.resetRoot()
        self.updatePkg(self.rootDir, "test:runtime", '2.0')
        self.updatePkg(self.rootDir, "test:runtime", '3.0', keepExisting=True)
        self.updatePkg(self.rootDir, "test", '1.0', recurse=False)
        db = database.Database(self.rootDir, self.cfg.dbPath)
        trv = db.getTrove(pristine=False, *db.trovesByName("test")[0])
        assert([ x[1].asString() for x in trv.iterTroveList(strongRefs=True) ] \
                    == [ "/localhost@rpl:linux/3.0-1-1" ])

        self.resetRoot()
        # suppose we have two collections available to link in to...
        # both collections will link in this one trove, but then
        # when test:runtime v2 is installed, test v2 will link to it
        self.updatePkg(self.rootDir, "test", '1.0', recurse=False)
        self.updatePkg(self.rootDir, "test", '2.0', keepExisting=True, 
                                                    recurse=False)
        self.updatePkg(self.rootDir, "test:runtime", '1.0')

        # FIXME: should we be updating to this link?
        self.updatePkg(self.rootDir, "test:runtime", '3.0', keepExisting=True)

        db = database.Database(self.rootDir, self.cfg.dbPath)
        trv1 = db.getTrove(pristine=False, *db.findTrove(None, ("test", '1.0', None))[0])
        trv2 = db.getTrove(pristine=False, *db.findTrove(None, ("test", '2.0', None))[0])
        assert([ x[1].asString() for x in trv1.iterTroveList(strongRefs=True) ]
                == [ "/localhost@rpl:linux/1.0-1-1" ])
        assert([ x[1].asString() for x in trv2.iterTroveList(strongRefs=True) ] 
                == [ "/localhost@rpl:linux/1.0-1-1" ])
        self.updatePkg(self.rootDir, "test:runtime", '2.0', keepExisting=True)
        trv1 = db.getTrove(pristine=False, *db.findTrove(None, ("test", '1.0', None))[0])
        trv2 = db.getTrove(pristine=False, *db.findTrove(None, ("test", '2.0', None))[0])
        assert([ x[1].asString() for x in trv1.iterTroveList(strongRefs=True) ]
                == [ "/localhost@rpl:linux/1.0-1-1" ])
        assert([ x[1].asString() for x in trv2.iterTroveList(strongRefs=True) ]
                == [ "/localhost@rpl:linux/2.0-1-1" ])


    def testLinkages2(self):
        # in this case, a link is added between something that already
        # had a link (1.0[~a,~b]) and the new update.  1.0[~a,~b]
        # was a better match for what was installed before, but now 
        # it's the best match for the new 3.0[~a,~b] trove.
        self.addComponent('foo:runtime', '1.0', '~a,~b', filePrimer=1)
        self.addComponent('foo:runtime', '1.0', '~!a', filePrimer=1)

        self.addComponent('foo:runtime', '2.0', '~a', filePrimer=1)
        self.addComponent('foo:runtime', '3.0', '~a,~b', filePrimer=2)

        self.addCollection('group-dist', '1.0',
                            [('foo:runtime', '1.0', '~a,~b'),
                             ('foo:runtime', '1.0', '~!a')])

        self.updatePkg('group-dist', '1.0', recurse=False)
        self.updatePkg('foo:runtime=2.0[~a]')

        self.updatePkg('foo:runtime[~a,~b]')


    def testDuplicateFileVersions(self):
        # make sure flipping components between flavors where a file version
        # stays the same but the file contents change works
        self.addComponent('test:runtime', '1.0-1-1', 
                                   flavor = 'use:foo', 
                                   fileContents = [ ('/foo', 'contents1') ])
        self.addComponent('test:runtime', '1.0-1-1', 
                                   flavor = 'use:bar', 
                                   fileContents = [ ('/foo', 'contents2') ])

        self.updatePkg(self.rootDir, 'test:runtime', flavor = 'use:foo', raiseError=True)
        self.updatePkg(self.rootDir, 'test:runtime', flavor = 'use:bar,!foo', 
                       raiseError=True)
        self.verifyFile(self.rootDir + '/foo', 'contents2')
        self.updatePkg(self.rootDir, 'test:runtime', flavor = 'use:foo', raiseError=True)
        self.verifyFile(self.rootDir + '/foo', 'contents1')

    def testLog(self):
        # *basic* test of logging
        self.addComponent('foo:lib', '1.0-1-1')
        self.updatePkg(self.rootDir, 'foo:lib')
        self.addComponent('foo:lib', '2.0-1-1')
        self.updatePkg(self.rootDir, 'foo:lib')
        self.erasePkg(self.rootDir, 'foo:lib')
        self.checkConaryLog(
                "update foo:lib\n"
                "installed foo:lib=/localhost@rpl:linux/1.0-1-1[]\n"
                "command complete\n"
                "update foo:lib\n"
                "updated foo:lib=/localhost@rpl:linux/1.0-1-1[]--"
                                "/localhost@rpl:linux/2.0-1-1[]\n"
                "command complete\n"
                "erase foo:lib\n"
                "removed foo:lib=/localhost@rpl:linux/2.0-1-1[]\n"
                "command complete\n")

    def testTestMode(self):
        # test mode had problems with multiple jobs
        self.addComponent('foo:runtime', '1.0-1-1')
        self.addComponent('bar:lib', '1.0-1-1', filePrimer = 1)
        oldThreshold = self.cfg.updateThreshold
        self.cfg.updateThreshold = 1

        self.updatePkg(self.rootDir, [ 'foo:runtime', 'bar:lib' ], test = True)
        self.updatePkg(self.rootDir, [ 'foo:runtime', 'bar:lib' ])
        self.erasePkg(self.rootDir, [ 'foo:runtime', 'bar:lib' ], test = True)

        self.cfg.updateThreshold = oldThreshold

    def testConfigFileMovesComponents(self):
        self.addComponent('foo:runtime', '1.0-1-1',
                                   fileContents=[ ('/etc/foo', 'top\n') ])
        self.updatePkg(self.rootDir, ['foo:runtime'])
        self.writeFile(self.rootDir + '/etc/foo', 'hello world!\n')
        self.addComponent('foo:runtime', '2.0-1-1', filePrimer=2)
        self.addComponent('foo:config', '2.0-1-1',
                       fileContents=[ ('/etc/foo', 'top\n') ])
        self.updatePkg(self.rootDir, ['foo:runtime', 'foo:config'])
        self.verifyFile(self.rootDir + '/etc/foo', 'hello world!\n')

        # now check that we do diff/merge on config files
        self.resetRoot()
        self.addComponent('foo:config', '3.0-1-1',
                                   fileContents=[ ('/etc/foo', 'top\nnext\n') ])
        self.updatePkg(self.rootDir, ['foo:runtime=1.0'])
        self.writeFile(self.rootDir + '/etc/foo', 'top\nhello world!\n')
        self.updatePkg(['foo:runtime', 'foo:config'])
        self.verifyFile(self.rootDir + '/etc/foo', 'top\nnext\nhello world!\n')

    def testIncompleteTroves(self):
        self.logFilter.add()
        db = self.openDatabase()
        t1 = self.addComponent('foo:runtime', '1.0')
        t2 = self.addComponent('foo:runtime', '2.0')
        t3 = self.addComponent('foo:runtime', '3.0')
        self.setTroveVersion(1)
        self.updatePkg('foo:runtime=1.0')
        assert(db.troveIsIncomplete(t1.getName(), t1.getVersion(), t1.getFlavor()))
        self.setTroveVersion(5)
        self.updatePkg('foo:runtime=2.0')
        assert(db.troveIsIncomplete(t2.getName(), t2.getVersion(), t2.getFlavor()))
        self.setTroveVersion(12)
        self.updatePkg('foo:runtime=3.0')
        assert(not db.troveIsIncomplete(t3.getName(), t3.getVersion(), t3.getFlavor()))
        self.restoreTroveVersion()
        self.logFilter.remove()

    def testRelativeInstallIncompleteLocalCook(self):
        self.logFilter.add()
        self.addComponent('foo:runtime', '1.0')
        self.addCollection('foo', '1.0', [':runtime'])

        os.chdir(self.workDir)
        self.newpkg('foo')
        os.chdir('foo')
        self.writeFile('foo.recipe',
"""
class FooRecipe(PackageRecipe):
    name = 'foo'
    version = '1.0'

    clearBuildReqs()
    def setup(r):
        r.Create('/foo')
""")
        repos = self.openRepository()

        self.setTroveVersion(1)
        self.updatePkg('foo')
        built = self.cookItem(repos, self.cfg, 'foo.recipe')
        self.checkUpdate('foo=local@local:COOK', 
                 ['foo=1.0--1.0', 'foo:runtime=1.0--1.0'], 
                 fromChangesets=[changeset.ChangeSetFromFile('foo-1.0.ccs')], 
                 apply=True)
        self.restoreTroveVersion()

    def testRelativeInstallIncompleteChangeset(self):
        self.logFilter.add()
        repos = self.openRepository()
        self.addComponent('foo:runtime', '1.0')

        self.setTroveVersion(1)

        os.chdir(self.workDir)
        self.changeset(repos, ['foo:runtime'], 'foo.ccs')
        self.updatePkg('foo.ccs')

        self.restoreTroveVersion()


    def testRelativeUpdateIncomplete(self):
        self.logFilter.add()
        repos = self.openRepository()
        trv1 = self.addComponent('foo:runtime', '1')
        self.addComponent('foo:runtime', '2')

        self.setTroveVersion(1)
        self.updatePkg('foo:runtime=1')

        self.restoreTroveVersion()
        db = self.openDatabase()
        assert(db.troveIsIncomplete(*trv1.getNameVersionFlavor()))
        self.updatePkg('foo:runtime=1--2')

    def testIncompleteWithRemovedFiles(self):
        self.logFilter.add()

        repos = self.openRepository()
        db = self.openDatabase()

        trv1 = self.addComponent('foo:runtime', '1.0', '', [('/usr/bin/foo', 'orig')])
        trv2 = self.addComponent('foo:runtime', '2.0', '', 
                                [('/usr/bin/foo', 'new')])

        self.setTroveVersion(1)

        self.updatePkg('foo:runtime=1.0')
        assert(db.troveIsIncomplete(trv1.getName(), trv1.getVersion(), trv1.getFlavor()))
        self.removeFile(self.rootDir, '/usr/bin/foo')
        self.restoreTroveVersion()
        self.updatePkg('foo:runtime=2.0')
        assert(not os.path.exists(os.path.join(self.rootDir, 'usr/bin/foo')))
        assert(not db.troveIsIncomplete(trv2.getName(), trv2.getVersion(), trv2.getFlavor()))

    def testCompatiblePreexistingFiles(self):
        self.addComponent("test:runtime", "1.0",
                 fileContents = [ ("/var/link", rephelp.Symlink("/foo") ),
                                  ("/var/regf", "contents\n") ] )
        shutil.rmtree(self.rootDir)
        os.mkdir(self.rootDir)
        os.mkdir(self.rootDir + "/var")
        os.symlink("/foo", self.rootDir + "/var/link")
        open(self.rootDir + "/var/regf", "w").write("contents\n")
        os.chmod(self.rootDir + "/var/regf", 0)
        self.updatePkg(self.rootDir, "test:runtime")
        assert(os.readlink(self.rootDir + "/var/link") == "/foo")
        assert(open(self.rootDir + "/var/regf").read() == "contents\n")
        assert(os.stat(self.rootDir + "/var/regf").st_mode & 0777 == 0644)

    @testhelp.context('splitting')
    def testJobSplitting(self):
        self.addComponent('test:run', '1.0')
        self.addCollection('test', '1.0', [':run'])
        self.addCollection('group-test', '1.0', ['test'])

        self.addComponent('foo:run', '1.0', filePrimer=1)
        self.addCollection('foo', '1.0', [':run'])
        self.addCollection('group-foo', '1.0', ['foo'])
        self.updatePkg(['group-test'])


        client = conaryclient.ConaryClient(self.cfg)
        updJob, suggMap = client.updateChangeSet(
                            [('group-foo', (None, None), (None, None), True),
                             ('-group-test', (None, None), (None, None), False),
                            ],
                            split=True, recurse=True)
        updJob = [[y[0] for y in x] for x in updJob.getJobs() ]
        assert(updJob == [['foo', 'foo:run'], ['group-foo'],
                          ['test:run', 'test'], ['group-test']])

    @testhelp.context('splitting')
    def testCriticalIgnoresABI(self):
        self.addComponent('foo:runtime', requires='abi: ELF32(SysV x86)')
        self.addComponent('bar:runtime', provides='abi: ELF32(SysV x86)',
                          filePrimer=1)
        self.addCollection('foo', [':runtime'])
        self.addCollection('bar', [':runtime'])

        updateInfo = conaryclient.CriticalUpdateInfo()
        # the dep on abi should not affect the ordering of the update
        # because we ignore abis
        client = self.getConaryClient()
        updateInfo.setCriticalTroveRegexps(['foo:.*'])
        csList = [(x, (None, None), (None, None), True) for x in ['foo', 'bar']]
        updJob, suggMap = client.updateChangeSet(csList,
                                                 criticalUpdateInfo=updateInfo)

        jobs = [ sorted([ x[0] for x in jobSet ]) for jobSet in 
                 updJob.getJobs()]
        self.assertEquals(jobs, [['foo:runtime'],
                          ['bar', 'bar:runtime', 'foo']])
        assert(updJob.getCriticalJobs() == [0])


    @testhelp.context('splitting')
    def testJobSplittingWithLastTroves(self):
        # corecomp needs to be updated first here.
        # but because we require it to be updated last, the whole thing
        # comes back as one job.
        req = 'trove:corecomp:runtime'
        self.addComponent('foo:runtime', '1', requires=req)
        self.addCollection('foo', '1', [':runtime'])
        self.addComponent('bar:runtime', '1', filePrimer=1, requires=req)
        self.addCollection('bar', '1', [':runtime'])
        self.addComponent('corecomp:runtime', '1', filePrimer=2)
        self.addCollection('corecomp', '1', [':runtime'])

        cfg = copy.deepcopy(self.cfg)
        cfg.updateThreshold = 2
        client = conaryclient.ConaryClient(cfg)
        csList = cmdline.parseChangeList(['foo', 'bar', 'corecomp'])
        updJob, suggMap = client.updateChangeSet(csList)
        jobs = [ sorted([ x[0] for x in jobSet ]) for jobSet in updJob.getJobs()]
        assert(jobs == [['corecomp', 'corecomp:runtime'],
                        ['bar', 'bar:runtime'],
                        ['foo', 'foo:runtime']])
        updateInfo = conaryclient.CriticalUpdateInfo()
        updateInfo.setFinalTroveRegexps(['corecomp:.*'])
        updJob, suggMap = client.updateChangeSet(csList,
                                                 criticalUpdateInfo=updateInfo)
        jobs = [ sorted([ x[0] for x in jobSet ]) for jobSet in updJob.getJobs()]
        assert(jobs == [['bar', 'bar:runtime', 'corecomp', 'corecomp:runtime',
                         'foo', 'foo:runtime']])

    @testhelp.context('splitting')
    def testJobSplittingWithOverlap(self):
        # conary should put foo:run and test:run together,
        # because they are modifying the same file.
        self.addComponent('test:run', '1.0', '', ['/tmp/foo', '/tmp/fooz', 
                                                  '/tmp/bam'])
        self.addCollection('test', '1.0', [':run'])
        self.addComponent('test:run', '2.0', '', ['/tmp/foo2', '/tmp/fooz2'])
        self.addCollection('test', '2.0', [':run'])
        self.addComponent('foo:run', '1.0', '', ['/tmp/foo', '/tmp/fooz'])
        self.addCollection('foo', '1.0', [':run'])
        self.addComponent('bam:run', '1.0', '', ['/tmp/bam'])
        self.addCollection('bam', '1.0', [':run'])
        self.addComponent('bar:run', '1.0', '', ['/tmp/bar'])
        self.addCollection('bar', '1.0', [':run'])
        self.updatePkg('test=1.0')
        cfg = copy.deepcopy(self.cfg)
        cfg.updateThreshold = 2
        client = conaryclient.ConaryClient(cfg)
        updJob, suggMap = client.updateChangeSet(
                            [('test', (None, None), (None, None), True),
                             ('foo', (None, None), (None, None), True),
                             ('bar', (None, None), (None, None), True),
                             ('bam', (None, None), (None, None), True),
                            ],
                            split=True, recurse=True)
        jobs = [[y[0] for y in x] for x in updJob.getJobs() ]
        assert(jobs == [['bar', 'bar:run'],
                        ['bam:run', 'foo:run', 'test:run'],
                        ['bam', 'foo', 'test']])

    @testhelp.context('splitting')
    def testGreedyCombining(self):
        cfg = copy.deepcopy(self.cfg)
        new = versions.NewVersion()

        def _test(threshold, troveNames, expected, criticalJobs = []):
            cfg.updateThreshold = threshold
            client = conaryclient.ConaryClient(cfg)

            job = []
            for l in troveNames:
                job.append([ (x, (None, None), (new, None), False)
                                    for x in l ])

            critical = [ x for i, x in enumerate(job) if i in criticalJobs ]

            uJob = client.newUpdateJob()
            client._combineJobs(uJob, job, critical)

            combined = [[y[0] for y in x] for x in uJob.getJobs() ]
            self.assertEqual(combined, expected)

            combinedCrit = uJob.getCriticalJobs()
            self.assertEqual(len(combinedCrit), len(criticalJobs))
            for i, j in itertools.izip(combinedCrit, criticalJobs):
                self.assertEqual(combined[i], troveNames[j])

        _test(2, [ [ "a:run" ], [ "a" ] ],
                 [ [ "a:run", "a" ] ])
        # updateThreshold won't force the splitting of packages and components
        _test(1, [ [ "a:run" ], [ "a" ] ],
                 [ [ "a:run", "a" ] ])

        _test(10, [ [ "a:run" ], [ "a" ], [ "group-main" ] ],
                  [ [ "a:run", "a" ], [ "group-main" ] ])

        _test(10, [ [ "a:run" ], [ "a" ], [ "group-main" ],
                        [ "group-other" ] ],
                  [ [ "a:run", "a" ], [ "group-main", "group-other" ] ])

        _test(10, [ [ "a:run" ], [ "a" ], [ "b" ] ],
                  [ [ "a:run", "a",  "b" ] ])

        _test(10, [ [ "a:run" ], [ "a" ], [ "info-b:user" ] ],
                [ [ "a:run", "a"], [ "info-b:user" ] ])

        _test(10, [ [ "info-a:user" ], [ "info-a" ], [ "b" ] ],
                  [ [ "info-a:user", "info-a" ], [ "b" ] ] )

        _test(10, [ [ "info-a" ], [ "info-b:user" ], [ "info-b" ] ],
                  [ [ "info-a" ], [ "info-b:user", "info-b" ] ])

        _test(2, [ [ "a:runtime", "b:runtime" ], [ "b" ] ],
                 [ [ "a:runtime", "b:runtime" ], [ "b" ] ])

        _test(3, [ [ "a:runtime", "b:runtime" ], [ "b" ] ],
                 [ [ "a:runtime", "b:runtime", "b" ] ])

        # make sure critical jobs don't get combined with anything
        _test(10, [ [ "a:runtime" ], [ "a:lib" ], [ "a" ] ],
                  [ [ "a:runtime", "a:lib", "a" ] ] )
        _test(10, [ [ "a:runtime" ], [ "a:lib" ], [ "a" ] ],
                  [ [ "a:runtime" ], [ "a:lib" ], [ "a" ] ],
                  criticalJobs = [ 1 ])
        _test(10, [ [ "a:runtime" ], [ "a:lib" ], [ "a" ] ],
                  [ [ "a:runtime" ], [ "a:lib", "a" ] ],
                  criticalJobs = [ 0 ])

    @testhelp.context('splitting')
    def testJobSplittingWithMultipleGroups(self):
        self.addComponent('foo:run', '1.0')
        self.addComponent('bar:run', '1.0')
        self.addCollection('group-foo', '1.0', ['foo:run'])
        self.addCollection('group-bar', '1.0', ['bar:run'])
        client = conaryclient.ConaryClient(self.cfg)
        updJob, suggMap = client.updateChangeSet(
                            [('group-foo', (None, None), (None, None), True),
                             ('group-bar', (None, None), (None, None), True),
                            ], split=True, recurse=True)
        jobs = [[y[0] for y in x] for x in updJob.getJobs() ]
        assert(jobs == [['bar:run', 'foo:run'], ['group-bar', 'group-foo']])

    def testEraseCritical(self):
        self.addComponent('test:run', '1.0', provides='trove:test:run(1.0)')
        self.addComponent('foo:run', '1.0', requires='trove:test:run(1.0)')
        self.addComponent('test:run', '2.0')

        self.updatePkg('test:run=1.0')
        client = conaryclient.ConaryClient(self.cfg)
        criticalUpdateInfo = updatecmd.CriticalUpdateInfo()
        criticalUpdateInfo.setCriticalTroveRegexps(['test:run'])

        updJob, suggMap = client.updateChangeSet(
                            [('test:run', ('1.0', None), (None, None), False) ],
                            split=True, recurse=True,
                            criticalUpdateInfo=criticalUpdateInfo)
        assert(updJob.getCriticalJobs() == [])
        self.updatePkg('foo:run=1.0', replaceFiles = True)
        # update test:run and erase foo:run (erase of foo:run has to be done
        # before update of test:run because it requires something new test:run
        # doesn't provide test:run(1.0)
        updJob, suggMap = client.updateChangeSet(
                            [('test:run', (None, None), (None, None), True),
                             ('foo:run', ('1.0', None), (None, None), False)],
                            split=True, recurse=True,
                            criticalUpdateInfo=criticalUpdateInfo)
        assert(updJob.getCriticalJobs() == [0])
        assert([x[0] for x in updJob.getJobs()[0]] == ['foo:run', 'test:run'])

    @testhelp.context('fileoverlap', 'rollback')
    def testOverlap(self):
        self.addComponent('foo:run', '1.0', pathIdSalt = 'foo',
                          fileContents = [ ('/file', 'foo') ])
        self.addComponent('bar:run', '1.0', pathIdSalt = 'bar',
                          fileContents = [ ('/file', 'bar') ])
        self.logCheck(self.updatePkg, ([ 'foo:run', 'bar:run' ],),
            (r'error: changeset cannot be applied:\n'
             r'applying update would cause errors:\n'
             r'%s/file conflicts with a file owned '
                 r'by (foo|bar):run=/localhost@rpl:linux/1.0-1-1\[\]' % 
                    self.rootDir), regExp = True)

        self.updatePkg('foo:run')
        db = self.openDatabase()
        assert([ x[0] for x in
            db.iterFindPathReferences('/file', justPresent = True) ] == 
                        [ 'foo:run' ])
        self.verifyFile(self.rootDir + '/file', 'foo')
        self.logCheck(self.updatePkg, ( 'bar:run', ),
            ('error: changeset cannot be applied:\n'
             'applying update would cause errors:\n'
             '%s/file conflicts with a file owned '
                'by foo:run=/localhost@rpl:linux/1.0-1-1[]' % self.rootDir))

        self.updatePkg('bar:run', replaceManagedFiles = True)
        self.verifyFile(self.rootDir + '/file', 'bar')
        assert([ x[0] for x in 
            db.iterFindPathReferences('/file', justPresent = True) ] == 
                        [ 'bar:run' ])
        self.rollback(2)
        self.verifyFile(self.rootDir + '/file', 'foo')
        assert([ x[0] for x in
            db.iterFindPathReferences('/file', justPresent = True) ] == 
                        [ 'foo:run' ])

        self.resetRoot()
        self.updatePkg('foo:run', justDatabase = True)
        self.logCheck(self.updatePkg, ( 'bar:run', ),
            ('error: changeset cannot be applied:\n'
             'applying update would cause errors:\n'
             '%s/file conflicts with a file owned '
                'by foo:run=/localhost@rpl:linux/1.0-1-1[]' % self.rootDir))
        self.updatePkg('bar:run', replaceManagedFiles = True)
        db = self.openDatabase()
        assert([ x[0] for x in 
            db.iterFindPathReferences('/file', justPresent = True) ] == 
                        [ 'bar:run' ])

        self.resetRoot()
        self.updatePkg( [ 'foo:run', 'bar:run' ], replaceManagedFiles = True)

        db = self.openDatabase()
        owner = [ x for x in 
            db.iterFindPathReferences('/file', justPresent = True) ]
        assert(len(owner) == 1)

        self.erasePkg(self.rootDir, [ 'foo:run', 'bar:run' ])
        self.rollback(self.rootDir, 1)
        assert([ x for x in
            db.iterFindPathReferences('/file', justPresent = True) ] == owner)

    @testhelp.context('fileoverlap', 'rollback')
    def testDirectoryOverlap(self):
        info = {
            'user': pwd.getpwuid(os.getuid())[0],
            'group': grp.getgrgid(os.getgid())[0],
        }

        self.addComponent('foo:run', '1.0', pathIdSalt = 'foo',
                          fileContents = [ ('/dir',
                                            rephelp.Directory(
                                                owner = info['user'],
                                                group = info['group'] ) ) ])

        self.addComponent('bar:run', '1.0', pathIdSalt = 'bar',
                          fileContents = [ ('/dir',
                                            rephelp.Directory(
                                                owner = info['user'],
                                                group = info['group'] ) ) ])

        self.updatePkg([ 'foo:run', 'bar:run' ],),
        #self.logCheck(self.updatePkg, ([ 'foo:run', 'bar:run' ],),
        #    (r'error: changeset cannot be applied:\n'
        #     r'applying update would cause errors:\n'
        #     r'%s/dir conflicts with a file owned '
        #        r'by (foo|bar):run=/localhost@rpl:linux/1.0-1-1\[\]'
        #            % self.rootDir), regExp = True)

        #self.updatePkg('foo:run')
        #self.logCheck(self.updatePkg, ( 'bar:run', ),
        #    ('error: changeset cannot be applied:\n'
        #     'applying update would cause errors:\n'
        #     '%s/dir conflicts with a file owned '
        #        'by foo:run=/localhost@rpl:linux/1.0-1-1[]' % self.rootDir))

        #self.updatePkg('bar:run', replaceManagedFiles = True)
        db = self.openDatabase()
        assert([ x[0] for x in 
            db.iterFindPathReferences('/dir', justPresent = True) ] == 
                        [ 'bar:run', 'foo:run' ])

        self.resetRoot()
        self.updatePkg('foo:run')
        #self.logCheck(self.updatePkg, ( 'bar:run', ),
        #    ('error: changeset cannot be applied:\n'
        #     'applying update would cause errors:\n'
        #     '%s/dir conflicts with a file owned '
        #        'by foo:run=/localhost@rpl:linux/1.0-1-1[]' % self.rootDir))
        self.updatePkg('bar:run')
        db = self.openDatabase()
        assert(set(x[0] for x in
            db.iterFindPathReferences('/dir', justPresent = True) ) ==
                        set([ 'foo:run',  'bar:run' ]))

        self.erasePkg(self.rootDir, [ 'foo:run', 'bar:run' ])
        self.rollback(self.rootDir, 2)
        assert(set(x[0] for x in
            db.iterFindPathReferences('/dir', justPresent = True) ) ==
                        set([ 'foo:run',  'bar:run' ]))

    @testhelp.context('fileoverlap')
    def testReplaceFilesOverlap(self):
        # this first test is a bit evil because foo11 is constructed as
        # having /bar, though the file is otherwise unchanged
        foo1 = self.addComponent('foo:runtime', '1.0',
                          fileContents = [ ('/bar', 'foo1.0'),
                                           ('/etc/foo', 'foo1.0\n') ])
        foo11 = self.addComponent('foo:runtime', '1.1',
                            fileContents = [ ('/bar', 'foo1.0'),
                                           ('/etc/foo', 'foo1.1\n') ])
        bar = self.addComponent('bar:runtime', '1.0',
                          fileContents = [ ('/bar', 'bar1.0') ])
        self.updatePkg('foo:runtime=1.0')
        self.verifyFile(util.joinPaths(self.rootDir, '/bar'), 'foo1.0')
        self.writeFile(util.joinPaths(self.rootDir, "/etc/foo"),
                       "hmm\nfoo1.0\n")
        db = self.openDatabase()
        assert(db.iterTrovesByPath('/bar') == [foo1])
        self.updatePkg([ 'foo:runtime', 'bar:runtime' ],
                       replaceManagedFiles = True,
                       replaceModifiedConfigFiles = True)
        self.verifyFile(util.joinPaths(self.rootDir, '/bar'), 'bar1.0')
        self.verifyFile(util.joinPaths(self.rootDir, '/etc/foo'), 'foo1.1\n')

        # this fails
        trvList = db.iterTrovesByPath('/etc/foo')
        assert(len(trvList) == 1)
        assert( (trvList[0].getName(), trvList[0].getVersion()) ==
                (foo11.getName(), foo11.getVersion()) )
        assert(db.iterTrovesByPath('/bar') == [bar])

        # same basic test, but /bar is exactly the same between foo:runtime
        # 1.0 and 1.2. that means the diff for the file included in the
        # changeset is None (like it really ought to be in both tests...)
        # rather than a string which means that nothing changed (like it
        # actually is in the above test case)
        foo12 = self.addComponent('foo:runtime', '1.2',
                            fileContents = [ ('/bar', 'foo1.0', '1.0'),
                                           ('/etc/foo', 'foo1.2\n') ])
        self.resetRoot()
        self.updatePkg('foo:runtime=1.0')
        self.updatePkg([ 'foo:runtime', 'bar:runtime' ],
                       replaceManagedFiles = True)
        self.verifyFile(util.joinPaths(self.rootDir, '/bar'), 'bar1.0')
        self.verifyFile(util.joinPaths(self.rootDir, '/etc/foo'), 'foo1.2\n')
        assert(db.iterTrovesByPath('/bar') == [bar])

    def testUnchangedFileOverlap(self):
        # make sure --replace-files works properly when the file doesn't change
        # between versions of the trove which the file is replaced in; make
        # sure to do it with two files to exercise the cache in
        # FilesystemJob.pathRemoved()
        foo1 = self.addComponent('foo:runtime', '1.0',
                  fileContents = [ ('/bar',
                                    rephelp.RegularFile(contents = 'foo1.0',
                                                        version = '1.0') ),
                                   ('/foo',
                                    rephelp.RegularFile(contents = 'foo1.0',
                                                        version = '1.0') ) ] )
        foo11 = self.addComponent('foo:runtime', '1.1',
                  fileContents = [ ('/bar',
                                    rephelp.RegularFile(contents = 'foo1.0',
                                                        version = '1.0') ),
                                   ('/foo',
                                    rephelp.RegularFile(contents = 'foo1.0',
                                                        version = '1.0') ) ] )
        bar1 = self.addComponent('bar:runtime', '1.0',
                  fileContents = [ ('/bar',
                                    rephelp.RegularFile(contents = 'bar1.0',
                                                        version = '1.0') ),
                                   ('/foo',
                                    rephelp.RegularFile(contents = 'bar1.0',
                                                        version = '1.0') ) ],
                  pathIdSalt = '1')

        self.updatePkg('foo:runtime=1.0')
        self.updatePkg('bar:runtime', replaceManagedFiles = True)
        self.updatePkg('foo:runtime=1.1')

    def testFileMissingWhenPathIdChanges(self):
        # make sure that we are able to perform an update when a file
        # has been removed from the local system and the pathId changed
        # for the missing file.  (CNY-869)
        self.addComponent('foo:runtime', '1.0', '',
                           [('/contents0', 'foo\n'),
                            ('/contents1', 'foo\n')])
        self.updatePkg('foo:runtime')
        os.unlink(self.rootDir + '/contents0')
        self.addComponent('foo:runtime', '1.1', pathIdSalt='a',
                       fileContents=[('/contents0', 'bar\n'),
                                     ('/contents1', 'bar\n')])
        self.logCheck(self.updatePkg, ('foo:runtime', ),
          "warning: /contents0 is missing (use remove if this is intentional)")
        assert(not os.path.exists('%s/contents0' % self.cfg.root))
        self.verifyFile('%s/contents1' % self.cfg.root, 'bar\n')

    def testFileMissingWhenPathIdChanges2(self):
        # CNY-869 - remove a path that's switching troves.
        self.addComponent('foo:runtime', '1.0', '', [('/contents0', 'foo\n'), 
                                                     ('/contents1', 'foo\n')],
                          pathIdSalt='a')

        self.addComponent('bar:runtime', '1.0', '', [('/contents2', 'foo\n')])

        self.updatePkg(['foo:runtime', 'bar:runtime'])

        os.unlink(self.rootDir + '/contents0')
        # now bar owns /contents0
        self.addComponent('bar:runtime', '1.1', '',
                          [('/contents0', 'bar\n'),
                           ('/contents1', 'bar\n'),
                           ('/contents2', 'bar\n')])
        (rc, str) = self.captureOutput(self.updatePkg,
                                       ['-foo:runtime', 'bar:runtime'])
        self.assertEquals(str,
            "warning: cannot remove /contents0: No such file or directory\n")
        assert(not os.path.exists('%s/contents0' % self.cfg.root))
        self.verifyFile('%s/contents1' % self.cfg.root, 'bar\n')
        self.verifyFile('%s/contents2' % self.cfg.root, 'bar\n')

    def testSameContentsForThreeFilesWhenFirstIsRemoved(self):
        # conary uses pointers to avoid having to store the same 
        # file contents multiple times in the changeset.  But there was
        # a bug when the file w/ the lowest pathId was manually removed.
        self.addComponent('foo:runtime', '1.0', '', [('/contents0', 'foo\n'), 
                                                     ('/contents1', 'foo\n'),
                                                     ('/contents2', 'foo\n')])
        self.updatePkg(['foo:runtime'])

        os.unlink(self.rootDir + '/contents0')
        self.addComponent('foo:runtime', '1.1', '',
                          [('/contents0', 'bar\n'),
                           ('/contents1', 'bar\n'),
                           ('/contents2', 'bar\n')])
        self.logCheck(self.updatePkg, (['foo:runtime'], ),
          "warning: /contents0 is missing (use remove if this is intentional)")
        assert(not os.path.exists('%s/contents0' % self.cfg.root))
        self.verifyFile('%s/contents1' % self.cfg.root, 'bar\n')
        self.verifyFile('%s/contents2' % self.cfg.root, 'bar\n')



    def testUserReplacedFileWithDirectoryErase(self):
        # test that file removals don't traceback if a non-directory was
        # replaced with a directory by the user
        foo = self.addComponent('foo:runtime')
        self.updatePkg('foo:runtime')
        os.unlink(self.rootDir + '/contents0')
        os.mkdir(self.rootDir + '/contents0')
        self.logFilter.add()
        self.erasePkg(self.rootDir, foo.name(), foo.version())
        self.logFilter.compare(['warning: /contents0 was changed into a'
                                ' directory - not removing'])

    def testUserReplacedFileWithDirectoryUpdate(self):
        # test that file removals don't traceback if a non-directory was
        # replaced with a directory by the user
        self.addComponent('foo:runtime', '1', '', [('/contents0', 'foo\n')])
        self.updatePkg('foo:runtime')
        os.unlink(self.rootDir + '/contents0')
        os.mkdir(self.rootDir + '/contents0')
        self.addComponent('foo:runtime', '2', '', [('/contents0', 'bar\n')])
        self.logFilter.add()
        self.updatePkg('foo:runtime')
        self.logFilter.compare(['error: changeset cannot be applied:\napplying update would cause errors:\nfile type of /contents0 changed'])

    def testCannotEraseFile(self):
        def failedUnlink(path):
            if '/contents0' in path:
                raise OSError(errno.EACCES, 'Permission denied', path)
            origUnlink(path)

        origUnlink = os.unlink
        os.unlink = failedUnlink

        try:
            foo = self.addComponent('foo:runtime', '1', '',[('/dir/contents0')])
            self.updatePkg('foo:runtime')
            # just so the erase doesn't try to erase the directory, add a file
            self.writeFile('%s/dir/foo' % self.rootDir, 'blah\n') 
            self.logFilter.add()
            try:
                self.erasePkg(self.rootDir, 'foo:runtime')
            except OSError:
                pass
            else:
                assert 0
            self.logFilter.compare([
                    'error: /dir/contents0 could not be removed: '
                        'Permission denied', 
                    'error: a critical error occured -- reverting '
                        'filesystem changes'])
        finally:
            os.unlink = origUnlink

    def testWarningCallback(self):
        # Tests that warnings are passed to a callback if one exists
        m = '/contents0 was changed into a directory - not removing'

        class TestError(Exception):
            errorIsUncatchable = True

        class TestCallback(callbacks.UpdateCallback):
            def warning(self, msg, *args, **kwargs):
                if (msg % args) != m:
                    raise TestError(msg)

        foo = self.addComponent('foo:runtime')
        self.updatePkg('foo:runtime')
        os.unlink(self.rootDir + '/contents0')
        os.mkdir(self.rootDir + '/contents0')

        cb = TestCallback()
        self.erasePkg(self.rootDir, foo.name(), foo.version(), callback=cb)

    def testUnknownUser(self):
        # CNY-1071
        repos = self.openRepository()
        self.addComponent('foo:runtime', '1', '', [('/foo/contents0', 'foo\n')])
        self.updatePkg('foo:runtime')

        uid, gid = self.findUnknownIds()

        import posix
        origLstat = os.lstat
        def myLstat(path):
            s = origLstat(path)
            if path == util.joinPaths(self.rootDir, '/foo/contents0'):
                # Convert the stat info to a tuple
                s = tuple(s)
                # Replace st_uid and st_gid
                s = s[:4] + (uid, gid) + s[6:]
                # Convert to stat_result
                s = posix.stat_result(s)
                self.assertEqual(s.st_uid, uid)
                self.assertEqual(s.st_gid, gid)
            return s

        try:
            os.lstat = myLstat
            # No errors here
            self.erasePkg(self.rootDir, 'foo:runtime', '1')

            db = database.Database(self.rootDir, self.cfg.dbPath)
            rbnum = db.rollbackStack.getList()[-1]

            rb = db.rollbackStack.getRollback(rbnum)

            # Make changeset absolute
            cs = rb.getLocalChangeset(0).makeAbsolute(repos)

            self.logFilter.add()
            ret, out = self.captureOutput(showchangeset.displayChangeSet,
                db, cs, None, self.cfg, lsl=True)
            # Verify there is a warning.
            self.logFilter.compare(
                ['warning: No primary troves in changeset, listing all troves']
            )
            # Make sure we don't display the plus sign
            self.assertEqual(out[:33], "-rw-r--r--    1 %-8s %-8s" %
                                 (uid, gid))

            # No errors here either
            self.rollback(self.rootDir, 1)
        finally:
            os.lstat = origLstat

    def testUpdateNotByDefaultFromPartialMirror(self):
        # test that updating from a repository such as a mirror that is
        # missing a byDefault=False component still updates the package
        self.addComponent('foo:lib', '1')
        self.addCollection('foo', '1', [':lib', (':debuginfo', False)])
        self.checkUpdate('foo',
                        ['foo', 'foo:lib' ])

    def testUpdateByDefaultFromPartialMirror(self):
        # test that updating from a repository such as a mirror that
        # is missing byDefault=True component returns a useful message
        self.addComponent('foo:debuginfo', '1')
        self.addCollection('foo', '1', [':lib', (':debuginfo', False)])
        try:
            self.checkUpdate('foo',
                             ['foo', 'foo:lib' ])
        except errors.MissingTrovesError, e:
            self.assertEqual(str(e), 'The following troves are missing from the repository and cannot be installed: foo:lib=/localhost@rpl:linux/1-1-1[]')
        else:
            self.fail('expected MissingTrovesError')

    def testDiskBasedUpdate(self):
        # CNY-1299

        repos = self.openRepository()
        # First, produce some changesets
        trvs = [ 
            ('corecomp', [':runtime'], []),
            ('secondcomp', [':runtime'], ['corecomp']),
            ('thirdcomp', [':runtime'], ['secondcomp']),
        ]
        csfiles = []
        for v in '1', '2', '3':
            for princtrv, complist, reqslist in trvs:
                for idx, comp in enumerate(complist):
                    reqs = [ "trove:%s(%s)" % (r, v) for r in reqslist ]
                    reqs = ', '.join(reqs)

                    fileContents = [ ( '/usr/share/%s-%s/file-%s' % 
                        (princtrv, v, comp), v), ]
                    self.addComponent('%s%s' % (princtrv, comp), v,
                                      fileContents=fileContents,
                                      provides='trove:%s(%s)' % (princtrv, v),
                                      requires=reqs,
                                      filePrimer=(10 * int(v) + idx))
                self.addCollection(princtrv, v, complist)
                csPath = os.path.join(self.workDir, "%s-%s.ccs" % 
                                        (princtrv, v))
                self.changeset(repos, '%s=%s' % (princtrv, v), csPath)
                csfiles.append(csPath)

        # Shut down repository, we should be able to update without it
        self.stopRepository()

        lazycache = util.LazyFileCache()
        changesets = [ changeset.ChangeSetFromFile(lazycache.open(f))
                       for f in csfiles ]

        trvl = ['corecomp:runtime', 'secondcomp:runtime', 'secondcomp']
        csList = cmdline.parseChangeList(f + '=1' for f in trvl)

        client = conaryclient.ConaryClient(self.cfg)
        # Make sure the client is disconnected
        client.repos = None

        updJob, suggMap = client.updateChangeSet(csList,
            fromChangesets=changesets)
        client.applyUpdate(updJob)

        ccinst = client.db.getTroveVersionList('corecomp:runtime')
        self.assertEqual(len(ccinst), 1)
        self.assertEqual(ccinst[0].asString(), '/localhost@rpl:linux/1-1-1')

        # Update to version 2
        csList = cmdline.parseChangeList(f + '=2' for f in trvl)
        resolveSource = resolve.DepResolutionByTroveList(self.cfg, client.db,
            changesets)
        updJob, suggMap = client.updateChangeSet(csList,
            fromChangesets=changesets, resolveSource=resolveSource)
        client.applyUpdate(updJob)

        ccinst = client.db.getTroveVersionList('corecomp:runtime')
        self.assertEqual(len(ccinst), 1)
        self.assertEqual(ccinst[0].asString(), '/localhost@rpl:linux/2-1-1')

        # Dep error
        csList = [ f + '=3' for f in trvl ]
        csList[0] = trvl[0] + '=1'
        csList = cmdline.parseChangeList(csList)
        self.assertRaises(conaryclient.DepResolutionFailure,
            client.updateChangeSet, csList, fromChangesets=changesets,
            resolveSource=resolveSource)

        # updateall
        # get version 3 files from filesystem
        # we filter out everything but -3.css because otherwise you would end
        # up with version 1 and version 3 installed side by side.
        # You could have extra troves as long as they are not installed, but
        # _not_ multiple versions of the same trove.
        flist = os.listdir(self.workDir)
        flist = [ os.path.join(self.workDir, x)
                  for x in flist if x.endswith('-3.ccs') ]
        lazycache = util.LazyFileCache()
        changesets = [ changeset.ChangeSetFromFile(lazycache.open(f))
                       for f in flist ]

        updateItems = client.fullUpdateItemList()
        csList = [ (x[0], (None, None), x[1:], True) for x in updateItems ]
        updJob, suggMap = client.updateChangeSet(csList,
            fromChangesets=changesets, resolveSource=resolveSource)
        client.applyUpdate(updJob)

        ccinst = client.db.getTroveVersionList('corecomp:runtime')
        self.assertEqual(len(ccinst), 1)
        self.assertEqual(ccinst[0].asString(), '/localhost@rpl:linux/3-1-1')

    def testDiskBasedGroupUpdate1(self):
        repos = self.openRepository()
        comps = [':runtime']
        packages = ['comp1', 'comp2']
        csfiles = []
        for v in range(1, 3):
            for i, package in enumerate(packages):
                for j, comp in enumerate(comps):
                    self.addComponent(package + comp, str(v),
                        filePrimer=v * (i + 1) + j)
                self.addCollection(package, str(v), comps)
                csPath = os.path.join(self.workDir, "%s-%s.ccs" % 
                                        (package, v))
                self.changeset(repos, '%s=%s' % (package, v), csPath)
                csfiles.append(csPath)

            if v == 2:
                # Include older version of the trove
                p = packages[:-1]
                p.append((packages[-1], str(v - 1)))
                self.addCollection('group-foo', str(v), p)
            else:
                self.addCollection('group-foo', str(v), packages)
            csPath = os.path.join(self.workDir, "group-foo-%s.ccs" % v)
            self.changeset(repos, 'group-foo=%s' % v, csPath)
            csfiles.append(csPath)

        client = conaryclient.ConaryClient(self.cfg)

        self.updatePkg('group-foo=1')

        dldir = os.path.join(self.workDir, "download-dir")

        itemList = [ ('group-foo', (None, None), ('2', None), True) ]
        updJob, suggMap = client.updateChangeSet(itemList)

        util.mkdirChain(dldir)
        client.downloadUpdate(updJob, dldir)

        # Shut down repository, we should be able to update without it
        self.stopRepository()

        flist = [ os.path.join(dldir, x) for x in os.listdir(dldir) ]
        lazycache = util.LazyFileCache()
        changesets = [ changeset.ChangeSetFromFile(lazycache.open(f))
                       for f in flist ]

        updJob, suggMap = client.updateChangeSet(itemList,
                fromChangesets=changesets)
        client.applyUpdate(updJob)

    def testJobInvalidationTransactionCounter(self):
        # CNY-1300
        repos = self.openRepository()
        self.addComponent('foo:run', '1.0-1-1', filePrimer=1)
        self.addComponent('foo:walk', '1.0-1-1', filePrimer=11)
        self.addCollection('foo', '1.0-1-1', [':run', ':walk'])

        self.addComponent('foo:run', '2.0-1-1', filePrimer=2)
        self.addComponent('foo:walk', '2.0-1-1', filePrimer=22)
        self.addCollection('foo', '2.0-1-1', [':run', ':walk'])

        client = conaryclient.ConaryClient(self.cfg)

        csList = [('foo', (None, None), ('1.0', None), True)]

        self.assertEqual(client.db.getTransactionCounter(), 0)
        updJob, suggMap = client.updateChangeSet(csList)
        self.assertEqual(updJob.getTransactionCounter(), 0)

        client.applyUpdate(updJob)
        self.assertEqual(client.db.getTransactionCounter(), 1)

        # Build the update job for updating to 2.0
        csList = [('foo', (None, None), ('2.0', None), True)]
        updJob, suggMap = client.updateChangeSet(csList)
        self.assertEqual(client.db.getTransactionCounter(), 1)
        self.assertEqual(updJob.getTransactionCounter(), 1)

        # But instead, remove a trove
        csList = [('-foo:walk', (None, None), (None, None), False)]
        updJob2, suggMap2 = client.updateChangeSet(csList)
        self.assertEqual(client.db.getTransactionCounter(), 1)
        self.assertEqual(updJob2.getTransactionCounter(), 1)

        client.applyUpdate(updJob2)
        self.assertEqual(client.db.getTransactionCounter(), 2)

        # We should no longer be able to apply updJob
        try:
            client.applyUpdate(updJob)
            self.fail("InternalConaryError not raised")
        except errors.InternalConaryError, e:
            self.assertEqual(str(e), "Stale update job")

        # Database state shouldn't have changed
        self.assertEqual(client.db.getTransactionCounter(), 2)

        # Simulate legacy RAA
        csList = [('foo:walk', (None, None), ('2.0', None), True)]
        updJob3, suggMap3 = client.updateChangeSet(csList)
        updJob3.transactionCounter = None
        try:
            fd, tempf = tempfile.mkstemp()
            oldstderr = sys.stderr
            stderr = sys.stderr = os.fdopen(fd, "w+")
            client.applyUpdate(updJob3)
        finally:
            sys.stderr = oldstderr
            os.unlink(tempf)
        stderr.seek(0)
        msg = "UserWarning: Update jobs without a transaction counter have been deprecated, use setTransactionCounter()\n"
        actual = stderr.read()
        self.assertTrue(msg in actual, "`%s' not in `%s'" % (msg, actual))

    def testJobFreezeThaw(self):
        # CNY-1300
        repos = self.openRepository()
        client = conaryclient.ConaryClient(self.cfg)
        updateInfo = conaryclient.CriticalUpdateInfo()
        updateInfo.setFinalTroveRegexps(['corecomp:.*'])
        trvNames = ['foo', 'bar', 'corecomp']
        for idx, name in enumerate(trvNames):
            for ver in ['1', '2']:
                self.addComponent('%s:run' % name, '%s.0-1-1' % ver,
                                  filePrimer=3*idx)
                self.addComponent('%s:walk' % name, '%s.0-1-1' % ver,
                                  filePrimer=(3*idx+1))
                self.addCollection(name, '%s.0-1-1' % ver, [':run', ':walk'])

        installs = ['foo=1.0', 'bar=1.0', 'corecomp=1.0']
        try:
            self.discardOutput(self.updatePkg, installs, raiseError=True)
        except errors.ReexecRequired, e:
            self.discardOutput(self.updatePkg, installs, raiseError=True,
                               restartInfo=e.data)

        # Make sure we got the update
        for name in trvNames:
            for trvn in [name, name + ':run', name + ':walk']:
                trv = client.db.trovesByName(trvn)[0]
                self.assertEqual(trv[1].asString(),
                                    '/localhost@rpl:linux/1.0-1-1')
        trvList = []
        for name in [ 'corecomp', 'foo' ]:
            for trvn in [name, name + ':run', name + ':walk']:
                trvList.append((trvn, None, None))
        trvs = client.repos.findTroves(self.defLabel, trvList)
        trvs = [ x[0] for x in trvs.values() ]
        troveObjs = client.repos.getTroves(trvs, withFiles = False)
        troveObjs = dict((x.getNameVersionFlavor(), x) for x in troveObjs)

        # Install some troves, remove some others
        csList = [
                  ('corecomp', (None, None), ('2.0', None), True),
                  ('foo', (None, None), ('2.0', None), True),
                  ('-bar:walk', (None, None), (None, None), False),
        ]

        updateInfo = conaryclient.CriticalUpdateInfo()
        updateInfo.setCriticalTroveRegexps(['corecomp:.*'])
        updJob, suggMap = client.updateChangeSet(csList,
                                            criticalUpdateInfo=updateInfo)
        # Manually adding some pre scripts
        jobs = [updJob.jobs[0][0], updJob.jobs[1][0]]
        trvs = [troveObjs[(x[0], ) + x[2]] for x in jobs ]
        preScripts = zip(jobs, ["echo corecomp", "echo foo"], [0, 0], [1, 1],
                         ['preupdate', 'preinstall'], trvs)
        for jb, script, oldCompatClass, newCompatClass, action, troveObj in preScripts:
            updJob.addJobPreScript(jb, script, oldCompatClass, newCompatClass,
                                   action, troveObj)

        # ... and some postrollback scripts
        postRBScripts = zip(jobs, ["echo corecomp", "echo foo"], [0, 0], [1, 1])
        expPostRBScripts = {}
        for job, script, oldCompatClass, newCompatClass in postRBScripts:
            updJob.addJobPostRollbackScript(job, script, oldCompatClass,
                newCompatClass)
            expPostRBScripts[job] = (script, oldCompatClass, newCompatClass)

        # Test freezing/thawing the update job
        frzdir = os.path.join(self.workDir, "frozen-update-job")
        util.rmtree(frzdir, ignore_errors=True)
        util.mkdirChain(frzdir)
        updJob.freeze(frzdir)
        # Make sure we have a features file
        self.assertTrue(os.path.exists(os.path.join(frzdir, 'features')))

        ud = client.newUpdateJob()
        ud.thaw(frzdir)

        self.assertEqual(ud.primaries, updJob.primaries)
        self.assertEqual(ud.jobs, updJob.jobs)
        self.assertEqual(ud.transactionCounter, updJob.transactionCounter)
        self.assertEqual(ud.updateInvalidatesRollbacks(),
                             updJob.updateInvalidatesRollbacks())
        # We need to strip out the trove, it's not returned by the iterator
        self.assertEqual(list(ud.iterJobPreScripts()),
            [ x[:-1] for x in preScripts])
        self.assertEqual(list(ud.iterJobPostRollbackScripts()),
                             postRBScripts)

        # Check the trove source
        # Length is the same (cheap test)
        self.assertEqual(len(ud.troveSource.csList),
                             len(updJob.troveSource.csList))

        # Changesets have the same troves
        def _getTrovesFromChangesetList(changesetList):
            for cs in changesetList:
                yield sorted([(t.getName(), t.getOldVersion(), t.getOldFlavor(),
                               t.getNewVersion(), t.getNewFlavor())
                                for t in cs.iterNewTroveList()])

        self.assertEqual(
            list(_getTrovesFromChangesetList(ud.troveSource.csList)),
            list(_getTrovesFromChangesetList(updJob.troveSource.csList)))

        self.assertEqual(
            sorted(ud.troveSource.idMap.values()),
            sorted(updJob.troveSource.idMap.values()))

        # troveCsMap and erasuresMap map back to changesets, so it's too hard
        # to verify the values
        self.assertEqual(sorted(ud.troveSource.troveCsMap.keys()),
                             sorted(updJob.troveSource.troveCsMap.keys()))
        self.assertEqual(sorted(ud.troveSource.erasuresMap.keys()),
                             sorted(updJob.troveSource.erasuresMap.keys()))

        self.assertEqual(ud.troveSource.providesMap,
                             updJob.troveSource.providesMap)

        self.assertEqual(updJob.getJobsChangesetList(),
                             ud.getJobsChangesetList())
        self.assertEqual(updJob.getJobsChangesetList(),
                             [])

        self.assertEqual(updJob.getItemList(), ud.getItemList())
        self.assertEqual(updJob.getKeywordArguments(),
                             ud.getKeywordArguments())

        # Test downloads
        # Reset changesets first - we wrote them to disk once
        for cs in updJob.getTroveSource().csList:
            cs.reset()

        downloadDir = os.path.join(self.workDir, "update-job-download")
        util.mkdirChain(downloadDir)
        client.downloadUpdate(updJob, downloadDir)

        self.assertEqual(len(os.listdir(downloadDir)),
                             len(updJob.getJobs()))

        # Test the rollback invalidation flag
        updJob.setInvalidateRollbacksFlag(True)

        util.rmtree(frzdir, ignore_errors=True)
        util.mkdirChain(frzdir)
        updJob.freeze(frzdir)

        ud = client.newUpdateJob()
        ud.thaw(frzdir)

        self.assertEqual(updJob.getJobsChangesetList(),
                             ud.getJobsChangesetList())
        self.assertFalse(updJob.getJobsChangesetList() == [])
        self.assertEqual(updJob.updateInvalidatesRollbacks(),
                             ud.updateInvalidatesRollbacks())

    def testJobFreezeThaw2(self):
        # CNY-1300
        # Like the above, but install from changesets instead of the
        # repository
        repos = self.openRepository()
        client = conaryclient.ConaryClient(self.cfg)
        updateInfo = conaryclient.CriticalUpdateInfo()

        installs = []
        updates = []
        updateccs = []

        ccsdir = os.path.join(self.workDir, "changesets-dir")
        util.rmtree(ccsdir, ignore_errors=True)
        util.mkdirChain(ccsdir)

        comps = [(':run', True), (':debuginfo', False)]

        # Produce troves
        trvcount = 4
        for trvnum in range(trvcount):
            name = "trove%02d" % trvnum
            for ver in range(1, 3):
                for j, (comp, byDefault) in enumerate(comps):
                    self.addComponent(name + comp, str(ver),
                        filePrimer=trvnum*ver+j)
                self.addCollection(name, str(ver), comps)
                destfile = os.path.join(ccsdir, "%s-%s.ccs" % (name, ver))

                self.changeset(repos, '%s=%s' % (name, ver), destfile)

            installs.append("%s=1" % name)
            updates.append((name, (None, None), ('2', None), True))
            updateccs.append(os.path.join(ccsdir, "%s-2.ccs" % name))

        client = conaryclient.ConaryClient(self.cfg)

        self.updatePkg(installs)
        self.assertEqual(len(list(client.db.iterAllTroveNames())),
                             2 * trvcount)

        # Cut access to upstream repo
        self.stopRepository()
        client.repos = None

        # Update only the first half
        updList = updates[:(trvcount // 2)]
        lazycache = util.LazyFileCache()
        changesets = [ changeset.ChangeSetFromFile(lazycache.open(f))
                       for f in updateccs ]

        # Need a resolveSource pointing to the changesets on the disk to solve
        # dependencies only in that set
        resolveSource = resolve.DepResolutionByTroveList(self.cfg, client.db,
            changesets)
        updJob, suggMap = client.updateChangeSet(updList,
                                                 migrate=True,
                                                 fromChangesets=changesets,
                                                 resolveSource=resolveSource)
        # Freeze update job

        frzdir = os.path.join(self.workDir, "frozen-update-job")
        util.rmtree(frzdir, ignore_errors=True)
        util.mkdirChain(frzdir)
        updJob.freeze(frzdir)

        # Thaw it
        ud = client.newUpdateJob()
        ud.thaw(frzdir)

        downloadDir = os.path.join(self.workDir, "download-dir")
        util.rmtree(downloadDir, ignore_errors=True)
        util.mkdirChain(downloadDir)

        # Download
        client.downloadUpdate(ud, downloadDir)

        # Freeze again, in a different directory (this is important since some
        # of the changeset files point to the former freeze directory)
        frzdir = os.path.join(self.workDir, "frozen-update-job-2")
        util.rmtree(frzdir, ignore_errors=True)
        util.mkdirChain(frzdir)
        ud.freeze(frzdir)

        # Thaw it again
        ud = client.newUpdateJob()
        ud.thaw(frzdir)

        client.applyUpdate(ud)

    def testJobFreezeThaw3(self):
         # Same as above, but with just one update job.
         repos = self.openRepository()
         for v in [1, 2]:
             self.addComponent('test:runtime', str(v), filePrimer=v)
             self.addCollection('test', str(v), [':runtime'])
         csList = [ ('test:runtime', (None, None), ('2', None), True) ]
         client = conaryclient.ConaryClient(self.cfg)
         # Check and freeze.
         ud, suggMap = client.updateChangeSet(csList)
         frzdir = os.path.join(self.workDir, "frozen-update-job")
         util.rmtree(frzdir, ignore_errors=True)
         util.mkdirChain(frzdir)
         ud.freeze(frzdir)
         # Thaw, download, and freeze.
         ud = client.newUpdateJob()
         ud.thaw(frzdir)
         downloadDir = os.path.join(self.workDir, "update-job-download")
         util.mkdirChain(downloadDir)
         client.downloadUpdate(ud, downloadDir)
         self.resetRepository()
         frzdir2 = os.path.join(self.workDir, "frozen-update-job2")
         util.rmtree(frzdir2, ignore_errors=True)
         util.mkdirChain(frzdir2)
         ud.freeze(frzdir2)

         # Thaw and update.
         # Re-create the client, to test that update callbacks get properly
         # initialized
         client = conaryclient.ConaryClient(self.cfg)
         ud = client.newUpdateJob()
         ud.thaw(frzdir2)
         client.repos = None
         client.applyUpdate(ud)

    def testJobFreezeThaw4(self):
        # CNY-1479
        self.addComponent('foo:lib', '1', filePrimer=1, flavor='~orange')
        client = conaryclient.ConaryClient(self.cfg)
        n,v,f = conaryclient.cmdline.parseTroveSpec('foo:lib=1[~orange]')

        updJob, suggMap = client.updateChangeSet([(n, (None, None), (v, f),
True)])
        frzdir = os.path.join(self.workDir, "frozen-update-job")
        util.rmtree(frzdir, ignore_errors=True)
        util.mkdirChain(frzdir)
        updJob.freeze(frzdir)

    def testJobFreezeThaw5(self):
        # Tests that absolute changesets containing troves that do not update
        # all packages do not hit the network (it's similar for
        # byDefault=False components too)
        # CNY-1473
        repos = self.openRepository()

        packages = [ 'trove1', 'trove2' ]
        # one of the components is byDefault=False
        comps = [(':run', True), (':debuginfo', False)]

        for i, name in enumerate(packages):
            for ver in range(1, 3):
                for j, (comp, byDefault) in enumerate(comps):
                    self.addComponent(name + comp, str(ver),
                        filePrimer=3 * i * ver + j)
                self.addCollection(name, str(ver), comps)

        # Create groups
        self.addCollection('group-foo', '1', packages)
        # One of the troves kept to the old version
        self.addCollection('group-foo', '2', packages[:-1] + [(packages[-1], '1')])

        csfile = os.path.join(self.workDir, "group-foo-2.ccs")
        self.changeset(repos, "group-foo=2", csfile)

        self.updatePkg("group-foo=1")

        frzdir = os.path.join(self.workDir, "frozen-update-job")
        util.rmtree(frzdir, ignore_errors=True)
        util.mkdirChain(frzdir)

        client = conaryclient.ConaryClient(self.cfg)

        # Disconnect
        self.stopRepository()
        client.repos = None

        changesets = [ changeset.ChangeSetFromFile(csfile) ]

        itemList = [ ('group-foo', (None, None), ('2', None), True) ]
        # This used to fail because of CNY-1473
        updJob, suggMap = client.updateChangeSet(itemList,
            fromChangesets=changesets)
        updJob.freeze(frzdir)

        ud = client.newUpdateJob()
        ud.thaw(frzdir)

        client.applyUpdateJob(ud)


    def testJobFreezeThaw6(self):
        # CNY-1521
        # Explicitly specifying the version to update to - make sure itemList
        # is properly frozen
        self.addComponent('foo:runtime', '1', filePrimer=1)
        trv = self.addComponent('foo:runtime', '2', filePrimer=2)
        self.updatePkg(['foo:runtime=1'])
        # Make sure we have no conary databases left open
        self.assertEqual(0, len([x for x in self._getOpenFiles()
                                    if x.endswith('/conarydb')]))
        # No lock files either
        self.assertEqual(0, len([x for x in self._getOpenFiles()
                                    if x.endswith('/syslock')]))

        client = conaryclient.ConaryClient(self.cfg)
        itemList = [ (trv.getName(),
                      (None, None),
                      (trv.getVersion(), trv.getFlavor()),
                      True) ]
        updJob, suggMap = client.updateChangeSet(itemList)
        frzdir = os.path.join(self.workDir, "frozen-update-job")
        util.rmtree(frzdir, ignore_errors=True)
        util.mkdirChain(frzdir)
        updJob.freeze(frzdir)

        ud = client.newUpdateJob()
        ud.thaw(frzdir)

        self.assertEqual(updJob.getItemList(), ud.getItemList())
        # we have one open conary database at this point...
        self.assertEqual(1, len([x for x in self._getOpenFiles()
                                    if x.endswith('/conarydb')]))
        # and exactly one open changeset file
        self.assertEqual(1, len([x for x in self._getOpenFiles()
                                    if x.endswith('.ccs')]))

        # When ud goes out of scope it should close the open changeset files
        del ud
        self.assertEqual(0, len([x for x in self._getOpenFiles()
                                    if x.endswith('.ccs')]))
        self.assertEqual(0, len([x for x in self._getOpenFiles()
                                    if x.endswith('/conarydb')]))

        # Reopen the database
        client.db.writeAccess()
        self.assertEqual(1, len([x for x in self._getOpenFiles()
                                    if x.endswith('/conarydb')]))

        # Log something, make sure the fd to the log is open
        log.syslog("test logger")
        self.assertEqual(1, len([x for x in self._getOpenFiles()
                                    if x.endswith('/var/log/conary')]))

        # Closing the client should close the database too
        client.close()
        self.assertEqual(0, len([x for x in self._getOpenFiles()
                                    if x.endswith('/conarydb')]))
        # ... and the log
        self.assertEqual(0, len([x for x in self._getOpenFiles()
                                    if x.endswith('/var/log/conary')]))
        # ... but make sure we can still log
        log.syslog("test logger")

    def _getOpenFiles(self):
        fddir = os.path.join('/proc', str(os.getpid()), 'fd')
        ret = []
        for fdnum in os.listdir(fddir):
            try:
                rf = os.readlink(os.path.join(fddir, fdnum))
            except OSError:
                continue
            if rf.startswith(self.tmpDir):
                ret.append(rf)
        return ret


    def testJobFreezeThaw7(self):
        # CNY-1737
        # Test intreaction between downloading, freezing and critical updates

        repos = self.openRepository()
        client = conaryclient.ConaryClient(self.cfg)
        trvNames = ['foo', 'bar', 'corecomp', 'critical2']
        for idx, name in enumerate(trvNames):
            for ver in ['1', '2']:
                self.addComponent('%s:run' % name, ver, filePrimer=3*idx)
                self.addComponent('%s:walk' % name, ver, filePrimer=(3*idx+1))
                self.addCollection(name, ver, [':run', ':walk'])

        installs = ['foo=1', 'bar=1', 'corecomp=1', 'critical2=1']
        self.discardOutput(self.updatePkg, installs, raiseError=True)

        updateInfo = conaryclient.CriticalUpdateInfo()
        updateInfo.setCriticalTroveRegexps(['corecomp.*', 'critical2:.*'])

        itemsList = [
            ('foo', (None, None), ('2', None), True),
            ('corecomp', (None, None), ('2', None), True),
            ('critical2', (None, None), ('2', None), True),
        ]

        updJob = client.newUpdateJob()
        suggMap = client.prepareUpdateJob(updJob, itemsList, migrate=True,
            criticalUpdateInfo = updateInfo)

        dldir = os.path.join(self.workDir, "download")
        util.rmtree(dldir, ignore_errors=True)
        util.mkdirChain(dldir)
        client.downloadUpdate(updJob, dldir)

        # Freeze update job
        frzdir = os.path.join(self.workDir, "frozen-update-job")
        util.rmtree(frzdir, ignore_errors=True)
        util.mkdirChain(frzdir)
        updJob.freeze(frzdir)

        # Shut down repository, we should be able to update without it
        client.disconnectRepos()
        self.stopRepository()

        # Thaw update job
        updJob = client.newUpdateJob()
        updJob.thaw(frzdir)
        self.assertTrue(updJob.getChangesetsDownloaded())

        # Apply critical updates
        restartDir = client.applyUpdateJob(updJob)
        self.assertTrue(restartDir, "No critical updates found")

        # This is where we would restart
        updJob = client.newUpdateJob()
        # Pass in migrate (for testing purposes only, it should be reset when
        # loading the restart info)
        suggMap = client.prepareUpdateJob(updJob, None, migrate=True,
            restartInfo=restartDir, criticalUpdateInfo = updateInfo)

        client.applyUpdateJob(updJob)
        currtrv = set((x[0], str(x[1]))
                      for x in client.db.iterAllTroves())
        expver = '/localhost@rpl:linux/2-1-1'
        expected = set()
        for trvname in ['foo', 'corecomp', 'critical2']:
            for comp in ['walk', 'run']:
                expected.add(("%s:%s" % (trvname, comp), expver))
            expected.add((trvname, expver))
        self.assertEqual(currtrv, expected)

    def testGroupByDefault(self):
        # CNY-1476
        def fooVer():
            return [x.asString() for x in client.db.getTroveVersionList('group-foo')]

        for v in [1, 2]:
            self.addComponent('foo:lib', str(v), filePrimer=4*v)
            groupFooContents = ['foo:lib']
            if v == 2:
                self.addComponent('bar:lib', str(v), filePrimer=4*v+1)
                self.addCollection('bar', str(v), ['bar:lib'])
                groupFooContents.append('bar')
            self.addCollection('group-foo', str(v), groupFooContents)
            self.addCollection('group-dist', str(v), [('group-foo', False)])

        client = conaryclient.ConaryClient(self.cfg)

        # Install group-dist
        self.updatePkg('group-dist=1')
        # group-foo should not be installed since it's byDefault=False
        self.assertFalse(fooVer())

        # Install group-foo
        self.updatePkg('group-foo=1')
        self.assertEquals(fooVer(), ['/localhost@rpl:linux/1-1-1'])

        # run updateall
        self.captureOutput(self.updateAll)
        self.assertEquals(fooVer(), ['/localhost@rpl:linux/2-1-1'])

        # bar should be installed
        try:
            self.assertTrue(client.db.getTroveVersionList('bar'))
        except AssertionError:
            raise testhelp.SkipTestException('CNY-1476 has to be fixed')
        else:
            raise Exception("Remove the SkipTestException")

    def testUpdateFromChangeSetAcrossJobs(self):
        # CNY-1534 - download update fails to create a usable
        # changeset here for foo:runtime - the files written out are
        # wrong.  This is because reset
        repos = self.openRepository()
        File = rephelp.RegularFile
        client = conaryclient.ConaryClient(self.cfg)

        t2 = self.addComponent('foo:data', '1',
                               [ ('/d/a', File(pathId='1')) ])
        t1 = self.addComponent('foo:runtime', '1',
                               [
                                ('/etc/b',   File(pathId='3')),
                                ('/a', File(pathId='2')) ],
                                requires='trove:foo:data')
        col = self.addCollection('foo', '1', [':runtime', ':data'])
        path = self.workDir + '/foo.ccs'

        self.changeset(repos, 'foo', path)
        cs = changeset.ChangeSetFromFile(path)

        oldThreshold = self.cfg.updateThreshold
        self.cfg.updateThreshold = 1
        try:
            updJob = client.updateChangeSet([('foo', (None, None),
                                    (col.getVersion(), col.getFlavor()), True)],
                                    fromChangesets=[cs])[0]
        finally:
            self.cfg.updateThreshold = oldThreshold
        os.mkdir(self.workDir + '/download')
        client.downloadUpdate(updJob, self.workDir + '/download')
        client.applyUpdate(updJob)

    def testUpdateallTroveAndGroup(self):
        # CNY-1584
        # Try to update an installed trove and install one from a different
        # branch through a group reference
        rev1 = versions.Revision('1:1-1-1', frozen=True)
        rev2 = versions.Revision('2:2-1-1', frozen=True)
        v11 = versions.Version([self.defLabel,
                versions.Label('localhost@rpl:devel'), rev1])
        v12 = versions.Version([self.defLabel,
                versions.Label('localhost@rpl:devel'), rev2])

        v21 = versions.Version([self.defLabel, rev1])
        v22 = versions.Version([self.defLabel, rev2])

        for (ver, primer) in [(v11, 1), (v12, 2), (v22, 2)]:
            self.addComponent('foo:run', ver, filePrimer=primer)
            self.addCollection('foo', ver, [':run'])

        for (ver, primer) in [(v21, 21), (v22, 22)]:
            self.addComponent('bar:run', ver, filePrimer=primer)
            self.addCollection('bar', ver, [':run'])

        self.addCollection('group-foo', v21, ['bar'])
        self.addCollection('group-foo', v22, ['foo', 'bar'])


        self.updatePkg(['group-foo=1', 'foo=localhost@rpl:devel/1-1-1'])
        client = conaryclient.ConaryClient(self.cfg)

        self.captureOutput(self.updateAll)

    def testLocalRollbackSetFromConfig(self):
        # CNY-1583
        self.addComponent('foo:runtime', '1')

        itemsList = [
            ('foo:runtime', (None, None), ('1', None), True),
        ]

        cfg = self.cfg
        client = conaryclient.ConaryClient(cfg)

        uJob = client.newUpdateJob()
        suggMap = client.prepareUpdateJob(uJob, itemsList)

        # Mock applyUpdate, collect its kwargs into a dict we can easily
        # examine.
        funcargs = {}
        def func(*args, **kwargs):
            funcargs.clear()
            funcargs.update(kwargs)

        self.mock(client, "_applyUpdate", func)
        client.cfg.localRollbacks = False
        client.applyUpdateJob(uJob)
        self.assertEqual(funcargs['commitFlags'].localRollbacks, False)
        self.assertEqual(funcargs['autoPinList'], [])

        client.cfg.localRollbacks = True
        client.cfg.pinTroves.append("kernel")
        client.applyUpdateJob(uJob)
        self.assertEqual(funcargs['commitFlags'].localRollbacks, True)
        self.assertEqual(funcargs['autoPinList'], ['kernel'])

        client.applyUpdateJob(uJob, localRollbacks = False,
                              autoPinList = ["a", "b"])
        self.assertEqual(funcargs['commitFlags'].localRollbacks, False)
        self.assertEqual(funcargs['autoPinList'], ['a', 'b'])

    def testOldConaryCachingProxy(self):
        self.addComponent('foo:run', '1')
        trv = self.addCollection('foo', '1', [':run'])

        callargs = [True, False, False, False]
        csspec = (trv.getName(),
                  (0, 0),
                  (trv.getVersion().asString(), trv.getFlavor().freeze()),
                  True)

        client = conaryclient.ConaryClient(self.cfg)
        srv = client.getRepos().c['localhost']
        srv.getChangeSet([csspec], *callargs)

    def testExactFlavors(self):
        self.addComponent('foo:run[~ssl]')
        self.addCollection('foo[~ssl]', [':run'])
        self.assertRaises(errors.TroveNotFound,
                    self.updatePkg, 'foo', exactFlavors=True, raiseError=True)
        self.assertRaises(errors.TroveNotFound,
                    self.updatePkg, 'foo[ssl]', exactFlavors=True, raiseError=True)
        self.updatePkg('foo[~ssl]', exactFlavors=True, raiseError=True)
        self.resetRoot()
        repos = self.openRepository()
        csPath = self.workDir + '/foo.ccs'
        self.changeset(repos, 'foo', csPath)
        self.assertRaises(errors.TroveNotFound,
                    self.updatePkg, 'foo', fromFiles=[csPath], 
                    exactFlavors=True,
                    raiseError=True)
        self.updatePkg('foo[~ssl]', fromFiles=[csPath], 
                    exactFlavors=True,
                    raiseError=True)

    def testUpdateFromGroupRelativeChangeSet(self):
        self.addComponent('foo:unchanged', '1', filePrimer=1)
        self.addComponent('foo:changed', '1', filePrimer=2)
        self.addComponent('foo:removed', '1', filePrimer=3)
        self.addComponent('foo:config', '1', 
                            [('/etc/config', 'line0\nline1\nline2\nline3\nline4\n')])
        self.addCollection('foo', '1', [':unchanged', ':changed', ':removed',
                                        ':config'])

        self.addComponent('foo:unchanged', '2', filePrimer=1)
        self.addComponent('foo:changed', '2', filePrimer=4)
        self.addComponent('foo:added', '2', filePrimer=5)
        self.addComponent('foo:config', '2', 
                             [('/etc/config', 'line0\nline1\nline2\nline5\nline6\n')])
        self.addCollection('foo', '2', [':unchanged', ':changed', ':added',
                            ':config'])
        self.addComponent('removed:runtime', '1', filePrimer=6)
        self.addCollection('removed', '1', [':runtime'])

        self.addComponent('added:runtime', '2', filePrimer=7)
        self.addCollection('added', '2', [':runtime'])
        self.addComponent('unchanged:runtime', '1')
        self.addCollection('unchanged', '1', [':runtime'])

        self.addCollection('group-foo', '1', ['foo', 'removed', ('unchanged', '1')])
        self.addCollection('group-foo', '2', ['foo', 'added', ('unchanged', '1')])

        self.updatePkg('group-foo=1')
        repos = self.openRepository()
        self.writeFile(self.cfg.root + '/etc/config',
                        'line0\nline1\nchange\nline3\nline4\n')
        self.changeset(repos, 'group-foo=1--2', self.workDir + '/foo.ccs')
        self.changeset(repos, 'group-foo=2--1', self.workDir + '/foo2.ccs')
        self.stopRepository()
        self.updatePkg('group-foo', fromFiles=[self.workDir + '/foo.ccs'])
        for fileId in (1,4,5,7):
            assert(os.path.exists(self.cfg.root + '/contents%s' % fileId))
        self.verifyFile(self.cfg.root + '/etc/config',
                        'line0\nline1\nchange\nline5\nline6\n')
        self.openRepository()
        self.updatePkg('group-foo', fromFiles=[self.workDir + '/foo2.ccs'])
        for fileId in (1,2,3,6):
            assert(os.path.exists(self.cfg.root + '/contents%s' % fileId))
        self.verifyFile(self.cfg.root + '/etc/config',
                        'line0\nline1\nchange\nline3\nline4\n')


    def testMigrateReplacesModifiedAndUnmanagedFiles(self):
        # CNY-1868, CNY-2165
        fCont1 = [ ( '/etc/changedconfig', 'Original\nSecond line\n3\n4\n5\n' ),
                   ( '/usr/share/changedfile', 'OriginalF'),
                 ]
        fCont2 = [
                   # We have to change the file in the new trove, see CNY-1800
                   ( fCont1[0][0], fCont1[0][1] + 'Sixth line\n' ),
                   ( fCont1[1][0], 'NewF'),
                   ( '/etc/managedunchanged', 'ManagedUF'),
                   ( '/etc/managedchanged', 'ManagedF'),
                   ( '/usr/share/managedunchanged', 'ManagedUF'),
                   ( '/usr/share/managedchanged', 'ManagedF'),
                 ]
        self.addComponent('test:runtime', '1.0', fileContents = fCont1)
        self.addCollection('test', '1.0', [':runtime'])
        self.addComponent('test:runtime', '2.0', fileContents = fCont2)
        self.addCollection('test', '2.0', [':runtime'])

        self.updatePkg(self.rootDir, 'test', '1.0')

        # Write on the disk. This will produce some managed and changed files,
        # as well as unmanaged (changed and unchanged) files.
        fCont2c = [
                    # To avoid warnings, change config so we can merge the
                    # change from upstream later
                    (fCont2[0][0], "3\n4\n5\n"),
                    (fCont2[1][0], "Changed file\n"),
                    fCont2[2],
                    (fCont2[3][0], "ChangedContent"),
                    fCont2[4],
                    (fCont2[5][0], "ChangedContent"),
                  ]
        for fname, fcont in fCont2c:
            open(util.joinPaths(self.rootDir, fname), "w").write(fcont)

        self.updatePkg(self.rootDir, 'test', '2.0', migrate=True)

        # Changed config should be preserved
        # Everything else should be replaced
        fres = [(fCont2c[0][0], "3\n4\n5\nSixth line\n")] + fCont2[1:]
        for fname, expcont in fres:
            f = open(util.joinPaths(self.rootDir, fname))
            self.assertEqual(f.read(), expcont)

    def testRestartAfterMigrateReplacesFiles(self):
        # CNY-2513
        self.addComponent('crit:run', '1.0')
        self.addCollection('group-test', '1.0', [('crit:run', '1.0')])

        self.addComponent('crit:run', '2.0')
        self.addComponent('foo:run', '2.0',
                          fileContents = [
                              ('/usr/share/somefile', 'Managed Content'),
                          ])
        self.addCollection('group-test', '2.0',
                           [('crit:run', '2.0'), ('foo:run', '2.0')])

        self.updatePkg('group-test=1.0')

        client = conaryclient.ConaryClient(self.cfg)
        criticalUpdateInfo = updatecmd.CriticalUpdateInfo()
        criticalUpdateInfo.setCriticalTroveRegexps(['crit:run'])

        # Create an unmanaged file which will get managed
        fPath = util.joinPaths(self.rootDir, '/usr/share/somefile')
        util.mkdirChain(os.path.dirname(fPath))
        file(fPath, "w+").write("Unmanaged Content")

        itemsList = [
            ('group-test', (None, None), ('2.0', None), True),
        ]

        updJob = client.newUpdateJob()
        client.prepareUpdateJob(updJob, itemsList, migrate=True,
            criticalUpdateInfo = criticalUpdateInfo)
        self.assertEqual(updJob.getKeywordArguments()['migrate'], True)

        assert(updJob.getCriticalJobs() != [])

        restartDir = client.applyUpdateJob(updJob)
        self.assertTrue(restartDir, "No critical updates found")

        updJob = client.newUpdateJob()
        client.prepareUpdateJob(updJob, None,
            restartInfo=restartDir, criticalUpdateInfo = criticalUpdateInfo)
        self.assertEqual(updJob.getKeywordArguments()['migrate'], True)

        client.applyUpdateJob(updJob)
        self.assertEqual(file(fPath).read(), 'Managed Content')

        # Roll back, produce some bogus XML for the job invocation
        self.rollback(self.rootDir, 1)

        criticalUpdateInfo = updatecmd.CriticalUpdateInfo()
        criticalUpdateInfo.setCriticalTroveRegexps(['crit:run'])

        updJob = client.newUpdateJob()
        client.prepareUpdateJob(updJob, itemsList, migrate=True,
            criticalUpdateInfo = criticalUpdateInfo)

        assert(updJob.getCriticalJobs() != [])

        restartDir = client.applyUpdateJob(updJob)
        self.assertTrue(restartDir, "No critical updates found")

        file(util.joinPaths(restartDir, "job-invocation"), "w+").write("<Junk")

        updJob = client.newUpdateJob()
        client.prepareUpdateJob(updJob, None,
            restartInfo=restartDir, criticalUpdateInfo = criticalUpdateInfo)
        # Job invocation unavailable, so migrate should be false (default)
        self.assertEqual(updJob.getKeywordArguments().get('migrate', False),
                             False)


    def testUpdatePathIdsPresent(self):
        # CNY-2273
        # Create components with more than 999 files, to make sure we're not
        # running over the max number of bound args
        self.addComponent('foo:run',
            fileContents = [ ( '/usr/share/dir%04d/file%04d' % (i,i), 'contents%04d\n' % i)
                             for i in range(1000) ])
        self.addComponent('bar:run',
            fileContents = [ ( '/usr/share/dir%04d/file%04d' % (i,i), 'contents%04d\n' % i)
                             for i in range(1000) ])
        self.updatePkg('foo:run')
        self.updatePkg('bar:run', replaceFiles = True)


    def testUpdateMirrorOutOfDate(self):
        self.addComponent('foo:run=1[is:x86]', filePrimer=0)
        self.addComponent('foo:run=1.1[is:x86]', filePrimer=0)
        self.addComponent('foo:run=2[is:x86]', filePrimer=1)
        self.addComponent('foo:run=:branch/3[is:x86]', filePrimer=2)
        self.updatePkg('foo:run=2[is:x86]')
        self.updatePkg('foo:run=:branch[is:x86]', keepExisting=True)
        self.updatePkg('foo:run=1[is:x86]', keepExisting=True)
        self.markRemoved('foo:run=2[is:x86]')
        try:
            self.updatePkg('foo:run', raiseError=True)
            assert(0)
        except update.DowngradeError, e:
            assert(str(e) == '''\
Updating would install older versions of the following packages.  This means that the installed version on the system is not available in the repository.  To override, specify the version explicitly.

foo:run
    Available versions
        localhost@rpl:linux/1.1-1-1[is: x86]
    Installed versions
        localhost@rpl:linux/1-1-1[is: x86]
        localhost@rpl:linux/2-1-1[is: x86]
''')
        self.updatePkg('foo:run=1.1', raiseError=True)

    def testDirectoryReplacesFile(self):
        raise testhelp.SkipTestException('CNY-2434')
        self.addComponent('test:config', '1.0', fileContents = [( '/etc/foo/stuff', '')])
        self.addCollection('test', '1.0', [':config'])
        util.mkdirChain(self.rootDir + '/etc')
        self.writeFile(self.rootDir + '/etc/foo', '')
        self.updatePkg(self.rootDir, 'test', '1.0')

    def testRepositoryUpdateOnRepoReset(self):
        self.addComponent('test:runtime=1.0')
        self.updatePkg('test:runtime')
        self.stopRepository()
        self.resetRepository()
        self.addComponent('test:runtime=2.0')
        self.updatePkg('test:runtime')

    def testUpdateFromChangesetChangeFlavor(self):
        # ensures that when using changesets, we can drop a flavor 
        # out of the package.  (CNY-3101)
        self.addComponent('test:runtime=1.0[qt]')
        self.addComponent('test:runtime=2.0')
        self.updatePkg('test:runtime=1.0[qt]')
        os.chdir(self.workDir)
        self.changeset(self.openRepository(), 'test:runtime=2.0', 'test.ccs')
        client = conaryclient.ConaryClient(self.cfg)
        client.disconnectRepos()
        updJob = client.newUpdateJob()
        client.prepareUpdateJob(
            updJob, [('test:runtime', (None, None), ('2.0', None), True)], 
            fromChangesets=[changeset.ChangeSetFromFile('test.ccs')],
            migrate=True)

    @testhelp.context('rollback')
    def testUpdateWithRename(self):
        def _test():
            inode = os.stat(self.rootDir + '/foo').st_ino
            os.chmod(self.rootDir + '/foo', 0755)
            self.updatePkg('test:runtime=2-2-2')
            assert(os.stat(self.rootDir + '/bar').st_ino == inode)
            self.rollback(self.rootDir, 1)
            assert(os.stat(self.rootDir + '/foo').st_ino == inode)
            assert(not os.path.exists(self.rootDir + '/bar'))

        self.addComponent('test:runtime=1-1-1',
              fileContents = [ ('/foo', rephelp.RegularFile(pathId = 'a',
                                                            contents = '1',
                                                            version = '1-1-1'))])
        self.addComponent('test:runtime=2-2-2',
              fileContents = [ ('/bar', rephelp.RegularFile(pathId = 'a',
                                                            contents = '1',
                                                            version = '1-1-1'))])

        self.updatePkg('test:runtime=1-1-1')

        try:
            self.cfg.localRollbacks = True
            _test()
        finally:
            self.cfg.localRollbacks = False

    @testhelp.context('rollback', 'fileoverlap')
    def testLocalPermissionChangesOnRollbackAcrossComponents(self):
        self.addComponent('foo:runtime=1-1-1',
              fileContents = [ ('/a',
                    rephelp.RegularFile(contents = '1',
                                        mode = 0600)) ])
        self.addComponent('bar:runtime=1-1-1',
              fileContents = [ ('/a',
                    rephelp.RegularFile(contents = '2',
                                        mode = 0640)) ])
        self.updatePkg('foo:runtime')
        os.chmod(self.rootDir + '/a', 0644)
        self.updatePkg('bar:runtime', replaceFiles = True)
        assert(os.lstat(self.rootDir + '/a').st_mode & 0777 == 0640)
        self.rollback(1)
        assert(os.lstat(self.rootDir + '/a').st_mode & 0777 == 0644)

    @testhelp.context('rollback')
    def testRemovedAndModifiedSharedFileId(self):
        # CNY-3226
        foo1 = self.addComponent('foo:runtime=1-1-1',
              fileContents = [ ('/a', rephelp.RegularFile(contents = '1')),
                               ('/b', rephelp.RegularFile(contents = '1')) ] )
        foo2 = self.addComponent('foo:runtime=2-2-2',
              fileContents = [ ('/a', rephelp.RegularFile(contents = '2')),
                               ('/b', rephelp.RegularFile(contents = '2')) ] )

        self.updatePkg('foo:runtime=1')
        os.unlink(self.rootDir + '/a')
        self.writeFile(self.rootDir + '/b', 'new')
        rc, str = self.captureOutput(self.updatePkg,
                                     'foo:runtime=2', replaceFiles = True)
        assert(str ==
                'warning: /a is missing (use remove if this is intentional)\n')
        assert(not os.path.exists(self.rootDir + '/a'))
        self.verifyFile(self.rootDir + '/b', '2')
        self.rollback(1)
        assert(not os.path.exists(self.rootDir + '/a'))
        self.verifyFile(self.rootDir + '/b', 'new')
        self.rollback(0)
        assert(not os.path.exists(self.rootDir + '/b'))

    @testhelp.context('rollback', 'fileoverlap')
    def testReplaceFilesNotOnDisk(self):
        foo = self.addComponent('foo:runtime=1-1-1',
              fileContents = [ ('/a',
                    rephelp.RegularFile(contents = '1') ) ] )
        bar = self.addComponent('bar:runtime=1-1-1',
              fileContents = [ ('/a',
                    rephelp.RegularFile(contents = '2') ) ] )

        self.updatePkg('foo:runtime')
        os.unlink(self.rootDir + '/a')
        self.updatePkg('bar:runtime', replaceFiles = True)

        db = self.openDatabase()
        self.assertEqual(db.iterTrovesByPath('/a'), [bar])
        self.rollback(1)
        self.assertEqual(db.iterTrovesByPath('/a'), [foo])

    def testSwapFileAndSymlink(self):
        # conary <= 2.0.50 hits "OSError: Too many levels of symbolic links"
        # in this test.  Note that filenames are relevant, as they
        # determine pathId, and therefore ordering in the changeset (CNY-3251)
        self.addComponent('test:lib=1.0',
                          fileContents = [
                          ('/usr/lib64/libsmbclient.so.0', 
                           rephelp.Symlink('libsmbclient.so')),
                          ('/usr/lib64/libsmbclient.so', 
                           rephelp.RegularFile(contents = 'bar\n')),
                          ])
        self.addComponent('test:lib=2.0', 
                          fileContents = [
                          ('/usr/lib64/libsmbclient.so.0', 
                           rephelp.RegularFile(contents = 'baz\n')),
                          ('/usr/lib64/libsmbclient.so', 
                           rephelp.Symlink('libsmbclient.so.0')),
                          ] )
        self.updatePkg("test:lib=1.0")
        self.updatePkg("test:lib=2.0")

    @testhelp.context('rollback', 'fileoverlap')
    def testSharedDirs(self):
        self.addComponent('foo:run', fileContents = [
                ( '/dir', rephelp.Directory() ) ] )
        self.addComponent('bar:run', fileContents = [
                ( '/dir', rephelp.Directory() ) ] )

        self.updatePkg('foo:run')
        self.updatePkg('bar:run')
        self.erasePkg(self.rootDir, 'foo:run')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert([ x[0] for x in 
            db.iterFindPathReferences('/dir', justPresent = True) ] ==
                        [ 'bar:run' ])
        self.rollback(2)
        assert(sorted([ x[0] for x in 
            db.iterFindPathReferences('/dir', justPresent = True) ]) ==
                        [ 'bar:run', 'foo:run' ])

    @testhelp.context('rollback')
    def testSharedLinkFileTypeChange(self):
        self.addComponent('foo:runtime=1', fileContents = [
                ('/foo', rephelp.Symlink(target = '/linktarget')) ] )
        self.addComponent('foo:runtime=2', fileContents = [
                ('/foo', rephelp.RegularFile(contents = 'foo')) ] )

        # try both directions, and try both local and repository rollbacks
        for localRollbacks in True, False:
            self.cfg.localRollbacks = localRollbacks

            self.resetRoot()
            self.updatePkg('foo:runtime=1')
            self.updatePkg('foo:runtime=2')
            self.rollback(1)
            self.assertEquals(os.readlink(self.rootDir + '/foo'), '/linktarget')

            self.resetRoot()
            self.updatePkg('foo:runtime=2')
            self.updatePkg('foo:runtime=1')
            self.rollback(1)
            self.verifyFile(self.rootDir + '/foo', 'foo')

    def testReplacedBySymlink(self):
        """
        Same ids but on a different label, and was replaced by a symlink on-disk
        @tests: CNY-3877
        """
        self.addComponent('foo:runtime=1', fileContents = [
                ('/foo', rephelp.RegularFile(contents = 'foo')),
                ('/bar', rephelp.RegularFile(contents = 'foo')),
                ('/baz', rephelp.RegularFile(contents = 'foo')),
                ('/beef', rephelp.Socket()),
                ] )
        self.addComponent('foo:runtime=:other-label/1', fileContents = [
                ('/foo', rephelp.RegularFile(contents = 'foo')), # identical
                ('/bar', rephelp.RegularFile(contents = 'foo', mode=0600)), # mode changes
                ('/baz', rephelp.RegularFile(contents = 'baz')), # content changes
                ('/beef', rephelp.Socket(mode=0600)), # not a regular file
                ] )
        self.updatePkg('foo:runtime=1')
        for name in ['/foo', '/bar', '/baz', '/beef']:
            os.unlink(self.rootDir + name)
            os.symlink('foo', self.rootDir + name)
        self.updatePkg('foo:runtime=:other-label/1', replaceModifiedFiles=True)

    def testDirectoryOrdering(self):
        self.addComponent('foo:runtime=1', fileContents =
                          [ ('/a', rephelp.RegularFile(contents = 'something',
                                                       pathId = '8') ) ] )
        self.addComponent('foo:runtime=2', fileContents =
                          [ ('/a', rephelp.Directory(pathId = '8') ),
                            ('/a/b/c', rephelp.RegularFile(contents = 'else',
                                                           pathId = '1') ) ]
                         )

        self.addComponent('foo:runtime=3', fileContents =
                          [ ('/a', rephelp.Directory(pathId = '8') ),
                            ('/a/b', rephelp.RegularFile(contents = 'else',
                                                         pathId = '1') ) ]
                         )
        self.updatePkg('foo:runtime=1')
        self.updatePkg('foo:runtime=2')

        self.resetRoot()
        self.updatePkg('foo:runtime=1')
        self.updatePkg('foo:runtime=3')

    @testhelp.context('sysmodel')
    def testModelUpdate(self):
        self.addComponent('foo:runtime', '1.0', filePrimer=1)
        self.addCollection('foo', '1.0', [ ':runtime' ])
        self.addComponent('foo:runtime', '2.0', filePrimer=2)
        self.addCollection('foo', '2.0', [ ':runtime' ])
        self.addComponent('bar:runtime', '2.0', filePrimer=3)
        self.addCollection('bar', '2.0', [ ':runtime' ])


        self.addCollection('group-dist', '1.0',
                            [('foo:runtime', '1.0')])
        self.addCollection('group-dist', '2.0',
                            [('foo:runtime', '2.0')])

        root = self.cfg.root
        util.mkdirChain(root+'/etc/conary')
        file(root+'/etc/conary/system-model', 'w').write(
            'search group-dist=localhost@rpl:linux/1.0\ninstall group-dist\n')
        model = cml.CML(self.cfg)
        modelFile = systemmodel.SystemModelFile(model)
        updatecmd.doModelUpdate(self.cfg, model, modelFile, [],
            keepExisting=True)
        self.assertEquals(file(root+'/contents1').read(), 'hello, world!\n')

        rc, txt = self.captureOutput(
            updatecmd.updateAll, self.cfg, systemModel=model,
                systemModelFile=modelFile)
        self.assertEquals(txt, '\n'.join(
            ('Applying update job 1 of 2:',
             '    Update  foo(:runtime) (1.0-1-1 -> 2.0-1-1)',
             'Applying update job 2 of 2:',
             '    Update  group-dist (1.0-1-1 -> 2.0-1-1)',
             '')))

        self.assertEquals(util.exists(root+'/contents1'), False)
        self.assertEquals(file(root+'/contents2').read(), 'hello, world!\n')
        self.assertEquals(file(root+'/etc/conary/system-model').read(),
            'search group-dist=localhost@rpl:linux/2.0-1-1\n'
            'install group-dist\n')

        updatecmd.doModelUpdate(self.cfg, model, modelFile,
            ['bar=localhost@rpl:linux'], keepExisting=False)
        self.assertEquals(util.exists(root+'/contents1'), False)
        self.assertEquals(file(root+'/contents2').read(), 'hello, world!\n')
        self.assertEquals(file(root+'/contents3').read(), 'hello, world!\n')
        self.assertEquals(file(root+'/etc/conary/system-model').read(),
            'search group-dist=localhost@rpl:linux/2.0-1-1\n'
            'install group-dist\n'
            'update bar=localhost@rpl:linux/2.0-1-1\n')

        updatecmd.doModelUpdate(self.cfg, model, modelFile,
            ['-bar=localhost@rpl:linux'], keepExisting=False)
        self.assertEquals(util.exists(root+'/contents1'), False)
        self.assertEquals(file(root+'/contents2').read(), 'hello, world!\n')
        self.assertEquals(util.exists(root+'/contents3'), False)
        self.assertEquals(file(root+'/etc/conary/system-model').read(),
            'search group-dist=localhost@rpl:linux/2.0-1-1\n'
            'install group-dist\n')

    @testhelp.context('sysmodel', 'fileoverlap')
    def testModelPathConflictsAllowed(self):
        self.addComponent('foo:runtime', '1.0', filePrimer=1)
        self.addCollection('foo', '1.0', [ ':runtime' ])
        self.addCollection('group-dist', '1.0',
                            [('foo:runtime', '1.0')])

        root = self.cfg.root
        util.mkdirChain(root+'/etc/conary')
        file(root+'/etc/conary/system-model', 'w').write(
            'install group-dist=localhost@rpl:linux/1.0\n')
        model = cml.CML(self.cfg)
        modelFile = systemmodel.SystemModelFile(model)
        updatecmd.doModelUpdate(self.cfg, model, modelFile, [],
            keepExisting=True)
        self.assertEquals(file(root+'/contents1').read(), 'hello, world!\n')

        self.addComponent('conf:runtime', '1.0', filePrimer=1)
        self.addCollection('conf', '1.0', [ ':runtime' ])
        self.addCollection('group-dist', '2.0',
                            [('foo:runtime', '1.0'),
                             ('conf:runtime', '1.0')])

        rc, txt = self.captureOutput(
            updatecmd.updateAll, self.cfg, systemModel=model,
                systemModelFile=modelFile)

        self.assertEquals(util.exists(root+'/contents1'), True)
        self.assertEquals(file(root+'/contents1').read(), 'hello, world!\n')
        self.assertEquals(file(root+'/etc/conary/system-model').read(),
            'install group-dist=localhost@rpl:linux/2.0-1-1\n')

        self.addComponent('conf:runtime', '2.0', fileContents=[
            ('/contents1', rephelp.RegularFile(contents = 'conflict\n'))])
        self.addCollection('conf', '2.0', [ ':runtime' ])
        self.addCollection('group-dist', '2.1',
                            [('foo:runtime', '1.0'),
                             ('conf:runtime', '2.0')])

        # unhandled error
        self.assertRaises(update.UpdateError, self.captureOutput,
            updatecmd.updateAll, self.cfg, systemModel=model,
                systemModelFile=modelFile)

        # OK, we'll allow it
        self.addCollection('group-dist', '2.2',
                            [('foo:runtime', '1.0'),
                             ('conf:runtime', '2.0')],
                           pathConflicts=['/contents1'])

        rc, txt = self.captureOutput(
            updatecmd.updateAll, self.cfg, systemModel=model,
                systemModelFile=modelFile)

        self.resetRoot()
        self.updatePkg('foo:runtime=1.0')
        self.updatePkg('group-dist=2.2', recurse = False)

        util.mkdirChain(root+'/etc/conary')
        file(root+'/etc/conary/system-model', 'w').write(
            'install group-dist=localhost@rpl:linux/1.0\n')

        self.addComponent('new1:runtime', '1.0', fileContents=[
            ('/new', rephelp.RegularFile(contents = 'a\n'))])
        self.addCollection('new1', '1.0', [ ':runtime' ])
        self.addComponent('new2:runtime', '1.0', fileContents=[
            ('/new', rephelp.RegularFile(contents = 'b\n'))])
        self.addCollection('new2', '1.0', [ ':runtime' ])
        self.addCollection('group-dist', '3.0',
                            [('foo:runtime', '1.0'),
                             ('conf:runtime', '2.0'),
                             ('new1:runtime', '1.0'),
                             ('new2:runtime', '1.0'),
                            ],
                           pathConflicts=['/new'])

        # unhandled error on /contents1
        e = self.assertRaises(update.UpdateError, self.captureOutput,
            updatecmd.updateAll, self.cfg, systemModel=model,
                systemModelFile=modelFile)
        assert '/contents1 ' in str(e), "failed with exception " + repr(str(e))
        assert '/new ' not in str(e), "failed with exception " + repr(str(e))

        # get rid of /contents1 conflict
        self.addCollection('group-dist', '3.1',
                            [('conf:runtime', '1.0'),
                             ('new1:runtime', '1.0'),
                             ('new2:runtime', '1.0'),
                            ],
                           pathConflicts=['/new'])

        rc, txt = self.captureOutput(
            updatecmd.updateAll, self.cfg, systemModel=model,
                systemModelFile=modelFile)

        self.assertEquals(file(root+'/etc/conary/system-model').read(),
            'install group-dist=localhost@rpl:linux/3.1-1-1\n')

        # get rid of /contents1 conflict a different way
        self.addCollection('group-dist', '3.2',
                            [('foo:runtime', '1.0'),
                             ('conf:runtime', '1.0'),
                             ('new1:runtime', '1.0'),
                             ('new2:runtime', '1.0'),
                            ],
                           pathConflicts=['/new', '/contents1'])

        rc, txt = self.captureOutput(
            updatecmd.updateAll, self.cfg, systemModel=model,
                systemModelFile=modelFile)

        self.assertEquals(file(root+'/etc/conary/system-model').read(),
            'install group-dist=localhost@rpl:linux/3.2-1-1\n')

        self.assertEquals(file(root+'/contents1').read(), 'hello, world!\n')
        self.assertEquals(file(root+'/new').read(), 'b\n')
        self.rollback(1)
        self.assertEquals(file(root+'/contents1').read(), 'hello, world!\n')
        self.assertEquals(util.exists(root+'/new'), False)

    @testhelp.context('sysmodel', 'rollback')
    def testModelUpdateFailResume(self):
        self.addComponent('foo:runtime', '1.0', filePrimer=1)
        self.addCollection('foo', '1.0', [ ':runtime' ])
        self.addComponent('foo:runtime', '2.0', filePrimer=2)
        self.addCollection('foo', '2.0', [ ':runtime' ])
        # info used to force separate jobs so that the second
        # job fails and there is a rollback on the stack to apply
        self.addComponent('info-bar:user', '2.0', filePrimer=3)
        self.addCollection('info-bar', '2.0', [ ':user' ])

        self.addCollection('group-dist', '1.0',
                            [('foo:runtime', '1.0')])
        self.addCollection('group-dist', '2.0',
                            [('foo:runtime', '2.0'),
                             ('info-bar:user', '2.0')])
        # separate all jobs so that we can differentiate partially
        # broken from completely broken
        self.cfg.updateThreshold = 1

        # Force an error during an update job
        root = self.cfg.root
        util.mkdirChain(root+'/etc/conary')
        file(root+'/etc/conary/system-model', 'w').write(
            'search group-dist=localhost@rpl:linux/1.0\ninstall group-dist\n')
        file(root+'/contents1', 'w')
        os.chmod(root+'/contents1', 0)
        model = cml.CML(self.cfg)
        modelFile = systemmodel.SystemModelFile(model)
        model.modelModified = True
        self.assertRaises(update.UpdateError, updatecmd.doModelUpdate,
            self.cfg, model, modelFile, [], keepExisting=True)
        # modified, but rolled back
        self.verifyNoFile(root+'/etc/conary/system-model.next')
        os.unlink(root+'/contents1')

        # ensure that an unrelated update attempt gets rejected
        # if a snapshot has been written
        file(root+'/etc/conary/system-model.next', 'w').write(
            'search group-dist=localhost@rpl:linux/1.0\ninstall group-dist\n')
        ic = conarycmd.InstallCommand()
        rc, txt = self.captureOutput(
            ic.runCommand, self.cfg, {}, ['conary', 'install', 'foo'])
        self.assertEquals(rc, 1)
        self.assertEquals(txt, 'error: The previous update was aborted;'
            ' resume with "conary sync" or revert with "conary rollback 1"\n')

        # now, mangle the main system model to make sure that the
        # next sync picks up where it left off:
        file(root+'/etc/conary/system-model', 'w').write(
            'search group-dist=localhost@rpl:linux/WRONG\ninstall group-dist\n')
        # create a new model with current state; this is a new command line
        model = cml.CML(self.cfg)
        modelFile = systemmodel.SystemModelFile(model)
        updatecmd.doModelUpdate(
            self.cfg, model, modelFile, [], keepExisting=True)
        # make sure that the snapshot got closed
        self.verifyFile(root+'/etc/conary/system-model',
            'search group-dist=localhost@rpl:linux/1.0\ninstall group-dist\n')
        self.verifyFile(root+'/contents1', 'hello, world!\n')
        self.assertEquals(util.exists(root+'/contents2'), False)
        self.assertEquals(util.exists(root+'/etc/conary/system-model.next'),
            False)

        # Now, blow up an updateall!
        file(root+'/contents2', 'w')
        os.chmod(root+'/contents2', 0)
        model = cml.CML(self.cfg)
        modelFile = systemmodel.SystemModelFile(model)
        _, txt = self.captureOutput(self.assertRaises, update.UpdateError,
            updatecmd.updateAll,
            self.cfg, systemModel=model, systemModelFile=modelFile)
        self.assertEquals(txt,
            'Applying update job 1 of 3:\n'
            '    Install info-bar(:user)=2.0-1-1\n'
            'Applying update job 2 of 3:\n'
            '    Update  foo(:runtime) (1.0-1-1 -> 2.0-1-1)\n')
        self.verifyFile(root+'/etc/conary/system-model.next',
            'search group-dist=localhost@rpl:linux/2.0-1-1\n'
            'install group-dist\n')

        # roll back from blowing up an updateall
        client = conaryclient.ConaryClient(self.cfg)
        rc, txt = self.captureOutput(client.applyRollback,
            '1', tagScript=None, justDatabase=False,
            noScripts=False, showInfoOnly=False, abortOnError=False)
        self.assertEquals(util.exists(root+'/etc/conary/system-model.next'),
            False)
        self.verifyFile(root+'/etc/conary/system-model',
            'search group-dist=localhost@rpl:linux/1.0\ninstall group-dist\n')

        # blow up an updateall again
        model = cml.CML(self.cfg)
        modelFile = systemmodel.SystemModelFile(model)
        _, txt = self.captureOutput(self.assertRaises, update.UpdateError,
            updatecmd.updateAll,
            self.cfg, systemModel=model, systemModelFile=modelFile)
        self.assertEquals(txt,
            'Applying update job 1 of 3:\n'
            '    Install info-bar(:user)=2.0-1-1\n'
            'Applying update job 2 of 3:\n'
            '    Update  foo(:runtime) (1.0-1-1 -> 2.0-1-1)\n')

        self.verifyFile(root+'/etc/conary/system-model.next',
            'search group-dist=localhost@rpl:linux/2.0-1-1\n'
            'install group-dist\n')

        # this time, sync forward after fixing the problem
        os.unlink(root+'/contents2')
        model = cml.CML(self.cfg)
        modelFile = systemmodel.SystemModelFile(model)
        updatecmd.doModelUpdate(
            self.cfg, model, modelFile, [], keepExisting=True)
        self.verifyFile(root+'/etc/conary/system-model',
            'search group-dist=localhost@rpl:linux/2.0-1-1\n'
            'install group-dist\n')
        self.verifyFile(root+'/contents2', 'hello, world!\n')
        self.assertEquals(util.exists(root+'/etc/conary/system-model.next'),
            False)
