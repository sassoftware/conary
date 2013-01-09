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
import itertools
import pwd
import grp
import re
import os
import tempfile

from conary_test import recipes
from conary_test import rephelp

from conary import conarycfg, errors
from conary.cmds import commit
from conary import trove
from conary import versions
from conary.build import use
from conary.deps import deps


class LocalCsTest(rephelp.RepositoryHelper):

    def setup(self):
        repos = self.openRepository()
        built = []
        info = {
            'user': pwd.getpwuid(os.getuid())[0],
            'uid':  pwd.getpwuid(os.getuid())[2],
            'group': grp.getgrgid(os.getgid())[0],
        }

        loader = rephelp.LoaderFromString(recipes.testRecipe1, "/path",
                                          cfg = self.cfg,
                                          repos = repos,
                                          component = 'TestRecipe1')

        built += self.cookObject(loader)

        self.build(recipes.userInfoRecipe %info, "UserMe")
        return (built, info)

    def testBasic(self):
        built, info = self.setup()
        path = self._testBasic1(built, info)
        self._testBasic2(path, info)

    def testIncomplete(self):
        raise testhelp.SkipTestException('Broken in hg rev 0efa49c1bcab')
        built, info = self.setup()
        OLD_TROVE_VERSION = trove.TROVE_VERSION
        OLD_TROVE_VERSION_1_1 = trove.TROVE_VERSION_1_1
        d = {}
        if use.Arch.x86:
            d = { 'flavor': 'is: x86' }
        elif use.Arch.x86_64:
            d = { 'flavor': 'is: x86_64' }
        else:
            raise NotImplementedError, 'edit test for this arch'
        try:
            trove.TROVE_VERSION = 1
            trove.TROVE_VERSION_1_1 = 2
            self.logFilter.add()
            try:
                path = self._testBasic1(built, info)
            except errors.ConaryError, err:
                assert(str(err) == '''\
Cannot create a local changeset using an incomplete troves:  Please ensure 
you have the latest conary and then reinstall these troves:
     testcase=/localhost@rpl:linux/1.0-1-1[%(flavor)s]
    testcase:runtime=/localhost@rpl:linux/1.0-1-1[%(flavor)s]
''' %d)
            else:
                assert(0)
        finally:
            trove.TROVE_VERSION = OLD_TROVE_VERSION
            trove.TROVE_VERSION_1_1 = OLD_TROVE_VERSION_1_1

        self.resetRoot()
        path = self._testBasic1(built, info)
        try:
            trove.TROVE_VERSION = 1
            trove.TROVE_VERSION_1_1 = 2
            self.resetRoot()
            self.updatePkg(self.rootDir, 'info-%(user)s' % info)
            self.updatePkg(self.rootDir, 'testcase')
            db = self.openDatabase()
            tups = db.findTroves(None, [('info-%(user)s' % info, None, None),
                                          ('testcase', None, None)])
            tups = list(itertools.chain(*tups.itervalues()))
            assert([ x for x in db.getTroves(tups) if not x.troveInfo.incomplete()] == [])
            try:
                self._testBasic2(path, info)
            except errors.ConaryError, err:
                assert(re.match(r'Cannot apply a relative changeset to an incomplete trove.  Please upgrade conary and/or reinstall testcase:?.*=/localhost@rpl:linux/1.0-1-1\[%(flavor)s\].' %d, str(err)))
            else:
                assert(0)
        finally:
            trove.TROVE_VERSION = OLD_TROVE_VERSION
            trove.TROVE_VERSION_1_1 = OLD_TROVE_VERSION_1_1

        os.remove(path)

    def _testBasic1(self, built, info):
        self.updatePkg(self.rootDir, 'info-%(user)s' % info)
        self.updatePkg(self.rootDir, 'testcase')

        (fd, path) = tempfile.mkstemp()
        os.close(fd)

        self.writeFile(self.rootDir + "/etc/changedconfig", 'new contents\n')
        self.writeFile(self.rootDir + "/usr/share/changed", 
                       'more contents\n')
        os.chmod(self.rootDir + "/usr/bin/hello", 0700)
        self.localChangeset(self.rootDir, 'testcase', path)

        self.resetRoot()
        self.updatePkg(self.rootDir, 'info-%(user)s' % info)
        self.updatePkg(self.rootDir, 'testcase')
        self.updatePkg(self.rootDir, path)

        self.verifyFile(self.rootDir + "/etc/changedconfig",
                        'new contents\n')
        self.verifyFile(self.rootDir + "/usr/share/changed",
                        'more contents\n')
        assert((os.stat(self.rootDir + "/usr/bin/hello").st_mode & 0700) 
                            == 0700)
        self.erasePkg(self.rootDir, 'testcase')
        return path

    def _testBasic2(self, path, info):
        commit.doCommit(self.cfg, path,
                        'localhost@commit:branch')

        self.resetRoot()
        self.updatePkg(self.rootDir, 'info-%(user)s' % info)
        self.updatePkg(self.rootDir, 'testcase', 
                       version = 'localhost@commit:branch')
        self.verifyFile(self.rootDir + "/etc/changedconfig",
                        'new contents\n')
        self.verifyFile(self.rootDir + "/usr/share/changed",
                        'more contents\n')
        assert((os.stat(self.rootDir + "/usr/bin/hello").st_mode & 0700) 
                            == 0700)
        self.erasePkg(self.rootDir, 'testcase')
        os.remove(path)

    def testShadowed(self):
        self.repos = self.openRepository()
        self.addTestPkg(1, 
                        content='''r.Create("/etc/foo", 
                                            contents="old contents\\n")''')
        self.mkbranch(self.cfg.buildLabel, "@rpl:branch", "test1:source",
                      shadow=True)
        self.cfg.buildLabel = versions.Label('localhost@rpl:branch')
        self.cfg.installLabelPath = \
                    conarycfg.CfgLabelList([self.cfg.buildLabel])
        # modify the trove on the shadow
        self.addTestPkg(1, content='''r.Create("/etc/foo", 
                                            contents="new contents\\n")''')
        self.cookTestPkg(1)
        self.updatePkg(self.rootDir, 'test1', depCheck=False)
        self.writeFile(self.rootDir + "/etc/foo", 'newer contents\\n')

        (fd, path) = tempfile.mkstemp()
        os.close(fd)
        self.localChangeset(self.rootDir, 'test1', path)
        commit.doCommit(self.cfg, path,
                        'localhost@commit:branch')
        assert(self.repos.hasTrove('test1', versions.VersionFromString(
                '/localhost@rpl:linux//branch//commit:branch/1.0-1.1-1.0.1'),
                deps.Flavor()))

        # commit it again to make sure the version gets incremented
        commit.doCommit(self.cfg, path,
                        'localhost@commit:branch')
        assert(self.repos.hasTrove('test1', versions.VersionFromString(
                '/localhost@rpl:linux//branch//commit:branch/1.0-1.1-1.0.2'),
                deps.Flavor()))

        os.remove(path)
