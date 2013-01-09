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


from conary.build import cook, policy, usergroup
from conary import files, versions
from conary_test import rephelp


class PwClass:

    def restore(self):
        files.userCache.nameLookupFn = self.real

    def getpwnam(self, user):
        if user == 'root':
            return (None, None, 0)

        f = file(self.root + '/etc/passwd')
        assert (f.readlines() == [
            'root:*:0:0:root:/root:/bin/bash\n',
            'foo:$1$XzHooEIT$hszQQcxv6tokTs46604IW1:1000:1000::/usr/share/foo:/bin/foosh\n',
        ])
        return (None, None, 1000)

    def __init__(self, root):
        self.real = files.userCache.nameLookupFn
        self.root = root
        files.userCache.nameLookupFn = self.getpwnam

class UserGroupInfoRecipeTest(rephelp.RepositoryHelper):

    def testUserInfoRecipe(self):
        recipestr1 = """
class TestUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-foo'
    version = '1'
    def setup(r):
        r.macros.foo = 'foo'
        r.macros.bar = 'bar'
        r.User('%(foo)s', 1000, group='%(bar)s', homedir='%(datadir)s/%(foo)s',
               shell='%(essentialbindir)s/foosh', 
               saltedPassword='$1$XzHooEIT$hszQQcxv6tokTs46604IW1')
"""
        self.reset()
        # do one test with logBuild because this code path is important
        # and has broken more than once
        (built, d) = self.buildRecipe(recipestr1, "TestUser", logBuild=True)
        self.assertEquals(len(built), 2)

        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        f = file(self.workDir + '/etc/conary/userinfo/foo')
        assert (f.readlines() == [
            'PREFERRED_UID=1000\n',
            'GROUP=bar\n',
            'HOMEDIR=/usr/share/foo\n',
            'SHELL=/bin/foosh\n',
            'PASSWORD=$1$XzHooEIT$hszQQcxv6tokTs46604IW1\n'])
        f.close()
        f = file(self.workDir + '/etc/passwd')
        assert (f.readlines() == [
            'root:*:0:0:root:/root:/bin/bash\n',
            'foo:$1$XzHooEIT$hszQQcxv6tokTs46604IW1:1000:1000::/usr/share/foo:/bin/foosh\n',
        ])
        f.close()
        f = file(self.workDir + '/etc/group')
        assert (f.readlines() == [
            'root:*:0:root\n',
            'bar:*:1000:\n',
        ])
        f.close()

        # test that the right dependencies are attached
        pathsFound = []
        repos = self.openRepository()
        for name, version, flavor in built:
            version = versions.VersionFromString(version)
            trove = repos.getTrove(name, version, flavor)
            for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
                    trove.getName(), trove.getVersion(), trove.getFlavor(),
                    withFiles=True):
                prov = str(fileObj.provides())
                req = str(fileObj.requires())
                pathsFound.append(path)
                if path == '/etc/conary/userinfo/foo':
                    self.failUnless(prov.find('userinfo: foo') != -1)
                    self.failUnless(req.find('groupinfo: bar') != -1)
                elif path == '/etc/conary/groupinfo/bar':
                    self.failUnless(prov.find('groupinfo: bar') != -1)
        self.failUnless('/etc/conary/userinfo/foo' in pathsFound)
        self.failUnless('/etc/conary/groupinfo/bar' in pathsFound)


        # now test installing the info-foo package along with a package
        # which requires it from a single change set, and make sure ownership
        # would have been set properly
        foorecipe = """
class FooRecipe(PackageRecipe):
    clearBuildReqs()
    name = 'foo'
    version = '1'
    def setup(r):
        r.Create('/foo', contents = "contents")
        r.Ownership('foo', 'root', '/foo')
"""
        (built, d) = self.buildRecipe(foorecipe, "FooRecipe", logBuild=True)
        csPath = self.workDir + '/test.ccs'
        self.resetRoot()
        # this makes sure that the /etc/passwd is correct before we try and
        # lookup foo in the user database. what a hack.
        self.resetRoot()
        c = PwClass(self.rootDir)
        self.mimicRoot()
        self.updatePkg(self.rootDir, 'foo', resolve=True)
        c.restore()
        self.realRoot()
        f = file(self.rootDir + '/etc/passwd')
        assert (f.readlines() == [
            'root:*:0:0:root:/root:/bin/bash\n',
            'foo:$1$XzHooEIT$hszQQcxv6tokTs46604IW1:1000:1000::/usr/share/foo:/bin/foosh\n',
        ])

        recipestr2 = """
class TestBadUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-foo'
    version = '1'
    def setup(r):
        r.User('blah', 1000, group='bar', homedir='%(datadir)s/foo',
               shell='%(essentialbindir)s/foosh')
"""
        self.assertRaises(usergroup.UserGroupError, self.buildRecipe,
                    recipestr2, "TestBadUser")

        recipestr3 = """
class TestBad2User(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-foo'
    version = '1'
    def setup(r):
        r.User('foo', 1000, group='bar', homedir='%(datadir)s/foo',
               shell='%(essentialbindir)s/foosh')
        r.User('foo', 1000, group='bar', homedir='%(datadir)s/foo',
               shell='%(essentialbindir)s/foosh')
"""
        self.assertRaises(usergroup.UserGroupError, self.buildRecipe,
                    recipestr3, "TestBad2User")



    def testUserInfoWithExistingDefaultGroup(self):
        recipestr1 = """
class TestUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.macros.foo = 'foo'
        r.macros.bar = 'bar'
        r.User('%(foo)s', 1000, group='root')
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "TestUser")
        self.updatePkg(self.workDir, 'info-foo', resolve=True)
        f = file(self.workDir + '/etc/conary/userinfo/foo')
        assert (f.readlines() == [
            'PREFERRED_UID=1000\n',
            'GROUP=root\n',
            'SHELL=/sbin/nologin\n',])
        f.close()
        f = file(self.workDir + '/etc/passwd')
        assert (f.readlines() == [
            'root:*:0:0:root:/root:/bin/bash\n',
            'foo:*:1000:0::/:/sbin/nologin\n',
        ])
        f.close()
        f = file(self.workDir + '/etc/group')
        assert (f.readlines() == [
            'root:*:0:root\n',
        ])
        f.close()



    def testUserInfoRecipeWithSupplemental(self):
        recipestr0 = """
class TestSupplementalGroupUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-bar'
    version = '1'
    def setup(r):
        r.User('bar', 999)
"""
        recipestr1 = """
class TestSupplementalGroupUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-baz'
    version = '1'
    def setup(r):
        r.User('baz', 998)
"""
        recipestr2 = """
class TestUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-foo'
    version = '1'
    def setup(r):
        r.macros.foo = 'foo'
        r.macros.bar = 'bar'
        r.macros.baz = 'baz'
        r.User('%(foo)s', 1000, groupid=998, # test group ID allocation
               supplemental=['%(bar)s', '%(baz)s'])
"""
        recipestr3 = """
class TestUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-fdsa'
    version = '1'
    def setup(r):
        r.macros.fdsa = 'fdsa'
        r.macros.bar = 'bar'
        r.macros.baz = 'baz'
        r.User('%(fdsa)s', 1000, groupid=998, # test ID allocation
               supplemental=['%(bar)s'])
"""
        self.reset()
        for recipestr in [recipestr0, recipestr1]:
            built, d = self.buildRecipe(recipestr, "TestSupplementalGroupUser")
            for p in built:
                self.updatePkg(self.workDir, p[0], p[1], resolve=True)
        for recipestr in [recipestr2, recipestr3]:
            built, d = self.buildRecipe(recipestr, "TestUser")
            for p in built:
                self.updatePkg(self.workDir, p[0], p[1], resolve=True)
        f = file(self.workDir + '/etc/conary/userinfo/foo')
        assert (f.readlines() == [
            'PREFERRED_UID=1000\n',
            'GROUPID=998\n',
            'SHELL=/sbin/nologin\n',
            'SUPPLEMENTAL=bar,baz\n',])
        f.close()
        f = file(self.workDir + '/etc/passwd')
        assert (f.readlines() == [
            'root:*:0:0:root:/root:/bin/bash\n',
            'fdsa:*:1:2::/:/sbin/nologin\n',
            'baz:*:998:998::/:/sbin/nologin\n',
            'bar:*:999:999::/:/sbin/nologin\n',
            'foo:*:1000:1::/:/sbin/nologin\n',
        ])
        f.close()
        f = file(self.workDir + '/etc/group')
        assert (f.readlines() == [
            'root:*:0:root\n',
            'foo:*:1:\n',
            'fdsa:*:2:\n',
            'baz:*:998:foo\n',
            'bar:*:999:foo,fdsa\n',
        ])
        f.close()



    def testGroupInfoRecipe(self):
        recipestr1 = """
class TestGroup(GroupInfoRecipe):
    name = 'info-foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.macros.foo = 'foo'
        r.Group('%(foo)s', 1000)
"""
        self.reset()
        (built, d) = self.buildRecipe(recipestr1, "TestGroup")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        f = file(self.workDir + '/etc/conary/groupinfo/foo')
        assert (f.readlines() == ['PREFERRED_GID=1000\n'])
        f.close()
        f = file(self.workDir + '/etc/group')
        assert (f.readlines() == [
            'root:*:0:root\n',
            'foo:*:1000:\n',
        ])
        f.close()

        recipestr2 = """
class TestBadGroup(GroupInfoRecipe):
    name = 'info-foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Group('blah', 1000)
"""
        self.assertRaises(usergroup.UserGroupError, self.buildRecipe,
                    recipestr2, "TestBadGroup")

        recipestr3 = """
class TestBad2Group(GroupInfoRecipe):
    name = 'info-foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Group('foo', 1000)
        r.SupplementalGroup('foo', 'bar', 999)
"""
        self.assertRaises(usergroup.UserGroupError, self.buildRecipe,
                    recipestr3, "TestBad2Group")

        recipestr4 = """
class TestBad2Group(GroupInfoRecipe):
    name = 'info-foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Group('foo', 1000)
        r.Group('foo', 999)
"""
        self.assertRaises(usergroup.UserGroupError, self.buildRecipe,
                    recipestr4, "TestBad2Group")


    def testSupplementalGroupInfoRecipe(self):
        recipestr0 = """
class TestSupplementalGroupUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-bar'
    version = '1'
    def setup(r):
        r.User('bar', 999)
"""
        recipestr1 = """
class TestSupplementalGroup(GroupInfoRecipe):
    clearBuildReqs()
    name = 'info-foo'
    version = '1'
    def setup(r):
        r.macros.foo = 'foo'
        r.macros.bar = 'bar'
        r.SupplementalGroup('%(bar)s', '%(foo)s', 1000)
"""
        self.reset()
        # satisfy dependency
        builtb, d = self.buildRecipe(recipestr0, "TestSupplementalGroupUser")
        for p in builtb:
            self.updatePkg(self.workDir, p[0], p[1])
        # now the group we are testing
        (built, d) = self.buildRecipe(recipestr1, "TestSupplementalGroup")
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        f = file(self.workDir + '/etc/conary/groupinfo/foo')
        assert (f.readlines() == [
            'PREFERRED_GID=1000\n',
            'USER=bar\n'])
        f.close()
        f = file(self.workDir + '/etc/group')
        assert (f.readlines() == [
            'root:*:0:root\n',
            'bar:*:999:\n',
            'foo:*:1000:bar\n',
        ])
        f.close()
        # now test if a group already exists
        self.resetWork()
        for p in builtb:
            self.updatePkg(self.workDir, p[0], p[1])
        f = file(self.workDir + '/etc/group', 'a')
        f.write('asdf:*:1000:\n')
        f.close()
        for p in built:
            self.updatePkg(self.workDir, p[0], p[1])
        f = file(self.workDir + '/etc/group')
        assert (f.readlines() == [
            'root:*:0:root\n',
            'foo:*:1:bar\n',
            'bar:*:999:\n',
            'asdf:*:1000:\n',
        ])

    def testBadPassword(self):
        recipestr1 = """
class TestBadPassword(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-foo'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.User('foo', 1000, group='bar', homedir='%(datadir)s/foo',
               saltedPassword='foo')
"""
        e = self.assertRaises(usergroup.UserGroupError,
            self.buildRecipe, recipestr1, "TestBadPassword")
        self.assertEqual(str(e),
            '"foo" is not a valid md5 salted password. Use md5pw (installed with conary) to  create a valid password.')

    def testSupplementalGroupInfoRecipeOrdering(self):
        recipestr0 = """
class TestSupplementalGroupUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-adm'
    version = '1'
    def setup(r):
        r.User('adm', 3, group='adm', groupid=4,
               supplemental=['sys'],
               homedir='%(localstatedir)s/adm')
"""
        recipestr1 = """
class TestSupplementalGroup(GroupInfoRecipe):
    clearBuildReqs()
    name = 'info-sys'
    version = '1'
    def setup(r):
        r.Group('sys', 3)
"""
        built, d = self.buildRecipe(recipestr0, "TestSupplementalGroupUser")
        built, d = self.buildRecipe(recipestr1, "TestSupplementalGroup")
        rc, str = self.captureOutput(self.updatePkg, self.workDir,
                                     'info-adm', resolve=True)
        f = file(self.workDir + '/etc/group')
        lines = f.readlines()
        assert (lines == [
            'root:*:0:root\n',
            'sys:*:3:adm\n',
            'adm:*:4:\n',
        ])
        f.close()
        f = file(self.workDir + '/etc/passwd')
        lines = f.readlines()
        assert (lines == [
            'root:*:0:0:root:/root:/bin/bash\n',
            'adm:*:3:4::/var/adm:/sbin/nologin\n'
        ])
        f.close()


    def testUserInfoRecipeWithExternalGroup(self):
        recipestr1 = """
class TestUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-foo'
    version = '1'
    def setup(r):
        r.macros.foo = 'foo'
        r.macros.bar = 'bar'
        r.User('%(foo)s', 1000, group='%(bar)s', provideGroup=False)
"""
        self.reset()
        # do one test with logBuild because this code path is important
        # and has broken
        (built, d) = self.buildRecipe(recipestr1, "TestUser", logBuild=True)


        # test that the right dependencies are attached
        repos = self.openRepository()
        (name, version, flavor) = built[0]
        version = versions.VersionFromString(version)
        trove = repos.getTrove(name, version, flavor)
        pathsFound = []
        for pathId, path, fileId, version, fileObj in repos.iterFilesInTrove(
                trove.getName(), trove.getVersion(), trove.getFlavor(),
                withFiles=True):
            req = str(fileObj.requires())
            prov = str(fileObj.provides())
            pathsFound.append(path)
            if path == '/etc/conary/userinfo/foo':
                assert prov.find('userinfo: foo') != -1, prov
                assert req.find('groupinfo: bar') != -1, req
        assert '/etc/conary/userinfo/foo' in pathsFound, pathsFound

    def testBadCommand(self):
        recipestr1 = """
class TestUser(UserInfoRecipe):
    clearBuildReqs()
    name = 'info-foo'
    version = '1'
    def setup(r):
        r.macros.foo = 'foo'
        r.macros.bar = 'bar'
        r.User('%(foo)s', 1000, group='%(bar)s', provideGroup=False)
        # ensure unifying Info Recipes with PackageRecipe didn't allow for
        # more than we intended CNY-2723
        r.Create('/etc/foo')
"""
        err = self.assertRaises(cook.CookError, self.buildRecipe,
                recipestr1, "TestUser", logBuild=True)
        self.assertFalse("AttributeError: 'TestUser' object has no " \
                "attribute 'Create'" not in str(err))

    def testUserPolicyInvocation(self):
        recipestr1 = r"""
class TestUserInfo(UserInfoRecipe):
    name = 'info-foo'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.User('foo', 11)
        # policies exist, even though they're not advertised. ensure they can't
        # do any harm
        r.PackageSpec('manpage', '.*')
"""
        built, d = self.buildRecipe(recipestr1, "TestUserInfo")
        self.assertEquals(built[0][0], 'info-foo:group')
        self.assertEquals(built[1][0], 'info-foo:user')

    def testUserPolicyInvocation2(self):
        recipestr1 = r"""
class TestUserInfo(UserInfoRecipe):
    name = 'info-foo'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.User('foo', 11)
        # policies exist, even though they're not advertised. ensure they can't
        # do any harm
        r.ComponentSpec('manpage:foo', '.*')
"""

        built, d = self.buildRecipe(recipestr1, "TestUserInfo")
        self.assertEquals(built[0][0], 'info-foo:group')
        self.assertEquals(built[1][0], 'info-foo:user')

    def testUserMissingParams(self):
        recipestr1 = r"""
class TestUserInfo(UserInfoRecipe):
    name = 'info-test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.User()
"""

        err = self.assertRaises(cook.CookError, self.buildRecipe,
                recipestr1, "TestUserInfo")

        recipestr2 = r"""
class TestUserInfo(UserInfoRecipe):
    name = 'info-test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.User('foo')
"""

        err = self.assertRaises(cook.CookError, self.buildRecipe,
                recipestr2, "TestUserInfo")

    def testGroupMissingParams(self):
        recipestr1 = r"""
class TestGroupInfo(GroupInfoRecipe):
    name = 'info-test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.Group()
"""

        err = self.assertRaises(cook.CookError, self.buildRecipe,
                recipestr1, "TestGroupInfo")

        recipestr2 = r"""
class TestGroupInfo(GroupInfoRecipe):
    name = 'info-test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.Group('foo')
"""

        err = self.assertRaises(cook.CookError, self.buildRecipe,
                recipestr2, "TestGroupInfo")

        recipestr3 = r"""
class TestGroupInfo(GroupInfoRecipe):
    name = 'info-test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.Group(badParam = 'foo')
"""

        err = self.assertRaises(cook.CookError, self.buildRecipe,
                recipestr3, "TestGroupInfo")

    def testBasePolicyClass(self):
        class DummyPolicy(policy.UserGroupBasePolicy):
            def __init__(x): pass
            def error(x, msg):
                self.assertEquals(msg, 'Do not directly invoke DummyPolicy')
        pol = DummyPolicy()
        pol.updateArgs('test')
