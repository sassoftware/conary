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


import sys
import os


#conary
from conary.build import loadrecipe, use, errors, defaultrecipes
from conary import changelog
from conary.conaryclient import filetypes
from conary.lib import util
from conary.deps import deps

#test
from conary_test import rephelp
from conary_test import resources


class PackageRecipeTest(rephelp.RepositoryHelper):

    def testCrossCompileSetup(self):
        siteConfigPath = os.path.abspath(resources.get_archive('site'))
        def _checkSite(macros, *paths):
            self.assertEquals(macros.env_siteconfig,
                          ' '.join([siteConfigPath + '/' + x for x in paths]))

        self.cfg.siteConfigPath = [ siteConfigPath]
        oldPath = os.environ['PATH']
        try:
            self.addTestPkg(1)
            repos = self.openRepository()
            if 'CONFIG_SITE' in os.environ: del os.environ['CONFIG_SITE']
            loader = loadrecipe.recipeLoaderFromSourceComponent('test1:source', 
                                                               self.cfg, repos)[0]
            recipeClass = loader.getRecipe()
            self.overrideBuildFlavor('is:x86(!i686,!i586,i486)')
            use.setBuildFlagsFromFlavor('test1', self.cfg.buildFlavor)

            nocross = recipeClass(self.cfg, None, None, {})
            assert(nocross.macros.target == 'i486-unknown-linux')
            assert(nocross.macros.host == 'i486-unknown-linux')
            assert(nocross.macros.build == 'i486-unknown-linux')
            assert(not nocross.isCrossCompiling())
            assert(not nocross.isCrossCompileTool())
            assert(not nocross.needsCrossFlags())

            if 'CONFIG_SITE' in os.environ: del os.environ['CONFIG_SITE']
            use.setBuildFlagsFromFlavor('test1', self.cfg.buildFlavor)

            try:
                invalid = recipeClass(self.cfg, None, None, {}, (None, '((', False))
            except errors.CookError:
                pass

            # cross compiling something for x86_64
            cross1 = recipeClass(self.cfg, None, None, {}, (None, 'x86_64', False))

            assert(cross1.macros.target == 'x86_64-unknown-linux')
            assert(cross1.macros.host == 'x86_64-unknown-linux')
            assert(cross1.macros.build == 'i486-unknown-linux')
            assert(cross1.hostmacros.lib == 'lib64')
            assert(cross1.targetmacros.lib == 'lib64')
            assert(cross1.buildmacros.lib == 'lib')
            assert(cross1.macros.lib == 'lib64')
            assert(cross1.isCrossCompiling())
            assert(not cross1.isCrossCompileTool())
            assert(cross1.needsCrossFlags())
            assert(use.Arch.x86_64)
            _checkSite(cross1.macros, 'x86_64', 'linux')
            _checkSite(cross1.buildmacros, 'x86', 'linux')
            _checkSite(cross1.hostmacros, 'x86_64', 'linux')

            if 'CONFIG_SITE' in os.environ: del os.environ['CONFIG_SITE']
            use.setBuildFlagsFromFlavor('test1', self.cfg.buildFlavor)

            # building a cross compiling tool for i686
            cross2 = recipeClass(self.cfg, None, None, {}, (None, 'x86(i686)', 
                                                                        True))
            assert(cross2.macros.target == 'i686-unknown-linux')
            assert(cross2.macros.host == 'i486-unknown-linux')
            # note the added _build here to differentiate host from build
            assert(cross2.macros.build == 'i486-unknown_build-linux')
            assert(use.Arch.x86.i686)

            # building a cross-compiler on i386 to run on x86_64 to compile
            # for ppc

            use.setBuildFlagsFromFlavor('test1', self.cfg.buildFlavor)
            if 'CONFIG_SITE' in os.environ: del os.environ['CONFIG_SITE']
            cross3 = recipeClass(self.cfg, None, None, {}, ('x86_64', 'ppc',
                                                                        True))
            assert(cross3.macros.target == 'powerpc-unknown-linux')
            assert(cross3.macros.host == 'x86_64-unknown-linux')
            assert(cross3.macros.build == 'i486-unknown-linux')
            assert(cross3.macros.cc == 'i486-unknown-linux-gcc')
            assert(cross3.macros.cxx == 'i486-unknown-linux-g++')
            # this is a cross-tool, so we're building for the system machine
            # so buildcc == cc
            assert(cross3.macros.buildcc == 'i486-unknown-linux-gcc')
            assert(cross3.macros.buildcxx == 'i486-unknown-linux-g++')
            assert(cross3.macros.lib == 'lib')
            assert(cross3.hostmacros.lib == 'lib64')
            assert(cross3.targetmacros.lib == 'lib')
            assert(cross3.buildmacros.lib == 'lib')
            assert(use.Arch.x86_64)

            assert(not cross3.isCrossCompiling())
            assert(cross3.isCrossCompileTool())
            assert(cross3.needsCrossFlags())

            # When creating a cross compiler for a ppc system
            # on an x86_64 box, configure should return results for the x86_64
            # box, not the target arch.
            _checkSite(cross3.macros, 'x86_64', 'linux')
            _checkSite(cross3.buildmacros, 'x86', 'linux')
            _checkSite(cross3.hostmacros, 'x86_64', 'linux')

            # cross compiling for i486 on i486
            # buildcc should have /usr/bin/ in front
            use.setBuildFlagsFromFlavor('test1', self.cfg.buildFlavor)
            if 'CONFIG_SITE' in os.environ: del os.environ['CONFIG_SITE']
            cross4 = recipeClass(self.cfg, None, None, {}, (None, 'x86(i486)', False))
            assert(cross4.macros.target == 'i486-unknown-linux')
            assert(cross4.macros.host == 'i486-unknown-linux')
            assert(cross4.macros.build == 'i486-unknown_build-linux')
            assert(cross4.macros.buildcc == '/usr/bin/i486-unknown-linux-gcc')
            assert(cross4.macros.buildcxx == '/usr/bin/i486-unknown-linux-g++')
            assert(cross4.macros.cc == 'i486-unknown-linux-gcc')
            assert(cross4.macros.cxx == 'i486-unknown-linux-g++')

        finally:
            os.environ['PATH'] = oldPath

    def testCrossCompileSetupMacrosOverride(self):

        macroOverrides = {'crossdir' : 'foo'}
        oldPath = os.environ['PATH']
        try:
            self.addTestPkg(1)
            repos = self.openRepository()
            loader = loadrecipe.recipeLoaderFromSourceComponent('test1:source', 
                                                               self.cfg, repos)[0]
            recipeClass = loader.getRecipe()
            self.overrideBuildFlavor('is:x86(!i686,!i586,i486)')
            use.setBuildFlagsFromFlavor('test1', self.cfg.buildFlavor)

            # cross compiling something for x86_64
            cross1 = recipeClass(self.cfg, None, None, macroOverrides, 
                                 (None, 'x86_64', False))

            assert(cross1.macros.target == 'x86_64-unknown-linux')
            assert(cross1.macros.host == 'x86_64-unknown-linux')
            assert(cross1.macros.build == 'i486-unknown-linux')
            assert(use.Arch.x86_64)
            assert(cross1.macros.crossprefix == '/opt/foo')
        finally:
            os.environ['PATH'] = oldPath





    def testCrossCompile(self):
        oldPath = os.environ['PATH']
        try:
            self.addTestPkg(1,
                content="""r.Run("echo $CONFIG_SITE | grep 'x86.*linux'")""")
            repos = self.openRepository()
            stdout = os.dup(sys.stdout.fileno())
            stderr = os.dup(sys.stderr.fileno())
            null = os.open('/dev/null', os.O_WRONLY)
            os.dup2(null, sys.stdout.fileno())
            os.dup2(null, sys.stderr.fileno())
            try:
                self.cookItem(repos, self.cfg, 'test1',
                              crossCompile=(None, 'x86_64', False))
            finally:
                os.dup2(stdout, sys.stdout.fileno())
                os.dup2(stderr, sys.stderr.fileno())
                os.close(null)
                os.close(stdout)
                os.close(stderr)
        finally:
            os.environ['PATH'] = oldPath

    def testMacroOverrides(self):
        self.addTestPkg(1)
        repos = self.openRepository()
        loader = loadrecipe.recipeLoaderFromSourceComponent('test1:source', 
                                                           self.cfg, repos)[0]
        recipeClass = loader.getRecipe()
        self.overrideBuildFlavor('is:x86(!i686,!i586,i486)')
        use.setBuildFlagsFromFlavor('test1', self.cfg.buildFlavor)

        dummy = recipeClass(self.cfg, None, None, {})
        assert(dummy.macros.dummyMacro == 'right')

    def testBasicMacros(self):
        oldPath = os.environ['PATH']
        try:
            self.addTestPkg(1)
            repos = self.openRepository()
            loader = loadrecipe.recipeLoaderFromSourceComponent('test1:source', 
                                                               self.cfg, repos)[0]
            recipeClass = loader.getRecipe()
            use.setBuildFlagsFromFlavor('test1', self.cfg.buildFlavor)

            # This should override the value from extraMacros
            self.cfg.configLine('macros thing value')
            # And this should override the default value
            self.cfg.configLine('macros bindir /binaries')

            recipeObj = recipeClass(self.cfg, None, None,
                extraMacros={'thing': 'wrong'})
            self.assertEqual(recipeObj.macros.name, 'test1')
            self.assertEqual(recipeObj.macros.version, '1.0')
            self.assertEqual(recipeObj.macros.bindir, '/binaries')
        finally:
            os.environ['PATH'] = oldPath

    def testNoBlankPackage(self):
        '''
        Check that the empty string doesn't get added to r.packages
        when a component is created with r.Install

        @tests: CNY-2846
        '''

        recipe = """
        class SomePackage(PackageRecipe):
            name = 'somepackage'
            version = '1'
            def setup(r):
                r.Create('blargh')
                r.Install('blargh', '%(datadir)s/', package=':foo')
        """

        loader = loadrecipe.RecipeLoaderFromString(self.trimRecipe(recipe),
            "/test", cfg=self.cfg, repos=self.openRepository(),
            component='somepackage')
        recipeClass = loader.getRecipe()
        recipeObj = recipeClass(self.cfg, None, None)
        recipeObj.loadPolicy()
        recipeObj.setup()
        self.assertFalse('' in recipeObj.packages)

    def testBaseRequiresRecipeDeps(self):
        baseReqRecipe = """class BaseRequiresRecipe(AbstractPackageRecipe):
            name = 'baserequires'
            version = '1.0.0'
            abstractBaseClass = 1

            buildRequires = ['foo:devel']"""

        pkgRecipe = defaultrecipes.PackageRecipe.replace( \
                'internalAbstractBaseClass', 'abstractBaseClass')
        pkgRecipe += "\n    version = '1'"

        simpleRecipe = """class Simple(PackageRecipe):
            name = 'simple'
            version = '1'

            # don't clear the buildReqs. that's what we're trying to test
            def setup(r):
                r.Create('/opt/foo')"""

        self.addComponent('baserequires:recipe',
                fileContents = [('/baserequires.recipe',
                    filetypes.RegularFile(contents = baseReqRecipe))])
        self.addCollection('baserequires',
                strongList = ['baserequires:recipe'])
        self.addComponent('package:recipe',
                fileContents = [('/package.recipe',
                    filetypes.RegularFile(contents = pkgRecipe))])
        self.addCollection('package',
                strongList = ['package:recipe'])

        self.cfg.autoLoadRecipes = ['baserequires', 'package']
        err = self.assertRaises(errors.RecipeDependencyError, self.buildRecipe, simpleRecipe, "Simple")
        for comp in ('foo:devel', 'bzip2:runtime', 'gzip:runtime',
                'tar:runtime', 'cpio:runtime', 'patch:runtime'):
            self.addComponent(comp)
            self.updatePkg(comp)
        self.buildRecipe(simpleRecipe, 'Simple')

    def testPackageRecipeDeps(self):
        pkgRecipe = """class PackageRecipe(SourcePackageRecipe):
            name = 'package'
            version = '1.0.0'
            abstractBaseClass = 1

            clearBuildReqs()
            buildRequires = ['foo:devel']"""

        simpleRecipe = """class Simple(PackageRecipe):
            name = 'simple'
            version = '1'

            # don't clear the buildReqs. that's what we're trying to test
            def setup(r):
                r.Create('/opt/foo')"""

        self.addComponent('foo:devel')
        self.addComponent('package:recipe',
                fileContents = [('/package.recipe',
                    filetypes.RegularFile(contents = pkgRecipe))])
        self.addCollection('package',
                strongList = ['package:recipe'])
        self.cfg.autoLoadRecipes = ['package']
        err = self.assertRaises(errors.RecipeDependencyError, self.buildRecipe, simpleRecipe, "Simple")
        self.updatePkg('foo:devel')
        self.buildRecipe(simpleRecipe, 'Simple')

    def testPackageRecipeDeps2(self):
        # ensure that we don't force any particular trovename to be associated
        # with a given recipe name.
        pkgRecipe = """class PackageRecipe(SourcePackageRecipe):
            name = 'differentname'
            version = '1.0.0'
            abstractBaseClass = 1

            clearBuildReqs()
            buildRequires = ['foo:devel']"""

        simpleRecipe = """class Simple(PackageRecipe):
            name = 'simple'
            version = '1'

            # don't clear the buildReqs. that's what we're trying to test
            def setup(r):
                r.Create('/opt/foo')"""

        self.addComponent('foo:devel')
        self.addComponent('differentname:recipe',
                fileContents = [('/differentname.recipe',
                    filetypes.RegularFile(contents = pkgRecipe))])
        self.addCollection('differentname',
                strongList = ['differentname:recipe'])
        self.cfg.autoLoadRecipes = ['differentname']
        err = self.assertRaises(errors.RecipeDependencyError, self.buildRecipe, simpleRecipe, "Simple")
        self.updatePkg('foo:devel')
        self.buildRecipe(simpleRecipe, 'Simple')

    def testUserInfoRecipeDeps(self):
        baseReqRecipe = """class BaseRequiresRecipe(AbstractPackageRecipe):
            name = 'baserequires'
            version = '1'
            abstractBaseClass = 1

            buildRequires = ['foo:devel']"""

        userInfoRecipe = defaultrecipes.UserInfoRecipe.replace( \
                'internalAbstractBaseClass', 'abstractBaseClass')
        userInfoRecipe += "\n    version = '1'"

        infoRecipe = """class InfoFoo(UserInfoRecipe):
            name = 'info-foo'
            version = '1'

            # don't clear the buildReqs. that's what we're trying to test
            def setup(r):
                r.User('foo', 2121)"""

        self.addComponent('foo:devel')
        self.addComponent('baserequires:recipe',
                fileContents = [('/baserequires.recipe',
                    filetypes.RegularFile(contents = baseReqRecipe))])
        self.addCollection('baserequires',
                strongList = ['baserequires:recipe'])

        self.addComponent('userinfo:recipe',
                fileContents = [('/userinfo.recipe',
                    filetypes.RegularFile(contents = userInfoRecipe))])
        self.addCollection('userinfo',
                strongList = ['userinfo:recipe'])

        self.cfg.autoLoadRecipes = ['baserequires', 'userinfo']
        err = self.assertRaises(errors.RecipeDependencyError, self.buildRecipe, infoRecipe, "InfoFoo")
        self.updatePkg('foo:devel')
        self.buildRecipe(infoRecipe, 'InfoFoo')

    def testUserInfoRecipeCook(self):
        userInfoRecipe = """
class UserInfoRecipe(UserGroupInfoRecipe, BaseRequiresRecipe):
    name = 'userinfo'
    version = '1'
    abstractBaseClass = 1"""
        for stubComp in (
            'bash:runtime',
            'conary-build:lib',
            'conary-build:python',
            'conary-build:runtime',
            'conary:python',
            'conary:runtime',
            'coreutils:runtime',
            'dev:runtime',
            'filesystem:runtime',
            'findutils:runtime',
            'gawk:runtime',
            'grep:runtime',
            'python:lib',
            'python:runtime',
            'sed:runtime',
            'setup:runtime',
            'sqlite:lib',
):
            self.addComponent(stubComp)
            self.updatePkg(stubComp)
        laDir = os.path.join(self.cfg.lookaside, 'userinfo')
        util.mkdirChain(laDir)
        open(os.path.join(laDir, 'userinfo.recipe'), 'w').write('')
        self.cfg.baseClassDir = '/usr/share/conary/baseclasses'
        res = self.buildRecipe(userInfoRecipe, 'UserInfoRecipe')
        self.assertEquals(res[0][0][0], 'userinfo:recipe')

    def testUserInfoRecipeCook2(self):
        userInfoRecipe = """
class UserInfoRecipe(UserGroupInfoRecipe, BaseRequiresRecipe):
    name = 'userinfo'
    clearBuildReqs()
    version = '1'
    abstractBaseClass = 1
"""
        # if we don't re-direct this setting, we can't build
        self.cfg.baseClassDir = '/usr/share/conary/baseclasses'
        self.openRepository()
        client = self.getConaryClient()
        repos = client.getRepos()

        fileDict = {'userinfo.recipe' : filetypes.RegularFile(contents = userInfoRecipe, config = True)}
        chLog = changelog.ChangeLog(name = 'foo', contact = 'bar', message = 'test\n')
        cs = client.createSourceTrove('userinfo:source', str(self.cfg.buildLabel), '1', fileDict, chLog)
        repos.commitChangeSet(cs)
        res = self.cookFromRepository('userinfo:source', buildLabel = self.cfg.buildLabel, repos = repos, logBuild = True)
        self.assertEquals(res[0][0], 'userinfo:recipe')

    def testGroupInfoRecipeDeps(self):
        pkgRecipe = """class BaseRequiresRecipe(AbstractPackageRecipe):
            name = 'baserequires'
            version = '1'
            abstractBaseClass = 1

            buildRequires = ['foo:devel']"""

        groupInfoRecipe = defaultrecipes.GroupInfoRecipe.replace( \
                'internalAbstractBaseClass', 'abstractBaseClass')
        groupInfoRecipe += "\n    version = '1'"

        infoRecipe = """class InfoFoo(GroupInfoRecipe):
            name = 'info-foo'
            version = '1'

            # don't clear the buildReqs. that's what we're trying to test
            def setup(r):
                r.Group('foo', 2121)"""

        self.addComponent('foo:devel')
        self.addComponent('baserequires:recipe',
                fileContents = [('/baserequires.recipe',
                    filetypes.RegularFile(contents = pkgRecipe))])
        self.addCollection('baserequires',
                strongList = ['baserequires:recipe'])
        self.addComponent('groupinfo:recipe',
                fileContents = [('/groupinfo.recipe',
                    filetypes.RegularFile(contents = groupInfoRecipe))])
        self.addCollection('groupinfo',
                strongList = ['groupinfo:recipe'])

        self.cfg.autoLoadRecipes = ['baserequires', 'groupinfo']
        err = self.assertRaises(errors.RecipeDependencyError, self.buildRecipe, infoRecipe, "InfoFoo")
        self.updatePkg('foo:devel')
        self.buildRecipe(infoRecipe, 'InfoFoo')

    def testGroupInfoRecipeCook(self):
        groupInfoRecipe = """
class GroupInfoRecipe(UserGroupInfoRecipe, BaseRequiresRecipe):
    name = 'groupinfo'
    version = '1'
    abstractBaseClass = 1"""
        for stubComp in ('conary-build:lib', 'conary-build:python', 'conary-build:runtime', 'conary:python', 'conary:runtime', 'filesystem:runtime', 'python:lib', 'python:runtime', 'setup:runtime', 'sqlite:lib', 'bash:runtime', 'coreutils:runtime', 'dev:runtime', 'findutils:runtime', 'gawk:runtime', 'grep:runtime', 'sed:runtime'):
            self.addComponent(stubComp)
            self.updatePkg(stubComp)
        laDir = os.path.join(self.cfg.lookaside, 'groupinfo')
        util.mkdirChain(laDir)
        open(os.path.join(laDir, 'groupinfo.recipe'), 'w').write('')
        self.cfg.baseClassDir = '/usr/share/conary/baseclasses'
        res = self.buildRecipe(groupInfoRecipe, 'GroupInfoRecipe')
        self.assertEquals(res[0][0][0], 'groupinfo:recipe')

    def testGroupInfoRecipeCook2(self):
        groupInfoRecipe = """
class GroupInfoRecipe(UserGroupInfoRecipe, BaseRequiresRecipe):
    name = 'groupinfo'
    clearBuildReqs()
    version = '1'
    abstractBaseClass = 1
"""
        # if we don't re-direct this setting, we can't build
        self.cfg.baseClassDir = '/usr/share/conary/baseclasses'
        self.openRepository()
        client = self.getConaryClient()
        repos = client.getRepos()

        fileDict = {'groupinfo.recipe' : filetypes.RegularFile(contents = groupInfoRecipe, config = True)}
        chLog = changelog.ChangeLog(name = 'foo', contact = 'bar', message = 'test\n')
        cs = client.createSourceTrove('groupinfo:source', str(self.cfg.buildLabel), '1', fileDict, chLog)
        repos.commitChangeSet(cs)
        res = self.cookFromRepository('groupinfo:source', buildLabel = self.cfg.buildLabel, repos = repos, logBuild = True)
        self.assertEquals(res[0][0], 'groupinfo:recipe')

    def testUserInfoGroupInfoSplitting(self):
        """
        We need to create users and groups sepparately to handle dependency
        loops in group memebership. For instance on CentOS 6 the daemon user is
        in the bin, lp, and daemon groups. The lp user is in the daemon and lp
        groups. This means that neither of these users can be installed due to
        the depency loop. The install code does put all of the uesrs into the
        same install job, but then can't deal with the loop. (CNY-3731)
        """

        daemonInfoRecipe = """
class InfoDaemon(UserInfoRecipe):
    name = 'info-daemon'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.User('daemon', 2, supplemental=['lp', 'bin'])
"""

        lpInfoRecipe = """
class InfoLp(UserInfoRecipe):
    name = 'info-lp'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.User('lp', 3, supplemental=['daemon', ])
"""

        binInfoRecipe = """
class InfoBin(UserInfoRecipe):
    name = 'info-bin'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.User('bin', 4, supplemental=['daemon', ])
"""

        comps1, r1 = self.buildRecipe(daemonInfoRecipe, 'InfoDaemon')
        comps2, r2 = self.buildRecipe(lpInfoRecipe, 'InfoLp')
        comps3, r3 = self.buildRecipe(binInfoRecipe, 'InfoBin')

        # Make sure there is a :user and :group component for each package.
        for comps in (comps1, comps2, comps3):
            self.failUnlessEqual(len([ x for x in comps
                if x[0].endswith(':user') or x[0].endswith(':group') ]), 2)

        group1, user1 = sorted(comps1)
        group2, user2 = sorted(comps2)
        group3, user3 = sorted(comps3)

        repos = self.openRepository()
        user1nvf = repos.findTrove(None, user1)
        user2nvf = repos.findTrove(None, user2)
        user3nvf = repos.findTrove(None, user3)

        user1trv = repos.getTrove(*user1nvf[0])
        user2trv = repos.getTrove(*user2nvf[0])
        user3trv = repos.getTrove(*user3nvf[0])

        group1nvf = repos.findTrove(None, group1)
        group2nvf = repos.findTrove(None, group2)
        group3nvf = repos.findTrove(None, group3)

        group1trv = repos.getTrove(*group1nvf[0])
        group2trv = repos.getTrove(*group2nvf[0])
        group3trv = repos.getTrove(*group3nvf[0])

        # info-daemon:user should require info-bin:group, info-daemon:group,
        # and info-lp:group.
        self.failUnlessEqual(user1trv.requires,
            deps.ThawDependencySet('8#bin|8#daemon|8#lp'))
        # info-lp:user should require info-daemon:group and info-lp:group.
        self.failUnlessEqual(user2trv.requires,
            deps.ThawDependencySet('8#daemon|8#lp'))
        # info-bin:user should require info-bin:group and info-dameon:group.
        self.failUnlessEqual(user3trv.requires,
            deps.ThawDependencySet('8#bin|8#daemon'))

        # Make sure the user components don't provide groups.
        self.failUnlessEqual(user1trv.provides,
            deps.ThawDependencySet('4#info-daemon::user|7#daemon'))
        self.failUnlessEqual(user2trv.provides,
            deps.ThawDependencySet('4#info-lp::user|7#lp'))
        self.failUnlessEqual(user3trv.provides,
            deps.ThawDependencySet('4#info-bin::user|7#bin'))

        # Make sure the group components provide the groups
        self.failUnlessEqual(group1trv.provides,
            deps.ThawDependencySet('4#info-daemon::group|8#daemon'))
        self.failUnlessEqual(group2trv.provides,
            deps.ThawDependencySet('4#info-lp::group|8#lp'))
        self.failUnlessEqual(group3trv.provides,
            deps.ThawDependencySet('4#info-bin::group|8#bin'))

        # And that the groups don't require anything
        self.failUnlessEqual(group1trv.requires, deps.ThawDependencySet(''))
        self.failUnlessEqual(group2trv.requires, deps.ThawDependencySet(''))
        self.failUnlessEqual(group3trv.requires, deps.ThawDependencySet(''))

        self.updatePkg('info-daemon:user', resolve=True)
