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


import tempfile
import os

#test
from testrunner.testhelp import context
from conary_test import recipes
from conary_test import rephelp

#conary
from conary.build import use
from conary.deps import deps
from conary.lib import log
from conary.local import database
from conary.versions import Label, VersionFromString

class DepTest(rephelp.RepositoryHelper):
    def getFlavor(self):
        if use.Arch.x86:
            return {'flavor': 'is: x86'}
        elif use.Arch.x86_64:
            return {'flavor': 'is: x86_64'}
        else:
            raise NotImplementedError, 'edit test for this arch'

    def testDependencies(self):
        self.resetRepository()
        self.resetRoot()
        (built, d) = self.buildRecipe(recipes.libhelloRecipe, "Libhello")
        version = built[0][1]
        # XXX need more keepExisting tests
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello:user", version,
                                       tagScript = '/dev/null',
                                       keepExisting=True)
        assert(str == '1 additional trove is needed:\n'
                      '    libhello:runtime=0-1-1 is required by:\n'
                      '       libhello:user=0-1-1\n')

        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello:user", version,
                                       tagScript = '/dev/null')
        assert(str == "1 additional trove is needed:\n"
                      "    libhello:runtime=0-1-1 is required by:\n"
                      "       libhello:user=0-1-1\n")
        self.cfg.fullVersions = self.cfg.fullFlavors = True

        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello:user", version,
                                       tagScript = '/dev/null')

        self.assertEqual(str,
                 "1 additional trove is needed:\n"
                 "    libhello:runtime=/localhost@rpl:linux/0-1-1[%(is)s]"
                 " is required by:\n"
                 "       libhello:user=/localhost@rpl:linux/0-1-1[%(is)s]\n"
            % self.buildIs)
        self.cfg.fullVersions = self.cfg.fullFlavors = False
        self.cfg.showLabels = True
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello:user", version,
                                       tagScript = '/dev/null')
        assert(str == 
                 "1 additional trove is needed:\n"
                 "    libhello:runtime=localhost@rpl:linux/0-1-1"
                 " is required by:\n"
                 "       libhello:user=localhost@rpl:linux/0-1-1\n")

        self.cfg.showLabels = False



        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello:runtime", version,
                                       tagScript = '/dev/null')
        assert(not str)
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello:user", version,
                                       tagScript = '/dev/null')
        assert(not str)

        self.resetRoot()
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello", version,
                                       tagScript = '/dev/null')
        self.assertEquals(str, '')

        self.resetRoot()
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello:user", version,
                                       tagScript = '/dev/null',
                                       resolve = True)
        assert(str == "Including extra troves to resolve dependencies:\n"
                      "    libhello:runtime=0-1-1\n")

        self.resetRoot()

        self.cfg.fullFlavors = self.cfg.fullVersions = True
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello:user", version,
                                       tagScript = '/dev/null',
                                       resolve = True)
        assert(str == "Including extra troves to resolve dependencies:\n"
                      "    libhello:runtime=/localhost@rpl:linux/0-1-1[%(flavor)s]\n" %self.getFlavor())
        self.cfg.fullFlavors = self.cfg.fullVersions = False
        self.resetRoot()





        # add an extra bogus value at the front of the installLabelPath
        # this ensures that conary searches the whole installLabelPath
        oldPath = self.cfg.installLabelPath[:]
        self.cfg.installLabelPath.insert(0, Label('localhost@foo:bar'))

        # :script needs :user, which needs :runtime -- this makes sure
        # multilevel (well, 2 level) dependency resolution works
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello:script", version,
                                       tagScript = '/dev/null',
                                       resolve = True)
        assert(str == "Including extra troves to resolve dependencies:\n"
                      "    libhello:runtime=0-1-1 libhello:user=0-1-1\n")

        self.cfg.installLabelPath = oldPath

        (rc, str) = self.captureOutput(self.erasePkg, 
                                       self.rootDir,
                                       "libhello:user")
        self.assertEquals(str, """\
The following dependencies would not be met after this update:

  libhello:script=0-1-1 (Already installed) requires:
    file: /sbin/user
  which is provided by:
    libhello:user=0-1-1 (Would be erased)
""")

        self.cfg.showLabels = self.cfg.fullFlavors = True
        (rc, str) = self.captureOutput(self.erasePkg, 
                                       self.rootDir,
                                       "libhello:user")
        assert(str == """\
The following dependencies would not be met after this update:

  libhello:script=localhost@rpl:linux/0-1-1[%(flavor)s] (Already installed) requires:
    file: /sbin/user
  which is provided by:
    libhello:user=localhost@rpl:linux/0-1-1[%(flavor)s] (Would be erased)
""" % self.getFlavor())
        self.cfg.showLabels = self.cfg.fullFlavors = False



        (rc, str) = self.captureOutput(self.erasePkg, 
                                       self.rootDir,
                                       "libhello:user",
                                       depCheck = False)
        assert(not str)

        # test erase breaking a dependency w/ flags (since getting the flags
        # set has caused problems before)
        self.resetRoot()
        
        self.updatePkg(self.rootDir, "libhello:runtime", version,
                       tagScript = '/dev/null')
        self.updatePkg(self.rootDir, "libhello:user", version,
                       tagScript = '/dev/null')

        (rc, str) = self.captureOutput(self.erasePkg, 
                                       self.rootDir,
                                       "libhello:runtime",
                                       depCheck = True)
        if use.Arch.x86:
            assert(str == """\
The following dependencies would not be met after this update:

  libhello:user=0-1-1 (Already installed) requires:
    soname: ELF32/libhello.so.0(SysV x86)
  which is provided by:
    libhello:runtime=0-1-1 (Would be erased)
""")
        elif use.Arch.x86_64:
            assert(str == """\
The following dependencies would not be met after this update:

  libhello:user=0-1-1 (Already installed) requires:
    soname: ELF64/libhello.so.0(SysV x86_64)
  which is provided by:
    libhello:runtime=0-1-1 (Would be erased)
""")
        else:
            raise NotImplementedError, 'modify test for this arch'

    def testUpdateTwoProvideSameDependencies(self):
        # CNY-2459 - moving from one trove providing foo:lib(1) to two
        # caused some mismatches in the dep resolution code
        self.addComponent('foo:lib[is:x86]', provides='trove:foo:lib')
        self.addComponent('foo:lib[is:x86_64]', provides='trove:foo:lib(1)',
                          filePrimer=1)
        self.addComponent('bar:lib', requires='trove:foo:lib(1)', filePrimer=2)
        self.updatePkg(['foo:lib[is:x86]', 'foo:lib[is:x86_64]', 'bar:lib'])
        self.addComponent('foo:lib=2[is:x86]', provides='trove:foo:lib(1)')
        self.addComponent('foo:lib=2[is:x86_64]', filePrimer=1,
                            provides='trove:foo:lib(1)')
        self.checkUpdate('foo:lib', ['foo:lib=1.0[is:x86_64]--2[is:x86_64]',
                                     'foo:lib=1.0[is:x86]--2[is:x86]'])

    def testNoVersionSonameDependencies(self):
        # just need to do one test to make sure the dependency shows up;
        # all the other functionality should be adequately tested in
        # testDependencies
        self.resetRepository()
        self.resetRoot()
        (built, d) = self.buildRecipe(recipes.libhelloRecipeNoVersion, "Libhello")
        version = built[0][1]
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "libhello:user", version,
                                       tagScript = '/dev/null',
                                       resolve = True)
        assert(str == "Including extra troves to resolve dependencies:\n"
                      "    libhello:runtime=0-1-1\n")

    def testFileDependency(self):
        self.resetRepository()
        self.resetRoot()

        (built, d) = self.buildRecipe(recipes.bashUserRecipe, "BashUser")
        version = built[0][1]

        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "bashuser", version,
                                       tagScript = '/dev/null')
        assert(str == "The following dependencies could not be resolved:\n"
                      "    bashuser:runtime=0-1-1:\n"
                      "\tfile: /bin/bash\n")

        (built, d) = self.buildRecipe(recipes.bashRecipe, "Bash")
        version = built[0][1]

        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "bashuser", tagScript = '/dev/null')
        assert(str == '1 additional trove is needed:\n'
                      '    bash:runtime=0-1-1 is required by:\n'
                      '       bashuser:runtime=0-1-1\n')

    def testTroveDependency(self):
        self.resetRepository()
        self.resetRoot()

        (built, d) = self.buildRecipe(recipes.bashTroveUserRecipe, 
                                      "BashTroveUser")
        version = built[0][1]

        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "bashtroveuser", version,
                                       tagScript = '/dev/null')
        assert(str == "The following dependencies could not be resolved:\n"
                      "    bashtroveuser:lib=0-1-1:\n"
                      "\ttrove: bash:runtime\n")

        (built, d) = self.buildRecipe(recipes.bashRecipe, "Bash")
        version = built[0][1]

        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "bashtroveuser", tagScript = '/dev/null')
        assert(str == "1 additional trove is needed:\n"
                      "    bash:runtime=0-1-1 is required by:\n"
                      "       bashtroveuser:lib=0-1-1\n")

    def testUnresolvedDependencies(self):
        self.resetRepository()
        self.resetRoot()

        (built, d) = self.buildRecipe(recipes.testUnresolved, 'Unresolved')
        version = built[0][1]
        (rc, s) = self.captureOutput(self.updatePkg, self.rootDir,
                                       'testcase:runtime', version,
                                       tagScript = '/dev/null')
        s = s.split('\n')
        expected = ['The following dependencies could not be resolved:',
                    '    testcase:runtime=1.0-1-1:',
                    '\ttrove: bar:foo',
                    '']
        assert(s == expected)

    def testBreakOnUpdate(self):
        # create a package which provides a file, then update that package to 
        # a version which does not provide that library while installing a 
        # package which requires that file. it should cause a dep failure.
        self.resetRepository()
        self.resetRoot()

        (built, d) = self.buildRecipe(recipes.bashRecipe, "Bash")
        bashVersion = built[0][1]
        (built, d) = self.buildRecipe(recipes.bashMissingRecipe, "Bash")
        noBashVersion = built[0][1]

        (built, d) = self.buildRecipe(recipes.bashUserRecipe, "BashUser")
        userVersion = built[0][1]

        repos = self.openRepository()
        # this changeset updates bash to a trove which removed /bin/bash
        # and installs bashuser, which requies /bin/bash
        cs = repos.createChangeSet([("bash", 
                                     (VersionFromString(bashVersion), deps.parseFlavor('ssl')), 
                                     (VersionFromString(noBashVersion), deps.parseFlavor('ssl')), 
                                    0),
                                    ("bashuser", (None, None),
                                     (VersionFromString(userVersion), 
                                      deps.Flavor()), 
                                    0)
                                  ])

        (fd, fileName) = tempfile.mkstemp()
        os.close(fd)
        cs.writeToFile(fileName)

        try:
            self.updatePkg(self.rootDir, 'bash', version = bashVersion)
            (rc, s) = self.captureOutput(self.updatePkg, self.rootDir, fileName)
        finally:
            os.unlink(fileName)
        expectedStr = """\
The following dependencies would not be met after this update:

  bashuser:runtime=0-1-1 (Would be newly installed) requires:
    file: /bin/bash
  which is provided by:
    bash:runtime=0-1-1 (Would be updated to 1-1-1)
"""
        self.assertEquals(s, expectedStr)

        # since /bin/bash is still installed, this should work fine
        self.updatePkg(self.rootDir, 'bashuser', version = bashVersion)

    def testNotBrokenOnUpdate(self):
        # create trove a and trove b, a file in trove b requires a:runtime
        # install both trove a and b.  build a new version of a.  update
        # to new version of a.
        self.resetRepository()
        self.resetRoot()

        (built, d) = self.buildRecipe(recipes.testTroveDepA, "A")
        aVersion = built[0][1]
        (built, d) = self.buildRecipe(recipes.testTroveDepB, "B")
        bVersion = built[0][1]

        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "a", version=aVersion,
                                       tagScript = '/dev/null')
        assert(str == "")
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "b", version=bVersion,
                                       tagScript = '/dev/null')
        assert(str == "")
        
        # build a new version of a to do an update
        (built, d) = self.buildRecipe(recipes.testTroveDepA, "A")
        aNewVersion = built[0][1]
        
        # since a:runtime is still provided, this should work fine
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       'a', version = aNewVersion)
        assert(str == "")

    def testNoSuggestionsSatisfyFlavor(self):
        # test that when none of the suggestion's flavors satisfy
        # the flavor, nothing is installed.
        self.resetRepository()
        self.resetRoot()

        # make sure that bash gets the ssl flavor
        self.overrideBuildFlavor('~ssl')
        (built, d) = self.buildRecipe(recipes.bashRecipe, 'Bash')
        (built, d) = self.buildRecipe(recipes.bashTroveUserRecipe,
                                      'BashTroveUser')
        userVersion = built[0][1]

        # require ssl to be off in all the flavors in self.cfg.flavor
        for flavor in self.cfg.flavor:
            flavor.union(deps.parseFlavor('use: !ssl'),
                          mergeType = deps.DEP_MERGE_TYPE_OVERRIDE)
        (rc, s) = self.captureOutput(self.updatePkg, self.rootDir,
                                       'bashtroveuser', userVersion,
                                       tagScript = '/dev/null',
                                       resolve = True)
        assert(s == 'The following dependencies could not be resolved:\n'
                    '    bashtroveuser:lib=0-1-1:\n'
                    '\ttrove: bash:runtime\n')

    # XXX test an update with --resolve where two troves are available
    # -- one that scores -1, one that scores False

    def testMultipleVersionsOfTroveNeeded(self):
        # this test builds two versions of foo:lib: 1.0-1-1, and
        # 2.0-1-1.  1.0-1-1 gets installed.  then we attempt to
        # install bar:runtime which requires foo:lib=1.0-1-1 and
        # baz:runtime which requires foo:lib=2.0-1-1.  the depsolver
        # should suggest foo:lib 2.0-1-1 to solve baz:runtime's dep.
        # but if you use --resolve, bar:runtime's dependency is broken.

        # IDEALY, the depsolver would tell you to install both versions
        # of foo:lib, but it has no idea if that will actually work, since
        # the files cannot overlap.

        self.resetRepository()
        self.resetRoot()

        (built, d) = self.buildRecipe(recipes.depsMultiVersionTest1, 'Foo')
        v1,f1 = built[0][1:3]
        (built, d) = self.buildRecipe(recipes.depsMultiVersionTest2, 'Foo')
        v2,f2 = built[0][1:3]

        (built, d) = self.buildRecipe(recipes.depsMultiVersionUser1, 'Bar')
        user1, flavor1 = built[0][1:3]

        (built, d) = self.buildRecipe(recipes.depsMultiVersionUser2, 'Baz')
        user2, flavor2 = built[0][1:3]

        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       'foo:lib', version=v1,
                                       tagScript = '/dev/null')
        assert(str == "")

        self.logFilter.add()
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       [ 'bar=%s' %user1, 'baz=%s' %user2 ],
                                       tagScript = '/dev/null')
        self.logFilter.clear()

        assert(str == "1 additional trove is needed:\n"
                      "    foo:lib=2.0-1-1 is required by:\n"
                      "       baz:runtime=1.0-1-1\n")


        # now attempt to resolve.  this will attempt to update foo:lib
        # to a version that does not have what bar:runtime needs
        self.logFilter.add()
        self.cfg.keepRequired = True
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       [ 'bar=%s' %user1, 'baz=%s' %user2 ],
                                       tagScript = '/dev/null',
                                       resolve = True)
        self.logFilter.clear()
        assert(str == 'Including extra troves to resolve dependencies:\n'
                      '    foo:lib=2.0-1-1\n')

    def testGroupDeps(self):
        self.addQuickTestComponent('test:runtime', '1.0-1-1')
        self.buildRecipe(recipes.dependencyGroup, 'DependencyGroup')
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       'group-test')
        assert(str == 'The following dependencies could not be resolved:\n'
                      '    group-test=1.0-1-1:\n'
                      '\ttrove: other\n')

    def testCrossFlavorDeps(self):
        provideRec = recipes.bashRecipe + '        if Arch.x86:pass'
        userRec = recipes.bashUserRecipe + '        if Arch.x86:pass'

        # build provides (foo) as x86
        self.overrideBuildFlavor('is:x86')
        (built, d) = self.buildRecipe(provideRec, 'Bash')
        v1,f1 = built[0][1:3]

        # build requires (bar) as x86
        (built, d) = self.buildRecipe(userRec, 'BashUser')
        user1, flavor1 = built[0][1:3]

        # set flavor to be x86_64
        x86Flavor = deps.overrideFlavor(self.cfg.flavor[0], 
                                        deps.parseFlavor('is:x86'))
        x86_64Flavor = deps.overrideFlavor(self.cfg.flavor[0], 
                                           deps.parseFlavor('is:x86_64'))
        self.cfg.flavor = [x86_64Flavor]

        # install w/ x86 flavor request
        rc, str = self.captureOutput(self.updatePkg, 
                            self.rootDir, 'bashuser:runtime', user1,
                           tagScript = '/dev/null',
                           resolve = True, 
                           flavor='is:x86')
        # XXX ideally, the x86 flavor of the trove we are installing should
        # override the install flavor path here, and find the right trove
        assert(str == 'The following dependencies could not be resolved:\n'
                      '    bashuser:runtime=0-1-1:\n'
                      '\tfile: /bin/bash\n')

        # flavorPath is now x86_64, x86
        self.cfg.flavor =  [ x86_64Flavor, x86Flavor ]
        rc, str = self.captureOutput(self.updatePkg, 
                           self.rootDir, 'bashuser:runtime', user1,
                           tagScript = '/dev/null',
                           resolve = True, 
                           justDatabase = True,
                           flavor='')
        assert(str == 'Including extra troves to resolve dependencies:\n'
                      '    bash:runtime=0-1-1\n')
        # these erases spew output because of the justDatabase flags used
        # during the install. squelch.
        self.captureOutput(self.erasePkg, self.rootDir, 'bashuser:runtime',
                           justDatabase=True)
        self.captureOutput(self.erasePkg, self.rootDir, 'bash:runtime',
                           justDatabase=True)
        self.cfg.buildFlavor = x86_64Flavor

        (built, d) = self.buildRecipe(provideRec, 'Bash')
        v1,f2 = built[0][1:3]
        rc, str = self.captureOutput(self.updatePkg, 
                           self.rootDir, 'bashuser:runtime', user1,
                           tagScript = '/dev/null',
                           resolve = True, 
                           justDatabase = True,
                           flavor='')
        assert(str == 'Including extra troves to resolve dependencies:\n'
                      '    bash:runtime=0-1-1\n')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        # make sure that we installed the x86_64 one -- the first one that
        # matches on our flavor path
        bashTroves = db.findTrove(None, ('bash:runtime', None, None))
        assert(len(bashTroves) == 1)
        assert(bashTroves[0][2].freeze() == '1#x86_64|5#use:ssl')

    def testGetTrovesWithProvides(self):
        v = '/localhost@rpl:linux/1.0-1-1' 
        f = ''
        db = database.Database(self.rootDir, self.cfg.dbPath)
        prov1 = deps.parseDep('trove: test1:runtime')
        prov2 = deps.parseDep('trove: test2:runtime')
        self.addQuickDbTestPkg(db, 'test1', v,f, provides=prov1)
        self.addQuickDbTestPkg(db, 'test2', v,f, provides=prov2)
        self.addQuickDbTestPkg(db, 'test1-foo', v,f, provides=prov1)
        provMap = db.getTrovesWithProvides([prov1, prov2])
        assert(sorted(x[0] for x in provMap[prov1]) == ['test1','test1-foo'])
        assert([x[0] for x in provMap[prov2]] == ['test2'])

    def testResolveUsingLatest(self):
        myDep = deps.parseDep('trove: test:runtime')
        self.addQuickTestComponent('prov:runtime', '1.0-1-1', '!builddocs', 
                                   provides=myDep)
        self.addQuickTestComponent('prov:runtime', '2.0-1-1', '~!builddocs', 
                                   provides=myDep)
        self.addQuickTestComponent('req:runtime', '2.0-1-1', '~!builddocs', 
                                   requires=myDep)
        oldFlavor = self.cfg.flavor
        try:
            self.cfg.flavor = [ deps.parseFlavor('!builddocs') ]
            rc, msg = self.captureOutput(self.updatePkg, 
                                         self.cfg.root, 'req:runtime')
            assert(msg == "1 additional trove is needed:\n"
                          "    prov:runtime=2.0-1-1 is required by:\n"
                          "       req:runtime=2.0-1-1\n")
        finally:
            self.cfg.flavor = oldFlavor

    def testMultDepsResolvedByOneTrove(self):
        # test multiple dependencies resolved by the same trove - make sure 
        # there's no duplicate information given
        myDep = deps.parseDep('trove: test:runtime trove: test:foo')
        self.addQuickTestComponent('prov:runtime', '2.0-1-1', '~!builddocs', 
                                   provides=myDep)
        self.addQuickTestComponent('req:runtime', '2.0-1-1', '~!builddocs', 
                                   requires=myDep)
        rc, msg = self.captureOutput(self.updatePkg, 
                                         self.cfg.root, 'req:runtime')
        assert(msg == "1 additional trove is needed:\n"
                      "    prov:runtime=2.0-1-1 is required by:\n"
                      "       req:runtime=2.0-1-1\n")


    def testUpdateWithCapabilityDeps(self):
        myDep = deps.parseDep('trove: test:python(1.0)')

        self.addComponent('test:python', '1.0-1-1', provides=myDep,
                          filePrimer=1)
        self.addComponent('test:runtime', '1.0-1-1', requires=myDep,
                          filePrimer=2)
        self.addCollection('test', '1.0-1-1', ['test:python', 'test:runtime'])
        self.updatePkg(self.rootDir, 'test')

        self.addComponent('test:python', '1.0-2-1', provides=myDep,
                          filePrimer=1)
        self.addComponent('test:runtime', '1.0-2-1', requires=myDep,
                          filePrimer=2)
        self.addCollection('test', '1.0-2-1', ['test:python', 'test:runtime'])

        self.updatePkg(self.rootDir, 'test')

    def testGroupResolution(self):
        self.addComponent('test:runtime', '1.0')
        self.addComponent('test:runtime', '2.0')
        trv = self.addCollection('group-dist', '1.0', ['test:runtime'])

        self.addComponent('foo:runtime', '1.0', requires='trove: test:runtime')

        self.checkUpdate('foo:runtime', ['test:runtime=2.0',
                                         'foo:runtime=1.0'], resolve=True)

        self.checkUpdate('foo:runtime', ['test:runtime=1.0',
                                         'foo:runtime=1.0'], resolve=True,
                         resolveGroupList=[('group-dist', None, None)])

    def testGroupResolutionFails(self):
        self.addComponent('test:runtime', '1.0',
                          provides='trove:test:runtime(1.0)')
        self.addCollection('test', '1.0', [':runtime'])
        self.addComponent('test:runtime', '2.0',
                          provides='trove:test:runtime(2.0)')
        self.addCollection('test', '2.0', [':runtime'])
        trv = self.addCollection('group-dist', '1.0', ['test'])

        self.addComponent('foo:runtime', '1.0', 
                          requires='trove: test:runtime(2.0)')

        self.checkUpdate('foo:runtime', ['test:runtime=2.0',
                                         'test=2.0',
                                         'foo:runtime=1.0'], resolve=True)

        try:
            self.checkUpdate('foo:runtime', ['test:runtime=2.0',
                                             'test=2.0',
                                             'foo:runtime=1.0'], resolve=True,
                             resolveGroupList=[('group-dist', None, None)])
        except Exception, err:
            pass
        else:
            assert 0, 'This test should fail because test 2.0 is not in the group'

    def testDepResolutionResultFormat(self):
        # test result formats in case dep resolution fails
        repos = self.openRepository()
        test1r = self.addComponent('test:runtime', '1.0')
        test1 = self.addCollection('test', '1.0', [':runtime'])
        test2r = self.addComponent('test:runtime', '2.0')
        test2 = self.addCollection('test', '2.0', [':runtime'])
        grp1 = self.addCollection('group-dist', '1.0', [('test', '1.0')])
        grp2 = self.addCollection('group-dist', '2.0', [('test', '2.0')])
        
        # test a match
        dep = deps.parseDep('trove:test:runtime')
        sugg = repos.resolveDependenciesByGroups([grp1], [ dep ])
        self.assertEqual(sugg[dep], [[test1r.getNameVersionFlavor()]])
        sugg = repos.resolveDependenciesByGroups([grp2], [ dep ])
        self.assertEqual(sugg[dep], [[test2r.getNameVersionFlavor()]])
        
        # test misses
        dep = deps.parseDep('trove:foo:runtime')
        sugg = repos.resolveDependenciesByGroups([grp1], [ dep ])
        self.assertEqual(sugg[dep], [ [] ])
        dep = deps.parseDep('trove:foo:runtime trove:bar:runtime')
        sugg = repos.resolveDependenciesByGroups([grp1], [ dep ])
        self.assertEqual(sugg[dep], [ [], [] ])

        # test hits and misses
        dep = deps.parseDep('trove:foo:runtime trove:test:runtime')
        sugg = repos.resolveDependenciesByGroups([grp1], [ dep ])
        self.assertEqual(sugg[dep], [ [], [test1r.getNameVersionFlavor()] ])
        dep = deps.parseDep('trove:foo:runtime trove:test:runtime trove:bar:runtime')
        sugg = repos.resolveDependenciesByGroups([grp2], [ dep ])
        self.assertEqual(sugg[dep], [ [], [], [test2r.getNameVersionFlavor()] ])

        # test a more complex case
        dep1 = deps.parseDep('trove:test:runtime')
        dep2 = deps.parseDep('trove:foo:runtime')
        dep3 = deps.parseDep('trove:foo:runtime trove:test:runtime')
        sugg = repos.resolveDependenciesByGroups([grp1, grp2], [ dep1, dep2, dep3 ])
        # tehse should return the latest deps from grp2
        self.assertEqual(sugg[dep1], [[test2r.getNameVersionFlavor()]])
        self.assertEqual(sugg[dep2], [ [] ])
        self.assertEqual(sugg[dep3], [ [], [test2r.getNameVersionFlavor()] ])
    def testDepResolutionResultFormat2(self):
        #CNY-2254
        self.addComponent('foo:python', '1', provides='python: foo(2.4)')
        self.addComponent('conary-policy:lib', '1')
        repos = self.openRepository()
        depSet = deps.parseDep('trove: conary-policy:lib python: foo')
        results = repos.resolveDependencies(self.cfg.buildLabel, [depSet])
        for idx, (depClass, dep) in enumerate(depSet.iterDeps(sort=True)):
            sol, = results[depSet][idx]
            solName = sol[0]
            if dep.name == 'conary-policy:lib':
                self.assertEquals(solName, 'conary-policy:lib')
            else:
                self.assertEquals(solName, 'foo:python')

    def testGroupResolutionAcrossRepos(self):
        self.openRepository()
        repos = self.openRepository(1)
        self.addComponent('test:runtime', '1.0', '!ssl')
        self.addComponent('test:runtime', '2.0', '!ssl')
        self.addComponent('test:runtime', '/localhost1@rpl:foobar/1.0', 'ssl')
        self.addComponent('test:runtime', '/localhost1@rpl:foobar/2.0', 'ssl')

        trv = self.addCollection('group-dist', '1.0',
                    [('test:runtime', '1.0', '!ssl'),
                     ('test:runtime', '/localhost1@rpl:foobar/1.0', 'ssl')])

        sugg = repos.resolveDependenciesByGroups([trv],
                                      [deps.parseDep('trove:test:runtime')])
        troves = sugg[deps.parseDep('trove:test:runtime')][0]
        troveVers = set((x[1].getHost(), x[1].trailingRevision().getVersion())
                        for x in troves)
        assert(troveVers == set([('localhost', '1.0'), ('localhost1', '1.0')]))
        # this doesn't match anything.  Make sure there's a spot in the suggestion list.
        sugg = repos.resolveDependenciesByGroups([trv],
                                      [deps.parseDep('trove:foo:runtime trove:bar:runtime')])
        resultList,  = sugg.values()
        self.assertEqual(resultList, [[], []])

    def testResolveAgainstTipFirst(self):
        self.addComponent('foo:run', '1', filePrimer=0,
                            requires='trove:bar:run(1.0)')
        self.addComponent('bar:run', '1-1-1', provides='trove:bar:run(1.0)', 
                          filePrimer=1)
        self.addComponent('bar:run', '1-1-2', provides='trove:bar:run', 
                          filePrimer=1)
        branchBar = self.addComponent('bar:run', ':branch/1',
                                      provides='trove:bar:run(1.0)', 
                                      filePrimer=1)
        self.cfg.installLabelPath.append(Label('localhost@rpl:branch'))
        self.checkUpdate('foo:run', ['foo:run', 'bar:run=:branch'],
                         resolve=True)
        # make the thing on :branch that provides the dep not be on tip either
        branchBar = self.addComponent('bar:run', ':branch/1-1-2',
                                      filePrimer=1)
        self.checkUpdate('foo:run', ['foo:run', 'bar:run=:linux'],
                         resolve=True)
        # now check when they're both on tip
        self.addComponent('bar:run', '1-1-3', provides='trove:bar:run(1.0)', 
                          filePrimer=1)
        branchBar = self.addComponent('bar:run', ':branch/3',
                                      provides='trove:bar:run(1.0)', 
                                      filePrimer=1)
        self.checkUpdate('foo:run', ['foo:run', 'bar:run=:linux'],
                         resolve=True)


    @context('resolvelog')
    def testResolutionLogging(self):
        myDep= ('soname: ELF32/a.so(x86)'
                ' soname: ELF32/b.so(x86)'
                ' soname: ELF32/c.so(x86)'
                ' soname: ELF32/d.so(x86)'
                ' soname: ELF32/e.so(x86)'
                ' soname: ELF32/f.so(x86)')
        log.setVerbosity(log.DEBUG)
        self.addComponent('prov:runtime', '1.0', provides=myDep, filePrimer=1)
        self.addComponent('req:runtime', '1.0', requires=myDep, filePrimer=2)

        self.logFilter.add()
        self.checkUpdate(['req:runtime'], ['req:runtime', 'prov:runtime'],
                         resolve=True)
        self.logFilter.remove()
        self.logFilter.compare('''\
+ Resolved:
    req:runtime=localhost@rpl:linux/1.0-1-1[]
    Required:  soname: ELF32/a.so(x86)
               soname: ELF32/b.so(x86)
               soname: ELF32/c.so(x86)
               soname: ELF32/d.so(x86)
               soname: ELF32/e.so(x86)
               ...
    Adding: prov:runtime=localhost@rpl:linux/1.0-1-1[]''')

    @context('resolvelog')
    def testResolutionKeepExistingLogging(self):
        self.addComponent('keepprov:runtime', '1.0',
                           provides='trove:keepreq(1.0)',
                          filePrimer=3)
        self.addComponent('keepprov:runtime', '2.0',
                          provides='trove:keepreq(2.0)',
                          filePrimer=4)
        self.addComponent('keepreq1:runtime', '1.0',
                          requires='trove:keepreq(1.0)',
                          filePrimer=5)
        self.addComponent('keepreq2:runtime', '1.0',
                          requires='trove:keepreq(2.0)',
                          filePrimer=6)
        self.updatePkg(['keepprov:runtime=1.0', 'keepreq1:runtime'])
        log.setVerbosity(log.DEBUG)
        self.logFilter.add()
        self.checkUpdate('keepreq2:runtime',
                         ['keepreq2:runtime', 'keepprov:runtime=--2.0'],
                        resolve=True, keepRequired=True)

        self.logFilter.compare([
'''+ Resolved:
    keepreq2:runtime=localhost@rpl:linux/1.0-1-1[]
    Required:  trove: keepreq(2.0)
    Adding: keepprov:runtime=localhost@rpl:linux/2.0-1-1[]''',
'''+ Update breaks dependencies!''',
'''+ Broken dependency: (dep needed by the system but being removed):
   keepreq1:runtime=1.0-1-1
   Requires:
     trove: keepreq(1.0)
   Provided by removed or updated packages: keepprov:runtime''',
'''+ Attempting to update the following packages in order to remove their dependency on something being erased:
   keepreq1''',
'''+ Resolved (installing side-by-side)
    keepreq1:runtime=1.0-1-1[]    Required: trove: keepreq(1.0)    Keeping: keepprov:runtime=1.0-1-1[]''',
'warning: keeping keepprov:runtime - required by at least keepreq1:runtime'])

    @context('resolvelog')
    def testResolutionKeepExistingLogging2(self):
        self.addComponent('keepprov:lib', '1.0',
                           provides='trove:keepreq(1.0)',
                          filePrimer=3)
        self.addCollection('keepprov', '1.0', [':lib'])

        self.addComponent('keepprov:data', '2.0', filePrimer=4) 
        # they conflict!
        self.addCollection('keepprov', '2.0', [':data'])
        self.addComponent('keepreq:runtime', '1.0',
                          requires='trove:keepreq(1.0)',
                          filePrimer=5)
        self.updatePkg(['keepprov=1.0', 'keepreq:runtime'])
        log.setVerbosity(log.DEBUG)
        self.logFilter.add()
        self.checkUpdate('keepprov', ['keepprov=--2.0',
                                      'keepprov:data=--2.0'], keepRequired=True)
        self.logFilter.compare([
'''+ Update breaks dependencies!''',
'''+ Broken dependency: (dep needed by the system but being removed):
   keepreq:runtime=1.0-1-1
   Requires:
     trove: keepreq(1.0)
   Provided by removed or updated packages: keepprov:lib''',
'''+ Resolved (undoing erasure)
    keepreq:runtime=1.0-1-1[]    Required: trove: keepreq(1.0)    Keeping: keepprov:lib=1.0-1-1[]''',
'''warning: keeping keepprov:lib - required by at least keepreq:runtime'''
])

    @context('resolvelog')
    def testResolutionUpdateExistingLogging(self):
        self.addComponent('foo:runtime', '1.0',
                          provides='trove:foo(1.0)',
                          filePrimer=7)
        self.addComponent('foo-build:runtime', '1.0',
                          requires='trove:foo(1.0)',
                          filePrimer=8)
        self.addComponent('foo:runtime', '2.0',
                        provides='trove:foo(2.0)',
                        filePrimer=9)
        self.addComponent('foo-build:runtime', '2.0',
                          requires='trove:foo(2.0)',
                          filePrimer=10)
        self.addCollection('foo-build', '2.0', [':runtime'])
        self.updatePkg(['foo:runtime=1.0', 'foo-build:runtime=1.0'])
        log.setVerbosity(log.DEBUG)
        self.logFilter.add()
        self.checkUpdate('foo:runtime',
                         ['foo:runtime=1.0--2.0', 'foo-build=2.0',
                          'foo-build:runtime=1.0--2.0'])
        self.logFilter.compare([
'''+ Update breaks dependencies!''',
'''+ Broken dependency: (dep needed by the system but being removed):
   foo-build:runtime=1.0-1-1
   Requires:
     trove: foo(1.0)
   Provided by removed or updated packages: foo:runtime''',
'''+ Attempting to update the following packages in order to remove their dependency on something being erased:
   foo-build''',
'''+ updated 2 troves:
   foo-build:runtime=localhost@rpl:linux/2.0-1-1[]
   foo-build=localhost@rpl:linux/2.0-1-1[]'''])

    def testKeepRequiredWithOverlappingFiles(self):
        # Test that keepRequired will function when an old trove overlaps with
        # a new trove providing that the old trove had the overlapping files
        # removed previously.
        prov1 = deps.parseDep('file: /lib/libfoo.so.1')
        prov2 = deps.parseDep('file: /lib/libfoo.so.2')
        self.addComponent('foo:lib=1.0', provides = prov1,
                          fileContents = [ ('/lib/libfoo.so.1', '1'),
                                           ('/etc/conflict', 'contents') ] )
        self.addComponent('foo:lib=2.0', provides = prov2,
                          fileContents = [ ('/lib/libfoo.so.2', '2'),
                                           ('/etc/conflict', 'contents') ] )
        self.addComponent('bar:runtime=1.0', requires = prov1,
                          fileContents = [ ('/bin/bar1', '1') ] )
        self.addComponent('bar:runtime=2.0', requires = prov2,
                          fileContents = [ ('/bin/bar2', '2') ] )

        self.updatePkg([ 'foo:lib=1.0', 'bar:runtime=1.0'])

        (rc, str) = self.captureOutput(self.updatePkg, 'bar:runtime=2.0',
                                       resolve = True, keepExisting = True,
                                       keepRequired = True)
        expectedStr = """\
The following dependencies would not be met after this update:

  bar:runtime=1.0-1-1 (Already installed) requires:
    file: /lib/libfoo.so.1
  which is provided by:
    foo:lib=1.0-1-1 (Would be updated to 2.0-1-1)
"""
        assert(str == expectedStr)

        self.removeFile(self.rootDir, '/etc/conflict')

        (rc, str) = self.captureOutput(self.updatePkg, 'bar:runtime=2.0',
                                       resolve = True, keepExisting = True,
                                       keepRequired = True)
        assert(str == 
            'Including extra troves to resolve dependencies:\n'
            '    foo:lib=2.0-1-1\n'
            'warning: keeping foo:lib - required by at least bar:runtime\n')
