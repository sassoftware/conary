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


from conary_test import rephelp

import os

from conary_test.cvctest.buildtest import policytest

from conary import versions
from conary.build import action, trovefilter
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import util


class PackageRecipeTest(rephelp.RepositoryHelper):
    def touch(self, fn):
        d = os.path.dirname(fn)
        if not os.path.exists(fn):
            util.mkdirChain(d)
            f = open(fn, 'w')
            f.write('')
            f.close()

    def getRecipe(self):
        return policytest.DummyRecipe(self.cfg)

    def getGlob(self, pattern, extraMacros = {}):
        recipe = self.getRecipe()
        recipe.macros.update(extraMacros)
        return action.Glob(recipe, pattern)

    def getRegexp(self, pattern):
        return action.Regexp(pattern)

    def testGlobBasics(self):
        ref = '^foo$'
        glob = self.getGlob('foo')
        self.assertEquals(glob(), ref)
        self.assertEquals(repr(glob), "Glob('foo')")
        self.assertEquals(str(glob), "Glob('foo')")
        self.assertEquals(ref, glob)
        self.assertEquals(hash(ref), hash(glob))
        self.assertEquals(glob, self.getGlob('foo'))
        self.assertEquals(glob, self.getRegexp(ref))

        glob = self.getGlob('foo*')
        self.assertEquals(glob(), '^foo[^/]*$')
        self.assertEquals(repr(glob), "Glob('foo*')")
        self.assertEquals(str(glob), "Glob('foo*')")

        glob = self.getGlob('f?o')
        self.assertEquals(glob(), '^f[^/]o$')

        glob = self.getGlob('foo.*')
        self.assertEquals(glob(), '^foo\\.[^/]*$')

        glob = self.getGlob('%(datadir)s/foo', {'datadir': '/usr/share'})
        self.assertEquals(glob(), '^\\/usr\\/share\\/foo$')
        self.assertEquals(repr(glob), "Glob('%(datadir)s/foo')")
        self.assertEquals(str(glob), "Glob('%%(datadir)s/foo')")

        glob = self.getGlob('%(datadir)s/?[!foo|bar][^baz][]', {'datadir': '/usr/share'})
        self.assertEquals(glob(), \
                '^\\/usr\\/share\\/[^/][^foo|bar][\\^baz]\\[\\]$')

    def testRegexpBasics(self):
        exp = '^foo$'
        reg = self.getRegexp(exp)
        self.assertEquals(reg, exp)
        self.assertEquals(reg, self.getRegexp(exp))
        self.assertEquals(reg, self.getGlob('foo'))
        self.assertEquals(str(reg), "Regexp('^foo$')")
        self.assertEquals(repr(reg), "Regexp('^foo$')")

    def testExpandPaths(self):
        recipe = self.getRecipe()
        tmpDir = recipe.macros.destdir
        fn = os.path.join(tmpDir, 'foo')
        self.touch(fn)
        reg = self.getRegexp('/foo')
        paths = action._expandPaths([reg], recipe.macros)
        self.assertEquals([fn], paths)
        paths = action._expandPaths(['/*'], recipe.macros)
        self.assertEquals([fn], paths)
        glob = self.getGlob('/f?o')
        paths = action._expandPaths([glob], recipe.macros)
        self.assertEquals([fn], paths)

    def testExpandGlobsWithDirs(self):
        recipe = self.getRecipe()
        tmpDir = recipe.macros.destdir
        goodFn = os.path.join(tmpDir, 'foo.bar')
        badFn = os.path.join(tmpDir, 'foo', 'bar')
        self.touch(goodFn)
        self.touch(badFn)
        glob = self.getGlob('/foo*')
        paths = action._expandPaths([glob], recipe.macros)
        self.assertEquals(sorted([goodFn, os.path.dirname(badFn)]), sorted(paths))
        glob = self.getGlob('/foo?*')
        paths = action._expandPaths([glob], recipe.macros)
        self.assertEquals([goodFn], paths)
        glob = self.getGlob('/foo?bar')
        paths = action._expandPaths([glob], recipe.macros)
        self.assertEquals([goodFn], paths)

    def testGlobExcludeDirs(self):
        recipeStr = """\
class TestGlob(PackageRecipe):
    name = 'testglob'
    version = '1.0.0'

    clearBuildRequires()

    def setup(r):
        r.MakeDirs('%(datadir)s/foo')
        r.ExcludeDirectories(exceptions = r.glob('%(datadir)s/*'))
"""

        built, d = self.buildRecipe(recipeStr, "TestGlob")
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'testglob:data')

    def testRegexpConfig(self):
        recipeStr = """\
class TestRegexp(PackageRecipe):
    name = 'testregexp'
    version = '1.0.0'

    clearBuildRequires()

    def setup(r):
        r.Create('/var/foo')
        r.Config(r.regexp('/var/.*'))
        r.Create('%(datadir)s/foo')
        r.Config(r.regexp('%(datadir)s/.*'))
"""

        built, d = self.buildRecipe(recipeStr, "TestRegexp")
        self.assertEquals(len(built), 2)
        self.assertEquals(sorted(x[0] for x in built),
            ['testregexp:data', 'testregexp:runtime'])

        repos = self.openRepository()
        for nvf in built:
            trvNVF = repos.findTrove(None, nvf)
            trv = repos.getTrove(*trvNVF[0])
            for fileInfo in trv.iterFileList():
                fileObj = repos.getFileVersion(fileInfo[0],
                        fileInfo[2], fileInfo[3])
                self.assertEquals(fileObj.flags.isConfig(), 1)

    def testGlobDoc(self):
        recipeStr = """\
class TestRegexp(PackageRecipe):
    name = 'testregexp'
    version = '1.0.0'

    clearBuildRequires()

    def setup(r):
        r.Create('foo', contents = 'foo')
        r.Doc(r.glob('f?o'))
"""
        built, d = self.buildRecipe(recipeStr, "TestRegexp")
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'testregexp:supdoc')

    def testRegexpDoc(self):
        recipeStr = """\
class TestRegexp(PackageRecipe):
    name = 'testregexp'
    version = '1.0.0'

    clearBuildRequires()

    def setup(r):
        r.Create('foo', contents = 'foo')
        r.Doc(r.regexp('f.o'))
"""
        built, d = self.buildRecipe(recipeStr, "TestRegexp")
        self.assertEquals(len(built), 1)
        self.assertEquals(built[0][0], 'testregexp:supdoc')

class TroveFilterTest(rephelp.RepositoryHelper):
    def setUp(self):
        rephelp.RepositoryHelper.setUp(self)

    def getRecipe(self):
        return policytest.DummyRecipe(self.cfg)

    @testhelp.context('trove-filter')
    def testTroveFilterBasics(self):
        recipe = self.getRecipe()
        filt = trovefilter.TroveFilter(recipe,
                'foo', version = 'test.rpath.local@rpl:devel')
        nvf = cmdline.parseTroveSpec('foo=test.rpath.local@rpl:devel')
        self.assertEquals(filt.match((nvf,)), True)
        nvf = cmdline.parseTroveSpec('foo=foo.rpath.local@rpl:devel')
        self.assertEquals(filt.match((nvf,)), False)
        filt = trovefilter.TroveFilter(recipe,
                'foo', version = '/test.rpath.local@rpl:devel')
        nvf = cmdline.parseTroveSpec('foo=/test.rpath.local@rpl:devel')
        self.assertEquals(filt.match((nvf,)), True)
        nvf = cmdline.parseTroveSpec('foo=/foo.rpath.local@rpl:devel')
        self.assertEquals(filt.match((nvf,)), False)
        filt = trovefilter.TroveFilter(recipe,
                'foo', version = '/test.rpath.local@rpl:devel/1-1-1')
        nvf = cmdline.parseTroveSpec('foo=/test.rpath.local@rpl:devel/1-1-1')
        self.assertEquals(filt.match((nvf,)), True)
        nvf = cmdline.parseTroveSpec('foo=/foo.rpath.local@rpl:devel/1-1-1')
        self.assertEquals(filt.match((nvf,)), False)

    @testhelp.context('trove-filter')
    def testBadTroveFilters(self):
        recipe = self.getRecipe()
        filt = trovefilter.AbstractFilter()
        self.assertRaises(NotImplementedError, filt.match)

        try:
            filt = trovefilter.TroveFilter(recipe, 'foo(')
        except RuntimeError, e:
            self.assertEquals(str(e), "Bad Regexp: 'foo(' for name")
        else:
            self.fail("Expected RuntimeError")

        nvf = cmdline.parseTroveSpec('foo=/test.rpath.local@rpl:devel')
        filt = trovefilter.TroveFilter(recipe, 'foo')
        self.assertEquals(filt.match((nvf,)), True)
        filt.compile()
        filt.versionType = True
        filt.version = 'foo'
        self.assertEquals(filt.match((nvf,)), False)

    @testhelp.context('trove-filter')
    def testTroveFilterVersion(self):
        recipe = self.getRecipe()
        filt = trovefilter.TroveFilter(recipe,
                'foo', version = 'test.rpath.local@rpl:linux')
        filt2 = trovefilter.TroveFilter(recipe,
                'bar', version = 'test.rpath.local@rpl:linux')
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)
        self.assertEquals(filt2.match((nvf,)), False)
        filt = trovefilter.TroveFilter(recipe,
                'foo', version = '/test.rpath.local@rpl:linux')
        self.assertEquals(filt.match((nvf,)), True)
        filt = trovefilter.TroveFilter(recipe,
                'foo', version = '/test.rpath.local@rpl:linux/1-1-1')
        self.assertEquals(filt.match((nvf,)), True)
        filt = trovefilter.TroveFilter(recipe,
                'foo', version = 'test.rpath.local@rpl:linux')
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:devel/1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), False)
        filt = trovefilter.TroveFilter(recipe,
                'foo', version = '/test.rpath.local@rpl:linux')
        self.assertEquals(filt.match((nvf,)), False)
        filt = trovefilter.TroveFilter(recipe,
                'foo', version = '/test.rpath.local@rpl:linux/1-1-1')
        self.assertEquals(filt.match((nvf,)), False)

    @testhelp.context('trove-filter')
    def testTroveFilterMacros(self):
        recipe = self.getRecipe()
        recipe.macros.name = 'foo'
        # test a macro in the name element
        filt = trovefilter.TroveFilter(recipe,
                '%(name)s', version = 'test.rpath.local@rpl:linux')
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)

        # test a macro in the version element
        filt = trovefilter.TroveFilter(recipe,
                '%(name)s', version = '%(name)s.rpath.local@rpl:linux')
        nvf = ('foo', versions.VersionFromString( \
                '/foo.rpath.local@rpl:linux/1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)

    @testhelp.context('trove-filter')
    def testTroveFilterRegexps(self):
        recipe = self.getRecipe()
        recipe.macros.name = 'foo'
        # test a regexp in the name element
        filt = trovefilter.TroveFilter(recipe,
                '%(name)s1+', version = 'test.rpath.local@rpl:linux')
        nvf = ('foo11', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)

        # test that name regexp is anchored
        nvf = ('foo113', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), False)

    @testhelp.context('trove-filter')
    def testTroveFilterFlavors(self):
        recipe = self.getRecipe()
        filt = trovefilter.TroveFilter(recipe, flavor = 'xen,domU')
        filt2 = trovefilter.TroveFilter(recipe, name = 'bar',
                flavor = 'xen,domU')
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), False)
        self.assertEquals(filt2.match((nvf,)), False)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                deps.parseFlavor('xen,domU is: x86'))
        self.assertEquals(filt.match((nvf,)), True)
        self.assertEquals(filt2.match((nvf,)), False)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                deps.parseFlavor('xen,domU is: x86_64'))
        self.assertEquals(filt.match((nvf,)), True)
        self.assertEquals(filt2.match((nvf,)), False)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                'xen,domU is: x86_64')
        self.assertEquals(filt.match((nvf,)), True)
        self.assertEquals(filt2.match((nvf,)), False)

    @testhelp.context('trove-filter')
    def testTroveFilterFlavors2(self):
        recipe = self.getRecipe()
        filt1 = trovefilter.TroveFilter(recipe, flavor = 'xen,domU is: x86')
        filt2 = trovefilter.TroveFilter(recipe, flavor = 'xen,domU is: x86_64')
        filt3 = trovefilter.TroveFilter(recipe,
                flavor = 'xen,domU is: x86_64 x86')
        nvf1 = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                deps.parseFlavor('is: x86'))
        nvf2 = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                deps.parseFlavor('xen,domU is: x86'))
        nvf3 = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                deps.parseFlavor('xen,domU is: x86_64'))
        nvf4 = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                deps.parseFlavor('xen,domU is: x86 x86_64'))
        self.assertEquals(filt1.match((nvf1,)), False)
        self.assertEquals(filt1.match((nvf2,)), True)
        self.assertEquals(filt2.match((nvf2,)), False)
        self.assertEquals(filt2.match((nvf3,)), True)
        self.assertEquals(filt2.match((nvf4,)), False)
        self.assertEquals(filt3.match((nvf4,)), True)
        self.assertEquals(filt3.match((nvf1,)), False)
        self.assertEquals(filt3.match((nvf2,)), False)
        self.assertEquals(filt3.match((nvf3,)), False)

    @testhelp.context('trove-filter')
    def testTroveFilterFlavors3(self):
        recipe = self.getRecipe()
        filt1 = trovefilter.TroveFilter(recipe, flavor = 'xen,domU is: x86')
        filt2 = trovefilter.TroveFilter(recipe,
                    flavor = 'xen,domU is: x86(sse, sse2, 486, 586, 686)')
        nvf1 = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                deps.parseFlavor('xen,domU is: x86'))
        nvf2 = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                deps.parseFlavor('xen,domU is: x86(sse, sse2, 486, 586, 686)'))
        self.assertEquals(filt1.match((nvf1,)), True)
        self.assertEquals(filt2.match((nvf2,)), True)
        # most important test. x86 filter matches x86(sse)
        self.assertEquals(filt1.match((nvf2,)), True)
        self.assertEquals(filt2.match((nvf1,)), False)

    @testhelp.context('trove-filter')
    def testTroveFilterFlavors4(self):
        recipe = self.getRecipe()
        filt1 = trovefilter.TroveFilter(recipe, flavor = 'xen,domU is: sparc')
        filt2 = trovefilter.TroveFilter(recipe,
                    flavor = 'xen,domU is: x86(sse, sse2, 486, 586, 686)')
        filt3 = trovefilter.TroveFilter(recipe, flavor = 'xen,domU')
        nvf1 = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                deps.parseFlavor('xen,domU is: sparc'))
        nvf2 = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'),
                deps.parseFlavor('xen,domU is: x86(sse, sse2, 486, 586, 686)'))
        self.assertEquals(filt1.match((nvf1,)), True)
        self.assertEquals(filt2.match((nvf2,)), True)
        self.assertEquals(filt1.match((nvf2,)), False)
        self.assertEquals(filt2.match((nvf1,)), False)
        self.assertEquals(filt3.match((nvf1,)), True)

    @testhelp.context('trove-filter')
    def testTroveFilterNoVersion(self):
        recipe = self.getRecipe()
        filt = trovefilter.TroveFilter(recipe, name = 'foo')
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:devel/1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)

    @testhelp.context('trove-filter')
    def testTroveFilterRevision(self):
        recipe = self.getRecipe()
        filt = trovefilter.TroveFilter(recipe, version = '1.1-1-1')
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1-1-2'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), False)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)

        filt = trovefilter.TroveFilter(recipe, version = '1.1-1')
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-1-2'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-2'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), False)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.2-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), False)

        filt = trovefilter.TroveFilter(recipe, version = '1.1')
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-1-2'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.2-1-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), False)

        filt = trovefilter.TroveFilter(recipe, version = '')
        self.assertEquals(filt.match((nvf,)), True)

    @testhelp.context('trove-filter')
    def testTroveFilterBlank(self):
        recipe = self.getRecipe()
        filt = trovefilter.TroveFilter(recipe)
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)
        nvf = ('bar', versions.VersionFromString( \
                '/test.rpath.local@rpl:devel/1.0-6-4'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)

    @testhelp.context('trove-filter')
    def testTroveFilterNot(self):
        recipe = self.getRecipe()
        filt = -trovefilter.TroveFilter(recipe, name = 'foo')
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), False)
        nvf = ('bar', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)

        filt = ~trovefilter.TroveFilter(recipe, name = 'foo')
        nvf = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), False)
        nvf = ('bar', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-1'), deps.parseFlavor(''))
        self.assertEquals(filt.match((nvf,)), True)

    @testhelp.context('trove-filter')
    def testTroveFilterOr(self):
        recipe = self.getRecipe()
        filt1 = trovefilter.TroveFilter(recipe, name = 'foo')
        filt2 = trovefilter.TroveFilter(recipe, name = 'group-foo')
        filt3 = filt1 - filt2
        filt4 = filt1 + - filt2
        filt5 = filt2 - filt1
        filt6 = filt2 | filt1

        nvf1 = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-1'), deps.parseFlavor(''))
        nvf2 = ('group-foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-1'), deps.parseFlavor(''))
        self.assertEquals(filt3.match((nvf1, nvf2)), True)
        self.assertEquals(filt3.match((nvf2,)), False)
        self.assertEquals(filt4.match((nvf1, nvf2)), True)
        self.assertEquals(filt5.match((nvf1, nvf2)), True)
        self.assertEquals(filt5.match((nvf1,)), False)
        self.assertEquals(filt6.match((nvf1, nvf2)), True)

    @testhelp.context('trove-filter')
    def testTroveFilterAnd(self):
        recipe = self.getRecipe()
        filt1 = trovefilter.TroveFilter(recipe, name = 'foo')
        filt2 = trovefilter.TroveFilter(recipe, name = 'group-foo')
        filt3 = filt1 * -filt2
        filt4 = filt2 * -filt1
        filt5 = filt2 & filt1
        filt6 = filt2 & ~filt1

        nvf1 = ('foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-1'), deps.parseFlavor(''))
        nvf2 = ('group-foo', versions.VersionFromString( \
                '/test.rpath.local@rpl:linux/1.1-2-1'), deps.parseFlavor(''))
        self.assertEquals(filt3.match((nvf1, nvf2)), False)
        self.assertEquals(filt3.match((nvf2,)), False)
        self.assertEquals(filt3.match((nvf1,)), True)
        self.assertEquals(filt4.match((nvf1, nvf2)), False)
        self.assertEquals(filt4.match((nvf1,)), False)
        self.assertEquals(filt4.match((nvf2,)), True)
        self.assertEquals(filt5.match((nvf1, nvf2)), True)
        self.assertEquals(filt5.match((nvf1,)), False)
        self.assertEquals(filt5.match((nvf2,)), False)
        self.assertEquals(filt6.match((nvf1, nvf2)), False)

    @testhelp.context('trove-filter')
    def testTroveFilterEquality(self):
        recipe = self.getRecipe()
        filt1 = trovefilter.TroveFilter(recipe, name = 'foo')
        filt2 = trovefilter.TroveFilter(recipe, name = 'group-foo')
        self.assertNotEquals(filt1, filt2)

        filt1 = trovefilter.TroveFilter(recipe, name = 'foo')
        filt2 = trovefilter.TroveFilter(recipe, name = 'foo')
        self.assertEquals(filt1, filt2)

        filt1 = trovefilter.TroveFilter(recipe, name = 'foo',
                version = 'c.r.c@rpl:linux')
        filt2 = trovefilter.TroveFilter(recipe, name = 'foo')
        self.assertNotEquals(filt1, filt2)

        filt1 = trovefilter.TroveFilter(recipe, name = 'foo',
                version = 'c.r.c@rpl:linux')
        filt2 = trovefilter.TroveFilter(recipe, name = 'foo',
                version = 'c.r.c@rpl:linux')
        self.assertEquals(filt1, filt2)

        filt1 = trovefilter.TroveFilter(recipe, name = 'foo',
                version = 'c.r.c@rpl:linux')
        filt2 = trovefilter.TroveFilter(recipe, name = 'foo',
                version = '/c.r.c@rpl:linux')
        self.assertNotEquals(filt1, filt2)

        filt1 = trovefilter.TroveFilter(recipe, name = 'foo',
                version = 'c.r.c@rpl:linux', flavor = 'is: x86')
        filt2 = trovefilter.TroveFilter(recipe, name = 'foo',
                version = 'c.r.c@rpl:linux')
        self.assertNotEquals(filt1, filt2)

        filt1 = trovefilter.TroveFilter(recipe, name = 'foo',
                flavor = 'is: x86')
        filt2 = trovefilter.TroveFilter(recipe, name = 'foo',
                flavor = 'is: x86 x86_64')
        self.assertNotEquals(filt1, filt2)

        filt1 = trovefilter.TroveFilter(recipe, name = 'foo',
                version = 'c.r.c@rpl:linux', flavor = 'is: x86')
        filt2 = trovefilter.TroveFilter(recipe, name = 'foo',
                version = 'c.r.c@rpl:linux', flavor = 'is: x86')
        self.assertEquals(filt1, filt2)
