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


import pprint
import re
from testrunner import testhelp

from conary_test import rephelp

from conary import versions
from conary.build import cook
from conary.deps import deps
from conary.build.errors import GroupPathConflicts

VFS = versions.VersionFromString

class GroupSetTest(rephelp.RepositoryHelper):

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

    def checkTroves(self, collection, desiredList):
        refs = sorted(
            ((x[0][0], x[0][1].asString(), str(x[0][2])), x[1], x[2])
                    for x in collection.iterTroveListInfo())
        desiredList = sorted(desiredList)
        if refs != desiredList:
            self.fail("Expected:\n%s\nActual:\n%s\n" % (
                pprint.pformat(desiredList),
                pprint.pformat(refs)))

    def _build(self, *coreList):
        recipe = (
"""
class TestRecipe(GroupSetRecipe):
    name = "group-test"
    version = "1.0"
    clearBuildReqs()

    def setup(r):
        world = r.Repository(r.labelPath[0], r.flavor)
%s
""" % "\n".join(["        %s" % x for x in coreList ]))
        grp = self.build(recipe, "TestRecipe")

        repos = self.openRepository()
        return repos.getTrove('group-test', grp.getVersion(), grp.getFlavor())

    @testhelp.context('sysmodel')
    def testInstallAndOptional(self):
        self.addComponent('foo:runtime=1.0')
        self.addComponent('foo:doc=1.0')
        self.addComponent('foo:lib=1.0')
        self.addComponent('bar:runtime=1.0')
        grp = self._build(
            'r.Group(world.find("foo:doc").makeOptional())')
        self.checkTroves(grp, [
            (('foo:doc', '/localhost@rpl:linux/1.0-1-1', ''), False, True)
          ])

        grp = self._build(
            'r.Group(world.find("foo:doc").makeOptional().makeInstall())')
        self.checkTroves(grp, [
            (('foo:doc', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

        grp = self._build(
            'doc = world.find("foo:doc").makeOptional()',
            'runtime = world.find("foo:runtime")',
            'r.Group(doc | runtime)')
        self.checkTroves(grp, [
            (('foo:doc', '/localhost@rpl:linux/1.0-1-1', ''), False, True),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

        grp = self._build(
            'doc = world.find("foo:doc").makeOptional()',
            'runtime = world.find("foo:runtime")',
            'both = doc | runtime',
            'r.Group(both.getInstall())')
        self.checkTroves(grp, [
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

        grp = self._build(
            'doc = world.find("foo:doc").makeOptional()',
            'runtime = world.find("foo:runtime")',
            'both = doc | runtime',
            'r.Group(both.getOptional())')
        self.checkTroves(grp, [
            (('foo:doc', '/localhost@rpl:linux/1.0-1-1', ''), False, True)
          ])

        grp = self._build(
            'doc = world.find("foo:doc")',
            'runtime = world.find("foo:runtime")',
            'foo = doc + runtime',
            'r.Group(foo.makeOptional(doc))')

        self.checkTroves(grp, [
            (('foo:doc', '/localhost@rpl:linux/1.0-1-1', ''), False, True),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

        grp = self._build(
            'doc = world.find("foo:doc")',
            'runtime = world.find("foo:runtime")',
            'foo = (doc + runtime).makeOptional()',
            'r.Group(foo.makeInstall(runtime))')

        self.checkTroves(grp, [
            (('foo:doc', '/localhost@rpl:linux/1.0-1-1', ''), False, True),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

        grp = self._build(
            'base = world.find("foo:runtime", "bar:runtime")',
            'new = base.makeOptional(base.find("foo:runtime"))',
            'newer = new.makeOptional(base.find("bar:runtime"))',
            'r.Group(newer)')

        self.checkTroves(grp, [
            (('bar:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, True),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, True)
          ])

    @testhelp.context('sysmodel')
    def testComponents(self):
        self.addComponent('foo:runtime=1.0')
        self.addComponent('foo:lib=1.0')
        self.addCollection('foo=1.0', [ ':runtime', ':lib' ])
        grp = self._build(
            'foo = world["foo"]',
            'lib = foo.flatten().components("lib")',
            'return r.Group(lib)')

        self.checkTroves(grp,
          [ (('foo',     '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('foo:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, False)
          ])

    @testhelp.context('sysmodel')
    def testDependencies(self):
        self.addComponent('foo:runtime=1.0',
                          requires = deps.parseDep('trove: bar:lib'))
        self.addComponent('bar:lib=1.0')
        grp = self._build(
            "runtime = world.find('foo:runtime')",
            "suggestions = runtime.depsNeeded(world)",
            "r.Group(runtime + suggestions)")

        self.checkTroves(grp,
          [ (('bar:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

        # resolve against a TroveSet
        grp = self._build(
            "lib = world.find('bar:lib')",
            "runtime = world.find('foo:runtime')",
            "suggestions = runtime.depsNeeded(lib)",
            "r.Group(runtime + suggestions)")

        self.checkTroves(grp,
          [ (('bar:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

        # test failOnUnresolved
        grp = self._build(
            "emptyWorld = r.Repository('localhost@foo:bar', r.flavor)",
            "runtime = world.find('foo:runtime')",
            "suggestions = runtime.depsNeeded(emptyWorld,",
            "                                failOnUnresolved = False)",
            "r.Group(runtime + suggestions)")

        self.assertRaises(cook.CookError, self._build,
            "emptyWorld = r.Repository('localhost@foo:bar', r.flavor)",
            "runtime = world.find('foo:runtime')",
            "suggestions = runtime.depsNeeded(emptyWorld)",
            "r.Group(runtime + suggestions)")

        # test requires in subgroups
        grp = self._build(
            "runtime = world['foo:runtime']",
            "sub = runtime.createGroup('group-sub')",
            "suggestions = sub.depsNeeded(world)",
            "r.Group(runtime + suggestions)")

        self.checkTroves(grp,
          [ (('bar:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

        # test provides in subgroups
        grp = self._build(
            "emptyWorld = r.Repository('localhost@foo:bar', r.flavor)",
            "lib = world['bar:lib']",
            "sub = lib.createGroup('group-sub')",
            "outer = world['foo:runtime']",
            "suggestions = outer.depsNeeded(sub)",
            "r.Group(outer)")

    @testhelp.context('sysmodel')
    def testDependencyChain(self):
        self.addComponent('a:runtime=1.0',
                          requires = deps.parseDep('trove: a:lib'))
        self.addComponent('a:lib=1.0',
                          requires = deps.parseDep('trove: b:lib'))
        self.addComponent('b:lib=1.0')

        grp = self._build(
            "avail = world.find('a:lib', 'b:lib')",
            "c = world['a:runtime']",
            "s = c.depsNeeded(avail)",
            "return r.Group(c + s)"
        )

        self.checkTroves(grp,
          [ (('a:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('a:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('b:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
          ])

    @testhelp.context('sysmodel')
    def testDependenciesSearchPath(self):
        self.addComponent('foo:lib=1.0')
        self.addCollection('foo=1.0', [ 'foo:lib' ])
        self.addComponent('bar:lib=/localhost@foo:bar/1.0')
        d = deps.parseDep('trove: foo:lib')
        d.union(deps.parseDep('trove: bar:lib'))
        self.addComponent('pkg:runtime', requires = d)

        grp = self._build(
            'pkg = world["pkg:runtime"]',
            'altLabel = r.Repository("localhost@foo:bar", r.flavor)',
            'sp = r.SearchPath(world, altLabel)',
            'return r.Group(pkg + pkg.depsNeeded(sp))')

        self.checkTroves(grp,
          [ (('bar:lib', '/localhost@foo:bar/1.0-1-1', ''), True, True),
            (('foo',     '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('foo:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('pkg:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

        grp = self._build(
            'pkg = world["pkg:runtime"]',
            'foo = world["foo"]',
            'bar = world["bar:lib=localhost@foo:bar"]',
            'altLabel = r.Repository("localhost@foo:bar", r.flavor)',
            'sp = r.SearchPath(foo, bar)',
            'return r.Group(pkg + pkg.depsNeeded(sp))')

        self.checkTroves(grp,
          [ (('bar:lib', '/localhost@foo:bar/1.0-1-1', ''), True, True),
            (('foo', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('foo:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('pkg:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

    @testhelp.context('sysmodel')
    def testDifference(self):
        self.addComponent('foo:runtime=1.0')
        self.addComponent('foo:lib=1.0')
        grp = self._build(
            "both = world.find('foo:runtime', 'foo:lib')",
            "lib = world.find('foo:lib')",
            "runtime = both.difference(lib)",
            "return r.Group(runtime)")

        self.checkTroves(grp,
          [(('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)])

        grp = self._build(
            "both = world.find('foo:runtime', 'foo:lib')",
            "runtime = both - 'foo:lib'",
            "return r.Group(runtime)")

        self.checkTroves(grp,
          [(('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)])

    @testhelp.context('sysmodel')
    def testEmpty(self):
        self.addComponent("foo:runtime=1.0")
        self.addComponent("foo:lib=1.0")
        self.addCollection("foo=1.0", [ ":runtime", ":lib" ])
        grp = self._build(
            "world['foo'].findByName('bar', emptyOkay = True).isEmpty()",
            "r.Group(world['foo'])"
        )

        self.assertRaises(cook.CookError,
                          self._build,
                          "world['foo'].findByName('foo.*').isEmpty()")

        grp = self._build(
            "world['foo'].findByName('foo.*').isNotEmpty()",
            "r.Group(world['foo'])"
        )

        self.assertRaises(cook.CookError,
                          self._build,
                          "world['foo'].findByName('bar').isNotEmpty()")

    @testhelp.context('sysmodel')
    def testFindByName(self):
        self.addComponent('foo:runtime=1.0')
        self.addComponent('bar:runtime=1.0')
        self.addComponent('baz:runtime=1.0')
        self.cfg.debugRecipeExceptions = True
        grp = self._build(
            "all = world.find('foo:runtime', 'bar:runtime', 'baz:runtime')",
            "r.Group(all.findByName('ba.:runtime'))"
        )

        self.checkTroves(grp,
          [(('bar:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
           (('baz:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)])

        self.assertRaises(cook.CookError, self._build,
            "all = world.find('bar:runtime', 'baz:runtime')",
            "r.Group(all.findByName('ba') + world['foo:runtime'])"
        )

        grp = self._build(
            "all = world.find('bar:runtime', 'baz:runtime')",
            "r.Group(all.findByName('bacon', emptyOkay = True) + "
                    "world['foo:runtime'])"
        )

        self.checkTroves(grp,
          [(('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True) ])

    @testhelp.context('sysmodel')
    def testFindBySourceName(self):
        self.addComponent('foo:runtime=1.0')
        self.addCollection('foo=1.0', [ ':runtime' ])
        self.addComponent('foo2:runtime=1.0', sourceName = 'foo:source')
        self.addCollection('foo2=1.0', [ ':runtime' ],
                           sourceName = 'foo:source')

        self.addComponent('bar:runtime=1.0')
        self.addCollection('bar=1.0', [ ':runtime' ])

        grp = self._build(
            "all = world.latestPackages()",
            "foo = all.findBySourceName('foo:source')",
            "r.Group(foo)"
        )

        self.checkTroves(grp,
          [(('foo', '/localhost@rpl:linux/1.0-1-1', ''), True, True ),
           (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
           (('foo2', '/localhost@rpl:linux/1.0-1-1', ''), True, True ),
           (('foo2:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False)
          ])

    @testhelp.context('sysmodel')
    def testImageGroup(self):
        repos = self.openRepository()
        self.addComponent('foo:runtime=1.0')
        self.addCollection('foo=1.0', [ ':runtime' ])

        grp = self._build(
            'grp = world["foo"].createGroup("group-sub")',
            'r.Group(grp)')
        subGrp = repos.getTrove('group-sub', grp.getVersion(),
                                grp.getFlavor())
        assert(not grp.troveInfo.imageGroup())
        assert(not subGrp.troveInfo.imageGroup())

        grp = self._build(
            'grp = world["foo"].createGroup("group-sub")',
            'r.Group(grp, imageGroup = True)')
        subGrp = repos.getTrove('group-sub', grp.getVersion(),
                                grp.getFlavor())
        assert(    grp.troveInfo.imageGroup())
        assert(not subGrp.troveInfo.imageGroup())

        grp = self._build(
            'grp = world["foo"].createGroup("group-sub", imageGroup = True)',
            'r.Group(grp, imageGroup = True)')
        subGrp = repos.getTrove('group-sub', grp.getVersion(),
                                grp.getFlavor())
        assert(    grp.troveInfo.imageGroup())
        assert(    subGrp.troveInfo.imageGroup())

    @testhelp.context('sysmodel')
    def testOptionalSubgroup(self):
        self.addComponent('foo:runtime=1.0')
        self.addComponent('foo:lib=1.0')
        grp = self._build(
            'runtime = world.find("foo:runtime").createGroup("group-rt")',
            'lib = world.find("foo:lib").createGroup("group-lib")',
            'return r.Group(lib + runtime.makeOptional())')

        self.checkTroves(grp,
          [ (('foo:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('group-lib', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('group-rt', '/localhost@rpl:linux/1.0-1-1', ''), False, True),
          ])

    @testhelp.context('sysmodel')
    def testCheckPathConflicts(self):
        self.addComponent('foo:runtime=1.0', 
                          fileContents = [ ( '/path', 'foo' ) ] )
        self.addComponent('bar:runtime=1.0', 
                          fileContents = [ ( '/path', 'bar' ) ] )

        self.assertRaises(GroupPathConflicts, self._build,
            "return r.Group(world.find('foo:runtime', 'bar:runtime'))")
        self._build(
            "return r.Group(world.find('foo:runtime', 'bar:runtime'), "
                           "checkPathConflicts = False)")

        self.assertRaises(GroupPathConflicts, self._build,
            "g = world.find('foo:runtime', 'bar:runtime')",
            "r.Group(g.createGroup('group-sub'), checkPathConflicts = False)")

        self._build(
            "g = world.find('foo:runtime', 'bar:runtime')",
            "r.Group(g.createGroup('group-sub', checkPathConflicts = False),"
                    "checkPathConflicts = False)")

    @testhelp.context('sysmodel')
    def testPackages(self):
        self.addComponent('foo:runtime=1.0')
        self.addCollection('foo=1.0', [ ':runtime' ])
        self.addCollection('group-foo=1.0', [ 'foo' ])

        self.addCollection('group-all=1.0', [ 'group-foo' ])

        grp = self._build(
            "all = world['group-all']",
            "return r.Group(all.packages())")

    @testhelp.context('sysmodel')
    def testMembers(self):
        self.addComponent('foo:runtime=1.0')
        self.addCollection('foo=1.0', [ ':runtime' ])
        self.addComponent('bar:runtime=1.0')
        self.addCollection('bar=1.0', [ ':runtime' ])
        self.addCollection('group-foo=1.0', [ 'foo', ('bar', False) ])

        grp = self._build(
            "world = r.Repository(r.labelPath[0], r.flavor)",
            "foo = world.find('group-foo')",
            "r.Group(foo.members())")

        self.checkTroves(grp,
          [ (('foo', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('bar', '/localhost@rpl:linux/1.0-1-1', ''), False, True),
            (('bar:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
          ])

        grp = self._build(
            "foo = world.find('group-foo')",
            "sub = foo.createGroup('group-sub')",
            "r.Group(sub.members())")

        self.checkTroves(grp,
          [ (('foo', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('bar', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('bar:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('group-foo', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
          ])

    @testhelp.context('sysmodel')
    def testRepositoryLatest(self):
        self.addComponent('foo:runtime=1.0')
        self.addCollection('foo=1.0', [ ':runtime' ])
        self.addComponent('foo2:runtime=1.0', sourceName = 'foo:source')
        self.addCollection('foo2=1.0', [ ':runtime' ],
                           sourceName = 'foo:source')
        self.addComponent('foo:runtime=2.0')
        self.addCollection('foo=2.0', [ ':runtime' ])
        self.makeSourceTrove('foo', 'class foo(PackageRecipe):\n'
                                    '  name = "foo"\n'
                                    '  version = "3.0"\n')
        # CNY-3779 - removed packages shouldn't break latest
        self.addComponent('removed:runtime=1.0')
        trv = self.addCollection('removed=1.0', [ ':runtime' ])
        self.markRemoved(trv.getNameVersionFlavor())

        grp = self.build(
"""
class TestRecipe(GroupSetRecipe):
    name = "group-test"
    version = "1.0"
    clearBuildReqs()

    def setup(r):
        world = r.Repository(r.labelPath[0], r.flavor)
        r.Group(world.latestPackages())
""", "TestRecipe")

        self.checkTroves(grp,
          [(('foo', '/localhost@rpl:linux/2.0-1-1', ''), True, True),
           (('foo:runtime', '/localhost@rpl:linux/2.0-1-1', ''), True, False),
          ])

    @testhelp.context('sysmodel')
    def testOverrides(self):
        self.addComponent('foo:runtime=1.0')
        self.addComponent('foo:lib=1.0')
        self.addCollection('foo=1.0', [ ':runtime', ':lib' ])
        grp = self._build(
                "foo = world['foo']",
                "runtime = foo['foo:runtime']",
                "foo = foo.makeOptional(runtime)",
                "r.Group(foo)" )

        return

        self.checkTroves(grp,
          [ (('foo',     '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('foo:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, False)
          ])

        grp = self._build(
                "foo = world['foo']",
                "runtime = foo['foo:runtime']",
                "sub = foo.makeOptional(runtime).createGroup('group-sub')",
                "r.Group(sub)" )

        self.checkTroves(grp,
          [ (('foo',     '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('foo:lib', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('group-sub', '/localhost@rpl:linux/1.0-1-2', ''), True, True)
          ])

    @testhelp.context('sysmodel')
    def testNestedOverrides(self):
        self.addComponent('foo:runtime=1.0')
        self.addCollection('foo=1.0', [ 'foo:runtime' ])
        self.addCollection('group-foo=1.0', [ 'foo' ],
                           weakRefList = [ 'foo:runtime' ])

        grp = self._build(
            'g = world.find("group-foo")',
            'g = g.makeOptional(g["foo"])',
            'return r.Group(g)'
        )

        self.checkTroves(grp,
          [ (('foo', '/localhost@rpl:linux/1.0-1-1', ''), False, True),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('group-foo', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

    @testhelp.context('sysmodel')
    def testRedirects(self):
        self.addComponent('bar:runtime=1.0')
        self.addCollection('bar=1.0', [ ':runtime'] )

        self.addComponent('redirect:runtime=1.0')
        self.addCollection('redirect=1.0', [ ':runtime'] )

        self.addComponent('foo:runtime=/localhost@foo:bar/2.0')
        self.addCollection('foo=/localhost@foo:bar/2.0', [ ':runtime' ])

        redirectRecipe = "\n".join([
                    'class testRedirect(RedirectRecipe):',
                    '    name = "redirect"',
                    '    version = "2.0"',
                    '    clearBuildReqs()',
                    '',
                    '    def setup(r):',
                    '        r.addRedirect("foo", "localhost@foo:bar")' ])

        built, d = self.buildRecipe(redirectRecipe, "testRedirect")

        self.assertRaisesRegexp(cook.CookError,
                '^' + re.escape('Cannot include redirect '
                'redirect:runtime=/localhost@rpl:linux/2.0-1-1[] in a group'
                ) + '$',

                self._build, 'g = world["redirect"]', 'r.Group(g)')

        self.assertRaisesRegexp(cook.CookError,
                '^' + re.escape('Cannot include redirect '
                'redirect:runtime=/localhost@rpl:linux/2.0-1-1[] in a group'
                ) + '$',

                self._build, 'g = world.latestPackages()', 'r.Group(g)')

    @testhelp.context('sysmodel')
    def testPatch(self):
        self.addComponent('foo:runtime=1.0')
        self.addComponent('foo:lib=1.0')
        self.addComponent('foo:doc=1.0')
        self.addCollection('foo=1.0', [ ':runtime', ':lib', (':doc', False) ])
        # This group includes foo, but turns off foo:runtime.
        self.addCollection('group-foo=1.0', [ 'foo' ],
                           weakRefList = [ ('foo:runtime', False),
                                           'foo:lib',
                                           ('foo:doc', False) ])

        self.addComponent('foo:runtime=2.0')
        self.addComponent('foo:lib=2.0')
        self.addComponent('foo:doc=2.0')
        self.addCollection('foo=2.0', [ ':runtime', ':lib', (':doc', False) ])

        self.cfg.debugRecipeExceptions = True
        grp = self._build(
                "base = world['group-foo']",
                "update = world['foo']",
                "result = base.patch(update)",
                "r.Group(result)")

        # group-foo is included, foo 1.0 is turned off, foo 2.0 is included
        # (but :runtime is still turned off, just like it was in group-foo)
        self.checkTroves(grp, [
            (('group-foo', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('foo', '/localhost@rpl:linux/1.0-1-1', ''), False, True),
            (('foo:doc', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('foo:lib', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('foo', '/localhost@rpl:linux/2.0-1-1', ''), True, True),
            (('foo:doc', '/localhost@rpl:linux/2.0-1-1', ''), False, False),
            (('foo:lib', '/localhost@rpl:linux/2.0-1-1', ''), True, False),
            (('foo:runtime', '/localhost@rpl:linux/2.0-1-1', ''), False, False),
        ])

    @testhelp.context('sysmodel')
    def testSimpleGroup(self):
        self.addComponent('foo:runtime=1.0')
        self.addComponent('foo:lib=1.0')
        grp = self._build(
            'inst = world.find("foo:runtime")',
            'avail = world.find("foo:lib").makeOptional()',
            'return r.Group(inst + avail)')

        self.checkTroves(grp,
          [ (('foo:lib', '/localhost@rpl:linux/1.0-1-1', ''), False, True),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, True)
          ])

    @testhelp.context('sysmodel')
    def testSearchPath(self):
        self.addComponent('foo:runtime=/localhost@test:main/1.0')
        self.addComponent('foo:runtime=/localhost@test:other/2.0')
        self.addComponent('bar:runtime=/localhost@test:main/3.0')
        self.addComponent('baz:runtime=/localhost@test:other/4.0')
        grp = self._build(
            "world = r.SearchPath(r.Repository('localhost@test:main',"
            "                                  r.flavor),",
            "                     r.Repository('localhost@test:other',"
            "                                  r.flavor))",
            'inst = world.find("foo:runtime", "bar:runtime", "baz:runtime")',
            "return r.Group(inst)")

        self.checkTroves(grp,
          [(('bar:runtime', '/localhost@test:main/3.0-1-1', ''), True, True),
           (('baz:runtime', '/localhost@test:other/4.0-1-1', ''), True, True),
           (('foo:runtime', '/localhost@test:main/1.0-1-1', ''), True, True)
          ])

    @testhelp.context('sysmodel')
    def testSearchPathErrors(self):
        e = self.assertRaises(cook.CookError, self._build,
            "world = r.SearchPath('host@ns:v', 'trv=host@ns:v2')")
        self.assertEquals(str(e),
            "/test.recipe:9:\n CookError: Invalid arguments"
            " 'host@ns:v', 'trv=host@ns:v2':"
            " SearchPath arguments must be Repository or TroveSet")

    @testhelp.context('sysmodel')
    def testCML(self):
        self.addComponent('foo:runtime=1.0')
        self.addCollection('foo=1.0', [ ':runtime' ])

        self.cfg.debugRecipeExceptions = True
        # test the most basic CML handling
        grp = self._build(
            "foo = r.CML([ 'install foo=localhost@rpl:linux' ])",
            "grp = r.Group(foo)",
            "return grp"
        )

        self.checkTroves(grp,
          [(('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
           (('foo',         '/localhost@rpl:linux/1.0-1-1', ''), True, True) ])

        # test the optional second argument for providing a search path
        # (also, test the normal string format here, since no newline required)
        grp = self._build(
            "foo = r.CML('install foo', world)",
            "grp = r.Group(foo)",
            "return grp"
        )

        self.checkTroves(grp,
          [(('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
           (('foo',         '/localhost@rpl:linux/1.0-1-1', ''), True, True) ])

        grp = self._build(
            "foo = r.CML([ 'search localhost@rpl:linux', "
                          "'install foo' ])",
            "grp = r.Group(foo)",
            "return grp"
        )

        self.checkTroves(grp,
          [(('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
           (('foo',         '/localhost@rpl:linux/1.0-1-1', ''), True, True) ])

        self.addComponent('bar:runtime=1.0', requires = 'trove: foo:runtime')
        self.addCollection('bar=1.0', [ ':runtime' ])

        self.addCollection('group-top=1.0', [ 'foo', 'bar' ])

        self.addComponent('bar:runtime=2.0')
        self.addCollection('bar=2.0', [ ':runtime' ])

        # test dependency resolution via the search path
        grp = self._build(
            "bar = r.CML([ 'search group-top=localhost@rpl:linux', "
                          "'install bar' ])",
            "needed = bar.depsNeeded(bar.searchPath)",
            "grp = r.Group(bar + needed)",
            "return grp"
        )

        self.checkTroves(grp,
          [(('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
           (('foo',         '/localhost@rpl:linux/1.0-1-1', ''), True, True),
           (('bar:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
           (('bar',         '/localhost@rpl:linux/1.0-1-1', ''), True, True),
          ])

        # test that erase moves the thing erased to the available set
        grp = self._build(
            "bar = r.CML([ 'install group-top=localhost@rpl:linux', "
                          "'erase bar' ])",
            "needed = bar.depsNeeded(bar.searchPath)",
            "grp = r.Group(bar + needed)",
            "return grp"
        )

        self.checkTroves(grp,
          [(('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
           (('foo',         '/localhost@rpl:linux/1.0-1-1', ''), True, False),
           (('bar:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
           (('bar',         '/localhost@rpl:linux/1.0-1-1', ''), False, True),
           (('group-top',   '/localhost@rpl:linux/1.0-1-1', ''), True, True),
          ])

        grp = self._build(
            "bar = r.CML([ 'search group-top=localhost@rpl:linux', "
                          "'install bar=localhost@rpl:linux/2.0' ])",
            "needed = bar.depsNeeded(bar.searchPath)",
            "grp = r.Group(bar + needed)",
            "return grp"
        )

        self.checkTroves(grp,
          [(('bar:runtime', '/localhost@rpl:linux/2.0-1-1', ''), True, False),
           (('bar',         '/localhost@rpl:linux/2.0-1-1', ''), True, True),
          ])

        self.addComponent('foo:runtime=2.0')
        self.addCollection('foo=2.0', [ (':runtime', False) ])

        # test dep resolution against optional set with required trove not
        # in a search line; bar:runtime should cause foo:runtime to be
        # pulled into the installed set
        grp = self._build(
            "bar = r.CML([ 'install foo=localhost@rpl:linux/2.0', "
                          "'install bar=localhost@rpl:linux/1.0' ])",
            "needed = bar.depsNeeded(bar.searchPath)",
            "grp = r.Group(bar + needed)",
            "return grp"
        )
        self.checkTroves(grp,
          [(('foo:runtime', '/localhost@rpl:linux/2.0-1-1', ''), True, False),
           (('foo',         '/localhost@rpl:linux/2.0-1-1', ''), True, True),
           (('bar:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
           (('bar',         '/localhost@rpl:linux/1.0-1-1', ''), True, True),
          ])

        # make sure include works for CML in a group set
        self.addComponent("cml:source",
                          fileContents = [ ( "some.cml", "install foo=1.0" ) ])
        grp = self._build(
            "world = r.Repository('localhost@rpl:linux', r.flavor)",
            "bar = r.CML([ 'include cml:source' ], searchPath = world)",
            "return r.Group(bar)"
        )
        self.checkTroves(grp,
          [(('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
           (('foo',         '/localhost@rpl:linux/1.0-1-1', ''), True, True),
          ])

    @testhelp.context('sysmodel')
    def testWalk(self):
        self.addComponent('foo:runtime=1')
        self.addComponent('foo:devel=1')
        self.addCollection('foo=1', [ ':runtime', (':devel', False) ])

        self.addComponent('bar:runtime=1')
        self.addComponent('bar:devel=1')
        self.addCollection('bar=1', [ ':runtime', (':devel', False) ])

        # override packages and install devel components
        self.addCollection('group-foo=1', [ 'foo' ],
                           weakRefList = [ ('foo:runtime', True),
                                           ('foo:devel', True ) ])
        self.addCollection('group-bar=1', [ 'bar' ],
                           weakRefList = [ ('bar:runtime', True),
                                           ('bar:devel', True ) ])

        grp = self._build(
                    'foo = world.find("group-foo")',
                    'bar = world.find("group-bar").makeOptional()',
                    'r.Group(foo + bar)')

        self.checkTroves(grp, [
           (('foo', '/localhost@rpl:linux/1-1-1', ''), True, False),
           (('foo:devel', '/localhost@rpl:linux/1-1-1', ''), True, False),
           (('foo:runtime', '/localhost@rpl:linux/1-1-1', ''), True, False),
           (('bar', '/localhost@rpl:linux/1-1-1', ''), False, False),
           (('bar:devel', '/localhost@rpl:linux/1-1-1', ''), False, False),
           (('bar:runtime', '/localhost@rpl:linux/1-1-1', ''), False, False),
           (('group-foo', '/localhost@rpl:linux/1-1-1', ''), True, True),
           (('group-bar', '/localhost@rpl:linux/1-1-1', ''), False, True)
        ])

    @testhelp.context('sysmodel')
    def testUpdate(self):
        self.addComponent('foo:runtime=1.0')
        self.addComponent('foo:lib=1.0')
        self.addComponent('foo:doc=1.0')
        self.addCollection('foo=1.0', [ ':runtime', ':lib', (':doc', False) ])
        # This group includes foo, but turns off foo:runtime.
        self.addCollection('group-foo=1.0', [ 'foo' ],
                           weakRefList = [ ('foo:runtime', False),
                                           'foo:lib',
                                           ('foo:doc', False) ])

        self.addComponent('foo:runtime=2.0')
        self.addComponent('foo:lib=2.0')
        self.addComponent('foo:doc=2.0')
        self.addCollection('foo=2.0', [ ':runtime', ':lib', (':doc', False) ])

        self.addComponent('bar:runtime=1.0')
        self.addCollection('bar=1.0', [ ':runtime' ])

        self.cfg.debugRecipeExceptions = True
        grp = self._build(
                "base = world['group-foo']",
                "update = world['foo']",
                "result = base.update(update)",
                "result = result.update(world['bar'])",
                "r.Group(result)")

        # group-foo is included, foo 1.0 is turned off, foo 2.0 is included
        # (and :runtime is still disabled since it was before
        self.checkTroves(grp, [
            (('bar', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('bar:runtime', '/localhost@rpl:linux/1.0-1-1', ''), True, False),
            (('group-foo', '/localhost@rpl:linux/1.0-1-1', ''), True, True),
            (('foo', '/localhost@rpl:linux/1.0-1-1', ''), False, True),
            (('foo:doc', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('foo:lib', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('foo:runtime', '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('foo', '/localhost@rpl:linux/2.0-1-1', ''), True, True),
            (('foo:doc', '/localhost@rpl:linux/2.0-1-1', ''), False, False),
            (('foo:lib', '/localhost@rpl:linux/2.0-1-1', ''), True, False),
            (('foo:runtime', '/localhost@rpl:linux/2.0-1-1', ''), False, False),
        ])

    @testhelp.context('sysmodel')
    def testWeakRefs(self):
        self.addComponent('foo:runtime=1')
        self.addCollection('foo=1', [ ':runtime' ])
        self.addComponent('bar:runtime=1')
        self.addCollection('bar=1', [ ':runtime' ])

        grp = self._build('r.Group(world.find("foo"))')
        self.checkTroves(grp,
          [(('foo', '/localhost@rpl:linux/1-1-1', ''), True, True),
           (('foo:runtime', '/localhost@rpl:linux/1-1-1', ''), True, False)])

        grp = self._build(
                    'foo = world.find("foo")',
                    'bar = world.find("bar").makeOptional()',
                    'r.Group(foo + bar)')

        self.checkTroves(grp, [
           (('foo', '/localhost@rpl:linux/1-1-1', ''), True, True),
           (('foo:runtime', '/localhost@rpl:linux/1-1-1', ''), True, False),
           (('bar', '/localhost@rpl:linux/1-1-1', ''), False, True),
           (('bar:runtime', '/localhost@rpl:linux/1-1-1', ''), False, False)
        ])

        grp = self._build(
                    'foo = world.find("foo")',
                    'bar = world.find("bar").makeOptional()',
                    'sub = (foo + bar).createGroup("group-sub")',
                    'r.Group(sub)')

        self.checkTroves(grp, [
           (('foo', '/localhost@rpl:linux/1-1-1', ''), True, False),
           (('foo:runtime', '/localhost@rpl:linux/1-1-1', ''), True, False),
           (('bar', '/localhost@rpl:linux/1-1-1', ''), False, False),
           (('bar:runtime', '/localhost@rpl:linux/1-1-1', ''), False, False),
           (('group-sub', '/localhost@rpl:linux/1.0-1-3', ''), True, True)
        ])

    @testhelp.context('sysmodel')
    def testTroveTupleLookup(self):
        self.addComponent('foo:runtime=1')
        self.addComponent('bar:runtime=1')
        self.addComponent('baz:runtime=1')
        grp = self._build(
            "world = r.Repository(r.labelPath[0], r.flavor)",
            "ts = world['foo:runtime'] + world['bar:runtime']",
            "ts2 = ts['foo:runtime']",
            "sub = ts2.createGroup('group-foo')",
            "groupFoo = sub['group-foo']",
            "r.Group(ts2)")

        self.checkTroves(grp,
          [ (('foo:runtime', '/localhost@rpl:linux/1-1-1', ''), True, True) ])

    @testhelp.context('sysmodel')
    def testScripts(self):
        '''
        Make sure we can attach scripts to groups correctly
        '''
        def _checkScripts(grp):
            scripts = grp.troveInfo.scripts
            for scriptType in scriptTypes:
                script = getattr(scripts, scriptType).script()
                self.assertEquals(script, '# %s' % scriptType)

        self.addComponent('foo:runtime=1.0')
        self.addComponent('foo:lib=1.0')
        self.addCollection('foo=1.0', [ 'foo:runtime', 'foo:lib' ])
        
        recipe = []
        # These 5 types are the only types allowed for groups; the
        # other 3 types are supported only for packages (for RPM import)
        scriptTypes = ('postInstall',
                       'preRollback', 'postRollback',
                       'preUpdate', 'postUpdate')
        for scriptType in scriptTypes:
            recipe.append('%s = r.Script(contents="# %s")' %
                            (2 * (scriptType,)))
        recipe.append('scripts = r.Scripts(%s)' %', '.join(
            [' = '.join((x, x)) for x in scriptTypes]))
        recipe.append('foo = world.find("foo")')
        recipe.append('groupInner = foo.createGroup('
                         '"group-inner", scripts=scripts)')
        recipe.append('r.Group(groupInner, scripts=scripts)')
        grp = self._build(*recipe)
        repos = self.openRepository()
        igrp = repos.getTrove('group-inner', grp.getVersion(), grp.getFlavor())
        for g in (grp, igrp):
            _checkScripts(g)

        # now try and build a new group which inherits some of those scripts
        grp = self._build("grp = world['group-test']",
                          "foo = world['foo']",
                          "r.name = 'group-bar'",
                          "scripts = grp.scripts()",
                          "r.Group(foo, scripts = scripts)")
        _checkScripts(grp)

        # these builds fail because of too many/too few items in the troveset
        e = self.assertRaises(cook.CookError,
                  self._build, "grp = world.find('group-test', 'foo')",
                               "scripts = grp.scripts()",
                               "r.Group(grp, scripts = scripts)")
        self.assertEquals(str(e), 'Multiple troves in trove set for scripts()')

        e = self.assertRaises(cook.CookError,
                  self._build, "grp = world.find('group-test')",
                               "scripts = (grp - grp['group-test']).scripts()",
                               "r.Group(grp, scripts = scripts)")
        self.assertEquals(str(e), 'Empty trove set for scripts()')

    @testhelp.context('sysmodel')
    def testMultipleFinds(self):
        self.addComponent('foo:runtime=1.0')
        self.addComponent('foo:lib=1.0')
        self.addCollection('foo=1.0', [ ':lib', ':runtime' ] )
        self._build(
                "t = world['foo']",
                "r.Group(t['foo:runtime'] + t['foo:lib'])",
                "r.writeDotGraph('%s/graph.dot')" % self.workDir
        )
        # ensure that the two lookups result in only one flatten
        # operation underneath; if this breaks, there will be two
        self.assertEquals(1, len([
            x for x in open('%s/graph.dot' % self.workDir).readlines()
            if 'Flatten' in x]))

    @testhelp.context('sysmodel')
    def testBuildReferences(self):
        self.addComponent('foo:lib=1.0')
        self.addCollection('foo=1.0', [ ':lib' ] )
        self.addCollection('group-foo=1.0', [ 'foo' ])

        trv = self._build(
                'subGroup = world["group-foo"].members().createGroup("group-sub")',
                'return r.Group(subGroup)')

        buildRefs = trv.getBuildRefs()
        self.assertEquals(buildRefs, [ ( 'group-foo', trv.getVersion(),
                                         trv.getFlavor() ) ] )
        repos = self.openRepository()
        subGroup = repos.getTrove('group-sub', trv.getVersion(),
                                  trv.getFlavor())
        buildRefs = subGroup.getBuildRefs()
        assert(not buildRefs)

    @testhelp.context('sysmodel')
    def testPackagesForComponents(self):
        """Packages are added for components in subgroups

        @tests: CNY-3720
        """
        self.addComponent('foo:lib=1.0')
        self.addCollection('foo=1.0', [':lib'])
        self.addComponent('bar:lib=1.0')
        self.addCollection('bar=1.0', [':lib'])
        self.addComponent('baz:lib=1.0')
        self.addCollection('baz=1.0', [':lib'])
        grp = self._build(
            'foo = world.find("foo:lib").createGroup("group-foo")',
            'bar = world.find("bar:lib").createGroup("group-bar")',
            'baz = world.find("baz:lib")',
            'return r.Group(foo + bar.makeOptional() + baz)')

        self.checkTroves(grp, [
            (('bar',        '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('bar:lib',    '/localhost@rpl:linux/1.0-1-1', ''), False, False),
            (('baz',        '/localhost@rpl:linux/1.0-1-1', ''), True,  True),
            (('baz:lib',    '/localhost@rpl:linux/1.0-1-1', ''), True,  False),
            (('foo',        '/localhost@rpl:linux/1.0-1-1', ''), True,  False),
            (('foo:lib',    '/localhost@rpl:linux/1.0-1-1', ''), True,  False),
            (('group-foo',  '/localhost@rpl:linux/1.0-1-1', ''), True,  True),
            (('group-bar',  '/localhost@rpl:linux/1.0-1-1', ''), False, True),
          ])
