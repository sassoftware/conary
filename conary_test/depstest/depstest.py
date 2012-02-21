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


import copy
import pickle
import unittest

from conary import errors
from conary.deps.deps import (
        getMinimalCompatibleChanges,
        parseDep,
        parseFlavor,
        overrideFlavor,
        mergeFlavorList,
        ParseError,
        mergeFlavor,
        formatFlavor,
        compatibleFlavors,
        ThawDependencySet,
        DEP_MERGE_TYPE_PREFS,
        DEP_MERGE_TYPE_NORMAL,
        DEP_MERGE_TYPE_DROP_CONFLICTS,
        FLAG_SENSE_REQUIRED,
        FLAG_SENSE_PREFERNOT,
        FLAG_SENSE_DISALLOWED,
        FLAG_SENSE_PREFERRED,
        getShortFlavorDescriptors,
        dependencyCache,
        Flavor,
        DependencyClass,
        AbiDependency,
        DEP_MERGE_TYPE_OVERRIDE,
        FileDependencies,
        SonameDependencies,
        ThawFlavor,
        filterFlavor,
        Dependency,
        DependencySet,
        TroveDependencies,
        InstructionSetDependency,
        flavorDifferences,
        )

class DepsTest(unittest.TestCase):

    def _testStr(self, strForm, frzForm):
        first = parseDep(strForm)
        frz = first.freeze()
        second = ThawDependencySet(frz)
        assert(frz == frzForm)
        assert(first == second)
        assert(parseDep(str(first)).freeze() == frzForm)

    def testColonFlag(self):
        self._testStr('rpm: foo(:flag)', '16#foo:\:flag')
        self.assertRaises(ParseError,
                          self._testStr, 'trove: foo(:flag)', '16#foo:\:flag')

    def testColonName(self):
        self._testStr('perl: foo::bar', '12#foo::::bar')

    def testRpmLibrary(self):
        self._testStr('rpm: libc.so.6', '16#libc.so.6')
        self._testStr('rpm: libc.so.6(ABC DEF)', '16#libc.so.6:ABC:DEF')

    def testSquareBracketNames(self):
        self._testStr('rpm: foo[something](flag)', '16#foo[something]:flag')
        self.assertRaises(ParseError,
                          self._testStr, 'trove: foo[something](flag)',
                                         '16#foo[something]:flag')

    def testDeps(self):
        first = Dependency("some", [])
        assert(str(first) == "some")
        first = Dependency("some", [ ("flag1", FLAG_SENSE_REQUIRED) ])
        assert(str(first) == "some(flag1)")
        first = Dependency("some", [ ("flag1", FLAG_SENSE_REQUIRED),
                                     ("flag2", FLAG_SENSE_REQUIRED) ] )
        assert(str(first) == "some(flag1 flag2)")

        second = Dependency("some", [ ("flag1", FLAG_SENSE_REQUIRED),
                                      ("flag2", FLAG_SENSE_REQUIRED) ] )

        assert(first == second)
        assert(first.satisfies(second))
        assert(second.satisfies(first))

        dict = {}
        dict[first] = True
        assert(dict.has_key(first))
        assert(dict.has_key(second))

        second = Dependency("some", [ ("flag1", FLAG_SENSE_REQUIRED),
                                      ("flag2", FLAG_SENSE_REQUIRED),
                                      ("flag3", FLAG_SENSE_REQUIRED) ] )

        assert(not dict.has_key(second))
        dict[second] = False
        assert(dict[first])
        assert(first != second)

        assert(not first.satisfies(second))
        assert(second.satisfies(first))

        second = Dependency("other", [ ("flag1", FLAG_SENSE_REQUIRED),
                                       ("flag2", FLAG_SENSE_REQUIRED) ] )

        assert(not first.satisfies(second))
        assert(not second.satisfies(first))

    def testDepClass(self):
        first = FileDependencies()
        awkDep = Dependency("/bin/awk")
        first.addDep(awkDep)
        assert(len(first.members) == 1)
        first.addDep(Dependency("/bin/grep"))
        assert(str(first) == "file: /bin/awk\nfile: /bin/grep")

        second = FileDependencies()
        second.addDep(Dependency("/bin/awk"))
        second.addDep(Dependency("/bin/grep"))

        assert(first.satisfies(second))
        assert(second.satisfies(first))
        assert(first == second)
        assert(hash(first) == hash(second))

        second.addDep(Dependency("/bin/sed"))
        assert(not first.satisfies(second))
        assert(second.satisfies(first))
        assert(first != second)

        second = AbiDependency()
        second.addDep(Dependency("x86", [ ("cmov", FLAG_SENSE_REQUIRED),
                                          ("i686", FLAG_SENSE_REQUIRED) ]))
        assert(not first.satisfies(second))
        assert(not second.satisfies(first))
        assert(first != second)

        first = FileDependencies()
        first.addDep(Dependency("/bin/sh"))
        second = FileDependencies()
        second.addDep(Dependency("/bin/awk"))
        second.addDep(Dependency("/bin/grep"))

        first.union(second)
        assert(str(first) == "file: /bin/awk\nfile: /bin/grep\nfile: /bin/sh")

        first = InstructionSetDependency()
        first.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_REQUIRED),
                                         ("mmx", FLAG_SENSE_REQUIRED) ]))
        second = InstructionSetDependency()
        second.addDep(Dependency("x86", [ ("cmov", FLAG_SENSE_REQUIRED),
                                          ("i686", FLAG_SENSE_REQUIRED) ]))
        first.union(second)
        assert(str(first) == "is: x86(cmov i686 mmx)")

    def testDepUnion(self):
        first = InstructionSetDependency()
        first.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_PREFERRED),
                                         ("mmx", FLAG_SENSE_REQUIRED) ]))
        second = InstructionSetDependency()
        second.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_REQUIRED),
                                          ("mmx", FLAG_SENSE_PREFERNOT) ]))
        first.union(second)
        assert(str(first) == "is: x86(i686 mmx)")

        first = InstructionSetDependency()
        first.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_PREFERNOT),
                                         ("mmx", FLAG_SENSE_DISALLOWED) ]))

        second = InstructionSetDependency()
        second.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_DISALLOWED),
                                          ("mmx", FLAG_SENSE_PREFERRED) ]))
        first.union(second)
        assert(str(first) == "is: x86(!i686 !mmx)")

        first = InstructionSetDependency()
        first.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_DISALLOWED) ]))
        second = InstructionSetDependency()
        second.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_REQUIRED) ]))
        self.assertRaises(RuntimeError, first.union, second)
        f = copy.deepcopy(first)
        f.union(second, mergeType = DEP_MERGE_TYPE_OVERRIDE)
        assert(str(f) == "is: x86(i686)")
        f.union(second, mergeType = DEP_MERGE_TYPE_PREFS)
        assert(str(f) == "is: x86(i686)")
        f.union(second, mergeType = DEP_MERGE_TYPE_DROP_CONFLICTS)
        assert(str(f) == "is: x86(i686)")


        first = InstructionSetDependency()
        first.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_PREFERNOT) ]))
        second = InstructionSetDependency()
        second.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_PREFERRED) ]))
        self.assertRaises(RuntimeError, first.union, second)

        f = copy.deepcopy(first)
        f.union(second, mergeType = DEP_MERGE_TYPE_DROP_CONFLICTS)
        assert(str(f) == "is: x86")
        f = copy.deepcopy(first)
        f.union(second, mergeType = DEP_MERGE_TYPE_OVERRIDE)
        assert(str(f) == "is: x86(~i686)")
        first.union(second, mergeType = DEP_MERGE_TYPE_PREFS)
        assert(str(first) == "is: x86(~i686)")

        first = InstructionSetDependency()
        first.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_DISALLOWED) ]))
        second = InstructionSetDependency()
        second.addDep(Dependency("x86", [ ("i686", FLAG_SENSE_PREFERNOT) ]))
        f = copy.deepcopy(first)
        f.union(second, mergeType = DEP_MERGE_TYPE_DROP_CONFLICTS)
        assert(str(f) == "is: x86(!i686)")
        f = copy.deepcopy(first)
        f.union(second, mergeType = DEP_MERGE_TYPE_OVERRIDE)
        assert(str(f) == "is: x86(~!i686)")
        first.union(second, mergeType = DEP_MERGE_TYPE_PREFS)
        assert(str(first) == "is: x86(!i686)")

    def testDepSet(self):
        requires = DependencySet()
        requires.addDep(FileDependencies, Dependency("/bin/sed"))
        requires.addDep(FileDependencies, Dependency("/bin/awk"))
        requires.addDep(TroveDependencies, Dependency("foo:runtime"))
        assert(str(requires) ==
                "file: /bin/awk\nfile: /bin/sed\ntrove: foo:runtime")
        assert(ThawDependencySet(requires.freeze()) == requires)

        provides = DependencySet()
        provides.addDep(FileDependencies, Dependency("/bin/sed"))
        provides.addDep(FileDependencies, Dependency("/bin/awk"))
        provides.addDep(TroveDependencies, Dependency("foo:runtime"))
        assert(ThawDependencySet(provides.freeze()) == provides)

        assert(provides.satisfies(requires))

        provides.addDep(FileDependencies, Dependency("/bin/grep"))
        assert(provides.satisfies(requires))

        requires.addDep(FileDependencies, Dependency("/bin/grep"))
        assert(provides.satisfies(requires))

        requires.addDep(TroveDependencies, Dependency("bar:runtime"))
        assert(not provides.satisfies(requires))

        provides.addDep(TroveDependencies, Dependency("bar:runtime"))
        assert(provides.satisfies(requires))

        requires.addDep(InstructionSetDependency,
                        Dependency("x86", [("cmov", FLAG_SENSE_REQUIRED),
                                           ("sse", FLAG_SENSE_REQUIRED)]))
        assert(not provides.satisfies(requires))

        provides.addDep(InstructionSetDependency,
                        Dependency("x86", [("cmov", FLAG_SENSE_REQUIRED),
                                           ("mmx", FLAG_SENSE_REQUIRED),
                                           ("sse", FLAG_SENSE_REQUIRED)]))
        assert(provides.satisfies(requires))

        requires = DependencySet()
        requires.addDep(InstructionSetDependency,
                        Dependency("x86", [("cmov", FLAG_SENSE_REQUIRED),
                                           ("sse", FLAG_SENSE_REQUIRED)]))

        provides = DependencySet()
        provides.addDep(InstructionSetDependency,
                        Dependency("x86", [("3dnow", FLAG_SENSE_REQUIRED),
                                           ("cmov",  FLAG_SENSE_REQUIRED)]))
        assert(not provides.satisfies(requires))

        first = DependencySet()
        first.addDep(InstructionSetDependency,
                        Dependency("x86", [("cmov", FLAG_SENSE_REQUIRED),
                                           ("sse", FLAG_SENSE_REQUIRED)]))
        first.addDep(FileDependencies, Dependency("/bin/awk"))
        first.addDep(FileDependencies, Dependency("/bin/grep"))
        first.addDep(TroveDependencies, Dependency("foo:runtime"))

        second = DependencySet()
        second.addDep(InstructionSetDependency,
                        Dependency("x86", [("cmov", FLAG_SENSE_REQUIRED),
                                           ("mmx", FLAG_SENSE_REQUIRED)]))
        second.addDep(FileDependencies, Dependency("/bin/sed"))
        second.addDep(SonameDependencies, Dependency("libc.so.6"))
        first.union(second)
        assert(str(first) == "is: x86(cmov mmx sse)\n"
                             "file: /bin/awk\n"
                             "file: /bin/grep\n"
                             "file: /bin/sed\n"
                             "trove: foo:runtime\n"
                             "soname: libc.so.6")

        first = DependencySet()
        second = DependencySet()
        assert(hash(first) == hash(second))
        first.addDep(FileDependencies, Dependency("/bin/sed"))
        second.addDep(FileDependencies, Dependency("/bin/sed"))
        assert(hash(first) == hash(second))

        # test that dictionary order has no affect on frozen form
        class shadowDict(dict):
            def __init__(self, otherdict):
                dict.__init__(self, otherdict)

            def iteritems(self):
                items = self.items()
                items.reverse()
                for key, value in items:
                    yield key, value

        # this isn't ordered by path; make sure the frozen dep is ordered
        # by path
        depset = DependencySet()
        for x in range(10):
            depset.addDep(FileDependencies, Dependency("/bin/%d" % x))

        frz = depset.freeze()
        l = frz.split("|")
        assert( [ int(x[-1]) for x in  l ] == list(range(10)) )

        # make sure removing works properly
        s = DependencySet()
        s.addDep(FileDependencies, Dependency('/bin/foo'));
        s.addDep(TroveDependencies, Dependency('bar') )
        self.assertEquals(str(s), 'file: /bin/foo\ntrove: bar')

        self.assertRaises(KeyError, s.removeDeps,
                          SonameDependencies, [ Dependency('foo') ])
        self.assertRaises(KeyError, s.removeDeps,
                          FileDependencies, [ Dependency('foo') ])
        s.removeDeps(SonameDependencies, [ Dependency('foo') ],
                     missingOkay = True)
        s.removeDeps(FileDependencies, [ Dependency('foo') ],
                     missingOkay = True)
        self.assertEquals(str(s), 'file: /bin/foo\ntrove: bar')

        s.removeDeps(FileDependencies, [ Dependency('/bin/foo') ],
                     missingOkay = True)
        self.assertEquals(str(s), 'trove: bar')
        s2 = ThawDependencySet(s.freeze())
        assert(s2.freeze() == s.freeze())

        s.removeDeps(TroveDependencies, [ Dependency('bar') ],
                     missingOkay = True)
        self.assertEquals(str(s), '')

        s2 = ThawDependencySet(s.freeze())
        assert(s2.freeze() == s.freeze())



    def testMatch(self):
        def testMatch(first, second):
            one = parseFlavor(first)
            two = parseFlavor(second)
            assert(one.match(two))

        def testMismatch(first, second):
            one = parseFlavor(first)
            two = parseFlavor(second)
            assert(one.match(two))

    def testError(self):
        try:
            one = parseFlavor('foo bar', raiseError=True)
            assert(0)
        except errors.ParseError, msg:
            assert(str(msg) == "invalid flavor 'foo bar'")

    def testUnicodeFlavor(self):
        # CNY-3381
        flv = u"is: x86"
        self.assertEqual(parseFlavor(flv).freeze(), "1#x86")
        flv = u"is: \u0163"
        self.assertRaises(errors.ParseError, parseFlavor, flv)

    def testStrongFlavor(self):
        def strong(flavor):
            return parseFlavor(flavor).toStrongFlavor().freeze()
        assert(strong('~foo,~bar,~!bam is: x86(~!i586)') \
                                    == '1#x86:!i586|5#use:!bam:bar:foo')

    def testOverrideFlavor(self):
        def override(flavor1, flavor2):
            return overrideFlavor(parseFlavor(flavor1),
                                  parseFlavor(flavor2)).freeze()

        assert(override('foo,bar is:x86(i686)', '!foo is:x86_64') \
                                    == '1#x86_64|5#use:bar:!foo')
        foo = override('foo is:x86(i686,!i586)', '!foo is:x86(i586)')
        assert(override('foo is:x86(i686,!i586) x86_64', '!foo is:x86(i586)') \
                                    == '1#x86:i586:i686|5#use:!foo')

    def testOverrideWeakPrefs(self):
        def override1(flavor1, flavor2):
            return str(overrideFlavor(parseFlavor(flavor1),
                                      parseFlavor(flavor2),
                                      mergeType=DEP_MERGE_TYPE_PREFS))

        def override2(flavor1, flavor2):
            return str(mergeFlavorList([parseFlavor(flavor1), parseFlavor(flavor2)],
                                       mergeType=DEP_MERGE_TYPE_PREFS))


        for override in override1, override2:
            assert(override('~foo', 'foo') == '~foo')
            assert(override('~foo', '~foo') == '~foo')
            assert(override('~foo', '~!foo') == '~!foo')
            assert(override('~foo', '!foo') == '~!foo')

            assert(override('~!foo', 'foo') == '~foo')
            assert(override('~!foo', '~foo') == '~foo')
            assert(override('~!foo', '~!foo') == '~!foo')
            assert(override('~!foo', '!foo') == '~!foo')

            assert(override('foo', 'foo') == 'foo')
            assert(override('foo', '~foo') == 'foo')
            assert(override('foo', '~!foo') == '~!foo')
            assert(override('foo', '!foo') == '!foo')

            assert(override('!foo', 'foo') == 'foo')
            assert(override('!foo', '~foo') == '~foo')
            assert(override('!foo', '~!foo') == '!foo')
            assert(override('!foo', '!foo') == '!foo')

    def testMergeNormal(self):
        def override1(flavor1, flavor2):
            return str(overrideFlavor(parseFlavor(flavor1), parseFlavor(flavor2),
                                      mergeType=DEP_MERGE_TYPE_NORMAL))

        def override2(flavor1, flavor2):
            return str(mergeFlavorList([parseFlavor(flavor1), parseFlavor(flavor2)],
                                       mergeType=DEP_MERGE_TYPE_NORMAL))


        for override in override1, override2:
            assert(override('~foo', 'foo') == 'foo')
            assert(override('~foo', '~foo') == '~foo')
            self.assertRaises(RuntimeError, override ,'~foo', '~!foo')
            assert(override('~foo', '!foo') == '!foo')

            assert(override('~!foo', 'foo') == 'foo')
            self.assertRaises(RuntimeError, override, '~!foo', '~foo')
            assert(override('~!foo', '~!foo') == '~!foo')
            assert(override('~!foo', '!foo') == '!foo')

            assert(override('foo', 'foo') == 'foo')
            assert(override('foo', '~foo') == 'foo')
            assert(override('foo', '~!foo') == 'foo')
            self.assertRaises(RuntimeError, override, 'foo', '!foo')

            self.assertRaises(RuntimeError, override, '!foo', 'foo')
            assert(override('!foo', '~foo') == '!foo')
            assert(override('!foo', '~!foo') == '!foo')
            assert(override('!foo', '!foo') == '!foo')

    def testMergeDropConflicts(self):
        def override1(flavor1, flavor2):
            return str(overrideFlavor(parseFlavor(flavor1),
                                      parseFlavor(flavor2),
                                      mergeType=DEP_MERGE_TYPE_DROP_CONFLICTS))

        def override2(flavor1, flavor2):
            return str(mergeFlavorList([parseFlavor(flavor1), parseFlavor(flavor2)],
                                       mergeType=DEP_MERGE_TYPE_DROP_CONFLICTS))


        for override in override1, override2:
            assert(override('~foo', 'foo') == 'foo')
            assert(override('~foo', '~foo') == '~foo')
            assert(override('~foo', '~!foo') == '')
            assert(override('~foo', '!foo') == '!foo')

            assert(override('~!foo', 'foo') == 'foo')
            assert(override('~!foo', '~foo') == '')
            assert(override('~!foo', '~!foo') == '~!foo')
            assert(override('~!foo', '!foo') == '!foo')

            assert(override('foo', 'foo') == 'foo')
            assert(override('foo', '~foo') == 'foo')
            assert(override('foo', '~!foo') == 'foo')
            assert(override('foo', '!foo') == '')

            assert(override('!foo', 'foo') == '')
            assert(override('!foo', '~foo') == '!foo')
            assert(override('!foo', '~!foo') == '!foo')
            assert(override('!foo', '!foo') == '!foo')
            assert(override('!foo', 'foo') == '')

        # check to make sure parsed flavor is actually empty and doesn't
        # have an empty use flag
        test1 = overrideFlavor(parseFlavor('!foo'), parseFlavor('foo'),
                               mergeType=DEP_MERGE_TYPE_DROP_CONFLICTS)
        assert(test1 == parseFlavor(''))


    def testMergeFlavorList(self):
        # tests of mergeFlavorLists with 3 or more flavors...
        def merge(*flavors):
            if isinstance(flavors[-1], int):
                mergeType = flavors[-1]
                flavors = flavors[:-1]
            else:
                mergeType = DEP_MERGE_TYPE_NORMAL
            return str(mergeFlavorList([parseFlavor(x) for x in flavors],
                                       mergeType))

        _DROP = DEP_MERGE_TYPE_DROP_CONFLICTS
        _PREFS = DEP_MERGE_TYPE_PREFS


        assert(merge('foo', '~bar', '!bar', 'baz')  == '!bar,baz,foo')
        assert(merge('foo', '~foo', '~!foo', '!foo', _DROP)  == '')
        assert(merge('foo', '~foo', '~!foo', 'bar',_DROP)  == 'bar,foo')
        assert(merge('foo', '!foo', _DROP)  == '')
        assert(merge('is:', 'is:', _DROP)  == '')
        assert(merge('foo', '~foo', '~foo', _PREFS)  == 'foo')
        assert(merge('foo', '~foo', '~!foo', _PREFS)  == '~!foo')
        assert(merge('foo', '~!foo', '', _PREFS)  == '~!foo')
        assert(merge('is:x86(!i686)', '~!foo', 'is:x86_64', 'is:x86(i686)', _PREFS)  == '~!foo is: x86(i686) x86_64')


    def testFlavorSetOps(self):
        a = parseFlavor('foo,bar is: x86')
        b = parseFlavor('foo,~!bar')
        x = a & b
        assert(x.freeze() == '5#use:foo')
        x = a - b
        assert(x.freeze() == '1#x86|5#use:bar')
        x = flavorDifferences((a,b))
        assert(x[a].freeze() == '1#x86|5#use:bar')
        assert(x[b].freeze() == '5#use:~!bar')
        a = DependencySet()
        a.addDep(TroveDependencies, Dependency("foo:runtime"))

        assert(a.copy() & a == a)
        assert(not (a - a))
        x = flavorDifferences((a, a))
        assert(not x[a])

        #now test non-strict ops
        a = parseFlavor('foo,bar')
        b = parseFlavor('~foo,~!bar,bam')
        assert(str(a.intersection(b, strict=False)) == 'foo')
        assert(str(a.difference(b, strict=False)) == 'bar')
        assert(str(flavorDifferences([a, b],strict=False)[a]) == 'bar')
        assert(str(flavorDifferences([a, b],strict=False)[b]) == 'bam,~!bar')

        # test empty set
        assert(flavorDifferences([]) == {})

    def testParseFlavor(self):
        def _test(first, second, testFormat = True):
            flavor = parseFlavor(first)
            assert(str(flavor) == second)
            if testFormat:
                assert(str(parseFlavor(formatFlavor(flavor))) == second)

        _test('','')
        _test('is: mips', 'is: mips')
        _test('is: x86(sse)', 'is: x86(sse)')
        _test('is: x86(!sse)', 'is: x86(!sse)')
        _test('is: x86(sse,mmx)', 'is: x86(mmx,sse)')
        _test('is: x86(~sse,~!mmx)', 'is: x86(~!mmx,~sse)')
        _test('is: x86(~sse,~!mmx) x86_64', 'is: x86(~!mmx,~sse) x86_64')
        _test('is: x86(~sse,~!mmx) x86_64(3dnow)', 'is: x86(~!mmx,~sse) x86_64(3dnow)')
        _test('ssl', 'ssl')
        _test('~ssl', '~ssl')
        _test('gtk,ssl', 'gtk,ssl')
        _test('!gtk,~!ssl', '!gtk,~!ssl')

        full = 'gtk,ssl is: x86(mmx,sse)'
        _test('use: gtk,ssl is: x86(sse,mmx)', full)
        _test('  gtk,ssl is: x86(sse, mmx)  ', full)
        _test('use: gtk,ssl is:x86(  sse,mmx)', full)
        _test('use:    gtk  ,ssl   is:    x86(sse,mmx)', full)
        _test('gtk,ssl is: x86(sse , mmx)', full)
        _test('foo.bar,ssl is: x86', 'foo.bar,ssl is: x86')
        _test('foo-valid.bar,ssl is: x86', 'foo-valid.bar,ssl is: x86')

        _test('is: x86 x86_64(cmov) ppc(cmov)',
              'is: ppc(cmov) x86 x86_64(cmov)')
        _test('is: x86 x86_64 ppc(cmov)',
              'is: ppc(cmov) x86 x86_64')
        _test('is: x86(cmov) x86_64(cmov) ppc(cmov)',
              'is: ppc(cmov) x86(cmov) x86_64(cmov)')
        _test('is: x86 x86_64 ppc',
              'is: ppc x86 x86_64')
        _test('target: x86(cmov)',
              'target: x86(cmov)')
        _test('target: x86', 'target: x86')
        _test('target: x86 x86_64', 'target: x86 x86_64', testFormat=False)
        _test('is: x86 x86_64(cmov) ppc(cmov) target: x86(cmov)',
              'is: ppc(cmov) x86 x86_64(cmov) target: x86(cmov)')
        _test('is: x86 x86_64 ppc(cmov) target: x86(cmov)',
              'is: ppc(cmov) x86 x86_64 target: x86(cmov)')
        _test('is: x86(cmov) x86_64(cmov) ppc(cmov) target: x86(cmov)',
              'is: ppc(cmov) x86(cmov) x86_64(cmov) target: x86(cmov)')
        _test('is: x86 x86_64 ppc target: x86(cmov)',
              'is: ppc x86 x86_64 target: x86(cmov)')
        _test('is: x86 x86_64 ppc target: x86(cmov) x86_64',
              'is: ppc x86 x86_64 target: x86(cmov) x86_64')

        # quick tests to make sure mergeBase behaves itself
        assert(formatFlavor(parseFlavor('is: x86(sse)',
                            mergeBase = parseFlavor('use: gtk')))
                    == 'gtk is: x86(sse)')

        assert(formatFlavor(parseFlavor('gnome is: x86(sse)',
                            mergeBase = parseFlavor('use: gtk')))
                    == 'gnome is: x86(sse)')
        assert(formatFlavor(parseFlavor('use: gnome',
                            mergeBase = parseFlavor('is: x86(mmx)')))
                    == 'gnome is: x86(mmx)')
        assert(formatFlavor(parseFlavor('use: gnome is:',
                            mergeBase = parseFlavor('is: x86(mmx)')))
                    == 'gnome')
        assert(formatFlavor(parseFlavor('use: is: ',
                            mergeBase = parseFlavor('x86(mmx)')))
                    == '')
        assert(formatFlavor(parseFlavor('use: ssl is: x86',
                            mergeBase = parseFlavor('use: gtk is: x86(mmx)')))
                    == 'ssl is: x86')
        assert(formatFlavor(parseFlavor('is: x86(sse)',
                            mergeBase = parseFlavor('use: gtk')))
                    == 'gtk is: x86(sse)')

        # mergeFlavor is separated now
        assert(formatFlavor(mergeFlavor(parseFlavor('gnome is: x86(sse)'),
                            parseFlavor('use: gtk')))
                    == 'gnome is: x86(sse)')
        assert(formatFlavor(mergeFlavor(parseFlavor('use: gnome'),
                            mergeBase = parseFlavor('is: x86(mmx)')))
                    == 'gnome is: x86(mmx)')
        assert(formatFlavor(mergeFlavor(parseFlavor('use: gnome is:'),
                            mergeBase = parseFlavor('is: x86(mmx)')))
                    == 'gnome')
        assert(formatFlavor(mergeFlavor(parseFlavor('use: is: '),
                            mergeBase = parseFlavor('x86(mmx)')))
                    == '')
        assert(formatFlavor(mergeFlavor(parseFlavor('use: ssl is: x86'),
                            mergeBase = parseFlavor('use: gtk is: x86(mmx)')))
                    == 'ssl is: x86')

        assert(formatFlavor(mergeFlavor(parseFlavor('use: ssl is: x86'),
                            mergeBase = parseFlavor('use: gtk is: x86(mmx)')))
                    == 'ssl is: x86')
        assert(formatFlavor(mergeFlavor(parseFlavor('use: ssl'),
                            mergeBase = parseFlavor('is: x86(mmx) x86_64')))
                    == 'ssl is: x86(mmx) x86_64')

    def testScoreFlavors(self):
        def _test(a, b, score):
            assert(parseFlavor(a).score(parseFlavor(b)) == score)
        def _testDep(a, b, score):
            assert(parseDep(a).score(parseDep(b)) == score)
        # use flavor testing
        _test('', '!ssl', 0) 
        _test('', '~!ssl', 1) 
        _test('', '~!ssl, ~!foo', 2)
        _test('', '~ssl', -1)
        _test('', 'ssl', False)

        # arch flavor testing
        _test('is:x86', 'is:x86', 1)
        _test('is:x86', 'is:x86 x86_64', False)
        _test('is:x86 x86_64', 'is:x86 x86_64', 2)
        _test('is:x86(!mmx) x86_64', 'is:x86(~!mmx) x86_64', 3)
        _test('is:x86_64', 'is:x86', False)

        _test('', 'is:x86', False)
        _test('', 'is:x86(!i686)', False)
        _test('is:x86_64', 'is:x86(!i686)', False)

        # dep scoring
        _testDep('trove: foo(a)', 'trove: foo(a)', 3)
        _testDep('trove: foo(a) trove: bar(a)', 'trove: foo(a) trove: bar(a)', 6)



    def testParseDependencies(self):
        def _test(s, result=None):
            if result is None:
                result = s
            # ignore whitespace changes
            assert(str(parseDep(s)).split() == result.split())
        _test('soname: ELF32/libstdc++.so.5(GLIBC_2.0 SysV x86)')
        _test('file: /bin/bash')
        _test('trove: bash')
        _test('trove: libstdc++')
        _test('userinfo: apache')
        _test('groupinfo: apache')
        _test('trove: bash:test')
        _test('CIL: System.EnterpriseServices(1.0.5000.0 2.0.3600.0)')
        _test('trove: sqlite:runtime(addcolumn threadsafe)')
        _test('abi: ELF32(SysV  x86)')
        _test('abi:   ELF32(SysV  x86)  trove:  sqlite:runtime')
        _test('abi: ELF32 ( SysV x86 )', 'abi: ELF32(SysV x86)')
        _test('trove: bash:test()',
              'trove: bash:test') # empty flags list is allowed
        _test('perl: LWP')
        _test('perl: File::Glob')
        _test('perl: File::Glob::More::Globs')
        _test('perl: bits::a.out')
        _test('java: sun.util.calendar.Gregorian')

    def testParseBadDependencies(self):
        def _test(s):
            try:
                depSet = parseDep(s)
            except ParseError:
                return
            else:
                assert(0)
        _test('foo: ELF32/libstdc++.so.5(GLIBC_2.0 SysV x86)') # no such foo
        _test('soname: ELF32/libstdc!!(GLIBC_2.0 SysV x86)') # ! is not valid
                                                             # anywhere
        _test('soname: ELF32/libstdc(GLIBC_2.0,SysV,x86)')
        _test('trove: test:runtime trove: test:')
        _test('CIL: System.EnterpriseServices') # requires flags
        _test('CIL: System.EnterpriseServices()') # requires flags
        _test('trove: test:runtime+') # plus is not a valid ident
        _test('groupinfo: apache(flag)') # flags not allowed
        _test('groupinfo: apache()') # flags not allowed
        _test('use: foo') # use: is disabled
        _test('is: x86') # is: is disabled
        _test('oldsoname: ELF32/libfoo.so.5') # oldsoname: is disabled
        _test('perl: File:SingleColon')
        _test('perl: File::DoubleColon:SingleColon')
        _test('perl: File:SingleColon::DoubleColon')

    def testCompatibleFlavors(self):
        compat = compatibleFlavors
        def compat(f1, f2):
            b = compatibleFlavors(parseFlavor(f1), parseFlavor(f2))
            b2 = compatibleFlavors(parseFlavor(f2), parseFlavor(f1))
            assert(b == b2)
            return b

        assert(compat('foo', '~foo'))
        assert(compat('foo,bar', '~foo is:x86'))
        assert(not compat('foo,bar is:x86_64', '~foo is:x86'))
        assert(compat('foo,bar', ''))
        assert(not compat('foo,bar is:x86(i686)', '~foo is:x86(~!i686)'))

    def testSorting(self):
        # sorting rules:
        # 1. sort by dep class Ids
        # 2. within a dep class, sort by names
        # 3. within a dependency, sort by flags.
        def _testlt(a, b):
            assert(parseDep(a) < parseDep(b))
            assert(parseDep(b) > parseDep(a))

        def _testeq(a, b):
            assert(not cmp(parseDep(a), parseDep(b)))

        # test equality (using cmp)
        _testeq('abi: ELF32(SysV) file:/foo', 'abi: ELF32(SysV) file:/foo')

        # abi has tag 1, file has tag 3
        _testlt('abi: ELF32(SysV) file:/zzz', 'file: /foo')

        # aaa is before bbb
        _testlt('abi: aaa(3)', 'abi: bbb(2)')

        # 2 is before 3
        _testlt('abi: aaa(2)', 'abi: aaa(3)')

    def testUnknownDependency(self):
        # Adding a dependency that is unknown to the current version of the
        # code

        intTag = 65535
        stringTag = "yet-to-be-defined"
        class YetToBeDefinedDependency(DependencyClass):
            tag = intTag
            tagName = stringTag
            justOne = False
            depClass = Dependency

        ds = DependencySet()
        depName = "some"
        depFlag = "flag1"
        ds.addDep(YetToBeDefinedDependency,
            Dependency(depName, [ (depFlag, FLAG_SENSE_REQUIRED) ]))
        frozen = ds.freeze()

        x = ThawDependencySet(frozen)
        self.assertEqual(str(x), "unknown-%s: %s(%s)" % 
            (intTag, depName, depFlag))

    def testUnknownFlavor(self):
        # Adding a flavor that is unknown to the current version of the
        # code

        intTag = 65535
        stringTag = "yet-to-be-defined"
        class YetToBeDefinedFlavor(DependencyClass):
            tag = intTag
            tagName = stringTag
            justOne = False
            depClass = Dependency

        flv = Flavor()
        flvName = "was"
        flvFlag = "flag1"
        flv.addDep(YetToBeDefinedFlavor,
            Dependency(flvName, [ (flvFlag, FLAG_SENSE_REQUIRED) ]))
        frozen = flv.freeze()

        x = ThawFlavor(frozen)
        # The code that implements str is very specific to what's currently
        # implemented
        self.assertEqual(str(x), '')
        # However, it's not the empty flavor
        self.assertNotEqual(x, parseFlavor(''))
        self.assertEqual(x.freeze(), '65535#was:flag1')

    def testFilterFlavor(self):
        def _test(flavor, filterList, result):
            if not isinstance(filterList, (list, tuple)):
                filterList = [filterList]
            filterList = [ parseFlavor(x, raiseError=True) for x in filterList]
            flavor = parseFlavor(flavor, raiseError=True)
            filteredFlavor = filterFlavor(flavor, filterList)
            self.assertEquals(str(filteredFlavor), result)
        _test('is:x86', '', '')
        _test('is:x86', 'is:x86(i586)', 'is: x86')
        _test('readline,!ssl is: x86', ['!readline','ssl'], 'readline,!ssl')

    def testGetSmallestDifference(self):
        def _test(flavor, flavorToMatch, result):
            flavor = parseFlavor(flavor, raiseError=True)
            flavorToMatch = parseFlavor(flavorToMatch, raiseError=True)
            minimalMatch = getMinimalCompatibleChanges(flavor, flavorToMatch)
            self.assertEquals(str(minimalMatch), result)
        _test('is:x86', '', '')
        _test('', 'is:x86', 'is: x86')
        _test('', 'use:', '')
        _test('is:x86', 'is:x86(i686)', 'is: x86(~i686)')
        _test('ssl is:x86', 'is:x86_64(i686)', 'is: x86_64(~i686)')
        _test('~ssl', '!ssl', '~!ssl')
        _test('ssl', '!ssl', '~!ssl')
        _test('~!ssl', '!ssl', '')
        _test('!ssl', '!ssl', '')
        _test('', '!ssl', '')
        _test('ssl', '~!ssl', '~!ssl')
        _test('~ssl', '~!ssl', '')
        _test('~!ssl', '~!ssl', '')
        _test('!ssl', '~!ssl', '')
        _test('', '~!ssl', '')
        _test('ssl', '~ssl', '')
        _test('~ssl', '~ssl', '')
        _test('~!ssl', '~ssl', '')
        _test('!ssl', '~ssl', '~ssl')
        _test('', '~ssl', '')
        _test('ssl', 'ssl', '')
        _test('~ssl', 'ssl', '')
        _test('~!ssl', 'ssl', '')
        _test('!ssl', 'ssl', '~ssl')
        _test('', 'ssl', '~ssl')
        _test('ssl', '', '')
        _test('~ssl', '', '')
        _test('~!ssl', '', '')
        _test('!ssl', '', '')
        _test('', '', '')

    def testCache(self):
        dependencyCache.clear()

        flavor = parseFlavor("is: x86(mmx)").freeze()
        a = ThawFlavor(flavor)
        b = ThawFlavor(flavor)
        assert(a.members[1].members.values()[0] ==
               b.members[1].members.values()[0])
        assert(a.members[1].members.values()[0] ==
               dependencyCache['x86:mmx'])
        del a
        assert(dependencyCache)
        del b
        assert(not dependencyCache)

    def testGetShortFlavorDescriptors(self):
        def _test(flavorList, resultingList, ordered=True):
            flavorList = [parseFlavor(x) for x in flavorList]
            results = getShortFlavorDescriptors(flavorList)
            results = [ str(results[x]) for x in flavorList]
            self.assertEquals(results, resultingList)
        _test(['!foo', ''], ['!foo', ''])
        _test(['!foo', '~!foo'], ['!foo', '~!foo'], False)
        _test(['foo', '~foo'], ['foo', '~foo'], False)

        _test(['foo', '~foo', '~foo'], ['foo', '~foo', '~foo'])
        _test(['foo', '~foo', '!foo'], ['foo', '~foo', '!foo'])
        _test(['foo,bar', '~foo', '!foo'], ['bar-foo', 'foo', ''])

        # test pre existing cases
        _test(['foo is: x86', 'foo,bar is: x86'], ['x86', 'x86-bar'])
        _test(['foo', 'foo,bar'], ['', 'bar'])

    def testPickleDeps(self):
        for text in ('', '~foo.bar target: sparc is: x86_64'):
            flavor = parseFlavor(text)
            self.assertEquals(flavor, pickle.loads(pickle.dumps(flavor)))

        for text in ('', '4#blam|4#foo'):
            dep = ThawDependencySet(text)
            self.assertEquals(dep, pickle.loads(pickle.dumps(dep)))
