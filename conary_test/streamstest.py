# -*- coding: utf-8 -*-
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

from conary.streams import (
        AbsoluteSha1Stream,
        AbsoluteStreamCollection,
        DependenciesStream,
        DYNAMIC,
        InfoStream,
        IntStream,
        LARGE,
        LongLongStream,
        MtimeStream,
        NonStandardSha256Stream,
        OptionalFlavorStream,
        OrderedBinaryStringsStream,
        OrderedStreamCollection,
        PRESERVE_UNKNOWN,
        Sha1Stream,
        ShortStream,
        SKIP_UNKNOWN,
        SMALL,
        splitFrozenStreamSet,
        StreamCollection,
        StreamSet,
        StringsStream,
        StringStream,
        StringVersionStream,
        )
from conary import versions
from conary.deps import deps
from conary.lib import sha1helper

class StreamsTest(testhelp.TestCase):

    def testShortStream(self):
        s = ShortStream(0x1234)
        s2 = ShortStream(s.freeze())
        assert(s == s2)

        s2 = ShortStream(0x1)
        assert(s != s2)
        diff = s.diff(s2)
        assert(diff == "\x12\x34")
        diff2 = s2.diff(s)
        assert(diff2 == "\0\1")
        assert(not s2.twm(diff, s2))
        assert(s == s2)

        s3 = ShortStream(0x2)
        assert(s2.twm(diff2, s3))

        # test diffs against None
        s4 = ShortStream(None)
        diff3 = s4.diff(s3)
        assert(diff3 == '')
        # test that merging a value back to None works
        assert(not s3.twm(diff3, s3))
        assert(s3() == None)
        assert(repr(s3))

        s4.set(1)
        s4.set(None)
        assert(s4.freeze() == '')

        s4.set(1)
        s4.thaw('')
        assert(s4.freeze() == '')
        
    def testIntStream(self):
        s = IntStream(0x12345678)
        s2 = IntStream(s.freeze())
        assert(s == s2)

        s2 = IntStream(0x1)
        assert(s != s2)
        diff = s.diff(s2)
        assert(diff == "\x12\x34\x56\x78")
        diff2 = s2.diff(s)
        assert(diff2 == "\0\0\0\1")
        assert(not s2.twm(diff, s2))
        assert(s == s2)

        s3 = IntStream(0x2)
        assert(s2.twm(diff2, s3))

        # test diffs against None
        s4 = IntStream(None)
        diff3 = s4.diff(s3)
        assert(diff3 == '')
        # test that merging a value back to None works
        assert(not s3.twm(diff3, s3))
        assert(s3() == None)

        s4.set(1)
        s4.set(None)
        assert(s4.freeze() == '')

        s.set(1)
        assert(s() == 1)
        s.thaw('')
        assert(s() == None)

    def testLongLong(self):
        s = LongLongStream(0x12345678)
        s2 = LongLongStream(s.freeze())
        assert(s == s2)

        s2 = LongLongStream(0x1)
        assert(s != s2)
        diff = s.diff(s2)
        assert(diff == "\0\0\0\0\x12\x34\x56\x78")
        diff2 = s2.diff(s)
        assert(diff2 == "\0\0\0\0\0\0\0\1")
        assert(not s2.twm(diff, s2))
        assert(s == s2)

        s3 = LongLongStream(0x2)
        assert(s2.twm(diff2, s3))

        # test diffs against None
        s4 = LongLongStream(None)
        diff3 = s4.diff(s3)
        assert(diff3 == '')
        # test that merging a value back to None works
        assert(not s3.twm(diff3, s3))
        assert(s3() == None)

        # big numbers
        s = LongLongStream(0x1234567812345678)
        assert(s() == 0x1234567812345678)
        s.set(0x8765432187654321)
        assert(s() == 0x8765432187654321)

        s.set(None)
        assert(s.freeze() == '')

        s.set(1)
        s.thaw('')
        assert(s() == None)

    def testString(self):
        s = StringStream("test")
        s2 = StringStream(s.freeze())
        assert(s == s2)

        s2 = StringStream("another")
        assert(s != s2)
        diff = s.diff(s2)
        assert(diff == "test")
        diff2 = s2.diff(s)
        assert(diff2 == "another")
        # after the merge, s2 will be "test"
        assert(not s2.twm(diff, s2))
        assert(s == s2)

        s3 = StringStream("yet one more")
        # check conflict behavior, this should return True
        assert(s2.twm(diff2, s3))
        # doing a merge to the final value should work
        diff = s3.diff(s)
        assert(not s3.twm(diff, s2))

        # check string streams that whole None as the value
        s4 = StringStream(None)
        assert(s3 != s4)
        assert(s3 > s4)
        assert(s4 < s3)
        # attempt to twm into a string that has a value of None
        # and a conflicting "other"
        assert(s4.twm(diff, s2))

        # ensure that two None string streams compare equal
        s5 = StringStream(None)
        assert(s4 == s5)

        # ensure that invalid types are handled properly
        try:
            StringStream(1)
        except TypeError, e:
            assert (str(e) == 'frozen value must be None or a string')
        else:
            raise

    def testStrings(self):
        s = StringsStream("test1\0test2")
        s2 = StringsStream(s.freeze())
        assert(s == s2)
        assert(s == ['test1', 'test2'])

        s2 = StringsStream('another1\0another2')
        assert(s != s2)
        diff = s.diff(s2)
        assert(diff == 'test1\0test2')
        diff2 = s2.diff(s)
        assert(diff2 == 'another1\0another2')
        assert(not s2.twm(diff, s2))
        assert(s == s2)

        # there cannot be conflicts in a StringsStream twm
        s3 = StringsStream('yet one more\0last one')
        assert(s2.twm(diff2, s3) == False)
        assert(s2 == ['another1', 'another2'])

        # test diffs to an empty list
        s4 = StringsStream("test1\0test2")
        s5 = StringsStream()
        diff = s5.diff(s4)
        assert (diff == '')
        assert (s4.twm(diff, s4) == False)
        assert (s4 == [])

        # test diffs from an empty list
        s4 = StringsStream('test1\0test2')
        s5 = StringsStream()
        diff = s4.diff(s5)
        assert (diff == 'test1\0test2')
        assert (s5.twm(diff, s5) == False)
        assert (s5 == ['test1', 'test2'])

    def testOrderedBinaryStrings(self):
        s = OrderedBinaryStringsStream()
        s.append('hello')
        s.append('world\0!')
        frz = s.freeze()
        self.assertEqual(frz, '\x05hello\x07world\x00!')

        s2 = OrderedBinaryStringsStream(frz)
        self.assertEqual(s, s2)

        s = OrderedBinaryStringsStream()
        for x in ((1 << 5), (1 << 6), (1 << 7),
                  (1 << 14), (1 << 15), (1 << 16),
                  (1 << 20)):
            s.append('1' * x)
        frz = s.freeze()
        s2 = OrderedBinaryStringsStream(frz)
        self.assertEqual(s, s2)

    def testInfoStream(self):
         i = InfoStream()
         self.assertRaises(NotImplementedError, i.freeze)
         self.assertRaises(NotImplementedError, i.diff, None)
         self.assertRaises(NotImplementedError, i.twm, None, None)

    def testDependencies(self):
        depSet = deps.DependencySet()
        depSet.addDep(deps.FileDependencies, deps.Dependency('/foo/bar', []))
        d = DependenciesStream()
        d.set(depSet)
        assert(d() == depSet)
        d2 = DependenciesStream(d.freeze())
        assert(d == d2)
        assert(str(d2.deps) == "file: /foo/bar")

        # currently there should be no diff between d and d2
        assert(d.diff(d2) is None)

        # add another dep to d (by modifying its dependency set)
        # now the diff should contain all of the dependencies in d
        # (when the diff method is used, d2 would be the "old" dependency
        # set)
        depSet.addDep(deps.FileDependencies, deps.Dependency('/foo/baz', []))
        d.set(depSet)
        diff = d.diff(d2)
        d3 = DependenciesStream(diff)
        assert(d == d3)

        # now change d's depset again, and do a three way merge back to
        # the d3 state
        depSet.addDep(deps.FileDependencies, deps.Dependency('/foo/fred', []))
        base = deps.DependencySet()
        d.twm(diff, base)
        assert(d == d3)

        # verify that going from an empty depSet to one with deps in it
        # works
        depSet = deps.DependencySet()
        d = DependenciesStream()
        d.set(depSet)
        diff = d2.diff(d)
        d.twm(diff, d)
        assert(d == d2)

        # verify that going from a depSet with deps in it to an empty one
        # works
        depSet = deps.DependencySet()
        d = DependenciesStream()
        d.set(depSet)
        assert(d2.freeze() != '')
        assert(d.freeze() == '')
        diff = d.diff(d2)
        assert(diff == '')
        d2.twm(diff, d2)
        assert(d == d2)
        assert(d2.freeze() == '')

    def testOptionalFlavor(self):
        depSet = deps.ThawFlavor('is:x86(i486 i586)')
        fs = OptionalFlavorStream()
        assert(fs.freeze() == '')
        fs.set(None)
        assert(fs.freeze() == '\0')
        fs.thaw('')
        assert(fs.freeze() == '')
        fs.thaw('\0')
        assert(fs.freeze() == '\0')

        fs.set(depSet)
        assert(fs.freeze() == 'is:x86(i486 i586)')
        fs2 = OptionalFlavorStream('\0')

        assert(fs2.freeze() == '\0')
        diff = fs2.diff(fs)
        newfs = fs.copy()
        newfs.twm(diff, newfs)
        assert(newfs == fs2)

        diff = fs.diff(fs2)
        newfs = fs2.copy()
        newfs.twm(diff, newfs)
        assert(newfs == fs)

    def testStreamSet(self):
        frozen = '\x01\x00\x07thename\x02\x00\x04\x00\x00z\xe3'

        class Rigid(StreamSet):
            streamDict = { 1 : ( SMALL, StringStream, "name" ) }

        class Flexible(StreamSet):
            streamDict = { 1 : ( SMALL, StringStream, "name" ) }
            ignoreUnknown = SKIP_UNKNOWN

        class Superset(StreamSet):
            streamDict = { 1 : ( SMALL, StringStream, "name" ),
                           2 : ( SMALL, IntStream, "number" ),
                           3 : ( SMALL, StringStream, "unused" ) }

        # "True" used to mean SKIP_UNKNOWN; make sure it still does
        assert(Flexible.ignoreUnknown == True)

        stream = Superset()
        stream.name.set('thename')
        stream.number.set(31459)
        frz = stream.freeze()
        expected = '\x01\x00\x07thename\x02\x00\x04\x00\x00z\xe3'
        assert(frz == expected)
        assert(stream.find(1, frz)() == "thename")
        assert(stream.find(2, frz)() == 31459)
        assert(stream.find(3, frz) == None)

        # test comparison and hash functions
        stream2 = Superset()
        stream2.name.set('thename')
        stream2.number.set(31459)
        assert(stream == stream2)
        assert(hash(stream) == hash(stream2))
        stream2.number.set(31460)
        assert(stream != stream2)
        assert(hash(stream) != hash(stream2))

        frz = stream2.freeze()
        split = splitFrozenStreamSet(frz)
        self.assertEqual(split, [(1, 'thename'), (2, '\x00\x00z\xe4')])

        # test ignoreUnknown for thawing
        self.assertRaises(ValueError, Rigid, frz)
        stream2 = Flexible(frz)
        assert(stream2.freeze() == '\x01\x00\x07thename')

        # test ignoreUnknown for twm
        first = Superset()
        first.name.set('name')
        first.number.set(4)
        second = Superset()
        second.name.set('another')
        second.number.set(7)
        diff = second.diff(first)

        flex = Flexible()
        flex.name.set('name')
        flex.twm(diff, flex)
        assert(flex.name() == 'another')
        assert(flex.freeze() == '\x01\x00\x07another')

        rigid = Rigid()
        rigid.name.set('name')
        self.assertRaises(ValueError, rigid.twm, diff, rigid)

        # test to make sure that empty streams are excluded properly
        stream.name.set('')
        frz = stream.freeze()
        expected = '\x02\x00\x04\x00\x00z\xe3'
        assert(frz == expected)

        # test the error handling when a class is missing _streamDict
        class Broken(StreamSet):
            streamDicty = { 1 : ( SMALL, StringStream, "name" ),
                           2 : ( SMALL, IntStream, "number" ) }
        try:
            broken = Broken()
        except ValueError, e:
            assert(str(e) == 'Broken class is missing the streamDict class variable')
        else:
            raise


        class Large(StreamSet):
            streamDict = { 1 : ( LARGE, StringStream, 'bigname' ) }

        large = Large()
        bigdata = '1' * 35000
        large.bigname.set(bigdata)
        frz = large.freeze()
        assert (frz == '\x01\x80\x00\x88\xb8' + bigdata)
        large2 = Large(frz)
        assert(large == large2)

        class Overflow(StreamSet):
            streamDict = { 1 : ( SMALL, StringStream, 'bigname' ) }
        overflow = Overflow()
        overflow.bigname.set(bigdata)
        try:
            frz = overflow.freeze()
        except TypeError, e:
            assert(str(e) == 'short int overflow')
        else:
            raise


        class Dynamic(StreamSet):
            streamDict = { 1 : ( DYNAMIC, StringStream, 'dynamic' ) }
        dynamic = Dynamic()
        dynamic.dynamic.set(bigdata)
        frz = dynamic.freeze()
        assert (frz == '\x01\x80\x00\x88\xb8' + bigdata)
        dynamic2 = Dynamic(frz)
        assert(dynamic == dynamic2)

        dynamic.dynamic.set('foo')
        frz = dynamic.freeze()
        assert (frz == '\x01\x00\x03foo')
        dynamic2 = Dynamic(frz)
        assert(dynamic == dynamic2)

    def testMtime(self):
        m1 = MtimeStream(100)
        m2 = MtimeStream(200)
        # mtimes always compare true
        assert(m1 == m2)

    def testStreamSetWithLargeTag(self):
        class Foo1(StreamSet):
            streamDict = { 128 : ( DYNAMIC, StringStream, 'x' ) }
        class Foo2(StreamSet):
            streamDict = { 255 : ( DYNAMIC, StringStream, 'x' ) }

        for cls in (Foo1, Foo2):
            f = cls()
            f.x.set('hello')
            f2 = cls(f.freeze())
            self.assertTrue(f2.x(), 'hello')

        class Foo3(StreamSet):
            streamDict = { 256 : ( DYNAMIC, StringStream, 'x' ) }

        f = Foo3()
        f.x.set('hello')
        try:
            f2 = Foo3(f.freeze())
        except TypeError, e:
            self.assertEqual(str(e), 'tag number overflow. max value is uchar')

    def testStreamCollection(self):

        class Collection(StreamCollection):
            streamDict = { 1 : StringStream,
                           2 : IntStream }

        c = Collection()
        c.addStream(1, StringStream("first"))
        c.addStream(1, StringStream("second"))
        c.addStream(1, StringStream("third"))
        assert(sorted([x() for x in c.getStreams(1) ]) == 
                [ "first", "second", "third" ])
        assert(sorted([x[1]() for x in c.iterAll() ]) == 
                [ "first", "second", "third" ])

        c.addStream(2, IntStream(1))
        c.addStream(2, IntStream(2))
        c.addStream(2, IntStream(3))
        assert(sorted([x() for x in c.getStreams(2) ]) == [ 1, 2, 3] )
        assert(sorted([x[1]() for x in c.iterAll() ]) == 
                    [1, 2, 3, 'first', 'second', 'third'] )

        c2 = Collection(c.freeze())
        assert(c == c2)

        c2.addStream(2, IntStream(4))
        c2.delStream(2, IntStream(3))
        d = c2.diff(c)
        assert(d == '\x00\x01\x00\x01\x02\x00\x04\x00\x00\x00\x03\x02\x00\x04\x00\x00\x00\x04')
        c.twm(d, c)
        assert(c == c2)
        assert(c2.diff(c) is None)

        # test overflow
        c2 = Collection()
        c2.addStream(1, StringStream(' ' * 65536))
        self.assertRaises(OverflowError, c2.freeze)

    def testAbsoluteStreamCollection(self):
        class Collection(AbsoluteStreamCollection):
            streamDict = { 1 : StringStream,
                           2 : IntStream }

        c = Collection()
        c.addStream(1, StringStream("first"))
        c.addStream(1, StringStream("second"))
        c.addStream(1, StringStream("third"))
        c.addStream(2, IntStream(1))
        c.addStream(2, IntStream(2))
        c.addStream(2, IntStream(3))

        c2 = Collection(c.freeze())
        assert(c == c2)

        c2.addStream(2, IntStream(4))
        c2.delStream(2, IntStream(3))
        d = c2.diff(c)
        assert(d == '\x01\x00\x05first\x01\x00\x06second\x01\x00\x05third\x02\x00\x04\x00\x00\x00\x01\x02\x00\x04\x00\x00\x00\x02\x02\x00\x04\x00\x00\x00\x04')
        
        c.twm(d, c)
        assert(c == c2)
        assert(c2.diff(c) == d)

        c3 = Collection(d)
        assert(c2 == c3)

    def testOrderedStreamCollection(self):
        class Collection(OrderedStreamCollection):
            streamDict = { 1 : StringStream }

        c = Collection()
        # make sure we can store Great Big Stuff
        s = ' ' * (1 << 17)
        c.addStream(1, StringStream(s))
        frz = c.freeze()
        self.assertEqual(frz, '\x01\x80\x02\x00\x00' + s)

        c = Collection()
        # make sure we can store smaller stuff
        s = ' ' * 63
        c.addStream(1, StringStream(s))
        frz = c.freeze()
        self.assertEqual(frz, '\x01?' + s)

        s2 = ' ' * 64
        c.addStream(1, StringStream(s2))
        frz = c.freeze()
        self.assertEqual(frz, '\x01?' + s + '\x01@@' + s2)

        # test add via diff
        c1 = Collection()
        c1.addStream(1, StringStream('one'))
        c1.addStream(1, StringStream('two'))
        c2 = Collection(c1.freeze())
        c2.addStream(1, StringStream('three'))
        c2.addStream(1, StringStream('four'))
        diff = c2.diff(c1)
        c1.twm(diff, c1)
        self.assertEqual(c1, c2)

        # test del via diff
        c1 = Collection()
        c1.addStream(1, StringStream('one'))
        c1.addStream(1, StringStream('two'))
        c2 = Collection(c1.freeze())
        c1.addStream(1, StringStream('three'))
        diff = c2.diff(c1)
        c1.twm(diff, c1)
        self.assertEqual(c1, c2)
        self.assertEqual([ x() for x in c1.getStreams(1) ],
                             ['one', 'two'])

    def testSha1Stream(self):
        s1 = Sha1Stream()
        s1.set('1' * 20)
        s2 = Sha1Stream()
        s2.set('2' * 20)
        d = s1.diff(s2)
        assert(d == '1' * 20)
        s2.twm(d, s2)
        assert(s2() == '1' * 20)
        assert(s1.freeze() == '1' * 20)

        # make sure that a diff against the same thing yields None
        assert(s1.diff(s2) == None)

        s1.setFromString('61' * 20)
        assert (s1() == 'a' * 20)

        try:
            s1.set('a' * 21)
        except:
            pass
        else:
            assert(0)

        try:
            s1.set('')
        except:
            pass
        else:
            assert(0)

        s1 = Sha1Stream()
        s1.compute('hello')
        assert(s1() == '\xaa\xf4\xc6\x1d\xdc\xc5\xe8\xa2\xda\xbe' \
                       '\xde\x0f\x3b\x48\x2c\xd9\xae\xa9\x43\x4d')
        assert(s1.verify('hello'))
        assert(not s1.verify('hello there'))

    def testAbsoluteSha1Stream(self):
        s1 = AbsoluteSha1Stream()
        s1.set('1' * 20)
        s2 = AbsoluteSha1Stream()
        s2.set('2' * 20)
        d = s1.diff(s2)
        assert(d == '1' * 20)
        s2.twm(d, s2)
        assert(s2() == '1' * 20)
        assert(s1.freeze() == '1' * 20)

        # make sure that a diff against the same thing yields s1()
        assert(s1.diff(s2) == '1' * 20)

        s1.setFromString('61' * 20)
        assert (s1() == 'a' * 20)

        try:
            s1.set('a' * 21)
        except:
            pass
        else:
            assert(0)

        s1.set('')
        assert(s1.diff(s2) == '')

    def testSha256Stream(self):
        s1 = NonStandardSha256Stream()
        s1.set('1' * 32)
        assert(s1() == '1' * 32)
        s2 = NonStandardSha256Stream(s1.freeze())
        assert(s1.freeze() == '1' * 32)

        s1 = NonStandardSha256Stream()
        s1.compute('hello')
        hash = \
            '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'
        assert(sha1helper.sha256ToString(s1()) == hash)
        assert(s1.verify('hello'))
        assert(not s1.verify('hello there'))
        s1 = NonStandardSha256Stream()
        s1.compute('\0' * 55)
        hash = \
            'df22e3f97aeb78f4cd406fea9719068123762a22b04157b2fca4f72cba173dc4'
        # NOTE: correct sha256 in this case has digest
        # 02779466cdec163811d078815c633f21901413081449002f24aa3e80f0b88ef7
        assert(sha1helper.sha256ToString(s1()) == hash)
        assert(s1.verify('\0' * 55))

    def testStringVersion(self):
        # make sure you can represent VERSION -> None diff
        v1 = StringVersionStream()
        v2 = StringVersionStream()
        v1.set(versions.ThawVersion('/a@b:c/15:1.1-1'))
        assert(v1.freeze() == '/a@b:c/1.1-1')
        v1.twm(v2.diff(v1), v1)
        assert(v1.freeze() == '')

        # make sure that twm works properly
        v1.set(versions.ThawVersion('/a@b:c/15:1.1-1'))
        v2 = StringVersionStream()
        diff = v1.diff(v2)
        v2.twm(diff, v2)
        assert(v1 == v2)
        assert(v1.diff(v2) is None)

    def testUnicodeStringStreamSet(self):
        ss = StringStream()
        ns = 'Foo bar baz quux'
        us = u'Iñtërnâtiônàlizætiøn'

        # sanity check on regular non-unicode string
        ss.set(ns)
        assert(ss.freeze() == ns)

        # now for the unicode string
        ss.set(us)
        assert(ss.freeze() == us.encode('utf-8'))
        nss = StringStream()
        nss.thaw(ss.freeze())
        assert(ss == nss)
        assert(nss() == us.encode('utf-8'))

    def testUnknownStreamSetEntries(self):
        class Subset(StreamSet):
            ignoreUnknown = PRESERVE_UNKNOWN
            streamDict = { 1 : ( SMALL, StringStream, "name" ) }

        class SubParent(StreamSet):
            ignoreUnknown = PRESERVE_UNKNOWN
            streamDict = { 1 : ( DYNAMIC, Subset, "nested" ) }

        class Superset(StreamSet):
            streamDict = { 1 : ( SMALL, StringStream, "name" ),
                           2 : ( SMALL, IntStream, "number" ),
                           3 : ( LARGE, IntStream, "number2" ) }

        class SuperParent(StreamSet):
            streamDict = { 1 : ( DYNAMIC, Superset, "nested" ) }

        super = Superset()
        super.name.set("test")
        super.number.set(12)
        super.number2.set(34)

        sub = Subset(super.freeze())
        assert(sub.freeze() == super.freeze())
        assert(sub.freeze(freezeUnknown = False) == '\x01\x00\x04test')
        known = sub.freeze(freezeUnknown = False)
        unknown = sub.freeze(freezeKnown = False)
        assert(sub.freeze() == known + unknown)
        sub = Subset()
        sub.thaw(known)
        sub.thaw(unknown)
        assert(sub.freeze() == super.freeze())

        super = SuperParent()
        super.nested.name.set("test")
        super.nested.number.set(12)
        super.nested.number2.set(34)

        sub = SubParent(super.freeze())
        assert(sub.freeze() == super.freeze())
        otherSub = SubParent()
        otherSub.nested.name.set("test")
        assert(otherSub.freeze() == sub.freeze(freezeUnknown = False))


        sub2 = SubParent()
        self.assertRaises(ValueError, sub.diff, sub2)
        self.assertRaises(ValueError, sub2.diff, sub)

        old = Superset()
        old.name.set('hello')
        new = Superset()
        new.name.set('world')
        new.number.set(10)
        new.number2.set(20)

        diff = new.diff(old)
        sub = Subset(old.freeze())
        sub.twm(diff, sub)
        assert(sub.freeze() == new.freeze())

        # this sets number2, which means number1 has to get inserted into the
        # unknown list rather than just appended
        old.number2.set(20)
        diff = new.diff(old)
        sub = Subset(old.freeze())
        sub.twm(diff, sub)
        assert(sub.freeze() == new.freeze())

        frz = new.freeze()
        split = splitFrozenStreamSet(frz)

        self.assertEqual(split, [(1, 'world'),
                                     (2, '\x00\x00\x00\n'),
                                     (3, '\x00\x00\x00\x14')])

    def testDerivedStreamSet(self):
        # make sure _streamDict doesn't get honored through inheritance
        class Parent(StreamSet):
            streamDict = { 1 : ( SMALL, StringStream, "parent" ) }

        class Child(Parent):
            streamDict = { 2 : ( SMALL, StringStream, "child" ) }

        p = Parent()
        c = Child()
        assert(hasattr(p, 'parent'))
        assert(hasattr(c, 'child'))

    def testFindOnUnInitedStream(self):
        # CNY-1524
        class Blah(StreamSet):
            streamDict = { 1 : ( SMALL, StringStream, 'blah' ) }

        class Blah2(StreamSet):
            streamDict = { 1 : ( SMALL, StringStream, 'blah' ) }

        b = Blah()
        b.blah.set('blah')
        frz = b.freeze()
        self.assertEqual(Blah2.find(1, frz)(), 'blah')
