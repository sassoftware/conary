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


import unittest
import cPickle

SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2

from conary.errors import ParseError, VersionStringError

from conary import versions
from conary.versions import AbstractRevision
from conary.versions import Label
from conary.versions import LocalLabel
from conary.versions import EmergeLabel
from conary.versions import CookLabel
from conary.versions import RollbackLabel
from conary.versions import NewVersion
from conary.versions import ThawVersion
from conary.versions import VersionFromString
from conary.versions import Revision
from conary.versions import SerialNumber
from conary.versions import strToFrozen
from conary.versions import VersionSequence

class VersionsTest(unittest.TestCase):

    def testSerialNumber(self):
        assert(not SerialNumber('0'))
        assert(SerialNumber('1'))

    def testRevision(self):
        try:
            Revision("-")
        except ParseError, e:
            assert(str(e) == "bad release string: -")
        else:
            self.fail("ParseError expected")

        self.assertRaises(ParseError, Revision, "1-abc")
        self.assertRaises(ParseError, Revision, "1-3:4")

        v = Revision(".2a-3")
        assert(v.getVersion() == ".2a")

        v = Revision("1.2a-3")
        assert(v.getVersion() == "1.2a")

        self.assertRaises(AttributeError, v.__setattr__, "foo", 1)

        v2 = Revision("1.2a-3")
        assert(v == v2)
        assert(v2 == v)

        v2 = Revision("1.2a-2")
        assert(not v == v2)
        v2._incrementSourceCount(0)
        assert(v == v2)
        assert(v2.asString() == "1.2a-3")
        assert(str(v2) == "1.2a-3")

        assert(AbstractRevision() != v2)
        assert(v2 != AbstractRevision())

        self.assertRaises(ParseError, Revision, "1@2")
        self.assertRaises(ParseError, Revision, "12")
        self.assertRaises(ParseError, Revision, "1-2.a")

        v = Revision("a(b)-1")
        assert(str(v) == "a(b)-1")

        for ch in '!"#$%&\'*-/:<=>?@[]^`{|}\\':
            s = "1%s-1" % ch
            self.assertRaises(ParseError, Revision, s)

        for i in range(0, 0x20):
            s = "1%s-1" % chr(i)
            self.assertRaises(ParseError, Revision, s)

        for i in range(0x7f, 0xff):
            s = "1%s-1" % chr(i)
            self.assertRaises(ParseError, Revision, s)

        v = Revision("15:1.2a-2", frozen = True)
        assert(v.timeStamp == 15)
        assert(v.asString() == "1.2a-2")
        assert(v.freeze() == "15.000:1.2a-2")

        v = Revision("1.2-2.1")
        assert(v.asString() == "1.2-2.1")
        v._incrementSourceCount(0)
        assert(v.asString() == "1.2-2.2")
        v._incrementSourceCount(0)
        assert(v.asString() == "1.2-2.3")

        v = Revision("1.2-2.1-3.4")
        assert(v.asString() == "1.2-2.1-3.4")

        v = Revision("1.2-1")
        v._incrementSourceCount(2)
        assert(v.asString() == "1.2-1.0.1")

        v = Revision("1.2-1")
        v._incrementSourceCount(3)
        assert(v.asString() == "1.2-1.0.0.1")

    def testLabel(self):
        self.assertRaises(ParseError, Label, "1.2-1")
        self.assertRaises(ParseError, Label, "foo@bar@bang")
        self.assertRaises(ParseError, Label, "foo@bar@bang:bash")
        self.assertRaises(ParseError, Label, "foo:bar@bang")
        self.assertRaises(ParseError, Label, "bar@bang")
        self.assertRaises(ParseError, Label, "bang")
        self.assertRaises(ParseError, Label, "foo:bang")
        self.assertRaises(ParseError, Label, "foo@bar:ba:ng")
        self.assertRaises(ParseError, Label, "1/2@foo:bar")
        self.assertRaises(ParseError, Label, "1'2@foo:bar")
        self.assertRaises(ParseError, Label, '1"2@foo:bar')
        self.assertRaises(ParseError, Label, '1\\2@foo:bar')
        self.assertRaises(ParseError, Label, '1(2@foo:bar')
        self.assertRaises(ParseError, Label, '1)2@foo:bar')
        self.assertRaises(ParseError, Label, '1[2@foo:bar')
        self.assertRaises(ParseError, Label, '1]2@foo:bar')
        self.assertRaises(ParseError, Label, "foo@bar:")
        self.assertRaises(ParseError, Label, "foo@:bar")
        self.assertRaises(ParseError, Label, "foo=bar@bang:baz")

        b = Label("this@nm:that")
        assert(b.getHost() == "this")
        b2 = Label("this@nm:that")
        assert(b == b2)
        assert(b2 == b)
        b2 = Label("this@nm:tha")
        assert(not b == b2)
        assert(not b2 == b)

        self.assertRaises(AttributeError, b.__setattr__, "foo", 1)

    def testSort(self):
        lst = [] 
        for i in range(1, 10):
            lst.append(ThawVersion("/foo.com@spc:bar/%d:1.0-%d" %((10-i), i)))
        # as time goes forward, source count #s go down
        # sorting is by timestamp only, so version with lowest timestamp
        # should be first in resulting list 
        lst.sort()
        for i in range(1, 10):
            assert(lst[i-1].freeze() == "/foo.com@spc:bar/%d.000:1.0-%d" %
                                                                (i, (10-i)))

    def testCompareVersionSequence(self):
        # CNY-2020
        v1 = VersionSequence([Label('foo@bar:baz')])
        v2 = VersionSequence([Label('foo@bar:beep')])
        self.failUnlessEqual(v1, v1)
        self.failUnless(cmp(v1, v2) in [-1, 1])
        self.failUnless(cmp(v2, v1) in [-1, 1])
        self.failUnlessEqual(cmp(v1, v2) + cmp(v2, v1), 0)

        # This should work, even if we return NotImplemented
        v3 = VersionSequence([Revision('1-1-1')])
        self.failUnless(cmp(v1, v3) in [-1, 1])
        self.failUnless(cmp(v3, v1) in [-1, 1])
        self.failUnlessEqual(cmp(v1, v3) + cmp(v3, v1), 0)

        # Compare with a random string, and make sure it's stable too
        v3 = 'abc'
        self.failUnless(cmp(v1, v3) in [-1, 1])
        self.failUnless(cmp(v3, v1) in [-1, 1])
        self.failUnlessEqual(cmp(v1, v3) + cmp(v3, v1), 0)

    def testLocalLabel(self):
        l = LocalLabel()
        l2 = LocalLabel()
        assert(l == l2)
        b = Label("this@some:that")
        assert(not l == b)

        self.assertRaises(AttributeError, b.__setattr__, "foo", 1)

    def testVersion(self):
        self.assertRaises(ParseError, VersionFromString, '0.50.1')


        verStr = "/foo.com@spc:bar/1.000:1.2-3/bang.com@spc:branch/10.000:2.4-5"
        verStr2 = "/foo.com@spc:bar/1.000:1.2-3/bang.com@spc:branch/15.000:2.4-6"
        v = ThawVersion(verStr)
        assert(v.freeze() == verStr)
        assert(v.asString(VersionFromString("/foo.com@spc:bar")) == 
                    "1.2-3/bang.com@spc:branch/2.4-5")
        assert(v.timeStamps() == [ 1, 10 ])
        v = v.copy()
        v.incrementSourceCount()
        assert(v.asString() == ThawVersion(verStr2).asString())
        assert(v.getHost() == 'bang.com')

        # test that cPickle works on a Version object (the changeset cache
        # database pickles versions)
        vpickled = cPickle.dumps(v)
        vunpickled = cPickle.loads(vpickled)
        assert(vunpickled.asString() == v.asString())

        v2 = VersionFromString("/foo.com@spc:bar/1.2-3/bang.com@spc:branch/2.4-5",
                                timeStamps = [1.000, 10.000])
        assert(v2.freeze() == verStr)
        assert([x.asString() for x in v2.iterLabels() ] == ['foo.com@spc:bar', 'bang.com@spc:branch'])

        last = v.trailingRevision()
        assert(last.asString() == "2.4-6")
        assert(not v.onLocalLabel())
        assert(not v.isInLocalNamespace())
        assert(not v.onEmergeLabel())
        assert(not v.onLocalCookLabel())
        assert(v2.getHost() == 'bang.com')

        assert(v.trailingLabel() == Label('bang.com@spc:branch'))

        branch = v.branch()
        assert(branch.getHost() == 'bang.com')
        strrep = branch.asString()
        assert(strrep == "/foo.com@spc:bar/1.2-3/bang.com@spc:branch")
        branch2 = VersionFromString(branch.asString())
        assert(branch == branch2)
        
        frozen = branch.freeze()
        branch2 = ThawVersion(frozen)
        assert(branch2 == branch)

        newVer = branch2.createVersion(Revision("1.1-2"))
        assert(newVer.asString() == "/foo.com@spc:bar/1.2-3/bang.com@spc:branch/1.1-2")
        assert(not newVer.onLocalLabel())
        assert(not newVer.isInLocalNamespace())
        assert(not newVer.onEmergeLabel())
        assert(not newVer.onLocalCookLabel())
        assert(not newVer.isOnLocalHost())
        
        assert(newVer.canonicalVersion() == newVer)
        assert(v.hasParentVersion())
        parent = v.parentVersion()
        assert(not parent.hasParentVersion())
        assert(parent.asString() == "/foo.com@spc:bar/1.2-3")

        # check emerge label
        emerge = parent.createBranch(EmergeLabel(), withVerRel = 1)
        assert(emerge.getHost() == 'local')
        assert(emerge.asString() == "/foo.com@spc:bar/1.2-3/local@local:EMERGE/3")
        assert(not emerge.onLocalLabel())
        assert(emerge.onEmergeLabel())
        assert(not emerge.onRollbackLabel())
        assert(not emerge.onLocalCookLabel())
        assert(emerge.isOnLocalHost())
        assert(emerge.isInLocalNamespace())

        # check local cook label
        cook = parent.createBranch(CookLabel(), withVerRel = 1)
        assert(cook.asString() == "/foo.com@spc:bar/1.2-3/local@local:COOK/3")
        assert(not cook.onLocalLabel())
        assert(not cook.onEmergeLabel())
        assert(not cook.onRollbackLabel())
        assert(cook.onLocalCookLabel())
        assert(cook.isOnLocalHost())
        assert(cook.isInLocalNamespace())

        # check local rollback label
        branch2 = parent.createBranch(RollbackLabel(), withVerRel = 1)
        assert(branch2.asString() == "/foo.com@spc:bar/1.2-3/local@local:ROLLBACK/3")
        assert(not branch2.onLocalLabel())
        assert(not branch2.onEmergeLabel())
        assert(    branch2.onRollbackLabel())
        assert(not branch2.onLocalCookLabel())
        assert(branch2.isOnLocalHost())
        assert(branch2.isInLocalNamespace())
       
        # check local branch label
        branch2 = parent.createBranch(LocalLabel(), withVerRel = 1)
        assert(branch2.asString() == "/foo.com@spc:bar/1.2-3/local@local:LOCAL/3")
        assert(branch2.onLocalLabel())
        assert(not branch2.onEmergeLabel())
        assert(not branch2.onRollbackLabel())
        assert(not branch2.onLocalCookLabel())
        assert(branch2.isOnLocalHost())
        assert(branch2.isInLocalNamespace())
       
        branch3 = VersionFromString(branch2.asString())
        assert(branch2 == branch3)

        branch2 = branch2.branch()
        assert(branch2.asString() == "/foo.com@spc:bar/1.2-3/local@local:LOCAL")

        parent = v.parentVersion()
        assert(parent.asString() == "/foo.com@spc:bar/1.2-3")
        branch2 = parent.createBranch(LocalLabel())
        assert(branch2.asString() == "/foo.com@spc:bar/1.2-3/local@local:LOCAL")

        shadow = parent.branch().createShadow(Label('foo.com@spc:shadow'))
        assert(shadow.asString() == "/foo.com@spc:bar//shadow")
        assert(shadow.getHost() == 'foo.com')

        branch = VersionFromString("/foo.com@spc:bar")
        v = VersionFromString("1.2-3", branch)
        assert(v.asString() == "/foo.com@spc:bar/1.2-3")

        # test getBinaryVersion and getSourceVersion
        v = ThawVersion(verStr)
        b = v.getBinaryVersion()
        assert(b.asString() == "/foo.com@spc:bar/1.2-3-0/bang.com@spc:branch/2.4-5")

        # make sure slots are working
        v = ThawVersion("/foo.com@spec:bar/10:1.2-3")
        self.assertRaises(AttributeError, v.__setattr__, "foo", 1)
        v = ThawVersion(v.freeze())
        self.assertRaises(AttributeError, v.__setattr__, "foo", 1)

        v = VersionFromString('/localhost@rpl:linux/1.0-1-0/'
                              'local@local:EMERGE/1/COOK/2')
        assert(VersionFromString(v.asString()) == v)

    def testIsInLocalNamespace(self):
        v = VersionFromString('/foo.com@local:linux/1.0-1')
        assert(v.isInLocalNamespace())

    def testParenthood(self):
        v = ThawVersion("/foo.com@spc:bar/123.0:1.2-3/branch/456:1.1-2")
        assert(v.hasParentVersion())
        parentVersion = v.parentVersion()
        assert(parentVersion.asString() == "/foo.com@spc:bar/1.2-3")
        assert(parentVersion.trailingRevision().timeStamp == 123)

        v = ThawVersion('/conary.rpath.com@rpl:devel//1//rpl-live.rb.rpath.com@rpl:1//local@local:COOK/123:1.0.1-0.6')
        assert(v.hasParentVersion())
        parentVersion = v.parentVersion()
        assert(parentVersion.asString() == '/conary.rpath.com@rpl:devel//1//rpl-live.rb.rpath.com@rpl:1/1.0.1-0.6')
        assert(parentVersion.trailingRevision().timeStamp == 0)

    def testShadowParenthood(self):
        v = VersionFromString("/foo.com@spc:bar//shadow1/1.0-2.1")
        # should this really have a parent?  
        assert(v.hasParentVersion())
        assert(v.parentVersion().asString() == "/foo.com@spc:bar/1.0-2")

        # 1. shadow source. 2. cook on shadow1. 3. shadow binary -> has parent
        v = VersionFromString("/foo.com@spc:bar//shadow1//shadow2/1.0-2.1")
        assert(v.hasParentVersion())
        assert(v.parentVersion().asString() == "/foo.com@spc:bar//shadow1/1.0-2.1")

        # if you change the upstream version for a trove, it doesn't
        # have a parent anymore!
        v = VersionFromString("/foo.com@spc:bar//shadow1/3.0-0.1")
        assert(not v.hasParentVersion())

        # 1. shadow source. 2.change version 3. shadow source -> has parent
        v = VersionFromString("/foo.com@spc:bar//shadow1//shadow2/3.0-0.1.1")
        assert(v.hasParentVersion())
        assert(v.parentVersion().asString() == "/foo.com@spc:bar//shadow1/3.0-0.1")

        v = VersionFromString("/foo.com@spc:bar//shadow1/3.0-1-1")
        assert(v.hasParentVersion())
        v = VersionFromString("/foo.com@spc:bar//shadow1/3.0-1-0.1")
        assert(not v.hasParentVersion())
        v = VersionFromString("/foo.com@spc:bar//shadow1//shadow2/3.0-1-0.1")
        assert(v.hasParentVersion())

    def testNew(self):
        v = NewVersion()
        assert(v.asString() == "@NEW@")
        assert(v.freeze() == "@NEW@")
        assert(not v.onLocalLabel())
        assert(not v.onEmergeLabel())
        assert(not v.onLocalCookLabel())
        v = VersionFromString("@NEW@")
        assert(v.asString() == "@NEW@")
        assert(v == NewVersion())
        assert(not v != NewVersion())
        self.assertRaises(AttributeError, v.__setattr__, "foo", 1)

    def doTestAbbrev(self, fullStr, abbrevStr):
        full = VersionFromString(fullStr)
        abbrev = VersionFromString(abbrevStr)
        assert(full.asString() == abbrevStr)
        assert(abbrev.asString() == abbrevStr)
        assert(full == abbrev)

    def doTestVersion(self, abbrevStr):
        abbrev = VersionFromString(abbrevStr)
        assert(abbrev.asString() == abbrevStr)

    def testVersionCache(self):
        versions.thawedVersionCache.clear()
        assert(not versions.thawedVersionCache)
        # test the thawed version cache
        verStr = "/foo.com@spc:bar/1:1.2-3/bang.com@spc:branch/2:2.4-5"
        v1 = ThawVersion(verStr)
        v2 = ThawVersion(verStr)
        assert(id(v1) == id(v2))
        assert(len(versions.thawedVersionCache) == 1)
        del v1
        assert(len(versions.thawedVersionCache) == 1)
        del v2
        assert(not versions.thawedVersionCache)

        versions.stringVersionCache.clear()
        v1 = VersionFromString("/foo.com@spc:bar/1.2-3")
        v2 = VersionFromString("/foo.com@spc:bar/1.2-3",
                               timeStamps = [1.000])
        v3 = VersionFromString("/foo.com@spc:bar/1.2-3")
        v4 = VersionFromString("/foo.com@spc:bar/1.2-3")
        assert(v1.timeStamps() == [ 0 ])
        assert(v2.timeStamps() == [ 1 ])
        assert(id(v1) != id(v2))
        assert(id(v2) != id(v3))
        assert(id(v1) == id(v3))
        assert(id(v3) == id(v4))

    def testGetSourceVersion(self):
        v = VersionFromString('/conary.rpath.com@rpl:linux/4.3-1-0/autconf213/1')
        assert(not v.isBranchedBinary())
        source = v.getSourceVersion()
        assert(not source.isBranchedBinary())
        assert(source.asString() == '/conary.rpath.com@rpl:linux/4.3-1/autconf213/1')

        # binary branched
        v = VersionFromString('/conary.rpath.com@rpl:linux/4.3-1-1/autconf213/1')
        assert(v.isBranchedBinary())
        source = v.getSourceVersion()
        assert(not source.isBranchedBinary())
        assert(source.asString() == '/conary.rpath.com@rpl:linux/4.3-1')

        # make sure timestamps are removed
        v = ThawVersion('/conary.rpath.com@rpl:linux/1:4.3-1-0/autconf213/2:1')
        source = v.getSourceVersion()
        assert(source.timeStamps() == [0,0])

        v = ThawVersion('/conary.rpath.com@rpl:linux/1:4.3-1-1/autconf213/2:1')
        source = v.getSourceVersion()
        assert(source.timeStamps() == [0])

    def testAbbreviations(self):
        self.doTestAbbrev("/foo.com@spc:trunk/1.2-3/foo.com@spc:linux/1.2-4",
                          "/foo.com@spc:trunk/1.2-3/linux/4")
        self.doTestAbbrev("/foo.com@spc:trunk/1.2-3/foo.com@spc:linux/1.3-4",
                          "/foo.com@spc:trunk/1.2-3/linux/1.3-4")
        self.doTestAbbrev("/foo.com@spc:trunk/1.2-3/foo.com@new:linux/1.3-4",
                          "/foo.com@spc:trunk/1.2-3/new:linux/1.3-4")
        self.doTestAbbrev("/foo.com@spc:trunk/1.2-3/bar.com@spc:linux/1.2-4",
                          "/foo.com@spc:trunk/1.2-3/bar.com@spc:linux/4")
        self.doTestAbbrev("/foo.com@spc:trunk/1.2-3-1/bar.com@spc:linux/1.2-4-2",
                          "/foo.com@spc:trunk/1.2-3-1/bar.com@spc:linux/4-2")
        self.doTestAbbrev("/foo.com@spc:trunk/1.2-3-1/bar.com@spc:linux/1.2-3-4",
                          "/foo.com@spc:trunk/1.2-3-1/bar.com@spc:linux/4")

        # simple shadows
        self.doTestAbbrev("/foo@spc:linux//bar@rpl:shadow",
                          "/foo@spc:linux//bar@rpl:shadow")
        self.doTestAbbrev("/foo@spc:linux//foo@spc:shadow",
                          "/foo@spc:linux//shadow")
        self.doTestAbbrev("/foo@spc:linux//foo@abc:shadow",
                          "/foo@spc:linux//abc:shadow")
        self.doTestAbbrev("/foo@spc:linux//foo@spc:shadow/1.1-2-3",
                          "/foo@spc:linux//shadow/1.1-2-3")
        self.doTestAbbrev("/foo@spc:linux//foo@spc:shadow/1.1-2-3.1",
                          "/foo@spc:linux//shadow/1.1-2-3.1")
        self.doTestAbbrev("/foo@spc:linux//foo@spc:shadow/1.1-2-3/foo@spc:bar",
                          "/foo@spc:linux//shadow/1.1-2-3/bar")

    def testShadows(self):
        self.doTestVersion('/foo@spc:linux//shadow/1-1.2')
        self.doTestVersion('/foo@spc:linux//shadow/1-1-1.2')
        self.doTestVersion('/foo@spc:linux//shadow/1-1-1/branch//shadow2/1.1')
        self.doTestVersion('/foo@spc:linux//shadow/1-1-1/branch//shadow2/'
                           '/shadow3/1.1.1')

        self.assertRaises(ParseError, VersionFromString, 
                          '/foo@spc:linux//shadow/1-1.2.3')
        self.assertRaises(ParseError, VersionFromString, 
                          '/foo@spc:linux//shadow/1-1-1.2.3')
        self.assertRaises(ParseError, VersionFromString, 
                          '/foo@spc:linux//shadow/1-1-1/branch/1.1')
        self.assertRaises(ParseError, VersionFromString, 
                          '/foo@spc:linux//shadow/1-1-1/branch//shadow2/1.1.1')
        self.assertRaises(ParseError, VersionFromString, 
                          '/foo@spc:linux//shadow/1-1-1/branch//shadow2/'
                           '/shadow3/1.1.1.1')

        trunk = VersionFromString('/foo@spc:linux')
        assert(not trunk.isShadow())
        assert(not trunk.hasParentBranch())
        shadow = trunk.createShadow(Label('foo@spc:shadow'))
        assert(shadow.isShadow())
        assert(shadow.asString() == '/foo@spc:linux//shadow')
        assert(shadow.hasParentBranch())
        assert(shadow.parentBranch() == trunk)
        version = shadow.createVersion(Revision('1.1-1'))
        assert(version.isShadow())
        assert(not version.isModifiedShadow())
        assert(version.asString() == '/foo@spc:linux//shadow/1.1-1')
        assert(version.canonicalVersion().asString() == '/foo@spc:linux/1.1-1')
        assert(version.shadowLength() == 1)
        newVer = version.createShadow(Label("foo@spc:shadow2"))
        assert(newVer.asString() == '/foo@spc:linux//shadow//shadow2/1.1-1')
        assert(newVer.shadowLength() == 2)
        assert(newVer.parentVersion().asString() == 
                    '/foo@spc:linux//shadow/1.1-1')
        assert(newVer.parentVersion().parentVersion().asString() == 
                    '/foo@spc:linux/1.1-1')
        assert(newVer.canonicalVersion().asString() == '/foo@spc:linux/1.1-1')
        assert(newVer.isShadow())
        assert(not newVer.canonicalVersion().isShadow())
        assert(not newVer.canonicalVersion().isModifiedShadow())

        self.assertRaises(AssertionError,
                          newVer.createBranch, Label("foo@spc:shadow2"))
        self.assertRaises(VersionStringError,
                          newVer.createShadow, Label("foo@spc:shadow2"))

        branch = newVer.createBranch(Label("foo@spc:branch"))
        assert(branch.asString() == 
                    '/foo@spc:linux//shadow//shadow2/1.1-1/branch')

        branchVer = newVer.createBranch(Label("foo@spc:branch"), 
                                        withVerRel = True)
        assert(branchVer.asString() == 
                    '/foo@spc:linux//shadow//shadow2/1.1-1/branch/1')
        assert(branchVer.shadowLength() == 0)
        assert(not branchVer.isShadow())

        newVer.incrementSourceCount()
        assert(newVer.asString() == '/foo@spc:linux//shadow//shadow2/1.1-1.0.1')
        assert(newVer.isModifiedShadow())
        branchVer.incrementSourceCount()
        assert(branchVer.asString() == 
                    '/foo@spc:linux//shadow//shadow2/1.1-1/branch/2')
        assert(ThawVersion(branchVer.freeze()) == branchVer)

        newShadow = branchVer.createShadow(Label('foo@spc:shadow3'))
        assert(newShadow.asString() == 
                    '/foo@spc:linux//shadow//shadow2/1.1-1/branch//shadow3/2')
        assert(newShadow.parentVersion().asString() == 
                    '/foo@spc:linux//shadow//shadow2/1.1-1/branch/2')
        assert(newShadow.parentVersion().parentVersion().asString() == 
                    '/foo@spc:linux//shadow//shadow2/1.1-1')
        assert(newShadow.parentVersion().parentVersion().parentVersion().asString() == 
                    '/foo@spc:linux//shadow/1.1-1')
        assert(newShadow.parentVersion().parentVersion().parentVersion().parentVersion().asString() == 
                    '/foo@spc:linux/1.1-1')
        assert(newShadow.canonicalVersion().asString() ==
                    '/foo@spc:linux//shadow//shadow2/1.1-1/branch/2')

        # shadowed binary has a parent version
        v = VersionFromString("/foo@spc:linux//shadow/1.2-1-1")
        assert(v.hasParentVersion())
        # shadowed source has a parent version
        v = VersionFromString("/foo@spc:linux//shadow/1.2-1")
        assert(v.hasParentVersion())
        # cooked shadowed binary doesn't has a parent version
        v = VersionFromString("/foo@spc:linux//shadow/1.2-1-0.1")
        assert(not v.hasParentVersion())

        v = VersionFromString("/foo@spc:linux//shadow/1.2-1").copy()
        assert(v.hasParentVersion())
        v.incrementBuildCount()
        assert(v.asString() == "/foo@spc:linux//shadow/1.2-1-0.1")
        assert(not v.hasParentVersion())
        v2 = v.createBranch(LocalLabel(), withVerRel = True)
        assert(v2.asString() == 
                    "/foo@spc:linux//shadow/1.2-1-0.1/local@local:LOCAL/1")

        v = VersionFromString("/foo@spc:linux//shadow/1.2-1.1").copy()
        v.incrementBuildCount()
        assert(v.asString() == "/foo@spc:linux//shadow/1.2-1.1-1")
        assert(not v.hasParentVersion())

        # test to make sure versions don't count -0 having a build count
        v = VersionFromString('/a@b:c/4.1.25-18-0/d//e/22.3-1')
        assert(not v.hasParentVersion())

        # If you create a pristine shadow of a modified shadow, 
        # that version should NOT be a modifiedShadow
        v = VersionFromString("/foo@spc:linux//shadow//shadow2/1.2-1.1")
        assert(v.isShadow() and not v.isModifiedShadow())
        v = v.parentVersion()
        assert(v.isShadow() and v.isModifiedShadow())

        v = VersionFromString("/foo@spc:linux//shadow//shadow2/1.2-1.0.1")
        assert(v.isShadow() and v.isModifiedShadow())



    def testStrToFrozen(self):
        assert(strToFrozen("/a/1.1", [ "123" ]) == '/a/123:1.1')
        assert(strToFrozen("/a/1.1//b", [ "123" ]) == '/a/123:1.1//b')
        assert(strToFrozen("/a/1.1//b//c/1.2/d", [ "123", "456" ]) ==
                    '/a/123:1.1//b//c/456:1.2/d')

        self.assertRaises(AssertionError, strToFrozen, "/a/1.1", 
                          [ "123", "456" ])

    def testIsAfter(self):
        thaw = ThawVersion
        assert(thaw('/foo@a:b/2:1-1').isAfter(thaw('/foo@a:b/1:1-1')))
        assert(not thaw('/foo@a:b/1:1-1').isAfter(thaw('/foo@a:b/1:1-1')))
        assert(not thaw('/foo@a:b/1:1-1').isAfter(thaw('/foo@a:b/2:1-1')))
        assert(thaw('/foo@a:b/2:1-1') > thaw('/foo@a:b/1:1-1'))
        assert(thaw('/foo@a:b/1:1-1') < thaw('/foo@a:b/2:1-1'))

    def testCloseness(self):
        def _cl(first, second):
            f = VersionFromString(first)
            s = VersionFromString(second)
            c = f.closeness(s)
            return c

        assert(_cl("/a@b:c/1.1-1", "/a@b:c/1.1-1/d@e:f/1.2-1") == 2.0/3)
        assert(_cl("/a@b:c/1.1-1", "/a@b:c//d@e:f/1.2-1") == 1.0/4)
        assert(_cl("/a@b:c//d@e:f", "/a@b:c//d@e:f//g@h:i") == 1.5)
        assert(_cl("/a@b:c//d@e:f//j@k:l", "/a@b:c//d@e:f//g@h:i") == 0.75)
        assert(_cl("/a@b:c/1.1-1/d@e:f", "/a@b:c/1.1-2/d@e:f") == 1.5)
        assert(_cl("/a@b:c/1.1-1/d@e:f", "/a@b:c//d@e:f") == 3)

    def testDoubleShadowParent(self):
        v = '/c.r.com@rpl:devel//a.r.org@rpl:devel//1/nubb-0.1'
        v = VersionFromString(v)
        assert(v.hasParentVersion())
        assert(v.parentVersion().asString() == 
                    '/c.r.com@rpl:devel//a.r.org@rpl:devel/nubb-0.1')

        v = '/c.r.com@rpl:devel//a.r.org@rpl:devel//1/nubb-1'
        v = VersionFromString(v)
        assert(v.hasParentVersion())
        assert(v.parentVersion().asString() == 
                    '/c.r.com@rpl:devel//a.r.org@rpl:devel/nubb-1')

    def testVersionFromStringOnRollbackLabel(self):
        v = '/localhost@rpl:linux//local@local:ROLLBACK/2-1-1'
        v = VersionFromString(v)
        self.failUnless(v.onRollbackLabel())

    def testUnmodifiedShadowParent(self):
        '''
        Shadows of unmodified shadows have parents, too.
        @tests: CNY-2812
        '''

        # Shadow of an unmodified shadow
        x = VersionFromString(
            '/c.r.com@rpl:devel//1//p.r.com@rpath:widgets/yarr-1.0.2')
        self.failUnless(x.hasParentVersion())
        self.failUnlessEqual(x.parentVersion(), VersionFromString(
            '/c.r.com@rpl:devel//1/yarr-1'))

        # Shadow of a shadow that changed upstream version
        y = VersionFromString(
            '/c.r.com@rpl:devel//1//p.r.com@rpath:widgets/yarr-0.1.2')
        self.failUnless(y.hasParentVersion())
        self.failUnlessEqual(y.parentVersion(), VersionFromString(
            '/c.r.com@rpl:devel//1/yarr-0.1'))

        # Shadow that changed upstream version (no parent)
        z = VersionFromString(
            '/c.r.com@rpl:devel//1//p.r.com@rpath:widgets/yarr-0.0.1')
        self.failIf(z.hasParentVersion())
