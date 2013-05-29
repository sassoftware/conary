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
from testrunner import testcase
import itertools
import time
from conary import changelog, streams, trove, trovetup
from conary.trove import Trove
from conary.trove import TroveError
from conary.trove import ThawTroveChangeSet
from conary.trove import PathHashes
from conary.trove import DigitalSignatureVerificationError
from conary.trove import MetadataItem
from conary.versions import ThawVersion, VersionFromString, NewVersion
from conary.deps.deps import DependencySet, Flavor
from conary.deps.deps import Dependency
from conary.deps.deps import FileDependencies
from conary.deps.deps import parseFlavor
from conary.lib.openpgpkey import getKeyCache
from conary.lib.sha1helper import md5FromString, md5String
from conary.lib.sha1helper import sha1FromString, sha1ToString, sha1String
from conary.lib.sha1helper import sha256FromString, sha256ToString
from conary.lib.sha1helper import nonstandardSha256String
from conary_test import resources


class TroveTest(testhelp.TestCase):

    id1 = md5FromString("00010001000100010001000100010001")
    id2 = md5FromString("00010001000100010001000100010002")
    id3 = md5FromString("00010001000100010001000100010003")
    id4 = md5FromString("00010001000100010001000100010004")
    id5 = md5FromString("00010001000100010001000100010005")

    fid1 = sha1FromString("1001000100010001000100010001000100010001")
    fid2 = sha1FromString("1001000100010001000100010001000100010002")
    fid3 = sha1FromString("1001000100010001000100010001000100010003")
    fid4 = sha1FromString("1001000100010001000100010001000100010004")
    fid5 = sha1FromString("1001000100010001000100010001000100010005")

    def testTroveError(self):
        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        new = ThawVersion("/conary.rpath.com@test:trunk/20:1.3-3")
        x86 = parseFlavor('is:x86')
        x86_64 = parseFlavor('is:x86_64')

        cl = changelog.ChangeLog("test", "test@foo.bar", """\
Some changes are good.
Some changes are bad.
Some changes just are.
""")

        provides = DependencySet()
        provides.addDep(FileDependencies, Dependency("foo"))
        requires = DependencySet()
        requires.addDep(FileDependencies, Dependency("bar"))

        p = Trove("name", old, x86, cl)
        p.setProvides(provides)
        p.setRequires(requires)
        p.getTroveInfo().size.set(1234)
        assert(p.getProvides() == provides)
        assert(p.getRequires() == requires)
        assert(p.getFlavor() == x86)
        assert(p.getName() == "name")
        assert(p.getVersion() == old)
        assert(p.getTroveInfo().size() == 1234)

        p.addFile(self.id1, "/path1", old, self.fid1)
        p.addFile(self.id2, "/path2", old, self.fid2)
        p.addFile(self.id3, "/path3", old, self.fid3)

        l = [ x for x in p.iterFileList() ]; l.sort()
        assert(l == [(self.id1, "/path1", self.fid1, old), 
                     (self.id2, "/path2", self.fid2, old),
                     (self.id3, "/path3", self.fid3, old)])

        p2 = p.copy()
        assert(p == p2)
        assert(p.freeze() == p2.freeze())
        assert(not p != p2)
        assert(p.getChangeLog() == p2.getChangeLog())
        assert(p2.getTroveInfo().size() == 1234)
        p.computeDigests()
        p2.computeDigests()
        assert(p.getSigs().sha1())
        assert(p.getSigs() == p2.getSigs())

        p3 = p.copy()
        p3.getTroveInfo().size.set(2345)
        assert(p != p3)
        del p3

        p.changeVersion(new)
        assert(p.getVersion() == new)
        assert(not p == p2)
        assert(p != p2)
        assert(p.freeze() != p2.freeze())
        p.changeVersion(old)
        assert(p.getVersion() == old)

        p.updateFile(self.id2, "/newpath", None, None)
        l = [ x for x in p.iterFileList() ]; l.sort()
        assert(l == [(self.id1, "/path1", self.fid1, old), 
                     (self.id2, "/newpath", self.fid2, old),
                     (self.id3, "/path3", self.fid3, old)])

        p.updateFile(self.id2, None, "diff", None)
        l = [ x for x in p.iterFileList() ]; l.sort()
        assert(l == [(self.id1, "/path1", self.fid1, old), 
                     (self.id2, "/newpath", self.fid2, "diff"),
                     (self.id3, "/path3", self.fid3, old)])

        p.updateFile(self.id2, "/path2", old, None)
        l = [ x for x in p.iterFileList() ]; l.sort()
        assert(l == [(self.id1, "/path1", self.fid1, old), 
                     (self.id2, "/path2", self.fid2, old),
                     (self.id3, "/path3", self.fid3, old)])

        p.removeFile(self.id2)
        l = [ x for x in p.iterFileList() ]; l.sort()
        assert(l == [(self.id1, "/path1", self.fid1, old), 
                     (self.id3, "/path3", self.fid3, old)])

        p.addFile(self.id2, "/path2", old, self.fid2)
        l = [ x for x in p.iterFileList() ]; l.sort()
        assert(l == [(self.id1, "/path1", self.fid1, old), 
                     (self.id2, "/path2", self.fid2, old),
                     (self.id3, "/path3", self.fid3, old)])

        assert(p.getFile(self.id1) == ("/path1", self.fid1, old))
        assert(p.hasFile(self.id3))

        pcs = p.diff(p2)[0]
        pcs2 = ThawTroveChangeSet(pcs.freeze())
        p2.applyChangeSet(pcs2)
        assert(p == p2)
        assert(p.freeze() == p2.freeze())
        assert(p.getChangeLog() == p2.getChangeLog())
        assert(p.getChangeLog() == cl)

        p2 = p.copy()

        assert(p == p2)
        assert(p.freeze() == p2.freeze())
        assert(not p != p2)

        p = Trove("name", old, x86, cl)

        p.addTrove("sub1", old, x86)
        p.addTrove("sub2", old, x86)
        p2 = p.copy()
        p.addTrove("sub3", old, x86)
        p.addTrove("sub3", old, x86_64)
        self.assertRaises(TroveError, p.addTrove, "sub3", old, x86)

        assert(p != p2)
        assert(p.freeze() != p2.freeze())
        assert(not p == p2)

        l = [ x for x in p.iterTroveList(strongRefs=True) ]
        assert(len(l) == 4)
        for item in [('sub1', old, x86), ('sub2', old, x86), 
                     ('sub3', old, x86_64), ('sub3', old, x86)]:
            l.index(item)

        p.delTrove("sub3", old, x86, False)
        l = [ x for x in p.iterTroveList(strongRefs=True) ]; l.sort()
        assert(l == [('sub1', old, x86), 
                     ('sub2', old, x86), 
                     ('sub3', old, x86_64)])
        
        p.delTrove("sub3", old, x86, True)
        self.assertRaises(TroveError, p.delTrove, "sub3", old, x86, 
                          False)
        l = [ x for x in p.iterTroveList(strongRefs=True) ]; l.sort()
        assert(l == [('sub1', old, x86), 
                     ('sub2', old, x86), 
                     ('sub3', old, x86_64)])
        
        assert(not p.hasTrove("sub3", old, x86))
        assert(p.hasTrove("sub3", old, x86_64))

    def testFileChanges(self):
        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        new = ThawVersion("/conary.rpath.com@test:trunk/20:1.3-3")
        x86 = parseFlavor("is:x86")
        x86_64 = parseFlavor("is:x86")

        p = Trove("trvname", old, x86, None)
        p.addFile(self.id1, "/path1", old, self.fid1)
        p.addFile(self.id2, "/path2", old, self.fid2)
        p.addFile(self.id3, "/path3", old, self.fid3)
        p.addFile(self.id5, "/path5", old, self.fid5)

        p2 = Trove("trvname", new, x86, None)
        p2.addFile(self.id1, "/path1", old, self.fid1)
        p2.addFile(self.id2, "/path2", new, self.fid2)
        p2.addFile(self.id4, "/path4", new, self.fid4)
        p2.addFile(self.id5, "/path5-new", old, self.fid5)

        (pcs, files, troves) = p2.diff(p)
        assert(troves == [])
        files.sort()
        assert(files == [ (self.id2, self.fid2, old, self.fid2, new),
                          (self.id4, None, None, self.fid4, new) ])
        
        assert(not pcs.isAbsolute())
        assert(pcs.getName() == "trvname")
        assert(pcs.getOldVersion() == old)
        assert(pcs.getNewVersion() == new)

        assert(pcs.getOldFileList() == [ self.id3 ])

        l = pcs.getChangedFileList(); l.sort()
        assert(l == [ (self.id2, None, self.fid2, new), 
                      (self.id5, "/path5-new", self.fid5, None) ])

        l = pcs.getNewFileList(); l.sort()
        assert(l == [ (self.id4, "/path4", self.fid4, new) ])

        pcs2 = ThawTroveChangeSet(pcs.freeze())
        l = pcs2.getNewFileList(); l.sort()
        assert(l == [ (self.id4, "/path4", self.fid4, new) ])

        p.applyChangeSet(pcs2)
        assert(pcs2.getOldVersion() == old)
        assert(p == p2)
        assert(p.freeze() == p2.freeze())
                         
    def testIncludedTroveChanges(self):
        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        new = ThawVersion("/conary.rpath.com@test:trunk/20:1.3-3")
        x86 = parseFlavor('is:x86')
        x86_64 = parseFlavor('is:x86_64')

        p = Trove("trvname", old, x86_64, None)
        p.addTrove("trv1", old, x86)
        p.addTrove("trv2", old, x86)
        p.addTrove("trv3", old, x86)
        p.addTrove("trv3", old, x86_64)
        p.addTrove("trv4", old, x86)

        p2 = Trove("trvname", new, x86_64, None)
        p2.addTrove("trv2", new, x86)
        p2.addTrove("trv3", old, x86)
        p2.addTrove("trv3", new, x86_64)
        p2.addTrove("trv4", old, x86_64)

        (pcs, files, troves) = p2.diff(p)
        assert(files == [])
        troves.sort()
        assert(troves[0] == ("trv1", (old, x86),    (None, None),  False))
        assert(troves[1] == ("trv2", (old, x86),    (new, x86),    False))
        assert(troves[2] == ("trv3", (old, x86_64), (new, x86_64), False))
        assert(troves[3] == ("trv4", (old, x86),    (old, x86_64), False))
        assert(len(troves) == 4)

        l = [x for x in pcs.iterChangedTroves()]
        l.sort()
        assert(l[0] == ("trv1", [("-", old, x86, None)]))
        assert(l[1] == ("trv2", [("+", new, x86, True), 
                                 ("-", old, x86, None)]))
        assert(l[2] == ("trv3", [("+", new, x86_64, True), 
                                 ("-", old, x86_64, None)]))
        assert(l[3] == ("trv4", [("+", old, x86_64, True), 
                                 ("-", old, x86, None)]))
        assert(len(l) == 4)

        pcs2 = ThawTroveChangeSet(pcs.freeze())
        p.applyChangeSet(pcs2)
        assert(p == p2)
        assert(p.freeze() == p2.freeze())

    @testhelp.context('redirect')
    def testRedirect(self):
        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        x86 = parseFlavor('is:x86')

        grp = Trove("trvname", old, x86, None)
        grp.addTrove("trv1", old, x86)

        redir = Trove("trvname", old, x86, None,
                      type = trove.TROVE_TYPE_REDIRECT)
        redir.addRedirect("trv1", old.branch(), x86)
        redirCopy = redir.copy()
        
        troveRedirs = [x for x in redir.redirects.iter()]
        troveCopyRedirs = [x for x in redirCopy.redirects.iter()]
        self.assertEquals(troveRedirs, troveCopyRedirs)

        assert(grp != redir)

        redir2 = Trove("trvname", old, x86, None,
                       type = trove.TROVE_TYPE_REDIRECT)
        assert(redir != redir2)
        redir2.addRedirect("trv1", old.branch(), x86)
        assert(redir == redir2)

        diff = redir.diff(None, absolute = True)[0]
        redir2 = Trove(diff)
        assert(redir == redir2)

        assert(redir.troveInfo.troveVersion() == 10)

    def testRemoved(self):
        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        new = ThawVersion("/conary.rpath.com@test:trunk/20:4.5-6")
        x86 = parseFlavor('is:x86')

        removed = Trove("trvname", old, x86, None,
                        type = trove.TROVE_TYPE_REMOVED,
                        setVersion = True)
        removed.computeDigests()

        diff = removed.diff(None, absolute = True)[0]
        removed2 = Trove(diff)
        assert(removed == removed2)

        assert(removed.troveInfo.troveVersion() == 11)

        newtrove = Trove("trvname", new, x86, None, setVersion=True)
        newtrove.computeDigests()
        diff = newtrove.diff(removed)[0]

        removedCopy = removed.copy()
        removedCopy.applyChangeSet(diff)
        self.assertEqual(removedCopy.freeze(), newtrove.freeze())

        newCopy = newtrove.copy()
        diff = removed.diff(newtrove)[0]
        newCopy.applyChangeSet(diff)
        self.assertEqual(newCopy.freeze(), removed.freeze())

    def testReferencedByDefault(self):
        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        new = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-4")
        x86 = parseFlavor('is:x86')

        grp = Trove("trvname", old, x86, None)
        grp.addTrove("trv1", old, x86, byDefault = False)
        grp.addTrove("trv2", old, x86, byDefault = True)
        grp.addTrove("trv3", old, x86, byDefault = True)
        newGrp = Trove("trvname", old, x86, None)
        cs = ThawTroveChangeSet(grp.diff(None)[0].freeze())
        newGrp.applyChangeSet(cs)
        assert(newGrp == grp)
        assert(newGrp.freeze() == grp.freeze())
        newGrp = Trove(grp.diff(None, absolute = True)[0])
        assert(newGrp == grp)
        assert(newGrp.freeze() == grp.freeze())

        dupGrp = Trove("trvname", old, x86, None)
        dupGrp.addTrove("trv1", old, x86, byDefault = False)
        dupGrp.addTrove("trv2", old, x86, byDefault = True)
        dupGrp.addTrove("trv3", old, x86, byDefault = True)
        assert(grp == dupGrp)
        assert(grp.freeze() == dupGrp.freeze())

        almostDupGrp = Trove("trvname", old, x86, None)
        almostDupGrp.addTrove("trv1", old, x86, byDefault = False)
        almostDupGrp.addTrove("trv2", old, x86, byDefault = True)
        almostDupGrp.addTrove("trv3", old, x86, byDefault = False)
        assert(grp != almostDupGrp)
        assert(grp.freeze() != almostDupGrp.freeze())

        grp2 = Trove("trvname", new, x86, None)
        grp2.addTrove("trv1", old, x86, byDefault = False)
        grp2.addTrove("trv2", old, x86, byDefault = True)
        grp2.addTrove("trv3", old, x86, byDefault = False)
        assert(grp != grp2)
        assert(grp.freeze() != grp2.freeze())

        (pcs, files, troves) = grp2.diff(grp)
        assert(not files)
        assert(not troves)

        newGrp2 = grp.copy()
        newGrp2.applyChangeSet(pcs)
        assert(newGrp2 == grp2)
        assert(newGrp2.freeze() == grp2.freeze())

        newGrp2 = grp.copy()
        frz = pcs.freeze()
        thaw = ThawTroveChangeSet(frz)
        newGrp2.applyChangeSet(thaw)
        assert(newGrp2 == grp2)
        assert(newGrp2.freeze() == grp2.freeze())

    def testTroveInfo(self):
        x86 = parseFlavor('is:x86')
        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        new = ThawVersion("/conary.rpath.com@test:trunk/20:1.3-3")

        p = Trove("trvname", old, x86, None)
        p.getTroveInfo().size.set(1)
        p.getTroveInfo().sourceName.set('src')

        p2 = Trove(p.diff(None, absolute = True)[0])
        p.troveInfo.__eq__(p2.troveInfo)
        
        assert(p == p2)
        assert(p.freeze() == p2.freeze())

        p2.getTroveInfo().size.set(2)
        p2.getTroveInfo().sourceName.set('src2')

        p.applyChangeSet(p2.diff(p)[0])
        assert(p == p2)
        assert(p.freeze() == p2.freeze())

    def testDiff(self):
        # this tests to make sure that label binds tighter than
        # flavor when looking for matches in a trove diff
        instGroup = Trove('@update', NewVersion(), Flavor(), None)
        v1 = VersionFromString('/conary.rpath.com@rpl:devel/2.4.0-3-2')
        f1 = parseFlavor('~!builddocs,~desktop,gnome is: x86')
        v2 = VersionFromString('/conary.rpath.com@rpl:devel/2.4.0-3-0/gnome14/0.17-4-1')
        f2 = parseFlavor('desktop,gnome is: x86')
        instGroup.addTrove('libglade', v1, f1)
        instGroup.addTrove('libglade', v2, f2)

        newGroup = Trove('@update', NewVersion(), Flavor(), None)
        v3 = VersionFromString('/conary.rpath.com@rpl:branch//devel/2.5.1-1-2')
        newGroup.addTrove('libglade', v3, f2)
        assert(newGroup.diff(instGroup)[2] ==
               [('libglade', (v1, f1), (v3, f2), False),
                ('libglade', (v2, f2), (None, None), False) ])

        # test parent version matching
        v1 = VersionFromString('/conary.rpath.com@rpl:devel/1.0-1-1')
        v2 = VersionFromString('/conary.rpath.com@rpl:devel//shadow/2.0-1-1')
        v3 = VersionFromString('/conary.rpath.com@rpl:devel//shadow//old/1.1-1-1')

        instGroup = Trove('@update', NewVersion(), Flavor(), None)
        instGroup.addTrove('foo:lib', v1, f1)
         
        newGroup = Trove('@update', NewVersion(), Flavor(), None)
        newGroup.addTrove('foo:lib', v2, f1)
        newGroup.addTrove('foo:lib', v3, f1)

        needs = newGroup.diff(instGroup)[2]
        assert(needs ==
                [ ('foo:lib', (v1, f1), (v2, f1), False),
                  ('foo:lib', (None, None), (v3, f1), False) ])

    def testDiff2(self):
        # test for the need for fully searching for all possible update
        # flavor combinations in trove.diff -- kernel with ~kernel.smp 
        # does match installed ~!kernel.smp flavor, but not very well.  
        # If we allow the first available flavor that matches to be picked
        # as the match, we may end up with updating ~kernel.smp -> ~!kernel.smp
        # - not what we meant
        instGroup = Trove('@update', NewVersion(), Flavor(), None)
        v1 = VersionFromString('/conary.rpath.com@rpl:devel/1-1-1')
        nosmp = parseFlavor('~!kernel.smp is: x86')
        nosmp2 = parseFlavor('~!kernel.smp is: x86(~i686)')
        smp = parseFlavor('~kernel.smp is: x86(~i686)')
        v2 = VersionFromString('/conary.rpath.com@rpl:devel/2.6.0-3-2')
        instGroup.addTrove('kernel', v1, nosmp)

        newGroup = Trove('@update', NewVersion(), Flavor(), None)
        newGroup.addTrove('kernel', v2, smp)
        newGroup.addTrove('kernel', v2, nosmp2)
        assert(newGroup.diff(instGroup)[2] ==
               [('kernel', (v1, nosmp), (v2, nosmp2), False),
                ('kernel', (None, None), (v2, smp), False) ])

    def testDigitalSignatures(self):
        t = Trove('foo', NewVersion(), Flavor(), None)
        keyCache = getKeyCache()
        keyCache.setPublicPath(resources.get_archive()+'/pubring.gpg')
        keyCache.setPrivatePath(resources.get_archive()+'/secring.gpg')
        keyCache.getPrivateKey('F94E405E', '111111')
        keyCache.getPrivateKey('90B1E477', '111111')
        t.addDigitalSignature('90B1E477')
        assert(t.verifyDigitalSignatures() > 0)
        t.addDigitalSignature('F94E405E')
        assert(t.verifyDigitalSignatures() > 0)
        t.name.set('foo2')

        # verifying shouldn't change the stored sha1
        oldSha1 = t.troveInfo.sigs.sha1()
        self.assertRaises(AssertionError, t.verifyDigitalSignatures)
        assert(oldSha1 == t.troveInfo.sigs.sha1())

        t.computeDigests()
        try:
            t.verifyDigitalSignatures()
        except DigitalSignatureVerificationError, message:
            pass

        # add a file to the trove, which ensures that file
        # information is included in the data that was signed
        t.addFile('0' * 16, "/path", t.getVersion(), '0' * 20)
        t.computeDigests()
        try:
            t.verifyDigitalSignatures()
        except DigitalSignatureVerificationError, message:
            pass

    def testBadName(self):
        try:
            t = Trove('foo:bar:baz', NewVersion(), Flavor(), None)
        except TroveError, e:
            assert(str(e) == 'More than one ":" is not allowed in a trove name')
        else:
            raise

        try:
            t = Trove(':component', NewVersion(), Flavor(), None)
        except TroveError, e:
            assert(str(e) == 'Trove and component names cannot be empty')
        else:
            raise

    def testPathHash(self):
        ph = PathHashes()
        ph.addPath("/first")
        ph.addPath("/second")
        assert(ph.freeze() == 'Y9\xde\xb7{\xeb\xbd\x88\xa7\xd5K\xb8\ns@\xd3')
        assert(ph == PathHashes(ph.freeze()))

        ph2 = PathHashes()
        ph2.addPath("/first")
        ph2.addPath("/third")
        assert(ph != ph2)

        diff = ph.diff(ph2)
        assert(diff == \
            '\x00\x00\x00\x01Y9\xde\xb7{\xeb\xbd\x88\xcf\x97\xd5J>\xbdd\xf5')
        ph2.twm(diff, ph2)
        assert(ph == ph2)

    def testTrovePathAndDirHashes(self):
        v = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        f = parseFlavor('is:x86')
        t = trove.Trove('name', v, f)

        t.addFile('0' * 16, '/bin/ls', v, '0' * 20)
        t.addFile('1' * 16, '/etc/cfg', v, '1' * 20)
        t.addFile('2' * 16, '/bin/cat', v, '2' * 20)
        t.computePathHashes()
        t.computeDigests()

        diff = t.diff(None, absolute = True)[0]
        t2 = trove.Trove(diff)

        assert(set(t2.troveInfo.pathHashes) ==
                    set( [ md5String('/bin/ls')[0:8],
                           md5String('/etc/cfg')[0:8],
                           md5String('/bin/cat')[0:8] ] ) )

        assert(set(t2.troveInfo.dirHashes) ==
                    set( [ md5String('/bin')[0:8],
                           md5String('/etc')[0:8] ] ) )

        # pathHashes and dirHashes are excluded from v0 signatures
        t2.troveInfo.pathHashes.addPath('/root/.bashrc')
        t2.computeDigests()
        tDigests = self._gatherDigests(t)
        t2Digests = self._gatherDigests(t2)
        assert(tDigests[0] == t2Digests[0])
        assert(tDigests[1] != t2Digests[1])

        t2 = trove.Trove(diff)
        t2.troveInfo.pathHashes.addPath('/root')
        t2.computeDigests()
        tDigests = self._gatherDigests(t)
        t2Digests = self._gatherDigests(t2)
        assert(tDigests[0] == t2Digests[0])
        assert(tDigests[1] != t2Digests[1])

    def testWeakRefs(self):
        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        new = ThawVersion("/conary.rpath.com@test:trunk/20:1.3-3")
        x86 = parseFlavor('is:x86')

        p = Trove("name", old, x86, None)
        p.addTrove("other", old, x86, False)

        p2 = Trove("name", new, x86, None)
        p2.addTrove("other", old, x86, False, weakRef = True)
        newTrv = Trove("name", new, x86, None)
        newTrv.applyChangeSet(p2.diff(None)[0])
        assert(newTrv == p2)

        assert(p != p2)
        d = p2.diff(p)[0]
        newTrv = p.copy()
        newTrv.applyChangeSet(d)
        assert(newTrv == p2)

        d = p.diff(p2)[0]
        newTrv = p2.copy()
        newTrv.applyChangeSet(d)
        assert(newTrv == p)

    def testTainting(self):
        originalTroveVersion = trove.TROVE_VERSION
        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        new = ThawVersion("/conary.rpath.com@test:trunk/20:1.3-3")
        x86 = parseFlavor('is:x86')

        try:
            origTrv = Trove("trv", old, x86)
            origTrv2 = Trove("trv", new, x86)
            absTrvCs = origTrv.diff(None, absolute = True)[0]
            relTrvCs = origTrv.diff(origTrv2)[0]

            trv = Trove(absTrvCs)
            assert(not trv.troveInfo.incomplete())
            trv.applyChangeSet(relTrvCs)
            assert(not trv.troveInfo.incomplete())

            trove.TROVE_VERSION = 1
            trove.TROVE_VERSION_1_1 = 2

            trv = Trove(absTrvCs)
            assert(trv.troveInfo.incomplete())
            self.assertRaises(AssertionError, trv.applyChangeSet, relTrvCs)

            trove.TROVE_VERSION = originalTroveVersion
            trv = Trove(absTrvCs)
            assert(not trv.troveInfo.incomplete())
            trove.TROVE_VERSION = 1
            trv.applyChangeSet(relTrvCs)
            assert(trv.troveInfo.incomplete())
        finally:
            trove.TROVE_VERSION = originalTroveVersion

    def testRefFileStream(self):
        # check that changing the path w/o changing the fileId works
        rfl = trove.ReferencedFileList()
        rfl.append(("1234567890123456", "/dir1", "path1",
                    "12345678901234567890", None))
        rfl.append(("1234567890123456", "/dir2", "path2", None, None))

        newRfl = trove.ReferencedFileList(rfl.freeze())
        assert(rfl == newRfl)

    def testPathHashAwareDiff(self):
        self.counter = 0

        def buildGroup(contents, hashMap):
            t = Trove('group-a', NewVersion(), Flavor(), None)
            for item in contents:
                versionInfo = item[0]
                pathList = item[1:]
                name = 'trv1'
                flavor = ''
                if len(versionInfo) == 1:
                    version = versionInfo[0]
                elif len(versionInfo) == 2:
                    version, flavor = versionInfo
                else:
                    name, version, flavor = versionInfo
                version = VersionFromString(version)
                version = version.copy()
                version.trailingRevision().timeStamp = time.time() + self.counter
                self.counter += 1
                flavor = parseFlavor(flavor)
                assert(flavor is not None)

                info = (name, version, flavor)
                t.addTrove(*info)
                ph = trove.PathHashes()
                [ ph.addPath(x) for x in pathList ]
                hashMap[info] = ph

            return t

        def lookup(infoList, hashMap):
            return [ hashMap[info] for info in infoList ]

        def checkUpdate(origActual, desired):
            actual = origActual[:]

            for item in desired:
                name = 'trv1'
                if len(item) == 3:
                    name, oldInfo, newInfo = item
                else:
                    oldInfo, newInfo = item

                if oldInfo:
                    oldVersion, oldFlavor = oldInfo
                    oldVersion = VersionFromString(oldVersion)
                    oldFlavor = parseFlavor(oldFlavor)
                else:
                    oldVersion = oldFlavor = None

                if newInfo:
                    newVersion, newFlavor = newInfo
                    newVersion = VersionFromString(newVersion)
                    newFlavor = parseFlavor(newFlavor)
                else:
                    newVersion = newFlavor = None
 
                match = None
                for i, (oName, (oOldV, oOldF), (oNewV, oNewF), absolute) in \
                                                        enumerate(actual):
                    if oName != name or \
                       oldVersion != oOldV or \
                       oldFlavor != oOldF or \
                       newVersion != oNewV or \
                       newFlavor != oNewF: continue

                    match = i
                    break
                assert(match is not None)
                del actual[i]

            assert(not actual)

        def test(old, new, normalDiff, hashedDiff=None):
            # Old and new are trove group specifications, and are a list
            # of (name, versionString, [ paths ]) 
            # or (name, (versionString, flavor), [paths]) tuples.
            #
            # the diffs specify how the change should have been matched up
            # as a list of (name, oldVersion, newVersion) tuples
            hashMap = {}
            old = buildGroup(old, hashMap)
            new = buildGroup(new, hashMap)

            chgList = new.diff(old)[2]
            checkUpdate(chgList, normalDiff)

            chgList = new.diff(old,
                   getPathHashes =
                    lambda l, old = False: lookup(l, hashMap) )[2]
            if not hashedDiff:
                hashedDiff = normalDiff
            checkUpdate(chgList, hashedDiff)

        f = 'f'
        frun = 'f:run'
        pfoo = '/foo'
        pbar = '/bar'
        pbam = '/bam'
        pbaz = '/baz'
        pbamph = '/bamph'
        a0 = '/a@a:a/0-0-0'
        a1 = '/c@c:c//a@a:a/1-1-1'
        a2 = '/d@d:d//a@a:a/2-2-2'
        a3 = '/e@d:e//a@a:a/3-3-3'
        a4 = '/a@a:f//a/4-4-4'
        a5 = '/a@a:g//a/5-5-5'
        b0 = '/b@b:b/0-0-0'
        b1 = '/b@b:b/1-1-1'
        x86 = 'is:x86'
        x86_pssl = '~ssl is:x86'
        x86_64 = 'is:x86_64'
        empty = ''
        pnossl = '~!ssl'

        # compatible flavors on same branch - should pick
        # path hash match (or latest, if no path hashes).
        test( old = [ ((a0, x86),  pfoo ) ],
              new = [ ((a1, x86),  pfoo),
                      ((a2, x86),  pbar) ],
              normalDiff = [ ((a0, x86), (a2, x86)),
                             (None, (a1, x86))],
              hashedDiff = [ ((a0, x86), (a1, x86)),
                             (None, (a2, x86))]
                             )

        # incompatible flavor (with path overlap) on same branch, 
        # compatible flavor on other branch
        # (should pick compatible flavor)
        test( old = [ ((a0, x86),  pfoo ) ],
              new = [ ((a1, x86_64), pfoo),
                      ((b1, x86),  pbar) ],
              normalDiff = [ ((a0, x86), (b1, x86)),
                             (None, (a1, x86_64))])


        # incompatible flavors with path overlap on same branch
        # incompatible flavor (with path overlap) on other branch
        # (pick one on same branch)
        test(old = [ ((a0, x86),  pfoo ) ],
             new = [ ((a1, x86_64), pfoo),
                     ((b1, x86_64),  pfoo) ],
             normalDiff = [ ((a0, x86), (a1, x86_64)),
                             (None, (b1, x86_64))])

        # two incompatible flavors on other branch (pick one with
        # path overlap).
        test(old = [ ((a0, x86),    pfoo ) ],
             new = [ ((b0, x86_64), pfoo),
                     ((b1, x86_64), pbar) ],
             normalDiff = [ ((a0, x86), (b0, x86_64)),
                            (None,      (b1, x86_64))],
             hashedDiff = [ ((a0, x86), (b0, x86_64)),
                             (None,     (b1, x86_64)) ])

        # two new troves to match up on same branch.  One of them
        # has a path overlap with something on another branch.
        # The one with the path overlap should be delayed.
        test(old = [ ((a0, x86),    pbar),
                     ((a1, x86),    pfoo) ],
             new = [ ((a2, x86),    pbam),
                     ((b0, x86),    pfoo) ],
             normalDiff = [ ((a1, x86), (a2, x86)),
                            ((a0, x86), (b0, x86))],
             hashedDiff = [ ((a0, x86), (a2, x86)),
                            ((a1, x86), (b0, x86))])

        # like above, only two old troves are available to match 
        # up to something on the same branch.
        test(old = [ ((b0, x86),    pfoo),
                     ((a2, x86),    pbam) ],
             new = [ ((a0, x86),    pbar),
                     ((a1, x86),    pfoo ),],
             normalDiff = [ ((a2, x86), (a1, x86)),
                            ((b0, x86), (a0, x86))],
             hashedDiff = [ ((a2, x86), (a0, x86)),
                            ((b0, x86), (a1, x86))])

                # test matching using components
        # f:runtime should match foo.
        test(old = [ ((f,     a0, x86), ),
                     ((frun, a0, x86), pfoo) ],
             new = [ ((f,     b0, x86_64),),
                     ((frun,  b0, x86_64), pbar),
                     ((f,     b1, x86_64),),
                     ((frun, b1, x86_64), pfoo) ],
             normalDiff = [ (f, (a0, x86), (b0, x86_64)),
                            (frun, (a0, x86), (b0, x86_64)),
                            (f,    None,      (b1, x86_64)),
                            (frun, None,      (b1, x86_64)) ],
             hashedDiff = [ (f,    (a0, x86), (b1, x86_64)),
                            (frun, (a0, x86), (b1, x86_64)),
                            (f,    None,      (b0, x86_64)),
                            (frun, None,      (b0, x86_64)) ])

        # both new packages have the same package.
        test( old = [ ((a0, x86),  pfoo ) ],
              new = [ ((a1, x86),  pfoo),
                      ((a2, x86),  pfoo) ],
              normalDiff = [ ((a0, x86), (a2, x86)),
                             (None, (a1, x86))],
              hashedDiff = [ ((a0, x86), (a2, x86)),
                             (None, (a1, x86))],
                             )

        # test matching empty flavor - empty flavor matches empty flavor
        # best.
        test(old = [ ((a0, empty),    pfoo) ],
             new = [ ((a1, empty),    pbar),
                     ((a2, pnossl),   pbam), ],
             normalDiff = [ ((a0, empty), (a1, empty)),
                            (None, (a2, pnossl)) ])

        # NOTE: I turned off this part of the algorithm, it is complicated
        # and probably not worth it.
        # Three new troves to match up on same branch, two old on same branch, 2 old on other branch.
        # Two of them have a path overlap with something on another branch.
        # Delay the one with the poorer flavor match.
        test(old = [ ((b0, x86),    pfoo),
                     ((b1, x86),    pbar),
                     ((a3, x86),    pbam),
                     ((a4, x86_pssl),    pbaz),
                     ],
             new = [ ((a0, x86),         pbamph),
                     ((a1, x86_pssl),    pbar ),
                     ((a2, x86),         pfoo )],
        #     # matches up a0 and b0 because they share the same upstream version
             normalDiff = [ ((a3, x86), (a2, x86)),
                            ((a4, x86_pssl), (a1, x86_pssl)),
                            ((b1, x86), None),
                            ((b0, x86), (a0, x86))],
        #     hashedDiff =  [ ((a3, x86), (a0, x86)),
        #                     ((a4, x86_pssl), (a1, x86_pssl)),
        #                     ((b0, x86), (a2, x86)),
        #                     ((b1, x86), None)]
                )

        # Like above, only three old troves and four new.
        test(
             old = [ ((a0, x86),         pbamph),
                     ((a1, x86_pssl),    pbar ),
                     ((a2, x86),         pfoo )],
             new = [ ((b0, x86),    pfoo),
                     ((b1, x86),    pbar),
                     ((a3, x86),    pbam),
                     ((a4, x86_pssl),    pbaz),
                     ],
             normalDiff = [ ((a0, x86), (b0, x86)),
                            ((a1, x86_pssl), (a4, x86_pssl)),
                            ((a2, x86), (a3, x86)),
                            (None, (b1, x86))],

        #     hashedDiff =  [ ((a1, x86_pssl), (a4, x86_pssl)),
        #                     ((a0, x86), (a3, x86)),
        #                     ((a2, x86), (b0, x86)),
        #                     (None, (b1, x86))]
        )

    def testTroveNames(self):
        v = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        f = parseFlavor('is:x86')

        # : is perfectly acceptable in a trove name
        trove.Trove('some:name', v, f)
        # We create @update troves for local change sets
        trove.Trove('@update', v, f)
        self.assertRaises(trove.TroveError, trove.Trove, 'some name', v, f)
        self.assertRaises(trove.TroveError, trove.Trove, 'some#name', v, f)
        self.assertRaises(trove.TroveError, trove.Trove, 'some#name', v, f)
        self.assertRaises(trove.TroveError, trove.Trove, 'some*name', v, f)

    @staticmethod
    def _gatherDigests(t):
        digests = {}
        for (sigVersion, sigDigest, signature) in t.troveInfo.sigs:
            assert(not signature)
            digests[sigVersion] = sigDigest()

        return digests

    def testTroveDigests(self):
        v = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        f = parseFlavor('is:x86')

        t = trove.Trove('some:name', v, f)
        t.addFile('0' * 16, '/path', v, '0' * 20)
        t.computeDigests()
        digests = self._gatherDigests(t)
        assert(len(digests) == 2)
        assert(digests[0] == 
                  '\xa8\x8bdD^\xceo\xbc\x98\xc1\xb7\xf7@e\xf1(\xd6\xb2hF')

        t.computePathHashes()
        t.computeDigests()
        newDigests = self._gatherDigests(t)
        assert(digests[0] == newDigests[0])
        assert(digests[1] != newDigests[1])

    def testUnknownTroveInfo(self):

        class ExtraTroveScripts(trove.TroveScripts):
            streamDict = dict(trove.TroveScripts.streamDict)
            streamDict[254] = \
                    (streams.LARGE, streams.StringStream, 'unknown2')

        class ExtraTroveInfo(trove.TroveInfo):

            streamDict = dict(trove.TroveInfo.streamDict)
            streamDict[254] = (streams.LARGE, streams.StringStream, 'unknown')
            streamDict[trove._TROVEINFO_TAG_SCRIPTS] = \
                    (streams.LARGE, ExtraTroveScripts, 'scripts')
            v0SignatureExclusions = trove._getTroveInfoSigExclusions(streamDict)
        class OtherTrove(trove.Trove):

            streamDict = dict(trove.Trove.streamDict)
            streamDict[trove._STREAM_TRV_TROVEINFO] = \
                    (streams.LARGE, ExtraTroveInfo, 'troveInfo')
            v0SkipSet = dict(trove.Trove.v0SkipSet)
            trove._mergeTroveInfoSigExclusions(v0SkipSet, streamDict)

        v = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        v2 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-4")
        f = parseFlavor('is:x86')

        t = OtherTrove('trvName', v, f, None)
        t.troveInfo.sourceName.set('foo')
        t.troveInfo.unknown.set('-----UNKNOWN-----')

        t.computeDigests()
        old = self._gatherDigests(t)

        t.troveInfo.unknown.set('-----NOTKNOWN-----')
        t.computeDigests()
        new = self._gatherDigests(t)

        assert(old[0] == new[0])
        assert(old[1] != new[1])

        t.verifyDigests()
        oldTrove = trove.Trove(t.diff(None, absolute = True)[0])
        oldTrove.verifyDigests()

        # test diffing with unknown trove info (CNY-1569)
        # first create a new Trove object with a different size
        t2 = Trove('trvName', v2, f, None)
        t2.troveInfo.size.set(100)
        # make sure that we can diff from the oldTrove (which is a Trove
        # object that has unknown troveInfo elements)
        diff = t2.diff(oldTrove)[0]
        # applyt the tcs to the OtherTrove object (which knows about the
        # new trove info)
        t.applyChangeSet(diff)
        # make sure that the new size is reflected
        self.assertEqual(t.troveInfo.size(), 100)

        # now create a diff where the unknown troveinfo changes
        t = OtherTrove('trvName', v, f, None)
        t.troveInfo.sourceName.set('foo')
        t.troveInfo.unknown.set('-----UNKNOWN-----')
        t.troveInfo.scripts.unknown2.set('blah')

        t2 = OtherTrove('trvName', v2, f, None)
        t2.troveInfo.sourceName.set('foo')
        t2.troveInfo.unknown.set('-----UNKNOWN 2-----')

        oldTrove = trove.Trove(t.diff(None, absolute = True)[0])
        oldTrove.verifyDigests()
        oldTrove2 = trove.Trove(t2.diff(None, absolute = True)[0])
        oldTrove2.verifyDigests()

        # now we have two Trove objects with unknown troveInfo that
        # differs.  Create a tcs to represent that change
        diff = oldTrove2.diff(oldTrove)[0]
        # apply the diff to the origional OtherTrove object and make
        # sure the change is reflected
        t.applyChangeSet(diff)
        self.assertEqual(t.troveInfo.unknown(), '-----UNKNOWN 2-----')

        # now create a diff where the unknown troveinfo changes and
        # we're diffing against two different versions of Trove
        t = OtherTrove('trvName', v, f, None)
        t.troveInfo.sourceName.set('foo')
        t.troveInfo.unknown.set('-----UNKNOWN-----')
        t.troveInfo.scripts.unknown2.set('blah')

        t2 = Trove('trvName', v2, f, None)
        t2.troveInfo.sourceName.set('foo')

        # create a Trove object that has unknown troveinfo
        oldTrove = trove.Trove(t.diff(None, absolute = True)[0])
        oldTrove.verifyDigests()
        # create a tcs to apply to the origional trove
        diff = t2.diff(oldTrove)[0]
        # apply it and verify
        t.applyChangeSet(diff)
        self.assertEqual(t.troveInfo.unknown(), '')
        self.assertEqual(t.troveInfo.scripts.unknown2(), '')

    def testMissingRawTroveInfo(self):
        v1 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        v2 = ThawVersion("/conary.rpath.com@test:trunk/20:1.2-3")
        f1 = parseFlavor('is:x86')
        f2 = parseFlavor('is:x86_64')

        ot = trove.Trove('group-foo', v1, f1, None)
        ot.computeDigests()

        nt = trove.Trove('group-foo', v2, f2, None)
        nt.computeDigests()

        diff = ot.diff(nt)[0]

        # simulate having gone through an old client at some point
        # which lost new troveinfo segment
        ot.troveInfo.sigs.vSigs.thaw('')

        ot.applyChangeSet(diff)

    def testTroveInfoMerge(self):
        # make sure we can merge troveinfo still; old troves need this
        v1 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        v2 = ThawVersion("/conary.rpath.com@test:trunk/20:1.2-3")
        f1 = parseFlavor('is:x86')
        f2 = parseFlavor('is:x86_64')

        ot = trove.Trove('group-foo', v1, f1, None)
        ot.troveInfo.sigs.vSigs.thaw('')
        ot.computeDigests()

        nt = trove.Trove('group-foo', v2, f2, None)
        nt.computeDigests()
        nt.troveInfo.sigs.vSigs.thaw('')

        diff = ot.diff(nt)[0]

        # simulate having gone through an old client at some point
        # which lost new troveinfo segment
        diff.absoluteTroveInfo.set('')

        ot.applyChangeSet(diff)

    def testMetadata(self):
        v = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        f = parseFlavor('is:x86')
        t = Trove("trvname", v, f, None)

        # check the frozen trove without metadata
        expected = '\x00\x00\x07trvname\x01\x00)/conary.rpath.com@test:trunk/10.000:1.2-3\x02\x80\x00\x00\x051#x86\x04\x80\x00\x00\x0b\r\x00\x04\x00\x00\x00\n\x0e\x00\x01\x00\t\x00\x01\x00'
        frozen = t.freeze()
        self.assertEqual(frozen, expected)

        mi = MetadataItem()
        mi.shortDesc.set('This is the short description of trvname')
        mi.longDesc.set('This is the long description of trvname')
        mi.licenses.set('Copyright License :: OSI Approved :: Common Public License')
        mi.licenses.set('Copyright License :: OSI Approved :: General Public License')
        mi.crypto.set('blowfish, symmetric, 512 bits')
        # make sure that metadata is excluded from sha1s
        sha1s = {}
        for ver in trove._TROVESIG_VER_ALL:
            sha1s[ver] = t._sigString(ver)
        t.troveInfo.metadata.addItem(mi)
        t.computeDigests()
        for ver in trove._TROVESIG_VER_ALL:
            self.assertTrue(sha1s[ver] == t._sigString(ver))
        # check the frozen form with metadata
        expected = "\x00\x00\x07trvname\x01\x00)/conary.rpath.com@test:trunk/10.000:1.2-3\x02\x80\x00\x00\x051#x86\x04\x80\x00\x01\x90\t\x80\x00\x00F\x00\x00\x14\x0c,!eT\x97\x85~\x81vF!\xf0$\xc4\xd3\xef\x8e\xad\x84\x02\x80\x00\x00*\x01\x00'\x00\x00\x01\x01\x01\x00 \xd4\xab\xdc\xf3\xcaU\xd2\x19`\xfe\xe6\x00\x86\xb5\xecC\x90\x18&\xb8\x90/\x18*\xa6\x8c4[P>\xe2\xa8\r\x00\x04\x00\x00\x00\n\x0e\x00\x01\x00\x11\x017\x01A4\x00\x00\x14'\x14\xc7\x97mBF\xd1B\x9e\xf6/\xc9\n\xb6w\x0f\x84F\xe8\x01\x00(This is the short description of trvname\x02\x00'This is the long description of trvname\x03\x00w:Copyright License :: OSI Approved :: Common Public License;Copyright License :: OSI Approved :: General Public License\x04\x00\x1e\x1dblowfish, symmetric, 512 bits\x08\x00*\x01\x00'\x00\x00\x01\x00\x01\x00 \x87\x9c\xa5ae\xa5\xd2w\x7f\xdc\xe7\xb4\xc0\xa4\xd9\xc8\xd7\x0c\xbc`\xcd\xa9\x14\xc3\x19\x0f\xe2\xa6^\xdfs\xa8\t\x00\x01\x00"
        frozen = t.freeze()
        self.assertEqual(frozen, expected)

        t.troveInfo.metadata.verifyDigitalSignatures()
        mi.shortDesc.set('tainted shortDesc')
        try:
            t.troveInfo.metadata.verifyDigitalSignatures()
        except DigitalSignatureVerificationError, e:
            self.assertEqual(str(e), 'metadata checksum does not match stored value')
        try:
            t.troveInfo.metadata.verifyDigests()
        except DigitalSignatureVerificationError, e:
            self.assertEqual(str(e), 'metadata checksum does not match stored value')
        try:
            t.verifyDigests()
        except DigitalSignatureVerificationError, e:
            self.assertEqual(str(e), 'metadata checksum does not match stored value')

        trvCs = t.diff(None, absolute = True)[0]
        try:
            trove.Trove(trvCs)
        except DigitalSignatureVerificationError, e:
            self.assertEqual(str(e), 'metadata checksum does not match stored value')

        mi.shortDesc.set('This is the short description of trvname')
        t.troveInfo.metadata.verifyDigitalSignatures()

        #-- test metadata signatures
        # first record the old frozen form and old metadata id
        frz1 = mi.freeze()
        id1 = mi.id()
        self.assertEqual(sha1ToString(id1),
                             '2714c7976d4246d1429ef62fc90ab6770f8446e8')
        # add a signature
        keyCache = getKeyCache()
        keyCache.setPublicPath(resources.get_archive()+'/pubring.gpg')
        keyCache.setPrivatePath(resources.get_archive()+'/secring.gpg')
        keyCache.getPrivateKey('90B1E477', '111111')
        mi.addDigitalSignature('90B1E477')
        # get the new frozen from and id
        frz2 = mi.freeze()
        id2 = mi.id()
        # make sure that the frozen form now has something differet (the sig)
        # the sig will be different every time, so we can't check for the
        # exact value
        self.assertTrue(frz1 != frz2)
        digests = set()
        for signature in itertools.chain(mi.oldSignatures, mi.signatures):
            digests.add(
                (signature.version(), sha256ToString(signature.digest())))
        self.assertEqual(digests,
                     set([(0, '879ca56165a5d2777fdce7b4c0a4d9c8'
                              'd70cbc60cda914c3190fe2a65edf73a8') ]))
        # make sure that the id did not change (since signatures should be
        # excluded from the id)
        self.assertEqual(id1, id2)
        # test signature verification
        t.computeDigests()
        t.verifyDigitalSignatures()
        sigs = mi.oldSignatures.getSignatures(0)
        ds = [ x[1] for x in sigs.signatures.iterAll() ][0]
        oldSig = ds.signature()
        # tamper with the signature
        ds.signature.set(ds.signature()[:-1] +
                         chr((ord(ds.signature()[-1]) + 1) %255))
        try:
            t.verifyDigitalSignatures(keyCache=keyCache)
        except DigitalSignatureVerificationError, e:
            self.assertEqual(str(e), 'Trove signatures made by the following keys are bad: F7440D78FE813C882212C2BF8AC2828190B1E477')
        ds.signature.set(oldSig[:-1])
        try:
            t.verifyDigitalSignatures(keyCache=keyCache)
        except DigitalSignatureVerificationError, e:
            self.assertEqual(str(e), 'Trove signatures made by the following keys are bad: F7440D78FE813C882212C2BF8AC2828190B1E477')
        ds.signature.set(oldSig)
        t.verifyDigitalSignatures(keyCache=keyCache)
        ds.timestamp.set(int(time.time() + 10000))
        try:
            t.verifyDigitalSignatures(keyCache=keyCache)
        except DigitalSignatureVerificationError, e:
            self.assertEqual(str(e), 'Trove signatures made by the following keys are bad: F7440D78FE813C882212C2BF8AC2828190B1E477')

        # test the easy access methods
        shortDesc = t.getMetadata()['shortDesc']
        self.assertEqual(shortDesc,
                             'This is the short description of trvname')
        # add another metadata item with update short description
        mi2 = MetadataItem()
        mi2.shortDesc.set('This is the updated short description of trvname')
        t.troveInfo.metadata.addItem(mi2)
        shortDesc = t.getMetadata()['shortDesc']
        self.assertEqual(shortDesc,
                             'This is the updated short description of trvname')
        # add an empty mi
        mi3 = MetadataItem()
        t.troveInfo.metadata.addItem(mi3)
        # should not override the last value
        shortDesc = t.getMetadata()['shortDesc']
        self.assertEqual(shortDesc,
                             'This is the updated short description of trvname')
        # now test a trove with no metadata
        v2 = ThawVersion("/conary.rpath.com@test:trunk/10:2.2-3")
        t2 = trove.Trove('trvname', v2, f, None)
        shortDesc = t2.getMetadata()['shortDesc']
        self.assertEqual(shortDesc, None)
        # test relative diff
        diff = t.diff(t2)[0]
        t2.applyChangeSet(diff)
        shortDesc = t2.getMetadata()['shortDesc']
        self.assertEqual(shortDesc,
                             'This is the updated short description of trvname')

    def testMetadataDigests(self):
        # make sure we only put new style digests on new style metadata
        mi = trove.MetadataItem()
        mi.shortDesc.set('hello world')
        mi.computeDigests()
        assert(mi.oldSignatures.freeze())
        origDigest = list(mi.oldSignatures)[0].digest()
        assert(not mi.signatures.freeze())

        mi = trove.MetadataItem()
        mi.shortDesc.set('hello world')
        mi.keyValue['hello'] = 'world'
        mi.computeDigests()
        assert(mi._digest(0) == origDigest)
        assert(mi._digest(0) == list(mi.oldSignatures)[0].digest())
        assert(mi._digest(1) == list(mi.signatures)[0].digest())

        mi.keyValue['hello'] = 'world1'
        assert(mi._digest(0) == list(mi.oldSignatures)[0].digest())
        assert(mi._digest(1) != list(mi.signatures)[0].digest())

        mi.keyValue['hello'] = 'world'
        mi.shortDesc.set('hello world1')
        assert(mi._digest(0) != list(mi.oldSignatures)[0].digest())
        assert(mi._digest(1) != list(mi.signatures)[0].digest())

        mi.shortDesc.set('hello world')
        assert(mi._digest(1) == list(mi.signatures)[0].digest())

    @testhelp.context('trovescripts')
    def testTroveScripts(self):
        def _t(trv):
            trv.computeDigests()
            tcs = trv.diff(None, absolute = True)[0]
            assert(trv == Trove(tcs))

            return tcs

        v1 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        f1 = parseFlavor('is:x86')

        g = Trove('foo', v1, f1)
        g.troveInfo.scripts.postUpdate.script.set('foo')
        tcs = _t(g)
        assert(not tcs.isRollbackFence())
        assert(not tcs.isRollbackFence(update = True))

        g = Trove('foo', v1, f1)
        g.troveInfo.scripts.postRollback.script.set('foo')
        g.troveInfo.scripts.postRollback.conversions.addList([ (2, 0) ])
        tcs = _t(g)
        assert(not tcs.isRollbackFence(update = False))
        assert(not tcs.isRollbackFence(update = True))
        # because we didn't set the compatibility class for this trove
        assert(not tcs.isRollbackFence(update = True,
                                       oldCompatibilityClass = 0))
        g.troveInfo.compatibilityClass.set(1)
        tcs = _t(g)
        # because we can rollback from 2->0, but we need to rollback from 1
        assert(    tcs.isRollbackFence(update = True,
                                       oldCompatibilityClass = 0))
        g.troveInfo.compatibilityClass.set(2)
        tcs = _t(g)
        assert(not tcs.isRollbackFence(update = True,
                                       oldCompatibilityClass = 0))

        # this is fence because the compatibility class changed without having
        # a rollback script in place
        g = Trove('foo', v1, f1)
        g.setCompatibilityClass(1)
        tcs = _t(g)
        assert(    tcs.isRollbackFence(update = True,
                                       oldCompatibilityClass = 0))

        # same idea, but slightly different code path becaues there are
        # some scripts
        g.troveInfo.scripts.postUpdate.script.set('foo')
        tcs = _t(g)
        assert(    tcs.isRollbackFence(update = True,
                                       oldCompatibilityClass = 0))

        # and we shouldn't be a fence if the compat class is the same
        assert(not tcs.isRollbackFence(update = True,
                                       oldCompatibilityClass = 1))

        # This trove was created with conary 1.1.17, which is the version in
        # the rBuilder job servers.  It's missing group scripts in trove info.
        frz = "\x00\x00\tgroup-foo\x02\x001/conary.rpath.com@test:trunk/1181891335.163:1-1-1\x04\x80\x00\x00\x0b4#group-foo\x07\x00\x04\x00\x00\x00\x01\x08\x80\x00\x00<foo\x00+\x00/conary.rpath.com@test:trunk/1181883231.115:1-1-1\x00-\x001\x00\r\x00\x01\x00\x0e\x80\x00\x00\xdb\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x04\x01\x00\x10group-foo:source\x02\x00\x08\x00\x00\x00\x00Fr;\x07\x03\x00\x061.1.28\x07\x00\x01\x01\t\x80\x00\x00F\x00\x00\x14,\xf7\x03\x1e\xda\xab\xb0\xe4\xe5\x90\x91\x1e(\xa7Y\xfb\x8f.`\xbf\x02\x80\x00\x00*\x01\x00'\x00\x00\x01\x01\x01\x00 M\xf0,\xb4\xd0\xe0o:\xa4,\xaa\x08\x98j\x81\x11\xac\xe6\xdf\xc8\xab\x97V\x85X\xbaA\xe3\x04\x9a\xa2P\x0b\x00\x1bconary.rpath.com@test:trunk\r\x00\x04\x00\x00\x00\n\x0e\x00\x01\x00\x10\x00)\x03\x00&\x00\x00\x13#!/bin/bash\n\necho 1\x02\x00\r\x01\x00\n\x00\x00\x02\x00\x02\x01\x00\x02\x00\x01\x13\x00\x02\x00\x02\x10\x80\x00\x00\x17\x00\x00\x14,\xf7\x03\x1e\xda\xab\xb0\xe4\xe5\x90\x91\x1e(\xa7Y\xfb\x8f.`\xbf\x11\x80\x00\x00\x8bfoo:debuginfo\x00+\x00/conary.rpath.com@test:trunk/1181883231.115:1-1-1\x00-\x000\x00\x00foo:runtime\x00+\x00/conary.rpath.com@test:trunk/1181883231.115:1-1-1\x00-\x001\x00"
        tcs = ThawTroveChangeSet(frz)
        t = trove.Trove(tcs)
        self.assertEquals(2, t.getCompatibilityClass())
        self.assertEquals([1], t.getTroveInfo().scripts.postRollback.conversions.getItems().keys())

        self.assertTrue(tcs.isRollbackFence(0))
        self.assertFalse(tcs.isRollbackFence(1))
        self.assertFalse(tcs.isRollbackFence(2))
        self.assertTrue(tcs.getPostRollbackScript())

    def testTroveScriptCompatSigs(self):
        # CNY-2997 - if a script has more than one conversion (toClass=)
        # we need to use a new vSigs2 block
        v1 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        f1 = parseFlavor('is:x86')

        # first, a group with a script with multiple conversion entries
        g = Trove('foo', v1, f1)
        g.troveInfo.scripts.postUpdate.script.set('foo')
        g.troveInfo.scripts.postUpdate.conversions.addList([ (2, 0), (1, 0) ])
        g.computeDigests()
        sigs = [(x[0], x[1](), x[2]) for x in g.troveInfo.sigs ]
        # expect v0, v2 sig
        expected = [(0, sha1FromString('4a96a173f304e408bb1925f78d2edf396975528d'), None),
                    (2, sha256FromString('ed7daba3caaffc4da0de41d5a90ba6d2f364daf205bbf7b00ed1c125abea2276'), None)]
        self.assertEqual(sigs, expected)
        frz = g.freeze()
        expected = '\x00\x00\x03foo\x01\x00)/conary.rpath.com@test:trunk/10.000:1.2-3\x02\x80\x00\x00\x051#x86\x04\x80\x00\x00\x7f\t\x80\x00\x00F\x00\x00\x14J\x96\xa1s\xf3\x04\xe4\x08\xbb\x19%\xf7\x8d.\xdf9iuR\x8d\x02\x80\x00\x00*\x01\x00\'\x00\x00\x01\x02\x01\x00 \xed}\xab\xa3\xca\xaf\xfcM\xa0\xdeA\xd5\xa9\x0b\xa6\xd2\xf3d\xda\xf2\x05\xbb\xf7\xb0\x0e\xd1\xc1%\xab\xea"v\r\x00\x04\x00\x00\x00\n\x0e\x00\x01\x00\x10\x00&\x02\x00#\x00\x00\x03foo\x02\x00\x1a\x01\x00\n\x00\x00\x02\x00\x01\x01\x00\x02\x00\x00\x01\x00\n\x00\x00\x02\x00\x02\x01\x00\x02\x00\x00\t\x00\x01\x00'
        self.assertEqual(frz, expected)

        # make sure it passes digest check
        self.assertTrue(g.verifyDigests())
        # manipulate the trove object
        g.troveInfo.scripts.postUpdate.conversions.addList([(3, 0)])
        # it should no longer pass
        self.assertTrue(not g.verifyDigests())


        # if only one toClass= is used, we can use the current versioned
        # sigs
        g = Trove('foo', v1, f1)
        g.troveInfo.scripts.postUpdate.script.set('foo')
        g.troveInfo.scripts.postUpdate.conversions.addList([ (2, 0) ])
        g.computeDigests()
        sigs = [(x[0], x[1](), x[2]) for x in g.troveInfo.sigs ]
        # expect v0, v1 sigs
        expected = [(0, sha1FromString('4a96a173f304e408bb1925f78d2edf396975528d'), None),
                    (1, sha256FromString('c7b964f5c17d81568a85adad73e2ac43496b9ff1fbe12971afbc218eceba294f'), None)]
        self.assertEqual(sigs, expected)

        frz = g.freeze()
        expected2 = "\x00\x00\x03foo\x01\x00)/conary.rpath.com@test:trunk/10.000:1.2-3\x02\x80\x00\x00\x051#x86\x04\x80\x00\x00r\t\x80\x00\x00F\x00\x00\x14J\x96\xa1s\xf3\x04\xe4\x08\xbb\x19%\xf7\x8d.\xdf9iuR\x8d\x02\x80\x00\x00*\x01\x00'\x00\x00\x01\x01\x01\x00 \xc7\xb9d\xf5\xc1}\x81V\x8a\x85\xad\xads\xe2\xacCIk\x9f\xf1\xfb\xe1)q\xaf\xbc!\x8e\xce\xba)O\r\x00\x04\x00\x00\x00\n\x0e\x00\x01\x00\x10\x00\x19\x02\x00\x16\x00\x00\x03foo\x02\x00\r\x01\x00\n\x00\x00\x02\x00\x02\x01\x00\x02\x00\x00\t\x00\x01\x00"
        self.assertEqual(frz, expected2)

        # this is an old trove changeset that has a vSig entry, and also
        # has more than one conversion on a script.  We need to ignore
        # the vSig.
        frztcs = "\x00\x00\x03foo\x02\x00)/conary.rpath.com@test:trunk/10.000:1.2-3\x07\x00\x04\x00\x00\x00\x02\x0c\x00\x051#x86\r\x00\x01\x00\x0e\x80\x00\x00\x7f\t\x80\x00\x00F\x00\x00\x14J\x96\xa1s\xf3\x04\xe4\x08\xbb\x19%\xf7\x8d.\xdf9iuR\x8d\x02\x80\x00\x00*\x01\x00'\x00\x00\x01\x01\x01\x00 \xc5\x86\xa9\xa5p\xf4>\xcb\x13\x13\xd4m\xf6\xe0\x8b\n/\xe2\xa2\x8e\xd8J\x00\xa3:\xd8\x98\x14\xe6U5?\r\x00\x04\x00\x00\x00\n\x0e\x00\x01\x00\x10\x00&\x02\x00#\x00\x00\x03foo\x02\x00\x1a\x01\x00\n\x00\x00\x02\x00\x02\x01\x00\x02\x00\x00\x01\x00\n\x00\x00\x02\x00\x01\x01\x00\x02\x00\x00\x10\x80\x00\x00F\x00\x00\x14J\x96\xa1s\xf3\x04\xe4\x08\xbb\x19%\xf7\x8d.\xdf9iuR\x8d\x02\x80\x00\x00*\x01\x00'\x00\x00\x01\x01\x01\x00 \xc5\x86\xa9\xa5p\xf4>\xcb\x13\x13\xd4m\xf6\xe0\x8b\n/\xe2\xa2\x8e\xd8J\x00\xa3:\xd8\x98\x14\xe6U5?\x13\x80\x00\x00\x7f\t\x80\x00\x00F\x00\x00\x14J\x96\xa1s\xf3\x04\xe4\x08\xbb\x19%\xf7\x8d.\xdf9iuR\x8d\x02\x80\x00\x00*\x01\x00'\x00\x00\x01\x01\x01\x00 \xc5\x86\xa9\xa5p\xf4>\xcb\x13\x13\xd4m\xf6\xe0\x8b\n/\xe2\xa2\x8e\xd8J\x00\xa3:\xd8\x98\x14\xe6U5?\r\x00\x04\x00\x00\x00\n\x0e\x00\x01\x00\x10\x00&\x02\x00#\x00\x00\x03foo\x02\x00\x1a\x01\x00\n\x00\x00\x02\x00\x02\x01\x00\x02\x00\x00\x01\x00\n\x00\x00\x02\x00\x01\x01\x00\x02\x00\x00"
        logFilter = testcase.LogFilter()
        logFilter.add()
        try:
            tcs = ThawTroveChangeSet(frztcs)
            t = Trove(tcs)
            # call getItems() for force the conversions to be thawed
            t.troveInfo.scripts.postUpdate.conversions.getItems()
            assert(t.verifyDigests())
            logFilter.compare(
                ['warning: Ignoring version 1 signature on '
                 'foo=/conary.rpath.com@test:trunk/1.2-3[is: x86] - '
                 'it has multiple conversion entries for a trove script'] * 2)
        finally:
            logFilter.remove()

    def testVerifyDigests(self):
        t = Trove('foo', NewVersion(), Flavor())
        self.assertEqual(t.verifyDigests(), True)
        self.assertEqual([x for x in t.troveInfo.sigs ], [])
        t.computeDigests()
        sigs = [(x[0], x[1](), x[2]) for x in t.troveInfo.sigs ]
        expected = [
            (0, sha1FromString('7035ad238e9dba2597839cef74d0e12746c8e9bb'), None),
            (1, sha256FromString('195e1e413328c895e75c0d6decbde51c225f805f8e376031c6cbd8995eb120e2'), None) ]
        self.assertEqual(sigs, expected)
        self.assertEqual(t.verifyDigests(), True)
        t.troveInfo.sigs.sha1.set(sha1String('blah'))
        self.assertEqual(t.verifyDigests(), False)
        t.computeDigests()
        sigs = [(x[0], x[1](), x[2]) for x in t.troveInfo.sigs ]
        self.assertEqual(sigs, expected)
        # get the v1 vds
        vds = [ x for x in t.troveInfo.sigs.vSigs if x.version() == 1 ][0]
        # muck with the digest
        vds.digest.set(nonstandardSha256String('blah'))
        self.assertEqual(t.verifyDigests(), False)
        t.computeDigests()
        sigs = [(x[0], x[1](), x[2]) for x in t.troveInfo.sigs ]
        self.assertEqual(sigs, expected)
        # add a bogus v1 sig
        t.troveInfo.sigs.vSigs.addDigest(nonstandardSha256String('blah'), 1)
        self.assertEqual(t.verifyDigests(), False)

    def testTroveInfoSize(self):
        # troveinfo elements added after BUILD_FLAVOR must be DYNAMIC
        for tag in trove.TroveInfo.streamDict.keys():
            if tag > trove._TROVEINFO_TAG_BUILD_FLAVOR:
                assert(trove.TroveInfo.streamDict[tag][0] == streams.DYNAMIC)

    def testRemoveAllFiles(self):
        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        x86 = parseFlavor('is:x86')
        p = Trove("name", old, x86, None)

        p.addFile(self.id1, "/path1", old, self.fid1)
        p.addFile(self.id2, "/path2", old, self.fid2)
        p.addFile(self.id3, "/path3", old, self.fid3)

        assert(len(p.idMap) == 3)
        p.removeAllFiles()
        assert(len(p.idMap) == 0)

from conary_test import rephelp
class TroveTest2(rephelp.RepositoryHelper):

    def _checkMetadata(self, d, **kw):
        for key, value in d.items():
            if value is None:
                assert(key not in kw or kw[key] == None)
            else:
                assert(value == kw[key])

    def _checkMetadataItem(self, item, **kw):
        d = {}
        d.update(item)
        self._checkMetadata(d, **kw)

    def testMetadataLanguage(self):
        mi = self.createMetadataItem(language='FO', shortDesc='FO Hello', 
                                     url='foo')
        trv, cs = self.Component('foo:run', metadata=mi)
        assert(trv.getMetadata()['shortDesc'] is None)
        assert(trv.getMetadata(language='FO')['shortDesc'] == 'FO Hello')
        mi = self.createMetadataItem(shortDesc='Hello', longDesc='Long Desc')
        trv.troveInfo.metadata.addItem(mi)
        self._checkMetadata(trv.getMetadata(),
                            shortDesc='Hello', longDesc='Long Desc')
        self._checkMetadata(trv.getMetadata(language='FO'),
                            shortDesc='FO Hello', longDesc='Long Desc',
                            url='foo', language='FO')

        mi = self.createMetadataItem(language='FO', shortDesc='FO Goodbye')
        trv.troveInfo.metadata.addItem(mi)
        items = trv.getAllMetadataItems()
        assert(len(items) == 2)
        fooItem = [ x for x in items if x.language() == 'FO'][0]
        noneItem = [ x for x in items if not x.language() ][0]
        self._checkMetadataItem(fooItem, language='FO', shortDesc='FO Goodbye',
                                url='foo')
        self._checkMetadataItem(noneItem, shortDesc='Hello',
                                longDesc='Long Desc')

    def testCopyMetadata(self):
        mi = self.createMetadataItem(language='FO', shortDesc='FO Hello',
                                     url='foo')
        mi2 = self.createMetadataItem(shortDesc='Hello', longDesc='Long Desc',
                                      sizeOverride=12345)
        mi3 = self.createMetadataItem(language='FO', shortDesc='FO Goodbye')
        trv, cs = self.Component('foo:run', metadata=[mi, mi2, mi3])
        trv2, cs = self.Component('foo2:run')
        trv2.copyMetadata(trv)
        self._checkMetadata(trv2.getMetadata(),
                            shortDesc='Hello', longDesc='Long Desc',
                            sizeOverride=None)
        self._checkMetadata(trv2.getMetadata(language='FO'),
                            shortDesc='FO Goodbye', longDesc='Long Desc',
                            url='foo', language='FO')
        trv2.copyMetadata(trv, skipSet=['shortDesc'])
        self._checkMetadata(trv2.getMetadata(), longDesc='Long Desc')
        self._checkMetadata(trv2.getMetadata(language='FO'),
                            longDesc='Long Desc', url='foo', language='FO')

    def testImageGroup(self):
        g = Trove('group-foo', NewVersion(), Flavor(), None)
        self.assertEquals(g.troveInfo.imageGroup(), None)
        g.troveInfo.imageGroup.set(True)
        self.assertEquals(g.troveInfo.imageGroup(), 1)
        g.troveInfo.imageGroup.set(False)
        self.assertEquals(g.troveInfo.imageGroup(), 0)

    def testFlattenMetadata(self):
        m = trove.Metadata()
        mi1 = trove.MetadataItem()
        mi1.language.set('lang')
        mi1.licenses.set('l1')
        mi1.licenses.set('l2')

        mi2 = trove.MetadataItem()
        mi2.language.set('lang')
        mi2.licenses.set('l3')

        # Add some key-value metadata
        mi2.keyValue['a'] = 'a'
        mi2.keyValue['b'] = 'b'

        # We want to make sure that only the last list of strings is visible
        m.addItems([mi1, mi2])
        licenses = m.get('lang')['licenses']
        self.assertEqual(licenses, ['l3'])

        # Make sure after we flatten the metadata we still get the item on the
        # top of the stack
        items = m.flatten(filteredKeyValues = ['a'])
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].licenses, ['l3'])
        self.assertEqual(list(items[0].keyValue.iteritems()),
            [('b', 'b')])

    def testKeyValueMetadata(self):
        vis = trove.KeyValueItemsStream()
        vis['a'] = 'A binary string\000'
        vis['b'] = 'Another string'
        self.assertEqual(vis['a'], 'A binary string\000')
        self.assertEqual(vis['b'], 'Another string')
        frozen = vis.freeze()
        vis2 = trove.KeyValueItemsStream(frozen)

        self.assertEqual(vis2['a'], vis['a'])
        self.assertEqual(vis2['b'], vis['b'])

        self.assertEqual(sorted(vis2.keys()), ['a', 'b'])
        self.assertEqual(sorted(vis2.items()),
            [('a', 'A binary string\000'), ('b', 'Another string')])
        self.assertEqual(sorted(vis2.iteritems()),
            [('a', 'A binary string\000'), ('b', 'Another string')])

        # An empty VIS
        vis = trove.KeyValueItemsStream()
        self.assertEqual(sorted(vis.keys()), [])

        frozen = vis.freeze()
        vis2 = trove.KeyValueItemsStream(frozen)
        self.assertEqual(sorted(vis2.keys()), [])

        # make sure keyvalue metadata doesn't show up in t

    def testKeyValueMetadata2(self):
        # Test key-value metadata in the cotext of a MetadataItem
        mi = trove.MetadataItem()
        ddata = dict(a='a', b='b')

        # Test update in the process
        mi.keyValue.update(ddata)
        frozen = mi.freeze()

        nmi = trove.MetadataItem(frozen)
        self.assertEqual(sorted(mi.keyValue.items()),
            sorted(ddata.iteritems()))

    def testExtendedMetadata(self):
        trv = Trove('test', NewVersion(), Flavor(), None)
        # key/value is the first metadata which needed to be frozen
        # separately from the rest
        mdItem = trove.MetadataItem()
        mdItem.keyValue['first'] = '1'
        mdItem.keyValue['second'] = '2'
        trv.troveInfo.metadata.addItem(mdItem)

        trvCs = trv.diff(None, absolute = True)[0]
        frzTrv = trvCs.freeze()
        frzTroveInfo = trove.AbstractTroveChangeSet.find(
                            trove._STREAM_TCS_ABSOLUTE_TROVEINFO, frzTrv)()
        frzMetadata = trove.TroveInfo.find(
                            trove._TROVEINFO_TAG_METADATA, frzTroveInfo)
        assert('first' not in frzMetadata)
        assert('first' in trvCs.extendedMetadata())

        thawedTrvCs = ThawTroveChangeSet(frzTrv)
        thawedTrv = Trove(thawedTrvCs)
        assert(trv == thawedTrv)

        # now make sure we can add new metadata w/o breaking old troves
        class NewMetadataItem(trove.MetadataItem):
            streamDict = trove.MetadataItem.streamDict.copy()
            streamDict[255] = \
                            (streams.DYNAMIC, streams.StringStream, 'newItem')

        class NewMetadata(trove.Metadata):
            streamDict = { 1 : NewMetadataItem }

        class NewTroveInfo(trove.TroveInfo):
            streamDict = trove.TroveInfo.streamDict.copy()
            streamDict[trove._TROVEINFO_TAG_METADATA] = \
                                    ( streams.DYNAMIC, NewMetadata, 'metadata')
            _newMetadataItems = trove.TroveInfo._newMetadataItems.copy()
            _newMetadataItems['newItem'] = True

        class NewTrove(trove.Trove):
            streamDict = trove.Trove.streamDict.copy()
            streamDict[trove._STREAM_TRV_TROVEINFO] = \
                                    ( streams.LARGE, NewTroveInfo, "troveInfo" )

        trv = NewTrove('new', NewVersion(), Flavor(), None)
        mdItem = NewMetadataItem()
        mdItem.shortDesc.set('somethingblue')
        mdItem.newItem.set('somethingnew')
        trv.troveInfo.metadata.addItem(mdItem)
        trv.computeDigests()
        trvCs = trv.diff(None, absolute = True)[0]
        frzTrv = trvCs.freeze()
        thawedTrvCs = ThawTroveChangeSet(frzTrv)
        thawedTrv = Trove(thawedTrvCs)

        # now build up the old trove which doesn't understand extended
        # metadata and make sure we can thaw new style troves
        class OldMetadataItem(trove.MetadataItem):
            ignoreUnknown = 0

            def _digest(self, version = 0):
                if version > 0:
                    return None
                return trove.MetadataItem._digest(self, version)

        class OldMetadata(trove.Metadata):
            streamDict = { 1 : OldMetadataItem }

        class OldTroveInfo(trove.TroveInfo):
            streamDict = trove.TroveInfo.streamDict.copy()
            streamDict[trove._TROVEINFO_TAG_METADATA] = \
                                    ( streams.DYNAMIC, OldMetadata, 'metadata')

        class OldTrove(trove.Trove):
            streamDict = trove.Trove.streamDict.copy()
            streamDict[trove._STREAM_TRV_TROVEINFO] = \
                                    ( streams.LARGE, OldTroveInfo, "troveInfo" )

        class OldTroveChangeSet(trove.AbstractTroveChangeSet):
            streamDict = trove.TroveChangeSet.streamDict.copy()
            del streamDict[trove._STREAM_TCS_EXTENDED_METADATA]

            def getTroveInfo(self, klass = OldTroveInfo):
                return klass(self.absoluteTroveInfo())

        oldThawedTrvCs = OldTroveChangeSet(frzTrv)

        thawedOldTrv = OldTrove(oldThawedTrvCs)
        thawedOldTrv.troveInfo.metadata.flatten()
        thawedOldTrv.verifyDigests()
        thawedOldTrv.verifyDigitalSignatures()

    def testTroveWithFiles(self):
        trv = trove.TroveWithFileObjects('test', NewVersion(), Flavor(), None)
        trv.addFileObject('1234', 'obj1')
        assert(trv.getFileObject('1234') == 'obj1')
        self.assertRaises(KeyError, trv.getFileObject, '12345.')

    def testTroveMtimes(self):
        l = trove.TroveMtimes()
        self.assertEquals(l.freeze(), '')
        l.append(0x20)
        l.append(0x3000)
        l.append(0x400000)
        l.append(0x80000000)
        self.assertEquals(l.freeze(), '\x00\x00\x00 \x00\x000\x00\x00@\x00\x00\x80\x00\x00\x00')

        l2 = trove.TroveMtimes(l.freeze())
        self.assertEquals(l, l2)
        self.assertEquals(l.freeze(), l2.freeze())
        self.assertEquals(l.diff(l2), l.freeze())

        l2 = trove.TroveMtimes()
        self.assertEquals(l.diff(l2), l.freeze())
        l2.twm(l.diff(l2), l2)
        assert(l == l2)

    def testTroveMetadata(self):
        # make sure that the size override metadata works correctly
        v = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        f = parseFlavor('is:x86')
        n = 'somename'
        t = trove.Trove(n, v, f)
        t.setSize(100)
        self.assertEquals(t.getSize(), 100)

        mi = trove.MetadataItem()
        mi.sizeOverride.set(105)
        t.troveInfo.metadata.addItem(mi)
        self.assertEquals(t.getSize(), 105)

    def testSizeOverrideMetadata(self):
        # override the size
        mi = self.createMetadataItem(sizeOverride=12345)
        trv, cs = self.Component('foo:run', metadata=[mi])
        self.assertEqual(trv.getSize(), 12345)

        # override with zero size
        mi2 = self.createMetadataItem(sizeOverride=0)
        trv2, cs = self.Component('foo2:run', metadata=[mi, mi2])
        self.assertEqual(trv2.getSize(), 0)

    def testGetNameVersionFlavorReturnsTroveTup(self):
        v = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        f = parseFlavor('is:x86')
        n = 'somename'
        t = trove.Trove(n, v, f)
        nvf = t.getNameVersionFlavor()
        self.assertTrue(isinstance(nvf, trovetup.TroveTuple))
        self.assertTrue(isinstance(nvf, tuple))
