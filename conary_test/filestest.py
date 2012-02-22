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
import os
import shutil
import stat
import tempfile

from conary import files, streams
from conary.repository import filecontents
from conary.files import DeviceStream
from conary.files import InodeStream
from conary.lib import sha1helper

class FilesTest(testhelp.TestCase):

    def testInodeStream(self):
        # this test wants dates in US/Eastern representation
        os.environ['TZ']='US/Eastern'

        i = InodeStream(0755, 0x1000, "user", "group")
        i2 = InodeStream(i.freeze())
        assert(i == i2)
        diff = i.diff(i2)
        assert(diff is None)
        assert(not i.twm(diff, i2))
        assert(i == i2)

        assert(i.perms() == 0755)
        assert(i.freeze() != i2.freeze(skipSet = { 'mtime' : True }) )

        i2 = InodeStream(0700, 0x1000, "user", "group")
        assert(i != i2)
        diff = i.diff(i2)
        assert(diff == '\x01\x00\x02\x01\xed')
        diff2 = i2.diff(i)
        assert(diff2 == '\x01\x00\x02\x01\xc0')
        assert(not i2.twm(diff, i2))
        assert(i == i2)

        i2 = InodeStream(0755, 0x1000, "person", "group")
        assert(i != i2)
        diff = i.diff(i2)
        assert(diff == '\x03\x00\x04user')
        diff2 = i2.diff(i)
        assert(diff2 == '\x03\x00\x06person')
        assert(not i2.twm(diff, i2))
        assert(i == i2)

        i2 = InodeStream(0755, 0x1000, "user", "set")
        assert(i != i2)
        diff = i.diff(i2)
        assert(diff == '\x04\x00\x05group')
        diff2 = i2.diff(i)
        assert(diff2 == "\x04\x00\x03set")
        assert(not i2.twm(diff, i2))
        assert(i == i2)

        i2 = InodeStream(0700, 0x1001, "person", "set")
        assert(i != i2)
        diff = i.diff(i2)
        assert(diff == '\x01\x00\x02\x01\xed\x02\x00\x04\x00\x00\x10\x00\x03\x00\x04user\x04\x00\x05group')
        diff2 = i2.diff(i)
        assert(diff2 == '\x01\x00\x02\x01\xc0\x02\x00\x04\x00\x00\x10\x01\x03\x00\x06person\x04\x00\x03set')

        assert(not i2.twm(diff, i2))
        assert(i == i2)

        i = InodeStream(04755, 1083767693, "root", "root")
        assert(i.permsString() == "rwsr-xr-x")
        i.perms.set(04644)
        assert(i.permsString() == "rwSr--r--")
        i.perms.set(01644)
        assert(i.permsString() == "rw-r--r-T")
        i.perms.set(01755)
        assert(i.permsString() == "rwxr-xr-t")

        # 20070420 CNY-855: we switched the mtime display to no longer match
        # the ls -l format, so these three tests could really be just one
        # Additionally, the extra argument to timeString is ignored now
        t = 1083768516
        self.assertEqual(i.timeString(t), "2004-05-05 14:34:53 UTC")
        i.mtime.set(1067697293)
        self.assertEqual(i.timeString(t), "2003-11-01 14:34:53 UTC")
        i.mtime.set(1099751693)
        self.assertEqual(i.timeString(t), "2004-11-06 14:34:53 UTC")

    def testDevice(self):
        d = DeviceStream()
        d.major.set(010)
        d.minor.set(020)
        d2 = DeviceStream(d.freeze())
        assert(d == d2)

        d2 = DeviceStream()
        d2.major.set(011)
        d2.minor.set(021)

        assert(d != d2)
        diff = d.diff(d2)
        assert(diff == 
                "\x01\x00\x04\x00\x00\x00\x08\x02\x00\x04\x00\x00\x00\x10" )
        diff2 = d2.diff(d)
        assert(diff2 == 
                "\x01\x00\x04\x00\x00\x00\t\x02\x00\x04\x00\x00\x00\x11" )

        assert(not d2.twm(diff, d2))
        assert(d == d2)

    def testSymbolicLink(self):
        s = files.SymbolicLink(None)
        s.inode.perms.set(0604)
        s.inode.mtime.set(0100)
        s.inode.owner.set('daemon')
        s.inode.group.set('bin')
        s.flags.set(0)
        s.target.set("/some/target")
        assert(s.sizeString() == "      12")
        d = tempfile.mkdtemp()
        try:
            p = d + "/bar/foo"
            s.restore(None, d, p)
            what = os.readlink(p)
            assert(what == "/some/target")
            s2 = files.SymbolicLink(None)
            s2.target.set("/some/place")
            s2.restore(None, d, p)
            s3 = files.FileFromFilesystem(p, None)
            assert(s2.target == s3.target)
            assert(s.target != s3.target)
        finally:
            shutil.rmtree(d)

        s2 = files.ThawFile(s.freeze(), None)
        assert(s == s2)

        # make sure that the frozen form matches what we expect
        assert(s.freeze() == 'l\x03\x00\x04\x00\x00\x00\x00\x05\x00\x1b\x01\x00\x02\x01\x84\x02\x00\x04\x00\x00\x00@\x03\x00\x06daemon\x04\x00\x03bin\t\x00\x0c/some/target')
    
    def testSocket(self):
        s = files.Socket(None)
        s.inode.perms.set(0604)
        s.inode.mtime.set(0100)
        s.inode.owner.set(self.owner)
        s.inode.group.set(self.group)
        s.flags.set(0)
        assert(s.sizeString() == "       0")
        d = tempfile.mkdtemp()
        try:
            p = d + "/bar/foo"
            s.restore(None, d, p)
            what = os.stat(p)
            assert(stat.S_ISSOCK(what.st_mode))
            assert(what.st_mtime == 0100)
            assert(what.st_mode & 07777 == 0604)

            s2 = files.FileFromFilesystem(p, None)
            assert(s == s2)

            # make sure an os.unlink() happens
            s.restore(None, d, p)
        finally:
            shutil.rmtree(d)

        s2 = files.ThawFile(s.freeze(), None)
        assert(s == s2)

    def compareChownLog(self, expected):
        if len(self.chownLog) != len(expected):
            return False
        for index in range(len(expected)):
            e = expected[index]
            r = self.chownLog[index]
            p = e[0]
            g = r[0]
            if os.path.dirname(p) != os.path.dirname(g):
                return False
            if os.path.basename(g).find(os.path.basename(p)) == -1:
                return False
            if e[1] != r[1] or e[2] != r[2]:
                return False
        return True

    def compareChmodLog(self, expected):
        if len(self.chmodLog) != len(expected):
            return False
        for index in range(len(expected)):
            e = expected[index]
            r = self.chmodLog[index]
            p = e[0]
            g = r[0]
            if os.path.dirname(p) != os.path.dirname(g):
                return False
            if os.path.basename(g).find(os.path.basename(p)) == -1:
                return False
            if e[1] != r[1]:
                return False
        return True

    def testOwnership(self):
        f = files.RegularFile(None)
        f.inode.perms.set(0604)
        f.inode.mtime.set(0100)
        f.inode.owner.set("daemon")
        f.inode.group.set("bin")

        s = "hello world"
        contents = filecontents.FromString(s)
        f.contents = files.RegularFileStream()
        f.contents.size.set(len(s))
        f.contents.sha1.set(sha1helper.sha1String(s))

        f.flags.set(0)

        # and setuid root
        fr = files.RegularFile(None)
        fr.inode.perms.set(06755)
        fr.inode.mtime.set(0100)
        fr.inode.owner.set("root")
        fr.inode.group.set("root")
        fr.contents = files.RegularFileStream()
        fr.contents.size.set(len(s))
        fr.contents.sha1.set(sha1helper.sha1String(s))
        fr.flags.set(0)

        # and unwriteable
        fo = files.RegularFile(None)
        fo.inode.perms.set(0444)
        fo.inode.mtime.set(0100)
        fo.inode.owner.set("root")
        fo.inode.group.set("root")
        fo.contents = files.RegularFileStream()
        fo.contents.size.set(len(s))
        fo.contents.sha1.set(sha1helper.sha1String(s))
        fo.flags.set(0)

        # and secret
        fs = files.RegularFile(None)
        fs.inode.perms.set(0400)
        fs.inode.mtime.set(0100)
        fs.inode.owner.set("root")
        fs.inode.group.set("root")
        fs.contents = files.RegularFileStream()
        fs.contents.size.set(len(s))
        fs.contents.sha1.set(sha1helper.sha1String(s))
        fs.flags.set(0)

        f2 = f.copy()
        assert(f == f2)
        d = tempfile.mkdtemp()

        # before we mimic root, test a non-root of setu/gid file
        pr = d+"/setuid"
        fr.restore(contents, d, pr)
        assert not os.stat(pr).st_mode & 04000

        try:
            self.mimicRoot()
            p = d + "/file"
            f.restore(contents, d, p)
            assert self.compareChownLog([ (p, 2, 1) ])
            self.chownLog = []

            f.inode.owner.set("rootroot")
            self.logCheck(f.restore, (contents, d, p),
                          "warning: user rootroot does not exist - using root")
            assert self.compareChownLog([ (p, 0, 1) ])
            self.chownLog = []

            f.inode.owner.set("bin")
            f.inode.group.set("grpgrp")
            self.logCheck(f.restore, (contents, d, p),
                          "warning: group grpgrp does not exist - using root")
            assert self.compareChownLog([ (p, 1, 0) ])

            self.chmodLog = []
            pr = d+"/setuid"
            fr.restore(contents, d, pr)
            assert self.compareChmodLog([ (pr, 06755) ])
            assert os.stat(pr).st_mode & 07777 == 06755

            self.chmodLog = []
            po = d+"/unwriteable"
            fo.restore(contents, d, po)
            assert self.compareChmodLog([ (po, 0444) ])
            assert os.stat(po).st_mode & 07777 == 0444

            self.chmodLog = []
            ps = d+"/secret"
            fs.restore(contents, d, ps)
            assert self.compareChmodLog([ (ps, 0400) ])
            assert os.stat(ps).st_mode & 07777 == 0400
            self.chmodLog = []
        finally:
            self.realRoot()
            shutil.rmtree(d)

    def doTestDevices(self, t):
        if t == stat.S_IFBLK:
            d = files.BlockDevice(None)
        else:
            d = files.CharacterDevice(None)
        d.inode.perms.set(0604)
        d.inode.mtime.set(0100)
        d.inode.owner.set("daemon")
        d.inode.group.set("bin")
        d.flags.set(0)
        d.devt.major.set(1)
        d.devt.minor.set(2)
        assert(d.sizeString() == "  1,   2")
        path = tempfile.mkdtemp()
        try:
            p = path + "/dev2/foo"
            self.mimicRoot()
            d.restore(None, path, p)
            assert(self.mknodLog == [(p, t, os.makedev(1, 2))])
            assert(self.chownLog == [(p, 2, 1)])
        finally:
            self.realRoot()
            shutil.rmtree(path)

        d2 = files.ThawFile(d.freeze(), None)
        assert(d2 == d)

    def testBlock(self):
        self.doTestDevices(stat.S_IFBLK)

    def testChar(self):
        self.doTestDevices(stat.S_IFCHR)

    def testLinkGroup(self):
        lg1 = sha1helper.sha1FromString('1' * 40)
        lg2 = sha1helper.sha1FromString('2' * 40)
        lg3 = sha1helper.sha1FromString('3' * 40)

        # test creating a new instance from frozen data
        s = files.LinkGroupStream(lg1)
        s2 = files.LinkGroupStream(s.freeze())
        assert(s == s2)

        # test diff
        s2 = files.LinkGroupStream(lg2)
        assert(s != s2)
        diff = s.diff(s2)
        assert(diff == lg1)
        diff2 = s2.diff(s)
        assert(diff2 == lg2)

        # test three way merge
        assert(not s2.twm(diff, s2))
        assert(s == s2)

        # test a failed three way merge
        s3 = files.LinkGroupStream(lg3)
        assert(s2.twm(diff2, s3))

        # test applying a diff that was generated from a linkgroup
        # that is unset (s == None) to a linkgroup that is set.
        # s4 is an unset linkgroup
        s4 = files.LinkGroupStream()
        diff = s4.diff(s)
        # the diff has a special value in this case, "\0"
        assert(diff == "\0")
        assert(not s.twm(diff, s))
        # s should now be unset
        assert(s == s4)

    def testUnknownUser(self):
        # CNY-1071
        # Tests that FileFromFilesystem packs '+UID' as the owner (and '+GID'
        # as the group) if the owner/group don't exist.
        open('test-unknown-user', "w+").write('test\n')
        fd, fpath = tempfile.mkstemp()
        os.write(fd, "test\n")
        os.close(fd)

        uid, gid = self.findUnknownIds()

        import posix
        origLstat = os.lstat
        def myLstat(path):
            s = origLstat(path)
            if path == fpath:
                # Convert the stat info to a tuple
                s = tuple(s)
                # Replace st_uid and st_gid
                s = s[:4] + (uid, gid) + s[6:]
                # Convert to stat_result
                s = posix.stat_result(s)
                self.assertEqual(s.st_uid, uid)
                self.assertEqual(s.st_gid, gid)
            return s

        try:
            os.lstat = myLstat
            # No failure here
            f = files.FileFromFilesystem(fpath, None)
            self.assertEqual(f.inode.owner(), "+" + str(uid))
            self.assertEqual(f.inode.group(), "+" + str(gid))
        finally:
            os.lstat = origLstat

    def testCachePlusId(self):
        # CNY-1071
        # Tests that '+UID' and '+GID' get properly expanded
        try:
            myuid = os.getuid()
            mygid = os.getgid()
            myuser = files.userCache.lookupId('/', myuid)
            mygroup = files.groupCache.lookupId('/', mygid)

            # Look myself up
            self.assertEqual(myuid, files.userCache.lookupName('/', myuser))
            self.assertEqual(mygid, files.groupCache.lookupName('/', mygroup))

            # Plus
            strid = '+%d' % myuid
            self.assertEqual(myuid, files.userCache.lookupName('/', strid))
            strid = '+%d' % mygid
            self.assertEqual(mygid, files.groupCache.lookupName('/', strid))

            # Plus, non-existent user
            uid, gid = self.findUnknownIds()
            strid = '+%d' % uid
            self.assertEqual(uid, files.userCache.lookupName('/', strid))
            strid = '+%d' % gid
            self.assertEqual(gid, files.groupCache.lookupName('/', strid))
        finally:
            files.userCache.nameCache = {'root': 0}
            files.userCache.idCache = {0: 'root'}
            files.groupCache.nameCache = {'root': 0}
            files.groupCache.idCache = {0: 'root'}

    def testJournal(self):
        class Journal:
            def __init__(self):
                self.perms = []
                self.devnodes = []

            def lchown(self, root, target, uid, gid):
                self.perms.append((root, target, uid, gid))

            def mknod(self, root, target, devtype, major, minor, mode,
                      uid, gid):
                self.devnodes.append((root, target, devtype, major, minor,
                                      mode, uid, gid))
        path = tempfile.mkdtemp()
        try:
            journal = Journal()

            filelist = []
            for (name, cls) in (('/dev/block', files.BlockDevice),
                                ('/dev/char', files.CharacterDevice)):
                d = cls(None)
                d.inode.perms.set(0604)
                d.inode.mtime.set(0100)
                d.inode.owner.set("daemon")
                d.inode.group.set("bin")
                d.flags.set(0)
                d.devt.major.set(3)
                d.devt.minor.set(1)
                filelist.append((name, d))

            for name, d in filelist:
                p = path + name
                d.restore(None, path, p, journal=journal)
            assert(journal.devnodes ==
                   [(path, path + '/dev/block', 'b', 3, 1, 0604, 'daemon', 'bin'),
                    (path, path + '/dev/char', 'c', 3, 1, 0604, 'daemon', 'bin')])

            d = files.RegularFile(None)
            d.inode.perms.set(1755)
            d.inode.owner.set('root')
            d.inode.group.set('root')
            d.inode.mtime.set(0100)
            contents = filecontents.FromString('Hello, world')
            d.restore(contents, path, path + '/sbin/ping', journal=journal)
            assert(journal.perms == [(path, path + '/sbin/ping', 'root', 'root')])
        finally:
            shutil.rmtree(path)

    def testFileId(self):
        # this test verifies that the value produced as the fileId
        # of a known stream matches its pre-calculated value.
        f = files.RegularFile(None)
        f.inode.perms.set(0604)
        f.inode.mtime.set(0100)
        f.inode.owner.set("daemon")
        f.inode.group.set("bin")
        s = "hello world"
        contents = filecontents.FromString(s)
        f.contents = files.RegularFileStream()
        f.contents.size.set(len(s))
        f.contents.sha1.set(sha1helper.sha1String(s))
        f.flags.set(0)
        expectedId = 'a508d35e0768a05de81815cfadee498084849952'
        assert(f.freeze() == '-\x01\x00"\x01\x00\x08\x00\x00\x00\x00\x00\x00\x00\x0b\x02\x00\x14*\xael5\xc9O\xcf\xb4\x15\xdb\xe9_@\x8b\x9c\xe9\x1e\xe8F\xed\x03\x00\x04\x00\x00\x00\x00\x05\x00\x1b\x01\x00\x02\x01\x84\x02\x00\x04\x00\x00\x00@\x03\x00\x06daemon\x04\x00\x03bin' )
        assert(f.fileId() == sha1helper.sha1FromString(expectedId))

    def testContentsChanged(self):
        f = files.RegularFile(None)
        f.inode.perms.set(0444)
        f.inode.mtime.set(0100)
        f.inode.owner.set("root")
        f.inode.group.set("root")
        f.contents = files.RegularFileStream()
        s = 'hi'
        f.contents.size.set(len(s))
        f.contents.sha1.set(sha1helper.sha1String(s))
        f.flags.set(0)

        # this file stream diff has no contents change. verify that
        # contentsChanged returns the correct value
        diff = f.diff(f)
        assert(files.contentsChanged(diff) == False)

        f2 = f.copy()
        s = 'bye'
        f2.contents.size.set(len(s))
        f2.contents.sha1.set(sha1helper.sha1String(s))

        # this diff should have changed contents
        diff = f.diff(f2)
        assert(files.contentsChanged(diff) == True)

        # non-regular files should always return False
        s = files.SymbolicLink(None)
        s.inode.perms.set(0604)
        s.inode.mtime.set(0100)
        s.inode.owner.set('daemon')
        s.inode.group.set('bin')
        s.flags.set(0)
        s.target.set("/some/target")

        s2 = s.copy()
        s2.target.set("/some/other/target")
        diff = s.diff(s2)
        assert(files.contentsChanged(diff) == False)
        diff = s.diff(s)
        assert(files.contentsChanged(diff) == False)

        # test some pre-generated diffs - no change
        diff = '\x01-\x01\x00\x00\x05\x00"\x01\x00\x02\x01\xa4\x02\x00\x04B\x82=4\x03\x00\x08kvandine\x04\x00\x08kvandine'
        assert(files.contentsChanged(diff) == False)
        # this one has contents changed
        diff = '\x01-\x01\x00"\x01\x00\x08\x00\x00\x00\x00\x00\x00\x02q\x02\x00\x14\xac\x87%\xeb1a/&\xdf\x81\xb9O\xee\xf9\x895\xd4\xb8i\xd4\x05\x00\x1d\x02\x00\x04B\x82Ec\x03\x00\x08kvandine\x04\x00\x08kvandine'
        assert(files.contentsChanged(diff) == True)


    def testDiff(self):
        f = files.RegularFile(None)
        f.inode.perms.set(0444)
        f.inode.mtime.set(0100)
        f.inode.owner.set("root")
        f.inode.group.set("root")
        f.contents = files.RegularFileStream()
        s = 'hi'
        f.contents.size.set(len(s))
        f.contents.sha1.set(sha1helper.sha1String(s))
        f.flags.set(0)

        s = files.SymbolicLink(None)
        s.inode.perms.set(0604)
        s.inode.mtime.set(0100)
        s.inode.owner.set('daemon')
        s.inode.group.set('bin')
        s.flags.set(0)
        s.target.set("/some/target")

        # when the lsTag is different, the diff should just the frozen
        # file object
        assert(s.diff(f) == s.freeze())

    def testEmptyPerms(self):
        # make sure we can read in a file with no perms; we mistook '0'
        # for None (CNY-1678)
        fd, fpath = tempfile.mkstemp()
        os.write(fd, "test\n")
        os.close(fd)
        try:
            os.chmod(fpath, 000)
            f = files.FileFromFilesystem(fpath, None)
            assert(f.inode.owner() != None)
            assert(f.inode.group() != None)
            assert(f.inode.perms() == 0)
        finally:
            os.unlink(fpath)

    def testNewFileStreamElements(self):
        # ensure file streams can have elements added to them while preserving
        # the fileId's calculated by old versions of Conary; checks elements
        # added to the file object itself as well as elements added to a new
        # inode class
        class NewInodeStream(files.InodeStream):
            streamDict = { 255 : (streams.SMALL, streams.IntStream,
                                   "inodeInfo") }
            streamDict.update(files.InodeStream.streamDict)

        class NewFile(files.RegularFile):
            streamDict = { 255 : (streams.SMALL, streams.IntStream,
                                   "newItem") }
            streamDict.update(files.RegularFile.streamDict)
            streamDict[files.FILE_STREAM_INODE] = (streams.SMALL,
                                                   NewInodeStream, "inode")

        f = NewFile('pathId')
        f.contents.size.set(1024)
        f.contents.sha1.set('0' * 20)
        f.inode.inodeInfo.set(5)
        f.newItem.set(10)

        oldF = files.ThawFile(f.freeze(), 'pathId')
        assert(oldF.fileId() == f.fileId())

        f2 = NewFile('pathId', f.freeze())
        assert(f.freeze() == f2.freeze())
