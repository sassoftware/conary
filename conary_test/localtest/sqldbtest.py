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


from conary_test import rephelp

import os
import tempfile
import shutil

from conary import dbstore 
from conary.deps import deps
from conary.local import sqldb
from conary.versions import ThawVersion
from conary.versions import VersionFromString
from conary import files
from conary import trove
from conary.lib.sha1helper import md5FromString, sha1FromString, md5String
from conary_test import resources


class SqlDB(rephelp.RepositoryHelper):

    id1 = md5FromString("00010001000100010001000100010001")
    id2 = md5FromString("00010001000100010001000100010002")
    id3 = md5FromString("00010001000100010001000100010003")
    id4 = md5FromString("00010001000100010001000100010004")
    id7 = md5FromString("00010001000100010001000100010007")

    fid1 = sha1FromString("0001000100010001000100010001000100010001")
    fid2 = sha1FromString("0001000100010001000100010001000100010002")
    fid3 = sha1FromString("0001000100010001000100010001000100010003")
    fid4 = sha1FromString("0001000100010001000100010001000100010004")
    fid7 = sha1FromString("0001000100010001000100010001000100010007")

    v10 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10")
    v20 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-20")

    emptyFlavor = deps.Flavor()

    noneTup = (None, None, None)

    def testPins(self):
        def _checkPins(*args):
            assert(db.trovesArePinned(troves) == list(args))

        x86Flavor = deps.parseFlavor('is:x86(cmov)')

        troves = [ ("first", self.v10, self.emptyFlavor),
                   ("second", self.v20, self.emptyFlavor),
                   ("third", self.v10, x86Flavor) ]

        db = sqldb.Database(':memory:')

        for name, ver, flavor in troves:
            ti = db.addTrove(trove.Trove(name, ver, flavor, None))
            db.addTroveDone(ti)

        _checkPins(False, False, False)

        checks = [ False ] * 3
        for i in range(len(troves)):
            checks[i] = True
            db.pinTroves(*troves[i])
            _checkPins(*checks)

        for i in reversed(range(len(troves))):
            checks[i] = False
            db.pinTroves(*troves[i] + (False, ) )
            _checkPins(*checks)

    def testIterAllTroves(self):
        trv1 = self.addComponent('foo:lib', '1')
        trv2 = self.addComponent('foo:debuginfo', '1')
        trv3 = self.addCollection('foo', '1', [':lib', (':debuginfo', False)])
        trv4 = self.addComponent('bar:run', '1', 'bam', filePrimer=1)
        self.updatePkg(['foo', 'bar:run[bam]'])
        db = self.openDatabase()
        assert(set(db.iterAllTroves()) == set(x.getNameVersionFlavor() for x in [trv1, trv3, trv4]))

    def testDBInstances(self):
        cx = dbstore.connect(":memory:", driver="sqlite")
        cx.loadSchema()
        idb = sqldb.DBInstanceTable(cx)
        idb.addId('fred', 1, 2, [1, 2])
        assert(idb[('fred', 1, 2)] == 1)
        assert(idb.getId(1) == ('fred', 1,2,1) )
        assert(idb.get(('fred', 1, 2), None) == 1)

        idb.addId('wilma', 5, 6, [1, 2])
        assert(idb[('wilma', 5, 6)] == 2)
        idb.delId(2)
        self.assertRaises(KeyError, idb.__getitem__, ('wilma', 5, 6))
        idb.delId(1)
        assert(idb.get(('fred', 1, 2), None) == None)
    
    def testDBTroveFiles(self):
        cx = dbstore.connect(":memory:", driver="sqlite")
        cx.loadSchema()
        fs = sqldb.DBTroveFiles(cx)
        cu = cx.cursor()
        fs.addItem(cu, self.id1, 1, "/bin/ls", self.fid1, 11, "abc", [ "tag1", "tag2"])
        fs.addItem(cu, self.id2, 2, "/bin/cat", self.fid2, 11, "def", ["tag1"])
        fs.addItem(cu, self.id3, 1, "/bin/dd", self.fid3, 12, "tuv", [])
        fs.addItem(cu, self.id4, 2, "/bin/bc", self.fid4, 12, "xyz", [])
        assert([x for x  in fs[11]] == [("/bin/ls", "abc"),
                                        ("/bin/cat", "def")])
        assert([x for x  in fs[12]] == [("/bin/dd", "tuv"),
                                        ("/bin/bc", "xyz")])
        assert([x for x in fs.iterFilesWithTag('tag1') ] == 
                                    ['/bin/cat', '/bin/ls'] )
        assert([x for x in fs.iterFilesWithTag('tag2') ] == ['/bin/ls' ])
        
        fs.delInstance(11)
        assert([x for x  in fs[11]] == [])
        
        # make sure the tags are gone
        assert([x for x in fs.iterFilesWithTag('tag1') ] == [])
        cu = cx.cursor()
        cu.execute('select * from DBFileTags')
        assert([x for x in cu ] == [])

        assert(fs.getFileByFileId(self.fid3, 0) == ("/bin/dd", "tuv"))
        self.assertRaises(KeyError, fs.getFileByFileId, self.fid7, 0)

        assert([x for x  in fs[12]] == [("/bin/dd", "tuv"),
                                        ("/bin/bc", "xyz")])
        fs.removePath(12, "/bin/dd")
        assert([x for x  in fs.getByInstanceId(12)] == [("/bin/bc", "xyz")])
        assert(fs.getFileByFileId(self.fid4, justPresent = False) == 
                ('/bin/bc', 'xyz'))
        assert([x for x  in fs.getByInstanceId(12, justPresent = False)] == 
                [("/bin/dd", "tuv"), ("/bin/bc", "xyz")])
        fs.delInstance(12)

        fs.addItem(cu, self.id1, 1, "/bin/ls", self.fid1, 11, "abc", [])
        fs.addItem(cu, self.id2, 2, "/bin/cat", self.fid2, 11, "def", [])
        fs.addItem(cu, self.id3, 1, "/bin/dd", self.fid3, 11, "tuv", [])
        fs.addItem(cu, self.id4, 2, "/bin/bc", self.fid4, 11, "xyz", [])
        assert([x for x  in fs[11]] == [("/bin/ls", "abc"),
                                        ("/bin/cat", "def"),
                                        ("/bin/dd", "tuv"),
                                        ("/bin/bc", "xyz")])

    def testDatabase1(self):
        db = sqldb.Database(':memory:')

        f1 = files.FileFromFilesystem("/etc/passwd", self.id1)
        f2 = files.FileFromFilesystem("/etc/services", self.id2)
        f3 = files.FileFromFilesystem("/etc/group", self.id3)

        trv = trove.Trove("testcomp", self.v10, self.emptyFlavor, None)
        trv.addFile(self.id1, "/bin/1", self.v10, f1.fileId())
        trv.addFile(self.id2, "/bin/2", self.v10, f2.fileId())
        trv.addFile(self.id3, "/bin/3", self.v10, f3.fileId())
        trv.troveInfo.size.set(1234)
        trv.troveInfo.sourceName.set('thesource')

        req = deps.DependencySet()
        req.addDep(deps.FileDependencies, deps.Dependency("/bin/bash"))
        req.addDep(deps.TroveDependencies, deps.Dependency("foo:runtime"))
        req.addDep(deps.SonameDependencies, deps.Dependency("libtest.so.1"))
        trv.setRequires(req)

        trvInfo = db.addTrove(trv)

        db.addFile(trvInfo, f1.pathId(), "/bin/1", f1.fileId(), self.v10,
                   fileStream = f1.freeze())
        db.addFile(trvInfo, f2.pathId(), "/bin/2", f2.fileId(), self.v10,
                   fileStream = f2.freeze())
        db.addFile(trvInfo, f3.pathId(), "/bin/3", f3.fileId(), self.v10,
                   fileStream = f3.freeze())

        db.addTroveDone(trvInfo)

        dbTrv = db.getTroves([ ("testcomp", self.v10, self.emptyFlavor) ])[0]
        assert(dbTrv == trv)
        assert(dbTrv.__class__ == trove.Trove)
        assert(db.trovesArePinned([("testcomp", self.v10, self.emptyFlavor)])
                    == [ False ] )
        dbTrv = db.getTroves([ ("testcomp", self.v10, self.emptyFlavor) ],
                             withFileObjects = True)[0]
        assert(dbTrv == trv)
        assert(dbTrv.__class__ == trove.TroveWithFileObjects)
        for f in (f1, f2, f3):
            assert(dbTrv.getFileObject(f.fileId()) == f)

        trv2 = trove.Trove("testpkg", self.v10, self.emptyFlavor, None)
        ti = trv2.addTrove(trv.getName(), self.v10, trv.getFlavor())
        trv2.addTrove("weakref", self.v10, trv.getFlavor(), weakRef = True)
        ti = db.addTrove(trv2, pin = True)
        db.addTroveDone(ti)
        assert(db.trovesArePinned([("testpkg", self.v10, self.emptyFlavor)])
                    == [ True ] )
        assert(db.getTroves([ ("testpkg", self.v10, self.emptyFlavor) ])[0] == 
                                    trv2)
        assert(db.getTroves([ ("testpkg", self.v10, 
                              self.emptyFlavor) ])[0].getVersion().timeStamps() 
                    == trv2.getVersion().timeStamps())

        assert(db.getTroves(
                    [("testpkg", self.v10, self.emptyFlavor), 
                     ("testcomp", self.v10, self.emptyFlavor),
                     ("testitem", self.v10, self.emptyFlavor)],
                                True)
                 == [trv2, trv, None])

        assert(db.getTroves(
                    [("testpkg", self.v10, self.emptyFlavor), 
                     ("testcomp", self.v10, req) ],
                                True)
                 == [trv2, None ])

        assert(db.findTroveContainers(
                    ["testpkg", "testcomp"])  ==
                    [ [], [ ("testpkg", self.v10, self.emptyFlavor ) ] ])

        assert(db.getTroveContainers(
                    [("testpkg", self.v10, self.emptyFlavor),
                     ("testcomp", self.v10, self.emptyFlavor)]) ==
                    [ [], [ ("testpkg", self.v10, self.emptyFlavor ) ] ])

        res = db.findTroveReferences(
                    ["testpkg", "testcomp"])  

        assert(db.findTroveReferences(
                    ["testpkg", "testcomp"])  ==
                    [ [], [ ("testcomp", self.v10, self.emptyFlavor ) ] ])

        v10new = VersionFromString("/conary.rpath.com@test:trunk/1.2-10")
        assert(db.getTroves([("testpkg", v10new, self.emptyFlavor)])[0] == trv2)
        assert(db.getTroves([("testpkg", v10new, 
                    self.emptyFlavor)])[0].getVersion().timeStamps() == 
                        trv2.getVersion().timeStamps())

        assert(set(db.findByNames([ 'testpkg', 'testcomp' ])) == 
                    set([("testpkg", self.v10, self.emptyFlavor),
                         ("testcomp", self.v10, self.emptyFlavor)]))

        db.eraseTrove("testcomp", self.v10, None)
        assert(db.getTroves([("testpkg", self.v10, self.emptyFlavor)])[0] == trv2)

        trv.computePathHashes()
        trvInfo = db.addTrove(trv)
        db.addFile(trvInfo, f1.pathId(), "/bin/1", f1.fileId(), self.v10,
                   fileStream = f1.freeze())
        db.addFile(trvInfo, f2.pathId(), "/bin/2", f2.fileId(), self.v10,
                   fileStream = f1.freeze())
        db.addFile(trvInfo, f3.pathId(), "/bin/3", f3.fileId(), self.v10,
                   fileStream = f1.freeze())
        db.addTroveDone(trvInfo)

        assert(db.getTroves([("testcomp", self.v10, self.emptyFlavor)])[0] == trv)
        db.removeFileFromTrove(trv, "/bin/1")
        changedTrv = db.getTroves([trv.getNameVersionFlavor()],
                                  pristine = False)[0]
        otherChangedTrv = db.getTroves([trv.getNameVersionFlavor()],
                                       withFiles = False,
                                       pristine = False)[0]
        assert(len(changedTrv.idMap) + 1 == len(trv.idMap))
        assert(len(changedTrv.troveInfo.pathHashes) + 1 ==
               len(trv.troveInfo.pathHashes))
        assert(changedTrv.troveInfo.pathHashes ==
               otherChangedTrv.troveInfo.pathHashes)
        assert(len(otherChangedTrv.idMap) == 0)
        changedTrv.addFile(self.id1, "/bin/1", self.v10, f1.fileId())
        assert(changedTrv.idMap == trv.idMap)
        changedTrv.computePathHashes()
        assert(changedTrv.troveInfo.pathHashes == trv.troveInfo.pathHashes)

        assert(db.getTroves([("testcomp", self.v10, self.emptyFlavor)], pristine = True)[0] == trv)
        db.eraseTrove("testpkg", self.v10, None)
        assert(db.getTroves([ ("testpkg", self.v10, self.emptyFlavor)]) ==
                    [ None ] )
        self.assertRaises(KeyError, db.instances.getVersion, 100)

        db.eraseTrove("testcomp", self.v10, None)
        db.commit()
        cu = db.db.cursor()

        # make sure the versions got removed; the None entry is still there
        cu.execute("SELECT count(*) FROM Versions")
        assert(cu.next()[0] == 1)

        # make sure the dependency table got cleaned up
        cu.execute("SELECT count(*) FROM Dependencies")
        assert(cu.next()[0] == 0)

        # make sure the instances table got cleaned up
        cu.execute("SELECT count(*) FROM Instances")
        assert(cu.next()[0] == 0)

        # make sure the troveInfo table got cleaned up
        cu.execute("SELECT count(*) FROM TroveInfo")
        assert(cu.next()[0] == 0)

    def testDatabase2(self):
        db = sqldb.Database(':memory:')

        f1 = files.FileFromFilesystem("/etc/passwd", self.id1)
        f2 = files.FileFromFilesystem("/etc/services", self.id2)
        f3 = files.FileFromFilesystem("/etc/group", self.id3)

        trv = trove.Trove("testcomp", self.v10, self.emptyFlavor, None)
        trv.addFile(self.id1, "/bin/1", self.v10, f1.fileId())
        trv.addFile(self.id2, "/bin/2", self.v10, f2.fileId())
        trv.addFile(self.id3, "/bin/3", self.v10, f3.fileId())
        trvInfo = db.addTrove(trv)

        db.addFile(trvInfo, f1.pathId(), "/bin/1", f1.fileId(), self.v10,
                   fileStream = f1.freeze())
        db.addFile(trvInfo, f2.pathId(), "/bin/2", f2.fileId(), self.v10,
                   fileStream = f2.freeze())
        db.addFile(trvInfo, f3.pathId(), "/bin/3", f3.fileId(), self.v10,
                   fileStream = f3.freeze())

        db.addTroveDone(trvInfo)

        assert(db.getTroves(
                    [("testcomp", self.v10, self.emptyFlavor), 
                     ("testcomp", self.v20, self.emptyFlavor) ], True)
                 == [trv, None ])
        assert(db.hasTroves(
                    [("testcomp", self.v10, self.emptyFlavor), 
                     ("testcomp", self.v20, self.emptyFlavor) ])
                 == [True, False ])

        f2 = files.FileFromFilesystem("/etc/hosts", self.id2)

        trv2 = trove.Trove("testcomp", self.v20, self.emptyFlavor, None)
        trv2.addFile(self.id1, "/bin/1", self.v10, self.fid1)
        trv2.addFile(self.id2, "/bin/2", self.v20, self.fid2)

        #trvInfo = db.addTrove(trv2)
        #db.addFile(trvInfo, f2.pathId(), f2, "/bin/2", self.v20)

        #assert(db.getTrove("testcomp", self.v20, None) == trv2)

    def testDatabaseTransactionCounter(self):
        db = sqldb.Database(':memory:')
        field = 'transaction counter'
        # We should have a row
        cu = db.db.cursor()
        cu.execute("SELECT value FROM DatabaseAttributes WHERE name = ?",
            field)
        row = cu.next()
        self.assertEqual(row[0], '0')

        updateq = "UPDATE DatabaseAttributes SET value = ? WHERE name = ?"
        # Update it manually
        cu.execute(updateq, '10', field)
        db.db.commit()

        self.assertEqual(db.getTransactionCounter(), 10)

        # Increment it
        self.assertEqual(db.incrementTransactionCounter(), 11)
        self.assertEqual(db.getTransactionCounter(), 11)

        # Delete entry, should reset the counter
        cu.execute("DELETE from DatabaseAttributes WHERE name = ?", field)
        db.db.commit()
        self.assertEqual(db.getTransactionCounter(), 0)
        self.assertEqual(db.incrementTransactionCounter(), 1)
        self.assertEqual(db.getTransactionCounter(), 1)

        self.assertEqual(db.incrementTransactionCounter(), 2)
        self.assertEqual(db.getTransactionCounter(), 2)

        # Mess it up
        cu.execute(updateq, 'not an integer', field)
        self.assertEqual(db.getTransactionCounter(), 0)
        self.assertEqual(db.incrementTransactionCounter(), 1)
        self.assertEqual(db.getTransactionCounter(), 1)

    def testDatabaseTroveInfoCleanup(self):
        # remove a trove but leave a reference - its troveInfo data 
        # should be removed
        flavor1 = deps.parseFlavor('is:x86(cmov)')
        db = sqldb.Database(':memory:')
        trv1 = trove.Trove("testcomp:runtime", self.v10, flavor1, None)
        trvInfo = db.addTrove(trv1)
        db.addTroveDone(trvInfo)
        trv2 = trove.Trove("testcomp", self.v10, flavor1, None)
        trv2.addTrove(*trv1.getNameVersionFlavor())
        trvInfo = db.addTrove(trv2)
        db.addTroveDone(trvInfo)
        db.commit()

        db.eraseTrove("testcomp:runtime", self.v10, flavor1)
        db.commit()

        cu = db.db.cursor()
        cu.execute('select count(*) from instances join troveInfo '
                    'using(instanceId) where troveName="testcomp:runtime"')
        assert(not cu.fetchall()[0][0])

    def testDatabaseVersionCleanup(self):
        flavor1 = deps.parseFlavor('is:x86(cmov)')
        db = sqldb.Database(':memory:')
        cu = db.db.cursor()
        v10 = ThawVersion('/conary.rpath.com@test:trunk/10:1.2-10')
        v20 = ThawVersion('/conary.rpath.com@test:trunk/10:1.2-20')
        v30 = ThawVersion('/conary.rpath.com@test:trunk/10:1.2-30')

        # test file version cleanup
        trv = trove.Trove('testcomp:runtime', v10, flavor1, None)
        f1 = files.FileFromFilesystem('/etc/passwd', self.id1)
        f2 = files.FileFromFilesystem('/etc/services', self.id2)
        f3 = files.FileFromFilesystem('/etc/group', self.id3)
        trv.addFile(self.id1, '/bin/1', v10, f1.fileId())
        trv.addFile(self.id2, '/bin/2', v20, f2.fileId())
        trv.addFile(self.id3, '/bin/3', v30, f3.fileId())
        trvInfo = db.addTrove(trv)
        db.addFile(trvInfo, f1.pathId(), '/bin/1', f1.fileId(), v10,
                   fileStream = f1.freeze())
        db.addFile(trvInfo, f2.pathId(), '/bin/2', f2.fileId(), v20,
                   fileStream = f2.freeze())
        db.addFile(trvInfo, f3.pathId(), '/bin/3', f3.fileId(), v30,
                   fileStream = f3.freeze())
        db.addTroveDone(trvInfo)
        # check to see how many versions we have
        cu.execute('select count(*) from Versions')
        count = cu.fetchall()[0][0]
        # should have only 0|NULL
        self.assertEqual(count, 4)
        # now erase
        db.eraseTrove('testcomp:runtime', v10, flavor1)
        db.commit()
        cu.execute('select count(*) from Versions')
        count = cu.fetchall()[0][0]
        # should have only 0|NULL
        self.assertEqual(count, 1)

        # test trove version cleanup
        trv1 = trove.Trove('testcomp:runtime', v10, flavor1, None)
        trv2 = trove.Trove('group-test', v20, flavor1, None)
        trv2.addTrove(trv1.getName(), trv1.getVersion(), trv1.getFlavor(),
                      weakRef=True)
        trvInfo = db.addTrove(trv2)
        db.addTroveDone(trvInfo)
        db.commit()
        cu.execute('select count(*) from Versions')
        count = cu.fetchall()[0][0]
        self.assertEqual(count, 3)

        db.eraseTrove('group-test', v20, flavor1)
        db.commit()
        cu.execute('select count(*) from Versions')
        count = cu.fetchall()[0][0]
        # should have only 0|NULL
        self.assertEqual(count, 1)

    def testMultipleFlavorsInstalled(self):
        """
        verify that only one (unique) flavor is returned from
        db.iterVersionByName if multiple troves with the same version
        but different flavors are installed at the same time
        """
        db = sqldb.Database(':memory:')
        flavor1 = deps.parseFlavor('is:x86(cmov)')
        flavor2 = deps.parseFlavor('is:x86(sse)')
        trv1 = trove.Trove("testcomp", self.v10, flavor1, None)
        trv2 = trove.Trove("testcomp", self.v10, flavor2, None)
        ti = db.addTrove(trv1)
        db.addTroveDone(ti)
        ti = db.addTrove(trv2)
        db.addTroveDone(ti)
        assert([ x for x in db.iterVersionByName('testcomp', False) ] == [ self.v10 ])
        assert([ x for x in db.iterVersionByName('testcomp', True) ] == 
                                    [ (self.v10, flavor1), (self.v10, flavor2) ])

    def testGroupMissingComponent(self):
        flavor1 = deps.parseFlavor('is:x86(cmov)')
        db = sqldb.Database(':memory:')
        trv1 = trove.Trove("group1", self.v10, flavor1, None)
        ti = trv1.addTrove("subcomp", self.v10, flavor1)
        ti = trv1.addTrove("subcomp2", self.v10, flavor1)
        ti = trv1.addTrove("subcomp3", self.v10, flavor1, weakRef=True)
        ti = db.addTrove(trv1)
        db.addTroveDone(ti)

        trv2 = trove.Trove("group2", self.v10, flavor1, None)
        trv2.addTrove("subcomp", self.v10, flavor1, weakRef=True)
        ti = db.addTrove(trv2)
        db.addTroveDone(ti)

        trv3 = trove.Trove("subcomp2", self.v10, flavor1, None)
        ti = db.addTrove(trv3)
        db.addTroveDone(ti)

        inst, instRefed, strongMissing, weakMissing = db.getCompleteTroveSet( 
                            [ "group1", "subcomp", "subcomp2", "subcomp3" ] )
        
        assert(inst == set([ ("group1", self.v10, flavor1) ]))
        # this ensures the version returns has timestamps on it
        assert([ x for x in inst ][0][1].freeze())
        assert(strongMissing == set([ ("subcomp", self.v10, flavor1) ]))
        assert(weakMissing == set([ ("subcomp3", self.v10, flavor1) ]))
        assert(instRefed == set([ ("subcomp2", self.v10, flavor1) ]))

    def testVersion2Migration(self):
        dbfile = os.path.join(resources.get_archive(), 'conarydbs',
                              'conarydb-version-2')
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        shutil.copyfile(dbfile, fn)
        db, str = self.captureOutput(sqldb.Database, fn)
        cu = db.db.cursor()
        # make sure that the -*none*- entry has been added
        cu.execute('select count(*) from provides join dependencies on provides.depid=dependencies.depid and name="sqlite:lib" and flag="-*none*-"')
        assert(cu.next() == (1,))
        db.close()
        os.unlink(fn)

    def testVersion3Migration(self):
        dbfile = os.path.join(resources.get_archive(), 'conarydbs',
                              'conarydb-version-3')
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        shutil.copyfile(dbfile, fn)
        db, str = self.captureOutput(sqldb.Database, fn)
        cu = db.db.cursor()
        db.close()
        os.unlink(fn)

    def testVersion11Migration(self):
        dbfile = os.path.join(resources.get_archive(), 'conarydbs',
                              'conarydb-version-11')
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        shutil.copyfile(dbfile, fn)
        db, str = self.captureOutput(sqldb.Database, fn)
        cu = db.db.cursor()

        # make sure there aren't any install buckets left
        cu.execute("select count(*) from troveinfo where infoType=?",
                   trove._TROVEINFO_TAG_INSTALLBUCKET)
        assert(cu.next()[0] == 0)
        
        # make sure the path hashs look right for libpng:lib
        cu.execute("select data from troveinfo, instances where "
                   "trovename='libpng:lib' and "
                   "troveinfo.instanceid=instances.instanceid "
                   "and infoType=?", trove._TROVEINFO_TAG_PATH_HASHES)
        ph = trove.PathHashes(cu.next()[0])

        cu.execute("select path from instances, dbtrovefiles where "
                   "instances.instanceid=dbtrovefiles.instanceid "
                   "and troveName='libpng:lib'")
        for path, in cu:
            hash = md5String(path)[:8]
            assert(hash in ph)
            ph.remove(hash)

        assert(not ph)

        db.close()
        os.unlink(fn)

    def testVersion20Migration(self):
        dbfile = os.path.join(resources.get_archive(), 'conarydbs',
                              'conarydb-version-19')
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        shutil.copyfile(dbfile, fn)
        # get a list of tables
        db = dbstore.connect(fn, driver='sqlite')
        db.loadSchema()
        cu = db.cursor()
        tableCounts = dict.fromkeys(db.tables.keys())
        for table in tableCounts.keys():
            tableCounts[table] = cu.execute('select count(*) from %s' %table).fetchall()[0][0]
        # DBInstances is gone...
        tableCounts.pop('DBInstances')
        # we have a VersionId 0 entry now
        tableCounts['Versions'] += 1
        # new table added
        tableCounts['DatabaseAttributes'] = 1

        # do the migration
        db, str = self.captureOutput(sqldb.Database, fn)
        cu = db.db.cursor()

        # make sure we have all the tables
        db2 = dbstore.connect(fn, driver='sqlite')
        db2.loadSchema()
        cu = db2.cursor()
        tableCounts2 = dict.fromkeys(db2.tables.keys())
        for table in tableCounts2.keys():
            tableCounts2[table] = cu.execute('select count(*) from %s' %table).fetchall()[0][0]
        self.assertEqual(tableCounts, tableCounts2)

        # check to make sure that we fixed our broken deps and troveinfo
        cu.execute("select count(*) from troveinfo where infoType=3 "
                   "and hex(data) == '31'");
        assert(cu.next()[0] == 0)
        cu.execute("select count(*) from dependencies where "
                   "name like 'conary:%' and flag='1'")
        assert(cu.next()[0] == 0)

        # verify the conary:runtime trove
        v = VersionFromString('/conary.rpath.com@rpl:devel//1/1.0-2-0.1')
        f = deps.parseFlavor('~!bootstrap is: x86')
        t = db.getTroves([('conary:runtime', v, f)])[0]
        t.verifyDigitalSignatures()

        # verify that we can insert a '1.0' into deps and troveinfo
        cu.execute("insert into Dependencies values (NULL, 4, 'test', '1.0')")
        cu.execute("select flag from Dependencies where name='test'")
        assert(cu.next()[0] == '1.0')

        cu.execute("insert into TroveInfo values (300, 3, '1.0')")
        cu.execute("select data from TroveInfo where instanceId=300 and "
                   "infotype=3")
        assert(cu.next()[0] == '1.0')

        db.close()
        db2.close()

        # make sure the addition of DatabaseAttributes happens correctly
        db = dbstore.connect(fn, driver='sqlite')
        db.loadSchema()
        self.assertTrue('DatabaseAttributes' in db.tables)
        cu = db.cursor()
        cu.execute("DROP TABLE DatabaseAttributes")
        db.commit()
        db.close()

        sdb = sqldb.Database(fn)
        self.assertTrue('DatabaseAttributes' in sdb.db.tables)
        del sdb
        
        os.unlink(fn)

    def testVersion20DeAnalyze(self):
        dbfile = os.path.join(resources.get_archive(), 'conarydbs',
                              'conarydb-version-20-with-analyze')
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        shutil.copyfile(dbfile, fn)
        db = sqldb.Database(fn)
        cu = db.db.cursor()
        cu.execute('select count(*) from sqlite_stat1')
        count = cu.fetchall()[0][0]
        self.assertEqual(count, 0)
        db.close()
        os.unlink(fn)

    def testMigrationReadOnly(self):
        dbfile = os.path.join(resources.get_archive(), 'conarydbs',
                              'conarydb-version-2')
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        shutil.copyfile(dbfile, fn)
        os.chmod(fn, 0444)
        try:
            db = sqldb.Database(fn)
        except sqldb.OldDatabaseSchema:
            pass
        else:
            raise
        os.unlink(fn)

    def testSubtroveUpdates(self):
        db = sqldb.Database(':memory:')
        
        pkg = trove.Trove("testpkg", self.v10, self.emptyFlavor, None)
        pkg.addTrove("testpkg:comp", self.v10, self.emptyFlavor)
        ti = db.addTrove(pkg)
        db.addTroveDone(ti)

        comp1 = trove.Trove("testpkg:comp", self.v10, self.emptyFlavor, None)
        ti = db.addTrove(comp1)
        db.addTroveDone(ti)
        comp2 = trove.Trove("testpkg:comp", self.v20, self.emptyFlavor, None)
        ti = db.addTrove(comp1)
        db.addTroveDone(ti)
        #
        ti = db.addTrove(
        #("testpkg:comp", self.v10, self.emptyFlavor), 
                    comp2)
        db.addTroveDone(ti)
        db.eraseTrove("testpkg:comp", self.v10, self.emptyFlavor)

        pristinePkg = db.getTroves([("testpkg", self.v10, self.emptyFlavor)],
                                  pristine = True)[0]
        instPkg = db.getTroves([("testpkg", self.v10, self.emptyFlavor)],
                               pristine = False)[0]
        assert(pristinePkg == pkg)
        assert(instPkg != pkg)

        ti = db.addTrove(
            #("testpkg:comp", self.v20, self.emptyFlavor), 
                    comp1)
        db.addTroveDone(ti)
        db.eraseTrove("testpkg:comp", self.v20, self.emptyFlavor)

        pristinePkg = db.getTroves([("testpkg", self.v10, self.emptyFlavor)],
                                   pristine = True)[0]
        instPkg = db.getTroves([("testpkg", self.v10, self.emptyFlavor)],
                               pristine = False)[0]
        assert(pristinePkg == pkg)
        assert(instPkg == pkg)

        # make sure there aren't broken bits in TroveTroves
        assert(db.db.cursor().execute("select count(*) from trovetroves").next()[0]
                    == 1)

    def testMapPinned(self):
        # test to ensure that a if you update a pinned trove twice,
        # a duplicate entry does not show up in TroveTroves. 
        db = sqldb.Database(':memory:')
        
        pkg = trove.Trove("testpkg", self.v20, self.emptyFlavor, None)
        pkg.addTrove("testpkg:comp", self.v20, self.emptyFlavor)
        ti = db.addTrove(pkg)
        db.addTroveDone(ti)

        comp1 = trove.Trove("testpkg:comp", self.v10, self.emptyFlavor, None)
        ti = db.addTrove(comp1)
        db.addTroveDone(ti)
        # we've got a trove with a link from trove to component.  
        # we assume the component was pinned at v10...now we need to update
        # the link to point to the v10 ver.
        db.mapPinnedTroves([('testpkg:comp', (self.v10, self.emptyFlavor),
                                             (self.v20, self.emptyFlavor))])
        cu = db.db.cursor()
        assert(cu.execute('SELECT COUNT(*) FROM TroveTroves WHERE inPristine=0').next()[0] == 1)
        db.mapPinnedTroves([('testpkg:comp', (self.v10, self.emptyFlavor),
                                             (self.v20, self.emptyFlavor))])
        assert(cu.execute('SELECT COUNT(*) FROM TroveTroves WHERE inPristine=0').next()[0] == 1)
        pkg = db.getTroves([('testpkg', self.v20, self.emptyFlavor)], pristine=False)[0]
        assert(pkg.strongTroves.keys()[0][1] == self.v10)
        assert(pkg.weakTroves.keys() == [])

class SqlDBWithRepos(rephelp.RepositoryHelper):
    def testIterUpdateContainerInfo(self):
        self.addComponent('test:run', '1')
        self.addComponent('test:run', '2')
        self.addCollection('test', '1', [':run'])
        self.addCollection('test', '2', [':run'])
        self.addCollection('group-test', '1', ['test'])
        self.updatePkg(['test=2', 'test:run=2', 'group-test=1'], recurse=False)

        db = self.openDatabase()
        updateTroves = set(x[0][0] for x in db.iterUpdateContainerInfo())
        assert(updateTroves == set(['test', 'group-test', 'test:run']))
