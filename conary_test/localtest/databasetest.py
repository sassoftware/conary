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


import os
import shutil
import tempfile

from testrunner import testhelp
from conary.deps.deps import parseFlavor
from conary.deps import deps
from conary.local import database
from conary.versions import Label
from conary.versions import ThawVersion
from conary.versions import VersionFromString
from conary import dbstore
from conary import errors
from conary import files
from conary import trove
from conary.lib.sha1helper import md5FromString
from conary_test import resources

class Database(testhelp.TestCase):

    id1 = md5FromString("00010001000100010001000100010001")
    id2 = md5FromString("00010001000100010001000100010002")

    def testFindTrove(self):
        db = database.Database(':memory:', ':memory:')
        self.assertEqual(db.getTransactionCounter(), 0)
        flavor = deps.parseFlavor('~readline,!foo')
        v10 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10-1")
        f1 = files.FileFromFilesystem("/etc/passwd", self.id1)
        trv = trove.Trove("testcomp", v10, flavor, None)
        trv.addFile(self.id1, "/bin/1", v10, f1.fileId())
        trvInfo = db.addTrove(trv)
        db.addTroveDone(trvInfo)
        f2 = files.FileFromFilesystem("/etc/group", self.id2)
        v20 = ThawVersion("/conary.rpath.com@local:blah/20:1.3-2-1")
        trv = trove.Trove("testcomp", v20, flavor, None)
        trv.addFile(self.id2, "/bin/2", v20, f2.fileId())
        trvInfo = db.addTrove(trv)
        db.addTroveDone(trvInfo)
        tup =  [('testcomp', v10, flavor)]
        tup2 =  [('testcomp', v10, flavor), ('testcomp', v20, flavor)]
        assert(db.findTrove(None, ('testcomp', '1.2-10-1', None)) == tup)
        assert(db.findTrove(None, ('testcomp', '1.2', None)) == tup)
        assert(set(db.findTrove(None, 
                        ('testcomp', None, parseFlavor('!foo')))) == set(tup2))
        assert(db.findTrove(None, ('testcomp', ':trunk', None)) == tup)
        assert(db.findTrove([Label('conary.rpath.com@test:foo')], 
                            ('testcomp', ':trunk', None)) == tup)
        assert(db.findTrove(None, ('testcomp', ':trunk', None)) == tup)
        assert(db.findTrove(None, ('testcomp', '@test:trunk', None)) == tup)
        assert(db.findTrove([Label('conary.rpath.com@blah:foo')], 
                            ('testcomp', '@test:trunk', None)) == tup)
        # Transaction counter changes upon commit
        self.assertEqual(db.getTransactionCounter(), 0)
        db.commit()
        self.assertEqual(db.getTransactionCounter(), 1)

    def testIntermediateDirNotWritable(self):
        # CNY-2405
        d = tempfile.mkdtemp()
        try:
            os.mkdir(d + '/var', 0400)
            try:
                db = database.Database(d, '/var/lib/conarydb/conarydb')
            except database.OpenError, e:
                self.assertEqual(str(e),
                       'Unable to open database %s/var/lib/conarydb/conarydb: '
                       'cannot create directory %s/var/lib' % (d, d))
        finally:
            os.chmod(d + '/var', 0755)
            shutil.rmtree(d)

    def testRootIsNotDir(self):
        # CNY-814
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        try:
            f = open(fn, 'w')
            f.close()
            try:
                db = database.Database(fn, '/var/lib/conarydb/conarydb')
            except database.OpenError, e:
                self.assertEqual(str(e),
                       'Unable to open database %s/var/lib/conarydb/conarydb: '
                       '%s is not a directory' % (fn, fn))
        finally:
            os.unlink(fn)

        d = tempfile.mkdtemp()
        f = open(d + '/var', 'w')
        f.close()
        try:
            f = open(fn, 'w')
            f.close()
            try:
                db = database.Database(d, '/var/lib/conarydb/conarydb')
            except database.OpenError, e:
                self.assertEqual(str(e),
                       'Unable to open database %s/var/lib/conarydb/conarydb: '
                       '%s/var is not a directory' % (d, d))
        finally:
            shutil.rmtree(d)

    def testDbIsLocked(self):
        # CNY-1175
        def fakeConnect(*args, **kw):
            raise dbstore.sqlerrors.DatabaseLocked('database is locked')
        d = tempfile.mkdtemp()
        oldconnect = dbstore.connect
        try:
            dbstore.connect = fakeConnect
            db = database.Database(d, '/var/lib/conarydb/conarydb')
            try:
                db.iterAllTroves()
            except errors.DatabaseLockedError, e:
                self.assertEqual(str(e), "The local database is locked.  It is possible that a database journal file exists that needs to be rolled back, but you don't have write permission to the database.")
            except:
                self.fail('unexpected exception')
            else:
                self.fail('expected exception not raised')
        finally:
            dbstore.connect = oldconnect
            shutil.rmtree(d)

    def testCommitLock(self):
        d = tempfile.mkdtemp()
        try:
            db = database.Database(d, '/var/lib/conarydb/conarydb')
            db.commitLock(True)
            childPid = os.fork()
            if childPid == 0:
                db.close()
                db = database.Database(d, '/var/lib/conarydb/conarydb')
                try:
                    db.commitLock(True)
                except errors.DatabaseLockedError:
                    os._exit(0)
                except Exception:
                    pass

                os._exit(1)

            pid, status = os.waitpid(childPid, 0)
            assert(status == 0)
        finally:
            shutil.rmtree(d)

    def testCommitLockWithMemoryDB(self):
        d = tempfile.mkdtemp()
        db = database.Database(d, ':memory:')
        db.commitLock(True)
        db.close()
        shutil.rmtree(d)

    def testCannotMakeDirs(self):
        d = tempfile.mkdtemp()
        try:
            os.chmod(d, 0500)
            self.assertRaises(database.OpenError, database.Database,
                              d, '/foo/conarydb')
        finally:
            os.chmod(d, 0700)
            shutil.rmtree(d)

    def testDbTimeout(self):
        # CNY-1840
        d = tempfile.mkdtemp()
        tmout = 12345
        try:
            db = database.Database(d, '/conarydb', timeout = tmout)
            self.assertEqual(db.db.timeout, tmout)
        finally:
            shutil.rmtree(d)

    def testLoadInvocationInfo(self):
        # CNY-2580
        jobFile = os.path.join(resources.get_archive(), "job-invocation")
        uJob = database.UpdateJob(None)
        uJob.loadInvocationInfo(jobFile)
        self.assertEqual(uJob.getItemList(),
            [('group-appliance', (None, None), (None, None), True)])

    def testEnsureReadableRollbacks(self):
        from conary.lib import util
        d = tempfile.mkdtemp()
        util.mkdirChain(d + '/var/lib/conarydb/conarydb/rollbacks')
        try:
            open(d + '/var/lib/conarydb/conarydb/rollbacks/status', 'w').write('0 0\n')
            db = database.Database(d, '/var/lib/conarydb/conarydb')
            db._ensureReadableRollbackStack()
        finally:
            shutil.rmtree(d)

    def testGetCapsulesTroveList(self):
        # make sure that getCapsulesTroveList is at least not removed...
        from conary.lib import util
        d = tempfile.mkdtemp()
        util.mkdirChain(d + '/var/lib/conarydb/conarydb/')
        db = database.Database(d, '/var/lib/conarydb/conarydb')
        db.getCapsulesTroveList(db.iterAllTroves())

class UpdateFeaturesTest(testhelp.TestCase):
    def testSetUpdateFeatures(self):
        ft = database.UpdateJobFeatures()
        self.assertEqual(ft.postRollbackScriptsOnRollbackStack, True)

        ft.setAll(False)
        self.assertEqual(ft.postRollbackScriptsOnRollbackStack, False)

        ft.setAll(True)
        self.assertEqual(ft.postRollbackScriptsOnRollbackStack, True)

    def testUpdateFeaturesLoadSave(self):
        fd, fpath = tempfile.mkstemp()
        os.close(fd)
        os.unlink(fpath)

        ft = database.UpdateJobFeatures()

        # Loading from a file that doesn't exist - flags should be unset
        ft.loadFromFile(fpath)
        self.assertEqual(ft.postRollbackScriptsOnRollbackStack, False)

        # No flags set
        ft.setAll(False)
        ft.saveToFile(fpath)
        self.assertEqual(file(fpath).read(), "")

        # Empty file means no flags set
        ft = database.UpdateJobFeatures()
        ft.loadFromFile(fpath)
        self.assertEqual(ft.postRollbackScriptsOnRollbackStack, False)

        # Saving to a bad file
        ft = database.UpdateJobFeatures()
        self.assertRaises(IOError, ft.saveToFile, "/some/path")

        ft.saveToFile(fpath)
        self.assertTrue(os.path.exists(fpath))
        self.assertEqual(file(fpath).readline().strip(),
                            "postRollbackScriptsOnRollbackStack")

        # Munge permissions
        os.chmod(fpath, 0)
        ft.loadFromFile(fpath)
        self.assertEqual(ft.postRollbackScriptsOnRollbackStack, False)

        # Munge permissions back
        os.chmod(fpath, 0644)
        ft.loadFromFile(fpath)
        self.assertEqual(ft.postRollbackScriptsOnRollbackStack, True)

        os.unlink(fpath)

class UpdateJobFreezeThawTests(testhelp.TestCase):
    def setUp(self):
        testhelp.TestCase.setUp(self)
        self.workDir = tempfile.mkdtemp()

    def tearDown(self):
        testhelp.TestCase.tearDown(self)
        shutil.rmtree(self.workDir, ignore_errors = True)

    def testFreezeThawJobOrder(self):
        db = database.Database(':memory:', ':memory:')
        uJob = database.UpdateJob(db)
        uJob.setTransactionCounter(100)
        uJob.freeze(self.workDir)

        uJob = database.UpdateJob(db)
        uJob.thaw(self.workDir)
        self.assertEqual(uJob._jobPreScriptsByJob, None)
        self.assertEqual(uJob.getTransactionCounter(), 100)
        shutil.rmtree(self.workDir); os.mkdir(self.workDir)

        uJob = database.UpdateJob(db)
        uJob.setTransactionCounter(100)
        # The third argument is bogus, but thawing should still work
        uJob._jobPreScriptsByJob = [
            [(0, [0, 1], "junk"), (1, [2, 3], "more junk")],
            [(10, [10, 11], "junk"), (11, [12, 13], "more junk")],
            [(20, [20, 21], "junk"), (21, [22, 23], "more junk")],
            ["just junk"],
        ]
        uJob.freeze(self.workDir)

        uJob = database.UpdateJob(db)
        uJob.thaw(self.workDir)
        self.assertEqual(uJob._jobPreScriptsByJob, [
            [(0, [0, 1]), (1, [2, 3])],
            [(10, [10, 11]), (11, [12, 13])],
            [(20, [20, 21]), (21, [22, 23])],
        ])
        shutil.rmtree(self.workDir); os.mkdir(self.workDir)

    def testFreezeThawScriptsAlreadyRun(self):
        db = database.Database(':memory:', ':memory:')
        uJob = database.UpdateJob(db)
        uJob.setTransactionCounter(100)

        now = 1234567890.0
        v1 = VersionFromString('/a@b:c/1.0-1', timeStamps = [ now ])
        v2 = VersionFromString('/a@b:c/1.0-2', timeStamps = [ now + 1 ])
        flv1 = parseFlavor("")
        flv2 = parseFlavor("is: x86")
        js1 = ("trove1", (None, None), (v1, flv1), True)
        js2 = ("trove2", (v1, flv2), (v2, flv2), True)

        runScripts = set([ ('preupdate', js1), ('preerase', js2) ])
        uJob._jobPreScriptsAlreadyRun = set(runScripts)
        uJob.freeze(self.workDir)

        uJob = database.UpdateJob(db)
        uJob.thaw(self.workDir)
        self.assertEqual(uJob.getTransactionCounter(), 100)
        self.assertEqual(uJob._jobPreScriptsAlreadyRun,
            set(runScripts))

    def testFreezeThawCompatClassNoneScripts(self):
        db = database.Database(':memory:', ':memory:')
        uJob = database.UpdateJob(db)
        uJob.setTransactionCounter(100)

        now = 1234567890.0
        v1 = VersionFromString('/a@b:c/1.0-1', timeStamps = [ now ])
        flv1 = parseFlavor("")
        js1 = ("trove1", (None, None), (v1, flv1), True)

        scripts = [
            (js1, '', None, None, 'preupdate'),
        ]

        uJob._jobPreScripts = scripts
        uJob.freeze(self.workDir)

        uJob = database.UpdateJob(db)
        uJob.thaw(self.workDir)
        self.assertEqual(uJob.getTransactionCounter(), 100)
        self.assertEqual(uJob._jobPreScripts, scripts)

    def testFreezeThawTroveMap(self):
        db = database.Database(':memory:', ':memory:')
        uJob = database.UpdateJob(db)
        uJob.setTransactionCounter(100)

        now = 1234567890.0
        v1 = VersionFromString('/a@b:c/1.0-1', timeStamps = [ now ])
        v2 = VersionFromString('/a@b:c/1.0-2', timeStamps = [ now + 1 ])
        flv1 = parseFlavor("")
        flv2 = parseFlavor("is: x86")
        trv1 = trove.Trove("trove1", v1, flv1)
        trv2 = trove.Trove("trove2", v2, flv2)
        nvf1 = trv1.getNameVersionFlavor()
        nvf2 = trv2.getNameVersionFlavor()

        uJob._troveMap[nvf1] = trv1
        uJob._troveMap[nvf2] = trv2
        expKeys = set([nvf1, nvf2])

        uJob.freeze(self.workDir)

        uJob = database.UpdateJob(db)
        uJob.thaw(self.workDir)
        self.assertEqual(uJob.getTransactionCounter(), 100)
        self.assertEqual(set(uJob._troveMap.keys()), expKeys)
        self.assertEqual(trv1.diff(None)[0].freeze(),
            uJob._troveMap[nvf1].diff(None)[0].freeze())
        self.assertEqual(trv2.diff(None)[0].freeze(),
            uJob._troveMap[nvf2].diff(None)[0].freeze())

    def testUpdateJobiterJobPreScriptsForJobSet(self):
        db = database.Database(':memory:', ':memory:')
        uJob = database.UpdateJob(db)

        js1 = ("trove1", (None, None), ('/a@b:c/1.0-1', ""), True)
        js2 = ("trove2", ('/a@b:c/1.0-1', None), ('/a@b:c/1.0-2', ""), True)
        js3 = ("trove3", (None, None), ('/a@b:c/1.0-1', ""), True)
        js4 = ("trove4", ('/a@b:c/1.0-1', None), ('/a@b:c/1.0-2', ""), True)
        js5 = ("trove5", ('/a@b:c/1.0-1', None), (None, None), True)
        js6 = ("trove6", ('/a@b:c/1.0-1', None), (None, None), True)

        uJob.jobs = [[js1, js2, js5], [js3, js4, js6]]

        sc1 = (js1, "preinst1", 0, 1, "preinstall")
        sc2 = (js3, "preinst3", 0, 1, "preinstall")
        sc3 = (js2, "preup2", 0, 1, "preupdate")
        sc4 = (js4, "preup4", 0, 1, "preupdate")
        sc5 = (js5, "preer5", 0, 1, "preerase")
        sc6 = (js6, "preer6", 0, 1, "preerase")
        sc7 = (js2, "prerb2", 0, 1, "prerollback")
        sc8 = (js4, "prerb4", 0, 1, "prerollback")
        uJob._jobPreScripts = [ sc1, sc2, sc3, sc4, sc5, sc6, sc7, sc8 ]

        uJob._jobPreScriptsByJob = [
            [(0, [6]), (1, [7])],
            [(0, [0]), (1, [1])],
            [(0, [2]), (1, [3])],
            [(0, [4]), (1, [5])],
        ]
        ret = list(uJob.iterJobPreScriptsForJobSet(0))
        self.assertEqual(ret, [ sc7, sc1, sc3, sc5 ])

        ret = list(uJob.iterJobPreScriptsForJobSet(1))
        self.assertEqual(ret, [ sc8, sc2, sc4, sc6 ])

        # Trigger an AssertionError
        uJob._jobPreScriptsByJob = [
            [],
            [(0, [2]), (1, [3])],
            [],
            []
        ]
        self.assertRaises(AssertionError,
            list, uJob.iterJobPreScriptsForJobSet(0))

        # No data
        uJob._jobPreScripts = []
        uJob._jobPreScriptsByJob = None
        ret = list(uJob.iterJobPreScriptsForJobSet(0))
        self.assertEqual(ret, [])

class UpdateJobTests(testhelp.TestCase):
    def testClose(self):
        dbobj = database.Database(':memory:', ':memory:')
        dbobj2 = database.Database(':memory:', ':memory:')
        # Make _db be non-None
        dbobj.db
        dbobj2.db

        uJob = database.UpdateJob(dbobj)
        uJob.close()
        self.assertEqual(dbobj._db, None)

        # We close correctly even if the search source object doesn't have a
        # db property
        uJob = database.UpdateJob(dbobj)
        uJob.troveSource.db = dbobj
        class MockSearchSource(object):
            pass
        uJob.setSearchSource(MockSearchSource())
        uJob.close()
        self.assertEqual(dbobj._db, None)

        # or the property is None
        uJob = database.UpdateJob(dbobj)
        class MockSearchSource(object):
            db = None
        uJob.setSearchSource(MockSearchSource())
        uJob.close()
        self.assertEqual(dbobj._db, None)

        # ... or real
        uJob = database.UpdateJob(dbobj)
        uJob = database.UpdateJob(dbobj)
        class MockSearchSource(object):
            db = dbobj2
        uJob.setSearchSource(MockSearchSource())
        uJob.close()
        self.assertEqual(dbobj._db, None)
        self.assertEqual(dbobj2._db, None)
