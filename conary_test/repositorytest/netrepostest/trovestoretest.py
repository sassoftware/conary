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

import decimal
import os
import shutil
import sys
import time
import tempfile

from conary_test import rephelp

from conary_test import dbstoretest

from conary import changelog
from conary import files
from conary import trove

from conary.deps import deps
from conary.local import schema as depSchema
from conary.repository.netrepos import instances, trovestore, netauth
from conary.lib.sha1helper import md5FromString, sha1FromString
from conary.server import schema
from conary.versions import ThawVersion, VersionFromString


class TroveStoreTest(dbstoretest.DBStoreTestBase):

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

    def _connect(self):
        db = self.getDB()
        schema.createSchema(db)
        schema.setupTempTables(db)
        depSchema.setupTempDepTables(db)
        store = trovestore.TroveStore(db)
        auth = netauth.NetworkAuthorization(db, ['localhost'])
        auth.addUser('anonymous', 'anonymous')
        auth.addRole('anonymous')
        auth.addRoleMember('anonymous', 'anonymous')
        auth.addAcl('anonymous', None, None, write = False, remove = False)
        auth.setAdmin('anonymous', False)
        return store

    def testTroves(self, flavor=None):
        if flavor is None:
            flavor = deps.Flavor()

        store = self._connect()

        dirSet = set(['/etc', '/bin'])
        baseSet = set(['passwd', 'services', 'group', '1', '2', '3',
                       'distributed'])

        v10 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10")

        branch = v10.branch()
        store.createTroveBranch("testtrove", branch)

        f1 = files.FileFromFilesystem("/etc/passwd", self.id1)
        f2 = files.FileFromFilesystem("/etc/services", self.id2)
        f3 = files.FileFromFilesystem("/etc/group", self.id3)
        # make a really huge dependency, thus a very large file stream
        req = deps.DependencySet()
        for x in xrange(10000):
            req.addDep(deps.SonameDependencies, deps.Dependency("libtest.so.%d" %x))
        f3.requires.set(req)
        # make sure it's way too big for a blob in mysql
        assert(len(f3.freeze()) >= 50000)

        cl = changelog.ChangeLog("test", "test@foo.bar", """\
Some changes are good.
Some changes are bad.
Some changes just are.
""")

        trv = trove.Trove('testcomp', v10, flavor, cl)
        trv.addFile(f1.pathId(), "/bin/1", v10, f1.fileId())
        trv.addFile(f2.pathId(), "/bin/2", v10, f2.fileId())
        trv.addFile(f3.pathId(), "/bin/3", v10, f3.fileId())
        trv.addFile(self.id4, "/bin/distributed", v10, self.fid4)
        trv.troveInfo.size.set(1234)
        trv.troveInfo.sourceName.set('somesource')

        req = deps.DependencySet()
        req.addDep(deps.FileDependencies, deps.Dependency("/bin/bash"))
        req.addDep(deps.TroveDependencies, deps.Dependency("foo:runtime"))
        req.addDep(deps.SonameDependencies, deps.Dependency("libtest.so.1"))
        trv.setRequires(req)

        # this also lets us peek at the database to make sure libtest.so.1
        # is only in the dep table once
        prv = deps.DependencySet()
        prv.addDep(deps.SonameDependencies, deps.Dependency("libtest.so.1"))
        trv.setProvides(prv)
        trv.computeDigests()

        store.db.transaction()
        store.addTroveSetStart([], dirSet, baseSet)
        troveInfo = store.addTrove(trv, trv.diff(None)[0])
        troveInfo.addFile(f1.pathId(), "/bin/1", f1.fileId(), v10,
                          fileStream = f1.freeze())
        troveInfo.addFile(f2.pathId(), "/bin/2", f2.fileId(), v10,
                          fileStream = f2.freeze())
        troveInfo.addFile(f3.pathId(), "/bin/3", f3.fileId(), v10,
                          fileStream = f3.freeze())
        troveInfo.addFile(self.id4, "/bin/distributed", self.fid4, v10)
        store.addTroveDone(troveInfo)
        store.addTroveSetDone()
        store.db.commit()

        cu = store.db.cursor()
        cu.execute("SELECT count(*) FROM Dependencies WHERE "
                   "name = 'libtest.so.1'")
        self.assertEqual(cu.next(), (1,))

        # make sure the sha1s were stored
        cu.execute("""
        SELECT dirname, basename, sha1
        FROM TroveFiles
        JOIN FileStreams USING (streamId)
        JOIN FilePaths ON TroveFiles.filePathId = FilePaths.filePathId
        JOIN Dirnames ON FilePaths.dirnameId = Dirnames.dirnameId
        JOIN Basenames ON FilePaths.basenameId = Basenames.basenameId
        ORDER BY dirname,basename""")
        items = [(os.path.join(cu.frombinary(x[0]), cu.frombinary(x[1])),
            cu.frombinary(x[2])) for x in cu.fetchall()]
        self.assertEqual(items,
                             [ ("/bin/1", f1.contents.sha1()),
                               ("/bin/2", f2.contents.sha1()),
                               ("/bin/3", f3.contents.sha1()),
                               ("/bin/distributed", None) ])

        cl = changelog.ChangeLog("test", "test@foo.bar", "another log\n")

        fromRepos = store.getTrove("testcomp", v10, flavor, cl)
        self.assertEqual(fromRepos, trv)
        self.assertEqual(fromRepos.getVersion().timeStamps(),
                             trv.getVersion().timeStamps())
        self.assertEqual(fromRepos.getChangeLog(), trv.getChangeLog())

        self.assertEqual(
            [ x for x in store.getTrove("testcomp", v10, flavor,
                                        withFiles = False).iterFileList() ],
            [] )

        l = store.iterFilesInTrove("testcomp", v10, flavor, sortByPath = True)
        l = [ x for x in l ]
        self.assertEqual(l,
                             [ (f1.pathId(), "/bin/1", f1.fileId(), v10),
                               (f2.pathId(), "/bin/2", f2.fileId(), v10),
                               (f3.pathId(), "/bin/3", f3.fileId(), v10),
                               (self.id4, "/bin/distributed", self.fid4, v10)])

        cl = changelog.ChangeLog("test", "test@foo.bar", "log for testpkg\n")
        trv2 = trove.Trove("testpkg", v10, flavor, cl)
        trv2.addTrove(trv.getName(), v10, flavor)
        trv2.addTrove("weakref", v10, flavor, weakRef = True)
        trv2.computeDigests()
        store.addTroveSetStart([], dirSet, baseSet)
        troveInfo = store.addTrove(trv2, trv2.diff(None)[0])
        store.addTroveDone(troveInfo)
        store.addTroveSetDone()
        self.assertEqual(store.getTrove("testpkg", v10, flavor), trv2)
      
        self.assertEqual(
            [ x for x in store.iterTroves([ ("testcomp", v10, flavor),
                                            ("testpkg", v10, flavor ) ]) ],
            [trv, trv2] )
        self.assertEqual(
            [ x for x in store.iterTroves([ ("testpkg", v10, flavor ),
                                            ("testcomp", v10, flavor) ]) ],
            [trv2, trv] )
        self.assertEqual(
            [ x for x in store.iterTroves([ ("testpkg", v10, flavor),
                                            ("testpkg", v10, flavor) ]) ],
            [trv2, trv2] )
        self.assertEqual(
            [ x for x in store.iterTroves([ ("testpkg", v10, flavor ),
                                            ("blah", v10, flavor) ]) ],
            [trv2, None] )
        self.assertEqual(
            [ x for x in store.iterTroves([ ("blah", v10, flavor ),
                                            ("testpkg", v10, flavor) ]) ],
            [None, trv2] )
        self.assertEqual(
            [ x for x in store.iterTroves([ ("blah", v10, flavor ) ]) ],
            [None] )
        self.assertEqual(
            [ x for x in store.iterTroves([ ("testcomp", v10, flavor),
                                            ("blah", v10, flavor ),
                                            ("testpkg", v10, flavor ) ]) ],
            [trv, None, trv2] )

        # erasing doesn't work
        #store.eraseTrove("testcomp", v10, None)
        #store.commit()
        self.assertEqual(store.getTrove("testpkg", v10, flavor), trv2)
        
        map = { 'testpkg': [ v10 ]}
        flavors = store.getTroveFlavors(map)
        if flavor is not None:
            flavorStr = flavor.freeze()
        else:
            flavorStr = ''
        self.assertEqual(flavors, { 'testpkg': {v10: [flavorStr]}})

        map = { 'testpkg3': [ v10 ]}
        flavors = store.getTroveFlavors(map)
        self.assertEqual(flavors, { 'testpkg3': {v10: []}})

        # test getFiles
        fileObjs = store.getFiles([(f1.pathId(), f1.fileId()),
                                   (f2.pathId(), f2.fileId())])
        self.assertEqual(fileObjs[(f1.pathId(), f1.fileId())], f1)
        self.assertEqual(fileObjs[(f2.pathId(), f2.fileId())], f2)

        # test that asking for an invalid fileid/pathid pair results
        # in no entry for the (pathid, fileid) in the returned dict
        invalidPathId = md5FromString('9' * 32)
        invalidFileId = sha1FromString('9' * 40)
        fileObjs = store.getFiles([(invalidPathId, invalidFileId)])
        # make sure fileObjs is empty
        assert(not fileObjs)

        # test that asking for contents that have to come from
        # a different repository works - we should get None
        # back
        fileObjs = store.getFiles([(self.id4, self.fid4)])
        self.assertEqual(fileObjs, {(self.id4, self.fid4): None})

    def testMetadata(self):
        store = self._connect()
        emptyFlavor = deps.Flavor()
        v1 = ThawVersion("/conary.rpath.com@test:trunk/1:1-1")
        branch = v1.branch()
        store.createTroveBranch("testtrove", branch)

        md_v1_l = {"shortDesc":    "Short Desc",
                   "longDesc":     "Long Desc",
                   "url":          ["url1", "url2"],
                   "license":      ["CPL", "GPL"],
                   "category":     ["cat1", "cat2"],
                   "version":      "/conary.rpath.com@test:trunk/1-1",
                   "source":       "local",
                   "language":     "C",
        }
        md_v1_fr_l = {"shortDesc": "French Short Desc",
                      "longDesc":  "French Long Desc",
                      "url":       ["url1", "url2"],
                      "license":   ["CPL", "GPL"],
                      "category":  ["cat1", "cat2"],
                      "version":   "/conary.rpath.com@test:trunk/1-1",
                      "source":    "local",
                      "language":  "fr",
        }
        md_v2_l = {"shortDesc":    "Short Desc V2",
                   "longDesc":     "Long Desc V2",
                   "url":          ["url1v2"],
                   "license":      ["CPLv2", "GPLv2"],
                   "category":     ['cat1v2', 'cat2v2', 'cat3v2'],
                   "version":      "/conary.rpath.com@test:trunk/1-2",
                   "source":       "foo",
                   "language":     "C",
        }

        trv3 = trove.Trove("testpkg3", v1, emptyFlavor, None)
        branch = v1.branch()

        store.db.transaction()

        store.updateMetadata("testpkg3", branch, "Short Desc",
            "Long Desc", urls=['url1', 'url2'],
            categories=['cat1', 'cat2'], licenses=['GPL', 'CPL'],
            source="", language="C")

        store.updateMetadata("testpkg3", branch, "French Short Desc",
            "French Long Desc", [], [], [], "", "fr")

        store.db.commit()

        md_v1 = store.getMetadata("testpkg3", branch)
        md_v1_fr = store.getMetadata("testpkg3", branch, language="fr")

        self.assertEqual(md_v1.freeze(), md_v1_l)
        self.assertEqual(md_v1_fr.freeze(), md_v1_fr_l)


        v2 = ThawVersion("/conary.rpath.com@test:trunk/1:1-2")

        store.db.transaction()

        store.updateMetadata("testpkg3", branch, "Short Desc V2",
            "Long Desc V2", urls=['url1v2'],
            categories=['cat1v2', 'cat2v2', 'cat3v2'],
            licenses=['CPLv2', 'GPLv2'], source="foo", language="C")

        store.db.commit()

        md_v2 = store.getMetadata("testpkg3", branch)
        md_v1 = store.getMetadata("testpkg3", branch, version=v1)
        md_v1_fr = store.getMetadata("testpkg3", branch, version=v1, language="fr")

        self.assertEqual(md_v2.freeze(), md_v2_l)
        self.assertEqual(md_v1.freeze(), md_v1_l)
        self.assertEqual(md_v1_fr.freeze(), md_v1_fr_l)

    def testTroveFlavor(self):
        flavor = deps.Flavor()
        flavor.addDep(deps.UseDependency,
                deps.Dependency('use', [('foo', deps.FLAG_SENSE_REQUIRED)]))
        self.testTroves(flavor=flavor)

    def testTroveMultiFlavor(self):
        # create a package with 3 components, each use a different use
        # flag.  add the package before the components.  the goal is
        # to make the trovestore create more than one flavor
        flags = ('foo', 'bar', 'baz')
        flavors = []
        for flag in flags:
            flavor = deps.Flavor()
            flavor.addDep(deps.UseDependency, deps.Dependency('use',
                                [(flag, deps.FLAG_SENSE_REQUIRED)]))
            flavor.addDep(deps.InstructionSetDependency,
                          deps.Dependency('x86', []))
            flavors.append(flavor)

        v10 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10")
        store = self._connect()

        # create components to add to the package
        troves = []
        for flag, flavor in zip(flags, flavors):
            trv = trove.Trove('test:%s' %flag, v10, flavor, None)
            trv.computeDigests()
            troves.append(trv)

        # add the package
        union = deps.Flavor()
        for flavor in flavors:
            union.union(flavor)
        trv2 = trove.Trove("test", v10, union, None)
        for trv in troves:
            trv2.addTrove(trv.getName(), v10, trv2.getFlavor())
        trv2.computeDigests()
        store.addTroveSetStart([], [], [])
        troveInfo = store.addTrove(trv2, trv2.diff(None)[0])
        store.addTroveDone(troveInfo)
        store.addTroveSetDone()

        # add the troves
        store.addTroveSetStart([], [], [])
        for trv in troves:
            troveInfo = store.addTrove(trv, trv.diff(None)[0])
            store.addTroveDone(troveInfo)
        store.addTroveSetDone()

        for trv in troves:
            self.assertEqual(trv, 
                store.getTrove(trv.getName(), trv.getVersion(), trv.getFlavor()))

        troveFlavors = store.getTroveFlavors({ 'test': [ v10 ] })
        self.assertEqual(troveFlavors['test'][v10], [union.freeze()])

    def testRemoved(self):
        store = self._connect()

        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        x86 = deps.parseFlavor("is:x86")

        removed = trove.Trove("trvname", old, x86, None,
                              type = trove.TROVE_TYPE_REMOVED)
        removed.computeDigests()

        store.addTroveSetStart([], [], [])
        troveInfo = store.addTrove(removed, removed.diff(None)[0])
        store.addTroveDone(troveInfo)
        store.addTroveSetDone()

        assert(store.getTrove("trvname", old, x86) == removed)

    def testRedirect(self):
        store = self._connect()

        old = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-3")
        x86 = deps.parseFlavor("is:x86")
        x86_64 = deps.parseFlavor("is:x86_64")

        redir = trove.Trove("trvname", old, x86, None,
                            type = trove.TROVE_TYPE_REDIRECT)
        redir.addRedirect("trv1", old.branch(), x86)
        redir.addRedirect("trv2", old.branch(), x86_64)
        redir.computeDigests()

        store.addTroveSetStart([], [], [])
        troveInfo = store.addTrove(redir, redir.diff(None)[0])
        store.addTroveDone(troveInfo)
        store.addTroveSetDone()

        assert(store.getTrove("trvname", old, x86) == redir)

    def testBrokenPackage(self):
        store = self._connect()

        v10 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10")
        v20 = ThawVersion("/conary.rpath.com@test:trunk/20:1.2-20")
        flavor = deps.Flavor()
        flavor2 = deps.parseFlavor('is:x86')

        trv = trove.Trove("testpkg", v10, flavor, None)
        trv.addTrove("testpkg:runtime", v20, flavor)
        trv.computeDigests()

        store.addTroveSetStart([], [], [])
        troveInfo = store.addTrove(trv, trv.diff(None)[0])
        self.assertRaises(AssertionError, store.addTroveDone, troveInfo)

        store.rollback()

        trv = trove.Trove("testpkg", v10, flavor, None)
        trv.addTrove("testpkg:runtime", v10, flavor2)
        trv.computeDigests()
        troveInfo = store.addTrove(trv, trv.diff(None)[0])
        self.assertRaises(AssertionError, store.addTroveDone, troveInfo)

    @testhelp.context('performance')
    def testMassiveIterTroves(self):
        store = self._connect()

        infoList = []
        expected = []
        f = deps.parseFlavor('is:x86')
        v = ThawVersion('/conary.rpath.com@test:trunk/10:1-1')
        # add 2000 test components
        store.addTroveSetStart([], [], [])
        for x in xrange(500):
            n = 'test%d:runtime' %x
            t = trove.Trove(n, v, f, None)
            t.computeDigests()
            troveInfo = store.addTrove(t, t.diff(None)[0])
            store.addTroveDone(troveInfo)
            # we want to iterTroves for each of our components (which
            # ends up being a no-op)
            infoList.append((n, v, f))
            expected.append(t)
        store.addTroveSetDone()
        start = time.time()
        result = [ x for x in store.iterTroves(infoList) ]
        end = time.time()

        # make sure we got the expected results
        assert(result == expected)
        # we should be able to iter through all of these troves in
        # well under one seconds
        if end - start > 5:
            sys.stderr.write("\nWarning: testMassiveIterTroves: test ran in "
                             "%.3f seconds, expected < 5\n\n" % (end - start))

    def testDuplicateStreams(self):
        store = self._connect()
        flavor = deps.Flavor()

        v10 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10")
        v20 = ThawVersion("/conary.rpath.com@test:trunk/20:1.2-20")

        dirNames = set(['/bin', '/etc'])
        baseNames = set(['1', '2'])

        f1 = files.FileFromFilesystem("/etc/passwd", self.id1)

        trv = trove.Trove("testpkg:runtime", v10, flavor, None)
        trv.addFile(f1.pathId(), "/bin/1", v10, f1.fileId())
        trv.computeDigests()

        store.db.transaction()
        store.addTroveSetStart([], dirNames, baseNames)
        troveInfo = store.addTrove(trv, trv.diff(None)[0])
        troveInfo.addFile(f1.pathId(), "/bin/1", f1.fileId(), v10)
        store.addTroveDone(troveInfo)
        store.addTroveSetDone()
        store.db.commit()

        trv = trove.Trove("testpkg:runtime", v20, flavor, None)
        trv.addFile(f1.pathId(), "/bin/1", v20, f1.fileId())
        trv.addFile(f1.pathId(), "/bin/2", v20, f1.fileId())
        trv.computeDigests()

        store.db.transaction()
        store.addTroveSetStart([], dirNames, baseNames)
        troveInfo = store.addTrove(trv, trv.diff(None)[0])
        troveInfo.addFile(f1.pathId(), "/bin/1", f1.fileId(), v10)
        troveInfo.addFile(f1.pathId(), "/bin/2", f1.fileId(), v10)
        store.addTroveDone(troveInfo)
        store.db.commit()


    def testRemoval(self):
        threshold = 60 * 5;         # 5 minutes

        def _dbStatus(db):
            stat = {}
            cu = db.cursor()

            for table in db.tables:
                cu.execute("SELECT * FROM %s" % table)
                l = cu.fetchall()
                # throw away anything which looks a timestamp; this will
                # break if the test case takes more than 5 minutes to run
                stat[table] = set()
                for row in l:
                    thisRow = []
                    for item in row:
                        if not isinstance(item, (float, decimal.Decimal)) or \
                                    abs(item - now) > threshold:
                            thisRow.append(item)

                    stat[table].add(tuple(thisRow))

            return stat

        def _checkStateDiff(one, two):
            if one != two:
                assert(one.keys() == two.keys())
                for key in one:
                    if one[key] != two[key]:
                        print "table %s has changed" % key
                raise AssertionError, "\n%s\n!=\n%s" % (one, two)

        store = self._connect()

        # get the current timestamp from the database
        cu = store.db.cursor()
        cu.execute('''create table timestamp(
                      foo     INTEGER,
                      changed NUMERIC(14,0) NOT NULL DEFAULT 0)''')
        store.db.loadSchema()
        store.db.createTrigger('timestamp', 'changed', "INSERT")
        cu.execute('insert into timestamp values(0, 0)')
        cu.execute('select changed from timestamp')
        now = cu.fetchall()[0][0]

        emptyState = _dbStatus(store.db)

        v10 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10")
        v20 = ThawVersion("/conary.rpath.com@test:trunk/20:1.2-20")
        vB = ThawVersion("/conary.rpath.com@test:branch/1:0-0")
        vOtherRepo = ThawVersion("/other.repos.com@some:label/1:0-0")
        branch = v10.branch()

        flavor = deps.parseFlavor('is:x86')
        flavor64 = deps.parseFlavor('is:x86_64')

        store.createTroveBranch("trv:comp", branch)

        f1 = files.FileFromFilesystem("/etc/passwd", self.id1)
        f2 = files.FileFromFilesystem("/etc/services", self.id2)
        # add a file that has no contents sha1
        try:
            d = tempfile.mkdtemp()
            os.symlink('foo', d + '/foo')
            f5 = files.FileFromFilesystem(d + '/foo', self.id5)
        finally:
            shutil.rmtree(d)

        req = deps.parseDep("file: /bin/bash file: /bin/awk")

        dirNames = set(['/bin', ''])
        baseNames = set(['1', '2', 'distributed', 'foo', 'group-foo.recipe'])

        cl = changelog.ChangeLog("test", "test@foo.bar", "changelog\n")

        trv = trove.Trove('trv:comp', v10, flavor, cl)
        trv.addFile(f1.pathId(), "/bin/1", v10, f1.fileId())
        trv.addFile(f2.pathId(), "/bin/2", v10, f2.fileId())
        trv.addFile(self.id4, "/bin/distributed", v10, self.fid4)
        trv.addFile(f5.pathId(), "/bin/foo", v10, f5.fileId())
        trv.troveInfo.size.set(1234)
        trv.setRequires(req)

        store.db.transaction()
        store.addTroveSetStart([], dirNames, baseNames)
        troveInfo = store.addTrove(trv, trv.diff(None)[0])
        troveInfo.addFile(f1.pathId(), "/bin/1", f1.fileId(), v10,
                          fileStream = f1.freeze())
        troveInfo.addFile(f2.pathId(), "/bin/2", f2.fileId(), v10,
                          fileStream = f2.freeze())
        troveInfo.addFile(f5.pathId(), "/bin/foo", f5.fileId(), v10,
                          fileStream = f5.freeze())
        store.addTroveDone(troveInfo)
        store.addTroveSetDone()
        store.db.commit()
        oneTroveState = _dbStatus(store.db)

        rc = store._removeTrove("trv:comp", v10, flavor)
        self.assertEqual(set(rc), set([ f1.contents.sha1(), f2.contents.sha1() ]))
        state = _dbStatus(store.db)
        _checkStateDiff(state, emptyState)
        store.db.rollback()

        # the redir itself doesn't overlap with trv:comp; this makes sure
        # that the tables which the redir target needs are preserved
        redir = trove.Trove('redir:comp', vB, flavor64, cl,
                            type = trove.TROVE_TYPE_REDIRECT)
        redir.addRedirect('trv:comp', v10.branch(), flavor)
        store.addTroveSetStart([], dirNames, baseNames)
        troveInfo = store.addTrove(redir, redir.diff(None)[0])
        store.addTroveDone(troveInfo)
        store.addTroveSetDone()
        rc = store._removeTrove("trv:comp", v10, flavor)
        redir2 = store.getTrove('redir:comp', vB, flavor64)
        assert(redir == redir2)
        rc = store._removeTrove("redir:comp", vB, flavor64)
        state = _dbStatus(store.db)
        _checkStateDiff(state, emptyState)

        store.db.rollback()

        trv2 = trv.copy()
        trv2.changeVersion(v20)

        store.db.transaction()
        store.addTroveSetStart([], dirNames, baseNames)
        troveInfo = store.addTrove(trv2, trv2.diff(None)[0])
        troveInfo.addFile(f1.pathId(), "/bin/1", f1.fileId(), v10,
                          fileStream = f1.freeze())
        troveInfo.addFile(f2.pathId(), "/bin/2", f2.fileId(), v10,
                          fileStream = f2.freeze())
        store.addTroveDone(troveInfo)
        store.addTroveSetDone()
        store.db.commit()
        twoTroveState = _dbStatus(store.db)

        rc = store._removeTrove("trv:comp", v20, flavor)
        assert(not rc)
        state = _dbStatus(store.db)
        _checkStateDiff(state, oneTroveState)
        rc = store._removeTrove("trv:comp", v10, flavor)
        assert(set(rc) == set([ f1.contents.sha1(), f2.contents.sha1() ]))
        state = _dbStatus(store.db)
        _checkStateDiff(state, emptyState)

        store.db.rollback()

        # add a trove which shares a file with trv:comp and make sure removing
        # it doesn't remove the sha1s (make sure the fileIds are different)
        anotherTrove = trove.Trove('another:comp', v10, flavor, cl)
        anotherF = f1.copy()
        anotherF.inode.owner.set('unowned')
        anotherTrove.addFile(f1.pathId(), "/bin/1", v10, anotherF.fileId())
        store.addTroveSetStart([], dirNames, baseNames)
        troveInfo = store.addTrove(anotherTrove, anotherTrove.diff(None)[0])
        troveInfo.addFile(f1.pathId(), "/bin/1", anotherF.fileId(),
                          v10, fileStream = f1.freeze())
        store.addTroveDone(troveInfo)
        rc = store._removeTrove("another:comp", v10, flavor)
        assert(not rc)
        state = _dbStatus(store.db)
        _checkStateDiff(state, twoTroveState)

        store.db.rollback()

        # now try just marking something as removed
        rc = store.markTroveRemoved("trv:comp", v20, flavor)
        assert(not rc)
        removedTrove = store.getTrove("trv:comp", v20, flavor)
        assert(removedTrove.type() == trove.TROVE_TYPE_REMOVED)

        rc = store.markTroveRemoved("trv:comp", v10, flavor)
        assert(set(rc) == set([ f1.contents.sha1(), f2.contents.sha1() ]))
        removedTrove = store.getTrove("trv:comp", v10, flavor)
        assert(removedTrove.type() == trove.TROVE_TYPE_REMOVED)

        store.db.rollback()

        # test removing group-*:source
        anotherTrove = trove.Trove('group-foo:source', v10, flavor, cl)
        anotherF = f1.copy()
        anotherF.inode.owner.set('unowned')
        anotherTrove.addFile(f1.pathId(), "group-foo.recipe", v10,
                             anotherF.fileId())
        troveInfo = store.addTrove(anotherTrove, anotherTrove.diff(None)[0])
        troveInfo.addFile(f1.pathId(), "group-foo.recipe", anotherF.fileId(),
                          v10, fileStream = anotherF.freeze())
        store.addTroveDone(troveInfo)
        rc = store._removeTrove("group-foo:source", v10, flavor)
        assert(not rc)
        state = _dbStatus(store.db)
        _checkStateDiff(state, twoTroveState)

        store.db.rollback()

        groupTrove = trove.Trove('group-foo', v10, flavor, cl)
        groupTrove.addTrove('foo', vOtherRepo, flavor)
        troveInfo = store.addTrove(groupTrove, groupTrove.diff(None)[0])
        store.addTroveDone(troveInfo)
        rc = store._removeTrove("group-foo", v10, flavor)
        state = _dbStatus(store.db)
        _checkStateDiff(state, twoTroveState)
        store.db.rollback()


    def testCommonFiles(self):
        # this test simulates a trove having the ame file in different
        # path locations with only changed mtimes.
        store = self._connect()
        flavor = deps.Flavor()
        version = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10")

        baseNames = set(['file'])
        dirNames = set(['/junk1', '/junk2'])

        f = files.FileFromFilesystem("/etc/passwd", self.id)
        trv = trove.Trove("junk:data", version, flavor, None)
        trv.addFile(self.id1, "/junk1/file", version, f.fileId())
        trv.addFile(self.id2, "/junk2/file", version, f.fileId())
        trv.computeDigests()

        store.db.transaction()
        store.addTroveSetStart([], dirNames, baseNames)
        ti = store.addTrove(trv, trv.diff(None)[0])
        f.inode.mtime.set(1)
        ti.addFile(self.id1, "/junk1/file", f.fileId(), version,
                   fileStream = f.freeze())
        f.inode.mtime.set(2)
        ti.addFile(self.id2, "/junk2/file", f.fileId(), version,
                   fileStream = f.freeze())
        store.addTroveDone(ti)
        store.commit()

    def testHidden(self):
        store = self._connect()
        cu = store.db.cursor()

        flavor = deps.Flavor()
        version = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10")

        f = files.FileFromFilesystem("/etc/passwd", self.id)
        trv = trove.Trove("junk:data", version, flavor, None)
        trv.computeDigests()

        store.addTroveSetStart([], set(['/etc']), set(['passwd']))
        ti = store.addTrove(trv, trv.diff(None)[0], hidden = True)
        store.addTroveDone(ti)
        store.addTroveSetDone()

        assert(cu.execute("select count(*) from latestcache").fetchall()[0][0] == 0)
        assert(cu.execute("select isPresent from instances").fetchall()[0][0]
                                        == instances.INSTANCE_PRESENT_HIDDEN)

        store.presentHiddenTroves()
        assert(cu.execute("select count(*) from latestcache").fetchall()[0][0] == 3)
        assert(cu.execute("select isPresent from instances").fetchall()[0][0]
                                        == instances.INSTANCE_PRESENT_NORMAL)

    def testDistributedRedirect(self):
        store = self._connect()
        cu = store.db.cursor()

        flavor = deps.Flavor()
        localVer1 = ThawVersion("/localhost@test:trunk/10:1.1-10")
        localVer2 = ThawVersion("/localhost@test:trunk/20:1.2-20")
        remoteVer = ThawVersion("/localhost1@test:trunk/10:1.2-10")

        # this places /localhost1@test:trunk into the branch table, but not
        # the labels table
        trv = trove.Trove("group-foo", localVer1, flavor, None,
                          type = trove.TROVE_TYPE_REDIRECT)
        trv.addRedirect("target", remoteVer.branch(), flavor)
        trv.computeDigests()
        store.addTroveSetStart([], [], [])
        ti = store.addTrove(trv, trv.diff(None)[0], hidden = True)
        store.addTroveDone(ti)

        # and this needs the label to exist
        trv = trove.Trove("group-foo", localVer2, flavor, None)
        trv.addTrove("target", remoteVer, flavor)
        trv.computeDigests()
        ti = store.addTrove(trv, trv.diff(None)[0], hidden = True)
        store.addTroveDone(ti)

    def testDuplicatePaths(self):
        store = self._connect()

        v10 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10")
        flavor1 = deps.Flavor()
        flavor2 = deps.parseFlavor('is:x86')
        cl = changelog.ChangeLog("test", "test@foo.bar", "Changes\n")
        f1 = files.FileFromFilesystem("/etc/passwd", self.id1)

        trv1 = trove.Trove('testcomp', v10, flavor1, cl)
        trv1.addFile(f1.pathId(), "/bin/1", v10, f1.fileId())

        trv2 = trove.Trove('testcomp', v10, flavor2, cl)
        trv2.addFile(f1.pathId(), "/bin/1", v10, f1.fileId())

        store.db.transaction()
        store.addTroveSetStart([], set(['/bin' ]), set(['1']))

        troveInfo = store.addTrove(trv1, trv1.diff(None)[0])
        troveInfo.addFile(f1.pathId(), "/bin/1", f1.fileId(), v10,
                          fileStream = f1.freeze())
        store.addTroveDone(troveInfo)

        troveInfo = store.addTrove(trv2, trv2.diff(None)[0])
        troveInfo.addFile(f1.pathId(), "/bin/1", f1.fileId(), v10,
                          fileStream = f1.freeze())
        store.addTroveDone(troveInfo)

        store.addTroveSetDone()
        store.db.commit()

        # make sure the path was inserted into FilePaths and friends once
        cu = store.db.cursor()
        for tbl in [ 'FilePaths', 'Dirnames', 'Basenames' ]:
            cu.execute("select count(*) from %s" % tbl)
            self.assertEquals(cu.next()[0], 1)

class TroveStoreTest2(rephelp.RepositoryHelper):
    def testNeedToStoreFile(self):
        # Tests a case when the same fileId is added to the repository twice 
        # in the same update - once with a null fileStream, meaning that the 
        # file is stored in another repository, and once with a stream 
        # associated with it that needs to be stored in the repository.
        self.addComponent('test:runtime', '1.0', '', 
                          [('file1', 'foo', 
                            '/localhost@rpl:linux/1.0-1-1')])
        self.addCollection('test', '1.0', [':runtime'])
        self.addComponent('test:source', '/localhost@rpl:linux/1.0-1', '')
        self.mkbranch(['test'], 'localhost@rpl:shadow', shadow=True, 
                      binaryOnly=True)
        self.addComponent('test:source', '/localhost@rpl:linux//shadow/2.0-0.1')
        self.addComponent('test:runtime', 
                          '/localhost@rpl:linux//shadow/2.0-0.1-1', '',
                          [('file1', 'foo', '/localhost@rpl:linux/1.0-1-1'),
                           ('file2', 'foo', 
                            '/localhost@rpl:linux//shadow/2.0-1-1')])
        self.addCollection('test', '/localhost@rpl:linux//shadow/2.0-0.1-1',
                           [':runtime'])
        self.openRepository(1)
        self.clone('/localhost@rpl:linux//localhost1@rpl:linux',
                   'test:source=:shadow')
        self.clone('/localhost@rpl:linux//localhost1@rpl:linux', 'test=:shadow')

    def testPrefixDirnames(self):
        self.addComponent("foo:source", "1.0")
        self.addComponent("foo:runtime", "1.0", '',
                          [("/usr/share/foo/bar/baz/file1", "file"),
                           ("/usr/share/foo/bar/baz/file2", "file")])
        self.addComponent("foo:lib", "1.0", '',
                          [("/usr/share/foo/file3", "file3")])
        self.addCollection("foo", "1.0", [":runtime", ":lib"], sourceName = "foo:source")
        repos = self.openRepository()
        branch = VersionFromString("/" + str(self.defLabel))
        # get everything we know
        allIds = repos.getPackageBranchPathIds("foo:source", branch)
        self.assertEqual(set(allIds.keys()), set([
            "/usr/share/foo/bar/baz/file1", "/usr/share/foo/bar/baz/file2",
            "/usr/share/foo/file3"]))
        ret = repos.getPackageBranchPathIds("foo:source", branch, ["/usr/share/foo"])
        # since protocol 62, dirnames are strictly honored, so ret should only have file3
        self.assertEqual(ret.keys(), ["/usr/share/foo/file3"])
        # protocol 61 will treat the dirlist as prefixes, which should return all files back
        repos.c[branch].setProtocolVersion(61)
        ret61 = repos.getPackageBranchPathIds("foo:source", branch, ["/usr/share/foo"])
        self.assertEqual(ret61, allIds)

    def testDuplicateFileIds(self):
        # This test sets up a relative changeset that specifies two different
        # streams for the same fileId. For this to happen we need relative
        # diff's for the streams (or they get collapsed in the changeset
        # itself because cs.files is a dict) and the commits have to happen
        # carefully (or they get collapsed on the repository). This exercises
        # CNY-3316
        repos = self.openRepository()
        foo1 = self.addComponent('foo:run=1',
            fileContents = [ ('/a', rephelp.RegularFile(contents = 'abc')) ] )
        foo2 = self.addComponent('foo:run=2',
            fileContents = [ ('/a', rephelp.RegularFile(contents = '123',
                                                        mtime = 10000)) ])
        fooCs = repos.createChangeSet([ ('foo:run',
                                         (None, None),
                                         foo1.getNameVersionFlavor()[1:],
                                         True) ])
        fooRelCs = repos.createChangeSet([ ('foo:run',
                                            foo1.getNameVersionFlavor()[1:],
                                            foo2.getNameVersionFlavor()[1:],
                                            False) ])

        self.resetRepository()
        repos = self.openRepository()

        bar1 = self.addComponent('bar:run=1',
            fileContents = [ ('/b', rephelp.RegularFile(contents = 'def')) ] )

        bar2 = self.addComponent('bar:run=2',
            fileContents = [ ('/b', rephelp.RegularFile(contents = '123',
                                                        mtime = 10001)) ])

        barCs = repos.createChangeSet([ ('bar:run',
                                         (None, None),
                                         bar1.getNameVersionFlavor()[1:],
                                         True) ])
        barRelCs = repos.createChangeSet([ ('bar:run',
                                            bar1.getNameVersionFlavor()[1:],
                                            bar2.getNameVersionFlavor()[1:],
                                            False) ])

        self.resetRepository()
        repos = self.openRepository()
        repos.commitChangeSet(fooCs)
        repos.commitChangeSet(barCs)
        fooRelCs.merge(barRelCs)
        repos.commitChangeSet(fooRelCs)
