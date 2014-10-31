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


from conary_test import dbstoretest

from conary.local import versiontable
from conary.versions import VersionFromString
from conary.versions import ThawVersion
from conary.versions import Label
from conary.repository.netrepos import versionops
from conary.repository.netrepos import trovestore
from conary.repository.netrepos import items
from conary.server import schema


class VersionsSqlTest(dbstoretest.DBStoreTestBase):
    def testVersionTable(self):
        db = self.getDB()
        schema.createIdTables(db)
        tbl = versiontable.VersionTable(db)

        v1 = VersionFromString("/conary.rpath.com@test:trunk/1.2-3")
        v2 = VersionFromString("/conary.rpath.com@test:trunk/1.4-5")

        tbl.addId(v1)
        tbl.addId(v2)
        assert(tbl[v1] == 1)
        assert(tbl[v2] == 2)
        assert(tbl.getBareId(1) == v1)
        assert(tbl.getBareId(2) == v2)
        #assert(tbl.getTimestamp(2) == v2.timeStamp)

        v2.versions[-1].timeStamp += 5
        assert(tbl[v2] == 2)

        assert(tbl.get(v1, "foo") == 1)
        del tbl[v1]
        assert(tbl.get(v1, "foo") == "foo")
        tbl.delId(2)
        self.assertRaises(KeyError, tbl.__getitem__, v2)
        #self.assertRaises(KeyError, tbl.getTimestamp, 1000)

    def testLabelTable(self):
        db = self.getDB()
        schema.createIdTables(db)
        tbl = versionops.LabelTable(db)

        l1 = Label("conary.rpath.com@test:trunk")
        l2 = Label("conary.rpath.com@test:branch")

        tbl.addId(l1)
        tbl.addId(l2)
        assert(tbl[l1] == 1)
        assert(tbl[l2] == 2)

        assert(tbl.get(l1, "foo") == 1)
        del tbl[l1]
        assert(tbl.get(l1, "foo") == "foo")
        tbl.delId(2)
        self.assertRaises(KeyError, tbl.__getitem__, l2)

    def testSqlVersioning(self):
        db = self.getDB()
        schema.createSchema(db)

        vTbl = versiontable.VersionTable(db)
        bTbl = versionops.BranchTable(db)
        sv = versionops.SqlVersioning(db, vTbl, bTbl)
        i = items.Items(db)
        # we need the FileStreams table for eraseVersion to work
        # properly. It is created as part of the createTroves() call

        v5 = ThawVersion("/conary.rpath.com@test:trunk/5:1.2-5")
        v10 = ThawVersion("/conary.rpath.com@test:trunk/10:1.2-10")
        v15 = ThawVersion("/conary.rpath.com@test:trunk/15:1.2-15")
        v20 = ThawVersion("/conary.rpath.com@test:trunk/20:1.2-20")

        branch = v10.branch()
        itemId = i.addId('foo')
        sv.createBranch(itemId, branch)

        sv.createVersion(itemId, v10, 0, "foo:source")
        assert(bTbl.has_key(branch))
        assert(vTbl.has_key(v10))
        assert(sv.hasVersion(itemId, vTbl[v10]))
        assert(i.has_key("foo:source"))
        assert(not sv.hasVersion(2, vTbl[v10]))

        branchId = bTbl[branch]

        itemId2 = i.addId('bar')
        sv.createBranch(itemId2, branch)
        sv.createVersion(itemId2, v10, 0, None)
        self.assertRaises(versionops.DuplicateVersionError,
                          sv.createVersion, itemId2, v10, 0, None)

        assert([vTbl.getId(x) for x in sv.versionsOnBranch(1, branchId)]
                    == [ str(v10) ])

        sv.createVersion(1, v20, 0, None)
        assert([vTbl.getId(x) for x in sv.versionsOnBranch(1, branchId)]
                    == [ str(v20), str(v10) ])

        sv.createVersion(1, v15, 0, None)
        db.commit()
        assert([vTbl.getId(x) for x in sv.versionsOnBranch(1, branchId)] ==
               [ str(v20), str(v15), str(v10) ])

        sv.createVersion(1, v5, 0, None)
        assert([vTbl.getId(x) for x in sv.versionsOnBranch(1, branchId)] ==
               [ str(v20), str(v15), str(v10), str(v5) ])

        label = Label("conary.rpath.com@test:trunk")
        assert [bTbl.getId(x) for x in sv.branchesOfLabel(1, label) ]\
                    == [ branch ]

        brLabel = Label("conary.rpath.com@test:br1")

        branch1 = v10.createBranch(brLabel, withVerRel = False)
        branch2 = v20.createBranch(brLabel, withVerRel = False)

        sv.createBranch(1, branch1)
        sv.createBranch(1, branch2)

        assert([bTbl.getId(x) for x in sv.branchesOfLabel(1, brLabel)] == \
               [branch1, branch2])

        assert([bTbl.getId(x) for x in sv.branchesOfItem(1)] ==
                    [ branch, branch1, branch2 ])

    def testNodesTable(self):
        db = self.getDB()
        schema.createSchema(db)
        b = versionops.BranchTable(db)
        v = versiontable.VersionTable(db)
        i = items.Items(db)
        sv = versionops.SqlVersioning(db, v, b)
        ver = ThawVersion("/a.b.c@d:e/1:1-1-1")
        
        itemId = i.getOrAddId("foo")
        nodeId, versionId = sv.createVersion(itemId, ver, 0, None)
        cu = db.cursor()
        cu.execute("select sourceItemId from Nodes where nodeId = ?", nodeId)
        self.assertEqual(cu.fetchall()[0][0], None)
        srcId = i.getOrAddId("foo:source")
        sv.nodes.updateSourceItemId(nodeId, srcId)
        cu.execute("select sourceItemId from Nodes where nodeId = ?", nodeId)
        self.assertEqual(cu.fetchall()[0][0], srcId)
