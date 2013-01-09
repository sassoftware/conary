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

from conary import versions
from conary.repository.netrepos import instances
from conary.repository.netrepos import items
from conary.repository.netrepos import flavors
from conary.server import schema
from conary.local import versiontable
from conary.deps import deps

class InstancesTest(dbstoretest.DBStoreTestBase):

    id1 = "0001000100010001000100010001000100010001"
    id2 = "0001000100010001000100010001000100010002"

    def testInstances(self):
        cx = self.getDB()
        schema.createSchema(cx)
        cx.commit()
        v = versiontable.VersionTable(cx)
        f = flavors.Flavors(cx)
        it = items.Items(cx)
        item = it.addId('foo')
        version = v.addId(versions.VersionFromString('/c@r:d/1.0-1-1'))
        d = deps.parseDep('')
        flavor = f.get(d, None)

        idb = instances.InstanceTable(cx)
        idb.addId(item, version, flavor, clonedFromId = None, troveType = 0)
        self.assertEqual(idb[(item, version, flavor)], 1)
        self.assertEqual(idb.getId(1), (item, version, flavor, 1) )
        del idb

        idb = instances.InstanceTable(cx)
        self.assertEqual(idb[(item, version, flavor)], 1)
        cu = cx.cursor()
        cu.execute("select isPresent, clonedFromId from instances where instanceid = 1")
        row = cu.fetchall()[0]
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], None)
        idb.update(1, isPresent = 0, clonedFromId = 1)
        cu.execute("select isPresent, clonedFromId from instances where instanceid = 1")
        row = cu.fetchall()[0]
        self.assertEqual(row[0], 0)
        self.assertEqual(row[1], 1)
