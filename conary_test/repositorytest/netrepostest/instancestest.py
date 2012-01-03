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
        self.failUnlessEqual(idb[(item, version, flavor)], 1)
        self.failUnlessEqual(idb.getId(1), (item, version, flavor, 1) )
        del idb

        idb = instances.InstanceTable(cx)
        self.failUnlessEqual(idb[(item, version, flavor)], 1)
        cu = cx.cursor()
        cu.execute("select isPresent, clonedFromId from instances where instanceid = 1")
        row = cu.fetchall()[0]
        self.failUnlessEqual(row[0], 1)
        self.failUnlessEqual(row[1], None)
        idb.update(1, isPresent = 0, clonedFromId = 1)
        cu.execute("select isPresent, clonedFromId from instances where instanceid = 1")
        row = cu.fetchall()[0]
        self.failUnlessEqual(row[0], 0)
        self.failUnlessEqual(row[1], 1)
