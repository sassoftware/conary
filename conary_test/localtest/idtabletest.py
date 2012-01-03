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

from conary.dbstore import idtable


class Items(idtable.IdTable):
    def __init__(self, db):
        if 'TestItems' not in db.tables:
            idtable.createIdTable(db, 'TestItems', 'itemId', 'item')
        idtable.IdTable.__init__(self, db, 'TestItems', 'itemId', 'item')

    def getOrAddIds(self, items):
        for item in items:
            yield self.getOrAddId(item)

class CachedItems(idtable.CachedIdTable):
    def __init__(self, db):
        if 'TestItems' not in db.tables:
            idtable.createIdTable(db, 'TestItems', 'itemId', 'item')
        idtable.CachedIdTable.__init__(self, db, 'TestItems', 'itemId', 'item')

class IdTableTest(dbstoretest.DBStoreTestBase):

    def testCachedIdTable(self):
        cx = self.getDB()

        itemTable = CachedItems(cx)
        assert(itemTable.addId("john") == 1)
        # check to make sure that using a db with an existing table works
        del itemTable
        itemTable = CachedItems(cx)

        assert(itemTable.getId(1) == "john")
        assert(itemTable["jane"] == 2)
        assert(itemTable.getId(2) == "jane")
        itemTable.addId("joe")
        assert(itemTable["joe"] == 3)
        itemTable.addId("jacob")
        assert(itemTable["jacob"] == 4)
        keys = itemTable.keys()
        keys.sort()
        assert(keys == ['jacob', 'jane', 'joe', 'john'])
        assert(itemTable.values() == [ 1, 2, 3, 4 ])
        assert(sorted(itemTable.items()) ==
               sorted([('john', 1), ('jane', 2), ('joe', 3), ('jacob', 4)] ))
        itemTable.addId("josh")
        assert(itemTable["josh"] == 5)

        assert(itemTable.get("mike", "hello") == "hello")
        assert(itemTable["mike"] == 6)
        assert(itemTable.get("mike", "hello") == 6)

    def testIdTable(self):
        cx = self.getDB()

        itemTable = Items(cx)
        assert(itemTable.addId("john") == 1)
        # check to make sure that using a db with an existing table works
        del itemTable
        itemTable = Items(cx)

        assert(itemTable.getId(1) == "john")
        itemTable.addId("jane")
        assert(itemTable["jane"] == 2)
        assert(itemTable.getId(2) == "jane")
        assert(itemTable.getItemDict(("john", "jane")) == {'john':1, 'jane':2})
        del itemTable["john"]
        itemTable.addId("joe")
        assert(itemTable["joe"] == 3)
        itemTable.delId(itemTable["joe"])
        itemTable.addId("jacob")
        assert(itemTable["jacob"] == 4)
        keys = itemTable.keys()
        keys.sort()
        assert(keys == [ "jacob", "jane" ])
        assert(itemTable.values() == [ 2, 4 ])
        assert(sorted(itemTable.items()) == [ ("jacob", 4), ("jane", 2) ])
        del itemTable["jane"]
        del itemTable["jabob"]
        itemTable.addId("josh")
        assert(itemTable["josh"] == 5)

        assert(itemTable.get("mike", "hello") == "hello")

        del itemTable["jacob"]
        # double deletions aren't detected for performance reasons
        del itemTable["jacob"]
        del itemTable["john"]
        del itemTable["josh"]
        assert(not itemTable.keys())

        self.assertRaises(KeyError, itemTable.__getitem__, "josh")

        ids = list(itemTable.getOrAddIds(['a', 'b', 'c']))
        assert(ids == list(itemTable.getOrAddIds(['a', 'b', 'c'])))
        ids2 = list(itemTable.getOrAddIds(['a', 'e']))
        assert(ids[0] == ids2[0])
        assert(max(ids) < max(ids2))

    def testIdPairMapping(self):
        cx = self.getDB()

        idtable.createIdPairTable(cx, "test", "first", "second", "val")
        tbl = idtable.IdPairMapping(cx, "test", "first", "second", "val")
        tbl[(1,2)] = 100
        tbl[(2,3)] = 101
        assert(tbl[(1,2)] == 100)
        assert(tbl[(2,3)] == 101)

        del tbl[(2,3)]
        self.assertRaises(KeyError, tbl.__getitem__, (2,3))
        assert(tbl.get((2,3), "foo") == "foo")

    def testIdMapping(self):
        cx = self.getDB()

        idtable.createMappingTable(cx, 'test', 'thekey', 'val')
        tbl = idtable.IdMapping(cx, "test", "thekey", "val")
        tbl[2] = 5
        tbl[5] = 10
        assert(tbl[2] == 5)
        assert(tbl[5] == 10)
        del(tbl[2])
        self.assertRaises(KeyError, tbl.__getitem__, 2)
        assert(tbl.get(2, 3.14159) == 3.14159)

    def testIdPairSet(self):
        cx = self.getDB()

        idtable.createIdPairTable(cx, 'test', 'first', 'second', 'val')
        tbl = idtable.IdPairSet(cx, "test", "first", "second", "val")
        tbl.addItem((1,2), 100)
        tbl.addItem((2,3), 200)
        a = tbl[(1,2)]
        assert([x for x in tbl[(1,2)]] == [ 100 ])
        assert([x for x in tbl[(2,3)]] == [ 200 ])
        assert([x for x in tbl.getByFirst(1)] == [ 100 ])

        tbl.addItem((1,2), 101)
        tbl.addItem((2,3), 201)
        assert([x for x in tbl[(1,2)]] == [ 100, 101 ])
        assert([x for x in tbl[(2,3)]] == [ 200, 201 ])
        assert([x for x in tbl.getByFirst(1)] == [ 100, 101 ])

        tbl.delItem((2, 3), 200)
        assert([x for x in tbl[(2,3)]] == [ 201 ])

        del tbl[(1,2)]
        self.assertRaises(KeyError, tbl.__getitem__, (1,2))

        tbl.delItem((2, 3), 201)
        self.assertRaises(KeyError, tbl.__getitem__, (2,3))
