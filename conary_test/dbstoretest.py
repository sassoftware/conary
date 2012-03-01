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
import time
import unittest
import tempfile
import itertools

from conary import dbstore
from conary.dbstore import sqllib, sqlerrors, shell
from conary.lib import cfgtypes

from testutils import sqlharness


class DBStoreTestBase(unittest.TestCase):
    def __init__(self, *args, **kw):
        unittest.TestCase.__init__(self, *args, **kw)
        self.harness = None
        self._dblist = []
        
    def setUp(self):
        if self.harness is None:
            self.harness = sqlharness.start()
        for db in self._dblist:
            db._reset()

    def tearDown(self):
        for db in self._dblist:
            db.stop()
        self._dblist = []
    
    def getDB(self, dbName="dbstore"):
        assert(self.harness)
        repodb = self.harness.getDB(dbName)
        db = repodb.connect()
        db.loadSchema()
        # keep a reference around because we don't want the db
        # database closed from under us if repodb goes away
        self._dblist.append(repodb)
        return db

class DummyDriver:
    def cursor(self):
        return DummyDriver()

    def lastrowid(self):
        return 1

class DBStoreBaseCursorTest(unittest.TestCase):
    def testCursor(self):
        cu = dbstore.Cursor(DummyDriver())
        assert(cu.binary != None)
        assert(cu.dbh != None)
        assert(cu._cursor != None)
        try:
            cu.badattribute
        except AttributeError:
            pass
        else:
            assert(0)
        assert(cu.lastrowid() == 1)

class DBStoreCfgTest(unittest.TestCase):
    def testCfg(self):
        cfg = dbstore.CfgDriver()
        self.assertRaises(cfgtypes.ParseError, cfg.parseString, 'splart')
        parsed = cfg.parseString('mysql root@localhost/db')
        assert(cfg.format(parsed) == 'mysql root@localhost/db')

class DBStoreCodeTest(unittest.TestCase):
    def testCaselessDict(self):
        def __testCD(d):
            assert("test" in d)
            assert("Test" in d)
            assert(d["Test"] == "value0")
            assert(d == {"test" : "value0"})
            d["TesT"] = "value1"
            assert(len(d) == 1)
            assert(d == {"test" : "value1"})
            d.update({"teSt" : "value"})
            assert(len(d) == 1)
            assert(d == {"test" : "value"})
            assert(d.items() == [("teSt", "value")])
            assert(d.keys() == ["teSt"])
            assert(d.values() == ["value"])
            assert([x for x in d.iterkeys()] == d.keys())
            assert([x for x in d.itervalues()] == d.values())
            assert([x for x in d.iteritems()] == d.items())
            del d["test"]
            assert(len(d) == 0)
            d.setdefault("Test", []).append("value")
            assert(len(d) == 1)
            assert(d.items() == [("Test", ["value"])])
        d = sqllib.CaselessDict()
        d["TEST"] = "value0"
        __testCD(d)
        d = sqllib.CaselessDict([("TEST", "value0")])
        __testCD(d)
        d = sqllib.CaselessDict({"TEST": "value0"})
        __testCD(d)
        d = sqllib.CaselessDict({"TEST0": "value0", "TEST1": "value1"})
        self.assertEqual(d["test0"], "value0")
        self.assertEqual(d["test1"], "value1")
        assert (d.has_key("test1"))
        del d["test1"]
        assert (not d.has_key("test1") )

    def testDBversion(self):
        dbv = sqllib.DBversion
        assert(2 == dbv(2))
        assert(2 == dbv(2,0))
        assert(2 < dbv(2,1))
        assert(2 < dbv(3,1))
        assert(2 > dbv(1,9))
        assert(0 == dbv(0))

        assert((2,0) == dbv(2))
        assert((0,0) == dbv(0,0))
        assert((2,0) == dbv(2,0))
        assert((2,1) < dbv(2,2))
        assert((2,9) < dbv(3,1))
        assert((2,1) > dbv(1,9))

        assert(dbv(3,3) < dbv(3,4))
        assert(dbv(3,3) < dbv(4,2))
        assert(dbv(3,3) > dbv(2,4))
        assert(dbv(3,3) == dbv(3,3))
        assert(dbv(3,3) >= dbv(3,3))

class DBStoreTest(DBStoreTestBase):
    def testURIParsing(self):
        fields = ["username", "password", "hostname", "port", "database"]
        values = {"username" : "johndoe", "password" : "l3tm31n",
                  "hostname" : 'localhost.localdomain', 'port' : 31337,
                  "database" : "foo" }
        def _runTest(values, fields = fields):
            def _test(uri, fields = fields, values = values):
                db = dbstore.Database(uri)
                try:
                    d = db._connectData(fields)
                    assert(False not in [ val in [values[key], None] for (key, val) in d.items()])
                except AssertionError:
                    print "\nFailed testing for", uri
                    print "Expecting:", values
                    print "Obtained: ", d
                    raise
                except:
                    print "\nERROR: Failed testing for", uri
                    raise
            # for these tests we have to use values from the fields
            _test("%(username)s:%(password)s@%(hostname)s:%(port)s/%(database)s" % values)
            _test("%(username)s:@%(hostname)s:%(port)s/%(database)s" % values)
            _test("%(username)s:%(password)s@%(hostname)s/%(database)s" % values)
            _test("%(username)s:@%(hostname)s/%(database)s" % values)
            _test("%(username)s@%(hostname)s:%(port)s/%(database)s" % values)
            _test("%(username)s@%(hostname)s/%(database)s" % values)
            _test("%(hostname)s:%(port)s/%(database)s" % values)
            _test("%(hostname)s/%(database)s" % values)
            _test("%(database)s" % values)
        _runTest(values)
        values["hostname"] = "localhost.localdomain"
        _runTest(values)
        values["database"] = "/tmp/sql.db"
        _runTest(values)

    def testSqliteRegexp(self):
        db = dbstore.connect(':memory:', 'sqlite')
        cu = db.cursor()
        cu.execute('create table foo(bar varchar)')
        cu.execute("insert into foo values ('foo1')")
        cu.execute("insert into foo values ('foo2')")
        cu.execute("insert into foo values ('foo3')")
        cu.execute('select * from foo where bar regexp ".*2"')
        matches = cu.fetchall()
        assert(matches == [ ('foo2',) ])

    def testUTF8(self):
        db = self.getDB()
        cu = db.cursor()

        create = "CREATE TABLE Testing (value varchar(128))"
        # ensure that the table we create is set up for utf8 contents
        driver = os.environ.get('CONARY_REPOS_DB', 'sqlite')
        if driver == "mysql":
            create += " CHARACTER SET utf8"
        cu.execute(create)

        val = 'This is \xe2\x99\xaa \xe2\x99\xac utf-8!'
        cu.execute("INSERT INTO Testing VALUES (?)", val)

        cu.execute("SELECT * FROM Testing")
        ret = cu.fetchone()
        assert(ret[0] == val)

    def testEmptyResults(self):
        db = self.getDB()
        cu = db.cursor()

        cu.execute("CREATE TABLE Testing (value CHAR(128))")
        cu.execute("SELECT * FROM Testing")

        ret = cu.fetchall()
        assert(len(ret) == 0)
        assert(isinstance(ret, (list, tuple)))

    def testCaselessDict(self):
        d = sqllib.CaselessDict({'abcd': 100, 'efgh': 200})

        assert(d['ABCD'] == 100)
        assert(d['efgh'] == 200)
        assert('abcd' in d)
        assert('EFGH' in d)

        assert(['abcd', 'efgh'] == d.keys())
        assert([100, 200] == d.values())

        assert([x for x in d] == ['abcd', 'efgh'])

    def testReturnTypesNumeric(self):
        db = self.getDB()
        cu = db.cursor()

        cu.execute("CREATE TABLE Testing (value NUMERIC(14,1))")
        cu.execute("INSERT INTO Testing VALUES (?)", (1000.1,))

        cu.execute("SELECT * FROM Testing")
        row = cu.fetchone()
        self.assertEqual(float(row[0]), 1000.1)

    def testReturnTypesInteger(self):
        db = self.getDB()
        cu = db.cursor()

        cu.execute("CREATE TABLE Testing (value INTEGER)")
        cu.execute("INSERT INTO Testing VALUES (?)", (1000,))

        cu.execute("SELECT * FROM Testing")
        row = cu.fetchone()
        assert(row == (1000,))

    def testReturnTypesBlob(self):
        db = self.getDB()
        cu = db.cursor()

        cu.execute("CREATE TABLE Testing (value %(BLOB)s)" % db.keywords)
        cu.execute("INSERT INTO Testing VALUES (?)",
                   (cu.binary('\x0b\x00\x0b'),))

        cu.execute("SELECT * FROM Testing")
        row = cu.fetchone()
        self.assertEqual(cu.frombinary(row[0]), '\x0b\x00\x0b')

    def testReturnTypesBinary(self):
        db = self.getDB()
        cu = db.cursor()

        cu.execute("CREATE TABLE Testing (value %(BINARY4)s)" % db.keywords)
        cu.execute("INSERT INTO Testing VALUES (?)",
                   (cu.binary('\x0b\x00\x0b\x0a'),))

        cu.execute("SELECT * FROM Testing")
        row = cu.fetchone()
        self.assertEqual(cu.frombinary(row[0]), '\x0b\x00\x0b\x0a')

    def testPrimaryKey(self):
        db = self.getDB()
        cu = db.cursor()
        cu.execute("CREATE TABLE Testing(id %(PRIMARYKEY)s, name VARCHAR(100))" % db.keywords)
        cu.execute("INSERT INTO Testing (name) VALUES (?)", "one")
        cu.execute("INSERT INTO Testing (name) VALUES (?)", "two")
        cu.execute("INSERT INTO Testing (name) VALUES (?)", "three")
        cu.execute("SELECT * FROM Testing ORDER BY id")
        ret = cu.fetchall()
        assert(ret == [(1, "one"), (2, "two"), (3, "three")])

        cu.execute("DELETE FROM Testing WHERE id = ?", 2)
        cu.execute("INSERT INTO Testing (name) VALUES (?)", "four")
        ret = cu.execute("SELECT * FROM Testing ORDER BY id")
        assert(cu.fetchall() == [(1, "one"), (3, "three"), (4, "four")])

        cu.execute("DELETE FROM Testing WHERE id > ?", 1)
        cu.execute("INSERT INTO Testing (name) VALUES (?)", "five")
        ret = cu.execute("SELECT * FROM Testing ORDER BY id")
        assert(cu.fetchall() == [(1, "one"), (5, "five")])

        cu.execute("DELETE FROM Testing")
        cu.execute("INSERT INTO Testing (name) VALUES (?)", "six")
        ret = cu.execute("SELECT * FROM Testing ORDER BY id")
        assert(cu.fetchall() == [(6, "six")])

    def testUnique(self):
        db = self.getDB()
        cu = db.cursor()
        cu.execute("CREATE TABLE Testing(id %(PRIMARYKEY)s, name VARCHAR(100))" % db.keywords)
        db.commit()

        db.loadSchema()
        db.createIndex("Testing", "TestingIdx", "name", unique=True)
        stmt = "INSERT INTO Testing(name) VALUES (?)"
        cu.execute(stmt, "foo")
        db.commit()

        self.assertRaises(sqlerrors.ColumnNotUnique, cu.execute, stmt, "foo")
        db.rollback()

        cu.execute(stmt, "Foo")
        db.commit()

        self.assertRaises(sqlerrors.ColumnNotUnique, cu.execute, stmt, "Foo")
        db.rollback()

        self.assertRaises(sqlerrors.ColumnNotUnique, cu.execute,
                          "UPDATE Testing SET name = ? "
                          "WHERE LOWER(name) = LOWER(?)", ("bar", "FOO"))
        db.rollback()

        # Create index again without loadSchema (CNY-3380)
        assert not db.createIndex("Testing", "TestingIdx", "name", unique=True)

        cu.execute(stmt, "FOO")

    def testTriggers(self):
        db = self.getDB()
        cu = db.cursor()
        cu.execute("""
        CREATE TABLE Testing(
            id        %(PRIMARYKEY)s,
            name      VARCHAR(100) NOT NULL,
            changed   NUMERIC(14,0) NOT NULL DEFAULT 0
        )""" % db.keywords)
        db.commit()
        db.loadSchema()
        db.createTrigger("Testing", "changed", "INSERT")
        db.createTrigger("Testing", "changed", "UPDATE")
        db.commit()
        db.loadSchema()
        self.assertTrue("testing_insert" in db.triggers)
        self.assertTrue("testing_update" in db.triggers)

        def __insert(val):
            cu = db.transaction()
            cu.execute("INSERT INTO Testing (name) VALUES (?)", val)
            time.sleep(1.1)
            db.commit()
        __insert("one")
        __insert("two")

        cu = db.cursor()
        cu.execute("SELECT changed, id from Testing ORDER BY id")
        ret1 = cu.fetchall()
        assert(len(ret1) == 2)
        assert(ret1[0][0] > 0 and ret1[1][0] > 0)
        assert(ret1[1][0] > ret1[0][0])

        # test update triggers
        time.sleep(1.1)
        cu.execute("UPDATE Testing SET name = ? where name = ?",
                   "ONE", "one")
        cu.execute("SELECT changed, id from Testing ORDER BY id")
        ret2 = cu.fetchall()
        assert(ret2[0][0] > 0 and ret2[1][0] > 0)
        assert(ret2[1] == ret1[1])
        assert(ret2[0][0] > ret2[1][0])

        # test trigger re-creation
        db.dropTrigger("Testing", "insert")
        db.loadSchema()
        self.assertFalse("testing_insert" in db.triggers)
        db.dropTrigger("Testing", "update")
        db.loadSchema()
        self.assertFalse("testing_update" in db.triggers)
        db.createTrigger("Testing", "changed", "UPDATE")
        db.loadSchema()
        self.assertTrue("testing_update" in db.triggers)

    def testReturnValues(self):
        db = self.getDB()
        cu = db.cursor()
        cu.execute("CREATE TABLE Testing(id %(PRIMARYKEY)s, name VARCHAR(100))" % db.keywords)
        for i in range(1, 11):
            cu.execute("INSERT INTO Testing (name) VALUES (?)", str(i))
        ret = cu.execute("SELECT id, name FROM Testing")
        # execute should return the cursor back
        assert (hasattr(ret, "fetchone"))
        assert (hasattr(ret, "next"))
        row = cu.fetchone()
        assert(len(row) == 2)
        assert(row == (1, "1"))
        rows = cu.fetchall()
        assert(len(rows) == 9)
        assert(isinstance(rows, list))

    def testTemporaryTables(self):
        # create the databases first
        db1 = self.getDB("repodb1")
        db2 = self.getDB("repodb2")

        # clean up temp tables automatically created by getDB()
        db1.close()
        db1.connect()
        
        cu = db1.cursor()
        cu.execute("CREATE TEMPORARY TABLE Testing(id INT)")
        db1.tempTables['Testing'] = True
        db1.commit()
        db1.loadSchema()

        # swap databases to check temporary table behavior
        db1.use("repodb2")
        db1.use("repodb1")
        db1.loadSchema()
        # Mysql is the weird one; it caches and preserves the
        # temporary tables as we use() from one to another and back
        if db1.driver == "mysql":
            self.assertEqual(db1.tempTables, {'Testing': True})
        else:
            self.assertEqual(db1.tempTables, {})
        
    def testPrecompiling(self):
        db = self.getDB()
        cu = db.cursor()
        cu.execute("create table foo (a %(PRIMARYKEY)s, val1 integer, val2 integer)" % db.keywords)
        stmt = cu.compile("insert into foo(val1, val2) values (?, ?)")
        ret = []
        for x in xrange(100,200):
            cu.execstmt(stmt, x, 2*x)
            ret.append((x, 2*x))
        assert(cu.execute("select val1, val2 from foo order by a").fetchall() == ret)

    def testLastRowID(self):
        db = self.getDB()
        cu = db.cursor()
        cu.execute("create table foo(id %(PRIMARYKEY)s, val integer)" % db.keywords)
        cu.execute("insert into foo(id, val) values (0,0)")
        for x in xrange(101,200):
            cu.execute("insert into foo(val) values (?)", x)
            assert(cu.lastrowid == x-100)

class DBStoreSQLTest(DBStoreTestBase):
    def testSQL(self):
        db = self.getDB()
        cu = db.cursor()
        cu.execute("create table foo (id %(PRIMARYKEY)s, name VARCHAR(100))" % db.keywords)

        # test handling of the ? syntax
        cu.execute("insert into foo (id, name) values (?,?)", (1, "one"))
        cu.execute("insert into foo (id, name) values (?, 'two?')", 2)
        cu.execute("update foo set name = 'two' where name = ?", "two?")
        # incorrect form, but it is used in the code, so we need to accept it
        cu.execute("insert into foo (id, name) values (?,?)", 3, "three")
        cu.execute("insert into foo (id, name) values (?, 'four')", (4,))
        db.commit()
        cu.execute("select id, name from foo where id between ? and ?", (2,2))
        self.assertEqual(cu.fetchall(), [(2,"two")])

        # test handling of the :id syntax
        cu.execute("insert into foo (id, name) values (:id, :name)", id=10, name=":source")
        cu.execute("insert into foo (id, name) values (:id, ':test')", id=11)
        cu.execute("insert into foo (id, name) values (:id, :name)",
                   {"id":12, "name":":runtime", "junk":0})
        db.commit()
        cu.execute("select id from foo where name = :val", val = ":test")
        self.assertEqual(cu.fetchall(), [(11,)])
        cu.execute("select id from foo where name = ':source'")
        self.assertEqual(cu.fetchall(), [(10,)])
        cu.execute("""select id
        from foo where name like
        '%%:%%' and name like :like and
        id between :id1 and :id2 and
        id in (:id1,11,:id2) LIMIT :lim""", extra_var = "blah",
                   like = "%:test", id1 = 10, id2=12, lim=2)
        self.assertEqual(cu.fetchall(), [(11,)])

    def testTransaction(self):
        hdb = self.harness.getDB()
        db = hdb.connect()
        db2 = dbstore.connect(hdb.path, hdb.driver)
        db2.transaction()
        self.assertTrue(sqlerrors.DatabaseLocked, db.transaction)
        db2.commit()
        db.transaction()

class DBStoreSchemaTest(DBStoreTestBase):
    def testCreateSchema(self):
        from conary.server import schema
        db = self.getDB()
        schema.createSchema(db)
        db.commit()
        # we sould not fail with "already exists errors"
        schema.createSchema(db)

    def testTableOps(self):
        db = self.getDB()
        cu = db.cursor()

        cu.execute("""
        create table foo(
        id %(PRIMARYKEY)s,
        name VARCHAR(100),
        address VARCHAR(767)
        )""" % db.keywords)
        db.createIndex("foo", "foo_name_address_uq", "name, address",
                       unique = True, check = False)
        cu.execute("""
        create table bar1(
        id INTEGER NOT NULL,
        name VARCHAR(100) NOT NULL,
        CONSTRAINT bar_pk PRIMARY KEY (id, name)
        )""")
        cu.execute("""
        create table bar2(
        fid INTEGER NOT NULL,
        fname VARCHAR(100) NOT NULL,
        data VARCHAR(500),
        CONSTRAINT bar_fk FOREIGN KEY (fid, fname) REFERENCES bar1(id, name)
        )""")
        for x in range(5):
            d = { "i":x, "n":"joe" + str(x), "a":"location" + str(x) }
            cu.execute("insert into foo (id, name, address) values (:i, :n, :a)", d)
            cu.execute("insert into bar1 values (:i, :n)", i = x, n = d["n"])
            cu.execute("insert into bar2 values (:i, :n, :bar)",
                       i = x, n = d["n"], bar = "unknown")
        db.commit()

        # test uniq index working
        self.assertRaises(dbstore.sqlerrors.ColumnNotUnique, cu.execute,
                          "insert into foo (name, address) values (:n, :a)",
                          a = "location2", n = "joe2")
        db.rollback()
        db.loadSchema()
        db.renameColumn("foo", "address", "location")
        cu = db.cursor()
        cu.execute("select * from foo where id = :id", id = 2)
        ret = cu.fetchall_dict()
        assert(ret == [{"id" : 2, "name" : "joe2", "location": "location2"}])

        db.renameColumn("bar2", "fname", "bar1name")
        db.renameColumn("bar2", "fid", "bar1id")

    # test Column renames for various tables
    def testRenameColumn(self):
        db = self.getDB()
        from conary.server import schema
        schema.createSchema(db)
        db.commit()
        db.loadSchema()

        # simple columns that are not in any sort of FK relationship
        db.renameColumn("branches", "branch", "foo")

        db.renameColumn("changelogs", "name", "author")

        db.renameColumn("changelogs", "message", "msg")
        db.renameColumn("changelogs", "changed", "lastChanged")
        db.renameColumn("dependencies", "class", "cls")
        db.renameColumn("entitlementaccessmap", "entGroupId", "egid")
        db.renameColumn("entitlementgroups", "entGroup", "eg")
        db.renameColumn("entitlementowners", "ownerGroupId", "ogid")
        db.renameColumn("entitlements", "entitlement", "e")
        db.renameColumn("filestreams", "stream", "data")
        db.renameColumn("instances", "changed", "lastChanged")
        db.renameColumn("items", "item", "i")
        db.renameColumn("labelmap", "branchId", "bid")
        db.renameColumn("labelmap", "labelId", "lid")
        db.renameColumn("latestmirror", "mark", "lastMark")
        db.renameColumn("metadata", "timeStamp", "ts")
        db.renameColumn("metadata", "changed", "lastChanged")
        db.renameColumn("metadataitems", "data", "stream")
        db.renameColumn("nodes", "changed", "last_changed")
        db.renameColumn("nodes", "versionId", "vid")
        db.renameColumn("pgpfingerprints", "fingerprint", "fp")
        db.renameColumn("pgpfingerprints", "changed", "lastchanged")
        db.renameColumn("pgpkeys", "changed", "lc")
        db.renameColumn("pgpkeys", "pgpkey", "pk")
        db.renameColumn("pgpkeys", "fingerprint", "fp")
        db.renameColumn("provides", "instanceId", "iid")
        db.renameColumn("provides", "depId", "did")
        db.renameColumn("requires", "depId", "did")
        db.renameColumn("requires", "depCount", "c")
        db.renameColumn("trovefiles", "streamId", "sid")
        db.renameColumn("filepaths", "pathId", "pid")
        db.renameColumn("filepaths", "path", "path")
        db.renameColumn("troveinfo", "infoType", "it")
        db.renameColumn("troveinfo", "changed", "c")
        db.renameColumn("troveinfo", "data", "d")
        db.renameColumn("troveredirects", "flavorId", "fid")
        db.renameColumn("troveredirects", "changed", "ch")
        db.renameColumn("trovetroves", "flags", "fl")
        db.renameColumn("usergroupmembers", "userGroupId", "ugid")
        db.renameColumn("usergroupmembers", "userId", "uid")
        db.renameColumn("usergroups", "canMirror", "cm")
        db.renameColumn("usergroups", "changed", "lastc")
        db.renameColumn("users", "userName", "uname")

        # changing a column that has FKs pointing out
        db.renameColumn("nodes", "itemId", "nodeItemId")
        db.renameColumn("trovetroves", "instanceId", "id1")
        db.renameColumn("trovetroves", "includedId", "id2")

        # renaming a primary key without any FKs pointing to it
        db.renameColumn("labelmap", "labelmapid", "lmid")

        # changing a column that has FKs pointing to it
        if db.driver == "mysql":
            # this fails horribly with constraint violations
            # because MySQL is such a lame toy
            self.assertRaises(dbstore.sqlerrors.ConstraintViolation,
                              db.renameColumn, "items", "itemId", "id")
        else:
            # others should know to update the FKs
            # sqlite does not enforce FKs so this should succeed as well
            db.renameColumn("items", "itemId", "id")

        # test on a table with some entries in it
        cu = db.cursor()
        db.renameColumn("flavors", "flavor", "flavor_string")
        ret, = cu.execute("select * from flavors where flavorId = 0").fetchall()
        self.assertEqual(ret['flavorId'], 0)
        self.assertEqual(ret['flavor_string'], '')

        set1 = cu.execute("select * from flavorscores").fetchall()
        db.renameColumn("flavorscores", "request", "req")
        db.renameColumn("flavorscores", "value", "val")
        set2 = cu.execute("select * from flavorscores").fetchall()
        assert(sorted(set1) == sorted(set2))

    def testExecuteMany(self):
        db = self.getDB()
        cu = db.cursor()

        cu.execute("CREATE TABLE foo(id %(PRIMARYKEY)s, no INTEGER)" % db.keywords)
        cu.executemany("INSERT INTO foo (id, no) VALUES (?,?)", [(0,0)])

        # verify results
        def _check(cu, sets):
            counter = len(range(101,200))*sets + 1
            cu.execute("SELECT COUNT(*) FROM foo")
            assert(cu.fetchall()[0][0] == counter)

            cu.execute("SELECT id, no FROM foo")
            set1 = [(x[0],x[1]) for x in cu]
            set2 = zip(range(0,counter+1), [0]+range(101,200)*sets)
            assert(set1 == set2)

        cu.executemany("INSERT INTO foo (no) VALUES (?)", [(x,) for x in range(101,200)])
        _check(cu, 1)
        cu.executemany("INSERT INTO foo (no) VALUES (?)", ((x,) for x in range(101,200)))
        _check(cu, 2)
        cu.executemany("INSERT INTO foo (no) VALUES (:id)", [{"id":x} for x in range(101,200)])
        _check(cu, 3)
        cu.executemany("INSERT INTO foo (no) VALUES (:id)", ({"id":x} for x in xrange(101,200)))
        _check(cu, 4)

    def testBulkload(self):
        db = self.getDB()
        cu = db.cursor()

        cu.execute("CREATE TABLE foo(id %(PRIMARYKEY)s, no INTEGER)" % db.keywords)
        db.bulkload("foo", itertools.izip(xrange(100,200), xrange(500,600)), ["id", "no"])
        cu.execute("select id, no from foo order by id")
        for x, y in itertools.izip (cu, itertools.izip(xrange(100,200), xrange(500,600))):
            self.assertEqual(x, y)
        db.bulkload("foo", itertools.izip(xrange(200,300), xrange(500,600)), ["id", "no"])
        cu.execute("select count(*) from foo")
        self.assertEqual(cu.fetchone()[0], 200)

        # test blobs with bulkload
        testData = []
        def _iterBlobs(cu, testData, count=100, size=20):
            fd = os.open("/dev/urandom", os.O_RDONLY)
            for i in xrange(count):
                data = os.read(fd, size)
                testData.append(data)
                yield (i, cu.binary(data))
            os.close(fd)
        cu.execute("create table bar(id %(PRIMARYKEY)s, data %(BINARY20)s)" % db.keywords)
        db.commit() # capture the current state
        cu = db.cursor()
        db.bulkload("bar", _iterBlobs(cu, testData, 100), ["id", "data"])
        cu.execute("select data from bar")
        for a,b in itertools.izip(cu, testData):
            self.assertEqual(cu.frombinary(a[0]), b)
        # test bulkload rollback-ability
        cu.execute("select count(*) from foo")
        self.assertEqual(cu.fetchone()[0], 200)
        cu.execute("select count(*) from bar")
        self.assertEqual(cu.fetchone()[0], 100)
        db.rollback()
        # bar should be all rolled back since we didn't have a commit or DDL since
        cu.execute("select count(*) from bar")
        self.assertEqual(cu.fetchone()[0], 0)
        
    def testExecuteArgs(self):
        db = self.getDB()
        cu = db.cursor()

        cu.execute("""CREATE TABLE foo(
            id %(PRIMARYKEY)s,
            name VARCHAR(100),
            val INTEGER )""" % db.keywords)
        # simple sql
        cu.execute("INSERT INTO foo (val, name) VALUES (1, 'one')")
        # one param
        cu.execute("INSERT INTO foo (val, name) VALUES (2, ?)", 'two')
        cu.execute("INSERT INTO foo (val, name) VALUES (2, :name)", name = 'two')
        cu.execute("INSERT INTO foo (val, name) VALUES (:val, 'two')", val = 2)
        # >1 params
        cu.execute("INSERT INTO foo (val, name) VALUES (?, ?)", 3, "three")
        cu.execute("INSERT INTO foo (val, name) VALUES (?, ?)", (3, "three"))
        cu.execute("INSERT INTO foo (val, name) VALUES (?, ?)", [3, "three"])
        cu.execute("INSERT INTO foo (name, val) VALUES (:name, :val)",
                   name="three", val=3)
        cu.execute("INSERT INTO foo (name, val) VALUES (:name, :val)",
                   {"name":"three", "val":3} )
        # check results
        cu.execute("SELECT COUNT(*) AS c, val, name FROM foo "
                   "GROUP BY val, name ORDER BY val")
        ret = cu.fetchall()
        assert (ret == [
            (1, 1, 'one'),
            (3, 2, 'two'),
            (5, 3, 'three')
            ])

    def testAutoIncrement(self):
        db = self.getDB()
        #if db.driver == "sqlite":
        #    raise testhelp.SkipTestException('sqlite lacks sqlite_sequence table')
        cu = db.cursor()
        cu.execute("""CREATE TABLE foo(
            id %(PRIMARYKEY)s,
            name VARCHAR(100),
            val INTEGER )""" % db.keywords)
        db.setAutoIncrement("foo", "id")
        cu.execute("insert into foo(name) values (?)", "first")
        self.assertEqual(cu.lastrowid, 1)
        db.commit()
        if db.driver == "sqlite":
            # for some reason sqlite wants the database CLOSED and REOPEN
            # to notice changes in the sqlite_sequence table
            db.close()
            db.connect()
        cu = db.cursor()
        db.setAutoIncrement("foo", "id", 19)
        cu.execute("insert into foo(name) values (?)", "twenty")
        self.assertEqual(cu.lastrowid, 20)
        cu.execute("select id from foo where name = ?", "twenty")
        ret = cu.fetchone()[0]
        self.assertEqual(ret, 20)
        
        cu.execute("insert into foo(name) values (?)", "twentyone")
        self.assertEqual(cu.lastrowid, 21)
        cu.execute("select id from foo where name = ?", "twentyone")
        ret = cu.fetchone()[0]
        self.assertEqual(ret, 21)

        # test auto(re)setting
        db.setAutoIncrement("foo", "id", 10)
        db.setAutoIncrement("foo", "id")
        cu.execute("insert into foo(name) values(?)", "22")
        self.assertEqual(cu.lastrowid, 22)
        
    def testIntegerStrings(self):
        "tests that the db backend does not convert strings to ints when least expected"
        db = self.getDB()
        cu = db.cursor()
        cu.execute("""
        create table foo(
           test1 %(STRING)s,
           test2 %(PATHTYPE)s
        )""" % db.keywords)
        cu.execute("insert into foo(test1,test2) values (?,?)", (1, "2"))
        cu.execute("insert into foo(test1,test2) values (?,?)", ("01", "2"))
        cu.execute("insert into foo(test1,test2) values (?,?)", ("001", "02"))
        cu.execute("select * from foo")
        ret = [ (x[0],x[1]) for x in cu.fetchall()]
        for actual, expected in zip(ret, [('1','2'), ('01','2'), ('001','02')]):
            actual = actual[0], cu.frombinary(actual[1])
            self.assertEqual(actual, expected)


class DbShellTestCase(testhelp.TestCase):
    def testShellDoShowLoadsSchema(self):
        fd, sqlitePath = tempfile.mkstemp()
        os.close(fd)

        # Open a database shell
        db = shell.DbShell(path=sqlitePath, driver='sqlite')
        try:
            output = self.captureOutput(db.do__show, 'tables')
        except AttributeError, e:
            if e.args[0] == "Database instance has no attribute 'tables'":
                self.fail("do__show still doesn't call loadSchema")
            raise

        self.assertEqual(output[1], '\n')

        # Create some tables
        db.cu.execute("create table foo (id int, val int)")
        db.cu.execute("create table bar (id int, val int)")
        db.db.commit()

        output = self.captureOutput(db.do__show, 'tables')
        self.assertEqual(output[1], 'bar\nfoo\n')

        # Make sure it's persistent
        del db
        db = shell.DbShell(path=sqlitePath, driver='sqlite')

        output = self.captureOutput(db.do__show, 'tables')
        self.assertEqual(output[1], 'bar\nfoo\n')

        os.unlink(sqlitePath)
