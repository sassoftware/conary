#!/usr/bin/env python
import testsupport
import os, string, sys, types, unittest
import sqlite3 as sqlite

class TransactionTests(unittest.TestCase, testsupport.TestSupport):
    def setUp(self):
        self.filename = self.getfilename()
        self.cnx = sqlite.connect(self.filename)
        self.cur = self.cnx.cursor()

    def tearDown(self):
        try:
            self.cnx.close()
            self.removefile()
        except AttributeError:
            pass
        except sqlite.InterfaceError:
            pass

    def CheckValueInTransaction(self):
        self.cur.execute("create table test (a)")
        self.cur.execute("insert into test (a) values (?)", "foo")
        self.cur.execute("select count(a) as count from test")
        res = self.cur.fetchone()
        self.failUnlessEqual(res.count, 1,
                             "Wrong number of rows during transaction.")

    def CheckValueAfterCommit(self):
        self.cur.execute("create table test (a)")
        self.cur.execute("insert into test (a) values (?)", "foo")
        self.cnx.commit()
        self.cur.execute("select count(a) as count from test")
        res = self.cur.fetchone()
        self.failUnlessEqual(res.count, 1,
                             "Wrong number of rows during transaction.")

    def CheckValueAfterRollback(self):
        self.cur.execute("create table test (a)")
        self.cnx.commit()
        self.cur.execute("insert into test (a) values (?)", "foo")
        self.cnx.rollback()
        self.cur.execute("select count(a) as count from test")
        res = self.cur.fetchone()
        self.failUnlessEqual(res.count, 0,
                             "Wrong number of rows during transaction.")

    def CheckImmediateCommit(self):
        try:
            self.cnx.commit()
        except:
            self.fail("Immediate commit raises exeption.")

    def CheckImmediateRollback(self):
        try:
            self.cnx.rollback()
        except:
            self.fail("Immediate rollback raises exeption.")


    def CheckRollbackAfterError(self):
        import tempfile, os
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        db1 = sqlite.connect(fn)
        db2 = sqlite.connect(fn)
        cu1 = db1.cursor()
        cu2 = db2.cursor()
        cu1.execute('create table test(a)')
        db1.commit()
        cu1.execute('insert into test values(1)')
        try:
            cu2.execute('insert into test values(2)')
        except sqlite.InternalError, e:
            assert(str(e) == "database is locked")
        db2.rollback()
        db1.commit()
        assert([ x for x in cu1.execute("select * from test")] == [(1,)])
        os.unlink(fn)

    def CheckExecStmtTransaction(self):
        self.cur.execute("create table test (a)")
        self.cnx.commit()
        self.cur.execute("insert into test (a) values (?)", 'foo')
        assert(self.cnx.inTransaction)
        self.cnx.rollback()
        stmt = self.cur.compile("insert into test (a) values (?)")
        self.cur.execstmt(stmt, 'foo')
        assert(self.cnx.inTransaction)
        self.cnx.rollback()

class AutocommitTests(unittest.TestCase, testsupport.TestSupport):
    def setUp(self):
        self.filename = self.getfilename()
        self.cnx = sqlite.connect(self.filename, autocommit=1)
        self.cur = self.cnx.cursor()

    def tearDown(self):
        try:
            self.cnx.close()
            self.removefile()
        except AttributeError:
            pass
        except sqlite.InterfaceError:
            pass

    def CheckCommit(self):
        self.cur.execute("select abs(5)")
        try:
            self.cnx.commit()
        except:
            self.fail(".commit() raised an exception")

    def CheckRollback(self):
        self.cur.execute("select abs(5)")
        self.failUnlessRaises(sqlite.ProgrammingError, self.cnx.rollback)

class ChangeAutocommitTests(unittest.TestCase):
    pass

def suite():
    transaction_tests = unittest.makeSuite(TransactionTests, "Check")
    autocommit_tests = unittest.makeSuite(AutocommitTests, "Check")
    change_autocommit_tests = unittest.makeSuite(ChangeAutocommitTests, "Check")

    test_suite = unittest.TestSuite((transaction_tests, autocommit_tests,
                                    change_autocommit_tests))
    return test_suite

def main():
    runner = unittest.TextTestRunner()
    runner.run(suite())

if __name__ == "__main__":
    main()
