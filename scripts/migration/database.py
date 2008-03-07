#!/usr/bin/env python2.4
#
# Copyright (c) 2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

# Cristian Gafton, <gafton@rpath.com>
# handles extracting and inserting into various backends for database migration

import sys

from conary import dbstore

from schema import getTables, getIndexes
from tablelist import TableList

# database loader that knows how to insert rows efficiently
class Loader:
    binaryCols = [
        "salt", "password", "data", "entitlement", "pgpkey",
        "stream", "fileid", "pathid", "sha1" ]
    def __init__(self, db, table, fields):
        self.db = db
        self.table = table
        self.sql = "INSERT INTO %s (%s) VALUES (%s)" % (
            table, ", ".join(fields), ",".join(["?"]*len(fields)))
        self.cu = self.db.cursor()
        # check the fields are all present in the target
        self.cu.execute("SELECT * FROM %s LIMIT 1" % (table,))
        set1 = set([x.lower() for x in fields])
        set2 = set([x.lower() for x in self.cu.fields()])
        assert (set1.difference(set2) == set()), "Fields differ from src to target: %s %s" %(set1, set2)
        self.fields = fields
        # make sure we know when to use cu.binary()
        self.funcs = [ self.__getfunc(x) for x in fields ]
    def announce(self):
        sys.stdout.write("1by1 load table: %s\r" % (self.table,))
        sys.stdout.flush()
    # fields which we should tag as binary
    def __getfunc(self, field):
        if field.lower() in self.binaryCols:
            return self.cu.binary
        return lambda a: a
    # fix up the values in a row to match the transforms
    def rowvals(self, row):
        return tuple(map(lambda (f, x): f(x), zip(self.funcs, row)))
    def insertRows(self, rows, callback=None):
        self.cu.executemany(self.sql, [self.rowvals(row) for row in rows])
        if callback:
            callback.increment(len(rows))
        return len(rows)

class PgSQLLoader(Loader):
    def __init__(self, db, table, fields):
        Loader.__init__(self, db, table, fields)
        self.__usebulk = False
        if hasattr(self.db.dbh, "bulkload"):
            self.__usebulk = True
        if not self.__usebulk:
            print "WARNING: not using bulk load, update your python-pgsql bindings!"
    def bulkInsert(self, rows, callback):
        self.db.dbh.bulkload(self.table, (self.rowvals(row) for row in rows), self.fields)
        if callback:
            callback.increment(len(rows))
        return len(rows)
    def insertRows(self, rows, callback=None):
        if self.__usebulk:
            return self.bulkInsert(rows, callback)
        else:
            return Loader.insertRows(self, rows, callback)

class Database:
    def __init__(self, driver, db, verbose=True):
        self.db = dbstore.connect(db, driver)
        self.driver = driver
        self.db.loadSchema()
        self.verbose = verbose
    def createSchema(self):
        # create the tables, avoid the indexes
        cu = self.db.cursor()
        for stmt in getTables(self.driver):
            if self.verbose:
                print stmt
            cu.execute(stmt)
        self.db.loadSchema()
    def createIndexes(self):
        cu = self.db.cursor()
        for stmt in getIndexes(self.driver):
            if self.verbose:
                print stmt
            cu.execute(stmt)
        self.db.loadSchema()
    # check self.db.tables against the TableList
    def checkTablesList(self, isSrc=True):
        #  check that we are migrating all the tables in the source
        self.db.loadSchema()
        skip = ['databaseversion', 'instructionsets', 'commitlock']
        knowns = [x.lower() for x in TableList]
        haves = [x.lower() for x in self.db.tables]
        if isSrc:
            which = "Source"
        else:
            which = "Target"
        # tableList should not have items not present in the db
        onlyKnowns = set(knowns).difference(set(haves)).difference(set(skip))
        if onlyKnowns:
            raise RuntimeError("%s schema (%s) does not have table(s) %s" %(
                which, self.driver, onlyKnowns))
        # we should not have extra tables in the source
        onlyHaves = set(haves).difference(set(knowns)).difference(set(skip))
        if onlyHaves and isSrc:
            raise RuntimeError("TableList needs to be updated to handle tables", onlyHaves)
        return True
    
    # functions for when the instance is a source
    def getCount(self, table):
        cu = self.db.cursor()
        cu.execute("select count(*) from %s" % (table,))
        return cu.fetchall()[0][0]
    def getFields(self, table):
        cu = self.db.cursor()
        cu.execute("SELECT * FROM %s LIMIT 1" % (table,))
        return [x.lower() for x in cu.fields()]
    def getTables(self):
        return [x.lower() for x in self.db.tables]
    def iterRows(self, table, fields = "*"):
        cu = self.db.itercursor()
        cu.execute("select %s from %s" % (fields, table))
        return cu
    # functions for when the instance is a target
    def prepareInsert(self, table, fields):
        return Loader(self.db, table, fields)
    def finalize(self, version):
        self.db.setVersion(version)
        self.db.commit()
    # useful shortcut
    def commit(self):
        return self.db.commit()
    def close(self):
        self.db.close()
        
class PgSQLDatabase(Database):
    def __init__(self, db, verbose=True):
        Database.__init__(self, "postgresql", db, verbose)
    def createIndexes(self):
        Database.createIndexes(self)
        # fix the primary keys
        self.fix_primary_keys()
        # update the primary key sequences for all tables
    def fix_primary_keys(self):
        cu = self.db.cursor()
        # get the name of the primary key
        cu.execute("""
        select
            t.relname as table_name,
            col.attname as column_name
        from pg_class t
        join pg_index i on t.oid = i.indrelid and i.indisprimary = true
        join pg_class ind on i.indexrelid = ind.oid
        join pg_attribute col on col.attrelid = t.oid and col.attnum = i.indkey[0]
        where i.indnatts = 1
          and pg_catalog.pg_table_is_visible(t.oid)
        """)
        for (t, col) in cu.fetchall():
            table = t.lower()
            cu.execute("select pg_catalog.pg_get_serial_sequence(?, ?)", (table, col))
            seqname = cu.fetchone()[0]
            if seqname is None:
                # this primary key does not have a sequence associated with it
                continue
            # get the max seq value
            cu.execute("select max(%s) from %s" % (col, table))
            seqval = cu.fetchone()[0]
            if not seqval:
                seqval = 1
            else:
                seqval += 1 # we need the next one in line
            # now reset the sequence for the primary key
            cu.execute("select pg_catalog.setval(?, ?, false)", (seqname, seqval))
            ret = cu.fetchone()[0]
            assert (ret == seqval)
            if self.verbose:
                print "SETVAL %s = %d (%s.%s)" % (seqname, ret, table, col)
    # functions for when the instance is a target
    def prepareInsert(self, table, fields):
        return PgSQLLoader(self.db, table, fields)
    def finalize(self, version):
        Database.finalize(self, version)
        print "VACUUM ANALYZE"
        cu = self.db.cursor()
        cu.execute("VACUUM ANALYZE")

class MySQLDatabase(Database):
    def __init__(self, db, verbose=True):
        Database.__init__(self, "mysql", db, verbose)
    # functions for when the instance is a target
    def finalize(self, version):
        Database.finalize(self, version)
        cu = self.db.cursor()
        for t in TableList:
            print "ANALYZE", t
            cu.execute("ANALYZE LOCAL TABLE %s" %(t,))

def getdb(driver, db, verbose=True):
    if driver == "postgresql":
        return PgSQLDatabase(db, verbose)
    elif driver == "mysql":
        return MySQLDatabase(db, verbose)
    return Database(driver, db, verbose)

