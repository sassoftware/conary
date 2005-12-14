#
# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import re
import MySQLdb as mysql
from base_drv import BaseDatabase, BindlessCursor, BaseSequence
import sqlerrors

class Cursor(BindlessCursor):
    driver = "mysql"
    def execute(self, sql, *params, **kw):
        if kw.has_key("start_transaction"):
            del kw["start_transaction"]
        try:
            BindlessCursor.execute(self, sql, *params, **kw)
        except mysql.IntegrityError, e:
            if e[1].startswith("Duplicate"):
                raise sqlerrors.ColumnNotUnique(e)
            raise errors.CursorError(e)
        except mysql.OperationalError, e:
            raise sqlerrors.DatabaseError(e.args[1], e.args)
        return self

# Sequence implementation for sqlite
class Sequence(BaseSequence):
    def __init__(self, db, name, cu = None):
        BaseSequence.__init__(self, db, name)
        self.cu = cu
        if cu is None:
            self.cu = db.cursor()
        if name in db.sequences:
            return
        self.cu.execute("""
        CREATE TABLE %s (
            val         INTEGER NOT NULL
        )""" % self.seqName)
        self.cu.execute("INSERT INTO %s VALUES(0)" % self.seqName)
        # refresh schema
        db.loadSchema()

    def nextval(self):
        self.cu.execute("UPDATE %s SET val=LAST_INSERT_ID(val+1)" % self.seqName)
        self.cu.execute("SELECT LAST_INSERT_ID()")
        self.__currval = self.cu.fetchone()[0]
        return self.__currval

    # Enforce garbage collection to avoid circular deps
    def __del__(self):
        # if we have used the sequence, make sure it is committed
        if self.__currval is not None:
            self.db.commit()
        self.db = self.cu = None


class Database(BaseDatabase):
    alive_check = "select version(), current_date()"
    basic_transaction = "begin"
    cursorClass = Cursor
    sequenceClass = Sequence
    driver = "mysql"

    def connect(self, **kwargs):
        assert(self.database)
        cdb = self._connectData(["user", "passwd", "host", "port", "db"])
        for x in cdb.keys()[:]:
            if cdb[x] is None:
                del cdb[x]
        if kwargs.has_key("timeout"):
            cdb["connect_time"] = kwargs["timeout"]
            del kwargs["timeout"]
        cdb.update(kwargs)
        self.dbh = mysql.connect(**cdb)
        self.dbName = cdb['db']
        cu = self.cursor()
        cu.execute("SELECT default_character_set_name "
                   "FROM INFORMATION_SCHEMA.SCHEMATA "
                   "where schema_name=?", self.dbName)
        self.characterSet = cu.fetchone()[0]
        cu.execute("set character set %s" % self.characterSet)
        self.loadSchema(cu)
        self.closed = False
        return True

    def loadSchema(self, cu = None):
        BaseDatabase.loadSchema(self)
        if cu is None:
            cu = self.cursor()
        cu.execute("""
        SELECT
            table_type as type, table_name as name,
            table_name as tname
        FROM information_schema.tables
        WHERE table_type in ('VIEW', 'BASE TABLE') AND table_schema = ?
        UNION
        SELECT DISTINCT
            'INDEX' as type, index_name as name, table_name as tname
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE table_schema = ?
        """, self.dbName, self.dbName)
        for (objType, name, tableName) in cu:
            if objType == "BASE TABLE":
                if tableName.endswith("_sequence"):
                    self.sequences.append(tableName[:-len("_sequence")])
                else:
                    self.tables.setdefault(tableName, [])
            elif objType == "VIEW":
                self.views.append(tableName)
            elif objType == "INDEX":
                assert(self.tables.has_key(tableName))
                self.tables.setdefault(tableName, []).append(name)
        if not len(self.tables):
            return self.version
        version = self.getVersion()

        return version
