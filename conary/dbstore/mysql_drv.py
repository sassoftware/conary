#
# Copyright (c) 2005-2006 rPath, Inc.
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
from base_drv import BaseDatabase, BindlessCursor, BaseSequence, BaseBinary
from base_drv import BaseKeywordDict
import sqlerrors

class KeywordDict(BaseKeywordDict):
    keys = BaseKeywordDict.keys
    keys['PRIMARYKEY'] = 'INTEGER PRIMARY KEY AUTO_INCREMENT'
    keys['MEDIUMBLOB'] = 'MEDIUMBLOB'

    def binaryVal(self, len):
        return "VARBINARY(%d)" % len

class Cursor(BindlessCursor):
    driver = "mysql"
    binaryClass = BaseBinary
    def execute(self, sql, *params, **kw):
        if kw.has_key("start_transaction"):
            del kw["start_transaction"]
        try:
            BindlessCursor.execute(self, sql, *params, **kw)
        except mysql.IntegrityError, e:
            if e[0] in (1062,):
                raise sqlerrors.ColumnNotUnique(e)
            raise errors.CursorError(e)
        except mysql.OperationalError, e:
            if e[0] in (1216, 1217, 1451, 1452):
                raise sqlerrors.ConstraintViolation(e.args[1], e.args)
            raise sqlerrors.DatabaseError(e.args[1], e.args)
        return self

# Sequence implementation for mysql
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
        self.cu.execute("INSERT INTO %s (val) VALUES(0)" % self.seqName)
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

    keywords = KeywordDict()

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

##     def reopen(self):
##         self.dbh.close()
##         self.connect()
##         return True

    def loadSchema(self, cu = None):
        BaseDatabase.loadSchema(self)
        if cu is None:
            cu = self.cursor()
        cu.execute("""
        SELECT
            table_type as type, table_name as name,
            table_name as tname
        FROM information_schema.tables
        WHERE table_type in ('VIEW', 'BASE TABLE')
        AND table_schema = ?
        """, self.dbName)
        ret = cu.fetchall()
        cu.execute("""
        SELECT DISTINCT
            'INDEX' as type, index_name as name, table_name as tname
        FROM information_schema.statistics
        WHERE table_schema = ?
        """, self.dbName)
        ret += cu.fetchall()
        cu.execute("""
        SELECT
            'TRIGGER' as type, trigger_name as name, event_object_table as tname
        FROM information_schema.triggers
        WHERE event_object_schema = ?
        """, self.dbName)
        ret += cu.fetchall()
        for (objType, name, tableName) in ret:
            if objType == "BASE TABLE":
                if tableName.endswith("_sequence"):
                    self.sequences.setdefault(tableName[:-len("_sequence")], None)
                else:
                    self.tables.setdefault(tableName, [])
            elif objType == "VIEW":
                self.views[name] = True
            elif objType == "INDEX":
                assert(self.tables.has_key(tableName))
                self.tables.setdefault(tableName, []).append(name)
            elif objType == "TRIGGER":
                self.triggers[name] = tableName
        if not len(self.tables):
            return self.version
        version = self.getVersion()

        return version

    # A trigger that syncs up a column to the timestamp
    def createTrigger(self, table, column, onAction, sql = ""):
        onAction = onAction.lower()
        assert(onAction in ["insert", "update"])
        # prepare the sql and the trigger name and pass it to the
        # BaseTrigger for creation
        when = "BEFORE"
        # force the current_timestamp into a numeric context
        sql = """
        SET NEW.%s = current_timestamp() + 0 ;
        %s
        """ % (column, sql)
        return BaseDatabase.trigger(self, table, when, onAction, sql)
