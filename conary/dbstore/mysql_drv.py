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
from MySQLdb import converters
from base_drv import BaseDatabase, BindlessCursor, BaseSequence, BaseBinary
from base_drv import BaseKeywordDict
import sqlerrors, sqllib

# modify the default conversion dictionary
conversions = converters.conversions.copy()
# don't convert blobs to arrays
del conversions[mysql.constants.FIELD_TYPE.BLOB]
# handle NEWDECIMAL (this needs to be fixed in MySQL-python)
MYSQL_TYPE_NEWDECIMAL = 246
conversions[MYSQL_TYPE_NEWDECIMAL] = float

class KeywordDict(BaseKeywordDict):
    keys = BaseKeywordDict.keys.copy()
    keys['PRIMARYKEY'] = 'INTEGER PRIMARY KEY AUTO_INCREMENT'
    keys['MEDIUMBLOB'] = 'MEDIUMBLOB'
    keys['TABLEOPTS'] = 'DEFAULT CHARACTER SET latin1 COLLATE latin1_bin'
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
            raise errors.CursorError(e.args[1], ("IntegrityError",) + tuple(e.args))
        except mysql.OperationalError, e:
            if e[0] in (1216, 1217, 1451, 1452):
                raise sqlerrors.ConstraintViolation(e.args[1], e.args)
            raise sqlerrors.DatabaseError(e.args[1], ("OperationalError",) + tuple(e.args))
        except mysql.ProgrammingError, e:
            if e[0] == 1146:
                raise sqlerrors.InvalidTable(e)
            raise sqlerrorrs.CursorError(e.args[1], ("ProgrammingError",) + tuple(e.args))
        except mysql.MySQLError, e:
            raise sqlerrors.DatabaseError(e.args[1], ("MySQLError",) + tuple(e.args))
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
        cdb["conv"] = conversions
        cdb.update(kwargs)
        try:
            self.dbh = mysql.connect(**cdb)
        except mysql.MySQLError, e:
            raise sqlerrors.DatabaseError(e.args[1], e.args)
        self.dbName = cdb['db']
        cu = self.cursor()
        cu.execute("SELECT default_character_set_name "
                   "FROM INFORMATION_SCHEMA.SCHEMATA "
                   "where schema_name=?", self.dbName)
        self.characterSet = cu.fetchone()[0]
        cu.execute("set character set %s" % self.characterSet)
        # reset the tempTables since we just lost them because of the (re)connect
        self.tempTables = sqllib.CaselessDict()
        self.closed = False
        return True

    def reopen(self):
        # make sure the connection is still valid by attempting a
        # ping.  If an exception happens, reconnect.
        try:
            self.dbh.ping()
        except mysql.MySQLError:
            return self.connect()
        return False

    # Important: MySQL can not report back a list of temporray tables
    # created in the current connection, therefore the self.tempTables
    # is managed separately outside of the loadSchema() calls.
    def loadSchema(self, cu = None):
        BaseDatabase.loadSchema(self)
        if cu is None:
            cu = self.cursor()
        cu.execute("SHOW FULL TABLES")
        for (name, objType) in cu:
            if objType == "BASE TABLE":
                if name.endswith("_sequence"):
                    seqName = name[:-len("_sequence")]
                    self.sequences[seqName] = True
                else:
                    self.tables[name] = []
            elif objType == "VIEW":
                self.views[name] = True
        for tableName in self.tables.keys():
            cu.execute("SHOW INDEX FROM %s" % tableName)
            self.tables[tableName] = [ x[2] for x in cu ]
        cu.execute("SHOW TRIGGERS")
        for row in cu:
            (name, event, tableName) = row[:3]
            self.triggers[name] = tableName
        if not len(self.tables):
            return self.version
        version = self.getVersion()
        return version

    # A trigger that syncs up a column to the timestamp
    def createTrigger(self, table, column, onAction, pinned = False):
        onAction = onAction.lower()
        assert(onAction in ["insert", "update"])
        # prepare the sql and the trigger name and pass it to the
        # BaseTrigger for creation
        when = "BEFORE"
        # force the current_timestamp into a numeric context
        if pinned:
            sql = """
            SET NEW.%s = OLD.%s ;
            """ % (column, column)
        else:
            sql = """
            SET NEW.%s = current_timestamp() + 0 ;
            """ % (column,)
        return BaseDatabase.createTrigger(self, table, when, onAction, sql)

    # MySQL uses its own "special" syntax for dropping indexes....
    def dropIndex(self, table, name):
        if name not in self.tables[table]:
            return False
        sql = "ALTER TABLE %s DROP INDEX %s" % (table, name)
        cu = self.dbh.cursor()
        cu.execute(sql)
        self.tables[table].remove(name)
        return True

    def use(self, dbName):
        try:
            self.dbh.select_db(dbName)
        except mysql.OperationalError, e:
            if e[0] == 1049:
                raise sqlerrors.UnknownDatabase(e.args[1], e.args)
            else:
                raise

        self.loadSchema()
        self.tempTables = sqllib.CaselessDict()
        BaseDatabase.use(self, dbName)
