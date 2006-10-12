#
# Copyright (c) 2005-2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import re

import MySQLdb as mysql
from MySQLdb import converters, cursors
from base_drv import BaseDatabase, BaseCursor, BaseSequence, BaseBinary
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
    keys['STRAIGHTJOIN'] = 'STRAIGHT_JOIN'
    def binaryVal(self, len):
        return "VARBINARY(%d)" % len

# using the "StoreResult" mixin automatically retrieves and flushes
# each cursor. The downside is increased memory footprint.
# (this is the default in the mysql bindings)
class StoreMySQLCursor(cursors.CursorStoreResultMixIn,
                       cursors.CursorTupleRowsMixIn,
                       cursors.BaseCursor):
    pass

# using the "UseResult" reduces the footprint of each "execute", as
# the rows are extracted one by one from the serevr side, but you have
# to make sure all cursors get properly flushed
class UseMySQLCursor(cursors.CursorUseResultMixIn,
                     cursors.CursorTupleRowsMixIn,
                     cursors.BaseCursor):
    pass

class Cursor(BaseCursor):
    driver = "mysql"
    binaryClass = BaseBinary
    MaxPacket = 1024 * 1024

    # execute with exception translation
    def _tryExecute(self, func, *params, **kw):
        try:
            ret = func(*params, **kw)
        except mysql.IntegrityError, e:
            if e[0] in (1062,):
                raise sqlerrors.ColumnNotUnique(e)
            raise sqlerrors.CursorError(e.args[1], (e,) + tuple(e.args))
        except mysql.OperationalError, e:
            if e[0] in (1216, 1217, 1451, 1452):
                raise sqlerrors.ConstraintViolation(e.args[1], e.args)
            if e[0] == 1205:
                raise sqlerrors.DatabaseLocked(e.args[1], (e,) + tuple(e.args))
            raise sqlerrors.DatabaseError(e.args[1], (e,) + tuple(e.args))
        except mysql.ProgrammingError, e:
            if e[0] == 1146:
                raise sqlerrors.InvalidTable(e)
            raise sqlerrors.CursorError(e.args[1], (e,) + tuple(e.args))
        except mysql.MySQLError, e:
            raise sqlerrors.DatabaseError(e.args[1], (e,) + tuple(e.args))
        return ret

    # edit the input query to make it python compatible
    def __mungeSQL(self, sql):
        keys = set()
        # take in a match for an :id and return a %(id)s python sub
        def __match_kw(m):
            d = m.groupdict()
            keys.add(d["kw"])
            return "%(pre)s%(s)s%%(%(kw)s)s" % d

        # handle the :id, :name type syntax
        sql = re.sub("(?i)(?P<pre>[(,<>=]|(LIKE|AND|BETWEEN|LIMIT|OFFSET)\s)(?P<s>\s*):(?P<kw>\w+)",
                     __match_kw, sql)
        # force dbi compliance here. args or kw or none, no mixes
        if len(keys):
            return (sql, tuple(keys))
        # handle the ? syntax
        sql = re.sub("(?i)(?P<pre>[(,<>=]|(LIKE|AND|BETWEEN|LIMIT|OFFSET)\s)(?P<s>\s*)[?]", "\g<pre>\g<s>%s", sql)
        return (sql, ())

    # we need to "fix" the sql code before calling out
    def execute(self, sql, *args, **kw):
        self._executeCheck(sql)
        sql, keys = self.__mungeSQL(sql)

        kw.pop("start_transaction", True)
        args, kw  = self._executeArgs(args, kw)

        # if we have args, we can not have keywords
        if len(args):
            if len(kw):
                raise sqlerrors.CursorError(
                    "Do not pass both positional and named bind arguments",
                    *args, **kw)
            ret = self._tryExecute(self._cursor.execute, sql, args)
        elif len(keys): # check that all keys used in the query appear in the kw
            if False in [kw.has_key(x) for x in keys]:
                raise CursorError(
                    "Query keys not defined in named argument dict",
                    sorted(keys), sorted(kw.keys()))
            ret = self._tryExecute(self._cursor.execute, sql, kw)
        else:
            ret = self._tryExecute(self._cursor.execute, sql)
        # FIXME: the MySQL bindings are not consistent about returning
        # the number of affected rows on all operations
        return self

    # coerce a query with parameters to the format MySQL bindings require
    def __parmsExecute(self, sql, parms):
        # MySQL bindings only accept tuples or dicts as  parameters
        if isinstance(parms, (tuple, dict)):
            return self._tryExecute(self._cursor.execute, sql, parms)
        if isinstance(parms, list):
            return self._tryExecute(self._cursor.execute, sql, tuple(parms))
        return self._tryExecute(self._cursor.execute, sql, (parms,))

    # This improves on the executemany() function defined in the MySQL
    # bindings by making sure we don't build queries that are more
    # than MaxPacket in size and that we work correctly when paramList
    # is an iterator
    def executemany(self, sql, paramList, **kw):
        insert_values = re.compile(r'\svalues\s*(\(.+\))', re.IGNORECASE)
        sql, keys = self.__mungeSQL(sql)
        kw.pop("start_transaction", True)

        m = insert_values.search(sql)
        # if this is not an insert...values() query, loop over the paramList
        if not m:
            for parms in paramList:
                ret = self.__parmsExecute(sql, parms)
            return self
        # build MySQL-optimized version of executemany for INSERT
        crtLen = startLen = m.start(1)
        valStr = sql[startLen:]
        vals = []
        for parms in paramList:
            try:
                ps = valStr % self.dbh.literal(parms)
            except TypeError, e:
                raise sqlerrors.CursorError(e.args[0], e.args)
            if crtLen + len(ps) + 1 >= self.MaxPacket:
                # need to execute with what we have
                self._tryExecute(self._cursor.execute, sql[:startLen] + ",".join(vals))
                crtLen = startLen
                vals = []
            crtLen += len(ps) + 1
            vals.append(ps)
        if len(vals):
            self._tryExecute(self._cursor.execute, sql[:startLen] + ",".join(vals))
        return self

    # "prepared" statements - we munge the SQL statement once and
    # execute multiple times
    def compile(self, sql):
        self._executeCheck(sql)
        sql, keys = self.__mungeSQL(sql.strip())
        return (sql, keys)
    def execstmt(self, (sql, keys), *args):
        if isinstance(args[0], (tuple, list)):
            ret = self._tryExecute(self._cursor.execute, sql, *args)
        else:
            ret = self._tryExecute(self._cursor.execute, sql, args)
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

class IterCursor(Cursor):
    # this will need to be provided by each separate driver
    def _getCursor(self):
        assert(self.dbh)
        return self.dbh.cursor(cursorclass = UseMySQLCursor)
    def execute(self, *args):
        while self._cursor.nextset(): pass
        return Cursor.execute(self, *args)

class Database(BaseDatabase):
    alive_check = "select version(), current_date()"
    basic_transaction = "begin"
    cursorClass = Cursor
    iterCursorClass = IterCursor
    sequenceClass = Sequence
    driver = "mysql"
    MaxPacket = 1024 * 1024

    keywords = KeywordDict()
    tempTableStorage = {}

    def _setCharSet(self, cu):
        cu.execute("SELECT default_character_set_name "
                   "FROM INFORMATION_SCHEMA.SCHEMATA "
                   "where schema_name=?", self.dbName)
        self.characterSet = cu.fetchall()[0][0]
        cu.execute("set character set %s" % self.characterSet)

    # this is used by the MySQL_specific execute_many to keep the max
    # packet sizes in check
    def _getMaxPacketSize(self, cu):
        cu.execute("show variables like 'max_allowed_packet'")
        name, size = cu.fetchall()[0]
        self.MaxPacket = size

    # need to propagate the MaxPacket value to the cursors
    def cursor(self):
        assert (self.dbh)
        ret = self.cursorClass(self.dbh)
        ret.MaxPacket = self.MaxPacket
        return ret

    def itercursor(self):
        assert (self.dbh)
        ret = self.iterCursorClass(self.dbh)
        ret.MaxPacket = self.MaxPacket
        return ret

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
        self._setCharSet(cu)
        self._getMaxPacketSize(cu)
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

    # Important: MySQL can not report back a list of temporary tables
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

    # MySQL requires us to redefine a column even if all we need is to
    # rename it...
    def renameColumn(self, table, oldName, newName):
        # avoid busywork
        if oldName.lower() == newName.lower():
            return True
        assert(self.dbh)
        cu = self.dbh.cursor()
        # first, we need to extract the old column definition
        cu.execute("SHOW FULL COLUMNS FROM %s LIKE '%s'" % (table, oldName))
        (oldName, colType, collation, null, key, default, extra, privs, comment) = \
                  cu.fetchone()
        # this is a bit tedious, but it has to be done...
        if collation is not None:
            collation = "COLLATE %s" % (collation,)
        else:
            collation = ''
        # null or not null?
        if null == "NO":
            null = "NOT NULL"
        else:
            null = ''
        if default in [None, '']:
            default = ''
        else:
            try:
                default = "DEFAULT %s" % (int(default),)
            except ValueError:
                default = "DEFAULT %s" % (self.dbh.literal(default),)
        # sanity check: do other tables link to this column via foreign keys?
        cu.execute("""
        SELECT table_name, column_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE lower(table_schema) = lower(%s)
          AND lower(referenced_table_schema) = lower(%s)
          AND lower(referenced_table_name) = lower(%s)
          AND lower(referenced_column_name) = lower(%s)
        """, (self.dbName, self.dbName, table, oldName))
        ret = cu.fetchall()
        if len(ret):
            raise sqlerrors.ConstraintViolation(
                "Column rename will invalidate FOREIGN KEY constraints",
                *tuple(ret))
        # MySQL lameness - we need to check if this column is a FK pointing out
        cu.execute("""
        SELECT DISTINCT constraint_name as name, referenced_table_name as tName
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE lower(table_schema) = lower(%s)
          AND lower(referenced_table_schema) = lower(%s)
          AND lower(table_name) = lower(%s)
          AND lower(column_name) = lower(%s)
        """, (self.dbName, self.dbName, table, oldName))
        ret = cu.fetchall()
        # a constraint - if exists - can only point to one table
        assert(len(ret) in [0,1])
        hasFK = None
        if len(ret):
            # if we have a FK constraint, we need to get all members
            # of the FKs in case we have somthing like this:
            # FOREIGN KEY (pid, pname) REFERENCES parent(id, name)
            (hasFK, fkTable) = ret[0]
            cu.execute("""
            SELECT ordinal_position as pos,
            CASE lower(column_name) WHEN lower(%s) THEN %s ELSE column_name END as name,
            referenced_column_name as fkName
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE lower(table_schema) = lower(%s)
              AND lower(referenced_table_schema) = lower(%s)
              AND lower(table_name) = lower(%s)
              AND constraint_name = %s
            ORDER BY ordinal_position
            """, (oldName, newName, self.dbName, self.dbName, table, hasFK))
            fkDef = cu.fetchall()
            # need to drop the foreign key constraint first so we can alter the table
            cu.execute("ALTER TABLE %s DROP FOREIGN KEY %s" % (table, hasFK))

        # now we can roughly rebuild the definition...
        sql = "ALTER TABLE %s CHANGE COLUMN %s %s %s %s %s %s %s" %(
            table, oldName, newName,
            colType, null, default, extra, collation)
        cu.execute(sql)
        # do we need to put back the FK constraint?
        if hasFK:
            sql = """ALTER TABLE %s ADD CONSTRAINT %s
                     FOREIGN KEY (%s) REFERENCES %s(%s)
                     ON UPDATE CASCADE""" % (
                table, hasFK, ",".join([x[1] for x in fkDef]),
                fkTable, ",".join([x[2] for x in fkDef]))
            cu.execute(sql)
        return True

    def use(self, dbName):
        cu = self.cursor()
        oldDbName = cu.execute("SELECT database()").fetchone()[0]
        self.tempTableStorage[oldDbName] = self.tempTables

        try:
            self.dbh.select_db(dbName)
        except mysql.OperationalError, e:
            if e[0] == 1049:
                raise sqlerrors.UnknownDatabase(e.args[1], e.args)
            else:
                raise
        self.dbName = dbName
        self._setCharSet(cu)
        self.loadSchema()
        self.tempTables = self.tempTableStorage.get(dbName, sqllib.CaselessDict())
        BaseDatabase.use(self, dbName)

    def analyze(self):
        self.loadSchema()
        cu = self.cursor()
        for table in self.tables:
            cu.execute("ANALYZE TABLE %s" % table)
