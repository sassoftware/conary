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

import os
import psycopg2
import sys
from psycopg2 import extensions as psy_ext

from conary.dbstore.base_drv import BaseDatabase, BaseCursor, BaseKeywordDict
from conary.dbstore import _mangle
from conary.dbstore import sqlerrors
from conary.dbstore import sqllib


class KeywordDict(BaseKeywordDict):
    keys = BaseKeywordDict.keys.copy()
    keys.update( {
        'PRIMARYKEY' : 'SERIAL PRIMARY KEY',
        'BLOB'       : 'BYTEA',
        'MEDIUMBLOB' : 'BYTEA',
        'PATHTYPE'   : 'BYTEA',
        'STRING'     : 'VARCHAR'
        } )

    def binaryVal(self, len):
        return "BYTEA"


class Cursor(BaseCursor):
    binaryClass = buffer
    driver = "psycopg2"
    _encodeRequired = False

    def _tryExecute(self, func, *params, **kw):
        try:
            return func(*params, **kw)
        except:
            e_type, e_value, e_tb = sys.exc_info()
            e_value = self._convertError(e_value)
            raise type(e_value), e_value, e_tb

    @staticmethod
    def _fixStatement(statement):
        return _mangle.swapPlaceholders(statement)

    @staticmethod
    def _convertError(exc_value):
        pgcode = getattr(exc_value, 'pgcode', None)
        if pgcode == '23503':
            new_type = sqlerrors.ConstraintViolation
        elif pgcode == '42P01':
            new_type = sqlerrors.InvalidTable
        elif pgcode == '23505':
            new_type = sqlerrors.ColumnNotUnique
        else:
            new_type = sqlerrors.CursorError
        new_value = new_type(str(exc_value))
        new_value.err_code = pgcode
        return new_value

    def execute(self, sql, *args, **kw):
        sql = self._fixStatement(sql)
        self._executeCheck(sql)
        kw.pop("start_transaction", True)
        args, kw  = self._executeArgs(args, kw)

        # if we have args, we can not have keywords
        if args:
            if kw:
                raise sqlerrors.CursorError(
                    "Do not pass both positional and named bind arguments",
                    args, kw)
            ret = self._tryExecute(self._cursor.execute, sql, args)
        elif kw:
            ret = self._tryExecute(self._cursor.execute, sql, kw)
        else:
            ret = self._tryExecute(self._cursor.execute, sql)

        return self

    def executemany(self, sql, argList, start_transaction=True):
        sql = self._fixStatement(sql)
        self._executeCheck(sql)
        return self._tryExecute(self._cursor.executemany, sql, argList)

    def fields(self):
        return [x[0] for x in self._cursor.description]

    def lastid(self):
        cu = self.dbh.cursor()
        cu.execute("SELECT lastval()")
        row = cu.fetchone()
        if row is None:
            return None
        else:
            return int(row[0])

    lastrowid = property(lastid)

    def _row(self, data):
        "Convert a data tuple to a C{Row} object."
        assert self._cursor
        if data is None:
            return None
        # This implementation does not request the unicode extension, but the
        # underlying connection might be shared with one that does. Callers
        # won't be expecting unicodes though so re-encode it.
        data = [self.encode(x) for x in data]
        return sqllib.Row(data, self.fields())


class Database(BaseDatabase):
    driver = "psycopg2"
    kind = "postgresql"
    alive_check = "select version() as version"
    cursorClass = Cursor
    keywords = KeywordDict()
    basic_transaction = "START TRANSACTION"
    poolmode = True

    def connect(self, **kwargs):
        assert self.database
        cdb = self._connectData()
        cdb = dict((x, y) for (x, y) in cdb.iteritems() if y is not None)
        try:
            self.dbh = psycopg2.connect(**cdb)
        except psycopg2.DatabaseError:
            raise sqlerrors.DatabaseError("Could not connect to database", cdb)
        self.tempTables = sqllib.CaselessDict()
        c = self.cursor()
        c.execute("""
        select c.relname as tablename from pg_class c
        where c.relnamespace = pg_my_temp_schema()
          and c.relkind = 'r'::"char"
        """)
        for table, in c.fetchall():
            self.tempTables[table] = sqllib.Llist()
        self.closed = False
        return True

    def close_fork(self):
        if self.dbh:
            # Close socket without notifying the server.
            os.close(self.dbh.fileno())
            self.dbh = None
        self.close()

    def loadSchema(self):
        BaseDatabase.loadSchema(self)
        c = self.cursor()
        # get tables
        c.execute("""
        select tablename as name, schemaname as schema
        from pg_tables
        where schemaname not in ('pg_catalog', 'pg_toast', 'information_schema')
        and ( schemaname !~ '^pg_temp_' OR schemaname = (pg_catalog.current_schemas(true))[1])
        """)
        for table, schema in c.fetchall():
            if schema.startswith("pg_temp"):
                self.tempTables[table] = sqllib.Llist()
            else:
                self.tables[table] = sqllib.Llist()
        if not len(self.tables):
            return self.version
        # views
        c.execute("""
        select viewname as name
        from pg_views
        where schemaname not in ('pg_catalog', 'pg_toast', 'information_schema')
        """)
        for name, in c.fetchall():
            self.views[name] = True
        # indexes
        c.execute("""
        select indexname as name, tablename as table, schemaname as schema
        from pg_indexes
        where schemaname not in ('pg_catalog', 'pg_toast', 'information_schema')
        and ( schemaname !~ '^pg_temp_' OR schemaname = (pg_catalog.current_schemas(true))[1])
        """)
        for (name, table, schema) in c.fetchall():
            if schema.startswith("pg_temp"):
                self.tempTables.setdefault(table, sqllib.Llist()).append(name)
            else:
                self.tables.setdefault(table, sqllib.Llist()).append(name)
        # sequences. I wish there was a better way...
        c.execute("""
        SELECT c.relname as name
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
        AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
        AND pg_catalog.pg_table_is_visible(c.oid)
        """)
        for name, in c.fetchall():
            self.sequences[name] = True
        # triggers
        # AWKWARD: postgres 9.0 changed tgisconstraint to tgisinternal, so we
        # have to detect which it is to maintain compatibility :(
        #   -- gxti 2010-11-01
        c.execute("""
            SELECT a.attname
            FROM pg_catalog.pg_attribute a
            LEFT JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_trigger'
            AND a.attname in ('tgisconstraint', 'tgisinternal')
            """)
        colname, = c.fetchone()

        c.execute("""
        SELECT t.tgname, c.relname
        FROM pg_catalog.pg_trigger t, pg_class c, pg_namespace n
        WHERE t.tgrelid = c.oid AND c.relnamespace = n.oid
        AND NOT t.%(colname)s
        AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
        AND ( n.nspname !~ '^pg_temp_' OR n.nspname = (pg_catalog.current_schemas(true))[1])
        """ % dict(colname=colname))
        for (name, table) in c.fetchall():
            self.triggers[name] = table
        version = self.getVersion()
        return version

    # Transaction support
    def inTransaction(self, default=None):
        """
        Return C{True} if the connection currently has an active
        transaction.
        """
        return self.dbh.status == psy_ext.STATUS_IN_TRANSACTION

    def createTrigger(self, table, column, onAction):
        onAction = onAction.lower()
        assert onAction in ('insert', 'update')

        # first create the trigger function
        triggerName = "%s_%s" % (table, onAction)
        if triggerName in self.triggers:
            return False
        funcName = "%s_func" % triggerName
        cu = self.dbh.cursor()
        cu.execute("""
        CREATE OR REPLACE FUNCTION %s()
        RETURNS trigger
        AS $$
        BEGIN
            NEW.%s := TO_NUMBER(TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS'), '99999999999999') ;
            RETURN NEW;
        END ; $$ LANGUAGE 'plpgsql';
        """ % (funcName, column))
        # now create the trigger based on the above function
        cu.execute("""
        CREATE TRIGGER %s
        BEFORE %s ON %s
        FOR EACH ROW
        EXECUTE PROCEDURE %s()
        """ % (triggerName, onAction, table, funcName))
        self.triggers[triggerName] = table
        return True

    def dropTrigger(self, table, onAction):
        onAction = onAction.lower()
        triggerName = "%s_%s" % (table, onAction)
        if triggerName not in self.triggers:
            return False
        funcName = "%s_func" % triggerName
        cu = self.dbh.cursor()
        cu.execute("DROP TRIGGER %s ON %s" % (triggerName, table))
        cu.execute("DROP FUNCTION %s()" % funcName)
        del self.triggers[triggerName]
        return True

    def getVersion(self):
        cu = self.dbh.cursor()
        cu.execute("SAVEPOINT getversion_save")
        try:
            try:
                return BaseDatabase.getVersion(self, raiseOnError=True)
            except sqlerrors.InvalidTable:
                self.version = sqllib.DBversion(0, 0)
                return self.version
        finally:
            cu.execute("ROLLBACK TO SAVEPOINT getversion_save")

    def analyze(self, table=""):
        cu = self.cursor()
        assert isinstance(table, basestring)
        cu.execute("ANALYZE " + table)

    def truncate(self, *tables):
        cu = self.cursor()
        cu.execute("TRUNCATE TABLE " + ", ".join(tables))

    def runAutoCommit(self, func, *args, **kwargs):
        """Call the given function in auto-commit mode. Needed to execute
        statements that cannot be run in a transaction, like CREATE
        DATABASE.

        WARNING: This will commit any open transaction!
        """
        old_level = self.dbh.isolation_level
        try:
            if self.inTransaction():
                self.dbh.commit()
            self.dbh.set_isolation_level(psy_ext.ISOLATION_LEVEL_AUTOCOMMIT)
            return func(*args, **kwargs)
        finally:
            self.dbh.set_isolation_level(old_level)

    # resetting the auto increment values of primary keys
    def setAutoIncrement(self, table, column, value=None):
        cu = self.cursor()
        seqName = "%s_%s_seq" % (table, column)
        usedVal = True
        if value is None:
            cu.execute("select max(%s) from %s" % (column, table))
            value = cu.fetchall()[0][0]
            if value is None:
                usedVal = False
                value = 1
            else:
                values = int(value)
        cu.execute("select setval(?, ?, ?)", (seqName, value, usedVal))
        ret = cu.fetchall()
        assert ret[0][0] == value
        return True

    def use(self, dbName, **kwargs):
        self.close()
        self.database = "/".join([self.database.rsplit("/", 1)[0], dbName])
        return self.connect(**kwargs)
