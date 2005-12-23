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
import pgdb

from base_drv import BaseDatabase, BindlessCursor, BaseCursor
import sqlerrors
import sqllib

# FIXME: we should channel exceptions into generic exception classes
# common to all backends
class Cursor(BindlessCursor):
    driver = "postgresql"
    def execute(self, sql, *params, **kw):
        if kw.has_key("start_transaction"):
            del kw["start_transaction"]
        try:
            ret = BindlessCursor.execute(self, sql, *params, **kw)
        except pgdb.DatabaseError, e:
            msg = e.args[0]
            if msg.find("violates foreign key constraint") > 0:
                raise sqlerrors.ConstraintViolation(msg)
            raise sqlerrors.CursorError(msg)
        return self

class Database(BaseDatabase):
    driver = "postgresql"
    avail_check = "select count(*) from pg_tables"
    cursorClass = Cursor
    keywords = BaseDatabase.keywords
    keywords['BINARY'] = 'VARCHAR'
    keywords['BLOB'] = 'BYTEA'
    keywords['MEDIUMBLOB'] = 'BYTEA'
    keywords['PRIMARYKEY'] = 'SERIAL PRIMARY KEY'

    def connect(self, **kwargs):
        assert(self.database)
        cdb = self._connectData()
        for x in cdb.keys():
            if cdb[x] is None:
                cdb[x] = ""
        cstr = "%s:%s:%s:%s" % (cdb["host"], cdb["database"],
                                cdb["user"], cdb["password"])
        host = cdb["host"]
        if cdb["port"]:
            host ="%s:%s" % (cdb["host"], cdb["port"])
        self.dbh = pgdb.connect(cstr, host = host)
        self.loadSchema()
        self.closed = False
        return True

    def loadSchema(self):
        BaseDatabase.loadSchema(self)
        c = self.cursor()
        # get tables
        c.execute("""
        select tablename as name
        from pg_tables
        where schemaname not in ('pg_catalog', 'pg_toast',
                                 'information_schema')
        """)
        for table, in c.fetchall():
            self.tables[table] = []
        if not len(self.tables):
            return self.version
        # views
        c.execute("""
        select viewname as name
        from pg_views
        where schemaname not in ('pg_catalog', 'pg_toast',
                                 'information_schema')
        """)
        for name, in c.fetchall():
            self.views[name] = True
        # indexes
        c.execute("""
        select indexname as name, tablename as table
        from pg_indexes
        where schemaname not in ('pg_catalog', 'pg_toast',
                                 'information_schema')
        """)
        for (name, table) in c.fetchall():
            self.tables.setdefault(table, []).append(name)
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
        c.execute("""
        SELECT t.tgname, c.relname
        FROM pg_catalog.pg_trigger t, pg_class c, pg_namespace n
        WHERE t.tgrelid = c.oid AND c.relnamespace = n.oid
        AND NOT tgisconstraint
        AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
        """)
        for (name, table) in c.fetchall():
            self.triggers[name] = table
        version = self.getVersion()
        return version

    # Postgresql's trigegr syntax kind of sucks because we have to
    # create a function first and then call that function from the
    # trigger
    def trigger(self, table, column, onAction, sql = ""):
        onAction = onAction.lower()
        assert(onAction in ["insert", "update"])
        # first create the trigger function
        cu = self.dbh.cursor()
        triggerName = "%s_%s" % (table, onAction)
        funcName = "%s_func" % triggerName
        cu.execute("""
        CREATE OR REPLACE FUNCTION %s()
        RETURNS trigger
        AS $$
        BEGIN
            NEW.%s := TO_NUMBER(TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) ;
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
        return True
