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

class Cursor(BindlessCursor):
    driver = "postgresql"
    def execute(self, sql, *params, **kw):
        if kw.has_key("start_transaction"):
            del kw["start_transaction"]
        try:
            ret = BindlessCursor.execute(self, sql, *params, **kw)
        except pgdb.DatabaseError, e:
            raise sqlerrors.CursorError(e)
        return ret

# FIXME: we should channel exceptions into generic exception classes
# common to all backends
class Database(BaseDatabase):
    driver = "postgresql"
    avail_check = "select count(*) from pg_tables"
    cursorClass = Cursor

    def connect(self, **kwargs):
        assert(self.database)
        cdb = self._connectData()
        for x in cdb.keys():
            if cdb[x] is None:
                cdb[x] = ""
        cstr = "%s:%s:%s:%s" % (cdb["host"], cdb["database"],
                                cdb["user"], cdb["password"])
        self.dbh = pgdb.connect(cstr)
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
        self.tables = {}.fromkeys([x[0] for x in c.fetchall()], [])
        if not len(self.tables):
            return self.version
        # views
        c.execute("""
        select viewname as name
        from pg_views
        where schemaname not in ('pg_catalog', 'pg_toast',
                                 'information_schema')
        """)
        self.views = [ x[0] for x in c.fetchall() ]
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
        self.sequences = [x[0] for x in c.fetchall()]
        version = self.getVersion()
        return version
