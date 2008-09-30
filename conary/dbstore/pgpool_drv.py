#
# Copyright (c) 2008 rPath, Inc.
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

from postgresql_drv import Cursor as PgCursor
from postgresql_drv import Database as PgDatabase
import sqllib

class Cursor(PgCursor):
    driver = "pgpool"

# A cursor class that wraps PostgreSQL's server side cursors
class IterCursor(Cursor):
    def _getCursor(self):
        assert(self.dbh)
        return self.dbh.itercursor()

class Database(PgDatabase):
    driver = "pgpool"
    poolmode = True
    cursorClass = Cursor
    iterCursorClass = IterCursor
           
    def connect(self, **kw):
        PgDatabase.connect(self, **kw)
        # we assume that the temptables are held by the pgpool
        # connection - retrieve that list
        c = self.cursor()
        c.execute("""
        select c.relname as tablename from pg_class c
        where c.relnamespace = pg_my_temp_schema()
          and c.relkind = 'r'::"char"
        """)
        for table, in c.fetchall():
            self.tempTables[table] = sqllib.Llist()
        return True
