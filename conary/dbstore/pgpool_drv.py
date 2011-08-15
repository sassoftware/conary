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
