#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
