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

from conary import sqlite3
from base_drv import BaseDatabase, BaseCursor
import sql_error

class Cursor(BaseCursor):
    def execute(self, sql, *params, **kw):
        try:
            inAutoTrans = False
            if not self.dbh.inTransaction:
                inAutoTrans = True    
            BaseCursor.execute(self, sql, *params, **kw)
            # commit any transactions which were opened automatically
            # by the sqlite3 bindings and left hanging:
            if inAutoTrans and self.dbh.inTransaction:
                self.dbh.commit()
        except sqlite3.ProgrammingError, e:
            if inAutoTrans and self.dbh.inTransaction:
                self.dbh.rollback()
            if e.args[0].startswith("column") and e.args[0].endswith("not unique"):
                raise sql_error.ColumnNotUnique(e)
            else:
                raise
        except:
            if inAutoTrans and self.dbh.inTransaction:
                self.dbh.rollback()
            raise

class Database(BaseDatabase):
    type = "sqlite"
    alive_check = "select count(*) from sqlite_master"
    cursorClass = Cursor

    def connect(self, timeout=10000):
        assert(self.database)
        cdb = self._connectData()
        assert(cdb["database"])
        # FIXME: we should channel exceptions into generic exception
        # classes common to all backends
        self.dbh = sqlite3.connect(cdb["database"], timeout=timeout)
        self._getSchema()
        return True
    
    def _getSchema(self):
        BaseDatabase._getSchema(self)
        c = self.cursor()
        c.execute("select type, name, tbl_name from sqlite_master")
        slist = c.fetchall()
        if not len(slist):
            return self.version
        for (type, name, tbl_name) in slist:
            if type == "table":
                if name.endswith("_sequence"):
                    self.sequences.append(name[:-len("_sequence")])
                else:
                    self.tables.setdefault(name, [])
            elif type == "view":
                self.views.append(name)
            elif type == "index":
                self.tables.setdefault(tbl_name, []).append(name)
        self._getSchemaVersion()
        return self.version
