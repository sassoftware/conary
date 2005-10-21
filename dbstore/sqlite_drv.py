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

import sqlite3
from base_drv import BaseDatabase, BaseCursor

class Cursor(BaseCursor):
    pass

class Database(BaseDatabase):
    def __init__(self, db):
        BaseDatabase.__init__(self, db)
        self.type = "sqlite"
        self.avail_check = "select count(*) from sqlite_master"
        self.cursorClass = Cursor
        
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
    
