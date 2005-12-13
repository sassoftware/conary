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
import MySQLdb as mysql
from base_drv import BaseDatabase, BindlessCursor
import sqlerrors

class Cursor(BindlessCursor):
    driver = "mysql"
    def execute(self, sql, *params, **kw):
        if kw.has_key("start_transaction"):
            del kw["start_transaction"]
        try:
            ret = BindlessCursor.execute(self, sql, *params, **kw)
        except mysql.IntegrityError, e:
            if e[1].startswith("Duplicate"):
                raise sqlerrors.ColumnNotUnique(e)
            raise errors.CursorError(e)
        return ret

# FIXME: we should channel exceptions into generic exception classes
# common to all backends
class Database(BaseDatabase):
    alive_check = "select version(), current_date()"
    basic_transaction = "begin"
    cursorClass = Cursor
    driver = "mysql"

    def connect(self, **kwargs):
        assert(self.database)
        cdb = self._connectData(["user", "passwd", "host", "db"])
        for x in cdb.keys()[:]:
            if cdb[x] is None:
                del cdb[x]
        self.dbh = mysql.connect(**cdb)
        self.loadSchema()
        self.closed = False
        return True

    def loadSchema(self):
        BaseDatabase.loadSchema(self)
        c = self.cursor()
        c.execute("select version()")
        version = c.fetchone()[0]
        # Basically, mysql blows at giving the user any details about the schema.
        if version < "5.0.2":
            # these old versions can only list tables. that's kind of lame.
            c.execute("show tables")
            self.tables = {}.fromkeys([x[0] for x in c.fetchall()], [])
            if version > "5":
                # starting at version 5, tables and views are listed
                # in one single output. how dumb is that?
                self.views = self.tables.keys()
        else:
            # after 5.0.2, we have a new syntax and a two column output
            c.execute("show full tables")
            for (name, nametype) in c.fetchall():
                if nametype == "BASE TABLE":
                    self.tables.setdefault(name, [])
                elif nametype == "VIEW":
                    self.views.append(name)
                else:
                    assert(nametype in ["BASE TABLE", "VIEW"])
        if not len(self.tables):
            return self.version
        for t in self.tables:
            c.execute("show index from %s" % (t,))
            self.tables[t] = [ x[2] for x in c.fetchall() ]
        version = self.schemaVersion()
        return version
