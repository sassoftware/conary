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
from conary_test import rephelp

from conary.local import schema


class ErrorOutputTest(rephelp.RepositoryHelper):
    def testDatabaseSchemaErrors(self):
        db = self.openDatabase()
        db.writeAccess()
        cu = db.db.db.cursor()
        cu.execute('update databaseVersion set version=10000')
        db.db.db.commit()

        try:
            db2 = self.openDatabase()
            db2.writeAccess()
        except schema.NewDatabaseSchema, err:
            assert(str(err) == '''The conary database on this system is too new.  You may have multiple versions of conary installed and be running the wrong one, or your conary may have been downgraded.  Please visit http://wiki.rpath.com for information on how to get support.''')
        else:
            assert(0)

        cu.execute('update databaseVersion set version=1')
        db.db.db.commit()

        os.chmod(self.rootDir + self.cfg.dbPath + '/conarydb', 0400)
        try:
            db2 = self.openDatabase()
            db2.writeAccess()
        except schema.OldDatabaseSchema, err:
            assert(str(err) == '''\
The Conary database on this system is too old.  It will be 
automatically converted as soon as you run Conary with 
write permissions for the database (which normally means 
as root). 
''')
        else:
            assert(0)

        os.chmod(self.rootDir + self.cfg.dbPath + '/conarydb', 0600)

        cu.execute('drop table databaseVersion')

        db.db.db.commit()
        try:
            db2 = self.openDatabase()
            db2.writeAccess()
        except schema.OldDatabaseSchema, err:
            assert(str(err) == '''\
The Conary database on this system is too old. For information on how to
convert this database, please visit http://wiki.rpath.com/ConaryConversion.''')
        else:
            assert(0)
