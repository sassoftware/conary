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
