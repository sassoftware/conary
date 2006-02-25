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

from base_drv import BaseDatabase as Database

# retrieve the Database version
def getDatabaseVersion(db):
    if isinstance(db, Database):
        return db.getVersion()
    cu = db.cursor()
    try:
        cu.execute("select max(version) as version from DatabaseVersion")
    except:
        return 0
    return cu.next()[0]

class SchemaMigration:
    Version = 0
    def __init__(self, db):
        self.db = db
        self.cu = db.cursor()
        self.msg = "Converting database schema to version %d..." % self.Version
        self.version = db.getVersion()

    # likely candidates for overrides
    def check(self):
        return self.version == self.Version - 1
    def migrate(self):
        pass
    def message(self, msg = None):
        pass

    def __call__(self):
        if not self.check():
            return self.version
        self.__start()
        self.version = self.migrate()
        if self.version == self.Version:
            self.__end()
        return self.version

    def __start(self):
        self.message()

    def __end(self):
        self.db.setVersion(self.Version)
        self.message("")

