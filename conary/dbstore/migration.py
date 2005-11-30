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
class SchemaMigration:
    Version = 0
    def __init__(self, db):
        self.db = db
        self.cu = db.cursor()
        self.msg = "Converting database schema to version %d..." % self.Version
        # DBSTORE: a dbstore.Database would have this done automatically
        self.version = self.__dbversion()
        
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
        ret = self.migrate()
        if ret == self.Version:
            self.__end()
        return ret
    
    def __start(self):
        self.message()

    def __end(self):
        self.cu.execute("UPDATE DatabaseVersion SET version=?", self.Version)
        self.db.commit()
        self.message("")

    def __dbversion(self):
        cu = self.db.cursor()
        # DBSTORE: migrating to dbstore will make this obsolete    
        try:
            ret = cu.execute("SELECT * FROM DatabaseVersion").next()[0]
        # DBSTORE: wire this exception through the dbstore exception handling
        except:
            return 0
        return ret
