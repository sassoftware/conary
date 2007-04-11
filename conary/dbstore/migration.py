#
# Copyright (c) 2005-2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from base_drv import BaseDatabase as Database
import sqllib, sqlerrors

# retrieve the Database version
def getDatabaseVersion(db):
    if isinstance(db, Database):
        return db.getVersion()
    cu = db.cursor()
    try:
        cu.execute("select * from DatabaseVersion")
    except:
        return sqllib.DBversion(0)
    ret = cu.fetchone_dict()
    if ret.has_key("version"):
        return sqllib.DBversion(ret["version"])
    return sqllib.DBversion(ret["major"], ret["minor"])

class SchemaMigration:
    Version = 0              # this current migration's version
    def __init__(self, db):
        self.db = db
        self.cu = db.cursor()
        self.version = db.getVersion()
        self.Version = self._dbVersion(self.Version)
        self.msg = "Converting database schema to version %s..." % self.Version
        # enforce strict pecking order on major schema revisions
        assert (self.canUpgrade())
        
    def _dbVersion(self, v):
        if isinstance(v, sqllib.DBversion):
            return v
        elif isinstance(v, int):
            return sqllib.DBversion(v)
        elif isinstance(v, tuple):
            return sqllib.DBversion(*v)
        raise RuntimeError("Invalid DBversion specification", DBversion)
    
    # likely candidates for overrides
    def canUpgrade(self):
        # comparing db version vs our Version
        if self.version.major > self.Version.major:
            return False
        if self.version.major == self.Version.major:
            return self.version.minor <= self.Version.minor
        if self.version.major == self.Version.major - 1:
            return True
        return False
    
    # "migrate" function handles major scham changes (ie, (14,7) -> (15,0)
    # for minor schema updates we will look up migrate1, migrate2, etc
    def migrate(self):
        return False
    def message(self, msg = None):
        pass

    def __migrate(self, toVer, func):
        self.message("converting from schema %s to schema %s..." % (self.version, toVer))
        if not func():
            raise sqlerrors.SchemaVersionError(
                "schema version migration failed from %s to %s" %(
                self.version, toVer), self.version, self.Version)
        self.db.setVersion(toVer)
        return toVer
    
    # XXX: need way to control only minor/with major schema updates
    def __call__(self, major=False):
        if not self.canUpgrade():
            return self.version
        # is a major schema update needed?
        if self.version.major < self.Version.major:
            if major: # major schema updates allowed
                # we can perform the major schema update
                toVer = self._dbVersion(self.Version.major)
                self.version = self.__migrate(toVer, self.migrate)
            else:
                raise sqlerrors.SchemaVersionError(
                    "major schema upgrade not allowed",
                    self.version, self.Version)
        assert(self.version.major == self.Version.major)

        # perform minor version upgrades, if needed
        while self.version.minor < self.Version.minor:
            nextmin = self.version.minor + 1
            toVer = self._dbVersion((self.version.major, nextmin))
            func = getattr(self, "migrate%d" % (nextmin,))
            self.version = self.__migrate(toVer, func)
        self.message("")
        # all done migrating
        return self.version

