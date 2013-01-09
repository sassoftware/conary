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
    ret = cu.fetchone()
    if ret.has_key("minor"):
        return sqllib.DBversion(ret["version"], ret["minor"])
    return sqllib.DBversion(ret["version"])

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

    def __migrate(self, toVer, func, skipCommit):
        self.message("converting from schema %s to schema %s..." % (self.version, toVer))
        if not skipCommit:
            self.db.transaction()
        try:
            if not func():
                raise sqlerrors.SchemaVersionError(
                    "schema version migration failed from %s to %s" %(
                    self.version, toVer), self.version, self.Version)
            self.db.setVersion(toVer, skipCommit=True)
        except:
            if not skipCommit:
                self.db.rollback()
            raise
        else:
            if not skipCommit:
                self.db.commit()
        return toVer

    def __call__(self, skipCommit=False):
        if not self.canUpgrade():
            return self.version
        # is a major schema update needed?
        if self.version.major < self.Version.major:
            # we can perform the major schema update
            toVer = self._dbVersion(self.Version.major)
            self.version = self.__migrate(toVer, self.migrate, skipCommit)
        assert(self.version.major == self.Version.major)

        # perform minor version upgrades, if needed
        while self.version.minor < self.Version.minor:
            nextmin = self.version.minor + 1
            toVer = self._dbVersion((self.version.major, nextmin))
            func = getattr(self, "migrate%d" % (nextmin,))
            self.version = self.__migrate(toVer, func, skipCommit)
        self.message("")
        # all done migrating
        return self.version
