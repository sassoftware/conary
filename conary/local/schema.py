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

import sys
import itertools
from conary import trove, deps, files
from conary.dbstore import idtable, migration

# Stuff related to SQL schema maintenance and migration

VERSION = 14

# Schema creation functions
def createFlavors(db):
    if "Flavors" in db.tables:
        return
    cu = db.cursor()
    f = idtable.IdTable(db, "Flavors", "flavorId", "flavor")
    cu.execute("SELECT FlavorID from Flavors")
    if cu.fetchone() == None:
        # reserve flavor 0 for "no flavor information"
        cu.execute("INSERT INTO Flavors VALUES (0, NULL)")
    db.commit()
    db.loadSchema()

def createDBTroveFiles(db):
    if "DBTroveFiles" in db.tables:
        return
    cu = db.cursor()
    cu.execute("""
    CREATE TABLE DBTroveFiles(
        streamId            INTEGER PRIMARY KEY,
        pathId              BINARY,
        versionId           INTEGER,
        path                STRING,
        fileId              BINARY,
        instanceId          INTEGER,
        isPresent           INTEGER,
        stream              BINARY
    )""")
    cu.execute("CREATE INDEX DBTroveFilesIdx ON DBTroveFiles(fileId)")
    cu.execute("CREATE INDEX DBTroveFilesInstanceIdx ON DBTroveFiles(instanceId)")
    cu.execute("CREATE INDEX DBTroveFilesPathIdx ON DBTroveFiles(path)")

    cu.execute("""
    CREATE TABLE DBFileTags(
        streamId            INTEGER,
        tagId               INTEGER
    )""")
    db.commit()
    db.loadSchema()

def createInstances(db):
    if "Instances" in db.tables:
        return
    cu = db.cursor()
    cu.execute("""
    CREATE TABLE Instances(
        instanceId      INTEGER PRIMARY KEY,
        troveName       STRING,
        versionId       INTEGER,
        flavorId        INTEGER,
        timeStamps      STRING,
        isPresent       INTEGER,
        pinned          BOOLEAN
    )""")
    cu.execute("CREATE INDEX InstancesNameIdx ON Instances(troveName)")
    cu.execute("CREATE UNIQUE INDEX InstancesIdx ON "
               "Instances(troveName, versionId, flavorId)")
    db.commit()
    db.loadSchema()

def createTroveTroves(db):
    if "TroveTroves" in db.tables:
        return
    cu = db.cursor()
    # FIXME; add foreign keys
    cu.execute("""
    CREATE TABLE TroveTroves(
        instanceId      INTEGER,
        includedId      INTEGER,
        byDefault       BOOLEAN,
        inPristine      BOOLEAN
    )""")
    # FIXME: this index is redundant. The UNIQUE below index should suffice
    cu.execute("CREATE INDEX TroveTrovesInstanceIdx ON TroveTroves(instanceId)")
    # this index is so we can quickly tell what troves are needed by another trove
    cu.execute("CREATE INDEX TroveTrovesIncludedIdx ON TroveTroves(includedId)")
    # XXX this index is used to enforce that TroveTroves only
    # contains unique TroveTrove (instanceId, includedId) pairs.
    cu.execute("CREATE UNIQUE INDEX TroveTrovesInstIncIdx ON "
               "TroveTroves(instanceId,includedId)")
    db.commit()
    db.loadSchema()

def createDepTable(cu, name, isTemp):
    if isTemp:
        tmp = "TEMPORARY"
    else:
        tmp = ""

    cu.execute("""
    CREATE %s TABLE %s(
        depId           INTEGER PRIMARY KEY,
        class           INTEGER,
        name            STRING,
        flag            STRING
    )""" % (tmp, name), start_transaction = (not isTemp))
    cu.execute("CREATE UNIQUE INDEX %sIdx ON %s(class, name, flag)" %
               (name, name), start_transaction = (not tmp))

def createRequiresTable(cu, name, isTemp):
    if isTemp:
        tmp = "TEMPORARY"
    else:
        tmp = ""
    # FIXME: add foreign keys
    cu.execute("""
    CREATE %s TABLE %s(
        instanceId      INTEGER,
        depId           INTEGER,
        depNum          INTEGER,
        depCount        INTEGER
    )""" % (tmp, name), start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx ON %s(instanceId)" % (name, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx2 ON %s(depId)" % (name, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx3 ON %s(depNum)" % (name, name),
               start_transaction = (not isTemp))

def createProvidesTable(cu, name, isTemp):
    if isTemp:
        tmp = "TEMPORARY"
    else:
        tmp = ""
    # FIXME: add foreign keys
    cu.execute("""
    CREATE %s TABLE %s(
        instanceId          INTEGER,
        depId               INTEGER
    )""" % (tmp, name), start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx ON %s(instanceId)" % (name, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx2 ON %s(depId)" % (name, name),
               start_transaction = (not isTemp))

def createDependencies(db):
    commit = False
    cu = db.cursor()
    if "Dependencies" not in db.tables:
        createDepTable(cu, "Dependencies", False)
        commit = True
    if "Requires" not in db.tables:
        createRequiresTable(cu, "Requires", False)
        commit = True
    if "Provides" not in db.tables:
        createProvidesTable(cu, "Provides", False)
        commit = True
    if commit:
        db.commit()
        db.loadSchema()

# SCHEMA Migration

# redefine to enable stdout messaging for the migration process
class SchemaMigration(migration.SchemaMigration):
    def message(self, msg = None):
        if msg is None:
            msg = self.msg
        print "\r%s\r" %(' '*len(self.msg)),
        self.msg = msg
        print msg,
        sys.stdout.flush()

class MigrateTo_5(SchemaMigration):
    Version = 5
    def check(self):
        return self.version in [2,3,4]

    def migrate(self):
        from conary.local import deptable
        class FakeTrove:
            def setRequires(self, req):
                self.r = req
            def setProvides(self, prov):
                self.p = prov
            def getRequires(self):
                return self.r
            def getProvides(self):
                return self.p
            def __init__(self):
                self.r = deps.deps.DependencySet()
                self.p = deps.deps.DependencySet()

        if self.version == 2:
            self.cu.execute(
                "ALTER TABLE DBInstances ADD COLUMN pinned BOOLEAN")

        instances = [ x[0] for x in
                      self.cu.execute("select instanceId from DBInstances") ]
        dtbl = deptable.DependencyTables(self.db)
        troves = []

        for instanceId in instances:
            trv = FakeTrove()
            dtbl.get(self.cu, trv, instanceId)
            troves.append(trv)

        self.cu.execute("delete from dependencies")
        self.cu.execute("delete from requires")
        self.cu.execute("delete from provides")
        for instanceId, trv in itertools.izip(instances, troves):
            dtbl.add(self.cu, trv, instanceId)
        return self.Version

class MigrateTo_6(SchemaMigration):
    Version = 6
    def migrate(self):
        self.cu.execute(
            "ALTER TABLE TroveTroves ADD COLUMN inPristine INTEGER")
        self.cu.execute("UPDATE TroveTroves SET inPristine=?", True)
        # erase unused versions
        self.message("Removing unused version strings...")
        self.cu.execute("""
        DELETE FROM Versions WHERE versionId IN
            ( SELECT versions.versionid
              FROM versions LEFT OUTER JOIN
              ( SELECT versionid AS usedversions FROM dbinstances
                UNION
                SELECT versionid AS usedversions FROM dbtrovefiles )
              ON usedversions = versions.versionid
              WHERE usedversions IS NULL )
         """)
        return self.Version

class MigrateTo_7(SchemaMigration):
    Version = 7
    def migrate(self):
        self.cu.execute("""
        DELETE FROM TroveTroves
        WHERE TroveTroves.ROWID in (
            SELECT Second.ROWID
            FROM TroveTroves AS First
            JOIN TroveTroves AS Second USING(instanceId, includedId)
            WHERE First.ROWID < Second.ROWID
            )""")
        self.cu.execute("CREATE UNIQUE INDEX TroveTrovesInstIncIdx ON "
                        "TroveTroves(instanceId,includedId)")
        return self.Version

class MigrateTo_8(SchemaMigration):
    Version = 8
    def migrate(self):
        # we don't alter here because lots of indices have changed
        # names; this is just easier
        self.cu.execute('DROP INDEX InstancesNameIdx')
        self.cu.execute('DROP INDEX InstancesIdx')
        createInstances(self.db)
        self.cu.execute('INSERT INTO Instances SELECT * FROM DBInstances')
        self.cu.execute('DROP TABLE DBInstances')
        createFlavors(self.db)
        self.cu.execute('INSERT INTO Flavors SELECT * FROM DBFlavors '
                        'WHERE flavor IS NOT NULL')
        self.cu.execute('DROP TABLE DBFlavors')
        return self.Version

class MigrateTo_9(SchemaMigration):
    Version = 9
    def migrate(self):
        for klass, infoType in [
            (trove.BuildDependencies, trove._TROVEINFO_TAG_BUILDDEPS),
            (trove.LoadedTroves,      trove._TROVEINFO_TAG_LOADEDTROVES) ]:
            for instanceId, data in \
                    [ x for x in self.cu.execute(
                        "select instanceId, data from TroveInfo WHERE "
                        "infoType=?", infoType) ]:
                obj = klass(data)
                f = obj.freeze()
                if f != data:
                    count += 1
                    self.cu.execute("update troveinfo set data=? where "
                                    "instanceId=? and infoType=?", f,
                                    instanceId, infoType)
                    self.cu.execute("delete from troveinfo where "
                                    "instanceId=? and infoType=?",
                                    instanceId, trove._TROVEINFO_TAG_SIGS)
        return self.Version

class MigrateTo_10(SchemaMigration):
    Version = 10
    def migrate(self):
        self.cu.execute("SELECT COUNT(*) FROM DBTroveFiles")
        total = self.cu.fetchone()[0]

        self.cu.execute("SELECT instanceId, fileId, stream FROM DBTroveFiles")
        changes = []
        changedTroves = set()
        for i, (instanceId, fileId, stream) in enumerate(self.cu):
            i += 1
            if i % 1000 == 0 or (i == total):
                self.message("Reordering streams and recalculating "
                             "fileIds... %d/%d" %(i, total))
            f = files.ThawFile(stream, fileId)
            if not f.provides() and not f.requires():
                # if there are no deps, skip
                continue
            newStream = f.freeze()
            newFileId = f.fileId()
            if newStream == stream and newFileId == fileId:
                # if the stream didn't change, skip
                continue
            changes.append((newFileId, newStream, fileId))
            changedTroves.add(instanceId)

        # make the changes
        for newFileId, newStream, fileId in changes:
            self.cu.execute(
                "UPDATE DBTroveFiles SET fileId=?, stream=? WHERE fileId=?",
                (newFileId, newStream, fileId))

        # delete signatures for the instances we changed
        for instanceId in changedTroves:
            self.cu.execute(
                "DELETE FROM troveinfo WHERE instanceId=? AND infoType=?",
                (instanceId, trove._TROVEINFO_TAG_SIGS))

        return self.Version


# convert contrib.rpath.com -> contrib.rpath.org
class MigrateTo_11(SchemaMigration):
    Version = 11
    def migrate(self):
        self.cu.execute('select count(*) from versions')
        total = self.cu.fetchone()[0]

        updates = []
        self.cu.execute("select versionid, version from versions")
        for i, (versionId, version) in enumerate(self.cu):
            self.message("Renaming contrib.rpath.com to contrib.rpath.org... "
                         "%d/%d" %(i+1, total))
            if not versionId:
                continue
            new = version.replace('contrib.rpath.com', 'contrib.rpath.org')
            if version != new:
                updates.append((versionId, new))

        for versionId, version in updates:
            self.cu.execute("update versions set version=? where versionid=?",
                            (version, versionId))
            # erase signature troveinfo since the version changed
            self.cu.execute("""
            delete from TroveInfo
            where infotype = 9
            and instanceid in (
              select instanceid
              from instances
              where instances.versionid = ? )""",
                       (versionId,))
        return self.Version

# calculate path hashes for every trove
class MigrateTo_12(SchemaMigration):
    Version = 12
    def migrate(self):
        instanceIds = [ x[0] for x in self.cu.execute(
            "select instanceId from instances") ]
        for i, instanceId in enumerate(instanceIds):
            if i % 20 == 0:
                self.message("Updating trove %d of %d" %(
                    i, len(instanceIds)))
            ph = trove.PathHashes()
            for path, in self.cu.execute(
                "select path from dbtrovefiles where instanceid=?",
                instanceId):
                ph.addPath(path)

            self.cu.execute("""
                insert into troveinfo(instanceId, infoType, data)
                    values(?, ?, ?)""", instanceId,
                    trove._TROVEINFO_TAG_PATH_HASHES, ph.freeze())
        return self.Version

class MigrateTo_13(SchemaMigration):
    Version = 13
    def migrate(self):
        self.cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                        trove._TROVEINFO_TAG_SIGS)
        self.cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                        trove._TROVEINFO_TAG_FLAGS)
        self.cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                        trove._TROVEINFO_TAG_INSTALLBUCKET)

        flags = trove.TroveFlagsStream()
        flags.isCollection(set = True)
        collectionStream = flags.freeze()
        flags.isCollection(set = False)
        notCollectionStream = flags.freeze()

        self.cu.execute("""
        INSERT INTO TroveInfo
            SELECT instanceId, ?, ? FROM Instances
            WHERE NOT (trovename LIKE '%:%' OR trovename LIKE 'fileset-%')
        """, trove._TROVEINFO_TAG_FLAGS, collectionStream)

        self.cu.execute("""
        INSERT INTO TroveInfo
            SELECT instanceId, ?, ? FROM Instances
            WHERE     (trovename LIKE '%:%' OR trovename LIKE 'fileset-%')
            """, trove._TROVEINFO_TAG_FLAGS, notCollectionStream)
        return self.Version

class MigrateTo_14(SchemaMigration):
    Version = 14
    def migrate(self):
        # we need to rerun the MigrateTo_10 migration since we missed
        # some trovefiles the first time around
        m10 = MigrateTo_10(self.db)
        m10.migrate()

        # We need to make sure that loadedTroves and buildDeps troveinfo
        # isn't included in any commponent's trove.
        self.cu.execute("""
        DELETE FROM TroveInfo
        WHERE
           infotype IN (4, 5)
        AND instanceid IN (SELECT instanceid
                           FROM Instances
                           WHERE trovename LIKE '%:%')""")
        return self.Version


def checkVersion(db):
    global VERSION
    version = db.getVersion()
    if version == VERSION:
        return version

    if version == 0:
        # assume we're setting up a new environment
        if "DatabaseVersion" not in db.tables:
            # if DatabaseVersion does not exist, but any other tables do exist,
            # then the database version is too old to deal with it
            if len(db.tables) > 0:
                raise OldDatabaseSchema
        version = db.setVersion(VERSION)

    # great candidate for some "smart" python foo...
    if version in [2,3,4]:
        version = MigrateTo_5(db)()
    if version == 5: version = MigrateTo_6(db)()
    if version == 6: version = MigrateTo_7(db)()
    if version == 7: version = MigrateTo_8(db)()
    if version == 8: version = MigrateTo_9(db)()
    if version == 9: version = MigrateTo_10(db)()
    if version == 10: version = MigrateTo_11(db)()
    if version == 11: version = MigrateTo_12(db)()
    if version == 12: version = MigrateTo_13(db)()
    if version == 13: version = MigrateTo_14(db)()

    return version

class OldDatabaseSchema(Exception):
    def __str__(self):
        return self.msg

    def __init__(self, msg = None):
        if msg:
            self.msg = msg
        else:
            self.msg = "The Conary database on this system is too old. "    \
                       "For information on how to\nconvert this database, " \
                       "please visit http://wiki.rpath.com/ConaryConversion."
