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

from conary.dbstore import migration, sqlerrors
from conary.lib.tracelog import logMe

from conary.local.schema import createDependencies, createTroveInfo, createMetadata

VERSION = 7

def createInstances(db):
    cu = db.cursor()
    commit = False
    if "Instances" not in db.tables:
        cu.execute("""
        CREATE TABLE Instances(
            instanceId      INTEGER PRIMARY KEY AUTO_INCREMENT,
            itemId          INTEGER,
            versionId       INTEGER,
            flavorId        INTEGER,
            isRedirect      INTEGER NOT NULL DEFAULT 0,
            isPresent       INTEGER NOT NULL DEFAULT 0,
            CONSTRAINT Instances_itemId_fk
                FOREIGN KEY (itemId) REFERENCES Items(itemId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Instances_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Instances_flavorId_fk
                FOREIGN KEY (flavorId) REFERENCES Flavors(flavorId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""")
        cu.execute(" CREATE UNIQUE INDEX InstancesIdx ON "
                   " Instances(itemId, versionId, flavorId) ")
        commit = True

    if "InstancesView" not in db.views:
        cu.execute("""
        CREATE VIEW
            InstancesView AS
        SELECT
            Instances.instanceId as instanceId,
            Items.item as item,
            Versions.version as version,
            Flavors.flavor as flavor
        FROM
            Instances
        JOIN Items on Instances.itemId = Items.itemId
        JOIN Versions on Instances.versionId = Versions.versionId
        JOIN Flavors on Instances.flavorId = Flavors.flavorId
        """)
        commit = True
    if commit:
        db.commit()
        db.loadSchema()

def createFlavors(db):
    cu = db.cursor()
    commit = False
    if "Flavors" not in db.tables:
        cu.execute("""
        CREATE TABLE Flavors(
            flavorId        INTEGER PRIMARY KEY AUTO_INCREMENT,
            flavor          VARCHAR(999)
        )""")
        cu.execute("CREATE UNIQUE INDEX FlavorsFlavorIdx ON Flavors(flavor)")
        cu.execute("INSERT INTO Flavors VALUES (0, 'none')")
        commit = True

    if "FlavorMap" not in db.tables:
        cu.execute("""
        CREATE TABLE FlavorMap(
            flavorId        INTEGER,
            base            VARCHAR(254),
            sense           INTEGER,
            flag            VARCHAR(254),
            CONSTRAINT FlavorMap_flavorId_fk
                FOREIGN KEY (flavorId) REFERENCES Flavors(flavorId)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""")
        cu.execute("CREATE INDEX FlavorMapIndex ON FlavorMap(flavorId)")
        commit = True

    if "FlavorScores" not in db.tables:
        from conary.deps import deps
        cu.execute("""
        CREATE TABLE FlavorScores(
            request         INTEGER,
            present         INTEGER,
            value           INTEGER NOT NULL DEFAULT -1000000,
            CONSTRAINT FlavorScores_request_fk
                    FOREIGN KEY (request) REFERENCES Flavors(flavorId)
                    ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT FlavorScores_present_fk
                    FOREIGN KEY (request) REFERENCES Flavors(flavorId)
                    ON DELETE CASCADE ON UPDATE CASCADE
        )""")
        cu.execute("""CREATE UNIQUE INDEX FlavorScoresIdx ON
                          FlavorScores(request, present)""")
        for (request, present), value in deps.flavorScores.iteritems():
            if value is None:
                value = -1000000
            cu.execute("INSERT INTO FlavorScores VALUES(?,?,?)",
                       request, present, value)
        commit = True
    if commit:
        db.commit()
        db.loadSchema()

def createNodes(db):
    cu = db.cursor()
    commit = False
    if 'Nodes' not in db.tables:
        cu.execute("""
        CREATE TABLE Nodes(
            nodeId          INTEGER PRIMARY KEY AUTO_INCREMENT,
            itemId          INTEGER,
            branchId        INTEGER,
            versionId       INTEGER,
            timeStamps      VARCHAR(1000),
            finalTimeStamp  NUMERIC(13,3),
            CONSTRAINT Nodes_itemId_fk
                FOREIGN KEY (itemId) REFERENCES Items(itemId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Nodes_branchId_fk
                FOREIGN KEY (branchId) REFERENCES Branches(branchId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Nodes_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Nodes_item_branch_version_uq
                UNIQUE(itemId, branchId, versionId)
        )""")
        cu.execute("""CREATE UNIQUE INDEX NodesItemBranchVersionIdx
                           ON Nodes(itemId, branchId, versionId)""")
        cu.execute("""CREATE INDEX NodesItemVersionIdx
                           ON Nodes(itemId, versionId)""")
        commit = True

    if 'NodesView' not in db.views:
        cu.execute("""
        CREATE VIEW
            NodesView AS
        SELECT
            Nodes.nodeId as nodeId,
            Items.item as item,
            Branches.branch as branch,
            Versions.version as version,
            Nodes.timestamps as timestamps,
            Nodes.finalTimestamp as finalTimestamp
        FROM
            Nodes
        JOIN Items on Nodes.itemId = Items.itemId
        JOIN Branches on Nodes.branchId = Branches.branchId
        JOIN Versions on Nodes.versionId = Versions.versionId
        """)
        commit = True
    if commit:
        db.commit()
        db.loadSchema()

def createLatest(db):
    cu = db.cursor()
    commit = False
    if 'Latest' not in db.tables:
        cu.execute("""
        CREATE TABLE Latest(
            itemId          INTEGER,
            branchId        INTEGER,
            flavorId        INTEGER,
            versionId       INTEGER,
            CONSTRAINT Latest_itemId_fk
                FOREIGN KEY (itemId) REFERENCES Items(itemId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Latest_branchId_fk
                FOREIGN KEY (branchId) REFERENCES Branches(branchId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Latest_flavorId_fk
                FOREIGN KEY (flavorId) REFERENCES Flavors(flavorId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Latest_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Latest_item_branch_flavor_uq
                UNIQUE(itemId, branchId, flavorId)
        )""")
        cu.execute("CREATE INDEX LatestItemIdx ON Latest(itemId)")
        cu.execute("CREATE UNIQUE INDEX LatestIdx ON "
                   "Latest(itemId, branchId, flavorId)")
        commit = True

    if 'LatestView' not in db.views:
        cu.execute("""
        CREATE VIEW
            LatestView AS
        SELECT
            Items.item as item,
            Branches.branch as branch,
            Versions.version as version,
            Flavors.flavor as flavor
        FROM
            Latest
        JOIN Items on Latest.itemId = Items.itemId
        JOIN Branches on Latest.branchId = Branches.branchId
        JOIN Versions on Latest.versionId = Versions.versionId
        JOIN Flavors on Latest.flavorId = Flavors.flavorId
        """)
        commit = True
    if commit:
        db.commit()
        db.loadSchema()

def createUsers(db):
    cu = db.cursor()
    commit = False

    if "Users" not in db.tables:
        cu.execute("""
        CREATE TABLE Users (
            userId          INTEGER PRIMARY KEY AUTO_INCREMENT,
            user            VARCHAR(254),
            salt            BINARY(4),
            password        VARCHAR(254),
            CONSTRAINT Users_userId_uq
                UNIQUE(user)
        )""")
        commit = True

    if "UserGroups" not in db.tables:
        cu.execute("""
        CREATE TABLE UserGroups (
            userGroupId     INTEGER PRIMARY KEY AUTO_INCREMENT,
            userGroup       VARCHAR(254),
            CONSTRAINT UserGroups_userGroup_uq
                UNIQUE(userGroup)
        )""")
        commit = True

    if "UserGroupMembers" not in db.tables:
        cu.execute("""
        CREATE TABLE UserGroupMembers (
            userGroupId     INTEGER,
            userId          INTEGER,
            CONSTRAINT UserGroupMembers_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES UserGroups(userGroupId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT UserGroupMembers_userId_fk
                FOREIGN KEY (userId) REFERENCES Users(userId)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""")
        cu.execute("""CREATE INDEX UserGroupMembersIdx ON
                                        UserGroupMembers(userGroupId)""")
        cu.execute("""CREATE INDEX UserGroupMembersIdx2 ON
                                        UserGroupMembers(userId)""")
        commit = True

    if "Permissions" not in db.tables:
        assert("Items" in db.tables)
        assert("Labels" in db.tables)
        cu.execute("""
        CREATE TABLE Permissions (
            permissionId    INTEGER PRIMARY KEY AUTO_INCREMENT,
            userGroupId     INTEGER NOT NULL,
            labelId         INTEGER NOT NULL,
            itemId          INTEGER NOT NULL,
            canWrite        INTEGER NOT NULL DEFAULT 0,
            capped          INTEGER NOT NULL DEFAULT 0,
            admin           INTEGER NOT NULL DEFAULT 0,
            CONSTRAINT Permissions_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES UserGroups(userGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Permissions_labelId_fk
                FOREIGN KEY (labelId) REFERENCES Labels(labelId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Permissions_itemId_fk
                FOREIGN KEY (itemid) REFERENCES Items(itemId)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""")
        cu.execute("""CREATE UNIQUE INDEX PermissionsIdx
                      ON Permissions(userGroupId, labelId, itemId)""")
        commit = True

    if "UsersView" not in db.views:
        cu.execute("""
        CREATE VIEW
            UsersView AS
        SELECT
            Users.user as user,
            Items.item as item,
            Labels.label as label,
            Permissions.canWrite as W,
            Permissions.admin as A,
            Permissions.capped as C
        FROM
            Users
        JOIN UserGroupMembers using (userId)
        JOIN Permissions using (userGroupId)
        JOIN Items using (itemId)
        JOIN Labels on Permissions.labelId = Labels.labelId
        """)
        commit = True

    if "EntitlementGroups" not in db.tables:
        cu.execute("""
        CREATE TABLE EntitlementGroups (
            entGroupId      INTEGER PRIMARY KEY AUTO_INCREMENT,
            entGroup        VARCHAR(254),
            userGroupId     INTEGER,
            CONSTRAINT EntitlementClasses_entitlementGroup_uq
                UNIQUE(entGroup),
            CONSTRAINT EntitlementGroups_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES userGroups(userGroupId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""")
        commit = True

    if "EntitlementOwners" not in db.tables:
        cu.execute("""
        CREATE TABLE EntitlementOwners (
            entGroupId      INTEGER,
            ownerGroupId    INTEGER,
            CONSTRAINT EntitlementOwners_entGroupId_fk
                FOREIGN KEY (entGroupId) REFERENCES
                                EntitlementGroups(entGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT EntitlementOwners_entOwnerId_fk
                FOREIGN KEY (ownerGroupId) REFERENCES
                                userGroups(groupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT EntitlementOwners_entGroupId_ownerGroupId_uq
                UNIQUE(entGroupId, ownerGroupId)
        )""")
        commit = True

    if "Entitlements" not in db.tables:
        cu.execute("""
        CREATE TABLE Entitlements (
            entGroupId      INTEGER,
            entitlement     BLOB,
            CONSTRAINT Entitlements_entGroupId_fk
                FOREIGN KEY (entGroupId) REFERENCES Flavors(entitlementGroups)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""")
        #CONSTRAINT EntitlementClasses_entitlement_uq
        #        UNIQUE(entGroupId, entitlement)

        commit = True

    if commit:
        db.commit()
        db.loadSchema()

def createPGPKeys(db):
    cu = db.cursor()
    commit = False
    if "PGPKeys" not in db.tables:
        cu.execute("""
        CREATE TABLE PGPKeys(
            keyId           INTEGER PRIMARY KEY AUTO_INCREMENT,
            userId          INTEGER,
            fingerprint     CHAR(40),
            pgpKey          BLOB,
            CONSTRAINT PGPKeys_userId_fk
                FOREIGN KEY (userId) REFERENCES Users(userId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT PGPKeys_fingerprint_uq
                UNIQUE(fingerprint)
        )""")
        commit = True
    if "PGPFingerprints" not in db.tables:
        cu.execute("""
        CREATE TABLE PGPFingerprints(
            keyId           INTEGER,
            fingerprint     CHAR(40) PRIMARY KEY,
            CONSTRAINT PGPFingerprints_keyId_fk
                FOREIGN KEY (keyId) REFERENCES PGPKeys(keyId)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""")
        commit = True
    if commit:
        db.commit()
        db.loadSchema()

def createTroves(db):
    cu = db.cursor()
    commit = False
    if 'FileStreams' not in db.tables:
        cu.execute("""
        CREATE TABLE FileStreams(
            streamId    INTEGER PRIMARY KEY AUTO_INCREMENT,
            fileId      BINARY(20),
            stream      BLOB
        )""")
        # in sqlite 2.8.15, a unique here seems to cause problems
        # (as the versionId isn't unique, apparently)
        cu.execute("""CREATE INDEX FileStreamsIdx ON FileStreams(fileId)""")
        commit = True

    if "TroveFiles" not in db.tables:
        cu.execute("""
        CREATE TABLE TroveFiles(
            instanceId      INTEGER NOT NULL,
            streamId        INTEGER NOT NULL,
            versionId       INTEGER NOT NULL,
            pathId          BINARY(16),
            path            VARCHAR(999),
            CONSTRAINT TroveFiles_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveFiles_streamId_fk
                FOREIGN KEY (streamId) REFERENCES FileStreams(streamId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveFiles_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""")
        cu.execute("CREATE INDEX TroveFilesIdx ON TroveFiles(instanceId)")
        cu.execute("CREATE INDEX TroveFilesIdx2 ON TroveFiles(streamId)")
        commit = True

    if "TroveTroves" not in db.tables:
        cu.execute("""
        CREATE TABLE TroveTroves(
            instanceId      INTEGER,
            includedId      INTEGER,
            byDefault       BOOLEAN,
            CONSTRAINT TroveTroves_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveTroves_includedId_fk
                FOREIGN KEY (includedId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveTroves_instance_included_uq
                UNIQUE(instanceId, includedId)
        )""")
        # ideally we would attempt to create a unique index on (instance, included)
        # for sqlite as well for integrity checking, but sqlite's performance will hurt
        cu.execute("CREATE INDEX TroveTrovesInstanceIdx ON TroveTroves(instanceId)")
        # this index is so we can quickly tell what troves are needed
        # by another trove
        cu.execute("CREATE INDEX TroveTrovesIncludedIdx ON TroveTroves(includedId)")
        commit = True

    if commit:
        db.commit()
        db.loadSchema()

def createAddTroveTables(cu):
    try:
        cu.execute("DELETE FROM NewFiles")
    except:
        cu.execute("""
        CREATE TEMPORARY TABLE NewFiles(
            pathId      BINARY(16),
            versionId   INTEGER,
            fileId      BINARY(20),
            stream      BLOB,
            path        VARCHAR(999)
        )""")

    try:
        cu.execute("DELETE FROM NeededFlavors")
    except:
        cu.execute("CREATE TEMPORARY TABLE NeededFlavors(flavor VARCHAR(999))")

def createInstructionSets(db):
    cu = db.cursor()
    if 'InstructionSets' not in db.tables:
        cu.execute("""
        CREATE TABLE InstructionSets(
            isnSetId        INTEGER PRIMARY KEY AUTO_INCREMENT,
            base            VARCHAR(254),
            flags           VARCHAR(254)
        )""")
        db.commit()
        db.loadSchema()

def createChangeLog(db):
    if "ChangeLogs" in db.tables:
        return
    cu = db.cursor()
    cu.execute("""
        CREATE TABLE ChangeLogs(
            nodeId          INTEGER,
            name            VARCHAR(254),
            contact         VARCHAR(254),
            message         TEXT,
            CONSTRAINT ChangeLogs_nodeId_fk
                FOREIGN KEY (nodeId) REFERENCES Nodes(nodeId)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""")
    cu.execute("CREATE UNIQUE INDEX ChangeLogsNodeIdx ON "
               "ChangeLogs(nodeId)")
    cu.execute("INSERT INTO ChangeLogs values(0, NULL, NULL, NULL)")
    db.commit()
    db.loadSchema()

def createLabelMap(db):
    if "LabelMap" in db.tables:
        return
    cu = db.cursor()
    cu.execute("""
    CREATE TABLE LabelMap(
        itemId          INTEGER NOT NULL,
        labelId         INTEGER NOT NULL,
        branchId        INTEGER NOT NULL,
        CONSTRAINT LabelMap_itemId_fk
            FOREIGN KEY (itemId) REFERENCES Items(itemId)
            ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT LabelMap_labelId_fk
            FOREIGN KEY (labelId) REFERENCES Labels(labelId)
            ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT LabelMap_branchId_fk
            FOREIGN KEY (branchId) REFERENCES Branches(branchId)
            ON DELETE CASCADE ON UPDATE CASCADE
    )""")
    # FIXME: rename indexes accordingly
    cu.execute("CREATE INDEX LabelMapItemIdx  ON LabelMap(itemId)")
    cu.execute("CREATE INDEX LabelMapLabelIdx ON LabelMap(labelId)")
    db.commit()
    db.loadSchema()

# SCHEMA Migration
class SchemaMigration(migration.SchemaMigration):
    def message(self, msg = None):
        if msg is None:
            msg = self.msg
        if msg == "":
            msg = "Finished migration to schema version %d" % (self.Version,)
        logMe(1, msg)
        self.msg = msg

# This is the update from using Null as the wildcard for
# Items/Troves and Labels to using 0/ALL
class MigrateTo_2(SchemaMigration):
    Version = 2
    def migrate(self):
        ## First insert the new Item and Label keys
        self.cu.execute("INSERT INTO Items VALUES(0, 'ALL')")
        self.cu.execute("INSERT INTO Labels VALUES(0, 'ALL')")

        ## Now replace all Nulls in the following tables with '0'
        itemTables =   ('Permissions', 'Instances', 'Latest',
                        'Metadata', 'Nodes', 'LabelMap')
        for table in itemTables:
            self.cu.execute('UPDATE %s SET itemId=0 WHERE itemId IS NULL' %
                table)
        labelTables =  ('Permissions', 'LabelMap')
        for table in labelTables:
            self.cu.execute('UPDATE %s SET labelId=0 WHERE labelId IS NULL' %
                table)

        ## Finally fix the index
        cu.execute("DROP INDEX PermissionsIdx")
        cu.execute("""CREATE UNIQUE INDEX PermissionsIdx ON
            Permissions(userGroupId, labelId, itemId)""")
        return self.Version

# add a smaller index for the Latest table
class MigrateTo_3(SchemaMigration):
    Version = 3
    def migrate(self):
        self.cu.execute("CREATE INDEX LatestItemIdx on Latest(itemId)")
        return self.Version

# FIXME: we should incorporate the script here
class MigrateTo_4(SchemaMigration):
    Version = 4
    def migrate(self):
        from conary.lib.tracelog import printErr
        printErr("""
        Conversion to version 4 requires script available
        from http://wiki.rpath.com/ConaryConversion
        """)
        return 0

class MigrateTo_5(SchemaMigration):
    Version = 5
    def migrate(self):
        # FlavorScoresIdx was not unique
        self.cu.execute("DROP INDEX FlavorScoresIdx")
        self.cu.execute("CREATE UNIQUE INDEX FlavorScoresIdx "
                   "    on FlavorScores(request, present)")
        # remove redundancy/rename
        self.cu.execute("DROP INDEX NodesIdx")
        self.cu.execute("DROP INDEX NodesIdx2")
        self.cu.execute("""CREATE UNIQUE INDEX NodesItemBranchVersionIdx
                          ON Nodes(itemId, branchId, versionId)""")
        self.cu.execute("""CREATE INDEX NodesItemVersionIdx
                          ON Nodes(itemId, versionId)""")
        # the views are added by the __init__ methods of their
        # respective classes
        return self.Version

class MigrateTo_6(SchemaMigration):
    Version = 6
    def migrate(self):
        # calculate path hashes for every trove
        instanceIds = [ x[0] for x in self.cu.execute(
                "select instanceId from instances") ]
        for i, instanceId in enumerate(instanceIds):
            ph = trove.PathHashes()
            for path, in self.cu.execute(
                "select path from trovefiles where instanceid=?",
                instanceId):
                ph.addPath(path)
            self.cu.execute("""
            insert into troveinfo(instanceId, infoType, data)
            values(?, ?, ?)""", instanceId,
                       trove._TROVEINFO_TAG_PATH_HASHES, ph.freeze())

        # add a hasTrove flag to the Items table for various
        # optimizations update the Items table
        self.cu.execute(" ALTER TABLE Items ADD COLUMN "
                        " hasTrove INTEGER NOT NULL DEFAULT 0 ")
        self.cu.execute("""
        UPDATE Items SET hasTrove = 1
        WHERE Items.itemId IN (
            SELECT Instances.itemId FROM Instances
            WHERE Instances.isPresent = 1 ) """)
        return self.Version

class MigrateTo_7(SchemaMigration):
    Version = 7
    def migrate(self):
        from conary import trove

        # erase signatures due to troveInfo storage changes
        self.cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                   trove._TROVEINFO_TAG_SIGS)
        # erase what used to be isCollection, to be replaced
        # with flags stream
        self.cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                   trove._TROVEINFO_TAG_FLAGS)
        # get rid of install buckets
        self.cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                   trove._TROVEINFO_TAG_INSTALLBUCKET)

        flags = trove.TroveFlagsStream()
        flags.isCollection(set = True)
        collectionStream = flags.freeze()
        flags.isCollection(set = False)
        notCollectionStream = flags.freeze()

        self.cu.execute("""
            INSERT INTO TroveInfo (instanceId, infoType, data)
            SELECT instanceId, ?, ?
            FROM Items JOIN Instances USING(itemId)
            WHERE NOT (item LIKE '%:%' OR item LIKE 'fileset-%')
            """, (trove._TROVEINFO_TAG_FLAGS, collectionStream))
        self.cu.execute("""
            INSERT INTO TroveInfo (instanceId, infoType, data)
            SELECT instanceId, ?, ?
            FROM Items JOIN Instances USING(itemId)
            WHERE (item LIKE '%:%' OR item LIKE 'fileset-%')
            """, (trove._TROVEINFO_TAG_FLAGS, notCollectionStream))
        return self.Version

class MigrateTo_8(SchemaMigration):
    Version = 8
    def migrate(self):
        # Permissions.write -> Permissions.canWrite
        for idx in self.db.tables["Permissions"]:
            self.cu.execute("DROP INDEX %s" % (idx,))
        self.cu.execute("ALTER TABLE Permissions RENAME TO oldPermissions")
        createUsers(db)
        self.cu.execute("""
        INSERT INTO Permissions
        (userGroupId, labelId, itemId, canWrite, admin)
        SELECT userGroupId, labelId, itemId, write, admin
        FROM oldPermissions
        """)
        self.cu.execute("DROP TABLE oldPermissions")
        # drop oldLatest
        if "oldLatest" in self.db.tables:
            self.cu.execute("DROP TABLE oldLatest")
        self.db.loadSchema()
        return self.Version

# create the server repository schema
def createSchema(db):
    # FIXME: find a better way to create the tables made by the __init__
    # methods of some of the classes used here
    from conary.repository.netrepos import items, versionops
    from conary.local import versiontable
    items.Items(db)
    versionops.BranchTable(db)
    versionops.LabelTable(db)
    versiontable.VersionTable(db)

    createLabelMap(db)

    createUsers(db)
    createPGPKeys(db)

    createFlavors(db)
    createInstances(db)
    createNodes(db)
    createChangeLog(db)
    createLatest(db)
    createInstructionSets(db)

    createTroves(db)

    createDependencies(db)
    createTroveInfo(db)
    createMetadata(db)

# schema creation/migration/maintenance entry point
def checkVersion(db):
    global VERSION
    version = db.getVersion()
    logMe(3, VERSION, version)
    if version == VERSION:
        return version

    # figure out if we're initializing a brand new database
    if version == 0:
        # assume we are setting up a brand new one
        if "DatabaseVersion" not in db.tables:
            # if DatabaseVersion does not exist, but any other tables do exist,
            # then the database version is too old to deal with it
            if len(db.tables) > 0:
                raise sqlerrors.SchemaVersionError(
                    "Can not migrate from this schema version")
        createSchema(db)
        version = db.setVersion(VERSION)

    # surely there is a more better way of handling this...
    if version == 1: MigrateTo_2(db)()
    if version == 2: MigrateTo_3(db)()
    if version == 3: MigrateTo_4(db)()
    if version == 4: MigrateTo_5(db)()
    if version == 5: MigrateTo_6(db)()
    if version == 6: MigrateTo_7(db)()

    return version

