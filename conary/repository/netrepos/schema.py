#
# Copyright (c) 2005-2006 rPath, Inc.
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

from conary.local.schema import createDependencies, createTroveInfo, createMetadata, resetTable

VERSION = 8

def createTrigger(db, table, column = "changed"):
    retInsert = db.trigger(table, column, "INSERT")
    retUpdate = db.trigger(table, column, "UPDATE")
    return retInsert or retUpdate

def createInstances(db):
    cu = db.cursor()
    commit = False
    if "Instances" not in db.tables:
        cu.execute("""
        CREATE TABLE Instances(
            instanceId      %(PRIMARYKEY)s,
            itemId          INTEGER NOT NULL,
            versionId       INTEGER NOT NULL,
            flavorId        INTEGER NOT NULL,
            isRedirect      INTEGER NOT NULL DEFAULT 0,
            isPresent       INTEGER NOT NULL DEFAULT 0,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT Instances_itemId_fk
                FOREIGN KEY (itemId) REFERENCES Items(itemId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Instances_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Instances_flavorId_fk
                FOREIGN KEY (flavorId) REFERENCES Flavors(flavorId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""" % db.keywords)
        cu.execute(" CREATE UNIQUE INDEX InstancesIdx ON "
                   " Instances(itemId, versionId, flavorId) ")
        commit = True

    if createTrigger(db, "Instances"):
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
            flavorId        %(PRIMARYKEY)s,
            flavor          VARCHAR(767)
        )""" % db.keywords)
        cu.execute("CREATE UNIQUE INDEX FlavorsFlavorIdx ON Flavors(flavor)")
        cu.execute("INSERT INTO Flavors (flavorId, flavor) VALUES (0, 'none')")
        commit = True

    if "FlavorMap" not in db.tables:
        cu.execute("""
        CREATE TABLE FlavorMap(
            flavorId        INTEGER NOT NULL,
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
            value           INTEGER NOT NULL DEFAULT -1000000
        )""")
        cu.execute("CREATE UNIQUE INDEX FlavorScoresIdx ON "
                   "FlavorScores(request, present)")

        # don't mix schema changes w/ table population
        db.commit()
        for (request, present), value in deps.flavorScores.iteritems():
            if value is None:
                value = -1000000
            cu.execute("INSERT INTO FlavorScores (request, present, value) VALUES (?,?,?)",
                       request, present, value)
        db.commit()
        commit = False

    if not resetTable(cu, 'ffFlavor'):
        db.commit()
        cu.execute("""
        CREATE TEMPORARY TABLE
        ffFlavor(
            flavorId    INTEGER,
            base        VARCHAR(254),
            sense       INTEGER,
            flag        VARCHAR(254)
        )""")
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
            nodeId          %(PRIMARYKEY)s,
            itemId          INTEGER NOT NULL,
            branchId        INTEGER NOT NULL,
            versionId       INTEGER NOT NULL,
            timeStamps      VARCHAR(1000),
            finalTimeStamp  NUMERIC(13,3) NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT Nodes_itemId_fk
                FOREIGN KEY (itemId) REFERENCES Items(itemId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Nodes_branchId_fk
                FOREIGN KEY (branchId) REFERENCES Branches(branchId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Nodes_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""" % db.keywords)
        cu.execute("""INSERT INTO Nodes
        (nodeId, itemId, branchId, versionId, timeStamps, finalTimeStamp)
        VALUES (0, 0, 0, 0, NULL, 0.0)""")
        cu.execute("CREATE UNIQUE INDEX NodesItemBranchVersionIdx "
                   "ON Nodes(itemId, branchId, versionId)")
        cu.execute("CREATE INDEX NodesItemVersionIdx ON Nodes(itemId, versionId)")
        commit = True

    if createTrigger(db, "Nodes"):
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
            itemId          INTEGER NOT NULL,
            branchId        INTEGER NOT NULL,
            flavorId        INTEGER NOT NULL,
            versionId       INTEGER NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
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
                ON DELETE CASCADE ON UPDATE CASCADE
        )""")
        cu.execute("CREATE INDEX LatestItemIdx ON Latest(itemId)")
        cu.execute("CREATE UNIQUE INDEX LatestIdx ON "
                   "Latest(itemId, branchId, flavorId)")
        commit = True

    if createTrigger(db, "Latest"):
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
            userId          %(PRIMARYKEY)s,
            userName        VARCHAR(254) NOT NULL,
            salt            %(BINARY4)s NOT NULL,
            password        %(BINARY254)s,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0
        )""" % db.keywords)
        cu.execute("CREATE UNIQUE INDEX UsersUser_uq on Users(userName)")
        commit = True

    if createTrigger(db, "Users"):
        commit = True

    if "UserGroups" not in db.tables:
        cu.execute("""
        CREATE TABLE UserGroups (
            userGroupId     %(PRIMARYKEY)s,
            userGroup       VARCHAR(254) NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0
        )""" % db.keywords)
        cu.execute("CREATE UNIQUE INDEX UserGroupsUserGroup_uq ON "
                   "UserGroups(userGroup)")
        commit = True

    if createTrigger(db, "UserGroups"):
        commit = True

    if "UserGroupMembers" not in db.tables:
        cu.execute("""
        CREATE TABLE UserGroupMembers (
            userGroupId     INTEGER NOT NULL,
            userId          INTEGER NOT NULL,
            CONSTRAINT UserGroupMembers_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES UserGroups(userGroupId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT UserGroupMembers_userId_fk
                FOREIGN KEY (userId) REFERENCES Users(userId)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""")
        cu.execute("CREATE UNIQUE INDEX UserGroupMembers_uq ON "
                   "UserGroupMembers(userGroupId, userId)")
        cu.execute("CREATE INDEX UserGroupMembersUserIdx ON "
                   "UserGroupMembers(userId)")
        commit = True

    if "Permissions" not in db.tables:
        assert("Items" in db.tables)
        assert("Labels" in db.tables)
        cu.execute("""
        CREATE TABLE Permissions (
            permissionId    %(PRIMARYKEY)s,
            userGroupId     INTEGER NOT NULL,
            labelId         INTEGER NOT NULL,
            itemId          INTEGER NOT NULL,
            canWrite        INTEGER NOT NULL DEFAULT 0,
            capped          INTEGER NOT NULL DEFAULT 0,
            admin           INTEGER NOT NULL DEFAULT 0,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT Permissions_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES UserGroups(userGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Permissions_labelId_fk
                FOREIGN KEY (labelId) REFERENCES Labels(labelId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Permissions_itemId_fk
                FOREIGN KEY (itemid) REFERENCES Items(itemId)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""" % db.keywords)
        cu.execute("CREATE UNIQUE INDEX PermissionsIdx ON "
                   "Permissions(userGroupId, labelId, itemId)")
        commit = True

    if createTrigger(db, "Permissions"):
        commit = True

    if "UsersView" not in db.views:
        cu.execute("""
        CREATE VIEW
            UsersView AS
        SELECT
            Users.userName as userName,
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
            entGroupId      %(PRIMARYKEY)s,
            entGroup        VARCHAR(254) NOT NULL,
            userGroupId     INTEGER NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT EntitlementGroups_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES userGroups(userGroupId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""" % db.keywords)
        cu.execute("CREATE UNIQUE INDEX EntitlementGroupsEntGroupIdx ON "
                   "EntitlementGroups(entGroup)")
        commit = True

    if createTrigger(db, "EntitlementGroups"):
        commit = True

    if "EntitlementOwners" not in db.tables:
        cu.execute("""
        CREATE TABLE EntitlementOwners (
            entGroupId      INTEGER NOT NULL,
            ownerGroupId    INTEGER NOT NULL,
            CONSTRAINT EntitlementOwners_entGroupId_fk
                FOREIGN KEY (entGroupId) REFERENCES EntitlementGroups(entGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT EntitlementOwners_entOwnerId_fk
                FOREIGN KEY (ownerGroupId) REFERENCES userGroups(userGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""")
        cu.execute("CREATE UNIQUE INDEX EntitlementOwnersEntOwnerIdx ON "
                   "EntitlementOwners(entGroupId, ownerGroupId)")
        cu.execute("CREATE INDEX EntitlementOwnersOwnerIdx ON "
                   "EntitlementOwners(ownerGroupId)")
        commit = True

    if "Entitlements" not in db.tables:
        cu.execute("""
        CREATE TABLE Entitlements (
            entGroupId      INTEGER NOT NULL,
            entitlement     %(BINARY255)s NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT Entitlements_entGroupId_fk
                FOREIGN KEY (entGroupId) REFERENCES EntitlementGroups(entGroupId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""" % db.keywords)
        cu.execute("CREATE UNIQUE INDEX EntitlementsEntGroupEntitlementIdx ON "
                   "Entitlements(entGroupId, entitlement)")
        commit = True

    if createTrigger(db, "Entitlements"):
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
            keyId           %(PRIMARYKEY)s,
            userId          INTEGER NOT NULL,
            fingerprint     CHAR(40) NOT NULL,
            pgpKey          %(BLOB)s NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT PGPKeys_userId_fk
                FOREIGN KEY (userId) REFERENCES Users(userId)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""" % db.keywords)
        cu.execute("CREATE UNIQUE INDEX PGPKeysFingerprintIdx ON "
                   "PGPKeys(fingerprint)")
        commit = True
    if createTrigger(db, "PGPKeys"):
        commit = True

    if "PGPFingerprints" not in db.tables:
        cu.execute("""
        CREATE TABLE PGPFingerprints(
            keyId           INTEGER NOT NULL,
            fingerprint     CHAR(40) PRIMARY KEY,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT PGPFingerprints_keyId_fk
                FOREIGN KEY (keyId) REFERENCES PGPKeys(keyId)
                ON DELETE CASCADE ON UPDATE CASCADE
        )""" % db.keywords)
        commit = True
    if createTrigger(db, "PGPFingerprints"):
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
            streamId    %(PRIMARYKEY)s,
            fileId      %(BINARY20)s,
            stream      %(BLOB)s,
            changed     NUMERIC(14,0) NOT NULL DEFAULT 0
        )""" % db.keywords)
        # in sqlite 2.8.15, a unique here seems to cause problems
        # (as the versionId isn't unique, apparently)
        cu.execute("CREATE INDEX FileStreamsIdx ON FileStreams(fileId)")
        commit = True
    if createTrigger(db, "FileStreams"):
        commit = True

    if "TroveFiles" not in db.tables:
        cu.execute("""
        CREATE TABLE TroveFiles(
            instanceId      INTEGER NOT NULL,
            streamId        INTEGER NOT NULL,
            versionId       INTEGER NOT NULL,
            pathId          %(BINARY16)s,
            path            VARCHAR(767),
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT TroveFiles_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveFiles_streamId_fk
                FOREIGN KEY (streamId) REFERENCES FileStreams(streamId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveFiles_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""" % db.keywords)
        cu.execute("CREATE INDEX TroveFilesIdx ON TroveFiles(instanceId)")
        cu.execute("CREATE INDEX TroveFilesIdx2 ON TroveFiles(streamId)")
        commit = True
    if createTrigger(db, "TroveFiles"):
        commit = True

    if "TroveTroves" not in db.tables:
        cu.execute("""
        CREATE TABLE TroveTroves(
            instanceId      INTEGER NOT NULL,
            includedId      INTEGER NOT NULL,
            byDefault       INTEGER NOT NULL DEFAULT 0,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT TroveTroves_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveTroves_includedId_fk
                FOREIGN KEY (includedId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""")
        # This index is used to enforce that TroveTroves only contains
        # unique TroveTrove (instanceId, includedId) pairs.
        cu.execute("CREATE UNIQUE INDEX TroveTrovesInstanceIncluded_uq ON "
                   "TroveTroves(instanceId,includedId)")
        # this index is so we can quickly tell what troves are needed by another trove
        cu.execute("CREATE INDEX TroveTrovesIncludedIdx ON TroveTroves(includedId)")
        commit = True
    if createTrigger(db, "TroveTroves"):
        commit = True

    if commit:
        db.commit()

    if not resetTable(cu, 'NewFiles'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE NewFiles(
            pathId      %(BINARY16)s,
            versionId   INTEGER,
            fileId      %(BINARY20)s,
            stream      %(BLOB)s,
            path        VARCHAR(767)
        )""" % db.keywords)
        db.commit()

    if not resetTable(cu, 'NeededFlavors'):
        db.rollback()
        cu.execute("CREATE TEMPORARY TABLE NeededFlavors(flavor VARCHAR(767))")
        db.commit()

    if not resetTable(cu, 'gtl'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE gtl(
        idx             %(PRIMARYKEY)s,
        name            VARCHAR(254),
        version         VARCHAR(767),
        flavor          VARCHAR(767)
        )""" % db.keywords)
        db.commit()

    if not resetTable(cu, 'gtlInst'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE gtlInst(
        idx             %(PRIMARYKEY)s,
        instanceId      INTEGER
        )""" % db.keywords)
        db.commit()

    if not resetTable(cu, 'getFilesTbl'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE getFilesTbl(
            itemId       INTEGER PRIMARY KEY,
            fileId      %(BINARY20)s
        )""" % db.keywords)
        db.commit()

    if not resetTable(cu, 'itf'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE itf(
        item            VARCHAR(254),
        version         VARCHAR(767),
        fullVersion     VARCHAR(767)
        )""")
        db.commit()

    if not resetTable(cu, 'gtvlTbl'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE
        gtvlTbl(
            item                VARCHAR(254),
            versionSpec         VARCHAR(767),
            flavorId            INTEGER
        )""")
        cu.execute("CREATE INDEX gtblIdx on gtvlTbl(item)")
        db.commit()

    db.loadSchema()

def createInstructionSets(db):
    cu = db.cursor()
    if 'InstructionSets' not in db.tables:
        cu.execute("""
        CREATE TABLE InstructionSets(
            isnSetId        %(PRIMARYKEY)s,
            base            VARCHAR(254),
            flags           VARCHAR(254)
        )""" % db.keywords)
        db.commit()
        db.loadSchema()

def createChangeLog(db):
    commit = False
    cu = db.cursor()
    if "ChangeLogs" not in db.tables:
        cu.execute("""
            CREATE TABLE ChangeLogs(
                nodeId          INTEGER NOT NULL,
                name            VARCHAR(254),
                contact         VARCHAR(254),
                message         TEXT,
                changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
                CONSTRAINT ChangeLogs_nodeId_fk
                    FOREIGN KEY (nodeId) REFERENCES Nodes(nodeId)
                    ON DELETE CASCADE ON UPDATE CASCADE
            )""")
        cu.execute("CREATE UNIQUE INDEX ChangeLogsNodeIdx ON "
                   "ChangeLogs(nodeId)")
        cu.execute("INSERT INTO ChangeLogs (nodeId, name, contact, message) "
                   "VALUES(0, NULL, NULL, NULL)")
        commit = True
    if createTrigger(db, "ChangeLogs"):
        commit = True
    if commit:
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
        self.cu.execute("INSERT INTO Items (itemId, item) VALUES(0, 'ALL')")
        self.cu.execute("INSERT INTO Labels (labelId, label) VALUES(0, 'ALL')")

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
        cu.execute("CREATE UNIQUE INDEX PermissionsIdx ON "
                   "Permissions(userGroupId, labelId, itemId)")
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
        self.cu.execute("CREATE UNIQUE INDEX FlavorScoresIdx ON "
                        "FlavorScores(request, present)")
        # remove redundancy/rename
        self.cu.execute("DROP INDEX NodesIdx")
        self.cu.execute("DROP INDEX NodesIdx2")
        self.cu.execute("CREATE UNIQUE INDEX NodesItemBranchVersionIdx ON "
                        "Nodes(itemId, branchId, versionId)")
        self.cu.execute("CREATE INDEX NodesItemVersionIdx ON "
                        "Nodes(itemId, versionId)")
        # the views are added by the __init__ methods of their
        # respective classes
        return self.Version

class MigrateTo_6(SchemaMigration):
    Version = 6
    def migrate(self):
        from conary import trove
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
            INSERT INTO TroveInfo(instanceId, infoType, data)
            VALUES (?, ?, ?)""", instanceId,
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
        # these views will have to be recreated because of the changed column names
        if "UserPermissions" in self.db.views:
            self.cu.execute("DROP VIEW UserPermissions")
        if "UsersView" in self.db.views:
            self.cu.execute("DROP VIEW UsersView")
        # drop oldLatest - obsolete table from many migrations ago
        if "oldLatest" in self.db.tables:
            self.cu.execute("DROP TABLE oldLatest")
        # Permissions.write -> Permissions.canWrite
        # Users.user -> Users.userName
        for idx in self.db.tables["Permissions"] + self.db.tables["Users"]:
            self.cu.execute("DROP INDEX %s" % (idx,))
        self.cu.execute("ALTER TABLE Permissions RENAME TO oldPermissions")
        self.cu.execute("ALTER TABLE Users RENAME TO oldUsers")
        self.db.loadSchema()
        createUsers(self.db)
        self.cu.execute("""
        INSERT INTO Permissions
        (userGroupId, labelId, itemId, canWrite, admin)
        SELECT userGroupId, labelId, itemId, write, admin
        FROM oldPermissions
        """)
        self.cu.execute("""
        INSERT INTO Users
        (userId, userName, salt, password)
        SELECT userId, user, salt, password
        FROM oldUsers
        """)
        self.cu.execute("DROP TABLE oldPermissions")
        self.cu.execute("DROP TABLE oldUsers")
        # add the changed columns to the important tables
        # Note: Permissions and Users have been recreated, they
        # already should have triggers defined
        for table in ["Instances", "Nodes", "ChangeLogs", "Latest",
                      "UserGroups", "EntitlementGroups", "Entitlements",
                      "PGPKeys", "PGPFingerprints",
                      "TroveFiles", "TroveTroves", "FileStreams",
                      "TroveInfo", "Metadata", "MetadataItems"]:
            self.cu.execute("ALTER TABLE %s ADD COLUMN "
                            "changed NUMERIC(14,0) NOT NULL DEFAULT 0" % table)
            createTrigger(self.db, table)
        # indexes we changed
        self.cu.execute("CREATE INDEX EntitlementOwnersOwnerIdx ON "
                        "EntitlementOwners(ownerGroupId)")
        self.cu.execute("CREATE INDEX TroveInfoTypeIdx ON "
                        "TroveInfo(infoType, instanceId)")
        if "TroveInfoIdx2" in self.db.tables["TroveInfo"]:
            self.cu.execute("DROP INDEX TroveInfoIdx2")
        if "TroveTrovesInstanceIdx" in self.db.tables["TroveTroves"]:
            self.cu.execute("DROP INDEX TroveTrovesInstanceIdx")
        self.cu.execute("CREATE UNIQUE INDEX TroveTrovesInstanceIncluded_uq ON "
                        "TroveTroves(instanceId,includedId)")
        self.cu.execute("CREATE INDEX MetadataItemsIdx ON MetadataItems(metadataId)")
        self.cu.execute("DROP INDEX UserGroupMembersIdx")
        self.cu.execute("DROP INDEX UserGroupMembersIdx2")
        self.cu.execute("CREATE UNIQUE INDEX UserGroupMembers_uq ON "
                        "UserGroupMembers(userGroupId, userId)")
        self.cu.execute("CREATE INDEX UserGroupMembersUserIdx ON "
                        "UserGroupMembers(userId)")
        if "UserGroupsUserGroupIdx" in self.db.tables["UserGroups"]:
            self.cu.execute("DROP INDEX UserGroupsUserGroupIdx")
        self.cu.execute("CREATE UNIQUE INDEX UserGroupsUserGroup_uq ON "
                        "UserGroups(userGroup)")
        # done...
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
    createTrigger(db, "TroveInfo")
    createMetadata(db)
    createTrigger(db, "Metadata")
    createTrigger(db, "MetadataItems")

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
    if version == 7: MigrateTo_8(db)()

    return version

