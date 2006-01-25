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

from conary.dbstore import migration, sqlerrors, idtable
from conary.lib.tracelog import logMe
from conary.local.schema import createDependencies, setupTempDepTables

TROVE_TROVES_BYDEFAULT = 1 << 0
TROVE_TROVES_WEAKREF   = 1 << 1

VERSION = 13

def createTrigger(db, table, column = "changed", pinned = False):
    retInsert = db.createTrigger(table, column, "INSERT")
    retUpdate = db.createTrigger(table, column, "UPDATE", pinned=pinned)
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
            clonedFromId    INTEGER,
            isRedirect      INTEGER NOT NULL DEFAULT 0,
            isPresent       INTEGER NOT NULL DEFAULT 0,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT Instances_itemId_fk
                FOREIGN KEY (itemId) REFERENCES Items(itemId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Instances_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Instances_flavorId_fk
                FOREIGN KEY (flavorId) REFERENCES Flavors(flavorId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Instances_clonedfromid_fk
                FOREIGN KEY (clonedFromId) REFERENCES Versions(versionId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Instances"] = []
        commit = True
    db.createIndex("Instances", "InstancesIdx",
                   "itemId, versionId, flavorId",
                   unique = True)
    db.createIndex("Instances", "InstancesChangedIdx",
                   "changed, instanceId")
    db.createIndex("Instances", "InstancesClonedFromIdx",
                   "clonedFromId, instanceId")
    if createTrigger(db, "Instances", pinned = True):
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
        ) %(TABLEOPTS)s""" % db.keywords)
        cu.execute("INSERT INTO Flavors (flavorId, flavor) VALUES (0, 'none')")
        db.tables["Flavors"] = []
        commit = True
    db.createIndex("Flavors", "FlavorsFlavorIdx", "flavor", unique = True)

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
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["FlavorMap"] = []
        commit = True
    db.createIndex("FlavorMap", "FlavorMapIndex", "flavorId")

    if "FlavorScores" not in db.tables:
        from conary.deps import deps
        cu.execute("""
        CREATE TABLE FlavorScores(
            request         INTEGER,
            present         INTEGER,
            value           INTEGER NOT NULL DEFAULT -1000000
        )  %(TABLEOPTS)s""" % db.keywords)
        db.tables["FlavorScores"] = []
        for (request, present), value in deps.flavorScores.iteritems():
            if value is None:
                value = -1000000
            cu.execute("INSERT INTO FlavorScores (request, present, value) VALUES (?,?,?)",
                       request, present, value)
        commit = True
    db.createIndex("FlavorScores", "FlavorScoresIdx", "request, present", unique = True)
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
            sourceItemId    INTEGER,
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
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Nodes_sourceItem_fk
                FOREIGN KEY (sourceItemId) REFERENCES Items(itemId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Nodes"] = []
        cu.execute("""INSERT INTO Nodes
        (nodeId, itemId, branchId, versionId, timeStamps, finalTimeStamp)
        VALUES (0, 0, 0, 0, NULL, 0.0)""")
        commit = True
    db.createIndex("Nodes", "NodesItemBranchVersionIdx",
                   "itemId, branchId, versionId",
                   unique = True)
    db.createIndex("Nodes", "NodesItemVersionIdx", "itemId, versionId")
    db.createIndex("Nodes", "NodesSourceItemIdx", "sourceItemId, branchId")
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
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Latest"] = []
        commit = True
    db.createIndex("Latest", "LatestIdx", "itemId, branchId, flavorId",
                   unique = True)
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
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Users"] = []
        commit = True
    db.createIndex("Users", "UsersUser_uq", "userName", unique = True)
    if createTrigger(db, "Users"):
        commit = True

    if "UserGroups" not in db.tables:
        cu.execute("""
        CREATE TABLE UserGroups (
            userGroupId     %(PRIMARYKEY)s,
            userGroup       VARCHAR(254) NOT NULL,
            canMirror       INTEGER NOT NULL DEFAULT 0,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["UserGroups"] = []
        commit = True
    db.createIndex("UserGroups", "UserGroupsUserGroup_uq", "userGroup",
                   unique = True)
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
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["UserGroupMembers"] = []
        commit = True
    db.createIndex("UserGroupMembers", "UserGroupMembers_uq",
                   "userGroupId, userId", unique = True)
    db.createIndex("UserGroupMembers", "UserGroupMembersUserIdx",
                   "userId")

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
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Permissions"] = []
        commit = True
    db.createIndex("Permissions", "PermissionsIdx",
                   "userGroupId, labelId, itemId", unique = True)
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
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["EntitlementGroups"] = []
        commit = True
    db.createIndex("EntitlementGroups", "EntitlementGroupsEntGroupIdx",
                   "entGroup", unique = True)
    db.createIndex("EntitlementGroups", "EntitlementGroupsUGIdx",
                   "userGroupId")
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
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["EntitlementOwners"] = []
        commit = True
    db.createIndex("EntitlementOwners", "EntitlementOwnersEntOwnerIdx",
                   "entGroupId, ownerGroupId", unique = True)
    db.createIndex("EntitlementOwners", "EntitlementOwnersOwnerIdx",
                   "ownerGroupId")

    if "Entitlements" not in db.tables:
        cu.execute("""
        CREATE TABLE Entitlements(
            entGroupId      INTEGER NOT NULL,
            entitlement     %(BINARY255)s NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT Entitlements_entGroupId_fk
                FOREIGN KEY (entGroupId) REFERENCES EntitlementGroups(entGroupId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Entitlements"] = []
        commit = True
    db.createIndex("Entitlements", "EntitlementsEntGroupEntitlementIdx",
                   "entGroupId, entitlement", unique = True)
    if createTrigger(db, "Entitlements"):
        commit = True

    if commit:
        db.commit()
        db.loadSchema()

def createPGPKeys(db):
    cu = db.cursor()
    commit = False
    if "PGPKeys" not in db.tables:
        # userId can be null (and hence so not in the usertable) when pgp
        # keys are imported by mirrors or proxies
        cu.execute("""
        CREATE TABLE PGPKeys(
            keyId           %(PRIMARYKEY)s,
            userId          INTEGER,
            fingerprint     CHAR(40) NOT NULL,
            pgpKey          %(BLOB)s NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT PGPKeys_userId_fk
                FOREIGN KEY (userId) REFERENCES Users(userId)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["PGPKeys"] = []
        commit = True
    db.createIndex("PGPKeys", "PGPKeysFingerprintIdx",
                   "fingerprint", unique = True)
    db.createIndex("PGPKeys", "PGPKeysUserIdx",
                   "userId")
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
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["PGPFingerprints"] = []
        commit = True
    db.createIndex("PGPFingerprints", "PGPFingerprintsKeyIdx",
                   "keyId, fingerprint", unique = True)
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
            stream      %(MEDIUMBLOB)s,
            changed     NUMERIC(14,0) NOT NULL DEFAULT 0
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["FileStreams"] = []
        commit = True
    db.createIndex("FileStreams", "FileStreamsIdx",
                   "fileId", unique = True)
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
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["TroveFiles"] = []
        commit = True
    # FIXME: rename these indexes
    db.createIndex("TroveFiles", "TroveFilesIdx", "instanceId")
    db.createIndex("TroveFiles", "TroveFilesIdx2", "streamId")
    if createTrigger(db, "TroveFiles"):
        commit = True

    if "TroveTroves" not in db.tables:
        cu.execute("""
        CREATE TABLE TroveTroves(
            instanceId      INTEGER NOT NULL,
            includedId      INTEGER NOT NULL,
            flags           INTEGER NOT NULL DEFAULT 0,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT TroveTroves_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveTroves_includedId_fk
                FOREIGN KEY (includedId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["TroveTroves"] = []
        commit = True
    db.createIndex("TroveTroves", "TroveTrovesInstanceIncluded_uq",
                   "instanceId,includedId", unique = True)
    db.createIndex("TroveTroves", "TroveTrovesIncludedIdx", "includedId")
    if createTrigger(db, "TroveTroves"):
        commit = True

    if "TroveInfo" not in db.tables:
        cu.execute("""
        CREATE TABLE TroveInfo(
            instanceId      INTEGER NOT NULL,
            infoType        INTEGER NOT NULL,
            data            %(MEDIUMBLOB)s,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT TroveInfo_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["TroveInfo"] = []
        commit = True
    db.createIndex("TroveInfo", "TroveInfoIdx", "instanceId")
    db.createIndex("TroveInfo", "TroveInfoTypeIdx", "infoType, instanceId",
                   unique = True)
    if createTrigger(db, "TroveInfo"):
        commit = True

    db.loadSchema()

def createMetadata(db):
    commit = False
    cu = db.cursor()
    if 'Metadata' not in db.tables:
        cu.execute("""
        CREATE TABLE Metadata(
            metadataId          %(PRIMARYKEY)s,
            itemId              INTEGER NOT NULL,
            versionId           INTEGER NOT NULL,
            branchId            INTEGER NOT NULL,
            timeStamp           NUMERIC(13,3) NOT NULL,
            changed             NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT Metadata_itemId_fk
                FOREIGN KEY (itemId) REFERENCES Items(itemId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Metadata_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT Metadata_branchId_fk
                FOREIGN KEY (branchId) REFERENCES Branches(branchId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Metadata"] = []
        commit = True
    if createTrigger(db, "Metadata"):
        commit = True
    if 'MetadataItems' not in db.tables:
        cu.execute("""
        CREATE TABLE MetadataItems(
            metadataId      INTEGER NOT NULL,
            class           INTEGER NOT NULL,
            data            TEXT NOT NULL,
            language        VARCHAR(254) NOT NULL DEFAULT 'C',
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT MetadataItems_metadataId_fk
                FOREIGN KEY (metadataId) REFERENCES Metadata(metadataId)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["MetadataItems"] = []
        commit = True
    db.createIndex("MetadataItems", "MetadataItemsIdx", "metadataId")
    if createTrigger(db, "MetadataItems"):
        commit = True

    if commit:
        db.commit()
        db.loadSchema()

def createMirrorTracking(db):
    cu = db.cursor()
    if 'LatestMirror' not in db.tables:
        cu.execute("""
        CREATE TABLE LatestMirror(
            host            VARCHAR(254),
            mark            NUMERIC(14,0) NOT NULL
        ) %(TABLEOPTS)s""" % db.keywords)
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
            ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["ChangeLogs"] = []
        cu.execute("INSERT INTO ChangeLogs (nodeId, name, contact, message) "
                   "VALUES(0, NULL, NULL, NULL)")
        commit = True
    db.createIndex("ChangeLogs", "ChangeLogsNodeIdx", "nodeId",
                   unique = True)
    if createTrigger(db, "ChangeLogs"):
        commit = True

    if commit:
        db.commit()
        db.loadSchema()

def createLabelMap(db):
    commit = False
    cu = db.cursor()
    if "LabelMap" not in db.tables:
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
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["LabelMap"] = []
        commit = True
    db.createIndex("LabelMap", "LabelMapItemIdx", "itemId")
    db.createIndex("LabelMap", "LabelMapLabelIdx", "labelId")
    if commit:
        db.commit()
        db.loadSchema()

def createIdTables(db):
    commit = False
    cu = db.cursor()
    if idtable.createIdTable(db, "Branches", "branchId", "branch"):
        cu.execute("INSERT INTO Branches (branchId, branch) VALUES (0, NULL)")
        commit = True
    if idtable.createIdTable(db, "Labels", "labelId", "label"):
        cu.execute("INSERT INTO Labels (labelId, label) VALUES (0, 'ALL')")
        commit = True
    if idtable.createIdTable(db, "Versions", "versionId", "version"):
        cu.execute("INSERT INTO Versions (versionId, version) VALUES (0, NULL)")
        commit = True
    if "Items" not in db.tables:
        cu.execute("""
        CREATE TABLE Items(
            itemId      %(PRIMARYKEY)s,
            item        VARCHAR(254),
            hasTrove    INTEGER NOT NULL DEFAULT 0
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Items"] = []
        cu.execute("INSERT INTO Items (itemId, item) VALUES (0, 'ALL')")
        commit = True
    db.createIndex("Items", "Items_uq", "item", unique = True)
    if commit:
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
        # create the OpenPGP tables
        createPGPKeys(self.db)

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
        self.db.dropIndex("Permissions", "PermissionsIdx")
        self.db.createIndex("Permissions", "PermissionsIdx",
                            "userGroupId, labelId, itemId", unique = True)
        return self.Version

# add a smaller index for the Latest table
class MigrateTo_3(SchemaMigration):
    Version = 3
    def migrate(self):
        self.db.createIndex("Latest", "LatestItemIdx", "itemId")
        return self.Version

class MigrateTo_4(SchemaMigration):
    Version = 4
    def migrate(self):
        import itertools
        from conary.local import deptable
        from conary.deps import deps

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
                self.r = deps.DependencySet()
                self.p = deps.DependencySet()

        instances = [ x[0] for x in
                      self.cu.execute("select instanceId from Instances") ]
        dtbl = deptable.DependencyTables(self.db)
        createDependencies(self.db)
        setupTempDepTables(self.db)
        troves = []

        logMe(1, 'Reading %d instances' % len(instances))
        for i, instanceId in enumerate(instances):
            logMe(3, "Reading %d of %d..." % (i + 1, len(instances)))

            trv = FakeTrove()
            dtbl.get(self.cu, trv, instanceId)
            troves.append(trv)

        self.cu.execute("delete from dependencies")
        self.cu.execute("delete from requires")
        self.cu.execute("delete from provides")

        logMe(1, 'Reading %d instances' % len(instances))
        for i, (instanceId, trv) in enumerate(itertools.izip(instances, troves)):
            logMe(3, 'Writing %d of %d...' % (i + 1, len(instances)))
            dtbl.add(self.cu, trv, instanceId)

        return self.Version

class MigrateTo_5(SchemaMigration):
    Version = 5
    def migrate(self):
        # FlavorScoresIdx was not unique
        self.db.dropIndex("FlavorScores", "FlavorScoresIdx")
        self.db.createIndex("FlavorScores", "FlavorScoresIdx", "request, present",
                            unique = True)
        # remove redundancy/rename
        self.db.dropIndex("Nodes", "NodesIdx")
        self.db.dropIndex("Nodes", "NodesIdx2")
        self.db.createIndex("Nodes", "NodesItemBranchVersionIdx",
                            "itemId, branchId, versionId", unique = True)
        self.db.createIndex("Nodes", "NodesItemVersionIdx", "itemId, versionId")
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
        # we have to deal with conflicts over trigger names, index names and constraint names.
        # since these are smallish tables, we can afford to take the "easy way out"
        self.cu.execute("CREATE TABLE oldUsers AS SELECT * FROM Users")
        self.cu.execute("DROP TABLE Users")
        self.cu.execute("CREATE TABLE oldPermissions AS SELECT * FROM Permissions")
        self.cu.execute("DROP TABLE Permissions")
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

            try:
                self.cu.execute("ALTER TABLE %s ADD COLUMN "
                                "changed NUMERIC(14,0) NOT NULL DEFAULT 0" % table)
                logMe(3, "add changed column and triggers to", table)
            except sqlerrors.DuplicateColumnName:
                # the column already exists, probably because we created
                # a brand new table.  Then it would use the already-current
                # schema
                pass
            createTrigger(self.db, table)
        # indexes we changed
        self.db.dropIndex("TroveInfo", "TroveInfoIdx2")
        self.db.dropIndex("TroveTroves", "TroveInfoIdx2")
        self.db.dropIndex("UserGroupMembers", "UserGroupMembersIdx")
        self.db.dropIndex("UserGroupMembers", "UserGroupMembersIdx2")
        self.db.dropIndex("UserGroups", "UserGroupsUserGroupIdx")
        self.db.dropIndex("Latest", "LatestItemIdx")
        # done...
        self.db.loadSchema()
        return self.Version

class MigrateTo_9(SchemaMigration):
    Version = 9
    def migrate(self):
        # UserGroups.canMirror
        self.cu.execute("ALTER TABLE UserGroups ADD COLUMN "
                        "canMirror INTEGER NOT NULL DEFAULT 0")
        # change the byDefault column to flags in TroveTroves
        # create the correct table under a new name, move the data over, drop the old one, rename
        self.cu.execute("""
        CREATE TABLE TroveTroves2(
            instanceId      INTEGER NOT NULL,
            includedId      INTEGER NOT NULL,
            flags           INTEGER NOT NULL DEFAULT 0,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT TroveTroves2_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveTroves2_includedId_fk
                FOREIGN KEY (includedId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""")
        # now move the data over
        logMe(3, "Updating the TroveTroves table...")
        self.cu.execute("""
        INSERT INTO TroveTroves2
        (instanceId, includedId, flags, changed)
            SELECT instanceId, includedId,
                   CASE WHEN byDefault THEN %d ELSE 0 END,
                   changed
            FROM TroveTroves""" % TROVE_TROVES_BYDEFAULT)
        self.cu.execute("DROP TABLE TroveTroves")
        self.cu.execute("ALTER TABLE TroveTroves2 RENAME TO TroveTroves")
        # reload the schema and call createTrove() to fill in the missing triggers and indexes
        self.db.loadSchema()
        logMe(3, "Updating indexes and triggers...")
        createTroves(self.db)
        # we changed the Instances update trigger to protect the changed column from changing
        self.db.dropTrigger("Instances", "UPDATE")
        createTrigger(self.db, "Instances", pinned=True)
        # done...
        return self.Version

class MigrateTo_10(SchemaMigration):
    Version = 10
    def migrate(self):
        from  conary import trove
        logMe(3, "Updating index TroveInfoTypeIdx")
        # redo the troveInfoTypeIndex to be UNIQUE
        self.db.dropIndex("TroveInfo", "TroveInfoTypeIdx")
        self.db.createIndex("TroveInfo", "TroveInfoTypeIdx",
                            "infoType, instanceId", unique = True)
        logMe(3, "Updating index InstancesChangedIdx")
        # add instanceId to the InstancesChanged index
        self.db.dropIndex("Instances", "InstancesChangedIdx")
        self.db.createIndex("Instances", "InstancesChangedIdx",
                            "changed, instanceId")
        # add the clonedFrom column to the Instances table
        logMe(3, "Adding column and index for Instances.clonedFromId")
        self.cu.execute("ALTER TABLE Instances ADD COLUMN "
                        "clonedFromId INTEGER REFERENCES Versions(versionId) "
                        "ON DELETE RESTRICT ON UPDATE CASCADE")
        self.db.createIndex("Instances", "InstancesClonedFromIdx",
                            "clonedFromId, instanceId")
        # add the sourceItemId to the Nodes table
        logMe(3, "Adding column and index for Nodes.sourceItemId")
        self.cu.execute("ALTER TABLE Nodes ADD COLUMN "
                        "sourceItemId INTEGER REFERENCES Items(itemId) "
                        "ON DELETE RESTRICT ON UPDATE CASCADE")
        self.db.createIndex("Nodes", "NodesSourceItemIdx",
                            "sourceItemId, branchId")
        # update Versions, Instances and clonedFromId
        logMe(3, "Updating the Versions table...")
        # create a temp table first
        self.cu.execute("""
        CREATE TEMPORARY TABLE TItemp(
            instanceId INTEGER,
            infoType INTEGER,
            data VARCHAR(767)
        )""")
        self.cu.execute("INSERT INTO TItemp (instanceId, infoType, data) "
                        "SELECT instanceId, infoType, data from TroveInfo "
                        "WHERE TroveInfo.infoType in (?, ?)", (
            trove._TROVEINFO_TAG_CLONEDFROM, trove._TROVEINFO_TAG_SOURCENAME))
        self.cu.execute("CREATE INDEX TItempIdx1 ON TItemp(infoType, data)")
        self.cu.execute("CREATE INDEX TItempIdx2 ON TItemp(instanceId, infoType, data)")
        self.cu.execute("""
        INSERT INTO Versions (version)
        SELECT TI.data
        FROM ( SELECT DISTINCT data FROM TItemp
               WHERE infoType = ? ) AS TI
        LEFT OUTER JOIN Versions as V ON V.version = TI.data
        WHERE V.versionId is NULL
        """, trove._TROVEINFO_TAG_CLONEDFROM)
        # update the instances table
        logMe(3, "Extracting data for Instances.clonedFromId from TroveInfo")
        self.cu.execute("""
        UPDATE Instances
        SET clonedFromId = (
            SELECT DISTINCT V.versionId
            FROM TItemp AS TI
            JOIN Versions as V ON TI.data = V.version
            WHERE Instances.instanceId = TI.instanceId
            AND TI.infoType = ? )
        """, trove._TROVEINFO_TAG_CLONEDFROM)
        # transfer the sourceItemIds from TroveInfo into the Nodes table
        logMe(3, "Updating the Items table...")
        # first, create the missing Items
        self.cu.execute("""
        INSERT INTO Items (item)
        SELECT TI.data
        FROM ( SELECT DISTINCT data FROM TItemp
               WHERE infoType = ? ) AS TI
        LEFT OUTER JOIN Items as AI ON TI.data = AI.item
        WHERE AI.itemId is NULL
        """, trove._TROVEINFO_TAG_SOURCENAME)
        # update the nodes table
        logMe(3, "Extracting data for Nodes.sourceItemId from TroveInfo")
        self.cu.execute("""
        UPDATE Nodes
        SET sourceItemId = (
            SELECT DISTINCT Items.itemId
            FROM Instances
            JOIN TItemp as TI USING (instanceId)
            JOIN Items on Items.item = TI.data
            WHERE TI.infotype = ?
            AND Nodes.itemId = Instances.itemId
            AND Nodes.versionId = Instances.versionId )
        """, trove._TROVEINFO_TAG_SOURCENAME)
        # finished with the TroveInfo migration
        self.cu.execute("DROP TABLE TItemp")
        return self.Version

class MigrateTo_11(SchemaMigration):
    Version = 11
    def migrate(self):
        from  conary import trove
        cu = self.cu
        cu2 = self.db.cursor()

	logMe(3, "Rebuilding the Latest table...")
        cu.execute("DROP TABLE Latest")
        self.db.loadSchema()
        createLatest(self.db)
	cu.execute("""
            insert into Latest (itemId, branchId, flavorId, versionId)
                select
                    instances.itemid as itemid,
                    nodes.branchid as branchid,
                    instances.flavorid as flavorid,
                    nodes.versionid as versionid
                from
                    ( select
                        i.itemid as itemid,
                        n.branchid as branchid,
                        i.flavorid as flavorid,
                        max(n.finalTimestamp) as finaltimestamp
                      from
                        instances as i, nodes as n
                      where
                            i.itemid = n.itemid
                        and i.versionid = n.versionid
                      group by i.itemid, n.branchid, i.flavorid
                    ) as tmp
                    join nodes on
                      tmp.itemid = nodes.itemid and
                      tmp.branchid = nodes.branchid and
                      tmp.finaltimestamp = nodes.finaltimestamp
                    join instances on
                      nodes.itemid = instances.itemid and
                      nodes.versionid = instances.versionid and
                      instances.flavorid = tmp.flavorid
        """)

        # the order used for some path hashes wasn't deterministic,
        # so we need to check all of them
        cu.execute("""
            CREATE TEMPORARY TABLE hashUpdatesTmp(
                instanceId INTEGER,
                data       %(MEDIUMBLOB)s
            )
        """ % self.db.keywords)
        cu.execute("CREATE INDEX hashUpdatesTmpIdx ON "
                   "hashUpdatesTmp(instanceId)")
        logMe(3, "Finding path hashes needing an update...")
        rows = cu2.execute("SELECT instanceId,data from TroveInfo "
                           "WHERE infoType=?", trove._TROVEINFO_TAG_PATH_HASHES)
        neededChanges = []
        PathHashes = trove.PathHashes
        for instanceId, data in rows:
            frzn = PathHashes(data).freeze()
            if frzn != data:
                cu.execute('INSERT INTO hashUpdatesTmp VALUES (?, ?)',
                           (instanceId, cu.binary(frzn)))

        logMe(3, "removing bad signatures due to path hashes...")
        cu.execute("""
        DELETE FROM TroveInfo
        WHERE infoType=?
          AND instanceId IN (SELECT instanceId from hashUpdatesTmp)
        """, trove._TROVEINFO_TAG_SIGS)

        logMe(3, "updating path hashes...")
	cu.execute("SELECT instanceId, data FROM hashUpdatesTmp")
	for (instanceId, data) in cu:
            cu2.execute("UPDATE TroveInfo SET data=? WHERE "
                       "infoType=? AND instanceId=?",
                        (data, trove._TROVEINFO_TAG_PATH_HASHES, instanceId))
        cu.execute("DROP TABLE hashUpdatesTmp")

        return self.Version


class MigrateTo_12(SchemaMigration):
    Version = 12
    def migrate(self):
        from  conary import trove
        cu = self.cu
        logMe(3, "Fixing NULL path hashes...")
        cu.execute("SELECT instanceId FROM TroveInfo "
                   "WHERE data IS NULL and infotype = ?",
                   trove._TROVEINFO_TAG_PATH_HASHES)
        cu2 = self.db.cursor()
        for instanceId, in cu:
            cu2.execute("SELECT path FROM TroveFiles WHERE instanceId=?", instanceId)
            ph = trove.PathHashes()
            for path, in cu2:
                ph.addPath(path)
            cu2.execute("UPDATE TroveInfo SET data=? "
                        "WHERE instanceId=? and infotype=?",
                        (ph.freeze(), instanceId,
			 trove._TROVEINFO_TAG_PATH_HASHES))
        return self.Version

class MigrateTo_13(SchemaMigration):
    Version = 13
    def migrate(self):
        from conary import files
        # fix the duplicate FileStreams.fileId fields
        logMe(3, "Looking for duplicate fileId entries...")
        # this takes a bit to execute, especially on sqlite
        self.cu.execute("""
        CREATE TEMPORARY TABLE origs AS
            SELECT a.streamId  AS streamId,
                   a.fileId    AS fileId
            FROM FileStreams AS a JOIN FileStreams AS b
            where a.fileId = b.fileId
              and a.streamId < b.streamId
              and a.fileId is not null
              and a.stream != b.stream
        """)
        # all the duplicate fileIds that have a streamId not in the
        # origs table are dupes
        # First, check that the duplicate streams differ only by the mtime field
        logMe(3, "Checking duplicate fileId streams...")
        self.cu.execute("""
        SELECT fs.streamId, fs.fileId, fs.stream
        FROM origs JOIN FileStreams AS fs USING(streamId)
        """)
        cu2 = self.db.cursor()
        for (streamId, fileId, stream) in self.cu:
            file = files.ThawFile(self.cu.frombinary(stream), None)
            # select all other streams with the same streamId
            cu2.execute("""
            SELECT fs.streamId, fs.stream
            FROM FileStreams AS fs
            WHERE fs.fileId = ?
              AND fs.streamId != ?
            """, (fileId, streamId))
            for (dupStreamId, dupStream) in cu2:
                file2 = files.ThawFile(cu2.frombinary(dupStream), None)
                file2.inode.mtime.set(file.inode.mtime())
                assert (file == file2)
        logMe(3, "Removing references to duplicate fileId entries...")
        self.cu.execute("""
        SELECT fs.streamId, fs.fileId
        FROM origs JOIN FileStreams AS fs USING (fileId)
        WHERE origs.streamId != fs.streamId
        """)
        # the above select holds FileStreams locked, so we have to
        # flush it with a fetchall()
        for (streamId, fileId) in self.cu.fetchall():
            # update the referential integrity on TroveFiles and dispose
            # of the duplicate streamId entries from FileStreams
            self.cu.execute("""
            UPDATE TroveFiles SET streamId = (
                SELECT streamId FROM origs
                WHERE origs.fileId = ? )
            WHERE TroveFiles.streamId = ?
            """, (self.cu.binary(fileId), streamId))
            self.cu.execute("""
            DELETE FROM FileStreams where streamId = ?""",
            (streamId,))
        self.cu.execute("DROP TABLE origs")
        # force the creation of the new unique index
        logMe(3, "Recreating the fileId index...")
        self.db.dropIndex("FileStreams", "FileStreamsIdx")
        createTroves(self.db)
        return self.Version

# sets up temporary tables for a brand new connection
def setupTempTables(db):
    cu = db.cursor()

    if "ffFlavor" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE ffFlavor(
            flavorId    INTEGER,
            base        VARCHAR(254),
            sense       INTEGER,
            flag        VARCHAR(254)
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["ffFlavor"] = True
    if "NewFiles" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE NewFiles(
            pathId      %(BINARY16)s,
            versionId   INTEGER,
            fileId      %(BINARY20)s,
            stream      %(MEDIUMBLOB)s,
            path        VARCHAR(767)
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["NewFiles"] = True
        # since this is an index on a temp table, don't check the
        # validity of the table
        db.createIndex("NewFiles", "NewFilesFileIdx", "fileId",
                       check = False)
    if "NeededFlavors" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE NeededFlavors(
            flavor      VARCHAR(767)
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["NeededFlavors"] = True
    if "gtl" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE gtl(
            idx         %(PRIMARYKEY)s,
            name        VARCHAR(254),
            version     VARCHAR(767),
            flavor      VARCHAR(767)
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["gtl"] = True
    if "gtlInst" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE gtlInst(
            idx         %(PRIMARYKEY)s,
            instanceId  INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["gtlInst"] = True
    if "getFilesTbl" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE getFilesTbl(
            itemId      INTEGER PRIMARY KEY,
            fileId      %(BINARY20)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["getFilesTbl"] = True
    if "itf" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE itf(
            item        VARCHAR(254),
            version     VARCHAR(767),
            fullVersion VARCHAR(767)
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["itf"] = True
    if "gtvlTbl" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE gtvlTbl(
            item        VARCHAR(254),
            versionSpec VARCHAR(767),
            flavorId    INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["gtvlTbl"] = True
    if "hasTrovesTmp" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE
        hasTrovesTmp(
            row         INTEGER,
            item        VARCHAR(254),
            version     VARCHAR(767),
            flavor      VARCHAR(767)
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["hasTrovesTmp"] = True
    db.commit()
    db.loadSchema()

def resetTable(cu, name):
    cu.execute("DELETE FROM %s" % name,
               start_transaction = False)

# create the (permanent) server repository schema
def createSchema(db):
    createIdTables(db)
    createLabelMap(db)

    createUsers(db)
    createPGPKeys(db)

    createFlavors(db)
    createInstances(db)
    createNodes(db)
    createChangeLog(db)
    createLatest(db)

    createTroves(db)

    createDependencies(db)
    createMetadata(db)
    createMirrorTracking(db)

# run through the schema creation and migration (if required)
def loadSchema(db):
    global VERSION
    version = db.getVersion()

    # surely there is a more better way of handling this...
    if version == 1: version = MigrateTo_2(db)()
    if version == 2: version = MigrateTo_3(db)()
    if version == 3: version = MigrateTo_4(db)()
    if version == 4: version = MigrateTo_5(db)()
    if version == 5: version = MigrateTo_6(db)()
    if version == 6: version = MigrateTo_7(db)()
    if version == 7: version = MigrateTo_8(db)()
    if version == 8: version = MigrateTo_9(db)()
    if version == 9: version = MigrateTo_10(db)()
    if version == 10: version = MigrateTo_11(db)()
    if version == 11: version = MigrateTo_12(db)()
    if version == 12: version = MigrateTo_13(db)()

    if version:
        db.loadSchema()
    # run through the schema creation to create any missing objects
    createSchema(db)
    if version > 0 and version != VERSION:
        # schema creation/conversion failed. SHOULD NOT HAPPEN!
        raise sqlerrors.SchemaVersionError("""
        Schema migration process has failed to bring the database
        schema version up to date. Please report this error at
        http://bugs.rpath.com/.

        Current schema version is %s; Required schema version is %s.
        """ % (version, VERSION))
    db.loadSchema()

    if version != VERSION:
        return db.setVersion(VERSION)

    return True

# this should only check for the proper schema version. This function
# is called usually from the multithreaded setup, so schema operations
# should be avoided here
def checkVersion(db):
    global VERSION
    version = db.getVersion()
    logMe(3, VERSION, version)
    if version == VERSION:
        return version

    if version > VERSION:
        raise sqlerrors.SchemaVersionError("""
        This code version is too old for the Conary repository
        database schema that you are running. you need to upgrade the
        conary repository code base to a more recent version.

        Current schema version is %s; Required schema version is %s.
        """ % (version, VERSION))

    raise sqlerrors.SchemaVersionError("""
    Your database schema is not initalized or it is too old.  Please
    run the standalone server with the --migrate argument to
    upgrade/initialize the database schema for the Conary Repository.

    Current schema version is %s; Required schema version is %s.
    """ % (version, VERSION))


