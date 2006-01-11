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
from conary.local.schema import createDependencies, resetTable

TROVE_TROVES_BYDEFAULT = 1 << 0
TROVE_TROVES_WEAKREF   = 1 << 1

VERSION = 10

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
        )""" % db.keywords)
        db.tables["Instances"] = []
        commit = True
    if "InstancesIdx" not in db.tables["Instances"]:
        cu.execute("CREATE UNIQUE INDEX InstancesIdx ON "
                   "Instances(itemId, versionId, flavorId) ")
        commit = True
    if "InstancesChangedIdx" not in db.tables["Instances"]:
        cu.execute("CREATE INDEX InstancesChangedIdx ON "
                   "Instances(changed, instanceId)")
    if "InstancesClonedFromIdx" not in db.tables["Instances"]:
        cu.execute("CREATE INDEX InstancesClonedFromIdx ON "
                   "Instances(clonedFromId, instanceId)")
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
        )""" % db.keywords)
        cu.execute("INSERT INTO Flavors (flavorId, flavor) VALUES (0, 'none')")
        db.tables["Flavors"] = []
        commit = True
    if "FlavorsFlavorIdx" not in db.tables["Flavors"]:
        cu.execute("CREATE UNIQUE INDEX FlavorsFlavorIdx ON Flavors(flavor)")
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
        db.tables["FlavorMap"] = []
        commit = True
    if "FlavorMapIndex" not in db.tables["FlavorMap"]:
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
        db.tables["FlavorScores"] = []
        for (request, present), value in deps.flavorScores.iteritems():
            if value is None:
                value = -1000000
            cu.execute("INSERT INTO FlavorScores (request, present, value) VALUES (?,?,?)",
                       request, present, value)
        commit = True
    if "FlavorScoresIdx" not in db.tables["FlavorScores"]:
        cu.execute("CREATE UNIQUE INDEX FlavorScoresIdx ON "
                   "FlavorScores(request, present)")
        commit = True

    if not resetTable(cu, 'ffFlavor'):
        cu.execute("""
        CREATE TEMPORARY TABLE ffFlavor(
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
        )""" % db.keywords)
        db.tables["Nodes"] = []
        cu.execute("""INSERT INTO Nodes
        (nodeId, itemId, branchId, versionId, timeStamps, finalTimeStamp)
        VALUES (0, 0, 0, 0, NULL, 0.0)""")
        commit = True
    if "NodesItemBranchVersionIdx" not in db.tables["Nodes"]:
        cu.execute("CREATE UNIQUE INDEX NodesItemBranchVersionIdx "
                   "ON Nodes(itemId, branchId, versionId)")
        commit = True
    if "NodesItemVersionIdx" not in db.tables["Nodes"]:
        cu.execute("CREATE INDEX NodesItemVersionIdx ON Nodes(itemId, versionId)")
        commit = True
    if "NodesSourceItemIdx" not in db.tables["Nodes"]:
        cu.execute("CREATE INDEX NodesSourceItemIdx ON Nodes(sourceItemId, branchId)")
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
        db.tables["Latest"] = []
        commit = True
    if "LatestIdx" not in db.tables["Latest"]:
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
        db.tables["Users"] = []
        commit = True
    if "UsersUser_uq" not in db.tables["Users"]:
        cu.execute("CREATE UNIQUE INDEX UsersUser_uq on Users(userName)")
        commit = True
    if createTrigger(db, "Users"):
        commit = True

    if "UserGroups" not in db.tables:
        cu.execute("""
        CREATE TABLE UserGroups (
            userGroupId     %(PRIMARYKEY)s,
            userGroup       VARCHAR(254) NOT NULL,
            canMirror       INTEGER NOT NULL DEFAULT 0,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0
        )""" % db.keywords)
        db.tables["UserGroups"] = []
        commit = True
    if "UserGroupsUserGroup_uq" not in db.tables["UserGroups"]:
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
        db.tables["UserGroupMembers"] = []
        commit = True
    if "UserGroupMembers_uq" not in db.tables["UserGroupMembers"]:
        cu.execute("CREATE UNIQUE INDEX UserGroupMembers_uq ON "
                   "UserGroupMembers(userGroupId, userId)")
        commit = True
    if "UserGroupMembersUserIdx" not in db.tables["UserGroupMembers"]:
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
        db.tables["Permissions"] = []
        commit = True
    if "PermissionsIdx" not in db.tables["Permissions"]:
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
        db.tables["EntitlementGroups"] = []
        commit = True
    if "EntitlementGroupsEntGroupIdx" not in db.tables["EntitlementGroups"]:
        cu.execute("CREATE UNIQUE INDEX EntitlementGroupsEntGroupIdx ON "
                   "EntitlementGroups(entGroup)")
        commit = True
    if "EntitlementGroupsUGIdx" not in db.tables["EntitlementGroups"]:
        cu.execute("CREATE INDEX EntitlementGroupsUGIdx ON "
                   "EntitlementGroups(userGroupId)")
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
        db.tables["EntitlementOwners"] = []
        commit = True
    if "EntitlementOwnersEntOwnerIdx" not in db.tables["EntitlementOwners"]:
        cu.execute("CREATE UNIQUE INDEX EntitlementOwnersEntOwnerIdx ON "
                   "EntitlementOwners(entGroupId, ownerGroupId)")
        commit = True
    if "EntitlementOwnersOwnerIdx" not in db.tables["EntitlementOwners"]:
        cu.execute("CREATE INDEX EntitlementOwnersOwnerIdx ON "
                   "EntitlementOwners(ownerGroupId)")
        commit = True

    if "Entitlements" not in db.tables:
        cu.execute("""
        CREATE TABLE Entitlements(
            entGroupId      INTEGER NOT NULL,
            entitlement     %(BINARY255)s NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT Entitlements_entGroupId_fk
                FOREIGN KEY (entGroupId) REFERENCES EntitlementGroups(entGroupId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        )""" % db.keywords)
        db.tables["Entitlements"] = []
        commit = True
    if "EntitlementsEntGroupEntitlementIdx" not in db.tables["Entitlements"]:
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
        )""" % db.keywords)
        db.tables["PGPKeys"] = []
        commit = True
    if "PGPKeysFingerprintIdx" not in db.tables["PGPKeys"]:
        cu.execute("CREATE UNIQUE INDEX PGPKeysFingerprintIdx ON "
                   "PGPKeys(fingerprint)")
        commit = True
    if "PGPKeysUserIdx" not in db.tables["PGPKeys"]:
        cu.execute("CREATE INDEX PGPKeysUserIdx ON PGPKeys(userId)")
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
        db.tables["PGPFingerprints"] = []
        commit = True
    if "PGPFingerprintsKeyIdx" not in db.tables["PGPFingerprints"]:
        cu.execute("CREATE UNIQUE INDEX PGPFingerprintsKeyIdx ON "
                   "PGPFingerprints(keyId,fingerprint)")
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
        db.tables["FileStreams"] = []
        commit = True
    if "FileStreamsIdx" not in db.tables["FileStreams"]:
        # XXX: is this still true now? --gafton
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
        db.tables["TroveFiles"] = []
        commit = True
    # FIXME: rename these indexes
    if "TroveFilesIdx" not in db.tables["TroveFiles"]:
        cu.execute("CREATE INDEX TroveFilesIdx ON TroveFiles(instanceId)")
        commit = True
    if "TroveFilesIdx2" not in db.tables["TroveFiles"]:
        cu.execute("CREATE INDEX TroveFilesIdx2 ON TroveFiles(streamId)")
        commit = True
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
        )""")
        db.tables["TroveTroves"] = []
        commit = True
    if "TroveTrovesInstanceIncluded_uq" not in db.tables["TroveTroves"]:
        # This index is used to enforce that TroveTroves only contains
        # unique TroveTrove (instanceId, includedId) pairs.
        cu.execute("CREATE UNIQUE INDEX TroveTrovesInstanceIncluded_uq ON "
                   "TroveTroves(instanceId,includedId)")
        commit = True
    if "TroveTrovesIncludedIdx" not in db.tables["TroveTroves"]:
        # this index is so we can quickly tell what troves are needed by another trove
        cu.execute("CREATE INDEX TroveTrovesIncludedIdx ON TroveTroves(includedId)")
        commit = True
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
        )""" % db.keywords)
        db.tables["TroveInfo"] = []
        commit = True
    if "TroveInfoIdx" not in db.tables["TroveInfo"]:
        cu.execute("CREATE INDEX TroveInfoIdx ON TroveInfo(instanceId)")
        commit = True
    if "TroveInfoTypeIdx" not in db.tables["TroveInfo"]:
        cu.execute("CREATE UNIQUE INDEX TroveInfoTypeIdx ON TroveInfo(infoType, instanceId)")
        commit = True
    if createTrigger(db, "TroveInfo"):
        commit = True

    # FIXME - move the temporary table handling into a separate
    # fucntion that can also be called before we start processing a
    # request (as opposed to the schema management which will be
    # restricted to standalone mode)
    if commit:
        db.commit()

    # FIXME
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

    # FIXME
    if not resetTable(cu, 'NeededFlavors'):
        db.rollback()
        cu.execute("CREATE TEMPORARY TABLE NeededFlavors(flavor VARCHAR(767))")
        db.commit()

    # FIXME
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

    # FIXME
    if not resetTable(cu, 'gtlInst'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE gtlInst(
        idx             %(PRIMARYKEY)s,
        instanceId      INTEGER
        )""" % db.keywords)
        db.commit()

    # FIXME
    if not resetTable(cu, 'getFilesTbl'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE getFilesTbl(
            itemId       INTEGER PRIMARY KEY,
            fileId      %(BINARY20)s
        )""" % db.keywords)
        db.commit()

    # FIXME
    if not resetTable(cu, 'itf'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE itf(
        item            VARCHAR(254),
        version         VARCHAR(767),
        fullVersion     VARCHAR(767)
        )""")
        db.commit()

    # FIXME
    if not resetTable(cu, 'gtvlTbl'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE
        gtvlTbl(
            item                VARCHAR(254),
            versionSpec         VARCHAR(767),
            flavorId            INTEGER
        )""")
        db.commit()

    if not resetTable(cu, 'hasTrovesTmp'):
        db.rollback()
        cu.execute("""
        CREATE TEMPORARY TABLE
        hasTrovesTmp(
            row                 INTEGER,
            item                VARCHAR(254),
            version             VARCHAR(767),
            flavor              VARCHAR(767)
        )""")
        db.commit()

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
        )""" % db.keywords)
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
        )""")
        db.tables["MetadataItems"] = []
        commit = True
    if "MetadataItemsIdx" not in db.tables["MetadataItems"]:
        cu.execute("CREATE INDEX MetadataItemsIdx ON MetadataItems(metadataId)")
        commit = True
    if createTrigger(db, "MetadataItems"):
        commit = True

    if commit:
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

def createMirrorTracking(db):
    cu = db.cursor()
    if 'LatestMirror' not in db.tables:
        cu.execute("""
        CREATE TABLE LatestMirror(
            host            VARCHAR(254),
            mark            NUMERIC(14,0) NOT NULL
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
        db.tables["ChangeLogs"] = []
        cu.execute("INSERT INTO ChangeLogs (nodeId, name, contact, message) "
                   "VALUES(0, NULL, NULL, NULL)")
        commit = True
    if "ChangeLogsNodeIdx" not in db.tables["ChangeLogs"]:
        cu.execute("CREATE UNIQUE INDEX ChangeLogsNodeIdx ON "
                   "ChangeLogs(nodeId)")
        commit = True
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
        )""")
        db.tables["LabelMap"] = []
        commit = True
    # FIXME: rename indexes accordingly
    if "" not in db.tables["LabelMap"]:
        cu.execute("CREATE INDEX LabelMapItemIdx  ON LabelMap(itemId)")
        commit = True
    if "" not in db.tables["LabelMap"]:
        cu.execute("CREATE INDEX LabelMapLabelIdx ON LabelMap(labelId)")
        commit = True
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
        self.cu.execute("DROP INDEX PermissionsIdx")
        self.cu.execute("CREATE UNIQUE INDEX PermissionsIdx ON "
                   "Permissions(userGroupId, labelId, itemId)")
        return self.Version

# add a smaller index for the Latest table
class MigrateTo_3(SchemaMigration):
    Version = 3
    def migrate(self):
        self.cu.execute("CREATE INDEX LatestItemIdx on Latest(itemId)")
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
        if "TroveInfoIdx2" in self.db.tables["TroveInfo"]:
            self.cu.execute("DROP INDEX TroveInfoIdx2")
        if "TroveTrovesInstanceIdx" in self.db.tables["TroveTroves"]:
            self.cu.execute("DROP INDEX TroveTrovesInstanceIdx")
        if "UserGroupMembersIdx" in self.db.tables["UserGroupMembers"]:
            self.cu.execute("DROP INDEX UserGroupMembersIdx")
        if "UserGroupMembersIdx2" in self.db.tables["UserGroupMembers"]:
            self.cu.execute("DROP INDEX UserGroupMembersIdx2")
        if "UserGroupsUserGroupIdx" in self.db.tables["UserGroups"]:
            self.cu.execute("DROP INDEX UserGroupsUserGroupIdx")
        if "LatestItemIdx" in self.db.tables["Latest"]:
            self.cu.execute("DROP INDEX LatestItemIdx")
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
        if "TroveInfoTypeIdx" in self.db.tables["TroveInfo"]:
            self.cu.execute("DROP INDEX TroveInfoTypeIdx")
        self.cu.execute("CREATE UNIQUE INDEX TroveInfoTypeIdx ON "
                        "TroveInfo(infoType, instanceId)")
        logMe(3, "Updating index InstancesChangedIdx")
        # add instanceId to the InstancesChanged index
        if "InstancesChangedIdx" in self.db.tables["Instances"]:
            self.cu.execute("DROP INDEX InstancesChangedIdx")
        self.cu.execute("CREATE INDEX InstancesChangedIdx ON "
                        "Instances(changed, instanceId)")
        # add the clonedFrom column to the Instances table
        logMe(3, "Adding column and index for Instances.clonedFromId")
        self.cu.execute("ALTER TABLE Instances ADD COLUMN "
                        "clonedFromId INTEGER REFERENCES Versions(versionId) "
                        "ON DELETE RESTRICT ON UPDATE CASCADE")
        if "InstancesClonedFromIdx" not in self.db.tables["Instances"]:
            self.cu.execute("CREATE INDEX InstancesClonedFromIdx ON "
                            "Instances(clonedFromId, instanceId)")
        # add the sourceItemId to the Nodes table
        logMe(3, "Adding column and index for Nodes.sourceItemId")
        self.cu.execute("ALTER TABLE Nodes ADD COLUMN "
                        "sourceItemId INTEGER REFERENCES Items(itemId) "
                        "ON DELETE RESTRICT ON UPDATE CASCADE")
        if "NodesSourceItemIdx" not in self.db.tables["Nodes"]:
            self.cu.execute("CREATE INDEX NodesSourceItemIdx ON "
                       "Nodes(sourceItemId, branchId)")
        # transfer the sourceItemIds from TroveInfo into the Nodes table
        logMe(3, "Extracting data for nodes.sourceItemId from TroveInfo")
        # first, create the missing Items
        self.cu.execute("""
        INSERT INTO Items (item)
        SELECT DISTINCT data
            FROM TroveInfo as TI
            LEFT OUTER JOIN Items as AI ON TI.data = AI.item
            WHERE TI.infoType = ?
            AND   AI.itemId is NULL
        """, trove._TROVEINFO_TAG_SOURCENAME)
        # update the nodes table
        self.cu.execute("""
        UPDATE Nodes
        SET sourceItemId = (
            SELECT DISTINCT Items.itemId
            FROM Instances
            JOIN TroveInfo as TI USING (instanceId)
            JOIN Items on TI.data = Items.item
            WHERE TI.infotype = ?
            AND Nodes.itemId = Instances.itemId
            AND Nodes.versionId = Instances.versionId )
        """, trove._TROVEINFO_TAG_SOURCENAME)
        # clean up TroveInfo
        self.cu.execute("DELETE FROM TroveInfo WHERE infoType = ?",
                        trove._TROVEINFO_TAG_SOURCENAME)
        # repeat the same deal for Versions, Instances and clonedFromId
        logMe(3, "Extracting data for Instances.clonedFromId from TroveInfo")
        self.cu.execute("""
        INSERT INTO Versions (version)
        SELECT DISTINCT data
            FROM TroveInfo as TI
            LEFT OUTER JOIN Versions as V ON TI.data = V.version
            WHERE TI.infoType = ?
            AND   V.version is NULL
        """, trove._TROVEINFO_TAG_CLONEDFROM)
        # update the instances table
        self.cu.execute("""
        UPDATE Instances
        SET clonedFromId = (
            SELECT DISTINCT V.versionId
            FROM TroveInfo AS TI
            JOIN Versions as V ON TI.data = V.version
            WHERE TI.infoType = ?
            AND Instances.instanceId = TI.instanceId )
        """, trove._TROVEINFO_TAG_CLONEDFROM)
        return self.Version

# create the server repository schema
def createSchema(db):
    global VERSION
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
    createMetadata(db)
    createMirrorTracking(db)


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
    if version == 1: version = MigrateTo_2(db)()
    if version == 2: version = MigrateTo_3(db)()
    if version == 3: version = MigrateTo_4(db)()
    if version == 4: version = MigrateTo_5(db)()
    if version == 5: version = MigrateTo_6(db)()
    if version == 6: version = MigrateTo_7(db)()
    if version == 7: version = MigrateTo_8(db)()
    if version == 8: version = MigrateTo_9(db)()
    if version == 9: version = MigrateTo_10(db)()

    return version

