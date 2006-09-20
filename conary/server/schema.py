#
# Copyright (c) 2005-2006 rPath, Inc.
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

import sys

from conary.dbstore import sqlerrors, idtable
from conary.lib.tracelog import logMe
from conary.local.schema import createDependencies, setupTempDepTables

TROVE_TROVES_BYDEFAULT = 1 << 0
TROVE_TROVES_WEAKREF   = 1 << 1

VERSION = 15

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
            troveType       INTEGER NOT NULL DEFAULT 0,
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
        cu.execute("INSERT INTO Flavors (flavorId, flavor) VALUES (0, '')")
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
                       (request, present, value))
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

    if commit:
        db.commit()
        db.loadSchema()

def createLatest(db):
    cu = db.cursor()
    commit = False
    if 'Latest' not in db.tables:
        assert("Items" in db.tables)
        assert("Branches" in db.tables)
        assert("Flavors" in db.tables)
        assert("Versions" in db.tables)
        assert("Caps" in db.tables)
        cu.execute("""
        CREATE TABLE Latest(
            itemId          INTEGER NOT NULL,
            branchId        INTEGER NOT NULL,
            flavorId        INTEGER NOT NULL,
            versionId       INTEGER NOT NULL,
            latestType      INTEGER NOT NULL,
            capId           INTEGER NOT NULL DEFAULT 0,
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
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Latest_capId_fk
                FOREIGN KEY (capId) REFERENCES Caps(capId)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Latest"] = []
        commit = True
    db.createIndex("Latest", "LatestIdx", "itemId, branchId, flavorId",
                   unique = False)
    db.createIndex("Latest", "LatestCheckIdx",
                   "itemId, branchId, flavorId, latestType",
                   unique = True)
    if createTrigger(db, "Latest"):
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

    if idtable.createIdTable(db, "Caps", "capId", "capName"):
        cu.execute("INSERT INTO Caps (capId, capName) VALUES (0, 'UNCAPPED')")
        commit = True

    if "Permissions" not in db.tables:
        assert("Items" in db.tables)
        assert("Labels" in db.tables)
        assert("UserGroups" in db.tables)
        assert("Caps" in db.tables)
        cu.execute("""
        CREATE TABLE Permissions (
            permissionId    %(PRIMARYKEY)s,
            userGroupId     INTEGER NOT NULL,
            labelId         INTEGER NOT NULL,
            itemId          INTEGER NOT NULL,
            canWrite        INTEGER NOT NULL DEFAULT 0,
            capId           INTEGER NOT NULL DEFAULT 0,
            admin           INTEGER NOT NULL DEFAULT 0,
            canRemove       INTEGER NOT NULL DEFAULT 0,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT Permissions_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES UserGroups(userGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Permissions_labelId_fk
                FOREIGN KEY (labelId) REFERENCES Labels(labelId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Permissions_itemId_fk
                FOREIGN KEY (itemid) REFERENCES Items(itemId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT Permissions_capId_fk
                FOREIGN KEY (capId) REFERENCES Caps(capId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Permissions"] = []
        commit = True
    db.createIndex("Permissions", "PermissionsIdx",
                   "userGroupId, labelId, itemId", unique = True)
    if createTrigger(db, "Permissions"):
        commit = True

    if "EntitlementGroups" not in db.tables:
        cu.execute("""
        CREATE TABLE EntitlementGroups (
            entGroupId      %(PRIMARYKEY)s,
            entGroup        VARCHAR(254) NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["EntitlementGroups"] = []
        commit = True
    db.createIndex("EntitlementGroups", "EntitlementGroupsEntGroupIdx",
                   "entGroup", unique = True)
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
    db.createIndex("Entitlements", "EntitlementsEgEIdx",
                   "entGroupId, entitlement", unique = True)
    if createTrigger(db, "Entitlements"):
        commit = True

    if "EntitlementAccessMap" not in db.tables:
        cu.execute("""
        CREATE TABLE EntitlementAccessMap(
            entGroupId      INTEGER NOT NULL,
            userGroupId     INTEGER NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT EntitlementAccessMap_entGroupId_fk
                FOREIGN KEY (entGroupId) REFERENCES
                                            EntitlementGroups(entGroupId),
            CONSTRAINT EntitlementAccessMap_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES userGroups(userGroupId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["EntitlementAccessMap"] = []
        commit = True
    db.createIndex("EntitlementAccessMap", "EntitlementAccessMapIndex",
                   "entGroupId, userGroupId", unique = True)
    if createTrigger(db, "EntitlementAccessMap"):
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
            fingerprint     CHAR(40) PRIMARY KEY NOT NULL,
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
            sha1        %(BINARY20)s,
            changed     NUMERIC(14,0) NOT NULL DEFAULT 0
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["FileStreams"] = []
        commit = True
    db.createIndex("FileStreams", "FileStreamsIdx",
                   "fileId", unique = True)
    db.createIndex("FileStreams", "FileStreamsSha1Idx",
                   "sha1", unique = False)
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
    db.createIndex("TroveFiles", "TroveFilesPathIdx", "path,instanceId",
                   unique=True)

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

    if "TroveRedirects" not in db.tables:
        cu.execute("""
        CREATE TABLE TroveRedirects(
            instanceId      INTEGER NOT NULL,
            itemId          INTEGER NOT NULL,
            branchId        INTEGER NOT NULL,
            flavorId        INTEGER,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT TroveRedirects_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveRedirects_itemId_fk
                FOREIGN KEY (itemId) REFERENCES Items(itemId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveRedirects_branchId_fk
                FOREIGN KEY (branchId) REFERENCES Branches(branchId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveRedirects_flavorId_fk
                FOREIGN KEY (flavorId) REFERENCES Flavors(flavorId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["TroveRedirects"] = []
        commit = True
    db.createIndex("TroveRedirects", "TroveRedirectsIdx", "instanceId")
    if createTrigger(db, "TroveRedirects"):
        commit = True

    if commit:
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

# sets up temporary tables for a brand new connection
def setupTempTables(db):
    logMe(3)
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
        db.createIndex("ffFlavor", "ffFlavorBaseIdx", "flavorId,base",
                       check = False)
        db.createIndex("ffFlavor", "ffFlavorSenseIdx", "flavorId,sense",
                       check = False)
    if "NewFiles" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE NewFiles(
            pathId      %(BINARY16)s,
            versionId   INTEGER,
            fileId      %(BINARY20)s,
            stream      %(MEDIUMBLOB)s,
            sha1        %(BINARY20)s,
            path        VARCHAR(767)
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["NewFiles"] = True
        # since this is an index on a temp table, don't check the
        # validity of the table
        db.createIndex("NewFiles", "NewFilesFileIdx", "fileId",
                       check = False)
    if "NewRedirects" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE NewRedirects(
            item        VARCHAR(767),
            branch      VARCHAR(767),
            flavor      VARCHAR(767)
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["NewRedirects"] = True
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
        db.createIndex("gtlInst", "gtlInstInstanceIdx", "instanceId, idx",
                       check = False)
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
            item        VARCHAR(767),
            versionSpec VARCHAR(767),
            flavorId    INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["gtvlTbl"] = True
        db.createIndex("gtvlTbl", "gtvlTblItemIdx", "item", check = False)
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
        db.createIndex("hasTrovesTmp", "hasTrovesTmpIdx", "item, version",
                       check = False)
 
    if "trovesByPathTmp" not in db.tempTables:
        cu.execute("""
             CREATE TEMPORARY TABLE
             trovesByPathTmp(
                 row                 INTEGER,
                 path                VARCHAR(767)
             )""")
        db.tempTables["trovesByPathTmp"] = True

    if "tmpInstances" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE
        tmpInstances(
            instanceId    INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpInstances"] = True
        db.createIndex("tmpInstances", "tmpInstancesIdx", "instanceId",
                       check = False)
    if "tmpInstances2" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE
        tmpInstances2(
            instanceId    INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpInstances2"] = True

    # temporary table for _getFileStreams
    if "gfsTable" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE
        gfsTable(
            idx         %(PRIMARYKEY)s,
            fileId      %(BINARY20)s NOT NULL
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["gfsTable"] = True
        db.createIndex("gfsTable", "gfsTableFileIdIdx", "fileId",
                       check = False)

    db.commit()

def resetTable(cu, name):
    cu.execute("DELETE FROM %s" % name,
               start_transaction = False)

# create the (permanent) server repository schema
def createSchema(db):
    if not hasattr(db, "tables"):
        db.loadSchema()
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

    logMe(1, "current =", version, "required =", VERSION)
    # load the current schema object list
    db.loadSchema()

    if version != 0 and version < 13:
        raise sqlerrors.SchemaVersionError(
            "Repository schemas from Conary versions older than 1.0 are not "
            "supported. Contact rPath for help converting your repository to "
            "a supported version.")

    if version and version < VERSION:
        # avoid a recursive import by importing just what we need
        from conary.server import migrate
        version = migrate.migrateSchema(db, version)

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

    return VERSION

# this should only check for the proper schema version. This function
# is called usually from the multithreaded setup, so schema operations
# should be avoided here
def checkVersion(db):
    global VERSION
    version = db.getVersion()
    logMe(2, VERSION, version)
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
