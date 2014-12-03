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


from conary.dbstore import sqlerrors, sqllib, idtable
from conary.lib.tracelog import logMe
from conary.local.schema import createDependencies

TROVE_TROVES_BYDEFAULT = 1 << 0
TROVE_TROVES_WEAKREF   = 1 << 1

# This is the major number of the schema we need
VERSION = sqllib.DBversion(18)

def createTrigger(db, table, column = "changed"):
    retInsert = db.createTrigger(table, column, "INSERT")
    retUpdate = db.createTrigger(table, column, "UPDATE")
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
            fingerprint     %(BINARY20)s,
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
    # this also serves in place of a fk index for itemId
    db.createIndex("Instances", "InstancesIdx", "itemId,versionId,flavorId",
                   unique = True)
    db.createIndex("Instances", "InstancesVersionId_fk", "versionId, instanceId")
    db.createIndex("Instances", "InstancesFlavorId_fk", "flavorId, instanceId")
    db.createIndex("Instances", "InstancesClonedFromIdx", "clonedFromId,instanceId")
    db.createIndex("Instances", "InstancesChangedIdx", "changed,instanceId")
    db.createIndex("Instances", "InstancesPresentIdx", "isPresent,instanceId")
    if createTrigger(db, "Instances"):
        commit = True

    if commit:
        db.loadSchema()


def createFlavors(db):
    cu = db.cursor()
    commit = False
    if "Flavors" not in db.tables:
        cu.execute("""
        CREATE TABLE Flavors(
            flavorId        %(PRIMARYKEY)s,
            flavor          %(STRING)s
        ) %(TABLEOPTS)s""" % db.keywords)
        cu.execute("INSERT INTO Flavors (flavorId, flavor) VALUES (0, '')")
        db.tables["Flavors"] = []
        commit = True
    db.createIndex("Flavors", "FlavorsFlavorIdx", "flavor", unique = True)

    if "FlavorMap" not in db.tables:
        cu.execute("""
        CREATE TABLE FlavorMap(
            flavorId        INTEGER NOT NULL,
            base            VARCHAR(254) NOT NULL,
            sense           INTEGER,
            depClass        INTEGER NOT NULL,
            flag            VARCHAR(254),
            CONSTRAINT FlavorMap_flavorId_fk
                FOREIGN KEY (flavorId) REFERENCES Flavors(flavorId)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["FlavorMap"] = []
        commit = True
    db.createIndex("FlavorMap", "FlavorMapIndex", "flavorId, depClass, base")

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
            timeStamps      %(STRING)s,
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
    db.createIndex("Nodes", "NodesItemVersionBranchIdx",
                        "itemId, versionId, branchId", unique = True)
    db.createIndex("Nodes", "NodesBranchId_fk", "branchId, itemId")
    db.createIndex("Nodes", "NodesVersionId_fk", "versionId, itemId")
    db.createIndex("Nodes", "NodesSourceItemIdx", "sourceItemId, branchId")
    if createTrigger(db, "Nodes"):
        commit = True

    if commit:
        db.loadSchema()


def createLatest(db, withIndexes = True):
    assert("Instances" in db.tables)
    assert("Nodes" in db.tables)
    assert("UserGroupInstancesCache" in db.tables)
    cu = db.cursor()
    commit = False
    from conary.repository.netrepos.versionops import LATEST_TYPE_ANY,\
         LATEST_TYPE_PRESENT, LATEST_TYPE_NORMAL
    from conary.repository.netrepos.instances import INSTANCE_PRESENT_MISSING,\
        INSTANCE_PRESENT_NORMAL, INSTANCE_PRESENT_HIDDEN
    from conary.trove import TROVE_TYPE_NORMAL, TROVE_TYPE_REDIRECT, \
         TROVE_TYPE_REMOVED

    # LATEST_TYPE_ANY: redirects, removed, and normal
    if "LatestViewAny_sub" not in db.views:
        cu.execute("""
        %(CREATEVIEW)s LatestViewAny_sub AS
        SELECT
            ugi.userGroupId AS userGroupId,
            n.itemId AS itemId,
            n.branchId AS branchId,
            i.flavorId AS flavorId,
            max(n.finalTimestamp) AS finalTimestamp
        FROM UserGroupInstancesCache AS ugi
        JOIN Instances AS i USING(instanceId)
        JOIN Nodes AS n USING(itemId, versionId)
        WHERE i.isPresent = %%(present)d
        GROUP BY ugi.userGroupId, n.itemId, n.branchId, i.flavorId
        """ % db.keywords % {"present" : INSTANCE_PRESENT_NORMAL, })
        db.views["LatestViewAny_sub"] = True
        commit = True
    if "LatestViewAny"  not in db.views:
        cu.execute("""
        %(CREATEVIEW)s LatestViewAny AS
        SELECT
            sub.userGroupId AS userGroupId,
            sub.itemId AS itemId,
            sub.branchId AS branchId,
            sub.flavorId AS flavorId,
            Nodes.versionId AS versionId
        FROM LatestViewAny_sub as sub
        JOIN Nodes USING(itemId, branchId, finalTimestamp)
        JOIN Instances USING(itemId, versionId)
        WHERE Instances.flavorId = sub.flavorId
          AND Instances.isPresent = %%(present)d
        """ % db.keywords % {"present" : INSTANCE_PRESENT_NORMAL})
        db.views["LatestViewAny"] = True
        commit = True

    # LATEST_TYPE_PRESENT: redirects and normal
    if "LatestViewPresent_sub" not in db.views:
        cu.execute("""
        %(CREATEVIEW)s LatestViewPresent_sub AS
        SELECT
            ugi.userGroupId AS userGroupId,
            n.itemId AS itemId,
            n.branchId AS branchId,
            i.flavorId AS flavorId,
            max(n.finalTimestamp) AS finalTimestamp
        FROM UserGroupInstancesCache AS ugi
        JOIN Instances AS i USING(instanceId)
        JOIN Nodes AS n USING(itemId, versionId)
        WHERE i.isPresent = %%(present)d
          AND i.troveType != %%(removed)d
        GROUP BY ugi.userGroupId, n.itemId, n.branchId, i.flavorId
        """ % db.keywords % { "present": INSTANCE_PRESENT_NORMAL,
                              "removed"  : TROVE_TYPE_REMOVED, })
        db.views["LatestViewPresent_sub"] = True
        commit = True
    if "LatestViewPresent"  not in db.views:
        cu.execute("""
        %(CREATEVIEW)s LatestViewPresent AS
        SELECT
            sub.userGroupId AS userGroupId,
            sub.itemId AS itemId,
            sub.branchId AS branchId,
            sub.flavorId AS flavorId,
            Nodes.versionId AS versionId
        FROM LatestViewPresent_sub as sub
        JOIN Nodes USING(itemId, branchId, finalTimestamp)
        JOIN Instances USING(itemId, versionId)
        WHERE Instances.flavorId = sub.flavorId
          AND Instances.isPresent = %%(present)d
          AND Instances.troveType != %%(trove)d
        """ % db.keywords % {"present" : INSTANCE_PRESENT_NORMAL,
                             "trove" : TROVE_TYPE_REMOVED, })
        db.views["LatestViewPresent"] = True
        commit = True

    # LATEST_TYPE_NORMAL: hide branches which end in redirects
    if "LatestViewNormal" not in db.views:
        assert("LatestViewPresent_sub" in db.views)
        cu.execute("""
        %(CREATEVIEW)s LatestViewNormal AS
        SELECT
            sub.userGroupId AS userGroupId,
            sub.itemId AS itemId,
            sub.branchId AS branchId,
            sub.flavorId AS flavorId,
            Nodes.versionId AS versionId
        FROM LatestViewPresent_sub as sub
        JOIN Nodes USING(itemId, branchId, finalTimestamp)
        JOIN Instances USING(itemId, versionId)
        WHERE Instances.flavorId = sub.flavorId
          AND Instances.isPresent = %%(present)d
          AND Instances.troveType = %%(trove)d
        """ % db.keywords % {"present" : INSTANCE_PRESENT_NORMAL,
                             "trove" : TROVE_TYPE_NORMAL,
                             "removed" : TROVE_TYPE_REMOVED, })
        db.views["LatestViewNormal"] = True
        commit = True

    # LatestView is a union of the 3 smaller latest views
    if "LatestView" not in db.views:
        cu.execute("""
        %%(CREATEVIEW)s LatestView AS
        SELECT %d as latestType, userGroupId, itemId, branchId, flavorId, versionId
        FROM LatestViewAny
        UNION ALL
        SELECT %d as latestType, userGroupId, itemId, branchId, flavorId, versionId
        FROM LatestViewPresent
        UNION ALL
        SELECT %d as latestType, userGroupId, itemId, branchId, flavorId, versionId
        FROM LatestViewNormal
        """ % (LATEST_TYPE_ANY,LATEST_TYPE_PRESENT,LATEST_TYPE_NORMAL) % db.keywords)
        db.views["LatestView"] = True
        commit = True

    # Latest, as seen by each usergroup
    if "LatestCache" not in db.tables:
        assert("Items" in db.tables)
        assert("Branches" in db.tables)
        assert("Flavors" in db.tables)
        assert("Versions" in db.tables)
        assert("UserGroups" in db.tables)
        cu.execute("""
        CREATE TABLE LatestCache(
            userGroupId     INTEGER NOT NULL,
            itemId          INTEGER NOT NULL,
            branchId        INTEGER NOT NULL,
            flavorId        INTEGER NOT NULL,
            versionId       INTEGER NOT NULL,
            latestType      INTEGER NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT LC_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES UserGroups(userGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT LC_itemId_fk
                FOREIGN KEY (itemId) REFERENCES Items(itemId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT LC_branchId_fk
                FOREIGN KEY (branchId) REFERENCES Branches(branchId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT LC_flavorId_fk
                FOREIGN KEY (flavorId) REFERENCES Flavors(flavorId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT LC_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["LatestCache"] = []
        commit = True
    if withIndexes:
        # sanity index that isn't very useful as an index due to its size...
        db.createIndex("LatestCache", "LC_userGroupId_uniq",
                       "userGroupId,latestType,itemId,branchId,flavorId",
                       unique=True)
        # create needed FKs
        db.createIndex("LatestCache", "LC_itemId_fk", "itemId")
        db.createIndex("LatestCache", "LC_branchId_fk", "branchId")
        db.createIndex("LatestCache", "LC_flavorId_fk", "flavorId")
        db.createIndex("LatestCache", "LC_versionId_fk", "versionId")
        if createTrigger(db, "LatestCache"):
            commit = True

    if commit:
        db.loadSchema()


def createUsers(db):
    cu = db.cursor()
    commit = False

    if "Users" not in db.tables:
        cu.execute("""
        CREATE TABLE Users (
            userId          %(PRIMARYKEY)s,
            userName        VARCHAR(254) NOT NULL,
            salt            %(STRING)s,
            password        %(STRING)s,
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
            admin           INTEGER NOT NULL DEFAULT 0,
            accept_flags    %(STRING)s,
            filter_flags    %(STRING)s,
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
        assert("UserGroups" in db.tables)
        cu.execute("""
        CREATE TABLE Permissions (
            permissionId    %(PRIMARYKEY)s,
            userGroupId     INTEGER NOT NULL,
            labelId         INTEGER NOT NULL,
            itemId          INTEGER NOT NULL,
            canWrite        INTEGER NOT NULL DEFAULT 0,
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
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["Permissions"] = []
        commit = True
    # this serves as a fk index for userGroupId
    db.createIndex("Permissions", "PermissionsIdx",
                   "userGroupId, labelId, itemId", unique = True)
    db.createIndex("Permissions", "PermissionsLabelId_fk", "labelId, userGroupId")
    db.createIndex("Permissions", "PermissionsItemId_fk", "itemId, userGroupId")
    if createTrigger(db, "Permissions"):
        commit = True

    if commit:
        db.loadSchema()


def createEntitlements(db):
    cu = db.cursor()
    commit = False

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
    db.createIndex("EntitlementAccessMap", "EntitlementAMapUserGroupId_fk",
                   "userGroupId, entGroupId", unique = True)
    if createTrigger(db, "EntitlementAccessMap"):
        commit = True

    if commit:
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
                ON DELETE SET NULL ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["PGPKeys"] = []
        commit = True
    db.createIndex("PGPKeys", "PGPKeysFingerprintIdx",
                   "fingerprint", unique = True)
    db.createIndex("PGPKeys", "PGPKeysUserIdx", "userId")
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
        db.loadSchema()


def createTroves(db, createIndex = True):
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
    db.createIndex("FileStreams", "FileStreamsIdx", "fileId", unique = True)
    db.createIndex("FileStreams", "FileStreamsSha1Idx", "sha1", unique = False)
    if createTrigger(db, "FileStreams"):
        commit = True

    if idtable.createIdTable(db, "Dirnames", "dirnameId", "dirname", colType = 'PATHTYPE'):
        commit = True
    if idtable.createIdTable(db, "Basenames", "basenameId", "basename", colType = 'PATHTYPE'):
        commit = True

    if "FilePaths" not in db.tables:
        cu.execute("""
        CREATE TABLE FilePaths(
            filePathId      %(PRIMARYKEY)s,
            dirnameId       INTEGER NOT NULL,
            basenameId      INTEGER NOT NULL,
            pathId          %(BINARY16)s NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT      FilePaths_dirnameId_fk
                FOREIGN KEY (dirnameId) REFERENCES Dirnames(dirnameId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT      FilePaths_basenameId_fk
                FOREIGN KEY (basenameId) REFERENCES Basenames(basenameId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["FilePaths"] = []
        commit = True
    if createIndex:
        db.createIndex("FilePaths", "FilesPathDirnameIdx", "dirnameId")
        db.createIndex("FilePaths", "FilesPathBasenameIdx", "basenameId")
        if createTrigger(db, "FilePaths"):
            commit = True

    if "TroveFiles" not in db.tables:
        cu.execute("""
        CREATE TABLE TroveFiles(
            instanceId      INTEGER NOT NULL,
            streamId        INTEGER NOT NULL,
            versionId       INTEGER NOT NULL,
            filePathId      INTEGER NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT TroveFiles_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveFiles_streamId_fk
                FOREIGN KEY (streamId) REFERENCES FileStreams(streamId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveFiles_versionId_fk
                FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            CONSTRAINT TroveFiles_filePathId_fk
                FOREIGN KEY (filePathId) REFERENCES FilePaths(filePathId)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["TroveFiles"] = []
        commit = True
    if createIndex:
        db.createIndex("TroveFiles", "TroveFilesInstanceId_fk", "instanceId")
        db.createIndex("TroveFiles", "TroveFilesStreamId_fk", "streamId")
        db.createIndex("TroveFiles", "TroveFilesVersionId_fk", "versionId")
        db.createIndex("TroveFiles", "TroveFilesFilePathId_fk", "filePathId")
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
    db.createIndex("TroveInfo", "TroveInfoChangedIdx", "changed")
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
    db.createIndex("TroveRedirects", "TroveRedirectsItemId_fk", "itemId")
    db.createIndex("TroveRedirects", "TroveRedirectsBranchId_fk", "branchId")
    db.createIndex("TroveRedirects", "TroveRedirectsFlavorId_fk", "flavorId")
    if createTrigger(db, "TroveRedirects"):
        commit = True

    if commit:
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
    db.createIndex("Metadata", "MetadataItemId_fk", "itemId")
    db.createIndex("Metadata", "MetadataVersionId_fk", "versionId")
    db.createIndex("Metadata", "MetadataBranchId_fk", "branchId")
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
        db.loadSchema()


def createMirrorTracking(db):
    cu = db.cursor()
    if 'LatestMirror' not in db.tables:
        cu.execute("""
        CREATE TABLE LatestMirror(
            host            VARCHAR(254),
            mark            NUMERIC(14,0) NOT NULL
        ) %(TABLEOPTS)s""" % db.keywords)
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
        db.loadSchema()

def createLabelMap(db):
    commit = False
    cu = db.cursor()
    if "LabelMap" not in db.tables:
        cu.execute("""
        CREATE TABLE LabelMap(
            labelmapId      %(PRIMARYKEY)s,
            itemId          INTEGER NOT NULL,
            labelId         INTEGER NOT NULL,
            branchId        INTEGER NOT NULL,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
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
        createTrigger(db, "LabelMap")
        commit = True
    db.createIndex("LabelMap", "LabelMapLabelIdx", "labelId")
    db.createIndex("LabelMap", "LabelMapItemIdBranchIdIdx", "itemId, branchId")
    db.createIndex("LabelMap", "LabelMapBranchId_fk", "branchId")

    if commit:
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
    commit |= db.createIndex("Items", "Items_uq", "item", unique = True)
    if commit:
        db.loadSchema()


# cached access map for (userGroupId, instanceId)
def createAccessMaps(db):
    commit = False
    cu = db.cursor()
    # permissions by group. This only expresses/implies read
    # permissions; for write and remove the acls in Permissions are
    # controlling
    if "UserGroupTroves" not in db.tables:
        assert("UserGroups" in db.tables)
        assert("Instances" in db.tables)
        cu.execute("""
        CREATE TABLE UserGroupTroves(
            ugtId           %(PRIMARYKEY)s,
            userGroupId     INTEGER NOT NULL,
            instanceId      INTEGER NOT NULL,
            recursive       INTEGER NOT NULL DEFAULT 0,
            changed         NUMERIC(14,0) NOT NULL DEFAULT 0,
            CONSTRAINT UserGroupTroves_ugid_fk
                FOREIGN KEY (userGroupId) REFERENCES UserGroups(userGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT UserGroupTroves_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["UserGroupTroves"] = []
        commit = True
    db.createIndex("UserGroupTroves", "UserGroupTroves_userGroupIdIdx",
                   "userGroupId,instanceId", unique=True)
    db.createIndex("UserGroupTroves", "UserGroupTroves_instanceId_fk", "instanceId")
    if createTrigger(db, "UserGroupTroves"):
        commit = True

    # this is a flattened version of UserGroupTroves
    if "UserGroupAllTroves" not in db.tables:
        assert("UserGroups" in db.tables)
        assert("Instances" in db.tables)
        assert("UserGroupTroves" in db.tables)
        cu.execute("""
        CREATE TABLE UserGroupAllTroves(
            ugtId           INTEGER NOT NULL,
            userGroupId     INTEGER NOT NULL,
            instanceId      INTEGER NOT NULL,
            CONSTRAINT UGAT_ugtId_fk
                FOREIGN KEY (ugtId) REFERENCES UserGroupTroves(ugtId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT UGAT_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES UserGroups(userGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT UGAT_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["UserGroupAllTroves"] = []
        commit = True
    db.createIndex("UserGroupAllTroves", "UGAT_ugtId_fk", "ugtId")
    db.createIndex("UserGroupAllTroves", "UGAT_userGroupId_fk", "userGroupId")
    db.createIndex("UserGroupAllTroves", "UGAT_instanceId_fk", "instanceId")

    # this holds in a flat structure the expansion of the Permissions table
    if "UserGroupAllPermissions" not in db.tables:
        assert("UserGroups" in db.tables)
        assert("Instances" in db.tables)
        assert("Permissions" in db.tables)
        cu.execute("""
        CREATE TABLE UserGroupAllPermissions(
            permissionId    INTEGER NOT NULL,
            userGroupId     INTEGER NOT NULL,
            instanceId      INTEGER NOT NULL,
            canWrite        INTEGER NOT NULL DEFAULT 0,
            CONSTRAINT UGAP_permissionId_fk
                FOREIGN KEY (permissionId) REFERENCES Permissions(permissionId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT UGAP_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES UserGroups(userGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT UGAP_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["UserGroupAllPermissions"] = []
        commit = True
    db.createIndex("UserGroupAllPermissions", "UGAP_permissionId_fk", "permissionId")
    db.createIndex("UserGroupAllPermissions", "UGAP_userGroupId_fk", "userGroupId")
    db.createIndex("UserGroupAllPermissions", "UGAP_instanceId_fk", "instanceId")

    # cache of what troves a usergroup can see. Summarizes the stuff from
    # UserGroupAllTroves and UserGroupAllPermissions
    if "UserGroupInstancesCache" not in db.tables:
        assert("UserGroups" in db.tables)
        assert("Instances" in db.tables)
        cu.execute("""
        CREATE TABLE UserGroupInstancesCache(
            userGroupId     INTEGER NOT NULL,
            instanceId      INTEGER NOT NULL,
            canWrite        INTEGER NOT NULL DEFAULT 0,
            CONSTRAINT UGIC_userGroupId_fk
                FOREIGN KEY (userGroupId) REFERENCES UserGroups(userGroupId)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT UGIC_instanceId_fk
                FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["UserGroupInstancesCache"] = []
        commit = True
    db.createIndex("UserGroupInstancesCache", "UGIC_userGroupIdIdx", "userGroupId,instanceId",
                   unique=True)
    db.createIndex("UserGroupInstancesCache", "UGIC_instanceId_fk", "instanceId")

    if commit:
        db.loadSchema()


def createLockTables(db):
    commit = False
    cu = db.cursor()
    if "CommitLock" not in db.tables:
        cu.execute("""
            CREATE TABLE CommitLock(
                lockId          %(PRIMARYKEY)s,
                lockName        VARCHAR(254) NOT NULL
            ) %(TABLEOPTS)s""" % db.keywords)
        db.tables["CommitLock"] = []
        cu.execute("INSERT INTO CommitLock (lockId, lockName) VALUES(0, 'ALL')")
        commit = True
    db.createIndex("CommitLock", "CommitLockName_uq", "lockName", unique=True)

    if commit:
        db.loadSchema()


# sets up temporary tables for a brand new connection
def setupTempTables(db):
    logMe(3)
    cu = db.cursor()

    # the following are specific temp tables for various functions.
    if "tmpFlavorMap" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpFlavorMap(
            flavorId    INTEGER NOT NULL,
            base        VARCHAR(254) NOT NULL,
            sense       INTEGER,
            depClass    INTEGER NOT NULL,
            flag        VARCHAR(254)
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpFlavorMap"] = True
        db.createIndex("tmpFlavorMap", "tmpFlavorMapBaseIdx", "flavorId,depClass,base",
                       check = False)
        db.createIndex("tmpFlavorMap", "tmpFlavorMapSenseIdx", "flavorId,sense",
                       check = False)
    if "tmpNewStreams" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpNewStreams(
            fileId      %(BINARY20)s,
            stream      %(MEDIUMBLOB)s,
            sha1        %(BINARY20)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpNewStreams"] = True
        db.createIndex("tmpNewStreams", "tmpNewStreamssFileIdx", "fileId",
                       check = False)

    if "tmpNewPaths" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpNewPaths(
            path        %(PATHTYPE)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpNewPaths"] = True

    if "tmpNewBasenames" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpNewBasenames(
            dir         %(PATHTYPE)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpNewBasenames"] = True

    if "tmpNewFiles" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpNewFiles(
            pathId      %(BINARY16)s,
            versionId   INTEGER,
            fileId      %(BINARY20)s,
            dirNameId   INTEGER,
            baseNameId  INTEGER,
            pathChanged INTEGER,
            instanceId  INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpNewFiles"] = True
        # it sucks that we have to create this many indexes for this table;
        # that's what we get for a non-normalized reprezentation...
        db.createIndex("tmpNewFiles", "tmpNewFilesPathIdIdx", "pathId",
                       check = False)
        db.createIndex("tmpNewFiles", "tmpNewFilesFileIdx", "fileId",
                       check = False)
        db.createIndex("tmpNewFiles", "tmpNewFilesVersionIdx", "versionId",
                       check = False)
    if "tmpNewRedirects" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpNewRedirects(
            item        %(STRING)s,
            branch      %(STRING)s,
            flavor      %(STRING)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpNewRedirects"] = True
    if "tmpNVF" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpNVF(
            idx         %(PRIMARYKEY)s,
            name        VARCHAR(254),
            version     %(STRING)s,
            flavor      %(STRING)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpNVF"] = True
        db.createIndex("tmpNVF", "tmpNVFnameIdx", "name", check=False)
    if "tmpInstanceId" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpInstanceId(
            idx         INTEGER,
            instanceId  INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpInstanceId"] = True
        db.createIndex("tmpInstanceId", "tmpInstanceIdIdx1", "idx", check=False)
        db.createIndex("tmpInstanceId", "tmpInstanceIdIdx2", "instanceId", check=False)
    if "tmpTroveInfo" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpTroveInfo(
            idx         INTEGER,
            instanceId  INTEGER,
            infoType    INTEGER,
            data        %(MEDIUMBLOB)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpTroveInfo"] = True
        db.createIndex("tmpTroveInfo", "tmpTIidx", "idx", check = False)
        db.createIndex("tmpTroveInfo", "tmpTIinfoTypeIdx", "infoType, instanceId", check = False)
    if "tmpFileId" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpFileId(
            itemId      %(PRIMARYKEY)s,
            fileId      %(BINARY20)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpFileId"] = True
        db.createIndex("tmpFileId", "tmpFileIdFileIdIdx", "fileId", check=False)
    if "tmpIVF" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpIVF(
            item        VARCHAR(254),
            version     %(STRING)s,
            fullVersion %(STRING)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpIVF"] = True
    if "tmpGTVL" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpGTVL(
            item        %(STRING)s,
            versionSpec %(STRING)s,
            flavorId    INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpGTVL"] = True
        db.createIndex("tmpGTVL", "tmpGTVLitemIdx", "item", check = False)
    if "tmpFilePaths" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpFilePaths(
            row         INTEGER,
            dirname     %(PATHTYPE)s,
            basename    %(PATHTYPE)s
        )""" % db.keywords)
        db.tempTables["tmpFilePaths"] = True
        db.createIndex("tmpFilePaths", "tmpFilePathsDirnameIdx", "dirname", check=False)
    # used primarily for dependency resolution
    if "tmpInstances" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpInstances(
            instanceId    INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpInstances"] = True
        db.createIndex("tmpInstances", "tmpInstancesIdx", "instanceId", check=False)
    # general purpose temporary lists of integers
    if "tmpId" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpId(
            id    INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpId"] = True
        db.createIndex("tmpId", "tmpIdIdx", "id", check=False)
    # general purpose list of strings
    if "tmpItems" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpItems(
            itemId    INTEGER,
            item      %(STRING)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpItems"] = True
        db.createIndex("tmpItems", "tmpItemsItemIdIdx", "itemId", check=False)
        db.createIndex("tmpItems", "tmpItemsItemIdx", "item", check=False)
    # general purpose list of paths
    if "tmpPaths" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpPaths(
            id    INTEGER,
            path  %(PATHTYPE)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpPaths"] = True
        db.createIndex("tmpPaths", "tmpPathsIdIdx", "id", check=False)
        db.createIndex("tmpPaths", "tmpPathsPathIdx", "path", check=False)
    # for processing UserGroupInstancesCache entries
    if "tmpUGI" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpUGI(
            userGroupid   INTEGER,
            instanceId    INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpUGI"] = True
        db.createIndex("tmpUGI", "tmpUGIIdx", "instanceId,userGroupId",
                       unique=True, check=False)
    if "tmpTroves" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpTroves(
            idx             %(PRIMARYKEY)s,
            instanceId      INTEGER NOT NULL,
            item            VARCHAR(254),
            version         %(STRING)s,
            branch          %(STRING)s,
            label           %(STRING)s,
            timestamps      %(STRING)s,
            finalTimestamp  NUMERIC(13,3) NOT NULL,
            frozenVersion   %(STRING)s,
            flavor          %(STRING)s,
            flags           INTEGER NOT NULL DEFAULT 0,
            troveType       INTEGER NOT NULL DEFAULT 0
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpTroves"] = True
        # XXX: this index helps postgresql and hurts mysql.
        #db.createIndex("tmpTroves", "tmpTrovesIdx", "item",
        #               check = False)

    if "tmpNewTroves" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpNewTroves(
            itemId        INTEGER NOT NULL,
            branchId      INTEGER NOT NULL,
            flavorId      INTEGER NOT NULL,
            versionId     INTEGER NOT NULL,
            instanceId    INTEGER NOT NULL,
            hidden        INTEGER NOT NULL,
            oldInstanceId INTEGER,
            finalTimestamp      NUMERIC(13,3) NOT NULL,
            troveType       INTEGER NOT NULL DEFAULT 0
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpNewTroves"] = True

    if "tmpNewLatest" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpNewLatest(
            userGroupId     INTEGER NOT NULL,
            itemId          INTEGER NOT NULL,
            branchId        INTEGER NOT NULL,
            flavorId        INTEGER NOT NULL,
            versionId       INTEGER NOT NULL,
            latestType      INTEGER NOT NULL
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpNewLatest"] = True

    # for processing markRemoved
    if "tmpRemovals" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpRemovals(
            instanceId      INTEGER,
            itemId          INTEGER,
            versionId       INTEGER,
            flavorId        INTEGER,
            branchId        INTEGER
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpRemovals"] = True
        db.createIndex("tmpRemovals", "tmpRemovalsInstances", "instanceId",
                       check = False)
    # for processing getDepSuggestion
    if "tmpDeps" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpDeps(
            idx         INTEGER,
            depNum      INTEGER NOT NULL,
            class       INTEGER NOT NULL,
            name        VARCHAR(254) NOT NULL,
            flag        VARCHAR(254) NOT NULL
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpDeps"] = True
        db.createIndex("tmpDeps", "tmpDepsIdx", "idx",
                       check = False)
        db.createIndex("tmpDeps", "tmpDepsClassIdx", "class",
                       check = False)
        db.createIndex("tmpDeps", "tmpDepsNameIdx", "name",
                       check = False)
    if "tmpDepNum" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpDepNum(
            idx         INTEGER NOT NULL,
            depNum      INTEGER NOT NULL,
            flagCount   INTEGER NOT NULL
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpDepNum"] = True
        db.createIndex("tmpDepNum", "tmpDepNumIdx", "idx, depNum",
                       check = False, unique=True)
    # for processing intermediary results for pathid lookups
    if "tmpPathIdLookup" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpPathIdLookup(
            versionId           INTEGER NOT NULL,
            filePathId          INTEGER NOT NULL,
            streamId            INTEGER NOT NULL,
            finalTimestamp      NUMERIC(13,3) NOT NULL
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpPathIdLookup"] = True

    if "tmpSha1s" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpSha1s(
            sha1        %(BINARY20)s
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpSha1s"] = True

    if "tmpGroupInsertShim" not in db.tempTables:
        cu.execute("""
        CREATE TEMPORARY TABLE tmpGroupInsertShim(
            itemId        INTEGER NOT NULL,
            versionId     INTEGER NOT NULL,
            flavorId      INTEGER NOT NULL,
            flags         INTEGER NOT NULL,
            instanceId    INTEGER NOT NULL
        ) %(TABLEOPTS)s""" % db.keywords)
        db.tempTables["tmpGroupInsertShim"] = True


def resetTable(cu, name):
    cu.execute("DELETE FROM %s" % name, start_transaction=False)


# create the (permanent) server repository schema
def createSchema(db, commit=True):
    if not hasattr(db, "tables"):
        db.loadSchema()

    createIdTables(db)
    createLabelMap(db)
    createFlavors(db)
    createInstances(db)
    createNodes(db)

    createUsers(db)
    createEntitlements(db)
    createPGPKeys(db)
    createAccessMaps(db)

    createChangeLog(db)
    createLatest(db)

    createTroves(db)

    createDependencies(db, skipCommit=True)
    createMetadata(db)
    createMirrorTracking(db)

    createLockTables(db)

    if commit:
        db.commit()

# we can only serialize commits after db schema 16.1. We need to
# do this in a way that avoids the necessity of a major version schema bump
def lockCommits(db):
    if db.version < sqllib.DBversion(16,1):
        logMe(1, "WARNING: commitlock noop", db.version)
        return True # noop, can't do it reliably without the CommitLock table
    cu = db.cursor()
    # on MySQL this will timout after lock_timeout seconds. MySQL's
    # server config has to be set to a reasonable value
    cu.execute("update CommitLock set lockName = lockName")
    return True

# this should only check for the proper schema version. This function
# is called usually from the multithreaded setup, so schema operations
# should be avoided here
def checkVersion(db):
    global VERSION
    version = db.getVersion()
    logMe(2, "current =", version, "required =", VERSION)

    # test for no version
    if version == 0:
        raise sqlerrors.SchemaVersionError("""
        Your database schema is not initalized or it is too old.  Please
        run the standalone server with the --migrate argument to
        upgrade/initialize the database schema for the Conary Repository.

        Current schema version is %s; Required schema version is %s.
        """ % (version, VERSION), version)

    # the major versions must match
    if version.major != VERSION.major:
        raise sqlerrors.SchemaVersionError("""
        This code schema version does not match the Conary repository
        database schema that you are running.

        Current schema version is %s; Required schema version is %s.
        """ % (version, VERSION), version)
    # the minor numbers are considered compatible up and down across a major
    return version

# run through the schema creation and migration (if required)
def loadSchema(db, doMigrate=False):
    global VERSION
    try:
        version =  checkVersion(db)
    except sqlerrors.SchemaVersionError, e:
        version = e.args[0]
    logMe(1, "current =", version, "required =", VERSION)
    # load the current schema object list
    db.loadSchema()

    # avoid a recursive import by importing just what we need
    from conary.server import migrate

    # expedite the initial repo creation
    if version == 0:
        createSchema(db)
        db.loadSchema()
        setVer = migrate.majorMinor(VERSION)
        return db.setVersion(setVer)
    # test if  the repo schema is newer than what we understand
    # (by major schema number)
    if version.major > VERSION.major:
        raise sqlerrors.SchemaVersionError("""
        The repository schema version is newer and incompatible with
        this code base. You need to update conary code to a version
        that undersand repo schema %s""" % version, version)
    # now we need to perform a schema migration
    if version.major < VERSION.major and not doMigrate:
        raise sqlerrors.SchemaVersionError("""
        Repository schema needs to have a major schema update performed.
        Please run server.py with --migrate option to perform this upgrade.
        """, version, VERSION)
    # now the version.major is smaller than VERSION.major - but is it too small?
    # we only support migrations from schema 13 on
    if version < 13:
        raise sqlerrors.SchemaVersionError("""
        Repository schemas from Conary versions older than 1.0 are not
        supported. Contact rPath for help converting your repository to
        a supported version.""", version)
    # compatible schema versions have the same major
    if version.major == VERSION.major and not doMigrate:
        return version
    # if we reach here, a schema migration is needed/requested
    version = migrate.migrateSchema(db)
    db.loadSchema()
    # run through the schema creation to create any missing objects
    logMe(2, "checking for/initializing missing schema elements...")
    createSchema(db)
    if version > 0 and version.major != VERSION.major:
        # schema creation/conversion failed. SHOULD NOT HAPPEN!
        raise sqlerrors.SchemaVersionError("""
        Schema migration process has failed to bring the database
        schema version up to date. Please report this error at
        http://issues.rpath.com/.

        Current schema version is %s; Required schema version is %s.
        """ % (version, VERSION))
    db.loadSchema()
    return VERSION
