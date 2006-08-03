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

import sys

from conary import files, trove, versions
from conary.dbstore import migration, sqlerrors
from conary.lib.tracelog import logMe
from conary.deps import deps
from conary.repository.netrepos import versionops, trovestore
from conary.server import schema

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
        schema.createPGPKeys(self.db)

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
        schema.createDependencies(self.db)
        schema.setupTempDepTables(self.db)
        troves = []

        logMe(1, 'Reading %d instances' % len(instances))
        for i, instanceId in enumerate(instances):
            trv = FakeTrove()
            dtbl.get(self.cu, trv, instanceId)
            troves.append(trv)

        self.cu.execute("delete from dependencies")
        self.cu.execute("delete from requires")
        self.cu.execute("delete from provides")

        logMe(1, 'Reading %d instances' % len(instances))
        for i, (instanceId, trv) in enumerate(itertools.izip(instances, troves)):
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
        schema.createUsers(self.db)
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
                logMe(1, "add changed column and triggers to", table)
            except sqlerrors.DuplicateColumnName:
                # the column already exists, probably because we created
                # a brand new table.  Then it would use the already-current
                # schema
                pass
            schema.createTrigger(self.db, table)
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
        logMe(1, "Updating the TroveTroves table...")
        self.cu.execute("""
        INSERT INTO TroveTroves2
        (instanceId, includedId, flags, changed)
            SELECT instanceId, includedId,
                   CASE WHEN byDefault THEN %d ELSE 0 END,
                   changed
            FROM TroveTroves""" % schema.TROVE_TROVES_BYDEFAULT)
        self.cu.execute("DROP TABLE TroveTroves")
        self.cu.execute("ALTER TABLE TroveTroves2 RENAME TO TroveTroves")
        # reload the schema and call createTrove() to fill in the missing triggers and indexes
        self.db.loadSchema()
        logMe(1, "Updating indexes and triggers...")
        schema.createTroves(self.db)
        # we changed the Instances update trigger to protect the changed column from changing
        self.db.dropTrigger("Instances", "UPDATE")
        schema.createTrigger(self.db, "Instances", pinned=True)
        # done...
        return self.Version

class MigrateTo_10(SchemaMigration):
    Version = 10
    def migrate(self):
        from  conary import trove
        logMe(1, "Updating index TroveInfoTypeIdx")
        # redo the troveInfoTypeIndex to be UNIQUE
        self.db.dropIndex("TroveInfo", "TroveInfoTypeIdx")
        self.db.createIndex("TroveInfo", "TroveInfoTypeIdx",
                            "infoType, instanceId", unique = True)
        logMe(1, "Updating index InstancesChangedIdx")
        # add instanceId to the InstancesChanged index
        self.db.dropIndex("Instances", "InstancesChangedIdx")
        self.db.createIndex("Instances", "InstancesChangedIdx",
                            "changed, instanceId")
        # add the clonedFrom column to the Instances table
        logMe(1, "Adding column and index for Instances.clonedFromId")
        self.cu.execute("ALTER TABLE Instances ADD COLUMN "
                        "clonedFromId INTEGER REFERENCES Versions(versionId) "
                        "ON DELETE RESTRICT ON UPDATE CASCADE")
        self.db.createIndex("Instances", "InstancesClonedFromIdx",
                            "clonedFromId, instanceId")
        # add the sourceItemId to the Nodes table
        logMe(1, "Adding column and index for Nodes.sourceItemId")
        self.cu.execute("ALTER TABLE Nodes ADD COLUMN "
                        "sourceItemId INTEGER REFERENCES Items(itemId) "
                        "ON DELETE RESTRICT ON UPDATE CASCADE")
        self.db.createIndex("Nodes", "NodesSourceItemIdx",
                            "sourceItemId, branchId")
        # update Versions, Instances and clonedFromId
        logMe(1, "Updating the Versions table...")
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
        logMe(1, "Extracting data for Instances.clonedFromId from TroveInfo")
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
        logMe(1, "Updating the Items table...")
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
        logMe(1, "Extracting data for Nodes.sourceItemId from TroveInfo")
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

	logMe(1, "Rebuilding the Latest table...")
        cu.execute("DROP TABLE Latest")
        self.db.loadSchema()
        schema.createLatest(self.db)
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
        logMe(1, "Finding path hashes needing an update...")
        rows = cu2.execute("SELECT instanceId,data from TroveInfo "
                           "WHERE infoType=?", trove._TROVEINFO_TAG_PATH_HASHES)
        neededChanges = []
        PathHashes = trove.PathHashes
        for instanceId, data in rows:
            frzn = PathHashes(data).freeze()
            if frzn != data:
                cu.execute('INSERT INTO hashUpdatesTmp VALUES (?, ?)',
                           (instanceId, cu.binary(frzn)))

        logMe(1, "removing bad signatures due to path hashes...")
        cu.execute("""
        DELETE FROM TroveInfo
        WHERE infoType=?
          AND instanceId IN (SELECT instanceId from hashUpdatesTmp)
        """, trove._TROVEINFO_TAG_SIGS)

        logMe(1, "updating path hashes...")
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
        logMe(1, "Fixing NULL path hashes...")
        cu.execute(
            "SELECT instanceId FROM TroveInfo WHERE data IS NULL and infotype = ?",
            trove._TROVEINFO_TAG_PATH_HASHES)
        for instanceId, in cu.fetchall():
            cu.execute("SELECT path FROM TroveFiles WHERE instanceId=?", instanceId)
            ph = trove.PathHashes()
            for path, in cu:
                ph.addPath(path)
            cu.execute(
                "UPDATE TroveInfo SET data=? WHERE instanceId=? and infotype=?",
                (cu.binary(ph.freeze()), instanceId, trove._TROVEINFO_TAG_PATH_HASHES))
        return self.Version

class MigrateTo_13(SchemaMigration):
    Version = 13
    def migrate(self):
        from conary import files
        # fix the duplicate FileStreams.fileId fields
        logMe(1, "Looking for duplicate fileId entries...")
        # this takes a bit to execute, especially on sqlite
        self.cu.execute("""
        CREATE TEMPORARY TABLE origs AS
            SELECT a.streamId  AS streamId,
                   a.fileId    AS fileId
            FROM FileStreams AS a JOIN FileStreams AS b
            where a.fileId = b.fileId
              and a.streamId < b.streamId
              and a.fileId is not null
        """)
        # all the duplicate fileIds that have a streamId not in the
        # origs table are dupes
        # First, check that the duplicate streams differ only by the mtime field
        logMe(1, "Checking duplicate fileId streams...")
        self.cu.execute("""
        SELECT fs.streamId, fs.fileId, fs.stream
        FROM origs JOIN FileStreams AS fs USING(streamId)
        """)
        cu2 = self.db.cursor()
        for (streamId, fileId, stream) in self.cu:
            if stream is not None:
                file = files.ThawFile(self.cu.frombinary(stream), None)
            else:
                file = None
            # select all other streams with the same streamId
            cu2.execute("""
            SELECT fs.streamId, fs.stream
            FROM FileStreams AS fs
            WHERE fs.fileId = ?
              AND fs.streamId != ?
            """, (fileId, streamId))
            for (dupStreamId, dupStream) in cu2:
                if file is None: # match None with None only
                    assert (dupStream is None)
                    continue
                file2 = files.ThawFile(cu2.frombinary(dupStream), None)
                file2.inode.mtime.set(file.inode.mtime())
                assert (file == file2)
        logMe(1, "Removing references to duplicate fileId entries...")
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
        logMe(1, "Droping old fileId index...")
        self.db.dropIndex("FileStreams", "FileStreamsIdx")
        logMe(1, "Recreating the fileId index...")
        schema.createTroves(self.db)

        # flavorId = 0 is now ''
        self.cu.execute("UPDATE Flavors SET flavor = '' WHERE flavorId = 0")

        logMe(1, "Changing absolute redirects to branch redirects...")
        self.cu.execute("""
                    SELECT instanceId, item, version FROM Instances 
                        JOIN Items USING (itemId)
                        JOIN Versions ON Instances.versionId = Versions.versionId
                        WHERE isRedirect = 1""")

        redirects = [ x for x in self.cu ]
        # the redirect conversion code is broken
        if len(redirects) > 0:
            msg = ("ERROR: old-style redirects have been found in this "
                   "repository, but the code to convert them is incomplete. "
                   "Please contact rPath for support.")
            logMe(1, msg)
            raise sqlerrors.SchemaVersionError(msg)

        for instanceId, name, version in self.cu:
            l = name.split(":")
            pkgName = l[0]
            if len(l) > 1:
                compName = l[1]
            else:
                compName = None

            branchStr = versions.VersionFromString(version).branch().asString()

            includedTroves = cu2.execute("""
            SELECT Items.item, Instances.itemId, Versions.version,
                   TroveTroves.includedId, Instances.flavorId
            FROM TroveTroves
            JOIN Instances ON TroveTroves.includedId = Instances.instanceId
            JOIN Items USING (itemId)
            JOIN Versions ON Instances.versionId = Versions.versionId
            WHERE TroveTroves.instanceId=?
            """, instanceId).fetchall()

            for subName, subItemId, subVersion, includedInstanceId, \
                                        subFlavorId in includedTroves:
                l = subName.split(":")
                subPkgName = l[0]
                if len(l) > 1:
                    subCompName = l[1]
                else:
                    subCompName = None

                subVersion = versions.VersionFromString(subVersion)
                subBranchStr = subVersion.branch().asString()
                if subPkgName != pkgName or branchStr != subBranchStr:
                    if compName == subCompName:
                        branchId = cu2.execute("SELECT branchId FROM "
                                               "Branches WHERE branch=?", 
                                               branchStr).fetchall()
                        if not branchId:
                            cu2.execute("INSERT INTO Branches (branch) "
                                        "VALUES (?)", branchStr)
                            branchId = cu2.lastrowid
                        else:
                            branchId = branchId[0][0]

                        cu2.execute("""
                                INSERT INTO TroveRedirects
                                    (instanceId, itemId, branchId, flavorId)
                                    VALUES (?, ?, ?, NULL)""",
                                instanceId, subItemId, branchId)

                    # we need to move this redirect to the redirect table
                    cu2.execute("""
                            DELETE FROM TroveTroves WHERE
                                instanceId=? AND includedId=?
                            """, instanceId, includedInstanceId)
            cu2.execute("DELETE FROM TroveInfo WHERE instanceId=? AND "
                        "infoType=?", (instanceId, trove._TROVEINFO_TAG_SIGS))
        # all done for migration to 13
        return self.Version

class MigrateTo_14(SchemaMigration):
    Version = 14
    def migrate(self):
        self.message('WARNING: do NOT interrupt this migration, you will leave your DB in a messy state')

        updateCursor = self.db.cursor()
        self.cu.execute("""
        CREATE TEMPORARY TABLE tmpSha1s(
        streamId INTEGER,
        sha1  BINARY(20))""",
                        start_transaction=False)
        self.cu.execute('CREATE INDEX tmpSha1sIdx ON tmpSha1s(streamId)')
        total = self.cu.execute('SELECT max(streamId) FROM FileStreams').fetchall()[0][0]
        pct = 0
        for idx, (streamId, fileId, stream) in \
                enumerate(updateCursor.execute("SELECT streamId, fileId, stream FROM "
                                "FileStreams ORDER BY StreamId")):
            if stream and files.frozenFileHasContents(stream):
                contents = files.frozenFileContentInfo(stream)
                sha1 = contents.sha1()
                self.cu.execute("INSERT INTO tmpSha1s (streamId, sha1) VALUES (?,?)",
                                (streamId, self.cu.binary(sha1)))
            newPct = (streamId * 100)/total
            if newPct >= pct:
                self.message('Calculating sha1 for fileStream %s/%s (%02d%%)...' % (streamId, total, pct))
                pct = newPct + 5

        self.message('Populating FileStream Table with sha1s...')

        # delay this as long as possible, any CTRL-C after this point
        # will make future migrations fail.
        self.cu.execute("ALTER TABLE FileStreams ADD COLUMN "
                        "sha1        %(BINARY20)s"
                        % self.db.keywords)

        self.cu.execute("""
        UPDATE FileStreams
        SET sha1 = (
            SELECT sha1 FROM tmpSha1s
            WHERE FileStreams.streamid = tmpSha1s.streamid )
        """)
        self.cu.execute('DROP TABLE tmpSha1s')

        # because of the foreign key ereferntial mess, we need to
        # destroy the FKs relationships, recreate the Entitlement
        # tables, and restore the data
        self.message('Updating Entitlements...')
        self.cu.execute("CREATE TABLE Entitlements2 AS SELECT * FROM "
                        "Entitlements")
        self.cu.execute("CREATE TABLE EntitlementGroups2 AS SELECT * FROM "
                        "EntitlementGroups")
        self.cu.execute("CREATE TABLE EntitlementOwners2 AS SELECT * FROM "
                        "EntitlementOwners")
        self.cu.execute("DROP TABLE Entitlements")
        self.cu.execute("DROP TABLE EntitlementOwners")
        self.cu.execute("DROP TABLE EntitlementGroups")

        self.db.loadSchema()
        schema.createUsers(self.db)

        self.cu.execute("INSERT INTO EntitlementGroups (entGroup, entGroupId) "
                        "SELECT entGroup, entGroupId FROM EntitlementGroups2")
        self.cu.execute("INSERT INTO EntitlementAccessMap (entGroupId, "
                        "userGroupId) SELECT entGroupId, userGroupId FROM "
                        "EntitlementGroups2")
        self.cu.execute("INSERT INTO Entitlements SELECT * FROM Entitlements2")
        self.cu.execute("INSERT INTO EntitlementOwners SELECT * FROM EntitlementOwners2")
        self.cu.execute("DROP TABLE Entitlements2")
        self.cu.execute("DROP TABLE EntitlementGroups2")
        self.cu.execute("DROP TABLE EntitlementOwners2")

        self.message("Updating the Permissions table...")
        self.cu.execute("ALTER TABLE Permissions ADD COLUMN "
                        "canRemove   INTEGER NOT NULL DEFAULT 0"
                        % self.db.keywords)

        self.message("Updating Instances table...")
        self.db.renameColumn("Instances", "isRedirect", "troveType")
        self.db.loadSchema()

        return self.Version

class MigrateTo_15(SchemaMigration):
    Version = 15

    def updateLatest(self, cu):
        self.message("Updating the Latest table...")
        cu.execute("DROP TABLE Latest")
        self.db.loadSchema()
        schema.createLatest(self.db)

        cu.execute("""
            insert into Latest (itemId, branchId, flavorId, versionId,
                                latestType)
                select
                    instances.itemid as itemid,
                    nodes.branchid as branchid,
                    instances.flavorid as flavorid,
                    nodes.versionid as versionid,
                    %d
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
        """ % versionops.LATEST_TYPE_ANY)

        self.cu.execute("""
            insert into Latest (itemId, branchId, flavorId, versionId,
                                latestType)
                select
                    instances.itemid as itemid,
                    nodes.branchid as branchid,
                    instances.flavorid as flavorid,
                    nodes.versionid as versionid,
                    %d
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
                        and i.troveType != %d
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
        """ % (versionops.LATEST_TYPE_PRESENT, trove.TROVE_TYPE_REMOVED))

        self.cu.execute("""
            insert into Latest (itemId, branchId, flavorId, versionId,
                                latestType)
                select
                    instances.itemid as itemid,
                    latest.branchid as branchid,
                    instances.flavorid as flavorid,
                    instances.versionid as versionid,
                    %d
                from Latest join Instances
                    using (itemId, versionId, flavorId)
                where
                    latest.latestType = %d AND
                    instances.troveType = %d
        """ % (versionops.LATEST_TYPE_NORMAL,
               versionops.LATEST_TYPE_PRESENT, trove.TROVE_TYPE_NORMAL))

    # update a trove signatures, if required
    def fixTroveSig(self, repos, instanceId):
        cu = self.db.cursor()
        cu.execute("""
        select Items.item as name, Versions.version, Flavors.flavor
        from Instances
        join Items using (itemId)
        join Versions on
            Instances.versionId = Versions.versionId
        join Flavors on
            Instances.flavorId = Flavors.flavorId
        where Instances.instanceId = ?""", instanceId)
        (name, version, flavor) = cu.fetchall()[0]
        # check the signature
        trv = repos.getTrove(name, versions.VersionFromString(version),
                             deps.ThawFlavor(flavor))
        if trv.verifySignatures():
            return
        self.message("updating trove sigs: %s %s %s" % (name, version, flavor))
        trv.computeSignatures()
        cu.execute("delete from TroveInfo where instanceId = ? "
                   "and infoType = ?", (instanceId, trove._TROVEINFO_TAG_SIGS))
        cu.execute("insert into TroveInfo (instanceId, infoType, data) "
                   "values (?, ?, ?)", (
            instanceId, trove._TROVEINFO_TAG_SIGS,
            cu.binary(trv.troveInfo.sigs.freeze())))

    def fixRedirects(self, cu, repos):
        self.message("removing dep provisions from redirects...")
        # remove dependency provisions from redirects -- the conary 1.0
        # branch would set redirects to provide their own names. this doesn't
        # clean up the dependency table; that would only matter on a trove
        # which was cooked as only a redirect in the repository; any other
        # instances would still need the depId anyway
        cu.execute("delete from provides where instanceId in "
                   "(select instanceId from instances where troveType=?)",
                   trove.TROVE_TYPE_REDIRECT)
        # loop over redirects...
        cu.execute("select instanceId from instances where troveType = ?",
                   trove.TROVE_TYPE_REDIRECT)
        for (instanceId,) in cu:
            self.fixTroveSig(repos, instanceId)

    # fix the duplicate path problems, if any
    def fixDuplicatePaths(self, cu, repos):
        self.db.dropIndex("TroveFiles", "TroveFilesPathIdx")
        # it is faster to select all the (instanceId, path) pairs into
        # an indexed table than create a non-unique index, do work,
        # drop the non-unique index and recreate it as a unique one
        cu.execute("""
        create temporary table tmpDupPath(
            instanceId integer not null,
            path varchar(767) not null
        ) %(TABLEOPTS)s""" % self.db.keywords)
        self.db.createIndex("tmpDupPath", "tmpDupPathIdx",
                            "instanceId, path", check = False)
        cu.execute("""
        create temporary table tmpDups(
            counter integer,
            instanceId integer,
            path varchar(767)
        ) %(TABLEOPTS)s""" % self.db.keywords)
        self.message("searching the trovefiles table...")
        cu.execute("insert into tmpDupPath (instanceId, path) "
                   "select instanceId, path from TroveFiles")
        self.message("looking for troves with duplicate paths...")
        cu.execute("""insert into tmpDups (counter, instanceId, path)
        select count(*) as c, instanceId, path
        from tmpDupPath
        group by instanceId, path
        having c > 1""")
        counter = cu.execute("select count(*) from tmpDups").fetchall()[0][0]
        self.message("detected %d duplicates" % (counter,))
        # loop over every duplicate and apply the appropiate fix
        cu.execute("select instanceId, path from tmpDups")
        for (instanceId, path) in cu.fetchall():
            cu.execute("""select distinct
            instanceId, streamId, versionId, pathId, path
            from trovefiles where instanceId = ? and path = ?
            order by streamId, versionId, pathId""", (instanceId, path))
            ret = cu.fetchall()
            # delete all the duplicates and put the first one back
            cu.execute("delete from trovefiles "
                       "where instanceId = ? and path = ?",
                       (instanceId, path))
            # in case they are different, we pick the oldest, chances are it is
            # more "original"
            cu.execute("insert into trovefiles "
                       "(instanceId, streamId, versionId, pathId, path) "
                       "values (?,?,?,?,?)", ret[0])
            if len(ret) > 1:
                # need to recompute the sha1 - we might have changed the trove manifest
                # if the records were different
                self.fixTroveSig(repos, instanceId)

        # recreate the indexes and triggers - including new path
        # index for TroveFiles.  Also recreates the indexes table.
        self.message('Recreating indexes... (this could take a while)')
        cu.execute("drop table tmpDups")
        cu.execute("drop table tmpDupPath")
        schema.createTroves(self.db)
        self.message('Indexes created.')

    # add the CapId reference to Permissions and Latest
    def fixPermissions(self):
        # because we need to add a foreign key to the Permissions and Latest
        # tables, it is easier if we save the tables and recreate them
        self.message("Updating the Permissions table...")
        cu = self.db.cursor()
        cu.execute("""create table tmpPerm as
        select permissionId, userGroupId, labelId, itemId, admin, canWrite, canRemove
        from Permissions""")
        cu.execute("drop table Permissions")
        self.db.loadSchema()
        schema.createUsers(self.db)
        cu.execute("""insert into Permissions
        (permissionId, userGroupId, labelId, itemId, admin, canWrite, canRemove)
        select permissionId, userGroupId, labelId, itemId, admin, canWrite, canRemove
        from tmpPerm
        """)
        cu.execute("drop table tmpPerm")

    def migrate(self):
        schema.setupTempTables(self.db)
        cu = self.db.cursor()
        # needed for signature recalculation
        repos = trovestore.TroveStore(self.db)

        self.fixRedirects(cu, repos)
        self.fixDuplicatePaths(cu, repos)
        self.fixPermissions()
        self.updateLatest(cu)
        return self.Version


# entrry point that migrates the schema
def migrateSchema(db, version):
    # instantiate and call appropriate migration objects in succession.
    while version and version < schema.VERSION:
        version = (lambda x : sys.modules[__name__].__dict__[ \
                            'MigrateTo_' + str(x + 1)])(version)(db)()
    return version
