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

# old migration code that we no longer need for live repo support

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

