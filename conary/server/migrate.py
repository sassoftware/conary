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


import os
import sys

from conary import files, trove, versions
from conary.dbstore import migration, sqlerrors, sqllib, idtable
from conary.lib.tracelog import logMe
from conary.deps import deps
from conary.repository.netrepos import versionops, trovestore, \
     flavors, accessmap
from conary.server import schema

# SCHEMA Migration
class SchemaMigration(migration.SchemaMigration):
    def message(self, msg = None):
        if msg is None:
            msg = self.msg
        if msg == "":
            msg = "Finished migration to schema version %s" % (self.Version,)
        logMe(1, msg)
        self.msg = msg

# dummy migration class that maintains compatbility with schema version13
class MigrateTo_13(SchemaMigration):
    Version = (13,1)
    # fix a migration step that could have potentially failed in the
    # migrations before conary 1.0
    def migrate1(self):
        # flavorId = 0 is now ''
        cu = self.db.cursor()
        cu.execute("UPDATE Flavors SET flavor = '' WHERE flavorId = 0")
        return True
    def migrate(self):
        return self.Version

# schema versions lower than 13 are not supported for migration through this pathway
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
        gpct = 0
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
                logMe(3, 'Calculating sha1 for fileStream %s/%s (%02d%%)...' % (streamId, total, pct))
                pct = newPct + 5

        logMe(2, 'Populating FileStream Table with sha1s...')

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

        # because of the foreign key referential mess, we need to
        # destroy the FKs relationships, recreate the Entitlement
        # tables, and restore the data
        logMe(2, 'Updating Entitlements...')
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
        schema.createEntitlements(self.db)

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

        logMe(2, "Updating the Permissions table...")
        self.cu.execute("ALTER TABLE Permissions ADD COLUMN "
                        "canRemove   INTEGER NOT NULL DEFAULT 0"
                        % self.db.keywords)

        logMe(2, "Updating Instances table...")
        self.db.renameColumn("Instances", "isRedirect", "troveType")
        self.db.loadSchema()
        return self.Version

def rebuildLatest(db, recreate=False):
    cu = db.cursor()
    logMe(2, "Updating the Latest table...")
    if recreate:
        cu.execute("DROP TABLE Latest")
        db.loadSchema()
        schema.createLatest(db)
    else:
        cu.execute("DELETE FROM Latest")
    # As a matter of choice, the Latest table only includes stuff that
    # has the isPresent flag to NORMAL. This means we exclude from
    # computation the missing and hidden troves
    cu.execute("""
    insert into Latest (itemId, branchId, flavorId, versionId, latestType)
    select
        instances.itemid as itemid,
        nodes.branchid as branchid,
        instances.flavorid as flavorid,
        nodes.versionid as versionid,
        %(type)d
    from
    ( select
        i.itemid as itemid,
        n.branchid as branchid,
        i.flavorid as flavorid,
        max(n.finalTimestamp) as finaltimestamp
      from instances as i join nodes as n using(itemId, versionId)
      where i.isPresent = %(present)d
      group by i.itemid, n.branchid, i.flavorid
    ) as tmp
    join nodes using(itemId, branchId, finalTimestamp)
    join instances using(itemId, versionId)
    where instances.flavorid = tmp.flavorid
      and instances.isPresent = %(present)d
    """ % {"type" : versionops.LATEST_TYPE_ANY,
           "present" : instances.INSTANCE_PRESENT_NORMAL, })
    # latest type present excludes the removed troves from computation
    cu.execute("""
    insert into Latest (itemId, branchId, flavorId, versionId, latestType)
    select
        instances.itemid as itemid,
        nodes.branchid as branchid,
        instances.flavorid as flavorid,
        nodes.versionid as versionid,
        %(type)d
    from
    ( select
        i.itemid as itemid,
        n.branchid as branchid,
        i.flavorid as flavorid,
        max(n.finalTimestamp) as finaltimestamp
      from instances as i join nodes as n using(itemId, versionId)
      where i.isPresent = %(present)d
        and i.troveType != %(trove)d
      group by i.itemid, n.branchid, i.flavorid
    ) as tmp
    join nodes using(itemId, branchId, finalTimestamp)
    join instances using(itemId, versionId)
    where instances.flavorid = tmp.flavorid
      and instances.isPresent = %(present)d
      and instances.troveType != %(trove)d
    """ % {"type" : versionops.LATEST_TYPE_PRESENT,
           "present" : instances.INSTANCE_PRESENT_NORMAL,
           "trove" : trove.TROVE_TYPE_REMOVED, })
    # for the LATEST_TYPE_NORMAL we only consider the PRESENT latest
    # entries that are not redirects. Branches that end in redirects
    # shouldn't have an entry in LATEST_TYPE_NORMAL
    cu.execute("""
    insert into Latest (itemId, branchId, flavorId, versionId, latestType)
    select itemId, branchId, flavorId, versionId, %(type)d
    from Latest
    join Instances using(itemId, versionId, flavorId)
    where Latest.latestType = %(latest)d
      and Instances.troveType = %(trove)d
    """ % {"type" : versionops.LATEST_TYPE_NORMAL,
           "latest" : versionops.LATEST_TYPE_PRESENT,
           "trove" : trove.TROVE_TYPE_NORMAL, })
    return True

class MigrateTo_15(SchemaMigration):
    Version = (15, 10)
    def updateLatest(self):
        return rebuildLatest(self.db, recreate=True)

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
        if trv.verifyDigests():
            return
        logMe(3, "updating trove sigs: %s %s %s" % (name, version, flavor))
        trv.computeDigests()
        cu.execute("delete from TroveInfo where instanceId = ? "
                   "and infoType = ?", (instanceId, trove._TROVEINFO_TAG_SIGS))
        cu.execute("insert into TroveInfo (instanceId, infoType, data) "
                   "values (?, ?, ?)", (
            instanceId, trove._TROVEINFO_TAG_SIGS,
            cu.binary(trv.troveInfo.sigs.freeze())))

    def fixRedirects(self, repos):
        logMe(2, "removing dep provisions from redirects...")
        self.db.loadSchema()
        # avoid creating this index until we had a chance to check the path indexes
        self.db.tables["TroveFiles"].append("TroveFilesPathIdx")
        # remove dependency provisions from redirects -- the conary 1.0
        # branch would set redirects to provide their own names. this doesn't
        # clean up the dependency table; that would only matter on a trove
        # which was cooked as only a redirect in the repository; any other
        # instances would still need the depId anyway
        cu = self.db.cursor()
        cu.execute("delete from provides where instanceId in "
                   "(select instanceId from instances "
                   "where troveType=? and isPresent=1)",
                   trove.TROVE_TYPE_REDIRECT)
        # need to make sure TroveRedirects is defined...
        schema.createTroves(self.db)
        # loop over redirects...
        cu.execute("select instanceId from instances "
                   "where troveType=? and isPresent=1",
                   trove.TROVE_TYPE_REDIRECT)
        for (instanceId,) in cu:
            self.fixTroveSig(repos, instanceId)

    # fix the duplicate path problems, if any
    def fixDuplicatePaths(self, repos):
        logMe(2, "checking database for duplicate path entries...")
        cu = self.db.cursor()
        # we'll have to do a full table scan on TroveFiles. no way
        # around it...
        cu.execute("""
        create temporary table tmpDups(
            instanceId integer,
            path %(PATHTYPE)s,
            counter integer
        ) %(TABLEOPTS)s""" % self.db.keywords)
        logMe(2, "looking for troves with duplicate paths...")
        # sqlite has a real challenge dealing with large datasets
        if self.db.driver == 'sqlite':
            cu2 = self.db.cursor()
            cu.execute("select distinct instanceId from TroveFiles")
            # so we split this in very little tasks. lots of them
            for (instanceId,) in cu:
                cu2.execute("""
                insert into tmpDups (instanceId, path, counter)
                select instanceId, path, count(*)
                from TroveFiles
                where instanceId = ?
                group by instanceId, path
                having count(*) > 1""", instanceId)
        else: # other backends should be able to process this in one shot
            cu.execute("""
            insert into tmpDups (instanceId, path, counter)
            select instanceId, path, count(*)
            from TroveFiles
            group by instanceId, path
            having count(*) > 1""")
        counter = cu.execute("select count(*) from tmpDups").fetchall()[0][0]
        if counter > 0:
            # drop the old index, if any
            self.db.loadSchema()
            self.db.dropIndex("TroveFiles", "TroveFilesPathIdx")
        logMe(3, "detected %d duplicates" % (counter,))
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
                       "values (?,?,?,?,?)", tuple(ret[0]))
            if len(ret) > 1:
                # need to recompute the sha1 - we might have changed the trove manifest
                # if the records were different
                self.fixTroveSig(repos, instanceId)
        # recreate the indexes and triggers - including new path
        # index for TroveFiles.  Also recreates the indexes table.
        logMe(2, 'Recreating indexes... (this could take a while)')
        cu.execute("drop table tmpDups")
        self.db.loadSchema()
        schema.createTroves(self.db)
        logMe(2, 'Indexes created.')

    # drop the unused views
    def dropViews(self):
        logMe(2, "dropping unused views")
        self.db.loadSchema()
        cu = self.db.cursor()
        for v in ["UsersView", "NodesView", "InstancesView", "LatestView"]:
            if v in self.db.views:
                cu.execute("drop view %s" % (v,))

    # add the CapId reference to Permissions and Latest
    def fixPermissions(self):
        # because we need to add a foreign key to the Permissions and Latest
        # tables, it is easier if we save the tables and recreate them
        logMe(2, "Updating the Permissions table...")
        cu = self.db.cursor()
        # handle the case where the admin field has been relocated from Permissions
        cu.execute("""create table tmpPerm as
        select userGroupId, labelId, itemId, admin, canWrite, canRemove
        from Permissions""")
        cu.execute("drop table Permissions")
        self.db.loadSchema()
        schema.createUsers(self.db)
        # check if we need to preserve the admin field for a while longer
        cu.execute("select * from Permissions limit 0")
        columns = [x.lower() for x in cu.fields()]
        if "admin" not in columns:
            cu.execute("alter table Permissions add column "
                       "admin integer not null default 0")
        cu.execute("""insert into Permissions
        (userGroupId, labelId, itemId, admin, canWrite, canRemove)
        select userGroupId, labelId, itemId, admin, canWrite, canRemove
        from tmpPerm
        """)
        cu.execute("drop table tmpPerm")

    # migrate to 15.0
    def migrate(self):
        schema.setupTempTables(self.db)
        cu = self.db.cursor()
        # needed for signature recalculation
        repos = trovestore.TroveStore(self.db)
        self.dropViews()
        self.fixRedirects(repos)
        self.fixDuplicatePaths(repos)
        self.fixPermissions()
        self.updateLatest()
        return True
    # migrate to 15.1
    def migrate1(self):
        # drop the pinned index on Instances and recreate it as not-pinned
        self.db.dropTrigger("Instances", "UPDATE")
        ret = schema.createTrigger(self.db, "Instances")
        self.db.loadSchema()
        return ret
    # this migration has been moved to a major schema migration
    def migrate2(self):
        return True
    # migrate to 15.3
    def migrate3(self):
        # we create a whole bunch of indexes for foreign keys to smooth out the differences
        # between MySQL and PostgreSQL
        self.db.loadSchema()
        # indexes on Nodes have to be cleaned up
        self.db.dropIndex("Nodes", "NodesItemBranchVersionIdx")
        self.db.createIndex("Nodes", "NodesItemVersionBranchIdx",
                            "itemId, versionId, branchId", unique = True)
        self.db.dropIndex("Nodes", "NodesItemVersionIdx")
        self.db.dropIndex("Latest", "LatestIdx")
        self.db.loadSchema()
        return True
    # conary 1.1.22 went out with a busted definition of LabelMap - we need to fix it
    def migrate4(self):
        return True
    # 15.5
    def migrate5(self):
        return True
    # 15.6 - fix for the wrong values of clonedFromId and sourceItemId
    def migrate6(self):
        # because Troveinfo.data is treated as a blob, we have to do
        # the processing in python
        nodesIdList = []
        instancesIdList = []
        cu = self.db.cursor()
        logMe(2, "checking for bad clonedFromId entries...")
        cu.execute("""
        select Instances.instanceId, TroveInfo.data, Versions.version
        from Instances
        join TroveInfo using(instanceId)
        left join Versions on Instances.clonedfromId = Versions.versionId
        where TroveInfo.infotype = ?""", trove._TROVEINFO_TAG_CLONEDFROM)
        for instanceId, tiVersion, currentVersion in cu:
            correctVersion = cu.frombinary(tiVersion)
            if correctVersion == currentVersion:
                continue
            instancesIdList.append((instanceId, correctVersion))
        logMe(2, "checking for bad sourceItemId entries...")
        # we need to force a "last one wins" policy
        cu.execute("""
        select maxNodes.nodeId, TroveInfo.data, Items.item
        from ( select N.nodeId as nodeId, max(I.instanceId) as instanceId
               from Nodes As N join Instances as I using(itemId, versionId)
               group by N.nodeId ) as maxNodes
        join Nodes on maxNodes.nodeId = Nodes.nodeId
        join TroveInfo on maxNodes.instanceId = TroveInfo.instanceId
        left join Items on Nodes.sourceItemId = Items.itemId
        where TroveInfo.infoType = ?
        """, trove._TROVEINFO_TAG_SOURCENAME)
        for nodeId, tiSourceName, currentSourceName in cu:
            correctSourceName = cu.frombinary(tiSourceName)
            if correctSourceName == currentSourceName:
                continue
            nodesIdList.append((nodeId, correctSourceName))
        # these are needed for looping ops
        iT = idtable.IdTable(self.db, 'Items', 'itemId', 'item')
        vT = idtable.IdTable(self.db, 'Versions', 'versionId', 'version')
        logMe(2, "Fixing %d bad clonedFromId entries..." % (len(instancesIdList),))
        # these shouldn't be that many, really - we can afford to loop over each one
        for (instanceId, versionStr) in instancesIdList:
            versionId = vT.getOrAddId(versionStr)
            cu.execute("update Instances set clonedFromId = ? where instanceId = ?",
                       (versionId, instanceId))
        logMe(2, "Fixing %d bad sourceItemId entries..." % (len(nodesIdList),))
        for (nodeId, sourceName) in nodesIdList:
            itemId = iT.getOrAddId(sourceName)
            cu.execute("update Nodes set sourceItemId = ? where nodeId = ?",
                       (itemId, nodeId))
        return True
    # 15.7 - rebuild the Latest table to consider only the isPresent = NORMAL instances
    def migrate7(self):
        # no longer rebuild here because we're rebuilding in migrate9 anyway
        return True
    # 15.8 - add an index on TroveInfo
    def migrate8(self):
        return True
    # 15.9 - rebuild Latest
    def migrate9(self):
        # we make this a noop since schema version 16 changes all this anyway
        return True
    # 15.10 - create the lock table(s)
    def migrate10(self):
        # we make this a noop because the same will be done at schema 16.1
        return True


class MigrateTo_16(SchemaMigration):
    Version = (16,1)
    # migrate to 16.0
    def migrate(self):
        cu = self.db.cursor()
        self.db.loadSchema()

        self._buildPermissions(cu)
        self._buildTroveFiles(cu)
        self._buildUserGroupInstances(cu)
        self._buildLabelMap(cu)
        self._buildFlavorMap(cu)

        return True

    def _buildPermissions(self, cu):
        # move the admin field from Permissions into UserGroups
        logMe(2, "Relocating the admin field from Permissions to UserGroups...")
        if "OldPermissions" in self.db.tables:
            cu.execute("drop table OldPermissions")
        cu.execute("create table OldPermissions as select * from Permissions")
        addFK = False
        if "UserGroupAllPermissions" in self.db.tables:
            self.db.dropForeignKey("UserGroupAllPermissions", name="UGAP_permissionId_fk")
            addFK = True
        cu.execute("drop table Permissions")
        self.db.loadSchema()
        schema.createUsers(self.db)
        cu.execute("alter table UserGroups add column "
                   "admin INTEGER NOT NULL DEFAULT 0 ")
        cu.execute("select userGroupId, max(admin) from OldPermissions "
                   "group by userGroupId")
        for ugid, admin in cu.fetchall():
            cu.execute("update UserGroups set admin = ? where userGroupId = ?",
                       (admin, ugid))
        fields = ",".join(["userGroupId", "labelId", "itemId", "canWrite", "canRemove"])
        cu.execute("insert into Permissions(%s) "
                   "select distinct %s from OldPermissions " %(fields, fields))
        cu.execute("drop table OldPermissions")
        if addFK:
            self.db.addForeignKey("UserGroupAllPermissions", "permissionId",
                                  "Permissions", "permissionId")
        self.db.loadSchema()
        return True

    def _buildFlavorMap(self, cu):
        # need to rebuild flavormap
        logMe(2, "Recreating the FlavorMap table...")
        cu.execute("drop table FlavorMap")
        self.db.loadSchema()
        schema.createFlavors(self.db)
        cu.execute("select flavorId, flavor from Flavors")
        flavTable = flavors.Flavors(self.db)
        for (flavorId, flavorStr) in cu.fetchall():
            flavor = deps.ThawFlavor(flavorStr)
            flavTable.createFlavorMap(flavorId, flavor, cu)
        return True

    def _buildTroveFiles(self, cu):
        # need to rebuild the TroveFiles and FilesPath tables
        logMe(2, "creating the FilePaths table...")
        self.db.loadSchema()

        # need to create an old-style FilePaths table for the
        # migration from old schema to succeed
        cu.execute("""
        CREATE TABLE FilePaths(
            filePathId      %(PRIMARYKEY)s,
            path            %(PATHTYPE)s,
            pathId          %(BINARY16)s NOT NULL
        ) %(TABLEOPTS)s""" % self.db.keywords)
        self.db.tables["FilePaths"] = []
        # create entries for the FilePaths table
        cu.execute("""
        insert into FilePaths (pathId, path)
        select distinct pathId, path from TroveFiles """)
        self.db.createIndex("FilePaths", "tmpFilePaths_idx", "pathId,path")
        self.db.analyze("FilePaths")
        # prepare for the new format of TroveFiles
        logMe(2, "creating the new TroveFiles table...")
        # attempt to keep the relative ordering of the stuff we had in the
        # old TroveFiles
        cu.execute("""
        create table newTroveFiles as
            select instanceId, streamId, versionId, filePathId
            from TroveFiles join FilePaths using(pathId,path)
            order by instanceId, streamId, versionId """)
        cu.execute("alter table newTroveFiles add column "
                   "changed NUMERIC(14,0) NOT NULL DEFAULT 0 ")
        cu.execute("drop table TroveFiles")
        cu.execute("alter table newTroveFiles rename to TroveFiles")
        self.db.loadSchema()
        logMe(3, "updating the TroveFiles table")
        self.db.addForeignKey("TroveFiles", "instanceId", "Instances", "instanceId")
        self.db.addForeignKey("TroveFiles", "streamId", "FileStreams", "streamId")
        self.db.addForeignKey("TroveFiles", "versionId", "Versions", "versionId")
        self.db.addForeignKey("TroveFiles", "filePathId", "FilePaths", "filePathId")
        logMe(3, "checking results and analyzing TroveFiles...")
        # create the indexes required
        self.db.loadSchema()
        self.db.analyze("TroveFiles")
        return True

    def _buildLabelMap(self, cu):
        logMe(2, "updating LabelMap")
        cu.execute("create table OldLabelMap as select * from LabelMap")
        cu.execute("drop table LabelMap")
        self.db.loadSchema()
        schema.createLabelMap(self.db)
        cu.execute("insert into LabelMap (itemId, labelId, branchId) "
                   "select itemId, labelId, branchId from OldLabelMap ")
        cu.execute("drop table OldLabelMap")
        self.db.loadSchema()
        return True

    # populate the UserGroupInstances map
    def _buildUserGroupInstances(self, cu):
        self.db.loadSchema()
        if "Latest" in self.db.tables:
            cu.execute("drop table Latest")
        if "Caps" in self.db.tables:
            cu.execute("drop table Caps")
        schema.createAccessMaps(self.db)
        schema.createLatest(self.db, withIndexes=False)
        logMe(2, "creating UserGroupInstancesCache table")
        ugi = accessmap.RoleInstances(self.db)
        ugi.rebuild()
        logMe(3, "creating the LatestCache indexes...")
        schema.createLatest(self.db)
        self.db.analyze("LatestCache")
        return True

    # create the lock tables
    def migrate1(self):
        self.db.loadSchema()
        schema.createLockTables(self.db)
        return True


class MigrateTo_17(SchemaMigration):
    Version = (17,4)

    # given a FilePaths table that only has a path column, split that into
    # a (dirnameid, basenameId) tuple and create/update the corresponding
    # Dirnames and Basenames tables
    def _createFilePaths(self):
        logMe(2, "splitting paths in dirnames and basenames")
        cu = self.db.cursor()
        cu.execute("""
        create table tmpDirnames (
            filePathId %(PRIMARYKEY)s,
            dirname %(PATHTYPE)s,
            basename %(PATHTYPE)s
        ) """ % self.db.keywords)
        # save a copy of FilePaths before updating the table definition
        cu.execute("""create table oldFilePaths as
        select filePathId, pathId, path from FilePaths""")
        self.db.createIndex("oldFilePaths", "oldFilePathsIdx", "filePathId",
                            check=False, unique=True)
        self.db.analyze("oldFilePaths")

        # drop the FK constraint from TroveFiles into FilePaths
        self.db.loadSchema()
        self.db.dropForeignKey("TroveFiles", name = "TroveFiles_filePathId_fk")
        # create Dirnames, Basenames and the new FilePaths tables
        cu.execute("drop table FilePaths")
        self.db.loadSchema()
        schema.createTroves(self.db, createIndex=False)

        # this is to avoid processing too many entries at once...
        sliceSize = 200000
        analyze = 1
        while True:
            cu.execute("""
            select fp.filePathId, fp.path from oldFilePaths as fp
            left join tmpDirnames as d using(filePathId)
            where d.filePathId is null limit ?""", sliceSize)
            tmpl = [ (_fpid, os.path.split(_path)) for _fpid,_path in cu.fetchall() ]
            if not tmpl:
                break # no more entries found
            self.db.bulkload("tmpDirnames", [ (x[0], x[1][0], x[1][1]) for x in tmpl ],
                             ["filePathId", "dirname", "basename"])
            # don't analyze too often for speed reasons
            analyze -= 1
            if not analyze:
                analyze = 5
                self.db.analyze("tmpDirnames")
        self.db.createIndex("tmpDirnames", "tmpDirnames_dirname_idx", "dirname",
                            check = False)
        logMe(2, "extracting unique dirnames and basenames...")
        self.db.analyze("tmpDirnames")
        # the '' and '/' dirnames should already be in the Dirnames table
        cu.execute("""
        insert into Dirnames(dirname)
        select distinct dirname from tmpDirnames
        order by dirname """)
        self.db.analyze("Dirnames")
        cu.execute("""
        insert into Basenames(basename)
        select distinct basename from tmpDirnames
        order by basename """)
        self.db.analyze("Basenames")

        logMe(2, "generating the new FilePaths table...")
        cu.execute("""insert into FilePaths(filePathId, dirnameId, basenameId, pathId)
        select fp.filePathId, d.dirnameId, b.basenameId, fp.pathId
        from oldFilePaths as fp
        join tmpDirnames as td using(filePathId)
        join Dirnames as d on td.dirname = d.dirname
        join Basenames as b on td.basename =  b.basename """)
        cu.execute("drop table oldFilePaths")
        cu.execute("drop table tmpDirnames")
        # fix the autoincrement primary key value on the new FilePaths
        cu.execute("select max(filePathId) from FilePaths")
        maxId = cu.fetchone()[0]
        if maxId:
            self.db.setAutoIncrement("FilePaths", "filePathId", maxId)
        self.db.analyze("FilePaths")

        # re-enable the FK constraint and create indexes
        logMe(3, "adding foreign key constraints...")
        self.db.addForeignKey("TroveFiles", "filePathId", "FilePaths", "filePathId")
        self.db.analyze("TroveFiles")
        schema.createTroves(self.db)

    # migrate to 17.0
    def migrate(self):
        logMe(1, "WARNING: this migration takes a LONG time. Do not interupt!")
        schema.setupTempTables(self.db)
        # migrate FilesPath to a dirnames-based setup
        self._createFilePaths()
        return True

    # migrate to 17.1
    def migrate1(self):
        return True

    # migrate to 17.2
    def migrate2(self):
        # fix the dirnames and basenames column types for postgresql
        cu = self.db.cursor()
        if self.db.driver == 'postgresql':
            logMe(2, "fixing column types for pathfields")
            cu.execute("create table saveDirnames as select dirnameId, dirname from Dirnames")
            cu.execute("create table saveBasenames as select basenameId, basename from Basenames")
            self.db.dropForeignKey("FilePaths", "dirnameId")
            self.db.dropForeignKey("FilePaths", "basenameId")
            cu.execute("drop table Dirnames")
            cu.execute("drop table Basenames")
            self.db.loadSchema()
            schema.createTroves(self.db, createIndex = False)
            cu.execute("select dirnameId, dirname from saveDirnames")
            self.db.bulkload("Dirnames", ( (x[0], cu.binary(x[1])) for x in cu.fetchall() ),
                             ["dirnameId", "dirname"])
            cu.execute("select basenameId, basename from saveBasenames")
            self.db.bulkload("Basenames", ( (x[0], cu.binary(x[1])) for x in cu.fetchall() ),
                             ["basenameId", "basename"])
            schema.createTroves(self.db, createIndex = True)
            self.db.addForeignKey("FilePaths", "dirnameId", "Dirnames", "dirnameId")
            self.db.addForeignKey("FilePaths", "basenameId", "Basenames", "basenameId")
            cu.execute("drop table saveDirnames")
            cu.execute("drop table saveBasenames")
            self.db.analyze("Dirnames")
            self.db.analyze("Basenames")
            self.db.setAutoIncrement("Dirnames", "dirnameId")
            self.db.setAutoIncrement("Basenames", "basenameId")
        return True

    # migrate to 17.3
    def migrate3(self):
        logMe(2, "fixing missing sha1s")
        cu = self.db.cursor()

        cu.execute("""select streamId, stream from filestreams
                      where sha1 is NULL and stream is not NULL""")

        count = 0
        for (streamId, stream) in list(cu):
            if not files.frozenFileHasContents(stream): continue
            contentInfo = files.frozenFileContentInfo(stream)
            cu.execute("update filestreams set sha1=? where streamId=?",
                       cu.binary(contentInfo.sha1()), streamId)
            count += 1

        logMe(2, "fixed up %d rows" % count)
        cu.execute("""select streamId, stream from filestreams
                      where sha1 is NULL and stream is not NULL""")

        return True

    def migrate4(self):
        if self.db.kind == 'sqlite':
            return True
        cu = self.db.cursor()
        cu.execute("""ALTER TABLE pgpkeys
            DROP CONSTRAINT PGPKeys_userId_fk,
            ADD CONSTRAINT PGPKeys_userId_fk
                FOREIGN KEY ( userId )
                REFERENCES Users ( userId )
                ON DELETE SET NULL ON UPDATE CASCADE""")
        return True

class MigrateTo_18(SchemaMigration):
    Version = 18
    def migrate(self):
        cu = self.db.cursor()
        cu.execute("alter table instances add column "
                   "fingerprint     %(BINARY20)s" % self.db.keywords)
        if self.db.kind == 'sqlite':
            cu.execute("UPDATE Users SET salt = hex(salt)")
        else:
            cu.execute("ALTER TABLE Users"
                    " ALTER password TYPE %(STRING)s,"
                    " ALTER salt TYPE %(STRING)s USING encode(salt, 'hex')"
                    % self.db.keywords)
        cu.execute("ALTER TABLE UserGroups ADD accept_flags %(STRING)s"
                % self.db.keywords)
        cu.execute("ALTER TABLE UserGroups ADD filter_flags %(STRING)s"
                % self.db.keywords)
        return True

    def migrate1(self):
        cu = self.db.cursor()
        if 'Prefixes' in self.db.tables:
            cu.execute("DROP TABLE Prefixes")
        if 'CheckTroveCache' in self.db.tables:
            cu.execute("DROP TABLE CheckTroveCache")
        return True

def _getMigration(major):
    try:
        ret = sys.modules[__name__].__dict__['MigrateTo_' + str(major)]
    except KeyError:
        return None
    return ret

# return the last major.minor version for a given major
def majorMinor(major):
    migr = _getMigration(major)
    if migr is None:
        return (major, 0)
    return migr.Version

# entry point that migrates the schema
def migrateSchema(db):
    version = db.getVersion()
    assert(version >= 13) # minimum version we support
    if version.major > schema.VERSION.major:
        return version # noop, should not have been called.
    logMe(2, "migrating from version", version)
    # first, we need to make sure that for the current major we're up
    # to the latest minor
    migrateFunc = _getMigration(version.major)
    if migrateFunc is None:
        raise sqlerrors.SchemaVersionError(
            "Could not find migration code that deals with repository "
            "schema %s" % version, version)
    # migrate all the way to the latest minor for the current major
    migrateFunc(db)()
    version = db.getVersion()
    # migrate to the latest major
    while version.major < schema.VERSION.major:
        migrateFunc = _getMigration(version.major+1)
        newVersion = migrateFunc(db)()
        assert(newVersion.major == version.major+1)
        version = newVersion
    return version
