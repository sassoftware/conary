# Copyright (c) 2005-2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.

import sys

from conary import files, trove, versions
from conary.dbstore import migration, sqlerrors, sqllib, idtable
from conary.lib.tracelog import logMe
from conary.deps import deps
from conary.repository.netrepos import versionops, trovestore, \
     netauth, flavors, accessmap
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
    # latest type normal excludes the removed and redirect troves from computation
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
        and i.troveType = %(trove)d
      group by i.itemid, n.branchid, i.flavorid
    ) as tmp
    join nodes using(itemId, branchId, finalTimestamp)
    join instances using(itemId, versionId)
    where instances.flavorid = tmp.flavorid
      and instances.isPresent = %(present)d
      and instances.troveType = %(trove)d
    """ % {"type" : versionops.LATEST_TYPE_NORMAL,
           "present" :  instances.INSTANCE_PRESENT_NORMAL,
           "trove" : trove.TROVE_TYPE_NORMAL, })
    return True
    
class MigrateTo_15(SchemaMigration):
    Version = (15, 7)
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
        schema.createSchema(self.db)
        return True
    # conary 1.1.22 went out with a busted definition of LabelMap - we need to fix it
    def migrate4(self):
        return updateLabelMap(self.db)
    # 15.5
    def migrate5(self):
        self.db.loadSchema()
        self.db.createIndex('LabelMap', 'LabelMapItemIdBranchIdIdx',
                            'itemId, branchId')
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
        return rebuildLatest(self.db)

# populate the CheckTroveCache table
def createCheckTroveCache(db):
    db.loadSchema()
    logMe(2, "creating the CheckTroveCache table...")
    assert("CheckTroveCache" in db.tables)
    cu = db.cursor()
    cu.execute("delete from CheckTroveCache")
    cu.execute("""
    select distinct
        P.itemId as patternId, Items.itemId as itemId,
        PI.item as pattern, Items.item as trove
    from Permissions as P
    join Items as PI using(itemId)
    cross join Items
    """)
    cu2 = db.cursor()
    auth = netauth.NetworkAuthorization(db, None)
    for (patternId, itemId, pattern, troveName) in cu:
        if not auth.checkTrove(pattern, troveName):
            continue
        cu2.execute("insert into CheckTroveCache (patternId, itemId) values (?,?)",
                    (patternId, itemId))
    db.analyze("CheckTroveCache")
    logMe(2, "done with CheckTroveCache")
    return True
        
# populate (and create if not exists) the UserGroupInstances table
def createUserGroupInstances(db):
    db.loadSchema()
    logMe(2, "creating UserGroupInstances table")
    assert("UserGroupInstancesCache" in db.tables)
    assert("CheckTroveCache" in db.tables)
    ugi = accessmap.UserGroupInstances(db)
    ugi.rebuild()
    return True

# looks like this LabelMap has to be recreated multiple times by
# different stages of migraton :-(
def updateLabelMap(db):
    cu = db.cursor()
    logMe(2, "updating LabelMap")
    cu.execute("create table OldLabelMap as select * from LabelMap")
    cu.execute("drop table LabelMap")
    db.loadSchema()
    schema.createLabelMap(db)
    cu.execute("insert into LabelMap (itemId, labelId, branchId) "
               "select itemId, labelId, branchId from OldLabelMap ")
    cu.execute("drop table OldLabelMap")
    db.loadSchema()
    return True
    
class MigrateTo_16(SchemaMigration):
    Version = (16,4)
    # migrate to 16.0
    # create a primary key for labelmap
    def migrate(self):
        return updateLabelMap(self.db)
    # move the admin field from Permissions into UserGroups
    def migrate1(self):
        logMe(2, "Relocating the admin field from Permissions to UserGroups...")
        cu = self.db.cursor()
        self.db.loadSchema()
        if "OldPermissions" in self.db.tables:
            cu.execute("drop table OldPermissions")
        cu.execute("create table OldPermissions as select * from Permissions")
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
        fields = ",".join(["userGroupId", "labelId", "itemId", "canWrite",
                           "canRemove", "capId"])
        cu.execute("insert into Permissions(%s) "
                   "select distinct %s from OldPermissions " %(fields, fields))
        cu.execute("drop table OldPermissions")
        self.db.loadSchema()
        return True
    def migrate2(self):
        # need to rebuild flavormap
        logMe(2, "Recreating the FlavorMap table...")
        cu = self.db.cursor()
        cu.execute("drop table FlavorMap")
        self.db.loadSchema()
        schema.createFlavors(self.db)
        cu.execute("select flavorId, flavor from Flavors")
        flavTable = flavors.Flavors(self.db)
        for (flavorId, flavorStr) in cu.fetchall():
            flavor = deps.ThawFlavor(flavorStr)
            flavTable.createFlavorMap(flavorId, flavor, cu)
        return True
    # populate the UserGroupInstances map
    def migrate3(self):
        schema.createAccessMaps(self.db)
        if not createCheckTroveCache(self.db):
            return False
        if not createUserGroupInstances(self.db):
            return False
        return True
    # drop the old Latest table and create views instead
    def migrate4(self):
        cu = self.db.cursor()
        self.db.loadSchema()
        if "Latest" in self.db.tables:
            cu.execute("drop table Latest")
        logMe(2, "creating the Latest by role tables...")
        schema.createLatest(self.db, withIndexes=False)
        logMe(3, "rebuilding the LatestCache entries...")
        latest = versionops.LatestTable(self.db)
        latest.rebuild()
        logMe(3, "creating the LatestCache indexes...")
        schema.createLatest(self.db)
        self.db.analyze("LatestCache")
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
