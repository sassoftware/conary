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
from conary.dbstore import migration, sqlerrors, sqllib
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
            msg = "Finished migration to schema version %s" % (self.Version,)
        logMe(1, msg)
        self.msg = msg

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

        logMe(2, "Updating the Permissions table...")
        self.cu.execute("ALTER TABLE Permissions ADD COLUMN "
                        "canRemove   INTEGER NOT NULL DEFAULT 0"
                        % self.db.keywords)

        logMe(2, "Updating Instances table...")
        self.db.renameColumn("Instances", "isRedirect", "troveType")
        self.db.loadSchema()
        return self.Version

class MigrateTo_15(SchemaMigration):
    Version = (15,1)
    def updateLatest(self, cu):
        logMe(2, "Updating the Latest table...")
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
        logMe(3, "updating trove sigs: %s %s %s" % (name, version, flavor))
        trv.computeSignatures()
        cu.execute("delete from TroveInfo where instanceId = ? "
                   "and infoType = ?", (instanceId, trove._TROVEINFO_TAG_SIGS))
        cu.execute("insert into TroveInfo (instanceId, infoType, data) "
                   "values (?, ?, ?)", (
            instanceId, trove._TROVEINFO_TAG_SIGS,
            cu.binary(trv.troveInfo.sigs.freeze())))

    def fixRedirects(self, cu, repos):
        logMe(2, "removing dep provisions from redirects...")
        # remove dependency provisions from redirects -- the conary 1.0
        # branch would set redirects to provide their own names. this doesn't
        # clean up the dependency table; that would only matter on a trove
        # which was cooked as only a redirect in the repository; any other
        # instances would still need the depId anyway
        cu.execute("delete from provides where instanceId in "
                   "(select instanceId from instances "
                   "where troveType=? and isPresent=1)",
                   trove.TROVE_TYPE_REDIRECT)
        # loop over redirects...
        cu.execute("select instanceId from instances "
                   "where troveType=? and isPresent=1",
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
        logMe(2, "searching the trovefiles table...")
        cu.execute("insert into tmpDupPath (instanceId, path) "
                   "select instanceId, path from TroveFiles")
        logMe(2, "looking for troves with duplicate paths...")
        cu.execute("""
        insert into tmpDups (counter, instanceId, path)
        select count(*) as c, instanceId, path
        from tmpDupPath
        group by instanceId, path
        having count(*) > 1""")
        counter = cu.execute("select count(*) from tmpDups").fetchall()[0][0]
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
        cu.execute("drop table tmpDupPath")
        schema.createTroves(self.db)
        logMe(2, 'Indexes created.')

    # drop the unused views
    def dropViews(self):
        logMe(2, "dropping unused views")
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

    # migrate to 15.0
    def migrate(self):
        schema.setupTempTables(self.db)
        cu = self.db.cursor()
        # needed for signature recalculation
        repos = trovestore.TroveStore(self.db)

        self.dropViews()
        self.fixRedirects(cu, repos)
        self.fixDuplicatePaths(cu, repos)
        self.fixPermissions()
        self.updateLatest(cu)
        return True
    # migrate to 15.1
    def migrate1(self):
        # drop the pinned index on Instances and recreate it as not-pinned
        self.db.dropTrigger("Instances", "UPDATE")
        ret = schema.createTrigger(self.db, "Instances")
        return ret
    
def _getMigration(major):
    try:
        ret = sys.modules[__name__].__dict__['MigrateTo_' + str(major)]
    except KeyError:
        return None
    return ret

# entry point that migrates the schema
def migrateSchema(db, major=True):
    version = db.getVersion()
    assert(version > 13) # minimum version we support
    if version > schema.VERSION:
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
    migrateFunc(db)(major=False)
    version = db.getVersion()
    # migrate to the latest major
    if not major:
        return version
    while version.major < schema.VERSION.major:
        migrateFunc = _getMigration(version.major+1)
        newVersion = migrateFunc(db)(major=major)
        assert(newVersion.major == version.major+1)
        version = newVersion
    return version
