#
# Copyright (c) 2005 rPath, Inc.
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

from conary.dbstore import migration
from conary.lib.tracelog import logMe

VERSION = 7

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
        self.cu.execute("INSERT INTO Items VALUES(0, 'ALL')")
        self.cu.execute("INSERT INTO Labels VALUES(0, 'ALL')")

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
        cu.execute("""CREATE UNIQUE INDEX PermissionsIdx ON 
            Permissions(userGroupId, labelId, itemId)""")
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
        self.cu.execute("CREATE UNIQUE INDEX FlavorScoresIdx "
                   "    on FlavorScores(request, present)")
        # remove redundancy/rename                
        self.cu.execute("DROP INDEX NodesIdx")
        self.cu.execute("DROP INDEX NodesIdx2")
        self.cu.execute("""CREATE UNIQUE INDEX NodesItemBranchVersionIdx
                          ON Nodes(itemId, branchId, versionId)""")
        self.cu.execute("""CREATE INDEX NodesItemVersionIdx
                          ON Nodes(itemId, versionId)""")
        # the views are added by the __init__ methods of their
        # respective classes
        return self.Version

class MigrateTo_6(SchemaMigration):
    Version = 6
    def migrate(self):
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
            insert into troveinfo(instanceId, infoType, data)
            values(?, ?, ?)""", instanceId,
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
    
def checkVersion(db):
    global VERSION
    version = migration.getDatabaseVersion(db)
    if version == VERSION:
        return version

    # surely there is a more better way of handling this...
    if version == 1: MigrateTo_2(db)()
    if version == 2: MigrateTo_3(db)()
    if version == 3: MigrateTo_4(db)()
    if version == 4: MigrateTo_5(db)()
    if version == 5: MigrateTo_6(db)()
    if version == 6: MigrateTo_7(db)()

    return version
