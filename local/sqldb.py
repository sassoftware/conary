#
# Copyright (c) 2004-2005 rPath, Inc.
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

import deps.arch
import deps.deps
import deptable
import files
import sqlite3
import trove
import troveinfo
import versions
import versiontable
from dbstore import idtable

class Tags(idtable.CachedIdTable):

    def __init__(self, db):
	idtable.CachedIdTable.__init__(self, db, "Tags", "tagId", "tag")

class DBTroveFiles:
    """
    pathId, versionId, path, instanceId, stream
    """

    addItemStmt = "INSERT INTO DBTroveFiles VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)"

    def __init__(self, db):
        self.db = db
	self.tags = Tags(self.db)
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "DBTroveFiles" not in tables:
            cu.execute("""CREATE TABLE DBTroveFiles(
					  streamId INTEGER PRIMARY KEY,
					  pathId BINARY,
					  versionId INTEGER,
					  path STR,
                                          fileId BINARY,
					  instanceId INTEGER,
					  isPresent INTEGER,
					  stream BINARY)
		       """)
	    cu.execute("CREATE INDEX DBTroveFilesIdx ON "
		       "DBTroveFiles(fileId)")
	    cu.execute("CREATE INDEX DBTroveFilesInstanceIdx ON "
		       "DBTroveFiles(instanceId)")
	    cu.execute("CREATE INDEX DBTroveFilesPathIdx ON "
		       "DBTroveFiles(path)")

	    cu.execute("""CREATE TABLE DBFileTags(
					  streamId INT,
					  tagId INT)
		       """)

    def __getitem__(self, instanceId):
	cu = self.db.cursor()
	cu.execute("SELECT path, stream FROM DBTroveFiles "
		   "WHERE instanceId=? and isPresent=1", instanceId)
	for path, stream in cu:
            yield (path, stream)

    def getByInstanceId(self, instanceId, justPresent = True):
	cu = self.db.cursor()

	if justPresent:
	    cu.execute("SELECT path, stream FROM DBTroveFiles "
		       "WHERE instanceId=? and isPresent=1", instanceId)
	else:
	    cu.execute("SELECT path, stream FROM DBTroveFiles "
		       "WHERE instanceId=?", instanceId)

	for path, stream in cu:
	    yield (path, stream)

    def delInstance(self, instanceId):
        cu = self.db.cursor()
 	cu.execute("""DELETE FROM DBFileTags WHERE streamId IN
        (SELECT streamId from DBTroveFiles WHERE instanceId=?)""", instanceId)
        cu.execute("DELETE from DBTroveFiles WHERE instanceId=?", instanceId)

    def getFileByFileId(self, fileId, justPresent = True):
	cu = self.db.cursor()
	if justPresent:
	    cu.execute("SELECT path, stream FROM DBTroveFiles "
		       "WHERE fileId=? AND isPresent = 1", fileId)
	else:
	    cu.execute("SELECT path, stream FROM DBTroveFiles "
		       "WHERE fileId=?", fileId)
	# there could be multiple matches, but they should all be redundant
	try:
            path, stream = cu.next()
            return (path, stream)
	except StopIteration:
            raise KeyError, fileId

    def addItem(self, cu, pathId, versionId, path, fileId, instanceId, 
                stream, tags, addItemStmt = None):
        assert(len(pathId) == 16)

        if not addItemStmt:
            addItemStmt = cu.compile(self.addItemStmt)

        cu.execstmt(addItemStmt, pathId, versionId, path, fileId, instanceId, 
                    1, stream)

	streamId = cu.lastrowid

	for tag in tags:
	    cu.execute("INSERT INTO DBFileTags VALUES (?, ?)",
		       streamId, self.tags[tag])

    def iterPath(self, path):
        cu = self.db.cursor()
	cu.execute("SELECT instanceId FROM DBTroveFiles WHERE "
		   "isPresent=1 AND path=?", path)
	for instanceId in cu:
	    yield instanceId[0]

    def removePath(self, instanceId, path):
        cu = self.db.cursor()
	cu.execute("UPDATE DBTroveFiles SET isPresent=0 WHERE path=? "
		   "AND instanceId=?", (path, instanceId))

    def removeFileIds(self, instanceId, pathIdList, forReal = False):
        pathIdListPattern = ",".join(( '?' ) * len(pathIdList))
        cu = self.db.cursor()
	cu.execute("""DELETE FROM DBFileTags WHERE 
			streamId IN (
			    SELECT streamId FROM DBTroveFiles
				WHERE instanceId=%d AND pathId in (%s)
			)
		    """ % (instanceId, pathIdListPattern), pathIdList)

	if forReal:
	    cu.execute("DELETE FROM DBTroveFiles WHERE instanceId=%d "
		       "AND pathId in (%s)" % (instanceId, pathIdListPattern),
                       pathIdList)
	else:
	    cu.execute("UPDATE DBTroveFiles SET isPresent=0 WHERE "
		       "instanceId=%d AND pathId in (%s)" % (instanceId,
			       pathIdListPattern), pathIdList)

    def iterFilesWithTag(self, tag):
	cu = self.db.cursor()
	cu.execute("""
	    SELECT path FROM Tags
                INNER JOIN DBFileTags ON Tags.tagId = DBFileTags.tagId
                INNER JOIN DBTroveFiles ON
                    DBFileTags.streamId = DBTroveFiles.streamId
		WHERE tag=? ORDER BY DBTroveFiles.path
	""", tag)

	for path, in cu:
	    yield path

class DBInstanceTable:
    """
    Generic table for assigning id's to (name, version, isnSet, use)
    tuples, along with a isPresent flag
    """
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "Instances" not in tables:
            cu.execute("""CREATE TABLE Instances(
				instanceId INTEGER PRIMARY KEY, 
				troveName STR, 
				versionId INT, 
				flavorId INT,
				timeStamps STR,
				isPresent INT,
                                pinned BOOLEAN)""")
	    cu.execute("CREATE INDEX InstancesNameIdx ON "
		       "Instances(troveName)")
	    cu.execute("CREATE UNIQUE INDEX InstancesIdx ON "
		       "Instances(troveName, versionId, flavorId)")

    def iterNames(self):
	cu = self.db.cursor()
	cu.execute("SELECT DISTINCT troveName FROM Instances "
		    "WHERE isPresent=1")
	for match in cu:
	    yield match[0]

    def hasName(self, name):
	cu = self.db.cursor()
	cu.execute("SELECT instanceId FROM Instances "
		   "WHERE troveName=? AND isPresent=1", 
		   name)
	return cu.fetchone() != None

    def iterByName(self, name):
	cu = self.db.cursor()
	cu.execute("SELECT instanceId, versionId, troveName, flavorId FROM "
		   "Instances WHERE troveName=? AND isPresent=1", name)
 	for match in cu:
	    yield match

    def addId(self, troveName, versionId, flavorId, timeStamps, 
	      isPresent = True, pinned = False):
	assert(min(timeStamps) > 0)
	if isPresent:
	    isPresent = 1
	else:
	    isPresent = 0

        cu = self.db.cursor()
        cu.execute("INSERT INTO Instances VALUES (NULL, ?, ?, ?, "
						   "?, ?, ?)",
                   (troveName, versionId, flavorId, 
		    ":".join([ "%.3f" % x for x in timeStamps]), isPresent,
                    pinned))
	return cu.lastrowid

    def delId(self, theId):
        assert(type(theId) is int)
        cu = self.db.cursor()
        cu.execute("DELETE FROM Instances WHERE instanceId=?", theId)

    def getId(self, theId, justPresent = True):
        cu = self.db.cursor()

	if justPresent:
	    pres = "AND isPresent=1"
	else:
	    pres = ""

        cu.execute("SELECT troveName, versionId, flavorId, isPresent "
		   "FROM Instances WHERE instanceId=? %s" % pres, theId)
	try:
	    return cu.next()
	except StopIteration:
            raise KeyError, theId

    def isPresent(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT isPresent FROM Instances WHERE "
			"troveName=? AND versionId=? AND "
			"flavorId=?", item)

	val = cu.fetchone()
	if not val:
	    return 0

	return val[0]

    def idIsPresent(self, instanceId):
        cu = self.db.cursor()
        cu.execute("SELECT isPresent FROM Instances WHERE "
			"instanceId=?", instanceId)

	val = cu.fetchone()
	if not val:
	    return 0

	return val[0]

    def setPresent(self, theId, val):
        cu = self.db.cursor()
	cu.execute("UPDATE Instances SET isPresent=? WHERE instanceId=%d" 
			% theId, val)

    def has_key(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"troveName=? AND versionId=? AND "
			"flavorId=?", item)
	return not(cu.fetchone() == None)

    def __getitem__(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"troveName=? AND versionId=? AND "
			"flavorId=?", item)
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, item

    def get(self, item, defValue, justPresent = True):
        cu = self.db.cursor()

	if justPresent:
	    pres = " AND isPresent=1"
	else:
	    pres = ""

        cu.execute("SELECT instanceId FROM Instances WHERE "
			"troveName=? AND versionId=? AND "
			"flavorId=? %s" % pres, item)
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

    def getVersion(self, instanceId):
        cu = self.db.cursor()
        cu.execute("""SELECT version, timeStamps FROM Instances
		      INNER JOIN Versions ON 
			    Instances.versionId = Versions.versionId
		      WHERE instanceId=?""", instanceId)
	try:
	    (s, t) = cu.next()
	    v = versions.VersionFromString(s)
	    v.setTimeStamps([ float(x) for x in t.split(":") ])
	    return v
	except StopIteration:
            raise KeyError, instanceId

class Flavors(idtable.IdTable):

    def addId(self, flavor):
	return idtable.IdTable.addId(self, flavor.freeze())

    def __getitem__(self, flavor):
        if not flavor:
            return 0
	return idtable.IdTable.__getitem__(self, flavor.freeze())

    def getId(self, flavorId):
	return deps.deps.ThawDependencySet(idtable.IdTable.getId(self, 
								 flavorId))

    def get(self, flavor, defValue):
        if not flavor:
            return 0
	return idtable.IdTable.get(self, flavor.freeze(), defValue)

    def __delitem__(self, flavor):
        assert(flavor)
	idtable.IdTable.__delitem__(self, flavor.freeze())

    def getItemDict(self, itemSeq):
	cu = self.db.cursor()
        cu.execute("SELECT %s, %s FROM %s WHERE %s in (%s)"
                   % (self.strName, self.keyName, self.tableName, self.strName,
		      ",".join(["'%s'" % x.freeze() for x in itemSeq])))
	return dict(cu)

    def __init__(self, db):
	idtable.IdTable.__init__(self, db, "Flavors", "flavorId", "flavor")
	cu = db.cursor()
	cu.execute("SELECT FlavorID from Flavors")
	if cu.fetchone() == None:
	    # reserve flavor 0 for "no flavor information"
	    cu.execute("INSERT INTO Flavors VALUES (0, NULL)")
	
class DBFlavorMap(idtable.IdMapping):

    def __init__(self, db):
	idtable.IdMapping.__init__(self, db, "DBFlavorMap", "instanceId", 
				   "flavorId")

def _doAnalyze(db):
    if sqlite3._sqlite.sqlite_version_info() <= (3, 2, 2):
        # ANALYZE didn't appear until 3.2.3
        return

    # perform table analysis to help the optimizer
    doAnalyze = False
    cu = db.cursor()
    # if there are pending changes, just re-run ANALYZE
    if db.inTransaction:
        doAnalyze = True
    else:
        # check to see if the sqlite_stat1 table exists. ANALYZE
        # creates it.
        cu.execute("SELECT COUNT(*) FROM sqlite_master "
                   "WHERE tbl_name='sqlite_stat1' AND type='table'")
        if not cu.fetchone()[0]:
            doAnalyze = True

    if doAnalyze:
        cu.execute('ANALYZE')

class Database:

    schemaVersion = 13

    def _createSchema(self, cu):
        cu.execute("SELECT COUNT(*) FROM sqlite_master WHERE "
                   "name='TroveTroves'")
        if cu.next()[0] == 0:
            cu.execute("""CREATE TABLE TroveTroves(instanceId INTEGER, 
					           includedId INTEGER,
                                                   byDefault BOOLEAN,
                                                   inPristine BOOLEAN)""")
	    cu.execute("CREATE INDEX TroveTrovesInstanceIdx ON "
			    "TroveTroves(instanceId)")
	    # this index is so we can quickly tell what troves are needed
	    # by another trove
	    cu.execute("CREATE INDEX TroveTrovesIncludedIdx ON "
			    "TroveTroves(includedId)")

            # XXX this index is used to enforce that TroveTroves  
            # only contains unique TroveTrove (instanceId, includedId)
            # pairs.
	    cu.execute("CREATE UNIQUE INDEX TroveTrovesInstIncIdx ON "
			    "TroveTroves(instanceId,includedId)")

    def __init__(self, path):
	self.db = sqlite3.connect(path, timeout=30000)

        try:
            self.db._begin()
        except sqlite3.ProgrammingError, e:
            # ignore attepting to write to a ro database, the db
            # might already be set up on the root filesystem while
            # conary is being run as a non-root user
            if str(e) != 'attempt to write a readonly database':
                raise

        if not self.versionCheck():
            raise OldDatabaseSchema
        self._createSchema(self.db.cursor())
	self.troveFiles = DBTroveFiles(self.db)
	self.instances = DBInstanceTable(self.db)
	self.versionTable = versiontable.VersionTable(self.db)
	self.flavors = Flavors(self.db)
	self.flavorMap = DBFlavorMap(self.db)
	self.depTables = deptable.DependencyTables(self.db)
	self.troveInfoTable = troveinfo.TroveInfoTable(self.db)

        _doAnalyze(self.db)

        if self.db.inTransaction:
            self.db.commit()
        self.needsCleanup = False
        self.addVersionCache = {}
        self.flavorsNeeded = {}

    def __del__(self):
        if not self.db.closed:
            self.db.close()
        del self.db

    def versionCheck(self):
        cu = self.db.cursor()
        count = cu.execute("SELECT COUNT(*) FROM sqlite_master WHERE "
                           "name='DatabaseVersion'").next()[0]
        if count == 0:
            # if DatabaseVersion does not exist, but any other tables do exist,
            # then the database version is old
            count = cu.execute("SELECT count(*) FROM sqlite_master").next()[0]
            if count:
                return False

            cu.execute("CREATE TABLE DatabaseVersion (version INTEGER)")
            cu.execute("INSERT INTO DatabaseVersion VALUES (?)", 
                       self.schemaVersion)
        else:
            version = cu.execute("SELECT * FROM DatabaseVersion").next()[0]
            if version == 2 or version == 3 or version == 4:
                import deptable, itertools, sys
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
                        self.r = deps.deps.DependencySet()
                        self.p = deps.deps.DependencySet()

                try:
                    cu.execute("UPDATE DatabaseVersion SET version=7")
                except sqlite3.ProgrammingError:
                    raise OldDatabaseSchema(
                      "The Conary database on this system is too old. "      \
                      "It will be automatically\nconverted as soon as you "  \
                      "run Conary with write permissions for the database\n" \
                      "(which normally means as root).")

                msg = "Converting database..."
                print msg,
                sys.stdout.flush()

                if version == 2:
                    cu.execute("ALTER TABLE DBInstances ADD COLUMN pinned "
                               "BOOLEAN")

                instances = [ x[0] for x in 
                              cu.execute("select instanceId from DBInstances") ]
                dtbl = deptable.DependencyTables(self.db)
                troves = []

                for instanceId in instances:
                    trv = FakeTrove()
                    dtbl.get(cu, trv, instanceId)
                    troves.append(trv)

                cu.execute("delete from dependencies")
                cu.execute("delete from requires")
                cu.execute("delete from provides")

                version = 5
                for instanceId, trv in itertools.izip(instances, troves):
                    dtbl.add(cu, trv, instanceId)

                self.db.commit()

                print "\r%s\r" %(' '*len(msg)),
                sys.stdout.flush()

            if version == 5:
                import sys
                cu.execute("ALTER TABLE TroveTroves ADD COLUMN inPristine "
                           "INTEGER")
                cu.execute("UPDATE TroveTroves SET inPristine=?", True)

                # erase unused versions
                msg = "Removing unused version strings...\r"
                print msg,
                sys.stdout.flush()
                cu.execute("""
                        DELETE FROM Versions WHERE versionId IN 
                            (SELECT versions.versionid FROM versions 
                                LEFT OUTER JOIN 
                                    (SELECT versionid AS usedversions FROM 
                                     dbinstances UNION 
                                     SELECT versionid AS
                                     usedversions FROM dbtrovefiles) 
                                ON usedversions = versions.versionid 
                             WHERE 
                                usedversions IS NULL)
                """)

                cu.execute("UPDATE DatabaseVersion SET version=6")
                version = 6
                print "\r%s\r" %(' ' * len(msg)),
                sys.stdout.flush()

            if version == 6:
                cu.execute('''DELETE FROM TroveTroves 
                              WHERE TroveTroves.ROWID in (
                                SELECT Second.ROWID
                                 FROM TroveTroves AS First
                                 JOIN TroveTroves as Second 
                                 ON(First.instanceId=Second.instanceId AND
                                    First.includedId=Second.includedId AND
                                    First.ROWID < Second.ROWID))''')
                cu.execute("CREATE UNIQUE INDEX TroveTrovesInstIncIdx ON "
                           "TroveTroves(instanceId,includedId)")
                cu.execute("UPDATE DatabaseVersion SET version=7")
                version = 7

            if version == 7:
                # we don't alter here because lots of indices have changed
                # names; this is just easier
                import sys
                msg = "Converting database to new schema..."
                print msg,
                sys.stdout.flush()
                cu.execute('DROP INDEX InstancesNameIdx')
                cu.execute('DROP INDEX InstancesIdx')
                DBInstanceTable(self.db)
                cu.execute('INSERT INTO Instances SELECT * FROM DBInstances')
                cu.execute('DROP TABLE DBInstances')
                Flavors(self.db)
                cu.execute('INSERT INTO Flavors SELECT * FROM DBFlavors '
                                'WHERE flavor IS NOT NULL')
                cu.execute('DROP TABLE DBFlavors')
                cu.execute("UPDATE DatabaseVersion SET version=8")
                version = 8
                self.db.commit()
                print "\r%s\r" %(' ' * len(msg)),

            if version == 8:
                import sys
                msg = "Reordering TroveInfo..."
                print msg,
                sys.stdout.flush()

                for klass, infoType in [
                              (trove.BuildDependencies,
                               trove._TROVEINFO_TAG_BUILDDEPS),
                              (trove.LoadedTroves,
                               trove._TROVEINFO_TAG_LOADEDTROVES) ]:
                    for instanceId, data in \
                            [ x for x in cu.execute(
                                "select instanceId, data from TroveInfo WHERE "
                                "infoType=?", infoType) ]:
                        obj = klass(data)
                        f = obj.freeze()
                        if f != data:
                            count += 1
                            cu.execute("update troveinfo set data=? where "
                                       "instanceId=? and infoType=?", f,
                                       instanceId, infoType)
                            cu.execute("delete from troveinfo where "
                                       "instanceId=? and infoType=?",
                                       instanceId, trove._TROVEINFO_TAG_SIGS)

                cu.execute("UPDATE DatabaseVersion SET version=9")
                self.db.commit()
                print "\r%s\r" %(' ' * len(msg)),
                version = 9

            if version == 9:
                import sys

                cu.execute("SELECT COUNT(*) FROM DBTroveFiles")
                total = cu.fetchone()[0]

                cu.execute("""SELECT
                                  instanceId, fileId, stream
                              FROM
                                  DBTroveFiles""")
                changes = []
                changedTroves = set()
                msg = ''
                for i, (instanceId, fileId, stream) in enumerate(cu):
                    i += 1
                    if i % 1000 == 0 or (i == total):
                        msg = ("\rReordering streams and recalculating "
                               "fileIds... %d/%d" %(i, total))
                        print msg,
                        sys.stdout.flush()

                    f = files.ThawFile(stream, fileId)
                    if not f.provides() and not f.requires():
                        # if there are no deps, skip
                        continue
                    newStream = f.freeze()
                    if newStream == stream:
                        # if the stream didn't change, skip
                        continue
                    newFileId = f.fileId()
                    changes.append((newFileId, newStream, fileId))
                    changedTroves.add(instanceId)

                # make the changes
                for newFileId, newStream, fileId in changes:
                    cu.execute("UPDATE DBTroveFiles SET "
                               "fileId=?, stream=? WHERE fileId=?",
                               (newFileId, newStream, fileId))

                # delete signatures for the instances we changed
                for instanceId in changedTroves:
                    cu.execute("""DELETE FROM
                                      troveinfo
                                  WHERE
                                      instanceId=? AND infoType=?""",
                               (instanceId, trove._TROVEINFO_TAG_SIGS))

                print "\r%s\r" %(' ' * len(msg)),
                cu.execute("UPDATE DatabaseVersion SET version=10")
                self.db.commit()
                version = 10

            if version == 10:
                # convert contrib.rpath.com -> contrib.rpath.org
                import sys
                msg = ''

                cu.execute('select count(*) from versions')
                total = cu.fetchone()[0]

                updates = []
                cu.execute("select versionid, version from versions")
                for i, (versionId, version) in enumerate(cu):
                    i += 1
                    msg = ('\rrenaming contrib.rpath.com to '
                           'contrib.rpath.org.  %d/%d' %(i, total))
                    print msg,
                    sys.stdout.flush()

                    if not versionId:
                        continue
                    new = version.replace('contrib.rpath.com',
                                          'contrib.rpath.org')
                    if version != new:
                        updates.append((versionId, new))

                for versionId, version in updates:
                    cu.execute("""update versions
                                      set version=?
                                  where versionid=?""", (version, versionId))
                    # erase signature troveinfo since the version changed
                    cu.execute("""delete from
                                      TroveInfo
                                  where
                                          infotype = 9
                                      and instanceid in
                                      (select
                                           instanceid
                                       from
                                           versions, instances
                                       where
                                           versions.versionid=?)""",
                               (versionId,))
                print "\r%s\r" %(' ' * len(msg)),
                cu.execute("UPDATE DatabaseVersion SET version=11")
                self.db.commit()
                version = 11

            if version == 11:
                import sys
                # calculate path hashes for every trove
                instanceIds = [ x[0] for x in cu.execute(
                        "select instanceId from instances") ]
                for i, instanceId in enumerate(instanceIds):
                    if i % 20 == 0:
                        print "Updating trove %d of %d\r" %  \
                                    (i, len(instanceIds)),
                        sys.stdout.flush()
                    ph = trove.PathHashes()
                    for path, in cu.execute(
                            "select path from dbtrovefiles where instanceid=?",
                            instanceId):
                        ph.addPath(path)
                    cu.execute("""
                        insert into troveinfo(instanceId, infoType, data)
                            values(?, ?, ?)""", instanceId,
                            trove._TROVEINFO_TAG_PATH_HASHES, ph.freeze())

                print " " * 40 + "\r",
                cu.execute("UPDATE DatabaseVersion SET version=12")
                self.db.commit()
                version = 12

            if version == 12:
                cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                           trove._TROVEINFO_TAG_SIGS)
                cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                           trove._TROVEINFO_TAG_FLAGS)
                cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                           trove._TROVEINFO_TAG_INSTALLBUCKET)

                flags = trove.TroveFlagsStream()
                flags.isCollection(set = True)
                collectionStream = flags.freeze()
                flags.isCollection(set = False)
                notCollectionStream = flags.freeze()

                cu.execute("""
                    INSERT INTO TroveInfo
                        SELECT instanceId, ?, ?
                            FROM Instances WHERE
                                NOT (   trovename LIKE '%:%'
                                     OR trovename LIKE 'fileset-%')
                    """, trove._TROVEINFO_TAG_FLAGS, collectionStream)

                cu.execute("""
                    INSERT INTO TroveInfo
                        SELECT instanceId, ?, ?
                            FROM Instances WHERE
                                (   trovename LIKE '%:%'
                                 OR trovename LIKE 'fileset-%')
                    """, trove._TROVEINFO_TAG_FLAGS, notCollectionStream)

                cu.execute("UPDATE DatabaseVersion SET version=13")
                self.db.commit()
                version = 13

            if version != self.schemaVersion:
                return False

        return True

    def iterAllTroveNames(self):
	return self.instances.iterNames()

    def iterFindByName(self, name, pristine = False):
	for (instanceId, versionId, troveName, flavorId) in self.instances.iterByName(name):
	    yield self._getTrove(troveName = troveName,
				 troveInstanceId = instanceId, 
				 troveVersionId = versionId,
				 troveFlavorId = flavorId,
				 pristine = pristine)

    def iterVersionByName(self, name, withFlavors):
	cu = self.db.cursor()

        if withFlavors:
            flavorCol = "flavor"
            flavorClause = """INNER JOIN Flavors ON
                            Flavors.flavorId = Instances.flavorId"""
        else:
            flavorCol = "NULL"
            flavorClause = ""

	cu.execute("""SELECT DISTINCT version, timeStamps, %s 
                        FROM Instances NATURAL JOIN Versions 
                        %s
		        WHERE troveName='%s' AND isPresent=1"""
                            % (flavorCol, flavorClause, name))

        flavors = {}

 	for (match, timeStamps, flavorStr) in cu:
            ts = [float(x) for x in timeStamps.split(':')]
            version = versions.VersionFromString(match, timeStamps=ts)

            if withFlavors:
                f = flavors.get(flavorStr, None)
                if f is None:
                    f = deps.deps.ThawDependencySet(flavorStr)
                    flavors[flavorStr] = f
                
                yield (version, f)
            else:
                yield (version)

    def getAllTroveFlavors(self, troveDict):
        outD = {}
        cu = self.db.cursor()
        for name, versionList in troveDict.iteritems():
            d = {}.fromkeys(versionList)
            outD[name] = d
            for key in d:
                d[key] = []
            cu.execute("""
                SELECT version, timeStamps, flavor FROM Instances
                    NATURAL JOIN Versions
                    INNER JOIN Flavors
                        ON Instances.flavorid = Flavors.flavorid
                WHERE troveName=? AND isPresent=1""", name)
            for (match, timeStamps, flavor) in cu:
                ts = [float(x) for x in timeStamps.split(':')]
                version = versions.VersionFromString(match, timeStamps=ts)
                if outD[name].has_key(version):
                    outD[name][version].append(deps.deps.ThawDependencySet(flavor))
        return outD

    def pinTroves(self, name, version, flavor, pin = True):
        if flavor.freeze() == "":
            flavorClause = "IS NULL"
        else:
            flavorClause = "= '%s'" % flavor.freeze()

        cu = self.db.cursor()
        cu.execute("""
            UPDATE Instances set pinned=? WHERE
                instanceId = (SELECT instanceId FROM Instances 
                    JOIN Flavors ON
                        Instances.flavorId = Flavors.flavorId
                    JOIN Versions ON
                        Instances.versionID = Versions.versionId
                    WHERE
                        troveName=? AND
                        version = ? AND
                        flavor %s)
        """ % flavorClause, pin, name, version.asString())

    def trovesArePinned(self, troveList):
        cu = self.db.cursor()
        cu.execute("CREATE TEMPORARY TABLE tlList (name STRING, "
                                "version STRING, flavor STRING)",
                   start_transaction = False)
        # count the number of items we're inserting
        count = 0
        for name, version, flavor in troveList:
            cu.execute("INSERT INTO tlList VALUES(?, ?, ?)", name, 
                       version.asString(), flavor.freeze(),
                       start_transaction = False)
            count += 1
        cu.execute("""
select
    pinned
from
    tlList, Instances, Versions, Flavors
where
        Instances.troveName = tlList.name
    and Versions.version = tlList.version
    and Instances.versionId = Versions.versionId
    and (    Flavors.flavor = tlList.flavor
          or Flavors.flavor is NULL and tlList.flavor = '' )
    and Instances.flavorId = Flavors.flavorId
order by
    tlList.rowId asc
""")
        results = [ x[0] for x in cu ]
        # make sure that we got the same number of results as our query
        assert(len(results) == count)
        cu.execute("DROP TABLE tlList", start_transaction = False)

        return results

    def hasByName(self, name):
	return self.instances.hasName(name)

    def getVersionId(self, version, cache):
	theId = cache.get(version, None)
	if theId:
	    return theId

	theId = self.versionTable.get(version, None)
	if theId == None:
	    theId = self.versionTable.addId(version)

	cache[version] = theId

	return theId

    def getInstanceId(self, troveName, versionId, flavorId,
		      timeStamps, isPresent = True):
	theId = self.instances.get((troveName, versionId, flavorId), 
				   None)
	if theId is None:
	    theId = self.instances.addId(troveName, versionId, flavorId,
					 timeStamps, isPresent = isPresent)

	return theId

    def addTrove(self, trove, pin = False):
	cu = self.db.cursor()

	troveName = trove.getName()
	troveVersion = trove.getVersion()
	troveVersionId = self.getVersionId(troveVersion, {})
	self.addVersionCache[troveVersion] = troveVersionId

	troveFlavor = trove.getFlavor()
	if troveFlavor:
	    self.flavorsNeeded[troveFlavor] = True

	for (name, version, flavor) in trove.iterTroveList():
	    if flavor:
		self.flavorsNeeded[flavor] = True

	if self.flavorsNeeded:
	    # create all of the flavor id's we'll need
	    cu.execute("CREATE TEMPORARY TABLE flavorsNeeded(empty INTEGER, "
							    "flavor STRING)")
	    for flavor in self.flavorsNeeded.keys():
		cu.execute("INSERT INTO flavorsNeeded VALUES(?, ?)", 
			   None, flavor.freeze())
	    cu.execute("""INSERT INTO Flavors 
			  SELECT flavorsNeeded.empty, flavorsNeeded.flavor
			  FROM flavorsNeeded LEFT OUTER JOIN Flavors
			      ON flavorsNeeded.flavor = Flavors.flavor
			      WHERE Flavors.flavorId is NULL
		       """)
	    cu.execute("DROP TABLE flavorsNeeded")
	    self.flavorsNeeded = {}

	# get all of the flavor id's we might need; this could be somewhat
	# more efficient for an update, but it's not clear making it
	# more efficient is actually a speedup (as we'd have to figure out
	# which flavorId's we need). it could be that all of this code
	# would get faster if we just added the files to a temporary table
	# first and insert'd into the final table???
	flavors = {}
	if troveFlavor:
	    flavors[troveFlavor] = True

	for (name, version, flavor) in trove.iterTroveList():
	    if flavor:
		flavors[flavor] = True

	flavorMap = self.flavors.getItemDict(flavors.iterkeys())
	del flavors

	if troveFlavor:
	    troveFlavorId = flavorMap[troveFlavor.freeze()]
	else:
	    troveFlavorId = 0

	# the instance may already exist (it could be referenced by a package
	# which has already been added, or it may be in the database as
	# not present)
	troveInstanceId = self.instances.get((troveName, troveVersionId, 
				    troveFlavorId), None, justPresent = False)
	if troveInstanceId:
	    self.instances.setPresent(troveInstanceId, 1)
	else:
	    assert(min(troveVersion.timeStamps()) > 0)
	    troveInstanceId = self.instances.addId(troveName, troveVersionId, 
				       troveFlavorId, troveVersion.timeStamps(),
                                       pinned = pin)
	
        assert(cu.execute("SELECT COUNT(*) FROM TroveTroves WHERE "
                          "instanceId=?", troveInstanceId).next()[0] == 0)

        cu.execute("""CREATE TEMPORARY TABLE IncludedTroves(
                                troveName STRING,
                                versionId INT,
                                flavorId INT,
                                timeStamps STRING,
                                byDefault BOOLEAN)
                   """)

	for (name, version, flavor) in trove.iterTroveList():
	    versionId = self.getVersionId(version, self.addVersionCache)
	    if flavor:
		flavorId = flavorMap[flavor.freeze()]
	    else:
		flavorId = 0
            cu.execute("INSERT INTO IncludedTroves VALUES(?, ?, ?, ?, ?)",
                       name, versionId, flavorId, 
                        ":".join([ "%.3f" % x for x in version.timeStamps()]), 
                       trove.includeTroveByDefault(name, version, flavor))

        # make sure every trove we include has an instanceid
        cu.execute("""
            INSERT INTO Instances SELECT NULL, IncludedTroves.troveName, 
                                           IncludedTroves.versionId, 
                                           IncludedTroves.flavorId,
                                           IncludedTroves.timeStamps, 0, 0
                FROM IncludedTroves LEFT OUTER JOIN Instances ON
                    IncludedTroves.troveName == Instances.troveName AND
                    IncludedTroves.versionId == Instances.versionId AND
                    IncludedTroves.flavorId == Instances.flavorId 
                WHERE
                    instanceId is NULL
            """)

        # now include the troves in this one
        cu.execute("""
            INSERT INTO TroveTroves SELECT ?, instanceId, byDefault, ?
                FROM IncludedTroves JOIN Instances ON
                    IncludedTroves.troveName == Instances.troveName AND
                    IncludedTroves.versionId == Instances.versionId AND
                    IncludedTroves.flavorId == Instances.flavorId 
            """, troveInstanceId, True)

        cu.execute("DROP TABLE IncludedTroves")

        self.depTables.add(cu, trove, troveInstanceId)
        self.troveInfoTable.addInfo(cu, trove, troveInstanceId)
        
        cu.execute("select instanceId from trovetroves where includedid=?", troveInstanceId)
        for x, in cu:
            self._sanitizeTroveCollection(cu, x, nameHint = trove.getName())

        self._sanitizeTroveCollection(cu, troveInstanceId)

        addFile = cu.compile(self.troveFiles.addItemStmt)

	return (cu, troveInstanceId, addFile)

    def _sanitizeTroveCollection(self, cu, instanceId, nameHint = None):
        # examine the list of present, missing, and not inPristine troves
        # for a collection and make sure the set is sane
        if nameHint:
            nameClause = "Instances.troveName = '%s' AND" % nameHint
        else:
            nameClause = ""

        #import lib
        #lib.epdb.st('f')

        cu.execute("""
            SELECT includedId, troveName, version, flavor, isPresent, inPristine
                FROM TroveTroves JOIN Instances ON
                    TroveTroves.includedId = Instances.instanceId
                JOIN Versions ON
                    Instances.versionId = Versions.versionId
                JOIN Flavors ON
                    Instances.flavorId = Flavors.flavorId
                WHERE
                    %s
                    TroveTroves.instanceId = ?
        """ % nameClause, instanceId)

        pristineTrv = trove.Trove('foo', versions.NewVersion(),
                                  deps.deps.DependencySet(), None)
        currentTrv = trove.Trove('foo', versions.NewVersion(),
                                  deps.deps.DependencySet(), None)
        instanceDict = {}
        for (includedId, name, version, flavor, isPresent, inPristine) in cu:
            if flavor is None:
                flavor = deps.deps.DependencySet()
            else:
                flavor = deps.deps.ThawDependencySet(flavor)

            version = versions.VersionFromString(version)

            instanceDict[(name, version, flavor)] = includedId
            if isPresent:
                currentTrv.addTrove(name, version, flavor)
            if inPristine:
                pristineTrv.addTrove(name, version, flavor)

        linkByName = {}
        trvChanges = currentTrv.diff(pristineTrv)[2]
        makeLink = set()
        for (name, oldVersion, newVersion, oldFlavor, newFlavor) in trvChanges:
            if oldVersion is None:
                badInstanceId = instanceDict[(name, newVersion, newFlavor)]
                # we know it isn't in the pristine; if it was, it would
                # be in both currentTrv and pristineTrv, and not show up
                # as a diff
                cu.execute("DELETE FROM TroveTroves WHERE instanceId=? AND "
                           "includedId=?", instanceId, badInstanceId)
            elif newVersion is None:
                # this thing should be linked to something else. 
                linkByName.setdefault(name, set()).add(oldVersion.branch())
                makeLink.add((name, oldVersion, oldFlavor))

        if not linkByName: return

        for (name, version, flavor) in self.findByNames(linkByName):
            if version.branch() in linkByName[name]:
                currentTrv.addTrove(name, version, flavor, presentOkay = True)

        trvChanges = currentTrv.diff(pristineTrv)[2]
        for (name, oldVersion, newVersion, oldFlavor, newFlavor) in trvChanges:
            if (name, oldVersion, oldFlavor) not in makeLink: continue
            if newVersion is None: continue

            oldIncludedId = instanceDict[(name, oldVersion, oldFlavor)]
            byDefault = cu.execute("""
                    SELECT byDefault FROM TroveTroves WHERE
                        instanceId=? and includedId=?""",
                    instanceId, oldIncludedId).next()[0]

            if newFlavor:
                flavorStr = "= '%s'" % newFlavor.freeze()
            else:
                flavorStr = "IS NULL"

            cu.execute("""
                INSERT INTO TroveTroves SELECT ?, instanceId, ?, 0
                    FROM Instances JOIN Versions ON 
                        Instances.versionId = Versions.versionId
                    JOIN Flavors ON
                        Instances.flavorId = Flavors.flavorId
                    WHERE
                        troveName = ? AND
                        version = ? AND
                        flavor %s
                """ % flavorStr, instanceId, byDefault, name,
                        newVersion.asString())

    def addFile(self, troveInfo, pathId, fileObj, path, fileId, fileVersion,
                fileStream = None):
	(cu, troveInstanceId, addFileStmt) = troveInfo
	versionId = self.getVersionId(fileVersion, self.addVersionCache)

	if fileObj:
            if fileStream is None:
                fileStream = fileObj.freeze()

	    self.troveFiles.addItem(cu, fileObj.pathId(), 
                                    versionId, path, 
                                    fileId, troveInstanceId, 
                                    fileStream, fileObj.tags,
                                    addItemStmt = addFileStmt)
	else:
	    cu.execute("""
		UPDATE DBTroveFiles SET instanceId=? WHERE
		    fileId=? AND pathId=? AND versionId=?""", 
                    troveInstanceId, fileId, pathId, versionId)

    def getFile(self, pathId, fileId, pristine = False):
	stream = self.troveFiles.getFileByFileId(fileId, 
						 justPresent = not pristine)[1]
	return files.ThawFile(stream, pathId)

    def findFileVersion(self, fileId):
        cu = self.db.cursor()
        cu.execute("""SELECT stream FROM DBTroveFiles
                          INNER JOIN Versions ON
                              DBTroveFiles.versionId == Versions.versionId
                      WHERE fileId == ?""", fileId)
                            
        for (stream,) in cu:
            return files.ThawFile(stream, None)

        return None

    def iterFiles(self, l):
	cu = self.db.cursor()

	cu.execute("""
	    CREATE TEMPORARY TABLE getFilesTbl(row INTEGER PRIMARY KEY,
                                               fileId BINARY)
	""", start_transaction = False)

	for (i, (pathId, fileId, version)) in enumerate(l):
	    cu.execute("INSERT INTO getFilesTbl VALUES (?, ?)", 
		       i, fileId, start_transaction = False)

	cu.execute("""
	    SELECT DISTINCT row, stream FROM getFilesTbl 
                JOIN DBTroveFiles ON
		    getFilesTbl.fileId = DBTroveFiles.fileId
	""")

        l2 = [ None ] * len(l)

	for (row, stream) in cu:
            fObj = files.ThawFile(stream, l[row][0])
            assert(l[row][1] == fObj.fileId())
            l2[row] = fObj

        cu.execute("DROP TABLE getFilesTbl", start_transaction = False)

        return l2

    def hasTroves(self, troveList):
        instances = self._lookupTroves(troveList)
        result = [ False ] * len(troveList)
        for i, instanceId in enumerate(instances):
            if instanceId is not None:
                result[i] = True

        return result

    def getTroves(self, troveList, pristine):
        # returns a list parallel to troveList, with nonexistant troves
        # filled in w/ None
        instances = self._lookupTroves(troveList)
        for i, instanceId in enumerate(instances):
            if instanceId is not None:
                instances[i] = self._getTrove(pristine, 
                                              troveInstanceId = instanceId)

        return instances

    def _lookupTroves(self, troveList):
        # returns a list parallel to troveList, with nonexistant troves
        # filled in w/ None
        cu = self.db.cursor()

        cu.execute("""CREATE TEMPORARY TABLE getTrovesTbl(
                                idx INTEGER PRIMARY KEY,
                                troveName STRING,
                                troveVersion STRING,
                                flavorId INT)
                   """, start_transaction = False)

        for i, (name, version, flavor) in enumerate(troveList):
            flavorId = self.flavors.get(flavor, "")
            if flavorId == "":
                continue

            cu.execute("INSERT INTO getTrovesTbl VALUES(?, ?, ?, ?)",
                       i, name, version.asString(), flavorId,
                       start_transaction = False)

        cu.execute("""SELECT idx, Instances.instanceId FROM getTrovesTbl 
                        INNER JOIN Versions ON
                            Versions.version == getTrovesTbl.troveVersion
                        INNER JOIN Instances ON
                            getTrovesTbl.troveName == Instances.troveName AND
                            getTrovesTbl.flavorId == Instances.flavorId AND
                            Instances.versionId == Versions.versionId AND
                            Instances.isPresent == 1
                    """)

        r = [ None ] * len(troveList)
        for (idx, instanceId) in cu:
            r[idx] = instanceId

        cu.execute("DROP TABLE getTrovesTbl", start_transaction = False)

        return r

    def getTrove(self, troveName, troveVersion, troveFlavor, pristine = True,
		 withFiles = True):
	return self._getTrove(troveName = troveName, 
			      troveVersion = troveVersion, 
			      troveFlavor = troveFlavor,
			      pristine = pristine,
			      withFiles = withFiles)

    def _getTrove(self, pristine, troveName = None, troveInstanceId = None, 
		  troveVersion = None, troveVersionId = None,
		  troveFlavor = 0, troveFlavorId = None, withFiles = True):
	if not troveName:
	    (troveName, troveVersionId, troveFlavorId) = \
		    self.instances.getId(troveInstanceId,
                                         justPresent = not pristine)[0:3]

	if not troveVersionId:
	    troveVersionId = self.versionTable[troveVersion]

	if troveFlavorId is None:
	    if troveFlavor is None:
		troveFlavorId = 0
	    else:
		troveFlavorId = self.flavors[troveFlavor]
	
	if troveFlavor is 0:
	    if troveFlavorId == 0:
		troveFlavor = deps.deps.DependencySet()
	    else:
		troveFlavor = self.flavors.getId(troveFlavorId)

	if not troveInstanceId:
	    troveInstanceId = self.instances.get((troveName, 
			    troveVersionId, troveFlavorId), None)
	    if troveInstanceId is None:
		raise KeyError, troveName

	if not troveVersion or min(troveVersion.timeStamps()) == 0:
	    troveVersion = self.instances.getVersion(troveInstanceId)

	trv = trove.Trove(troveName, troveVersion, troveFlavor, None)

	flavorCache = {}

	# add all of the troves which are references from this trove; the
	# flavor cache is already complete
	cu = self.db.cursor()
        if pristine:
            pristineClause = "TroveTroves.inPristine = 1"
        else:
            pristineClause = "Instances.isPresent = 1"

	cu.execute("""
	    SELECT troveName, versionId, byDefault, timeStamps, 
                   Flavors.flavorId, flavor FROM 
		TroveTroves INNER JOIN Instances INNER JOIN Flavors ON 
		    TroveTroves.includedId = Instances.instanceId AND
		    Flavors.flavorId = Instances.flavorId 
		WHERE TroveTroves.instanceId = ? AND
                      %s
	""" % pristineClause, troveInstanceId)

	versionCache = {}
	for (name, versionId, byDefault, timeStamps, flavorId, flavorStr) in cu:
	    version = self.versionTable.getBareId(versionId)
	    version.setTimeStamps([ float(x) for x in timeStamps.split(":") ])

	    if not flavorId:
		flavor = deps.deps.DependencySet()
	    else:
		flavor = flavorCache.get(flavorId, None)
		if flavor is None:
		    flavor = deps.deps.ThawDependencySet(flavorStr)
		    flavorCache[flavorId] = flavor

	    trv.addTrove(name, version, flavor, byDefault = byDefault)

        cu.execute("SELECT pathId, path, versionId, fileId, isPresent FROM "
                   "DBTroveFiles WHERE instanceId = ?", troveInstanceId)
	for (pathId, path, versionId, fileId, isPresent) in cu:
	    if not pristine and not isPresent:
		continue
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getBareId(versionId)
		versionCache[versionId] = version

	    trv.addFile(pathId, path, version, fileId)

        self.depTables.get(cu, trv, troveInstanceId)
        self.troveInfoTable.getInfo(cu, trv, troveInstanceId)

	return trv

    def eraseTrove(self, troveName, troveVersion, troveFlavor):
        cu = self.db.cursor()

        if not self.needsCleanup:
            self.needsCleanup = True
            cu.execute("CREATE TEMPORARY TABLE RemovedVersions "
                       "(rmvdVer INTEGER PRIMARY KEY)")

	troveVersionId = self.versionTable[troveVersion]
	if troveFlavor is None:
	    troveFlavorId = 0
	else:
	    troveFlavorId = self.flavors[troveFlavor]
	troveInstanceId = self.instances[(troveName, troveVersionId, 
					  troveFlavorId)]

        cu.execute("INSERT OR IGNORE INTO RemovedVersions "
                   "VALUES (?)", troveVersionId)
        cu.execute("""
                INSERT OR IGNORE INTO RemovedVersions 
                    SELECT DISTINCT DBTroveFiles.versionId FROM DBTroveFiles
                        WHERE 
                            DBTroveFiles.instanceId = ?""")
        cu.execute("""
                INSERT OR IGNORE INTO RemovedVersions 
                    SELECT DISTINCT Instances.versionId FROM
                        TroveTroves JOIN Instances ON
                            TroveTroves.instanceId = Instances.instanceId
                        WHERE 
                            TroveTroves.instanceId = ?""")

        wasIn = [ x for x in cu.execute("select distinct troveTroves.instanceId from instances join trovetroves on instances.instanceid = trovetroves.includedId where troveName=? and (trovetroves.inPristine = 0 or instances.isPresent = 0)", troveName) ] 

	self.troveFiles.delInstance(troveInstanceId)
        cu.execute("DELETE FROM TroveTroves WHERE instanceId=?",
                   troveInstanceId)
        cu.execute("DELETE FROM TroveTroves WHERE includedId=? AND "
                   "inPristine=0", troveInstanceId)
        self.depTables.delete(self.db.cursor(), troveInstanceId)

	# mark this trove as not present
	self.instances.setPresent(troveInstanceId, 0)

        for x, in wasIn:
            self._sanitizeTroveCollection(cu, x, nameHint = troveName)

    def commit(self):
	if self.needsCleanup:
	    # this join could be slow; it would be much better if we could
	    # restrict the select on Instances by instanceId, but that's
	    # not so easy and may require multiple passes (since we may
	    # now be able to remove a trove which was included by a trove
	    # which was included by a trove which was removed; getting that
	    # closure may have to be iterative?). that process may be faster 
	    # then the full join?
	    cu = self.db.cursor()
	    cu.execute("""
		DELETE FROM Instances WHERE instanceId IN 
		    (SELECT Instances.instanceId FROM 
			Instances LEFT OUTER JOIN TroveTroves 
			ON Instances.instanceId = TroveTroves.includedId 
			WHERE isPresent = 0 AND TroveTroves.includedId is NULL
		    );
		""")
            cu.execute("""DELETE FROM Versions WHERE Versions.versionId IN 
                            (SELECT rmvdVer FROM RemovedVersions
                                LEFT OUTER JOIN Instances ON
                                    rmvdVer == Instances.versionId
                                LEFT OUTER JOIN DBTroveFiles ON
                                    rmvdVer == DBTroveFiles.versionId
                                WHERE
                                    Instances.versionId is NULL AND
                                    DBTroveFiles.versionId is NULL)""")
            cu.execute("DROP TABLE RemovedVersions")
	    self.needsCleanup = False

	self.db.commit()
	self.addVersionCache = {}
	self.flavorsNeeded = {}

    def depCheck(self, jobSet, troveSource, findOrdering = False):
        return self.depTables.check(jobSet, troveSource, 
                                    findOrdering = findOrdering)
	
    def pathIsOwned(self, path):
	for instanceId in self.troveFiles.iterPath(path):
	    if self.instances.idIsPresent(instanceId):
		return True

	return False

    def iterFindByPath(self, path, pristine = False):
	for instanceId in self.troveFiles.iterPath(path):
	    if not self.instances.idIsPresent(instanceId):
		continue

	    trv = self._getTrove(troveInstanceId = instanceId, 
				 pristine = pristine)
	    yield trv
    
    def iterFindPathReferences(self, path):
        cu = self.db.cursor()
        cu.execute("""SELECT troveName, version, flavor, pathId 
                            FROM DBTroveFiles JOIN Instances ON
                                DBTroveFiles.instanceId = Instances.instanceId
                            JOIN Versions ON
                                Instances.versionId = Versions.versionId
                            JOIN Flavors ON
                                Flavors.flavorId = Instances.flavorId
                            WHERE
                                path = ?
                    """, path)

        for (name, version, flavor, pathId) in cu:
            version = versions.VersionFromString(version)
            if flavor is None:
                flavor = deps.deps.DependencySet()
            else:
                flavor = deps.deps.ThawDependencySet(flavor)

            yield (name, version, flavor, pathId)
            
    def removeFileFromTrove(self, trove, path):
	versionId = self.versionTable[trove.getVersion()]
        flavorId = self.flavors[trove.getFlavor()]
	instanceId = self.instances[(trove.getName(), versionId, flavorId)]
	self.troveFiles.removePath(instanceId, path)

    def removeFilesFromTrove(self, troveName, troveVersion, troveFlavor, pathIdList):
	versionId = self.versionTable[troveVersion]
        flavorId = self.flavors[troveFlavor]
	instanceId = self.instances[(troveName, versionId, flavorId)]
	self.troveFiles.removeFileIds(instanceId, pathIdList)

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False,
			 pristine = False):
	if sortByPath:
	    sort = " ORDER BY path"
	else:
	    sort =""
	cu = self.db.cursor()

	troveVersionId = self.versionTable[version]
	if flavor is None:
	    troveFlavorId = 0
	else:
	    troveFlavorId = self.flavors[flavor]
	troveInstanceId = self.instances[(troveName, troveVersionId, 
					  troveFlavorId)]
	versionCache = {}

	if pristine:
	    cu.execute("SELECT pathId, path, fileId, versionId, stream FROM "
		       "DBTroveFiles WHERE instanceId = ? "
		       "%s" % sort, troveInstanceId)
	else:
	    cu.execute("SELECT pathId, path, fileId, versionId, stream FROM "
		       "DBTroveFiles WHERE instanceId = ? "
		       "AND isPresent=1 %s" % sort, troveInstanceId)

	versionCache = {}
	for (pathId, path, fileId, versionId, stream) in cu:
	    version = versionCache.get(versionId, None)
	    if not version:
		version = self.versionTable.getBareId(versionId)
		versionCache[versionId] = version

	    if withFiles:
		fileObj = files.ThawFile(stream, pathId)
		yield (pathId, path, fileId, version, fileObj)
	    else:
		yield (pathId, path, fileId, version)

    def mapPinnedTroves(self, mapList):
        if not mapList:
            return

        cu = self.db.cursor()
        cu.execute("""CREATE TEMPORARY TABLE mlt(
                            name STRING,
                            pinnedVersion STRING,
                            pinnedFlavor STRING,
                            mappedVersion STRING,
                            mappedTimestamps STRING,
                            mappedFlavor STRING)""")
        
        for (name, pinnedInfo, mapInfo) in mapList:
            assert(sum(mapInfo[0].timeStamps()) > 0)
            if not pinnedInfo[1]:
                pinnedFlavor = None
            else:
                pinnedFlavor = pinnedInfo[1].freeze()

            if not mapInfo[1]:
                mapFlavor = None
            else:
                mapFlavor = mapInfo[1].freeze()

            cu.execute("INSERT INTO mlt VALUES(?, ?, ?, ?, ?, ?)", 
                       name, pinnedInfo[0].asString(), pinnedFlavor,
                       mapInfo[0].asString(), 
                        ":".join([ "%.3f" % x for x in 
                                    mapInfo[0].timeStamps()]),
                       mapFlavor)

        # now add link collections to these troves
        cu.execute("""INSERT INTO TroveTroves 
                        SELECT TroveTroves.instanceId, pinnedInst.instanceId,
                               TroveTroves.byDefault, 0 FROM
                            mlt JOIN Flavors AS pinFlv ON
                                pinnedFlavor == pinFlv.flavor OR
                                pinnedFlavor IS NULL and pinFlv.flavor IS NULL
                            JOIN Versions AS pinVers ON
                                pinnedVersion == pinVers.version
                            JOIN Instances as pinnedInst ON
                                pinnedInst.troveName == mlt.name AND
                                pinnedInst.flavorId == pinFlv.flavorId AND
                                pinnedInst.versionId == pinVers.versionId
                            JOIN Flavors AS mapFlv ON
                                mappedFlavor == mapFlv.flavor OR
                                mappedFlavor IS NULL and mapFlv.flavor IS NULL
                            JOIN Versions AS mapVers ON
                                mappedVersion == mapVers.version
                            JOIN Instances as mapInst ON
                                mapInst.troveName == mlt.name AND
                                mapInst.flavorId == mapFlv.flavorId AND
                                mapInst.versionId == mapVers.versionId
                            JOIN TroveTroves ON
                                TroveTroves.includedId == mapInst.instanceId
                            LEFT JOIN TroveTroves AS dup ON
                                (dup.instanceId == TroveTroves.instanceId AND
                                 dup.includedId == pinnedInst.instanceId)
                            WHERE dup.instanceId IS NULL
                    """)

        cu.execute("DROP TABLE mlt")

    def getTroveContainers(self, l):
        cu = self.db.cursor()
        cu.execute("CREATE TEMPORARY TABLE ftc(idx INTEGER, name STRING, "
                                              "version STRING, "
                                              "flavor STRING)", 
                                              start_transaction = False)
        result = []
        for idx, info in enumerate(l):
            cu.execute("INSERT INTO ftc VALUES(?, ?, ?, ?)", idx, info[0],
                       info[1].asString(), info[2].freeze(), 
                       start_transaction = False)
            result.append([])

        cu.execute("""SELECT idx, Instances.troveName, Versions.version,
                             Flavors.flavor
                        FROM ftc JOIN Versions AS IncVersion ON
                            ftc.version = IncVersion.version
                        JOIN Flavors AS IncFlavor ON
                            ftc.flavor = IncFlavor.flavor OR
                            (ftc.flavor = "" AND IncFlavor.flavor IS NULL)
                        JOIN Instances AS IncInst ON
                            ftc.name = IncInst.troveName AND
                            IncVersion.versionId = IncInst.versionId AND
                            IncFlavor.flavorId = IncInst.flavorId
                        JOIN TroveTroves ON
                            IncInst.instanceId = TroveTroves.includedId
                        JOIN Instances ON
                            TroveTroves.instanceId = Instances.instanceId
                        JOIN Flavors ON
                            Instances.flavorId = Flavors.flavorId
                        JOIN Versions ON
                            Instances.versionId = Versions.versionId
                """)
        for (idx, name, version, flavor) in cu:
            result[idx].append((name, versions.VersionFromString(version),
                                deps.deps.ThawDependencySet(flavor)))

        cu.execute("DROP TABLE ftc", start_transaction = False)

        return result

    def findTroveContainers(self, names):
        # XXX this fn could be factored out w/ getTroveContainers above 
        cu = self.db.cursor()
        cu.execute("CREATE TEMPORARY TABLE ftc(idx INTEGER, name STRING)",
                                              start_transaction = False)
        for idx, name in enumerate(names):
            cu.execute("INSERT INTO ftc VALUES(?, ?)", idx, name,
                       start_transaction = False)

        cu.execute("""SELECT idx, Instances.troveName, Versions.version,
                             Flavors.flavor
                        FROM ftc
                        JOIN Instances AS IncInst ON
                            ftc.name = IncInst.troveName 
                        JOIN TroveTroves ON
                            IncInst.instanceId = TroveTroves.includedId
                        JOIN Instances ON
                            TroveTroves.instanceId = Instances.instanceId
                        JOIN Flavors ON
                            Instances.flavorId = Flavors.flavorId
                        JOIN Versions ON
                            Instances.versionId = Versions.versionId
                """)
        result = [ [] for x in names ]
        for (idx, name, version, flavor) in cu:
            result[idx].append((name, versions.VersionFromString(version),
                                deps.deps.ThawDependencySet(flavor)))

        cu.execute("DROP TABLE ftc", start_transaction = False)

        return result

    def findTroveReferences(self, names):
        """ return trove tuples that a) have a name in the given list of 
            names and b) are referenced by the pristine version of troves 
            installed in this system.
            Note that the trove tuples returned may not be installed - they 
            merely must be referenced by an installed trove.
        """
        cu = self.db.cursor()
        cu.execute("CREATE TEMPORARY TABLE ftc(idx INTEGER, name STRING)",
                                              start_transaction = False)
        for idx, name in enumerate(names):
            cu.execute("INSERT INTO ftc VALUES(?, ?)", idx, name,
                       start_transaction = False)

        # the JOIN TroveTroves on includedId ensures that this trove
        # is pointed to somewhere!
        cu.execute("""SELECT idx, Instances.troveName, Versions.version,
                             Flavors.flavor
                        FROM ftc
                        JOIN Instances AS IncInst ON
                            ftc.name = IncInst.troveName 
                        JOIN TroveTroves ON
                            (IncInst.instanceId = TroveTroves.includedId 
                             AND TroveTroves.inPristine = 1)
                        JOIN Instances ON
                            IncInst.instanceId = Instances.instanceId
                        JOIN Flavors ON
                            IncInst.flavorId = Flavors.flavorId
                        JOIN Versions ON
                            IncInst.versionId = Versions.versionId
                """)
        result = [ [] for x in names ]
        for (idx, name, version, flavor) in cu:
            result[idx].append((name, versions.VersionFromString(version),
                                deps.deps.ThawDependencySet(flavor)))
        cu.execute("DROP TABLE ftc", start_transaction = False)
        return result

    def findUnreferencedTroves(self):
        cu = self.db.cursor()
        cu.execute("""
                SELECT troveName, version, flavor FROM Instances 
                    LEFT OUTER JOIN TroveTroves ON 
                        Instances.instanceId = TroveTroves.includedId 
                    JOIN Versions ON 
                        Instances.versionId = Versions.versionId 
                    JOIN Flavors ON
                        Instances.flavorId = Flavors.flavorId
                    WHERE 
                        includedid IS NULL AND 
                        version NOT LIKE "%/local@LOCAL:%"
        """)

        l = []
        for (name, version, flavorStr) in cu:
            if flavorStr is None:
                flavorStr = deps.deps.DependencySet()
            else:
                flavorStr = deps.deps.ThawDependencySet(flavorStr)

            l.append((name, versions.VersionFromString(version), flavorStr))
                    
        return l

    def findRemovedByName(self, name):
        """
        Returns information on erased troves with a given name.
        """

        cu = self.db.cursor()

        cu.execute("""SELECT troveName, version, flavor FROM
                            Instances JOIN Versions ON
                                Instances.versionId = Versions.versionId
                            JOIN Flavors ON
                                Instances.flavorId = Flavors.flavorId
                            WHERE
                                isPresent = 0 AND
                                troveName = (?)""", name)

        return [ (n, versions.VersionFromString(v),
                  deps.deps.ThawDependencySet(f)) for (n, v, f) in cu ]

    def findByNames(self, nameList):
        cu = self.db.cursor()

        cu.execute("""SELECT troveName, version, flavor FROM
                            Instances JOIN Versions ON
                                Instances.versionId = Versions.versionId
                            JOIN Flavors ON
                                Instances.flavorId = Flavors.flavorId
                            WHERE
                                isPresent = 1 AND
                                troveName IN (%s)""" %
                    ",".join(["'%s'" % x for x in nameList]))

        return [ (n, versions.VersionFromString(v),
                  deps.deps.ThawDependencySet(f)) for (n, v, f) in cu ]

    def iterFilesWithTag(self, tag):
	return self.troveFiles.iterFilesWithTag(tag)

    def getTrovesWithProvides(self, depSetList):
        return self.depTables.getLocalProvides(depSetList)

    def close(self):
	self.db.close()

class OldDatabaseSchema(Exception):

    def __str__(self):
        return self.msg

    def __init__(self, msg = None):
        if msg:
            self.msg = msg
        else:
            self.msg = "The Conary database on this system is too old. "    \
                       "For information on how to\nconvert this database, " \
                       "please visit http://wiki.rpath.com/ConaryConversion."
