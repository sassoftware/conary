#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import idtable
import sqlite

class InstanceTable:
    """
    Generic table for assigning id's to a 3-tuple of IDs.
    """
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "Instances" not in tables:
            cu.execute("""CREATE TABLE Instances(
				instanceId INTEGER PRIMARY KEY, 
				itemId INT, 
				versionId INT, 
				flavorId INT,
				silentRedirect INT,
				isPresent INT);
			  CREATE UNIQUE INDEX InstancesIdx ON 
		               Instances(itemId, versionId, flavorId);
			""")

    def addId(self, itemId, versionId, flavorId):
        cu = self.db.cursor()
        cu.execute("INSERT INTO Instances VALUES (NULL, %d, %d, %d, 0, 1)",
                   (itemId, versionId, flavorId))
	return cu.lastrowid

    def addRedirect(self, itemId, versionId, redirectId):
        cu = self.db.cursor()
        cu.execute("INSERT INTO Instances VALUES (NULL, %d, %d, -1, %d, 1)",
                   (itemId, versionId, redirectId))
	return cu.lastrowid

    def delId(self, theId):
        assert(type(theId) is int)
        cu = self.db.cursor()
        cu.execute("DELETE FROM Instances WHERE instanceId=%d", theId)

    def getId(self, theId):
        cu = self.db.cursor()
        cu.execute("SELECT itemId, versionId, flavorId, isPresent "
		   "FROM Instances WHERE instanceId=%d", theId)
	try:
	    return cu.next()
	except StopIteration:
            raise KeyError, theId

    def isPresent(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT isPresent FROM Instances WHERE "
			"itemId=%d AND versionId=%d AND flavorId=%d", item)

	val = cu.fetchone()
	if not val:
	    return 0

	return val[0]

    def setPresent(self, theId, val):
        cu = self.db.cursor()
	cu.execute("UPDATE Instances SET isPresent=0 WHERE instanceId=%d" 
			% theId)

    def has_key(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"itemId=%d AND versionId=%d AND flavorId=%d", item)
	return not(cu.fetchone() == None)

    def __delitem__(self, item):
        cu = self.db.cursor()
        cu.execute("DELETE FROM Instances WHERE "
			"itemId=%d AND versionId=%d AND flavorId=%d", item)

    def __getitem__(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"itemId=%d AND versionId=%d AND flavorId=%d", item)
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, item

    def get(self, item, defValue):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"itemId=%d AND versionId=%d AND flavorId=%d", item)
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

    def removeUnused(self):
        cu = self.db.cursor()
	cu.execute("""
		DELETE from instances WHERE instanceId IN 
		    (SELECT Instances.instanceId from Instances 
		      LEFT OUTER JOIN TroveTroves ON 
		      Instances.instanceId = TroveTroves.includedId 
		      WHERE TroveTroves.includedId is NULL and 
			     Instances.isPresent = 0
		    );""")

class FileStreams:
    def __init__(self, db):
        self.db = db
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'FileStreams' not in tables:
            cu.execute("""CREATE TABLE FileStreams(streamId INTEGER PRIMARY KEY,
						   fileId STR,
						   versionId INT,
						   flavorId INT,
                                                   stream BINARY);""")
	    cu.execute("""CREATE UNIQUE INDEX FileStreamsIdx ON
			  FileStreams(fileId, versionId)""")
	    cu.execute("""CREATE INDEX FileStreamsVersionIdx ON
			  FileStreams(versionId)""")

	    #cu.execute("""
		#CREATE TRIGGER FileStreamsDel AFTER DELETE ON TroveFiles 
		#FOR EACH ROW 
		    #BEGIN 
		        #DELETE FROM FileStreams WHERE streamId = OLD.streamId; 
		    #END;
	    #""")

    def _rowGenerator(self, cu):
        for row in cu:
            yield row[0]
        
    def addStream(self, key, stream):
	(fileId, versionId, flavorId) = key
        cu = self.db.cursor()
        cu.execute("INSERT INTO FileStreams VALUES (NULL, %s, %d, %d, %s)",
                   (fileId, versionId, flavorId, sqlite.encode(stream)))
	return cu.lastrowid
        
    def __delitem__(self, key):
	(fileId, versionId) = key
        cu = self.db.cursor()
        cu.execute("DELETE FROM FileStreams WHERE "
			"fileId=%s and versionId=%d",
                   (fileId, versionId))

    def has_key(self, key):
	(fileId, versionId) = key
        cu = self.db.cursor()
        cu.execute("SELECT stream from FileStreams WHERE "
		    "fileId=%s and versionId=%d",
                   (fileId, versionId))
        row = cu.fetchone()
	return row is not None

    def __getitem__(self, key):
	(fileId, versionId) = key
        cu = self.db.cursor()
        cu.execute("SELECT stream from FileStreams WHERE "
		    "fileId=%s and versionId=%d",
                   (fileId, versionId))
        row = cu.fetchone()
        if row is None:
            raise KeyError, key
        return row[0]

    def getStreamId(self, key):
	(fileId, versionId) = key
        cu = self.db.cursor()
        cu.execute("SELECT streamId from FileStreams WHERE "
		    "fileId=%s and versionId=%d",
                   (fileId, versionId))
        row = cu.fetchone()
        if row is None:
            raise KeyError, key
        return row[0]

    def removeUnusedStreams(self):
        cu = self.db.cursor()
	cu.execute("""
	    DELETE from fileStreams WHERE streamId in 
		(SELECT streamId FROM 
		    (SELECT fileStreams.streamId, troveFiles.instanceId 
			from FileStreams LEFT OUTER JOIN TroveFiles ON 
			FileStreams.streamId = trovefiles.streamId) 
		WHERE instanceId is NULL)
	    """)
