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

import sqlite3

class InstanceTable:
    """
    Generic table for assigning id's to a 3-tuple of IDs.
    """
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("""SELECT tbl_name FROM sqlite_master
                      WHERE type='table' or type='view' """)
        tables = [ x[0] for x in cu ]
        if "Instances" not in tables:
            cu.execute("""
            CREATE TABLE Instances(
                instanceId      INTEGER PRIMARY KEY, 
                itemId          INTEGER, 
                versionId       INTEGER, 
                flavorId        INTEGER,
                isRedirect      INTEGER NOT NULL DEFAULT 0,
                isPresent       INTEGER NOT NULL DEFAULT 0,
                CONSTRAINT Instances_itemId_fk
                    FOREIGN KEY (itemId) REFERENCES Items(itemId)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT Instances_versionId_fk
                    FOREIGN KEY (versionId) REFERENCES Versions(versionId)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT Instances_flavorId_fk
                    FOREIGN KEY (flavorId) REFERENCES Flavors(flavorId)
                    ON DELETE RESTRICT ON UPDATE CASCADE
            )""")
            cu.execute(" CREATE UNIQUE INDEX InstancesIdx ON "
                       " Instances(itemId, versionId, flavorId) ")
        if "InstancesView" not in tables:
            cu.execute("""
            CREATE VIEW
                InstancesView AS
            SELECT
                Instances.instanceId as instanceId,
                Items.item as item,
                Versions.version as version,
                Flavors.flavor as flavor
            FROM
                Instances
            JOIN Items on Instances.itemId = Items.itemId
            JOIN Versions on Instances.versionId = Versions.versionId
            JOIN Flavors on Instances.flavorId = Flavors.flavorId
            """)

    def addId(self, itemId, versionId, flavorId, isRedirect, isPresent = True):
	if isPresent:
	    isPresent = 1
	else:
	    isPresent = 0

	if isRedirect:
	    isRedirect = 1
	else:
	    isRedirect = 0

        cu = self.db.cursor()
        cu.execute("INSERT INTO Instances VALUES (NULL, ?, ?, ?, ?, ?)",
                   (itemId, versionId, flavorId, isRedirect, isPresent))
        # XXX: sqlite-ism
	return cu.lastrowid

    def getId(self, theId):
        cu = self.db.cursor()
        cu.execute(" SELECT itemId, versionId, flavorId, isPresent "
		   " FROM Instances WHERE instanceId=? ", theId)
	try:
	    return cu.next()
	except StopIteration:
            raise KeyError, theId

    def isPresent(self, item):
        cu = self.db.cursor()
        cu.execute(" SELECT isPresent FROM Instances WHERE "
                   " itemId=? AND versionId=? AND flavorId=?", item)
	val = cu.fetchone()
	if not val:
	    return 0
	return val[0]

    def setPresent(self, theId, val):
        cu = self.db.cursor()
	cu.execute("UPDATE Instances SET isPresent=? WHERE instanceId=?",
                   (val, theId))

    def has_key(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"itemId=? AND versionId=? AND flavorId=?", item)
	return not(cu.fetchone() == None)

    def __getitem__(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"itemId=? AND versionId=? AND flavorId=?", item)
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, item

    def get(self, item, defValue):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"itemId=? AND versionId=? AND flavorId=?", item)
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

class FileStreams:
    def __init__(self, db):
        self.db = db
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'FileStreams' not in tables:
            cu.execute("""
            CREATE TABLE FileStreams(
                streamId INTEGER PRIMARY KEY,
                fileId BINARY,
                stream BINARY
            )""")
	    # in sqlite 2.8.15, a unique here seems to cause problems
	    # (as the versionId isn't unique, apparently)
	    cu.execute("""CREATE INDEX FileStreamsIdx ON FileStreams(fileId)""")
