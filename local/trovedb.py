#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import dbhash
import sqlite
import log
import os
import package
import struct
import versions
import sys

from StringIO import StringIO

class TroveDatabase:

    """
    Maintains an indexed package database. All of these access functions
    could be much more efficient; they instantiate a complete package object
    when quite often we just need the version or name.
    """
    def addTrove(self, trv):
	"""
	Add a trove to the database, along with the appropriate index
	entries.
	"""
	trvKey = self.trvs['COUNTER']
	trvId = struct.unpack('!i', trvKey)[0]
	self.trvs['COUNTER'] = struct.pack('!i', trvId + 1)
	str = "\0".join([trv.getName(), trv.getVersion().freeze(), 
			 trv.freeze()])
	self.trvs[trvKey] = str

        # update the SQL indexes
        cu = self.db.cursor()
        cu.execute("INSERT INTO TroveNames(id, name) VALUES (%d, %s)",
                   (trvId, trv.getName()))
	for (fileId, path, version) in trv.iterFileList():
            cu.execute("INSERT INTO TrovePaths(id, path) VALUES (%d, %s)",
                       (trvId, path))
	for (name, version) in trv.iterPackageList():
            cu.execute("INSERT INTO TrovePartOf(id, name) VALUES (%d, %s)",
                       (trvId, name))
        self.db.commit()

    def updateTrove(self, trv):
	"""
	Updates a trove in the database, along with the appropriate index
	entries.
	"""
	# FIXME: this could be more efficient
	self.delTrove(trv.getName(), trv.getVersion(), forUpdate = True)
	self.addTrove(trv)

    def delTrove(self, name, version, forUpdate = False):
        cu = self.db.cursor()
        cu.execute("SELECT id from TroveNames WHERE name=%s", (name,))
        for row in cu:
            trvId = row[0]
            trvKey = struct.pack('!i', trvId)
	    trv = self._getPackage(trvId)

	    if not trv.getVersion() == version:
		continue

	    del self.trvs[trvKey]
            cu = self.db.cursor()
            cu.execute("DELETE FROM TroveNames WHERE id=%d AND name=%s",
                       (trvId, trv.getName()))
            for (fileId, path, version) in trv.iterFileList():
                cu.execute("DELETE FROM TrovePaths WHERE id=%d AND path=%s",
                           (trvId, path))
            for (name, version) in trv.iterPackageList():
                cu.execute("DELETE FROM TrovePartOf WHERE id=%d AND name=%s",
                           (trvId, name))
            self.db.commit()

	if forUpdate:
	    return

        cu.execute("SELECT id FROM TrovePartOf WHERE name=%s", (name,))
        for row in cu:
            trv = self._getPackage(row[0])
	    updateTrove = False

	    if trv.hasPackageVersion(name, version):
		updateTrove = True
		trv.delPackageVersion(name, version, missingOkay = False)
		self.updateTrove(trv)

    def iterAllTroveNames(self):
        cu = self.db.cursor()
        cu.execute("SELECT name FROM TroveNames")
        for row in cu:
            yield row[0]

    def iterFindByName(self, name):
	"""
	Returns all of the troves with a particular name.

	@param name: name of the trove
	@type name: str
	@rtype: list of package.Trove
	"""
        cu = self.db.cursor()
        cu.execute("SELECT id FROM TroveNames WHERE name=%s", (name,))
        for row in cu:
	    yield self._getPackage(row[0])

    def iterFindByPath(self, path):
	"""
	Returns all of the troves containing a particular path.

	@param path: path to find in the troves
	@type path: str
	@rtype: list of package.Trove
	"""
        cu = self.db.cursor()
        cu.execute("SELECT id FROM TrovePaths WHERE path=%s", (path,))
        for row in cu:
	    yield self._getPackage(row[0])

    def pathIsOwned(self, path):
        cu = self.db.cursor()
        cu.execute("SELECT COUNT(*) FROM TrovePaths WHERE path=%s", (path,))
        return cu.fetchone()[0] > 0

    def hasByName(self, name):
        cu = self.db.cursor()
        cu.execute("SELECT COUNT(*) FROM TroveNames WHERE name=%s", (name,))
        return cu.fetchone()[0] > 0

    def _getPackage(self, trvId):
        trvKey = struct.pack('!i', trvId)
	(name, version, str) = self.trvs[trvKey].split("\0", 2)
	version = versions.ThawVersion(version)
	return package.TroveFromFile(name, StringIO(str), version)

    def __init__(self, top, mode):
	"""
	Initialize a new trove database.

	@param top: directory the data files are stored in
	@type top: str
	@param mode: mode of the database
	@type mode: "c" or "r"
	"""
	self.top = top
	p = top + "/troves.db"
	if not os.path.exists(p) and mode == "c":
	    self.trvs = dbhash.open(p, mode)
	    self.trvs['COUNTER'] = struct.pack("!i", 0)
	else:
	    self.trvs = dbhash.open(p, mode)

        p = top + "/troves.sql"
        self.db = sqlite.connect(p)
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master "
                   "WHERE type='table' ORDER BY tbl_name")
        tables = [ x[0] for x in cu.fetchall() ]
        if len(tables) == 0:
            cu.execute("CREATE TABLE TroveNames(id int, name string)")
            cu.execute("CREATE TABLE TrovePaths(id int, path string)")
            cu.execute("CREATE TABLE TrovePartof(id int, name string)")
	    cu.execute("CREATE INDEX TroveNamesIdx on TroveNames(name)")
	    cu.execute("CREATE INDEX TrovePathsIdx on TrovePaths(path)")
            self.db.commit()
        elif tables != ['TroveNames', 'TrovePartof', 'TrovePaths']:
            raise RuntimeError, 'database has unknown table layout'
