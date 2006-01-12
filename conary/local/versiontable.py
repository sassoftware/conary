#
# Copyright (c) 2004 rPath, Inc.
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

from conary import versions
from conary.local import schema
from conary.dbstore import idtable

# XXX: this looks awfully similar to an idtable...
class VersionTable:
    """
    Maps a version to an id and timestamp pair.
    """
    noVersion = 0

    def __init__(self, db):
        self.db = db

    def addId(self, version):
        cu = self.db.cursor()
        cu.execute("INSERT INTO Versions (version) VALUES (?)",
		   version.asString())
	return cu.lastrowid

    def delId(self, theId):
        assert(type(theId) is int)
        cu = self.db.cursor()
        cu.execute("DELETE FROM Versions WHERE versionId=?", theId)

    def _makeVersion(self, str, timeStamps):
	v = versions.VersionFromString(str)
	v.setTimeStamps([ float(x) for x in timeStamps.split(":")])
	return v

    def getBareId(self, theId):
	"""
	Gets a version object w/o setting any timestamps.
	"""
        cu = self.db.cursor()
        cu.execute("""SELECT version FROM Versions
		      WHERE Versions.versionId=?""", theId)
	try:
	    (s, ) = cu.next()
	    return versions.VersionFromString(s)
	except StopIteration:
            raise KeyError, theId

    def has_key(self, version):
        cu = self.db.cursor()
        cu.execute("SELECT versionId FROM Versions WHERE version=?",
                   version.asString())
	return not(cu.fetchone() == None)

    def __delitem__(self, version):
        cu = self.db.cursor()
        cu.execute("DELETE FROM Versions WHERE version=?", version.asString())

    def __getitem__(self, version):
	v = self.get(version, None)
	if v == None:
            raise KeyError, version

	return v

    def get(self, version, defValue):
        cu = self.db.cursor()
        cu.execute("SELECT versionId FROM Versions WHERE version=?",
		   version.asString())

	item = cu.fetchone()
	if item:
	    return item[0]
	else:
	    return defValue

    def removeUnused(self):
	# removes versions which don't have parents and aren't used
	# by any FileStreams
        cu = self.db.cursor()
	cu.execute("""
	    DELETE FROM Versions WHERE versionId IN
		(SELECT versionId from Versions LEFT OUTER JOIN
		    (SELECT versionId AS fooId from Parent UNION
		     SELECT versionId AS fooId FROM FileStreams)
		ON Versions.versionId = fooId WHERE fooId is NULL)
	    """)

