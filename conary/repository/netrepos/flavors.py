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

from conary.deps import deps

class Flavors:
    # manages the Flavors and FlavorMap tables
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "Flavors" not in tables:
	    cu.execute("""
            CREATE TABLE Flavors(
                flavorId        INTEGER PRIMARY KEY,
                flavor          STRING,
                CONSTRAINT Flavors_flavor_uq
                    UNIQUE(flavor)
            )""")
            cu.execute("""
            CREATE TABLE FlavorMap(
                flavorId        INTEGER,
                base            STRING,
                sense           INTEGER,
                flag            STRING,
                CONSTRAINT FlavorMap_flavorId_fk
                    FOREIGN KEY (flavorId) REFERENCES Flavors(flavorId)
                    ON DELETE CASCADE ON UPDATE CASCADE
            )""")
            cu.execute("""CREATE INDEX FlavorMapIndex ON FlavorMap(flavorId)""")
            cu.execute("""INSERT INTO Flavors VALUES (0, 'none')""")

    def createFlavor(self, flavor):
	cu = self.db.cursor()
	cu.execute("INSERT INTO Flavors VALUES (NULL, ?)", flavor.freeze())
	flavorId = cu.lastrowid

	for depClass in flavor.getDepClasses().itervalues():
	    for dep in depClass.getDeps():
		cu.execute("INSERT INTO FlavorMap VALUES (?, ?, ?, NULL)",
			   flavorId, dep.name, deps.FLAG_SENSE_REQUIRED)
		for (flag, sense) in dep.flags.iteritems():
		    cu.execute("INSERT INTO FlavorMap VALUES (?, ?, ?, ?)",
			       flavorId, dep.name, sense, flag)

    def __getitem__(self, flavor):
	val = self.get(flavor, 0)

	if val == '0':
            raise KeyError, flavor

	return val

    def get(self, flavor, defValue):
	if flavor is None:
	    return 0

	cu = self.db.cursor()
	cu.execute("SELECT flavorId FROM Flavors WHERE flavor = ?", 
		   flavor.freeze())
	item = cu.fetchone()
	if item is None:
	    return defValue
	return item[0]

    def getId(self, flavorId):
	if flavorId == 0:
	    return deps.DependencySet()

	cu = self.db.cursor()
	cu.execute("SELECT flavor FROM Flavors WHERE flavorId = ?", 
		   flavorId)
	try:
	    return deps.ThawDependencySet(cu.next()[0])
	except StopIteration:
            raise KeyError, flavorId

class FlavorScores:

    def __init__(self, db):
        cu = db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "FlavorScores" not in tables:
            cu.execute("""
            CREATE TABLE FlavorScores(
                request         INTEGER,
                present         INTEGER,
                value           INTEGER NOT NULL DEFAULT -1000000,
                CONSTRAINT FlavorScores_request_fk
                        FOREIGN KEY (request) REFERENCES Flavors(flavorId)
                        ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT FlavorScores_present_fk
                        FOREIGN KEY (request) REFERENCES Flavors(flavorId)
                        ON DELETE CASCADE ON UPDATE CASCADE
            )""")
            cu.execute("""CREATE UNIQUE INDEX FlavorScoresIdx ON 
                              FlavorScores(request, present)""")

            for (request, present), value in deps.flavorScores.iteritems():
                if value is None:
                    value = -1000000
                cu.execute("INSERT INTO FlavorScores VALUES(?,?,?)", 
                           request, present, value)
                            
