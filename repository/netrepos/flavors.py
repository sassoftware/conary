#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import deps.deps

class Flavors:

    # manages the Flavors and FlavorMap tables

    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "Flavors" not in tables:
	    cu.execute("""CREATE TABLE Flavors(flavorId INTEGER PRIMARY KEY,
					       flavor STR UNIQUE);
	                  CREATE TABLE FlavorMap(flavorId INT,
						 base STR,
						 flag STR);
			  CREATE INDEX FlavorMapIndex ON FlavorMap(flavorId);
			  INSERT INTO Flavors VALUES (0, 'none');
		       """)

    def createFlavor(self, flavor):
	cu = self.db.cursor()
	cu.execute("INSERT INTO Flavors VALUES (NULL, %s)", flavor.freeze())
	flavorId = cu.lastrowid

	for depClass in flavor.getDepClasses().itervalues():
	    for dep in depClass.getDeps():
		cu.execute("INSERT INTO FlavorMap VALUES (%d, %s, NULL)",
			   flavorId, dep.name)
		for flag in dep.flags.iterkeys():
		    cu.execute("INSERT INTO FlavorMap VALUES (%d, %s, %s)",
			       flavorId, dep.name, flag)

    def __getitem__(self, flavor):
	val = self.get(flavor, 0)

	if val == '0':
            raise KeyError, flavor

	return val

    def get(self, flavor, defValue):
	if flavor is None:
	    return 0

	cu = self.db.cursor()
	cu.execute("SELECT flavorId FROM Flavors WHERE flavor = %s", 
		   flavor.freeze())
	item = cu.fetchone()
	if item is None:
	    return defValue
	return item[0]

    def getId(self, flavorId):
	if flavorId is 0:
	    return None

	cu = self.db.cursor()
	cu.execute("SELECT flavor FROM Flavors WHERE flavorId = %d", 
		   flavorId)
	try:
	    return deps.deps.ThawDependencySet(cu.next()[0])
	except StopIteration:
            raise KeyError, theId
