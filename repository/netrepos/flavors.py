#
# Copyright (c) 2004 Specifix, Inc.
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
					       flavor STR UNIQUE)""")
            cu.execute("""CREATE TABLE FlavorMap(flavorId INT,
						 base STR,
						 flag STR)""")
            cu.execute("""CREATE INDEX FlavorMapIndex ON FlavorMap(flavorId)""")
            cu.execute("""INSERT INTO Flavors VALUES (0, 'none')""")

    def createFlavor(self, flavor):
	cu = self.db.cursor()
	cu.execute("INSERT INTO Flavors VALUES (NULL, ?)", flavor.freeze())
	flavorId = cu.lastrowid

	for depClass in flavor.getDepClasses().itervalues():
	    for dep in depClass.getDeps():
		cu.execute("INSERT INTO FlavorMap VALUES (?, ?, NULL)",
			   flavorId, dep.name)
		for flag in dep.flags.iterkeys():
		    cu.execute("INSERT INTO FlavorMap VALUES (?, ?, ?)",
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
	cu.execute("SELECT flavorId FROM Flavors WHERE flavor = ?", 
		   flavor.freeze())
	item = cu.fetchone()
	if item is None:
	    return defValue
	return item[0]

    def getId(self, flavorId):
	if flavorId == 0:
	    return deps.deps.DependencySet()

	cu = self.db.cursor()
	cu.execute("SELECT flavor FROM Flavors WHERE flavorId = ?", 
		   flavorId)
	try:
	    return deps.deps.ThawDependencySet(cu.next()[0])
	except StopIteration:
            raise KeyError, flavorId
