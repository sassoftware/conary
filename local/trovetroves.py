#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed with the whole that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

class TroveTroves:
    """
    Maps an id onto (possibly multiple) id(s)
    ids.
    """
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "TroveTroves" not in tables:
            cu.execute("CREATE TABLE TroveTroves(instanceId integer, "
					        "includedId integer)")
	    cu.execute("CREATE INDEX TroveTrovesInstanceIdx ON "
			    "TroveTroves(instanceId)")
	    # this index is so we can quickly tell what troves are needed
	    # by another trove
	    cu.execute("CREATE INDEX TroveTrovesIncludedIdx ON "
			    "TroveTroves(includedId)")

    def has_key(self, key):
        cu = self.db.cursor()
	
        cu.execute("SELECT includedId FROM TroveTroves "
			    "WHERE instanceId=%d", (key,))
		   
	item = cu.fetchone()	
	return item != None

    def __delitem__(self, key):
        cu = self.db.cursor()
	
        cu.execute("DELETE from TroveTroves WHERE instanceId=%d", key)

    def __getitem__(self, key):
        cu = self.db.cursor()
	
        cu.execute("SELECT includedId FROM TroveTroves "
			    "WHERE instanceId=%d", (key,))

	for match in cu:
	    yield match[0]

    def getIncludedBy(self, key):
        cu = self.db.cursor()
	
        cu.execute("SELECT instanceId FROM TroveTroves "
			    "WHERE includedId=%d", (key,))

	for match in cu:
	    yield match[0]

    def isIncluded(self, key):
        cu = self.db.cursor()
	
        cu.execute("SELECT instanceId FROM TroveTroves "
			    "WHERE includedId=%d", (key,))

	return cu.fetchone() is not None

    def addItem(self, key, val):
        cu = self.db.cursor()
        cu.execute("INSERT INTO TroveTroves VALUES (%d, %d)", (key, val))
