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

class TroveFiles:
    """
    Maps an instanceId onto a (fileId, versionId, path) tuple
    """
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "TroveFiles" not in tables:
            cu.execute("""CREATE TABLE TroveFiles(
					  instanceId integer,
					  streamId integer,
					  path str)
		       """)
	    cu.execute("CREATE INDEX TroveFilesIdx ON TroveFiles(instanceId)")
	    cu.execute("CREATE INDEX TroveFilesIdx2 ON TroveFiles(streamId)")

    def has_key(self, key):
        cu = self.db.cursor()
	
        cu.execute("SELECT instanceId FROM TroveFiles "
			    "WHERE instanceId=%d", (key,))
		   
	item = cu.fetchone()	
	return item != None

    def __getitem__(self, key):
	cu = self.db.cursor()
	cu.execute("SELECT streamId, path FROM TroveFiles "
		   "WHERE instanceId=%s", (key,))
	for match in cu:
	    yield match

    def __delitem__(self, key):
        cu = self.db.cursor()
	
        cu.execute("DELETE from TroveFiles WHERE instanceId=%s", key)

    def addItem(self, instanceId, streamId, path):
        cu = self.db.cursor()
        cu.execute("INSERT INTO TroveFiles VALUES (%d, %d, %s)",
                   (instanceId, streamId, path ))

