#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
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

