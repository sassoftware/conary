
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
