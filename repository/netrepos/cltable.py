import changelog

class ChangeLogTable:
    """
    Table for changelogs.
    """
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "ChangeLogs" not in tables:
            cu.execute("""CREATE TABLE ChangeLogs(
				nodeId INTEGER UNIQUE,
				name STR, 
				contact STR, 
				message STR);
			""")
	    cu.execute("INSERT INTO ChangeLogs values(0, NULL, NULL, NULL)")

    def add(self, nodeId, cl):
        cu = self.db.cursor()
        cu.execute("INSERT INTO ChangeLogs VALUES (%d, %s, %s, %s)",
                   (nodeId, cl.name, cl.contact, cl.message))
	return cu.lastrowid
