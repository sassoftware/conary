#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# XXX todo - normalize (sort) the that flags are joined, otherwise
# changes in hashing could change what we're looking for

class DepTable:
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'Dependencies' not in tables:
            cu.execute("""CREATE TABLE Dependencies(depId integer primary key,
                                                    class str,
                                                    name str,
                                                    flags str,
                                                    verConstraint str
                                                    )""")
    
    def add(self, depClass, dep):
        cu = self.db.cursor()
        cu.execute("""INSERT INTO Dependencies(depId, class, name, flags,
                                               verConstraint)
                      VALUES (NULL, %s, %s, %s, %s)""",
                   (depClass.tag, dep.name, " ".join(dep.flags.iterkeys()), ""))

    def delId(self, theId):
        assert(type(theId) is int)
        cu = self.db.cursor()
        cu.execute("DELETE FROM Dependencies WHERE depId=%d", theId)

    def delDep(self, depClass, dep):
        cu = self.db.cursor()
        cu.execute("""DELETE FROM Dependencies WHERE
		      class = %s AND
                      name = %s AND
                      flags = %s""",
                   (depClass.tag, dep.name, " ".join(dep.flags.iterkeys())))

    def get(self, depClass, dep):
        cu = self.db.cursor()
        cu.execute("""SELECT depId from Dependencies WHERE
                      class = %s AND
                      name = %s AND 
                      flags = %s""",
                   (depClass.tag, dep.name, " ".join(dep.flags.iterkeys())))
        row = cu.fetchone()
        if row is None:
            raise KeyError, dep
        return row[0]
