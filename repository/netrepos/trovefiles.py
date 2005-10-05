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

class TroveFiles:
    """
    Maps an instanceId onto a (pathId, versionId, path) tuple
    """
    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "TroveFiles" not in tables:
            cu.execute("""
            CREATE TABLE TroveFiles(
                instanceId      INTEGER,
                streamId        INTEGER,
                versionId       BINARY,
                pathId          BINARY,
                path            STRING,
                CONSTRAINT TroveFiles_instanceId_fk
                    FOREIGN KEY (instanceId) REFERENCES Instances(instanceId)
                    ON DELETE RESTRICT ON UPDATE CASCADE,
                CONSTRAINT TroveFiles_streamId_fk
                    FOREIGN KEY (streamId) REFERENCES FileStreams(streamId)
                    ON DELETE RESTRICT ON UPDATE CASCADE
            )""")
	    cu.execute("CREATE INDEX TroveFilesIdx ON TroveFiles(instanceId)")
	    cu.execute("CREATE INDEX TroveFilesIdx2 ON TroveFiles(streamId)")
