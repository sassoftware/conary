#!/usr/bin/python 
#
# Copyright (c) 2005 rPath, Inc.
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

class TroveInfoTable:

    def __init__(self, db):
        cu = db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'TroveInfo' not in tables:
            cu.execute("""CREATE TABLE TroveInfo(instanceId INT,
                                                 infoType INT,
                                                 data BIN)""")
            cu.execute("CREATE INDEX TroveInfoIdx ON TroveInfo(instanceId)")
            cu.execute("""CREATE INDEX TroveInfoIdx2 ON TroveInfo(infoType, 
                                                                  data)""")

    def addInfo(self, cu, trove, idNum):
        for (tag, (size, streamType, name)) in trove.troveInfo.streamDict.iteritems():
            frz = trove.troveInfo.__getattribute__(name).freeze()
            if frz:
                cu.execute("INSERT INTO TroveInfo VALUES (?, ?, ?)",
                           idNum, tag, frz)

    def getInfo(self, cu, trove, idNum):
        cu.execute("SELECT infoType, data FROM TroveInfo WHERE instanceId=?", 
                   idNum)
        for (tag, frz) in cu:
            name = trove.troveInfo.streamDict[tag][2]
            trove.troveInfo.__getattribute__(name).thaw(frz)
