
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


    def addInfo(self, cu, trove, idNum):
        for (tag, (streamType, name)) in trove.troveInfo.streamDict.iteritems():
            frz = trove.troveInfo.__getattribute__(name).freeze()
            if frz:
                cu.execute("INSERT INTO TroveInfo VALUES (?, ?, ?)",
                           idNum, tag, frz)

    def getInfo(self, cu, trove, idNum):
        cu.execute("SELECT infoType, data FROM TroveInfo WHERE instanceId=?", 
                   idNum)
        for (tag, frz) in cu:
            name = trove.troveInfo.streamDict[tag][1]
            trove.troveInfo.__getattribute__(name).thaw(frz)
