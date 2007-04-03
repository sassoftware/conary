# Copyright (c) 2005-2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
from conary.local import schema

class TroveInfoTable:
    def __init__(self, db):
        self.db = db
        schema.createTroveInfo(db)

    def addInfo(self, cu, trove, idNum):
        # c = True if the trove is a component
        n = trove.getName()
        c = ':' in n and not n.endswith(':source')
        for (tag, (size, streamType, name)) in trove.troveInfo.streamDict.iteritems():
            frz = trove.troveInfo.__getattribute__(name).freeze()
            if frz:
                # FIXME: somehow we're getting buildReqs and loadedTrovs in the
                # troveInfo table for components.  prevent this until the bug
                # is found.
                if c and (tag == 4 or tag == 5):
                    raise RuntimeError('attempted to add build requires '
                                       'trove info for a component: %s' %n)
                cu.execute("INSERT INTO TroveInfo (instanceId, infoType, data) "
                           "VALUES (?, ?, ?)", (idNum, tag, cu.binary(frz)))

        frz = trove.troveInfo.freeze(freezeKnown = False, freezeUnknown = True)
        if frz is not None:
            cu.execute("INSERT INTO TroveInfo (instanceId, infoType, data) "
                       "VALUES (?, ?, ?)", (idNum, -1, cu.binary(frz)))


    def getInfo(self, cu, trove, idNum):
        cu.execute("SELECT infoType, data FROM TroveInfo WHERE instanceId=?",
                   idNum)
        for (tag, frz) in cu:
            if tag == -1:
                trove.troveInfo.thaw(cu.frombinary(frz))
            else:
                name = trove.troveInfo.streamDict[tag][2]
                trove.troveInfo.__getattribute__(name).thaw(cu.frombinary(frz))
