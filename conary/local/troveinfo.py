# Copyright (c) 2005-2008 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
from conary import streams
from conary.local import schema

def createFreezer(tag):

    class Freezer(streams.StreamSet):

        pass

    Freezer.streamDict = { tag : (streams.DYNAMIC, streams.StringStream,
                                  'val' ) }

    return Freezer

class TroveInfoTable:
    def __init__(self, db):
        self.db = db
        schema.createTroveInfo(db)

    def addInfo(self, cu, trove, idNum):
        # c = True if the trove is a component
        n = trove.getName()
        # complete fixup is internal to a single client run; it should never be stored
        # anywhere
        assert(trove.troveInfo.completeFixup() is None)
        c = ':' in n and not n.endswith(':source')
        newInfo = []
        for (tag, (size, streamType, name)) in trove.troveInfo.streamDict.iteritems():
            frz = trove.troveInfo.__getattribute__(name).freeze()
            if frz:
                # FIXME: somehow we're getting buildReqs and loadedTrovs in the
                # troveInfo table for components.  prevent this until the bug
                # is found.
                if c and (tag == 4 or tag == 5):
                    raise RuntimeError('attempted to add build requires '
                                       'trove info for a component: %s' %n)
                newInfo.append((idNum, tag, cu.binary(frz)))

        frz = trove.troveInfo.freeze(freezeKnown = False, freezeUnknown = True)
        if frz:
            newInfo.append((idNum, -1, cu.binary(frz)))

        self.db.bulkload("TroveInfo", newInfo,
                         [ 'instanceId', 'infoType', 'data'] )


    def getInfo(self, cu, trove, idNum):
        cu.execute("SELECT infoType, data FROM TroveInfo WHERE instanceId=?",
                   idNum)
        unknown = ''
        for (tag, frz) in cu:
            if tag in trove.troveInfo.streamDict:
                name = trove.troveInfo.streamDict[tag][2]
                trove.troveInfo.__getattribute__(name).thaw(cu.frombinary(frz))
            elif tag == -1:
                unknown += cu.frombinary(frz)
            else:
                Freezer = createFreezer(tag)
                f = Freezer()
                f.val.set(frz)
                unknown += f.freeze()

        if unknown:
            trove.troveInfo.thaw(unknown)
