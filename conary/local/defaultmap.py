#
# Copyright (c) 2009 rPath, Inc.
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

from conary import trove

class DefaultMap(object):

    def __init__(self, db, troveInfoList = []):
        self.buildMap(db, troveInfoList)

    def isByDefault(self, troveInfo):
        return (troveInfo in map)

    def buildMap(self, db, troveInfoList):
        map = set()
        topTroves = db.getTroves([ x for x in troveInfoList
                                   if trove.troveIsCollection(x[0]) ],
                                 pristine = True)
        for trv in topTroves:
            for trvInfo, byDefault, isStrong in trv.iterTroveListInfo():
                if byDefault:
                    map.add(trvInfo)

        self.map = map
