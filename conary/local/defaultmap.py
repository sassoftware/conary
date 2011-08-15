#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
