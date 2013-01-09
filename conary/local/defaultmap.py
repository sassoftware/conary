#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
