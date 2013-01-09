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


from testrunner import testhelp
from conary import streams, trove
from conary.local import sqldb

class TroveInfoTest(testhelp.TestCase):

    def testUnknownTroveInfo(self):

        class ExtraTroveInfo(trove.TroveInfo):

            streamDict = dict(trove.TroveInfo.streamDict)
            streamDict[254] = (streams.DYNAMIC, streams.IntStream, 'unknown')

        class MockTrove:

            def getName(self):
                return 'sometrove'

            def __init__(self, troveInfo = None):
                self.troveInfo = troveInfo

        full = ExtraTroveInfo()
        full.sourceName.set('foo')
        full.unknown.set(10)

        db = sqldb.Database(':memory:')
        cu = db.db.cursor()

        tiTable = db.troveInfoTable

        f = full.freeze()
        trove.TroveInfo(f)

        tiTable.addInfo(cu, MockTrove(trove.TroveInfo(full.freeze())), 1)

        returned = trove.TroveInfo()
        tiTable.getInfo(cu, MockTrove(returned), 1)
        assert(full.freeze() == returned.freeze())

        # make sure that if there is no unknown troveinfo, that nothing
        # gets stored
        ti = trove.TroveInfo()
        ti.size.set(10)
        tiTable.addInfo(cu, MockTrove(ti), 2)
        cu.execute('select count(*) from troveinfo where length(data)=0')
        count = cu.fetchall()[0][0]
        self.assertTrue(count == 0)

        # now store all of the trove info in a database (with nothing
        # unknown) and make sure it can be read using old objects
        db = sqldb.Database(':memory:')
        cu = db.db.cursor()

        tiTable = db.troveInfoTable
        tiTable.addInfo(cu, MockTrove(full), 1)
        returned = trove.TroveInfo()
        tiTable.getInfo(cu, MockTrove(returned), 1)

        assert(full.freeze() == returned.freeze())
