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
