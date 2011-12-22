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


from conary_test import dbstoretest

from conary import changelog
from conary.repository.netrepos import cltable
from conary.server import schema

from testrunner.runner import SkipTestException


class ChangeLogtest(dbstoretest.DBStoreTestBase):

    def testChangeLogTable(self):
        # this only works under mysql because we violate foreign key
        # constraints nodeId (and if we fixed that, on versionId and itemId)
        db = self.getDB()

        if db.driver != "sqlite":
            raise SkipTestException, "this test requires sqlite"

        schema.createChangeLog(db)
        db.commit()

        clt = cltable.ChangeLogTable(db)
        first = clt.add(1, changelog.ChangeLog("name", "contact", "message\n"))
        second = clt.add(2, changelog.ChangeLog("name2", "contact2", 
                                                "message2\n"))
        assert(first != second)
