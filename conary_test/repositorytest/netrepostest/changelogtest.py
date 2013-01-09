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
