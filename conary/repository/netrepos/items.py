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


import re

from conary.dbstore import idtable

_cacheRe = {}
def checkTrove(pattern, trove):
    global _cacheRe
    if pattern == 'ALL' or trove is None:
        return True
    regExp = _cacheRe.get(pattern, None)
    if regExp is None:
        regExp = _cacheRe[pattern] = re.compile(pattern + '$')
    if regExp.match(trove):
        return True
    return False

class Items(idtable.IdTable):
    def __init__(self, db):
        idtable.IdTable.__init__(self, db, 'Items', 'itemId', 'item')

    def setTroveFlag(self, itemId, val):
        cu = self.db.cursor()
        if val: val = 1
        else:   val = 0
        # we attempt to avoid doing busywork here in order to reduce
        # lock contention on the items table during multiple commits
        cu.execute("UPDATE Items SET hasTrove = ? "
                   "WHERE itemId = ? AND hasTrove != ?",
                   (val, itemId, val))

    def iterkeys(self):
        cu = self.db.cursor()
        cu.execute("SELECT item FROM Items ORDER BY item")
        for row in cu:
            yield row[0]

    def removeUnused(self):
        cu = self.db.cursor()
        cu.execute("""
            DELETE FROM Items WHERE Items.itemId IN
                (SELECT items.itemId FROM items
                 LEFT OUTER JOIN instances ON items.itemId = instances.itemId
                 WHERE instances.itemId is NULL)
        """)
