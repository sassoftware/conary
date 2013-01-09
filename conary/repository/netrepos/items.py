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

    def updateCheckTrove(self, itemId, item):
        cu = self.db.cursor()
        # having a CheckTroveCace entry for (item, ALL) is a marker
        # we've already processed this
        cu.execute("select 1 from CheckTroveCache "
                   "where itemId = ? and patternId = 0", itemId)
        if len(cu.fetchall()) > 0:
            return
        # need to process a new itemId
        cu.execute("""
        select distinct i.item, i.itemId from Permissions as p
        join Items as i on p.itemId = i.itemId
        where not exists (
            select 1 from CheckTroveCache as ctc
            where i.itemId = ctc.patternId and ctc.itemId = ? ) """, itemId)
        pattSet = set([(x[0],x[1]) for x in cu.fetchall()])
        # add the marker - this should not exist since we checked it earlier
        pattSet.add(("ALL", 0))
        for (pattern, patternId) in pattSet:
            if checkTrove(pattern, item):
                cu.execute("""
                insert into CheckTroveCache(itemId, patternId)
                values (?,?) """, (itemId, patternId))

    def delId(self, theId):
        cu = self.db.cursor()
        cu.execute("delete from CheckTroveCache where itemId = ?", theId)
        return idtable.IdTable.delId(self, theId)

    # XXX: __setitem__ and __delitem__ aren't currently used, but if
    # we do, they'll have to handle the CheckTrovesCache as well
    def addPattern(self, pattern):
        cu = self.db.cursor()
        itemId = self.get(pattern, None)
        if itemId is None:
            itemId = idtable.IdTable.addId(self, pattern)
        else:
            # check if we're already tracking the permissions for this pattern
            cu.execute("select count(*) from CheckTroveCache where patternId = ?",
                       itemId)
            pCount = cu.fetchall()[0][0]
            if pCount > 0:
                return itemId
            # need to update CheckTroveCache for this pattern
        cu.execute("select Troves.itemId, Troves.item from Items as Troves")
        for (tid, t) in cu.fetchall():
            if checkTrove(pattern, t):
                cu.execute("insert into CheckTroveCache(itemId, patternId) "
                           "values (?,?)", (tid, itemId))
        return itemId
