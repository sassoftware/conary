#
# Copyright (c) 2004 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from conary.dbstore import idtable

class Items(idtable.IdTable):
    # we create the table as a "traditional" IdTable and then personalize it
    def __init__(self, db):
        idtable.IdTable.__init__(self, db, 'Items', 'itemId', 'item')
    def initTable(self, cu):
        cu.execute(" ALTER TABLE Items ADD COLUMN "
                   " hasTrove INTEGER NOT NULL DEFAULT 0")

    def setTroveFlag(self, itemId, val):
        cu = self.db.cursor()
        if val: val = 1
        else:   val = 0
	cu.execute("UPDATE Items SET hasTrove=? WHERE itemId=?", (val, itemId))

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
