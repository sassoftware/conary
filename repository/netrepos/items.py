#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import idtable

class Items(idtable.IdTable):
    def __init__(self, db):
        idtable.IdTable.__init__(self, db, 'item')

    def removeUnused(self):
	cu = self.db.cursor()
	cu.execute("""
	    DELETE FROM Items WHERE Items.itemId IN 
		(SELECT items.itemId FROM items
		 LEFT OUTER JOIN instances ON items.itemId = instances.itemId 
		 WHERE instances.itemId is NULL);
	""")
