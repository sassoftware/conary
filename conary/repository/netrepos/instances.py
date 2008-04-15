#
# Copyright (c) 2004-2008 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

INSTANCE_PRESENT_MISSING = 0
INSTANCE_PRESENT_NORMAL  = 1
INSTANCE_PRESENT_HIDDEN  = 2

from conary import versions
from conary.deps import deps

class InstanceTable:
    """
    Generic table for assigning id's to a 3-tuple of IDs.
    """
    def __init__(self, db):
        self.db = db

    def addId(self, itemId, versionId, flavorId, clonedFromId,
              troveType, isPresent = INSTANCE_PRESENT_NORMAL):
        cu = self.db.cursor()
        cu.execute("INSERT INTO Instances "
                   "(itemId, versionId, flavorId, clonedFromId, troveType, isPresent) "
                   "VALUES (?, ?, ?, ?, ?, ?)",
                   (itemId, versionId, flavorId, clonedFromId, troveType, isPresent))
	return cu.lastrowid

    def getId(self, theId):
        cu = self.db.cursor()
        cu.execute(" SELECT itemId, versionId, flavorId, isPresent "
		   " FROM Instances WHERE instanceId=? ", theId)
	try:
	    return cu.next()
	except StopIteration:
            raise KeyError, theId

    def isPresent(self, item):
        cu = self.db.cursor()
        cu.execute(" SELECT isPresent FROM Instances WHERE "
                   " itemId=? AND versionId=? AND flavorId=?", item)
	val = cu.fetchone()
	if not val:
	    return 0
	return val[0]

    def setPresent(self, theId, val):
        cu = self.db.cursor()
	cu.execute("UPDATE Instances SET isPresent=? WHERE instanceId=?",
                   (val, theId))

    def update(self, theId, isPresent = None, clonedFromId = None):
        sets = []
        args = []
        if isPresent is not None:
            sets.append("isPresent=?")
            args.append(isPresent)
        if clonedFromId is not None:
            sets.append("clonedFromId=?")
            args.append(clonedFromId)
        if len(args):
            cu = self.db.cursor()
            args.append(theId)
            cu.execute("UPDATE Instances SET %s WHERE instanceId=?" % (", ".join(sets),),
                       args)
        return theId
    
    def has_key(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"itemId=? AND versionId=? AND flavorId=?", item)
	return not(cu.fetchone() == None)

    def __getitem__(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"itemId=? AND versionId=? AND flavorId=?", item)
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, item

    def get(self, item, defValue):
        cu = self.db.cursor()
        cu.execute("SELECT instanceId FROM Instances WHERE "
			"itemId=? AND versionId=? AND flavorId=?", item)
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

    def getInstanceId(self, troveName, troveVersion, troveFlavor):
        """ return the instanceId for a n,v,f string tuple """
        cu = self.db.cursor()

        vStr = troveVersion
        if isinstance(troveVersion, versions.Version):
            vStr = troveVersion.asString()
        fStr = troveFlavor 
        if isinstance(troveFlavor, deps.Flavor):
            fStr = troveFlavor.freeze()
        # get the instanceId we're looking for
        cu.execute("""
        select instanceId from Instances
        join Items on Instances.itemId = Items.itemId
        join Versions on Instances.versionId = Versions.versionId
        join Flavors on Instances.flavorId = Flavors.flavorId
        where Items.item = ?
          and Versions.version = ?
          and Flavors.flavor = ? """, (troveName, vStr, fStr))
        try:
            return cu.next()[0]
        except StopIteration:
            raise KeyError, (troveName, troveVersion, troveFlavor)
        # not reached
        assert(0)
