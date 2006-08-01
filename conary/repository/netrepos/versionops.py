#
# Copyright (c) 2004-2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from conary import trove, versions
from conary.dbstore import idtable
from conary.repository.errors import DuplicateBranch
from conary.repository.netrepos import items

LATEST_TYPE_ANY     = 0         # redirects, removed, and normal
LATEST_TYPE_PRESENT = 1         # redirects and normal
LATEST_TYPE_NORMAL  = 2         # only normal troves

class BranchTable(idtable.IdTable):
    def __init__(self, db):
        idtable.IdTable.__init__(self, db, "Branches", "branchId", "branch")

    def addId(self, branch):
        assert(isinstance(branch, versions.Branch))
        cu = self.db.cursor()
        cu.execute("INSERT INTO Branches (branch) VALUES (?)",
		   branch.asString())
	return cu.lastrowid

    def getId(self, theId):
	return versions.VersionFromString(idtable.IdTable.getId(self, theId))

    def __getitem__(self, branch):
        assert(isinstance(branch, versions.Branch))
	return idtable.IdTable.__getitem__(self, branch.asString())

    def get(self, branch, defValue):
        assert(isinstance(branch, versions.Branch))
	return idtable.IdTable.get(self, branch.asString(), defValue)

    def __delitem__(self, branch):
        assert(isinstance(branch, versions.Branch))
	idtable.IdTable.__delitem__(self, branch.asString())

    def has_key(self, branch):
        assert(isinstance(branch, versions.Branch))
	return idtable.IdTable.has_key(self, branch.asString())

    def iterkeys(self):
	raise NotImplementedError

    def iteritems(self):
	raise NotImplementedError

class LabelTable(idtable.IdTable):
    def __init__(self, db):
        idtable.IdTable.__init__(self, db, 'Labels', 'labelId', 'label')

    def addId(self, label):
	idtable.IdTable.addId(self, label.asString())

    def __getitem__(self, label):
	return idtable.IdTable.__getitem__(self, label.asString())

    def get(self, label, defValue):
	return idtable.IdTable.get(self, label.asString(), defValue)

    def __delitem__(self, label):
	idtable.IdTable.__delitem__(self, label.asString())

    def has_key(self, label):
	return idtable.IdTable.has_key(self, label.asString())

    def iterkeys(self):
	raise NotImplementedError

    def iteritems(self):
	raise NotImplementedError

class LatestTable:
    def __init__(self, db):
        self.db = db

    def _findLatest(self, cu, itemId, branchId, flavorId, troveTypeFilter = ""):
        cu.execute("""
            SELECT versionId, troveType FROM Nodes
                JOIN Instances USING (itemId, versionId)
                WHERE
                    Nodes.itemId = ? AND
                    Nodes.branchId = ? AND
                    flavorId = ? AND
                    isPresent = 1
                    %s
                ORDER BY finalTimestamp DESC
                LIMIT 1
        """ % troveTypeFilter, itemId, branchId, flavorId)

        try:
            latestVersionId, troveType = cu.next()
        except StopIteration:
            latestVersionId = None
            troveType = None

        return latestVersionId, troveType

    def _add(self, cu, itemId, branchId, flavorId, versionId, latestType):
        if versionId is None:
            import epdb
            epdb.st()
        cu.execute("""INSERT INTO Latest 
                        (itemId, branchId, flavorId, versionId, latestType)
                        VALUES (?, ?, ?, ?, ?)""",
                    itemId, branchId, flavorId, versionId, latestType)

    def update(self, itemId, branchId, flavorId):
        cu = self.db.cursor()
        cu.execute("DELETE FROM Latest WHERE itemId=? AND branchId=? AND "
                   "flavorId=?", itemId, branchId, flavorId)

        versionId, troveType = self._findLatest(cu, itemId, branchId, flavorId)

        if versionId is None:
            return

        self._add(cu, itemId, branchId, flavorId, versionId, LATEST_TYPE_ANY)

        if troveType == trove.TROVE_TYPE_NORMAL:
            self._add(cu, itemId, branchId, flavorId, versionId,
                      LATEST_TYPE_PRESENT)
            self._add(cu, itemId, branchId, flavorId, versionId,
                      LATEST_TYPE_NORMAL)
            return


        presentVersionId, troveType = \
            self._findLatest(cu, itemId, branchId, flavorId,
                            "AND troveType != %d" % trove.TROVE_TYPE_REMOVED)
        if presentVersionId is not None:
            self._add(cu, itemId, branchId, flavorId, presentVersionId,
                      LATEST_TYPE_PRESENT)

        normalVersionId, troveType = self._findLatest(cu, itemId, branchId,
                     flavorId, "AND troveType = %d" % trove.TROVE_TYPE_NORMAL)
        if normalVersionId is not None and normalVersionId == presentVersionId:
            self._add(cu, itemId, branchId, flavorId, normalVersionId,
                      LATEST_TYPE_NORMAL)

class LabelMap(idtable.IdPairSet):
    def __init__(self, db):
	idtable.IdPairMapping.__init__(self, db, 'LabelMap', 'itemId', 'labelId', 'branchId')

    def branchesByItem(self, itemId):
	return self.getByFirst(itemId)

class Nodes:
    def __init__(self, db):
	self.db = db

    def addRow(self, itemId, branchId, versionId, sourceItemId, timeStamps):
        cu = self.db.cursor()
	cu.execute("""
        INSERT INTO Nodes
        (itemId, branchId, versionId, sourceItemId, timeStamps, finalTimeStamp)
        VALUES (?, ?, ?, ?, ?, ?)""",
		   itemId, branchId, versionId, sourceItemId,
		   ":".join(["%.3f" % x for x in timeStamps]),
		   '%.3f' %timeStamps[-1])
	return cu.lastrowid

    def hasItemId(self, itemId):
        cu = self.db.cursor()
        cu.execute("SELECT itemId FROM Nodes WHERE itemId=?",
		   itemId)
	return not(cu.fetchone() == None)

    def hasRow(self, itemId, versionId):
        cu = self.db.cursor()
        cu.execute("SELECT itemId FROM Nodes "
			"WHERE itemId=? AND versionId=?", itemId, versionId)
	return not(cu.fetchone() == None)

    def getRow(self, itemId, versionId, default):
        cu = self.db.cursor()
        cu.execute("SELECT nodeId FROM Nodes "
			"WHERE itemId=? AND versionId=?", itemId, versionId)
	nodeId = cu.fetchone()
	if nodeId is None:
	    return default

	return nodeId[0]

class SqlVersioning:

    def versionsOnBranch(self, itemId, branchId):
	cu = self.db.cursor()
	cu.execute("""
	    SELECT versionId FROM Nodes WHERE
		itemId=? AND branchId=? ORDER BY finalTimeStamp DESC
	""", itemId, branchId)

	for (versionId,) in cu:
	    yield versionId

    def branchesOfLabel(self, itemId, label):
	labelId = self.labels[label]
	return self.labelMap[(itemId, labelId)]

    def versionsOfItem(self, itemId):
	for branchId in self.labelMap.branchesByItem(itemId):
	    for versionId in self.versionsOnBranch(itemId, branchId):
		yield versionId

    def branchesOfItem(self, itemId):
	return self.labelMap.branchesByItem(itemId)

    def hasVersion(self, itemId, versionId):
	return self.nodes.hasItemId(itemId)

    def createVersion(self, itemId, version, flavorId, sourceName,
                      updateLatest = True):
	"""
	Creates a new versionId for itemId. The branch must already exist
	for the given itemId.
	"""
	# make sure the branch exists; we need the branchId in case we
	# need to make this the latest version on the branch
	branch = version.branch()
	branchId = self.branchTable.get(branch, None)
	if not branchId:
	    # should we implicitly create these? it's certainly easier...
	    #raise MissingBranchError(itemId, branch)
	    branchId = self.createBranch(itemId, branch)
	else:
	    # make sure the branch exists for this itemId
	    labelId = self.labels[branch.label()]

            existingBranchId = None
            for existingBranchId in self.labelMap.get((itemId, labelId), []):
                if existingBranchId == branchId: break

            if existingBranchId != branchId:
		self.createBranch(itemId, branch)

	versionId = self.versionTable.get(version, None)
	if versionId == None:
	    self.versionTable.addId(version)
	    versionId = self.versionTable.get(version, None)

	if self.nodes.hasRow(itemId, versionId):
	    raise DuplicateVersionError(itemId, version)

        sourceItemId = None
        if sourceName:
            sourceItemId = self.items.getOrAddId(sourceName)

	nodeId = self.nodes.addRow(itemId, branchId, versionId, sourceItemId,
				   version.timeStamps())

	return (nodeId, versionId)

    def updateLatest(self, itemId, branchId, flavorId):
        self.latest.update(itemId, branchId, flavorId)

    def createBranch(self, itemId, branch):
	"""
	Creates a new branch for the given node.
	"""
	label = branch.label()
	branchId = self.branchTable.get(branch, None)
	if not branchId:
	    branchId = self.branchTable.addId(branch)
	    if not self.labels.has_key(label):
		self.labels.addId(label)

	labelId = self.labels[label]

	if self.labelMap.has_key((itemId, labelId)) and \
           branchId in self.labelMap[(itemId, labelId)]:
            raise DuplicateBranch
	self.labelMap.addItem((itemId, labelId), branchId)

	return branchId

    def __init__(self, db, versionTable, branchTable):
        self.items = items.Items(db)
	self.labels = LabelTable(db)
	self.latest = LatestTable(db)
	self.labelMap = LabelMap(db)
	self.versionTable = versionTable
        self.branchTable = branchTable
	self.needsCleanup = False
	self.nodes = Nodes(db)
	self.db = db

class SqlVersionsError(Exception):
    pass

class MissingBranchError(SqlVersionsError):
    def __str__(self):
	return "node %d does not contain branch %s" % (
            self.itemId, self.branch.asString())
    def __init__(self, itemId, branch):
	SqlVersionsError.__init__(self)
	self.branch = branch
	self.itemId = itemId

class DuplicateVersionError(SqlVersionsError):
    def __str__(self):
	return "node %d already contains version %s" % (
            self.itemId, self.version.asString())
    def __init__(self, itemId, version):
	SqlVersionsError.__init__(self)
	self.version = version
	self.itemId = itemId

