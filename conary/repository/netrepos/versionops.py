#
# Copyright (c) 2004-2005 rPath, Inc.
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

from conary import versions
from conary.dbstore import idtable
from conary.repository.errors import DuplicateBranch
from conary.repository.netrepos import schema

class BranchTable(idtable.IdTable):
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

    def initTable(self, cu):
        cu.execute("INSERT INTO Branches (branchId, branch) VALUES (0, NULL)")

    def __init__(self, db):
        idtable.IdTable.__init__(self, db, "Branches", "branchId", "branch")

class LabelTable(idtable.IdTable):

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

    def __init__(self, db):
        idtable.IdTable.__init__(self, db, 'Labels', 'labelId', 'label')

    def initTable(self, cu):
        cu.execute("INSERT INTO Labels (labelId, label) VALUES (0, 'ALL')")

class LatestTable:
    def __init__(self, db):
        self.db = db
        schema.createLatest(db)

    def __setitem__(self, key, versionId):
	(itemId, branchId, flavorId) = key

        cu = self.db.cursor()

        cu.execute("""
        DELETE FROM Latest
        WHERE itemId = ?
        AND   branchId = ?
        AND   flavorId = ?
        """, (itemId, branchId, flavorId))
        cu.execute("INSERT INTO Latest (itemId, branchId, flavorId, versionId) "
                   "VALUES (?, ?, ?, ?)",
                   (itemId, branchId, flavorId, versionId))

    def get(self, key, defValue):
	(first, second, third) = key

        cu = self.db.cursor()

        cu.execute("SELECT versionId FROM Latest WHERE itemId=? AND branchId=? "
                   "AND flavorId=?", (first, second, third))
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

class LabelMap(idtable.IdPairSet):
    def __init__(self, db):
        if "LabelMap" not in db.tables:
            schema.createLabelMap(db)
	idtable.IdPairMapping.__init__(self, db, 'LabelMap', 'itemId', 'labelId', 'branchId')

    def branchesByItem(self, itemId):
	return self.getByFirst(itemId)

class Nodes:
    def __init__(self, db):
	self.db = db
        schema.createNodes(db)

    def addRow(self, itemId, branchId, versionId, timeStamps):
        cu = self.db.cursor()
	cu.execute("""
        INSERT INTO Nodes
        (itemId, branchId, versionId, timeStamps, finalTimeStamp)
        VALUES (?, ?, ?, ?, ?)""",
		   itemId, branchId, versionId,
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
        cu.execute("SELECT itemId FROM Nodes "
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

    def createVersion(self, itemId, version, flavorId, updateLatest = True):
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

	if updateLatest:
	    latestId = self.latest.get((itemId, branchId, flavorId), None)
	    if latestId == None:
		# this must be the first thing on the branch
		self.latest[(itemId, branchId, flavorId)] = versionId
	    else:
		currVer = self.versionTable.getId(latestId, itemId)
		if not currVer.isAfter(version):
		    self.latest[(itemId, branchId, flavorId)] = versionId

	nodeId = self.nodes.addRow(itemId, branchId, versionId,
				   version.timeStamps())

	return (nodeId, versionId)

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

