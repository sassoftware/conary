#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import idtable
import versions

class VersionTable:
    """
    Maps a version to an id and timestamp pair.
    """
    noVersion = 0

    def __init__(self, db):
        self.db = db
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'Versions' not in tables:
            cu.execute("CREATE TABLE Versions(versionId INTEGER PRIMARY KEY,"
		       "version str UNIQUE)")
	    cu.execute("INSERT INTO Versions VALUES (%d, NULL)", 
			self.noVersion)

    def addId(self, version):
        cu = self.db.cursor()
        cu.execute("INSERT INTO Versions VALUES (NULL, %s)",
		   version.asString())
	return cu.lastrowid

    def delId(self, theId):
        assert(type(theId) is int)
        cu = self.db.cursor()
        cu.execute("DELETE FROM Versions WHERE versionId=%d", theId)

    def _makeVersion(self, str, timeStamps):
	v = versions.VersionFromString(str)
	v.setTimeStamps([ float(x) for x in timeStamps.split(":")])
	return v

    def getBareId(self, theId):
	"""
	Gets a version object w/o setting any timestamps.
	"""
        cu = self.db.cursor()
        cu.execute("""SELECT version FROM Versions
		      WHERE Versions.versionId=%d""", theId)
	try:
	    (s, ) = cu.next()
	    return versions.VersionFromString(s)
	except StopIteration:
            raise KeyError, theId

    def has_key(self, version):
        cu = self.db.cursor()
        cu.execute("SELECT versionId FROM Versions WHERE version=%s",
                   version.asString())
	return not(cu.fetchone() == None)

    def __delitem__(self, version):
        cu = self.db.cursor()
        cu.execute("DELETE FROM Versions WHERE version=%s", version.asString())

    def __getitem__(self, version):
	v = self.get(version, None)
	if v == None:
            raise KeyError, version

	return v

    def get(self, version, defValue):
        cu = self.db.cursor()
        cu.execute("SELECT versionId FROM Versions WHERE version=%s", 
		   version.asString())

	item = cu.fetchone()
	if item:
	    return item[0]
	else:
	    return defValue

    def removeUnused(self):
	# removes versions which don't have parents and aren't used
	# by any FileStreams
        cu = self.db.cursor()
	cu.execute("""
	    DELETE FROM Versions WHERE versionId IN 
		(SELECT versionId from Versions LEFT OUTER JOIN 
		    (SELECT versionId AS fooId from Parent UNION 
		     SELECT versionId AS fooId FROM FileStreams) 
		ON Versions.versionId = fooId WHERE fooId is NULL);
	    """)

class BranchTable(idtable.IdTable):
    def addId(self, branch, parentId):
        assert(branch.isBranch())
        cu = self.db.cursor()
        cu.execute("INSERT INTO Branches VALUES (NULL, %s, %d)", 
		   branch.asString(), parentId)
	return cu.lastrowid

    def getId(self, theId):
	return versions.VersionFromString(idtable.IdTable.getId(self, theId))

    def __getitem__(self, branch):
        assert(branch.isBranch())
	return idtable.IdTable.__getitem__(self, branch.asString())

    def get(self, branch, defValue):
        assert(branch.isBranch())        
	return idtable.IdTable.get(self, branch.asString(), defValue)

    def __delitem__(self, branch):
        assert(branch.isBranch())        
	idtable.IdTable.__delitem__(self, branch.asString())

    def has_key(self, branch):
        assert(branch.isBranch())
	return idtable.IdTable.has_key(self, branch.asString())

    def iterkeys(self):
	raise NotImplementedError

    def iteritems(self):
	raise NotImplementedError

    def removeUnused(self):
	# removes versions which don't have parents and aren't used
	# by any FileStreams
        cu = self.db.cursor()
	cu.execute("""
            DELETE FROM LabelMap WHERE LabelMap.LabelId IN
                (SELECT LabelMap.LabelId FROM LabelMap LEFT OUTER JOIN
                    Latest ON LabelMap.branchId = Latest.branchId
                    WHERE Latest.versionId IS NULL);
	    DELETE FROM Branches WHERE branchId IN 
		(SELECT branchId from Branches LEFT OUTER JOIN 
		    (SELECT branchId AS fooId from LabelMap)
		ON Branches.branchId = fooId WHERE fooId is NULL);
	    """)

    def __init__(self, db):
        self.db = db
	self.tableName = 'branches';
	self.keyName = 'branchId'
	self.strName = 'branch';
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'Branches' not in tables:
            cu.execute("CREATE TABLE Branches(branchId integer primary key,"
					     "branch str unique,"
					     "parentNode integer)")
	    self.initTable()

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

class LatestTable(idtable.IdPairMapping):
    def __init__(self, db):
	idtable.IdPairMapping.__init__(self, db, 'Latest',
				       'itemId', 'branchId', 'versionId')

class LabelMap(idtable.IdPairSet):
    def __init__(self, db):
	idtable.IdPairMapping.__init__(self, db, 'LabelMap',
		                       'itemId', 'labelId', 'branchId')
	cu = db.cursor()
        cu.execute("SELECT name FROM sqlite_master WHERE type='index'")
        tables = [ x[0] for x in cu ]
        if "LabelMapLabelIdx" not in tables:
	    cu.execute("CREATE INDEX LabelMapLabelIdx on LabelMap(labelId)")

    def branchesByItem(self, itemId):
	return self.getByFirst(itemId)

    def removeUnused(self):
        cu = self.db.cursor()
	cu.execute("""
	    DELETE FROM Labels WHERE Labels.LabelId IN 
		(SELECT Labels.labelId from Labels LEFT OUTER JOIN 
			LabelMap ON Labels.labelId = LabelMap.labelId 
		 WHERE LabelMap.labelId is NULL);
	""")

class ParentTable(idtable.IdPairMapping):
    def __init__(self, db):
	idtable.IdPairMapping.__init__(self, db, 'Parent',
		                       'itemId', 'versionId', 'parentId')
	cu = db.cursor()
        cu.execute("SELECT name FROM sqlite_master WHERE type='index'")
        tables = [ x[0] for x in cu ]
        if "ParentVersionIdx" not in tables:
	    cu.execute("CREATE INDEX ParentVersionIdx on Parent(versionId)")
	    cu.execute("INSERT INTO Parent VALUES (0,0,0)")

class Nodes:

    def __init__(self, db):
	self.db = db
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'Nodes' not in tables:
	    cu.execute("""
		CREATE TABLE Nodes(nodeId INTEGER PRIMARY KEY,
					    itemId INT,
					    branchId INT,
					    versionId INT,
					    timeStamps STR,
					    finalTimeStamp FLOAT);
		CREATE INDEX NodesIdx ON 
			Nodes(itemId, branchId);
		CREATE INDEX NodesIdx2 ON 
			Nodes(itemId, versionId);
	    """)

    def addRow(self, itemId, branchId, versionId, timeStamps):
        cu = self.db.cursor()
	cu.execute("INSERT INTO Nodes VALUES (NULL, %d, %d, %d, %s, %.3f)",
		   itemId, branchId, versionId, 
		   ":".join(["%.3f" % x for x in timeStamps]),
		   timeStamps[-1],)
		    
    def hasItemId(self, itemId):
        cu = self.db.cursor()
        cu.execute("SELECT itemId FROM Nodes WHERE itemId=%d",
		   itemId)
	return not(cu.fetchone() == None)

    def hasRow(self, itemId, versionId):
        cu = self.db.cursor()
        cu.execute("SELECT itemId FROM Nodes "
			"WHERE itemId=%d AND versionId=%d", itemId, versionId)
	return not(cu.fetchone() == None)

class SqlVersioning:

    def versionsOnBranch(self, itemId, branchId):
	cu = self.db.cursor()
	cu.execute("""
	    SELECT versionId FROM Nodes WHERE
		itemId=%d AND branchId=%d ORDER BY finalTimeStamp DESC
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

    def latestOnBranch(self, itemId, branchId):
	versionId = self.latest.get((itemId, branchId), None)
        if versionId is None:
            branch = self.branchTable.getId(branchId)
            return self.versionTable[branch.parentNode()]
        return versionId

    def hasVersion(self, itemId, versionId):
	return self.nodes.hasItemId(itemId)

    def createVersion(self, itemId, version):
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
	    if not self.labelMap.has_key((itemId, labelId)):
		self.createBranch(itemId, branch)

	versionId = self.versionTable.get(version, None)
	if versionId == None:
	    self.versionTable.addId(version)
	    versionId = self.versionTable.get(version, None)

	if self.nodes.hasRow(itemId, versionId):
	    raise DuplicateVersionError(itemId, version)

	latestId = self.latest.get((itemId, branchId), None)
	if latestId == None:
	    # this must be the first thing on the branch
	    self.latest[(itemId, branchId)] = versionId
	else:
	    currVer = self.versionTable.getId(latestId, itemId)
	    if not currVer.isAfter(version):
		del self.latest[(itemId, branchId)]
		self.latest[(itemId, branchId)] = versionId

	self.nodes.addRow(itemId, branchId, versionId, version.timeStamps())

	return versionId

    def eraseVersion(self, itemId, versionId):
	# should we make them pass in the version as well to save the
	# lookup?
	assert(0)
	version = self.versionTable.getId(versionId, itemId)
	branch = version.branch()
	branchId = self.branchTable[branch]
	latestId = self.latest[(itemId, branchId)]
	if versionId == latestId:
	    parentId = self.parents.get((itemId, versionId), None)
	    del self.latest[(itemId, branchId)]
	    del self.parents[(itemId, versionId)]
	    if parentId != self.versionTable.noVersion:
		self.latest[(itemId, branchId)] = parentId

	else:
	    currId = latestId
	    while versionId != currId:
		childId = currId
		currId = self.parents[(itemId, currId)]

	    parentId = self.parents.get((itemId, versionId), None)

	    del self.parents[(itemId, childId)]
	    del self.parents[(itemId, versionId)]
	    self.parents[(itemId, childId)] = parentId

	self.needsCleanup = True
	
    def createBranch(self, itemId, branch, topVersionId = None,
		     topVersionTimestamps = None):
	"""
	Creates a new branch for the given node. If topVersionId is
	not None, that node is considered the only node on the branch.
	"""
	assert(not branch.hasParent() or 
	       min(branch.parentNode().timeStamps()) > 0)
	label = branch.label()
	branchId = self.branchTable.get(branch, None)
	if not branchId:
	    if branch.hasParent():
		parent = branch.parentNode()
		parentId = self.versionTable.get(parent, None)
		if parentId is None:
		    parentId = self.versionTable.addId(parent)
	    else:
		parentId = 0

	    branchId = self.branchTable.addId(branch, parentId)
	    if not self.labels.has_key(label):
		self.labels.addId(label)

	labelId = self.labels[label]

	assert(not self.labelMap.has_key((itemId, labelId)) or
	       branchId not in self.labelMap[(itemId, labelId)])
	self.labelMap.addItem((itemId, labelId), branchId)

	if topVersionId is not None:
	    self.latest[(itemId, branchId)] = topVersionId
	    self.nodes.addRow(itemId, branchId, topVersionId,
				       topVersionTimestamps)

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
	return "node %d does not contain branch %s" % (self.itemId, 
						       self.branch.asString())

    def __init__(self, itemId, branch):
	SqlVersionsError.__init__(self)
	self.branch = branch
	self.itemId = itemId

class DuplicateVersionError(SqlVersionsError):

    def __str__(self):
	return "node %d already contains version %s" % (self.itemId, 
						        self.version.asString())

    def __init__(self, itemId, version):
	SqlVersionsError.__init__(self)
	self.version = version
	self.itemId = itemId

