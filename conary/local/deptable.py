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

from conary import sqlite3
from conary import versions
from conary.deps import deps

NO_FLAG_MAGIC = '-*none*-'

def createDepTable(cu, name, isTemp):
    if isTemp:
        tmp = "TEMPORARY"
    else:
        tmp = ""

    cu.execute("""CREATE %s TABLE %s(depId integer primary key,
                                  class integer,
                                  name str,
                                  flag str
                                 )""" % (tmp, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE UNIQUE INDEX %sIdx ON %s(class, name, flag)" % 
               (name, name), start_transaction = (not tmp))

def createRequiresTable(cu, name, isTemp):
    if isTemp:
        tmp = "TEMPORARY"
    else:
        tmp = ""

    cu.execute("""CREATE %s TABLE %s(instanceId integer,
                                  depId integer,
                                  depNum integer,
                                  depCount integer
                                 )""" % (tmp, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx ON %s(instanceId)" % (name, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx2 ON %s(depId)" % (name, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx3 ON %s(depNum)" % (name, name),
               start_transaction = (not isTemp))

def createProvidesTable(cu, name, isTemp):
    if isTemp:
        tmp = "TEMPORARY"
    else:
        tmp = ""

    cu.execute("""CREATE %s TABLE %s(instanceId integer,
                                  depId integer
                                 )""" % (tmp, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx ON %s(instanceId)" % (name, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx2 ON %s(depId)" % (name, name),
               start_transaction = (not isTemp))

class DepTable:
    def __init__(self, db, name):
        cu = db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'Dependencies' not in tables:
            createDepTable(cu, name, False)

class DepRequires:
    def __init__(self, db, name):
        cu = db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if name not in tables:
            createRequiresTable(cu, name, False)

class DepProvides:
    def __init__(self, db, name):
        cu = db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if name not in tables:
            createProvidesTable(cu, name, False)

class DependencyTables:

    def _createTmpTable(self, cu, name, makeTable = True, makeIndex = True):
	if makeTable:
	    cu.execute("""CREATE TEMPORARY TABLE %s(
						  troveId INT,
						  depNum INT,
						  flagCount INT,
						  isProvides BOOL,
						  class INTEGER,
						  name STRING,
						  flag STRING)""" % name,
		       start_transaction = False)
	if makeIndex:
	    cu.execute("CREATE INDEX %sIdx ON %s(troveId, class, name, flag)"
			    % (name, name), start_transaction = False)

    def _populateTmpTable(self, cu, stmt, depList, troveNum, requires, 
                          provides, multiplier = 1):
        allDeps = []
        if requires:
            allDeps += [ (False, x) for x in 
                            requires.getDepClasses().iteritems() ]
        if provides:
            allDeps += [ (True,  x) for x in 
                            provides.getDepClasses().iteritems() ]

        for (isProvides, (classId, depClass)) in allDeps:
            for dep in depClass.getDeps():
                for (depName, flags) in zip(dep.getName(), dep.getFlags()):
                    cu.execstmt(stmt,
                                   troveNum, multiplier * len(depList), 
                                    1 + len(flags), isProvides, classId, 
                                    depName, NO_FLAG_MAGIC)
                    if flags:
                        for (flag, sense) in flags:
                            # conary 0.12.0 had mangled flags; this check
                            # prevents them from making it into any repository
                            assert("'" not in flag)
                            assert(sense == deps.FLAG_SENSE_REQUIRED)
                            cu.execstmt(stmt,
                                        troveNum, multiplier * len(depList), 
                                        1 + len(flags), isProvides, classId, 
                                        depName, flag)

                if not isProvides:
                    depList.append((troveNum, classId, dep))

    def _mergeTmpTable(self, cu, tmpName, depTable, reqTable, provTable,
                       dependencyTables, multiplier = 1):
        substDict = { 'tmpName'   : tmpName,
                      'depTable'  : depTable,
                      'reqTable'  : reqTable,
                      'provTable' : provTable }

        cu.execute("""INSERT INTO %(depTable)s 
                        SELECT DISTINCT
                            NULL,
                            %(tmpName)s.class,
                            %(tmpName)s.name,
                            %(tmpName)s.flag
                        FROM %(tmpName)s LEFT OUTER JOIN Dependencies ON
                            %(tmpName)s.class == Dependencies.class AND
                            %(tmpName)s.name == Dependencies.name AND
                            %(tmpName)s.flag == Dependencies.flag
                        WHERE
                            Dependencies.depId is NULL
                    """ % substDict, start_transaction = False)

        if multiplier != 1:
            cu.execute("UPDATE %s SET depId=depId * %d"  
                           % (depTable, multiplier), start_transaction = False)

        cu.execute("SELECT MAX(depNum) FROM %(reqTable)s" % substDict)
        base = cu.next()[0]
        if base is None:
            base = 0
        substDict['baseReqNum'] = base + 1

        if len(dependencyTables) == 1:
            substDict['depId'] = "%s.depId" % dependencyTables
        else:
            substDict['depId'] = "COALESCE(%s)" % \
                ",".join(["%s.depId" % x for x in dependencyTables])

        selectClause = """\
""" % substDict
        selectClause = ""
        for depTable in dependencyTables:
            d = { 'tmpName' : substDict['tmpName'],
                  'depTable' : depTable }
            selectClause += """\
                        LEFT OUTER JOIN %(depTable)s ON
                            %(tmpName)s.class == %(depTable)s.class AND
                            %(tmpName)s.name == %(depTable)s.name AND
                            %(tmpName)s.flag == %(depTable)s.flag
""" % d

        repQuery = """\
                INSERT INTO %(reqTable)s
                    SELECT %(tmpName)s.troveId, 
                           %(depId)s,
                           %(baseReqNum)d + %(tmpName)s.depNum, 
                           %(tmpName)s.flagCount 
                        FROM %(tmpName)s 
""" % substDict
        repQuery += selectClause
        repQuery += """\
                        WHERE
                            %(tmpName)s.isProvides == 0""" % substDict
        cu.execute(repQuery, start_transaction = False)

        if provTable is None:   
            return

        repQuery = """\
                INSERT INTO %(provTable)s
                    SELECT %(tmpName)s.troveId, 
                           %(depId)s
                        FROM %(tmpName)s 
""" % substDict
        repQuery += selectClause
        repQuery += """\
                        WHERE
                            %(tmpName)s.isProvides == 1""" % substDict
        cu.execute(repQuery, start_transaction = False)

    def get(self, cu, trv, troveId):
        for (tblName, setFn) in (('Requires', trv.setRequires),
                                 ('Provides', trv.setProvides)):
            cu.execute("SELECT class, name, flag FROM %s NATURAL JOIN "
                       "Dependencies WHERE instanceId=? ORDER BY class, name"
                    % tblName, troveId)

            last = None
            flags = []
            depSet = deps.DependencySet()
            for (classId, name, flag) in cu:
                if (classId, name) == last:
                    if flag != NO_FLAG_MAGIC:
                        flags.append((flag, deps.FLAG_SENSE_REQUIRED))
                else:
                    if last:
                        depSet.addDep(deps.dependencyClasses[last[0]],
                                      deps.Dependency(last[1], flags))
                        
                    last = (classId, name)
                    flags = []
                    if flag != NO_FLAG_MAGIC:
                        flags.append((flag, deps.FLAG_SENSE_REQUIRED))
                    
            if last:
                depSet.addDep(deps.dependencyClasses[last[0]],
                              deps.Dependency(last[1], flags))
                setFn(depSet)

    def add(self, cu, trove, troveId):
        assert(cu.con.inTransaction)
        self._add(cu, troveId, trove.getProvides(), trove.getRequires())

    def _add(self, cu, troveId, provides, requires):
        self._createTmpTable(cu, "NeededDeps")

	stmt = cu.compile("INSERT INTO NeededDeps VALUES(?, ?, ?, ?, ?, ?, ?)")
        self._populateTmpTable(cu, stmt, [], troveId, 
                               requires, provides)
        self._mergeTmpTable(cu, "NeededDeps", "Dependencies", "Requires", 
                            "Provides", ("Dependencies",))

        cu.execute("DROP TABLE NeededDeps", start_transaction = False)

    def delete(self, cu, troveId):
        cu.execute("CREATE TEMPORARY TABLE suspectDepsOrig(depId integer)")
        for tbl in ('Requires', 'Provides'):
            cu.execute("INSERT INTO suspectDepsOrig SELECT depId "
                       "FROM %s WHERE instanceId=%d" % (tbl, troveId))
            cu.execute("DELETE FROM %s WHERE instanceId=%d" % (tbl, troveId))

        cu.execute("CREATE TEMPORARY TABLE suspectDeps(depId integer)")
        cu.execute("INSERT INTO suspectDeps SELECT DISTINCT depId "
                   "FROM suspectDepsOrig")
        cu.execute("DROP TABLE suspectDepsOrig")

        cu.execute("""DELETE FROM Dependencies WHERE depId IN 
                (SELECT DISTINCT suspectDeps.depId FROM suspectDeps 
                 LEFT OUTER JOIN 
                    (SELECT depId AS depId1,
                            instanceId AS instanceId1 FROM Requires UNION 
                     SELECT depId AS depId1,
                            instanceId AS instanceId1 FROM Provides)
                    ON suspectDeps.depId = depId1
                 WHERE instanceId1 IS NULL)""")

        cu.execute("DROP TABLE suspectDeps")

    def _restrictResolveByLabel(self, label):
        """ Restrict resolution by label
            We move this out so that other dependency algorithms 
            can restrict resolution by other criteria.  Not exactly providing
            a clean external interface but it avoids having to rewrite 
            dependency code to use different criterea
        """
        if not label:
            return "", ""
            
            
        restrictJoin = """\
                           INNER JOIN Instances ON
                              %(provides)s.instanceId = Instances.instanceId
                           INNER JOIN Nodes ON
                              Instances.itemId = Nodes.itemId AND
                              Instances.versionId = Nodes.versionId
                           INNER JOIN LabelMap ON
                              LabelMap.itemId = Nodes.itemId AND
                              LabelMap.branchId = Nodes.branchId
                           INNER JOIN Labels ON
                              Labels.labelId = LabelMap.labelId
""" 
        restrictWhere = """\
                            WHERE 
                              Labels.label = '%s'
""" % label

        return restrictJoin, restrictWhere
        
        
    def _resolveStmt(self, requiresTable, providesTableList, depTableList,
                     restrictBy = None, restrictor=None):
        subselect = ""

        depTableClause = ""
        for depTable in depTableList:
            substTable = { 'requires' : "%-15s" % requiresTable,
                           'deptable' : "%-15s" % depTable }

            depTableClause += """\
                         LEFT OUTER JOIN %(deptable)s ON
                              %(requires)s.depId = %(deptable)s.depId\n""" % substTable

        for provTable in providesTableList:
            substTable = { 'provides' : "%-15s" % provTable,
                           'requires' : "%-15s" % requiresTable,
                           'depClause': depTableClause }

            for name in ( 'class', 'name', 'flag' ):
                if len(depTableList) > 1:
                    s = "COALESCE(%s)" % ", ".join([ "%s.%s" % (x, name) 
                                                    for x in depTableList])
                else:
                    s = "%s.%s" % (depTableList[0], name)

                substTable[name] = s

            if subselect:
                subselect += """\
                     UNION ALL\n"""

            subselect += """\
                       SELECT %(requires)s.depId      AS reqDepId,
                              %(requires)s.instanceId AS reqInstId,
                              %(provides)s.depId      AS provDepId,
                              %(provides)s.instanceId AS provInstId,
                              %(class)s AS class,
                              %(name)s AS name,
                              %(flag)s AS flag
                         FROM %(requires)s INNER JOIN %(provides)s ON
                              %(requires)s.depId = %(provides)s.depId
""" % substTable

            if restrictor:
                joinRestrict, whereRestrict = restrictor(restrictBy)
                subselect += joinRestrict % substTable
       

            subselect += """\
%(depClause)s""" % substTable
            
            if restrictor:
                subselect += whereRestrict % substTable
                

        return """
                SELECT Matched.reqDepId as depId,
                       depCheck.depNum as depNum,
                       Matched.reqInstId as reqInstanceId,
                       Matched.provInstId as provInstanceId
                    FROM (
%s                       ) AS Matched
                    INNER JOIN DepCheck ON
                        Matched.reqInstId == DepCheck.troveId AND
                        Matched.class == DepCheck.class AND
                        Matched.name == DepCheck.name AND
                        Matched.flag == DepCheck.flag
                    WHERE
                        NOT DepCheck.isProvides
                    GROUP BY
                        DepCheck.depNum,
                        Matched.provInstId
                    HAVING
                        COUNT(DepCheck.troveId) == DepCheck.flagCount
                """ % subselect

    def check(self, jobSet, troveSource, findOrdering = False):
	"""
	Check the database for closure against the operations in
	the passed changeSet.

	@param changeSet: The changeSet which defined the operations
	@type changeSet: repository.ChangeSet
	@rtype: tuple of dependency failures for new packages and
		dependency failures caused by removal of existing
		packages
	"""
        def _depItemsToSet(idxList, depInfoList, provInfo = True,
                           wasIn = None):
            failedSets = [ (x, None, None, None) for x in troveNames]
            stillNeededMap = [ [] for x in troveNames ]

            for idx in idxList:
                (troveIndex, classId, dep) = depInfoList[-idx]

                if classId in [ deps.DEP_CLASS_ABI ]:
                    continue

                troveIndex = -(troveIndex + 1)

                if failedSets[troveIndex][2] is None:
                    failedSets[troveIndex] = (failedSets[troveIndex][0],
                                              failedSets[troveIndex][1],
                                              deps.DependencySet(),
                                              []
                                              )
                failedSets[troveIndex][2].addDep(
                                deps.dependencyClasses[classId], dep)

                if wasIn is not None:
                    failedSets[troveIndex][3].extend(wasIn[idx])

            failedList = []
            for (name, classId, depSet, neededByList) in failedSets:
                if depSet is not None:
                    if not wasIn:
                        failedList.append((name, depSet))
                    else:
                        failedList.append((name, depSet, neededByList))

            return failedList

        def _brokenItemsToSet(cu, depIdSet, wasIn):
            # this only works for databases (not repositories)
            if not depIdSet: return []

            cu.execute("CREATE TEMPORARY TABLE BrokenDeps (depNum INTEGER)",
                       start_transaction = False)
            for depNum in depIdSet:
                cu.execute("INSERT INTO BrokenDeps VALUES (?)", depNum,
                           start_transaction = False)

            cu.execute("""
                    SELECT DISTINCT troveName, version, flavor, class, 
                                    name, flag, BrokenDeps.depNum FROM 
                        BrokenDeps INNER JOIN Requires ON 
                            BrokenDeps.depNum = Requires.DepNum
                        JOIN Dependencies ON
                            Requires.depId = Dependencies.depId
                        JOIN Instances ON
                            Requires.instanceId = Instances.instanceId
                        JOIN Versions ON
                            Instances.versionId = Versions.versionId
                        JOIN Flavors ON
                            Instances.flavorId = Flavors.flavorId
                """, start_transaction = False)

            failedSets = {}
            for (troveName, troveVersion, troveFlavor, depClass, depName, 
                            flag, depNum) in cu:
                info = (troveName, versions.VersionFromString(troveVersion),
                        deps.ThawDependencySet(troveFlavor))

                if info not in failedSets:
                    failedSets[info] = (deps.DependencySet(), [])

                if flag == NO_FLAG_MAGIC:
                    flags = []
                else:
                    flags = [ (flag, deps.FLAG_SENSE_REQUIRED) ]

                failedSets[info][0].addDep(
                        deps.dependencyClasses[depClass], 
                        deps.Dependency(depName, flags))
                failedSets[info][1].extend(wasIn[depNum])

            cu.execute("DROP TABLE BrokenDeps", start_transaction = False)

            return [ (x[0], x[1][0], x[1][1]) 
                                for x in failedSets.iteritems() ]

        def _expandProvidedBy(cu, itemList):
            for info, depSet, provideList in itemList:
                for instanceId in provideList:
                    assert(instanceId > 0)
                cu.execute("""
                        SELECT DISTINCT troveName, version, flavor FROM 
                            Instances JOIN Versions ON
                                Instances.versionId = Versions.versionId
                            JOIN Flavors ON
                                Instances.flavorId = Flavors.flavorId
                            WHERE
                                instanceId IN (%s)""" %
                        ",".join(["%d" % x for x in provideList]))

                del provideList[:]
                for name, version, flavor in cu:
                    if flavor is None:
                        flavor = ""
                    provideList.append((name, 
                                        versions.VersionFromString(version),
                                        deps.ThawDependencySet(flavor)))

        def _createEdges(result, depList):
            oldNewEdges = set()
            oldOldEdges = set()
            newNewEdges = set()
            newOldEdges = set()

            for (depId, depNum, reqInstId, reqNodeIdx, 
                 provInstId, provNodeIdx) in result:
                if depNum < 0:
                    fromNodeId = -depList[-depNum][0]
                    assert(fromNodeId > 0)

                    if provNodeIdx is not None:
                        # new trove depends on something old
                        toNodeId = provNodeIdx
                        if fromNodeId == toNodeId:
                            continue
                        newOldEdges.add((fromNodeId, toNodeId, depId))
                    elif provInstId > 0:
                        # new trove depends on something already installed
                        # which is not being removed. not interesting.
                        pass
                    else:
                        # new trove depends on something new
                        toNodeId = -provInstId
                        if fromNodeId == toNodeId:
                            continue
                        newNewEdges.add((fromNodeId, toNodeId, depId))
                else: # dependency was provided by something before this 
                      # update occurred
                    if reqNodeIdx is not None:
                        fromNodeId = reqNodeIdx
                        # requirement is old
                        if provNodeIdx is not None:
                            # provider is old
                            toNodeId = provNodeIdx
                            if fromNodeId == toNodeId:
                                continue
                            oldOldEdges.add((fromNodeId, toNodeId, depId))
                        else:
                            # provider is new
                            toNodeId = -provInstId
                            if fromNodeId == toNodeId:
                                continue
                            oldNewEdges.add((fromNodeId, toNodeId, depId))
                    else:
                        # trove with the requirement is not being removed.
                        if provNodeIdx is None: 
                            # the trove that provides this requirement is being
                            # installed.  We probably don't care.
                            continue
                        else:
                            # the trove that provides this requirement is being
                            # removed.  We probably care -- if this dep is
                            # being provided by some other package, we need
                            # to connect these two packages
                            # XXX fix this
                            continue

            return oldNewEdges, oldOldEdges, newNewEdges, newOldEdges

        def _collapseEdges(oldOldEdges, oldNewEdges, newOldEdges, newNewEdges):
            # these edges cancel each other out -- for example, if Foo
            # requires both the old and new versions of Bar the order between
            # Foo and Bar is irrelevant
            oldOldEdges.difference_update(oldNewEdges)
            newNewEdges.difference_update(newOldEdges)

        def _buildGraph(nodes, oldOldEdges, newNewEdges):
            for (reqNodeId, provNodeId, depId) in oldOldEdges:
                # remove the provider after removing the requirer
                nodes[provNodeId][1].add(reqNodeId)
                nodes[reqNodeId][2].add(provNodeId)

            for (reqNodeId, provNodeId, depId) in newNewEdges:
                nodes[reqNodeId][1].add(provNodeId)
                nodes[provNodeId][2].add(reqNodeId)

        def _treeDFS(nodes, nodeIdx, seen, finishes, timeCount):
            seen[nodeIdx] = True
            
            for nodeId in nodes[nodeIdx][1]:
                if not seen[nodeId]:
                    timeCount = _treeDFS(nodes, nodeId, seen, finishes,
                                         timeCount)

            finishes[nodeIdx] = timeCount
            timeCount += 1
            return timeCount

        def _connectDFS(nodes, compList, nodeIdx, seen, finishes):
            seen[nodeIdx] = True
            edges = [ (finishes[x], x) for x in nodes[nodeIdx][2] ]
            edges.sort()
            edges.reverse()

            compList.append(nodeIdx)
            
            for finishTime, nodeId in edges:
                if not seen[nodeId]:
                    _connectDFS(nodes, compList, nodeId, seen, finishes)

        def _stronglyConnect(nodes):
            # Converts the graph to a graph of strongly connected components.
            # We return a list of lists, where each sublist represents a
            # single components. All of the edges for that component are
            # in the nodes list, and are from or two the first node in the
            # sublist for that component

            # Now for a nice, simple strongly connected componenet algorithm.
            # If you don't understand this, try _Introductions_To_Algorithms_
            # by Cormen, Leiserson and Rivest. If you google for "strongly
            # connected components" (as of 4/2005) you'll find lots of snippets
            # from it
            finishes = [ -1 ] * len(nodes)
            seen = [ False ] * len(nodes)
            nextStart = 1
            timeCount = 0
            while nextStart != len(nodes):
                if not seen[nextStart]:
                    timeCount = _treeDFS(nodes, nextStart, seen, finishes, 
                                         timeCount)
                
                nextStart += 1

            nodeOrders = [ (f, i) for i, f in enumerate(finishes) ]
            nodeOrders.sort()
            nodeOrders.reverse()
            # get rid of the placekeeper "None" node
            del nodeOrders[-1]

            nextStart = 0
            seen = [ False ] * len(nodes)
            allSets = []
            while nextStart != len(nodeOrders):
                nodeId = nodeOrders[nextStart][1]
                if not seen[nodeId]:
                    compSet = []
                    _connectDFS(nodes, compSet, nodeId, seen, finishes)
                    allSets.append(compSet)

                nextStart += 1

            # map node indexes to nodes in the component graph
            componentMap = {}
            for i, nodeSet in enumerate(allSets):
                componentMap.update(dict.fromkeys(nodeSet, i))
                
            componentGraph = []
            for i, nodeSet in enumerate(allSets):
                edges = {}
                componentNodes = []
                for nodeId in nodeSet:
                    componentNodes.append(nodes[nodeId][0])
                    edges.update(dict.fromkeys(
                            [ componentMap[targetId] 
                                        for targetId in nodes[nodeId][1] ]
                                ))
                componentGraph.append((componentNodes, edges))

            return componentGraph

        def _orderDFS(compGraph, nodeIdx, seen, order):
            seen[nodeIdx] = True
            for otherNode in compGraph[nodeIdx][1]:
                if not seen[otherNode]:
                    _orderDFS(compGraph, otherNode, seen, order)

            order.append(nodeIdx)

        def _orderComponents(compGraph):
	    # Returns a topological sort of compGraph. It's biased to
	    # putting info-*: first in the list.
            order = []
            seen = [ False ] * len(compGraph)
            nextIndex = 0

            while (nextIndex < len(compGraph)):
                if not seen[nextIndex]:
		    # if any item in this component is an info- trove, go
		    # ahead and process this component now
		    for component in compGraph[nextIndex][0]:
                        name = component[0]
			if name.startswith("info-"):
			    _orderDFS(compGraph, nextIndex, seen, order)
			    break

		nextIndex += 1

            nextIndex = 0
            while (nextIndex < len(compGraph)):
                if not seen[nextIndex]:
                    _orderDFS(compGraph, nextIndex, seen, order)

                nextIndex += 1

            return [ compGraph[x][0] for x in order ]

        # this works against a database, not a repository
        cu = self.db.cursor()

	# this begins a transaction. we do this explicitly to keep from
	# grabbing any exclusive locks (when the python binding autostarts
	# a transaction, it uses "begin immediate" to grab an exclusive
	# lock right away. since we're only updating tmp tables, we don't
	# need a lock at all, but we'll live with a reserved lock since that's
	# the best we can do with sqlite and still get the performance benefits
	# of being in a transaction)
	cu.execute("BEGIN")

        self._createTmpTable(cu, "DepCheck", makeIndex = False)
        createDepTable(cu, 'TmpDependencies', isTemp = True)
        createProvidesTable(cu, 'TmpProvides', isTemp = True)
        createRequiresTable(cu, 'TmpRequires', isTemp = True)
    
        # build the table of all the requirements we're looking for
        depList = [ None ]
        oldTroves = []
        troveNames = []

	stmt = cu.compile("""INSERT INTO DepCheck 
                                    (troveId, depNum, flagCount, isProvides,
                                     class, name, flag)
                             VALUES(?, ?, ?, ?, ?, ?, ?)""")

        # We build up a graph to let us split the changeset into pieces.
        # Each node in the graph represents a remove/add pair. Note that
        # for (troveNum < 0) nodes[abs(troveNum)] is the node for that
        # addition. The initial None makes that work out. For removed nodes,
        # the index is built into the sql tables. Each node stores the
        # old trove info, new trode info, list of nodes whose operations
        # need to occur before this nodes, and a list of nodes whose
        # operations should occur after this nodes (the two lists form
        # the ordering graph and it's transpose)
        nodes = [ None ]

        troveInfo = {}

	# This sets up negative depNum entries for the requirements we're
	# checking (multiplier = -1 makes them negative), with (-1 * depNum) 
	# indexing depList. depList is a list of (troveNum, depClass, dep) 
	# tuples. Like for depNum, negative troveNum values mean the
	# dependency was part of a new trove.
        i = 0
        for job in jobSet:
            # removal jobs are handled elsewhere
            if job[2][0] is None: continue

            newInfo = (job[0], job[2][0], job[2][1])

            trv = troveSource.getTrove(withFiles = False, *newInfo)
            
            troveNum = -i - 1
            troveNames.append(newInfo)
            self._populateTmpTable(cu, stmt, 
                                   depList = depList, 
                                   troveNum = troveNum,
                                   requires = trv.getRequires(), 
                                   provides = trv.getProvides(),
                                   multiplier = -1)

            troveInfo[newInfo] = i + 1

            if job[1][0] is not None:
		oldInfo = (job[0], job[1][0], job[1][1])
                oldTroves.append((oldInfo, len(nodes)))
	    else:
		oldInfo = None

            nodes.append((job, set(), set()))

            i += 1

        # create the index for DepCheck
        self._createTmpTable(cu, "DepCheck", makeTable = False)

        # merge everything into TmpDependencies, TmpRequires, and tmpProvides
        self._mergeTmpTable(cu, "DepCheck", "TmpDependencies", "TmpRequires",
                            "TmpProvides", 
                            ("Dependencies", "TmpDependencies"), 
                            multiplier = -1)

        # now build a table of all the troves which are being erased
        cu.execute("""CREATE TEMPORARY TABLE RemovedTroveIds 
                        (troveId INTEGER, nodeId INTEGER)""")
	cu.execute("""CREATE INDEX RemovedTroveIdsIdx ON RemovedTroveIds(troveId)""")

        for job in jobSet:
            if job[2][0] is not None: continue
            oldInfo = (job[0], job[1][0], job[1][1])
            oldTroves.append((oldInfo, len(nodes)))
            nodes.append((job, set(), set()))

        if oldTroves:
            # this sets up nodesByRemovedId because the temporary RemovedTroves
            # table exactly parallels the RemovedTroveIds we set up
            cu.execute("""CREATE TEMPORARY TABLE RemovedTroves 
                            (name STRING, version STRING, flavor STRING,
                             nodeId INTEGER)""",
                       start_transaction = False)
            for (name, version, flavor), nodeIdx in oldTroves:
                if flavor:
                    flavor = flavor.freeze()
                else:
                    flavor = None

                cu.execute("INSERT INTO RemovedTroves VALUES(?, ?, ?, ?)",
                           (name, version.asString(), flavor, nodeIdx))

            cu.execute("""INSERT INTO RemovedTroveIds 
                            SELECT instanceId, nodeId FROM 
                                RemovedTroves 
                            INNER JOIN Versions ON
                                RemovedTroves.version = Versions.version
                            INNER JOIN Flavors ON
                                RemovedTroves.flavor = Flavors.flavor OR
                                (RemovedTroves.flavor is NULL AND
                                 Flavors.flavor is NULL)
                            INNER JOIN Instances ON
                                Instances.troveName = RemovedTroves.name AND
                                Instances.versionId = Versions.versionId AND
                                Instances.flavorId  = Flavors.flavorId""")

            # no need to remove RemovedTroves -- this is all in a transaction
            # which gets rolled back

        # Check the dependencies for anything which depends on things which
        # we've removed. We insert those dependencies into our temporary
	# tables (which define everything which needs to be checked) with
	# a positive depNum which mathes the depNum from the Requires table.
        cu.execute("""
                INSERT INTO TmpRequires SELECT 
                    DISTINCT Requires.instanceId, Requires.depId, 
                             Requires.depNum, Requires.depCount
                FROM 
                    RemovedTroveIds 
                INNER JOIN Provides ON
                    RemovedTroveIds.troveId == Provides.instanceId
                INNER JOIN Requires ON
                    Provides.depId = Requires.depId
        """)

        cu.execute("""
                INSERT INTO DepCheck SELECT
                    Requires.instanceId, Requires.depNum,
                    Requires.DepCount, 0, Dependencies.class,
                    Dependencies.name, Dependencies.flag
                FROM 
		    RemovedTroveIds 
		INNER JOIN Provides ON
                    RemovedTroveIds.troveId == Provides.instanceId
                INNER JOIN Requires ON
                    Provides.depId = Requires.depId
                INNER JOIN Dependencies ON
                    Dependencies.depId == Requires.depId
        """)

        # dependencies which could have been resolved by something in
        # RemovedIds, but instead weren't resolved at all are considered
        # "unresolvable" dependencies. (they could be resolved by something
        # in the repository, but that something is being explicitly removed
        # and adding it back would be a bit rude!)
        cu.execute("""
                SELECT depId, depNum, reqInstanceId, Required.nodeId,
                       provInstanceId, Provided.nodeId
		    FROM
			(%s) 
                    LEFT OUTER JOIN RemovedTroveIds AS Required ON
                        reqInstanceId == Required.troveId
                    LEFT OUTER JOIN RemovedTroveIds AS Provided ON
                        provInstanceId == Provided.troveId
                """ % self._resolveStmt("TmpRequires",
                                        ("Provides", "TmpProvides"),
                                        ("Dependencies", "TmpDependencies")))
	result = [ x for x in cu ]
	# XXX there's no real need to instantiate this; we're just doing
	# it for convienence while this code gets reworked

        changeSetList = []

        # None in depList means the dependency got resolved; we track
        # would have been resolved by something which has been removed as
        # well

        # depNum is the dependency number
        #    negative ones are for dependencies being added (and they index
        #    depList); positive ones are for dependencies broken by an
        #    erase (and need to be looked up in the Requires table in the 
        #    database to get a nice description)
        # removedInstanceId != None means that the dependency was resolved by 
        #    something which is being removed. If it is None, the dependency
        #    was resolved by something which isn't disappearing. It could
        #    occur multiple times for the same dependency with both None
        #    and !None, in which case the None wins (as long as one item
        #    resolves it, it's resolved)
        unresolveable = set()

        # these track the nodes which satisfy each depId. brokenByErase
        # tracks what used to provide something but is being removed, while
        # satisfied tracks what now provides it
        brokenByErase = {}
        satisfied = { 0 : 0 }
        wasIn = {}

        for (depId, depNum, reqInstanceId, 
             reqNodeIdx, provInstId, provNodeIdx) in result:
            if provNodeIdx is not None:
                if reqNodeIdx is not None:
                    # this is an old dependency and an old provide.  
                    # ignore it
                    continue
                if depNum < 0:
                    # the dependency would have been resolved, but this
                    # change set removes what would have resolved it
                    unresolveable.add(depNum)
                    wasIn.setdefault(depNum, []).append(provInstId)
                else:
                    # this change set removes something which is needed
                    # by something else on the system (it might provide
		    # a replacement; we handle that later)
                    brokenByErase[depNum] = provNodeIdx
                    wasIn.setdefault(depNum, []).append(provInstId)
            else:
                # if we get here, the dependency is resolved; mark it as
                # resolved by clearing it's entry in depList
                if depNum < 0:
                    satisfied[depNum] = provInstId
                else:
                    # if depNum > 0, this was a dependency which was checked
                    # because of something which is being removed, but it
                    # remains satisfied
                    satisfied[depNum] = provInstId

        if findOrdering:
            # there are four kinds of edges -- old needs old, old needs new,
            # new needs new, and new needs old. Each edge carries a depId
            # to aid in cancelling them out. Our initial edge representation
            # is a simple set of edges.
            oldNewEdges, oldOldEdges, newNewEdges, newOldEdges = \
                        _createEdges(result, depList)

            # Create dependencies from collections to the things they include.
            # This forces collections to be installed after all of their
            # elements
            i = 0
            for job in jobSet:
                if job[2][0] is None: continue
                trv = troveSource.getTrove(job[0], job[2][0], job[2][1], 
                                           withFiles = False)

                for name, version, flavor in trv.iterTroveList():
                    targetTrove = troveInfo.get((name, version, flavor), -1)
                    if targetTrove >= 0:
                        newNewEdges.add((i + 1, targetTrove, None))

                i += 1

            resatisfied = set(brokenByErase) & set(satisfied)
            if resatisfied:
                # These dependencies are ones where the same dependency
                # is being both removed and added, and which is required
                # by something already installed on the system. To ensure
                # dependency closure, these two operations must happen
                # simultaneously. Create a loop between the nodes.
                for depId in resatisfied:
                    oldNodeId = brokenByErase[depId]
                    newNodeId = -satisfied[depId]
                    if oldNodeId != newNodeId and newNodeId > 0:
                        # if newNodeId < 0, the dependency remains satisfied
                        # by something on the system and we don't need
                        # to do anything special. Creating the loop
                        # this way is a bit abusive of the edge types since
                        # they aren't really descriptive in this case
                        oldOldEdges.add((oldNodeId, newNodeId, depId))
                        newNewEdges.add((oldNodeId, newNodeId, depId))

            # Remove nodes which cancel each other
            _collapseEdges(oldOldEdges, oldNewEdges, newOldEdges, newNewEdges)

            # the edges left in oldNewEdges represent dependencies which troves
            # slated for removal have on troves being installed. either those
            # dependencies will already be guaranteed by edges in oldOldEdges,
            # or they were broken to begin with. either way, we don't have to
            # care about them
            del oldNewEdges
            # newOldEdges are dependencies which troves being installed have on
            # troves being removed. since those dependencies will be broken
            # after this operation, we don't need to order on them (it's likely
            # they are filled by some other trove being added, and the edge
            # in newNewEdges will make that work out
            del newOldEdges

            # Now build up a unified node list. The different kinds of edges
            # and the particular depId no longer matter. The direction here is
            # a bit different, and defines the ordering for the operation, not
            # the order of the dependency
            _buildGraph(nodes, oldOldEdges, newNewEdges)
            del oldOldEdges
            del newNewEdges

            componentGraph = _stronglyConnect(nodes)
            del nodes
            ordering = _orderComponents(componentGraph)
            for component in ordering:
                oneList = []
                for job in component:
                    oneList.append(job)

                changeSetList.append(oneList)

        del troveInfo

        satisfied = set(satisfied)
        brokenByErase = set(brokenByErase)

        # things which are listed in satisfied should be removed from
        # brokenByErase; they are dependencies that were broken, but are
        # resolved by something else
        brokenByErase.difference_update(satisfied)

        # sort things out of unresolveable which were resolved by something
        # else. 
        unresolveable.difference_update(satisfied)

        # build a list of all of the depnums which need to be satisfied
        # (which is -1 * each index into depList), and subtract out the
        # dependencies which were satistied. what's left are the depNum's
        # (negative) of the dependencies which failed
        unsatisfied = set([ -1 * x for x in range(len(depList)) ]) - satisfied
        # don't report things as both unsatisfied and unresolveable
        unsatisfied = unsatisfied - unresolveable

        unsatisfiedList = _depItemsToSet(unsatisfied, depList)
        unresolveableList = _depItemsToSet(unresolveable, depList,
                                           wasIn = wasIn )
        unresolveableList += _brokenItemsToSet(cu, brokenByErase, wasIn)

        _expandProvidedBy(cu, unresolveableList)

        # no need to drop our temporary tables since we're rolling this whole
        # transaction back anyway
	self.db.rollback()

        return (unsatisfiedList, unresolveableList, changeSetList)

    def _resolve(self, depSetList, selectTemplate, restrictor=None, 
                 restrictBy=None):

        cu = self.db.cursor()

	cu.execute("BEGIN")

        self._createTmpTable(cu, "DepCheck")
        createDepTable(cu, 'TmpDependencies', isTemp = True)
        createRequiresTable(cu, 'TmpRequires', isTemp = True)

        depList = [ None ]
	stmt = cu.compile("INSERT INTO DepCheck VALUES(?, ?, ?, ?, ?, ?, ?)")
        for i, depSet in enumerate(depSetList):
            self._populateTmpTable(cu, stmt, depList, -i - 1, 
                                   depSet, None, multiplier = -1)


        self._mergeTmpTable(cu, "DepCheck", "TmpDependencies", "TmpRequires",
                            None, ("Dependencies", "TmpDependencies"), 
                            multiplier = -1)

        full = selectTemplate % self._resolveStmt( "TmpRequires", 
                                ("Provides",), ("Dependencies",),
                                restrictBy = restrictBy, restrictor = restrictor)
                    
        cu.execute(full,start_transaction = False)

        return depList, cu

    def _addResult(self, depId, value, depList, depSetList, result):
        depSetId = -depList[depId][0] - 1
        depSet = depSetList[depSetId]
        result.setdefault(depSet, []).append(value)


    def resolve(self, label, depSetList):
        """ Determine troves that provide the given dependencies, 
            restricting by label and limiting to latest version for 
            each (name, flavor) pair.
        """
        selectTemplate = """SELECT depNum, Items.item, Versions.version, 
                             Nodes.timeStamps, flavor FROM 
                            (%s)
                          INNER JOIN Instances ON
                            provInstanceId == Instances.instanceId
                          INNER JOIN Items ON
                            Instances.itemId == Items.itemId
                          INNER JOIN Versions ON
                            Instances.versionId == Versions.versionId
                          INNER JOIN Flavors ON
                            Instances.flavorId == Flavors.flavorId
                          INNER JOIN Nodes ON
                            Instances.itemId == Nodes.itemId AND
                            Instances.versionId == Nodes.versionId
                          ORDER BY
                            Nodes.finalTimestamp DESC
                        """ 
        depList, cu = self._resolve(depSetList, selectTemplate,
                                    restrictBy = label.asString(), 
                                    restrictor = self._restrictResolveByLabel)

        depSolutions = [ {} for x in xrange(len(depList)) ]

        for (depId, troveName, versionStr, timeStamps, flavorStr) in cu:
            depId = -depId

            # remember the first version for each troveName/flavorStr pair
            depSolutions[depId].setdefault((troveName, flavorStr),
                                           (versionStr, timeStamps))

        result = {}
        for depId, troveSet in enumerate(depSolutions):
            if not troveSet:
                continue

            troveSet = [ (x[0][0], 
                          versions.strToFrozen(x[1][0], x[1][1].split(":")),
                          x[0][1]) for x in troveSet.items() ]
            self._addResult(depId, troveSet, depList, depSetList, result)
        self.db.rollback()
        return result

    def resolveToIds(self, depSetList):
        """ Resolve dependencies, leaving the results as instanceIds
        """
        selectTemplate = """SELECT depNum, provInstanceId FROM (%s)"""
        depList, cu = self._resolve(depSetList, selectTemplate)

        result = {}
        depSolutions = {}
        for depId, troveId in cu:
            depId = -depId
            depSolutions.setdefault(depId, []).append(troveId)

        for depId, sols in depSolutions.iteritems():
            if not sols:
                continue
            self._addResult(depId, sols, depList, depSetList, result)

        self.db.rollback()
        return result

    def getLocalProvides(self, depSetList):
        cu = self.db.cursor()

	cu.execute("BEGIN")

        self._createTmpTable(cu, "DepCheck")
        createDepTable(cu, 'TmpDependencies', isTemp = True)
        createRequiresTable(cu, 'TmpRequires', isTemp = True)

        depList = [ None ]
	stmt = cu.compile("INSERT INTO DepCheck VALUES(?, ?, ?, ?, ?, ?, ?)")
        for i, depSet in enumerate(depSetList):
            self._populateTmpTable(cu, stmt, depList, -i - 1, 
                                   depSet, None, multiplier = -1)


        self._mergeTmpTable(cu, "DepCheck", "TmpDependencies", "TmpRequires",
                            None, ("Dependencies", "TmpDependencies"), 
                            multiplier = -1)

        full = """SELECT depNum, troveName, Versions.version, 
                         timeStamps, Flavors.flavor FROM 
                        (%s)
                      INNER JOIN Instances ON
                        provInstanceId == Instances.instanceId
                      INNER JOIN Versions USING(versionId)
                      INNER JOIN Flavors 
                            ON (Instances.flavorId == Flavors.flavorId)
                    """ % self._resolveStmt( "TmpRequires", 
                                ("Provides",), ("Dependencies",))

        cu.execute(full,start_transaction = False)

        depSolutions = [ [] for x in xrange(len(depList)) ]

        for (depId, troveName, versionStr, timeStamps, flavorStr) in cu:
            depId = -depId
            # remember the first version for each troveName/flavorStr pair
            ts = [ float(x) for x in timeStamps.split(":") ]
            v = versions.VersionFromString(versionStr, timeStamps=ts)
            f = deps.ThawDependencySet(flavorStr)
            depSolutions[depId].append((troveName, v, f))

        result = {}

        for depId, troveSet in enumerate(depSolutions):
            if not troveSet: continue
            depNum = depList[-depId][0]
            depSet = depSetList[depNum]
            result[depSet] = troveSet
        self.db.rollback()
        return result

    def __init__(self, db):
        self.db = db
        DepTable(db, "Dependencies")
        DepProvides(db, 'Provides')
        DepRequires(db, 'Requires')

class DependencyDatabase(DependencyTables):
    """ Creates a thin database (either on disk or in memory) 
        for managing dependencies
    """
    def __init__(self, path=":memory:"):
	db = sqlite3.connect(path, timeout=30000)
        DependencyTables.__init__(self, db)

    def add(self, troveId, provides, requires):
        cu = self.db.cursor()
        self._add(cu, troveId, provides, requires)

    def delete(self):
        cu = self.db.cursor()
        DependencyDatabase.delete(self, cu, troveId)

    def commit(self):
        self.db.commit()

    def resolve(self, label, depSetList):
        return self.resolveToIds(list(depSetList))
