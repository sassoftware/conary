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

from conary import dbstore, trove, versions
from conary.deps import deps
from conary.lib import graph
from conary.local import schema

import itertools

DEP_REASON_ORDER = 0
DEP_REASON_OLD_NEEDS_OLD = 1
DEP_REASON_NEW_NEEDS_NEW = 2
DEP_REASON_LINKED = 3
DEP_REASON_FORCED_LAST = 4
DEP_REASON_COLLECTION = 5

NO_FLAG_MAGIC = '-*none*-'

class DependencyWorkTables:

    def _mergeTmpTable(self, tmpName, depTable, reqTable, provTable,
                       dependencyTables, multiplier = 1):
        substDict = { 'tmpName'   : tmpName,
                      'depTable'  : depTable,
                      'reqTable'  : reqTable,
                      'provTable' : provTable }

        self.cu.execute("""
        INSERT INTO %(depTable)s
            (class, name, flag)
        SELECT DISTINCT
            %(tmpName)s.class, %(tmpName)s.name, %(tmpName)s.flag
        FROM %(tmpName)s
        LEFT OUTER JOIN Dependencies USING (class, name, flag)
        WHERE Dependencies.depId is NULL
        """ % substDict, start_transaction = False)

        if multiplier != 1:
            self.cu.execute("UPDATE %s SET depId=depId * %d"
                           % (depTable, multiplier), start_transaction = False)

        self.cu.execute("SELECT MAX(depNum) FROM %(reqTable)s" % substDict)
        base = self.cu.next()[0]
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
                            %(tmpName)s.class = %(depTable)s.class AND
                            %(tmpName)s.name = %(depTable)s.name AND
                            %(tmpName)s.flag = %(depTable)s.flag
""" % d

        repQuery = """\
                INSERT INTO %(reqTable)s
                    (instanceId, depId, depNum, depCount)
                    SELECT %(tmpName)s.troveId,
                           %(depId)s,
                           %(baseReqNum)d + %(tmpName)s.depNum,
                           %(tmpName)s.flagCount
                        FROM %(tmpName)s
""" % substDict
        repQuery += selectClause
        repQuery += """\
                        WHERE
                            %(tmpName)s.isProvides = 0""" % substDict
        self.cu.execute(repQuery, start_transaction = False)

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
                            %(tmpName)s.isProvides = 1""" % substDict
        self.cu.execute(repQuery, start_transaction = False)

    def _populateTmpTable(self, depList, troveNum, requires,
                          provides, multiplier = 1):
        # FIXME: switch back to preparsed statments when dbstore supports it
        allDeps = []
        if requires:
            allDeps += [ (0, x) for x in
                            sorted(requires.getDepClasses().iteritems()) ]
        if provides:
            allDeps += [ (1,  x) for x in
                            sorted(provides.getDepClasses().iteritems()) ]

        populateStmt = self.cu.compile("""
            INSERT INTO DepCheck
            (troveId, depNum, flagCount, isProvides, class, name, flag)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """)

        for (isProvides, (classId, depClass)) in allDeps:
            # getDeps() returns sorted deps
            for dep in depClass.getDeps():
                for (depName, flags) in zip(dep.getName(), dep.getFlags()):
                    self.cu.execstmt(populateStmt,
                                   troveNum, multiplier * len(depList),
                                    1 + len(flags), isProvides, classId,
                                    depName, NO_FLAG_MAGIC)
                    if flags:
                        for (flag, sense) in flags:
                            # conary 0.12.0 had mangled flags; this check
                            # prevents them from making it into any repository
                            assert("'" not in flag)
                            assert(sense == deps.FLAG_SENSE_REQUIRED)
                            self.cu.execstmt(populateStmt,
                                        troveNum, multiplier * len(depList),
                                        1 + len(flags), isProvides, classId,
                                        depName, flag)

                if not isProvides:
                    depList.append((troveNum, classId, dep))

    def merge(self, intoDatabase = False, skipProvides = False):
        if intoDatabase:
            assert(not skipProvides)
            self._mergeTmpTable("DepCheck", "Dependencies", "Requires",
                                "Provides", ("Dependencies",))
        elif skipProvides:
            self._mergeTmpTable("DepCheck", "TmpDependencies", "TmpRequires",
                                None, ("Dependencies", "TmpDependencies"),
                                multiplier = -1)
        else:
            self._mergeTmpTable("DepCheck", "TmpDependencies", "TmpRequires",
                                "TmpProvides",
                                ("Dependencies", "TmpDependencies"),
                                multiplier = -1)

    def mergeRemoves(self):
        self.cu.execute("""INSERT INTO RemovedTroveIds
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

        schema.resetTable(self.cu, "RemovedTroves")

        # Check the dependencies for anything which depends on things which
        # we've removed. We insert those dependencies into our temporary
        # tables (which define everything which needs to be checked) with
        # a positive depNum which mathes the depNum from the Requires table.
        self.cu.execute("DELETE FROM TmpRequires WHERE depNum > 0")
        self.cu.execute("""
                INSERT INTO TmpRequires SELECT
                    DISTINCT Requires.instanceId, Requires.depId,
                             Requires.depNum, Requires.depCount
                FROM
                    RemovedTroveIds
                INNER JOIN Provides ON
                    RemovedTroveIds.troveId = Provides.instanceId
                INNER JOIN Requires ON
                    Provides.depId = Requires.depId
        """)

        self.cu.execute("DELETE FROM DepCheck WHERE depNum > 0")
        self.cu.execute("""
                INSERT INTO DepCheck SELECT
                    Requires.instanceId, Requires.depNum,
                    Requires.DepCount, 0, Dependencies.class,
                    Dependencies.name, Dependencies.flag
                FROM
                    RemovedTroveIds
                INNER JOIN Provides ON
                    RemovedTroveIds.troveId = Provides.instanceId
                INNER JOIN Requires ON
                    Provides.depId = Requires.depId
                INNER JOIN Dependencies ON
                    Dependencies.depId = Requires.depId
        """)

    def removeTrove(self, (name, version, flavor), nodeId):
        if flavor is None or flavor.isEmpty():
            flavor = None
        else:
            flavor = flavor.freeze()

        self.cu.execute("INSERT INTO RemovedTroves VALUES(?, ?, ?, ?)",
                        (name, version.asString(), flavor, nodeId))

    def __init__(self, cu, removeTables = False):
        self.cu = cu

        schema.resetTable(self.cu, "DepCheck")
        schema.resetTable(self.cu, "RemovedTroveIds")
        schema.resetTable(self.cu, "TmpDependencies")
        schema.resetTable(self.cu, "TmpProvides")
        schema.resetTable(self.cu, "TmpRequires")

        if removeTables:
            schema.resetTable(self.cu, "RemovedTroveIds")

class DependencyChecker:

    # We build up a graph to let us split the changeset into pieces.
    # Each node in the graph represents a remove/add pair. Note that
    # for (troveNum < 0) nodes[abs(troveNum)] is the node for that
    # addition. The initial None makes that work out. For removed nodes,
    # the index is built into the sql tables. Each node stores the
    # old trove info, new trode info, list of nodes whose operations
    # need to occur before this nodes, and a list of nodes whose
    # operations should occur after this nodes (the two lists form
    # the ordering graph and it's transpose)

    def _addJob(self, job):
        nodeId = len(self.nodes)
        self.g.addNode(nodeId)
        self.nodes.append((job, set(), set()))

        if job[2][0] is not None:
            self.newInfoToNodeId[(job[0], job[2][0], job[2][1])] = nodeId

        if job[1][0] is not None:
            self.oldInfoToNodeId[(job[0], job[1][0], job[1][1])] = nodeId

        return nodeId

    def _buildEdges(self, oldOldEdges, newNewEdges, collectionEdges,
                    linkedIds, finalIds, criticalUpdates):
        for (reqNodeId, provNodeId, depId) in oldOldEdges:
            # remove the provider after removing the requirer
            self.g.addEdge(reqNodeId, provNodeId, (DEP_REASON_OLD_NEEDS_OLD,
                                                   depId))

        for (reqNodeId, provNodeId, depId) in newNewEdges:
            self.g.addEdge(provNodeId, reqNodeId, (DEP_REASON_NEW_NEEDS_NEW,
                                                   depId))

        for nodeIdList in linkedIds:
            # create a circular link here, to make sure
            # these troves have to be in the same job:
            #  a -> b -> c -> a.
            l = len(nodeIdList)
            for i in range(l):
                self.g.addEdge(nodeIdList[i], nodeIdList[(i + 1) % l],
                               (DEP_REASON_LINKED, None))

        for finalId in finalIds:
            # these jobs are required to be last.  To force that, we simply
            # make them after all the leaves - those with no edges requiring
            # anything after them.
            for leafId in self.g.getLeaves():
                if leafId in finalIds:
                    continue
                self.g.addEdge(leafId, finalId, (DEP_REASON_FORCED_LAST, None))

        for leafId in self.g.getDisconnected():
            # if nothing depends on a node and the node 
            # depends on nothing, tie the node to its
            # parent.  This will create a cycle and ensure that
            # they get installed together.
            job = self.nodes[leafId][0]
            if trove.troveIsCollection(job[0]): continue

            # if this job is part of a critical update, its ordering is
            # important!  Don't drag in the whole trove update.
            if criticalUpdates and job in criticalUpdates: continue

            newPkgInfo = (job[0].split(':', 1)[0], job[2][0], job[2][1])

            parentId = self.newInfoToNodeId.get(newPkgInfo, 0)
            if not parentId:
                oldPkgInfo = (job[0].split(':', 1)[0], job[2][0], job[2][1])
                parentId = self.oldInfoToNodeId.get(oldPkgInfo, 0)
                if not parentId:
                    continue

            self.g.addEdge(parentId, leafId, (DEP_REASON_ORDER, None))


        for (reqNodeId, provNodeId, depId) in collectionEdges:
            self.g.addEdge(provNodeId, reqNodeId, (DEP_REASON_COLLECTION, None))

    def _collapseEdges(self, oldOldEdges, oldNewEdges, newOldEdges, 
                       newNewEdges):
        # these edges cancel each other out -- for example, if Foo
        # requires both the old and new versions of Bar the order between
        # Foo and Bar is irrelevant
        oldOldEdges.difference_update(oldNewEdges)
        newNewEdges.difference_update(newOldEdges)

    def _createCollectionEdges(self):
        edges = set()
        
        nodes = iter(self.nodes)
        nodes.next()

        for i, (job, _, _) in enumerate(nodes):
            if not trove.troveIsCollection(job[0]): continue

            if job[1][0]:
                trv = self.troveSource.db.getTrove(job[0], job[1][0], job[1][1],
                                                   withFiles = False)
                for info in trv.iterTroveList(strongRefs=True, weakRefs=True):
                    targetTrove = self.oldInfoToNodeId.get(info, -1)
                    if targetTrove >= 0:
                        edges.add((i + 1, targetTrove, None))

            if job[2][0]:
                trv = self.troveSource.getTrove(job[0], job[2][0], job[2][1],
                                                withFiles = False)

                for info in trv.iterTroveList(strongRefs=True, weakRefs=True):
                    targetTrove = self.newInfoToNodeId.get(info, -1)
                    if targetTrove >= 0:
                        edges.add((i + 1, targetTrove, None))

        return edges

    def _createDependencyEdges(self, result, depList):
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

    def _gatherDependencyErrors(self, satisfied, brokenByErase, unresolveable, 
                                wasIn):
        from conary.local import sqldb
        flavorCache = sqldb.FlavorCache()
        versionCache = sqldb.VersionCache()
        def _depItemsToSet(idxList, depInfoList, provInfo = True,
                           wasIn = None):
            failedSets = [ ((x[0], x[2][0], x[2][1]), None, None, None) 
                    for x in self.iterNodes() ]
            ignoreDepClasses = set((deps.DEP_CLASS_ABI,))

            for idx in idxList:
                (troveIndex, classId, dep) = depInfoList[-idx]

                if classId in ignoreDepClasses:
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

            schema.resetTable(cu, 'BrokenDeps')
            cu.executemany("INSERT INTO BrokenDeps VALUES (?)", depIdSet,
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
                        flavorCache.get(troveFlavor))

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
                                        flavorCache.get(flavor)))
        # def _gatherDependencyErrors starts here

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
        unsatisfied = set([ -1 * x for x in range(len(self.depList)) ]) - \
                                    satisfied
        # don't report things as both unsatisfied and unresolveable
        unsatisfied = unsatisfied - unresolveable

        unsatisfiedList = _depItemsToSet(unsatisfied, self.depList)
        unresolveableList = _depItemsToSet(unresolveable, self.depList,
                                           wasIn = wasIn )
        unresolveableList += _brokenItemsToSet(self.cu, brokenByErase, wasIn)

        _expandProvidedBy(self.cu, unresolveableList)

        return (unsatisfiedList, unresolveableList)

    def _gatherResolution(self, result):
        # these track the nodes which satisfy each depId. brokenByErase
        # tracks what used to provide something but is being removed, while
        # satisfied tracks what now provides it
        unresolveable = set()
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

        return satisfied, brokenByErase, wasIn, unresolveable

    @staticmethod
    def _resolveStmt(requiresTable, providesTableList, depTableList,
                     restrictBy = None, restrictor=None):
        subselect = ""

        depTableClause = ""
        for depTable in depTableList:
            substTable = { 'requires' : requiresTable,
                           'deptable' : depTable }

            depTableClause += """\
                 LEFT OUTER JOIN %(deptable)s ON
                      %(requires)s.depId = %(deptable)s.depId\n""" % substTable

        for provTable in providesTableList:
            substTable = { 'provides' : provTable,
                           'requires' : requiresTable,
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

        # XXX: FIXME: this GROUP BY is invalid SQL, since we're
        # selecting more fields than we're grouping for. We need to
        # use aggregate functions on the others or rewrite with an
        # extra join
        return """
                SELECT Matched.reqDepId as depId,
                       depCheck.depNum as depNum,
                       Matched.reqInstId as reqInstanceId,
                       Matched.provInstId as provInstanceId,
                       DepCheck.flagCount as flagCount
                    FROM ( %s ) AS Matched
                    INNER JOIN DepCheck ON
                        Matched.reqInstId = DepCheck.troveId AND
                        Matched.class = DepCheck.class AND
                        Matched.name = DepCheck.name AND
                        Matched.flag = DepCheck.flag
                    WHERE
                        NOT DepCheck.isProvides
                    GROUP BY
                        DepCheck.depNum,
                        Matched.provInstId
                    HAVING
                        COUNT(DepCheck.troveId) = DepCheck.flagCount
                """ % subselect

    def _getCriticalJobSets(self, jobSetList, criticalJobs):

        def _findRelatedJobs(job):
            # return jobs that must be updated before or after job
            # due to dependencies in order to have a consistent system.
            jobs = []
            if job[2][0]:
                nodeId = self.newInfoToNodeId[job[0], job[2][0], job[2][1]]
            else:
                nodeId = self.oldInfoToNodeId[job[0], job[1][0], job[1][1]]

            nodeIds= [ nodeId ]
            seen = set(nodeIds)
            while nodeIds:
                nodeId = nodeIds.pop()
                for parentNode, edgeInfo in self.g.getParents(nodeId,
                                                              withEdges=True):
                    # return all of the troves that are required to go before
                    # this job.  We ignore child nodes - nodes that are required
                    # to go after - because either a) they can safely be put
                    # off until later because the system is in a stable state
                    # after this update or b) the dep checking will make sure
                    # that the updates are done together anyway.
                    if parentNode in seen:
                        continue
                    nodeIds.append(parentNode)
                    seen.add(parentNode)
            return set(self.nodes[x][0] for x in seen)

        # create index from nodeIdx -> jobSetIdx for creating a SCC graph.
        jobSetsByJob = {}
        for jobSet in jobSetList:
            for job, nodeIdx in jobSet:
                jobSetsByJob[job] = jobSet

        criticalJobsSets = []
        if criticalJobs:
            criticalJobSet = set()
            for job in criticalJobs:
                allJobs = _findRelatedJobs(job)
                # convert jobSets to tuples so we can create a set and eliminate
                # duplicates
                criticalJobSet.update(tuple(jobSetsByJob[x]) for x in allJobs)

            # convert back to lists for equality checks elsewhere.
            criticalJobsSets.append([ list(x) for x in criticalJobSet])
        
        return criticalJobsSets

    def _orderJobSets(self, jobSets, criticalJobSetsList):
        # sort jobSets so that critical jobs are first, then 
        # info packages, then packages/groups, then sort alphabetically.
        # This ordering will determine how the jobs are ordered when there's
        # no dependency reason to order them a particular way.
        jobComp = {}
        for jobSet in jobSets:
            value = []

            isCritical = 0
            for idx, jobSetList in enumerate(reversed(criticalJobSetsList)):
                if jobSet in jobSetList:
                    isCritical = idx + 1
                    break

            hasInfo = 0
            hasPackage = 0
            for comp, idx in jobSet:
                if comp[0].startswith('info-'):
                    hasInfo = 1
                if ':' not in comp[0]:
                    hasPackage = 1

            # we can't sort versions w/o timeStamps, so convert them to strings
            compJobSet = [((x[0][0], (str(x[0][1][0]), x[0][1][1]),
                           (str(x[0][2][0]), x[0][1][1]), x[0][3]), x[1])
                           for x in jobSet]
            cmpValue = (-isCritical, -hasInfo, -hasPackage, compJobSet)
            jobComp[tuple(jobSet)] = cmpValue

        jobSets.sort(key=lambda x: jobComp[tuple(x)])


    def _stronglyConnect(self, criticalJobs=None):
        # gets final job sets - ordered as necessary.

        # get sets of strongly connected components - each component has
        # a cycle where something at the beginning requires something at the
        # end.
        compSets = self.g.getStronglyConnectedComponents()

        # expand the job indexes to the actual jobs, so we can sort the
        # strongly connected components as we would if there were no
        # required ordering between them.  We'll use this preferred ordering to
        # help create a repeatable total ordering.
        # We sort them so that info- packages are first, then we sort them
        # alphabetically.
        jobSets = [ sorted((self.nodes[nodeIdx][0], nodeIdx)
                           for nodeIdx in idxSet) for idxSet in compSets ]

        # criticalJobSetsList will contain both the jobs and everything that 
        # needs to be updated with them to keep consistent ordering -
        # it is a orderd list of list of jobSets.
        criticalJobSetsList = self._getCriticalJobSets(jobSets, criticalJobs)
        self._orderJobSets(jobSets, criticalJobSetsList)

        # create index from nodeIdx -> jobSetIdx for creating a SCC graph.
        jobSetsByJob = {}
        for jobSetIdx, jobSet in enumerate(jobSets):
            for job, nodeIdx in jobSet:
                jobSetsByJob[nodeIdx] = jobSetIdx

        sccGraph = graph.DirectedGraph()
        for jobSetIdx, jobSet in enumerate(jobSets):
            sccGraph.addNode(jobSetIdx)
            for job, nodeIdx in jobSet:
                for childNodeIdx in self.g.iterChildren(nodeIdx):
                    childJobSetIdx = jobSetsByJob[childNodeIdx]
                    sccGraph.addEdge(jobSetIdx, childJobSetIdx)

        # create an ordering based on dependencies, and then, when forced
        # to choose between several choices, use the index order for jobSets
        # - that's the order we created by calling _orderJobSets above.
        
        # for debugging, remember, child nodes are ordered _after_ their 
        # parents, so expect groups to be leaf nodes.
        orderedComponents = sccGraph.getTotalOrdering(
                                    nodeSort=lambda a, b: cmp(a[1],  b[1]))
        orderedComponents = [ [y[0] for y in jobSets[x]] for x in orderedComponents ]
        criticalUpdates = []
        if criticalJobSetsList:
            # find out the last trove that needs to be updated for
            # this critical update to be complete.
            max = 0
            criticalUpdates = []
            for criticalJobSets in criticalJobSetsList:
                criticalJobSets = [ [x[0] for x in jobSet] for jobSet in criticalJobSets]
                for idx, jobSet in enumerate(orderedComponents):
                    if jobSet in criticalJobSets:
                        max = idx
                criticalUpdates.append(max)

        return (orderedComponents, criticalUpdates)

    def _createDepGraph(self, result, brokenByErase, satisfied,
                        linkedJobSets, criticalJobs, finalJobs, 
                        createCollectionEdges=False):
        # there are four kinds of edges -- old needs old, old needs new,
        # new needs new, and new needs old. Each edge carries a depId
        # to aid in cancelling them out. Our initial edge representation
        # is a simple set of edges.
        oldNewEdges, oldOldEdges, newNewEdges, newOldEdges = \
                    self._createDependencyEdges(result, self.depList)

        if createCollectionEdges:
            # Create dependencies from collections to the things they include.
            # This forces collections to be installed after all of their
            # elements.  We include weak references in case the intermediate
            # trove is not part of the update job.
            collectionEdges =  (self._createCollectionEdges())
        else:
            collectionEdges = []

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
        self._collapseEdges(oldOldEdges, oldNewEdges, newOldEdges, newNewEdges)

        linkedNodeLists = [self._getNodeListFromJobSet(x)
                           for x in linkedJobSets]
        if finalJobs:
            finalNodes  = self._getNodeListFromJobSet(finalJobs)
        else:
            finalNodes = []

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

        # for debugging, remember, child nodes are ordered _after_ their 
        # parents, so expect groups to be leaf nodes in the graph
        self._buildEdges(oldOldEdges, newNewEdges,
                         collectionEdges, linkedNodeLists, finalNodes, 
                         criticalJobs)
        del oldOldEdges
        del newNewEdges
        return self.g


    def _findOrdering(self, result, brokenByErase, satisfied, linkedJobSets,
                      criticalJobs, finalJobs):
        changeSetList = []
        self._createDepGraph(result, brokenByErase, satisfied, linkedJobSets,
                             criticalJobs, finalJobs,
                             createCollectionEdges=True)

        componentLists, criticalUpdates = self._stronglyConnect(criticalJobs)

        for componentList in componentLists:
            changeSetList.append(list(componentList))
        return changeSetList, criticalUpdates

    def _getNodeListFromJobSet(self, jobSet):
        # convert from jobSet -> list of nodes
        nodeList = []
        for job in jobSet:
            if job[1][0]:
                nodeId = self.oldInfoToNodeId[job[0], job[1][0], job[1][1]]
            else:
                nodeId = self.newInfoToNodeId[job[0], job[2][0], job[2][1]]
            nodeList.append(nodeId)
        return nodeList

    def iterNodes(self):
        # skips the None node on the front
        return [ x[0] for x in itertools.islice(self.nodes, 1, None) ]

    def addJobs(self, jobSet):
        # This sets up negative depNum entries for the requirements we're
        # checking (multiplier = -1 makes them negative), with (-1 * depNum)
        # indexing depList. depList is a list of (troveNum, depClass, dep)
        # tuples. Like for depNum, negative troveNum values mean the
        # dependency was part of a new trove.
        for job in jobSet:
            if job[2][0] is None:
                nodeId = self._addJob(job)
                self.workTables.removeTrove((job[0], job[1][0], job[1][1]), 
                                            nodeId)
            else:
                (provides, requires) = self.troveSource.getDepsForTroveList(
                                        [ (job[0], job[2][0], job[2][1]) ])[0]

                newNodeId = self._addJob(job)

                # this reduces the size of our tables by removing things
                # which this trove both provides and requires conary 1.0.11
                # and later remove these from troves at build time
                requires = requires - provides

                self.workTables._populateTmpTable(depList = self.depList,
                                                  troveNum = -newNodeId,
                                                  requires = requires,
                                                  provides = provides,
                                                  multiplier = -1)

                del provides, requires

                if job[1][0] is not None:
                    self.workTables.removeTrove((job[0], job[1][0], job[1][1]),
                                                newNodeId)

        # track the complete job set
        self.jobSet.update(jobSet)

        # merge everything into TmpDependencies, TmpRequires, and tmpProvides
        self.workTables.merge()
        self.workTables.mergeRemoves()

    def _check(self, findOrdering = False, linkedJobs = None,
              criticalJobs = None, finalJobs = None, createGraph = False):
        # dependencies which could have been resolved by something in
        # RemovedIds, but instead weren't resolved at all are considered
        # "unresolvable" dependencies. (they could be resolved by something
        # in the repository, but that something is being explicitly removed
        # and adding it back would be a bit rude!)
        stmt = """
                SELECT depId, depNum, reqInstanceId, Required.nodeId,
                       provInstanceId, Provided.nodeId
                    FROM
                        (%s) AS Resolved
                    LEFT OUTER JOIN RemovedTroveIds AS Required ON
                        reqInstanceId = Required.troveId
                    LEFT OUTER JOIN RemovedTroveIds AS Provided ON
                        provInstanceId = Provided.troveId
                """ % self._resolveStmt("TmpRequires",
                                        ("Provides", "TmpProvides"),
                                        ("Dependencies", "TmpDependencies"))
        self.cu.execute(stmt)

        # it's a shame we instantiate this, but merging _gatherResoltion
        # and _findOrdering doesn't seem like any fun
        result = self.cu.fetchall()

        # None in depList means the dependency got resolved; we track
        # would have been resolved by something which has been removed as
        # well

        # depNum is the dependency number
        #    negative ones are for dependencies being added (and they index
        #    depList); positive ones are for dependencies broken by an
        #    erase (and need to be looked up in the Requires table in the
        #    database to get a nice description)
        satisfied, brokenByErase, wasIn, unresolveable = \
                                self._gatherResolution(result)

        if linkedJobs is None:
            linkedJobs = set()
        if findOrdering:
            changeSetList, criticalUpdates = self._findOrdering(result,
                                               brokenByErase, satisfied,
                                               linkedJobSets = linkedJobs,
                                               criticalJobs = criticalJobs,
                                               finalJobs = finalJobs)
        else:
            changeSetList = []
            criticalUpdates = []
        if createGraph:
            depGraph = self._createDepGraph(result, brokenByErase, satisfied,
                                            linkedJobSets=linkedJobs,
                                            criticalJobs=criticalJobs,
                                            finalJobs=finalJobs)
        else:
            depGraph = None

        brokenByErase = set(brokenByErase)
        satisfied = set(satisfied)

        unsatisfiedList, unresolveableList = \
                self._gatherDependencyErrors(satisfied, brokenByErase,
                                                unresolveable,
                                                wasIn)

        return (unsatisfiedList, unresolveableList, changeSetList, depGraph, 
                criticalUpdates)

    def check(self, findOrdering = False, linkedJobs = None,
              criticalJobs = None, finalJobs = None):
        (unsatisfiedList, unresolveableList, changeSetList, depGraph,
         criticalUpdates) = \
                self._check(findOrdering=findOrdering, linkedJobs=linkedJobs,
                            createGraph=False, criticalJobs=criticalJobs,
                            finalJobs=finalJobs)
        return unsatisfiedList, unresolveableList, changeSetList, criticalUpdates

    def createDepGraph(self, linkedJobs=None):
        unsatisfiedList, unresolveableList, changeSetList, depGraph, criticalUpdates = \
                self._check(findOrdering=False, linkedJobs=linkedJobs,
                            createGraph=True)

        externalDepGraph = graph.DirectedGraph()
        for nodeId in depGraph.iterNodes():
            # translate from nodeId -> job for external consumption
            job = self.nodes[nodeId][0]
            externalDepGraph.addNode(job)
        for fromNode, toNode in depGraph.iterEdges():
            externalDepGraph.addEdge(self.nodes[fromNode][0], 
                                     self.nodes[toNode][0])

        return unsatisfiedList, unresolveableList, externalDepGraph

    def done(self):
        if self.inTransaction:
            self.db.rollback()
            self.inTransaction = False

    def __del__(self):
        self.done()

    def __init__(self, db, troveSource):
        self.g = graph.DirectedGraph()
        # adding None to the front prevents us from using nodeId's of 0, which
        # would be a problem since we use negative nodeIds in the SQL
        # to differentiate troves added by this job from troves already
        # present, and -1 * 0 == 0
        self.nodes = [ None ]
        self.newInfoToNodeId = {}
        self.oldInfoToNodeId = {}
        self.depList = [ None ]
        self.jobSet = set()
        self.db = db
        self.cu = self.db.cursor()
        self.troveSource = troveSource
        self.workTables = DependencyWorkTables(self.cu, removeTables = True)

        # this begins a transaction. we do this explicitly to keep from
        # grabbing any exclusive locks (when the python binding autostarts
        # a transaction, it uses "begin immediate" to grab an exclusive
        # lock right away. since we're only updating tmp tables, we don't
        # need a lock at all, but we'll live with a reserved lock since that's
        # the best we can do with sqlite and still get the performance benefits
        # of being in a transaction)
        self.cu.execute("BEGIN")
        self.inTransaction = True

class DependencyTables:
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
        self._add(cu, troveId, trove.getProvides(), trove.getRequires())

    def _add(self, cu, troveId, provides, requires):
        workTables = DependencyWorkTables(cu)

        workTables._populateTmpTable([], troveId, requires, provides)
        workTables.merge(intoDatabase = True)

    def delete(self, cu, troveId):
        schema.resetTable(cu, "suspectDepsOrig")
        schema.resetTable(cu, "suspectDeps")

        for tbl in ('Requires', 'Provides'):
            cu.execute("INSERT INTO suspectDepsOrig SELECT depId "
                       "FROM %s WHERE instanceId=%d" % (tbl, troveId))
            cu.execute("DELETE FROM %s WHERE instanceId=%d" % (tbl, troveId))

        cu.execute("INSERT INTO suspectDeps SELECT DISTINCT depId "
                   "FROM suspectDepsOrig")

        cu.execute("""
                DELETE FROM Dependencies WHERE depId IN
                (SELECT suspectDeps.depId FROM suspectDeps WHERE depId NOT IN
                    (SELECT distinct depId AS depId1 FROM Requires UNION
                     SELECT distinct depId AS depId1 FROM Provides))
                 """)

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

    def _restrictResolveByTrove(self, *args):
        """ Restricts deps to being solved by the given instanceIds or
            their children
        """
        # LEFT join in case the instanceId we're given is not included in any
        # troves on this host and we wish to match it.
        restrictJoin = """JOIN tmpInstances
                            ON (%(provides)s.instanceId = tmpInstances.instanceId)"""
        return restrictJoin, ''

    def _resolve(self, depSetList, selectTemplate, restrictor=None,
                 restrictBy=None):

        cu = self.db.cursor()
        workTables = DependencyWorkTables(cu)

	cu.execute("BEGIN")

        depList = [ None ]
        for i, depSet in enumerate(depSetList):
            workTables._populateTmpTable(depList, -i - 1,
                                         depSet, None, multiplier = -1)

        workTables.merge(skipProvides = True)

        full = selectTemplate % DependencyChecker._resolveStmt( "TmpRequires",
                                ("Provides",), ("Dependencies",),
                                restrictBy = restrictBy, restrictor = restrictor)
        cu.execute(full, start_transaction = False)

        return depList, cu

    def _addResult(self, depId, value, depList, depSetList, result):
        depSetId = -depList[depId][0] - 1
        depSet = depSetList[depSetId]
        result.setdefault(depSet, []).append(value)


    def resolve(self, label, depSetList, troveList=[]):
        """ Determine troves that provide the given dependencies,
            restricting by label and limiting to latest version for
            each (name, flavor) pair.
        """
        # dep set list must be unique and indexable.
        depSetList = list(set(depSetList))

        selectTemplate = """SELECT depNum, Items.item, Versions.version,
                             Nodes.timeStamps, flavor FROM
                            (%s) as DepsSelect
                          INNER JOIN Instances ON
                            provInstanceId = Instances.instanceId
                          INNER JOIN Items ON
                            Instances.itemId = Items.itemId
                          INNER JOIN Versions ON
                            Instances.versionId = Versions.versionId
                          INNER JOIN Flavors ON
                            Instances.flavorId = Flavors.flavorId
                          INNER JOIN Nodes ON
                            Instances.itemId = Nodes.itemId AND
                            Instances.versionId = Nodes.versionId
                          ORDER BY
                            Nodes.finalTimestamp DESC
                        """
        if troveList:
            # we need to extract the instanceids for the troves we
            # were passed in, plus the instanceIds of their included
            # troves
            cu = self.db.cursor()
            schema.resetTable(cu, "tmpInstances")
            schema.resetTable(cu, "tmpInstances2")
            cu.executemany("""
            INSERT INTO tmpInstances
                SELECT instanceId
            FROM Instances
            JOIN Items ON Instances.itemId = Items.itemId
            JOIN Versions ON Instances.versionId = Versions.versionId
            JOIN Flavors ON Instances.flavorId = Flavors.flavorId
            WHERE Items.item = ? AND Versions.version = ? AND Flavors.flavor = ?
            """, ( (n, v.asString(), f.freeze()) for (n, v, f)
                           in troveList), start_transaction = False )
            # now grab the instanceIds of their included troves, avoiding duplicates
            # and the instanceIds that already exist
            cu.execute("""
            INSERT INTO tmpInstances2
                SELECT DISTINCT TT.includedId
            FROM tmpInstances AS TI
            JOIN TroveTroves AS TT USING(instanceId)
            LEFT JOIN tmpInstances as haveTI ON
                TT.includedId = haveTI.instanceId
            WHERE haveTI.instanceId IS NULL
            """, start_transaction=False)
            cu.execute("INSERT INTO tmpInstances SELECT instanceId FROM tmpInstances2",
                       start_transaction=False)
            restrictBy = None
            restrictor = self._restrictResolveByTrove
        else:
            restrictBy = label.asString()
            restrictor = self._restrictResolveByLabel

        depList, cu = self._resolve(depSetList, selectTemplate,
                                    restrictBy = restrictBy,
                                    restrictor = restrictor)

        depSolutions = [ {} for x in xrange(len(depList)) ]

        for (depId, troveName, versionStr, timeStamps, flavorStr) in cu:
            depId = -depId

            # remember the first version for each troveName/flavorStr pair
            depSolutions[depId].setdefault((troveName, flavorStr),
                                           (versionStr, timeStamps))

        result = {}
        for depId, troveSet in enumerate(depSolutions):
            # we are adding elements in the order of depIds, which 
            # are ordered by dependency.  Thus, we should be guaranteed
            # that the return order of deps in a dependency set is consistent.
            # Note that some lists may be empty, they are still needed
            # so that the slot in which the results for a dep is returned 
            # is not dependendent on the current contents of a repository.
            if not depId:
                continue

            troveSet = [ (x[0][0],
                          versions.strToFrozen(x[1][0], x[1][1].split(":")),
                          x[0][1]) for x in troveSet.items() ]
            self._addResult(depId, troveSet, depList, depSetList, result)
        self.db.rollback()
        return result

    def _resolveToIds(self, depSetList, restrictor=None, restrictBy=None):
        """ Resolve dependencies, leaving the results as instanceIds
        """
        # dep set list must be unique and indexable.
        depSetList = list(set(depSetList))

        selectTemplate = """SELECT depNum, provInstanceId FROM (%s)"""
        depList, cu = self._resolve(depSetList, selectTemplate)

        result = {}
        depSolutions = [ [] for x in xrange(len(depList)) ]
        for depId, troveId in cu:
            depId = -depId
            depSolutions[depId].append(troveId)

        for depId, sols in enumerate(depSolutions):
            if not depId:
                continue
            self._addResult(depId, sols, depList, depSetList, result)

        self.db.rollback()
        return result

    def resolveToIds(self, depSetList):
        return self._resolveToIds(depSetList)


    def getLocalProvides(self, depSetList):
        # dep set list must be unique and indexable.
        from conary.local import sqldb
        flavorCache = sqldb.FlavorCache()
        versionCache = sqldb.VersionCache()
        depSetList = list(set(depSetList))

        cu = self.db.cursor()

        workTables = DependencyWorkTables(cu)

	cu.execute("BEGIN")

        depList = [ None ]
        for i, depSet in enumerate(depSetList):
            workTables._populateTmpTable(depList, -i - 1,
                                         depSet, None, multiplier = -1)

        workTables.merge(skipProvides = True)

        full = """SELECT depNum, troveName, Versions.version,
                         timeStamps, Flavors.flavor FROM
                        (%s) as Resolved
                      INNER JOIN Instances ON
                        provInstanceId = Instances.instanceId
                      INNER JOIN Versions USING(versionId)
                      INNER JOIN Flavors
                            ON (Instances.flavorId = Flavors.flavorId)
                    """ % DependencyChecker._resolveStmt( "TmpRequires",
                                ("Provides",), ("Dependencies",))

        cu.execute(full,start_transaction = False)

        depSolutions = [ [] for x in xrange(len(depList)) ]

        for (depId, troveName, versionStr, timeStamps, flavorStr) in cu:
            depId = -depId
            # remember the first version for each troveName/flavorStr pair
            v = versionCache.get(versionStr, timeStamps)
            f = flavorCache.get(flavorStr)
            depSolutions[depId].append((troveName, v, f))

        result = {}

        for depId, sols in enumerate(depSolutions):
            if not depId: continue
            if not sols: continue
            self._addResult(depId, sols, depList, depSetList, result)
        self.db.rollback()
        return result

    def __init__(self, db):
        self.db = db

class DependencyDatabase(DependencyTables):
    """ Creates a thin database (either on disk or in memory)
        for managing dependencies
    """
    def __init__(self, path=":memory:", driver="sqlite"):
	db = dbstore.connect(path, driver=driver, timeout=30000)
        db.loadSchema()
        schema.setupTempDepTables(db)
        schema.createDependencies(db)
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
