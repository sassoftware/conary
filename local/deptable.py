#
# Copyright (c) 2004-2005 Specifix, Inc.
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

from deps import deps

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
    cu.execute("CREATE INDEX %sIdx ON %s(class, name, flag)" % 
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

    def _createTmpTable(self, cu, name):
        cu.execute("""CREATE TEMPORARY TABLE %s(
                                              troveId INT,
                                              depNum INT,
                                              flagCount INT,
                                              isProvides BOOL,
                                              class INTEGER,
                                              name STRING,
                                              flag STRING)""" % name,
                   start_transaction = False)
        cu.execute("CREATE INDEX %sIdx ON %s(troveId, class, name, flag)"
                        % (name, name), start_transaction = False)

    def _populateTmpTable(self, cu, name, depList, troveNum, requires, 
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
                    if flags:
                        for (flag, sense) in flags:
                            # conary 0.12.0 had mangled flags; this check
                            # prevents them from making it into any repository
                            assert("'" not in flag)
                            assert(sense == deps.FLAG_SENSE_REQUIRED)
                            cu.execute("INSERT INTO %s VALUES(?, ?, ?, ?, "
                                                "?, ?, ?)" % name,
                                       (troveNum, multiplier * len(depList), 
                                        len(flags), isProvides, classId, 
                                        depName, flag),
                                       start_transaction = False)
                    else:
                        cu.execute(    "INSERT INTO %s VALUES(?, ?, ?, ?, "
                                                "?, ?, ?)" % name,
                                       (troveNum, multiplier * len(depList), 
                                        1, isProvides, classId, 
                                        depName, NO_FLAG_MAGIC),
                                       start_transaction = False)

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
            if tblName == 'Provides':
                classClause = "AND class != %d" % deps.DEP_CLASS_TROVES
            else:
                classClause = ""

            cu.execute("SELECT class, name, flag FROM %s NATURAL JOIN "
                       "Dependencies WHERE instanceId=? %s ORDER BY class, name"
                    % (tblName, classClause), troveId)

            last = None
            flags = []
            depSet = deps.DependencySet()
            for (classId, name, flag) in cu:
                if (classId, name) == last:
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
        self._createTmpTable(cu, "NeededDeps")

        prov = trove.getProvides()

        self._populateTmpTable(cu, "NeededDeps", [], troveId, 
                               trove.getRequires(), prov)

        cu.execute("INSERT INTO NeededDeps VALUES(?, ?, ?, ?, ?, ?, ?)",
                       (troveId, 1, 1, 
                        1, deps.DEP_CLASS_TROVES, 
                        trove.getName(), NO_FLAG_MAGIC), 
                        start_transaction = False)
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

    def _resolveStmt(self, requiresTable, providesTableList, depTableList,
                     providesLabel = None):
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
                         FROM %(requires)s JOIN %(provides)s ON
                              %(requires)s.depId = %(provides)s.depId
""" % substTable

            if providesLabel:
                subselect += """\
                           JOIN Instances ON
                              %(provides)s.instanceId = Instances.instanceId
                           JOIN Nodes ON
                              Instances.itemId = Nodes.itemId AND
                              Instances.versionId = Nodes.versionId
                           JOIN LabelMap ON
                              LabelMap.itemId = Nodes.itemId AND
                              LabelMap.branchId = Nodes.branchId
                           JOIN Labels ON
                              Labels.labelId = LabelMap.labelId
""" % substTable

            subselect += """\
%(depClause)s""" % substTable
            
            if providesLabel:
                subselect += """\
                            WHERE 
                              Labels.label = '%s'
""" % providesLabel

        return """
                SELECT depCheck.depNum as depNum,
                       Matched.reqInstId as reqInstanceId,
                       Matched.provInstId as provInstanceId
                    FROM (
%s                       ) AS Matched
                    JOIN DepCheck ON
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

    def check(self, changeSet):
        def _depItemsToSet(depInfoList):
            failedSets = [ (x, None, None) for x in troveNames]

            for depInfo in depInfoList:
                if depInfo is not None:
                    (troveIndex, classId, dep) = depInfo

                    if classId in [ deps.DEP_CLASS_ABI ]:
                        continue

                    missingDeps = True
                    troveIndex = -(troveIndex + 1)

                    if failedSets[troveIndex][2] is None:
                        failedSets[troveIndex] = (failedSets[troveIndex][0],
                                                  failedSets[troveIndex][1],
                                                  deps.DependencySet())
                    failedSets[troveIndex][2].addDep(
                                    deps.dependencyClasses[classId], dep)

            failedList = []
            for (name, classId, depSet) in failedSets:
                if depSet is not None:
                    failedList.append((name, depSet))

            return failedList

        def _brokenItemsToSet(cu, depIdList):
            # this only works for databases (not repositories)
            if not depIdList: return []

            cu.execute("CREATE TEMPORARY TABLE BrokenDeps (depNum INTEGER)",
                       start_transaction = False)
            for depNum in depIdList:
                cu.execute("INSERT INTO BrokenDeps VALUES (?)", depNum,
                           start_transaction = False)

            cu.execute("""
                    SELECT DISTINCT troveName, class, name, flag FROM 
                        BrokenDeps JOIN Requires ON 
                            BrokenDeps.depNum = Requires.DepNum
                        JOIN Dependencies ON
                            Requires.depId = Dependencies.depId
                        JOIN DBInstances ON
                            Requires.instanceId = DBInstances.instanceId
                """, start_transaction = False)

            failedSets = {}
            for (troveName, depClass, depName, flag) in cu:
                if not failedSets.has_key(troveName):
                    failedSets[troveName] = deps.DependencySet()

                if flag == NO_FLAG_MAGIC:
                    flags = []
                else:
                    flags = [ (flag, deps.FLAG_SENSE_REQUIRED) ]

                failedSets[troveName].addDep(deps.dependencyClasses[depClass],
                            deps.Dependency(depName, flags))

            cu.execute("DROP TABLE BrokenDeps", start_transaction = False)

            return failedSets.items()

        # this works against a database, not a repository
        cu = self.db.cursor()

        self._createTmpTable(cu, "DepCheck")
        createDepTable(cu, 'TmpDependencies', isTemp = True)
        createProvidesTable(cu, 'TmpProvides', isTemp = True)
        createRequiresTable(cu, 'TmpRequires', isTemp = True)
    
        # build the table of all the requirements we're looking for
        depList = [ None ]
        oldTroves = []
        troveNames = []
        for i, trvCs in enumerate(changeSet.iterNewPackageList()):
            troveNames.append((trvCs.getName()))
            self._populateTmpTable(cu, "DepCheck", depList, -i - 1, 
                                   trvCs.getRequires(), trvCs.getProvides(),
                                   multiplier = -1)

            if trvCs.getOldVersion():
                oldTroves.append((trvCs.getName(), trvCs.getOldVersion(),
                                  trvCs.getOldFlavor()))

            # using depNum 0 is a hack, but it's just on a provides so
            # it shouldn't matter
            cu.execute("INSERT INTO DepCheck VALUES(?, ?, ?, ?, ?, ?, ?)",
                       (-i - 1, 0, 1, True, deps.DEP_CLASS_TROVES, 
                        trvCs.getName(), NO_FLAG_MAGIC), 
                       start_transaction = False)

        # now build a table of all the troves which are being erased
        cu.execute("""CREATE TEMPORARY TABLE RemovedTroveIds 
                        (troveId INTEGER PRIMARY KEY)""", 
                    start_transaction = False)

        oldTroves += changeSet.getOldPackageList()

        if oldTroves:
            cu.execute("""CREATE TEMPORARY TABLE RemovedTroves 
                            (name STRING, version STRING, flavor STRING)""",
                       start_transaction = False)
            for (name, version, flavor) in oldTroves:
                if flavor:
                    flavor = flavor.freeze()
                else:
                    flavor = None

                cu.execute("INSERT INTO RemovedTroves VALUES(?, ?, ?)",
                           (name, version.asString(), flavor), 
                           start_transaction = False)

            cu.execute("""INSERT INTO RemovedTroveIds 
                            SELECT instanceId FROM
                                RemovedTroves JOIN Versions ON
                                RemovedTroves.version = Versions.version
                            JOIN DBFlavors ON
                                RemovedTroves.flavor = DBFlavors.flavor OR
                                (RemovedTroves.flavor is NULL AND
                                 DBFlavors.flavor is NULL)
                            JOIN DBInstances ON
                                DBInstances.troveName = RemovedTroves.name AND
                                DBInstances.versionId = Versions.versionId AND
                                DBInstances.flavorId  = DBFlavors.flavorId""",
                        start_transaction = False)
            cu.execute("DROP TABLE RemovedTroves", start_transaction = False)

        self._mergeTmpTable(cu, "DepCheck", "TmpDependencies", "TmpRequires",
                            "TmpProvides", 
                            ("Dependencies", "TmpDependencies"), 
                            multiplier = -1)

        # check the dependencies for anything which depends on things which
        # we've removed
        cu.execute("""
                INSERT INTO TmpRequires SELECT 
                    DISTINCT Requires.instanceId, Requires.depId, 
                             Requires.depNum, Requires.depCount
                FROM RemovedTroveIds JOIN Provides ON
                    RemovedTroveIds.troveId == Provides.instanceId
                JOIN Requires ON
                    Provides.depId = Requires.depId
        """, start_transaction = False)

        cu.execute("""
                INSERT INTO DepCheck SELECT
                    Requires.instanceId, Requires.depNum,
                    Requires.DepCount, 0, Dependencies.class,
                    Dependencies.name, Dependencies.flag
                FROM RemovedTroveIds JOIN Provides ON
                    RemovedTroveIds.troveId == Provides.instanceId
                JOIN Requires ON
                    Provides.depId = Requires.depId
                JOIN Dependencies ON
                    Dependencies.depId == Requires.depId
        """, start_transaction = False)

        # dependencies which could have been resolved by something in
        # RemovedIds, but instead weren't resolved at all are considered
        # "unresolvable" dependencies. (they could be resolved by something
        # in the repository, but that something is being explicitly removed
        # and adding it back would be a bit rude!)
        cu.execute("""
                SELECT depNum, RemovedTroveIds.troveId FROM
                    (%s) 
                    LEFT OUTER JOIN RemovedTroveIds ON
                        provInstanceId == RemovedTroveIds.troveId
                    LEFT OUTER JOIN RemovedTroveIds AS Removed ON
                        reqInstanceId == Removed.troveId
                    WHERE 
                        Removed.troveId IS NULL
                """ % self._resolveStmt("TmpRequires",
                                        ("Provides", "TmpProvides"),
                                        ("Dependencies", "TmpDependencies"))
                , start_transaction = False)

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
        brokenByErase = {}
        unresolveable = [ None ] * (len(depList) + 1)
        satisfied = []
        for (depNum, removedInstanceId) in cu:
            if removedInstanceId is not None:
                if depNum < 0:
                    # the dependency would have been resolved, but this
                    # change set removes what would have resolved it
                    unresolveable[-depNum] = True
                else:
                    # this change set removes something which is needed
                    # by something else on the system w/o providing a
                    # replacement
                    brokenByErase[depNum] = True
            else:
                # if we get here, the dependency is resolved; mark it as
                # resolved by clearing it's entry in depList
                if depNum < 0:
                    depList[-depNum] = None
                else:
                    # if depNum > 0, this was a dependency which was checked
                    # because of something which is being removed, but it
                    # remains satisfied
                    satisfied.append(depNum)

        # things which are listed in satisfied should be removed from
        # brokenByErase; they are dependencies that were broken, but are
        # resolved by something else
        for depNum in satisfied:
            if brokenByErase.has_key(depNum):
                del brokenByErase[depNum]

        # sort things out of unresolveable which were resolved by something
        # else
        for depNum in range(len(unresolveable)):
            if unresolveable[depNum] is None:
                pass
            elif depList[depNum] is None:
                unresolveable[depNum] = None
            else:
                unresolveable[depNum] = depList[depNum]
                # we handle this as unresolveable; we don't need it in
                # depList any more
                depList[depNum] = None

        failedList = _depItemsToSet(depList)
        unresolveableList = _depItemsToSet(unresolveable)
        unresolveableList += _brokenItemsToSet(cu, brokenByErase.keys())

        # no need to drop the DepCheck table since we're rolling this whole
        # transaction back anyway
        cu.execute("DROP TABLE TmpDependencies", start_transaction= False)
        cu.execute("DROP TABLE TmpRequires", start_transaction= False)
        cu.execute("DROP TABLE TmpProvides", start_transaction= False)
        cu.execute("DROP TABLE DepCheck", start_transaction = False)
        cu.execute("DROP TABLE RemovedTroveIds", start_transaction = False)

        assert(not self.db.inTransaction)

        return (failedList, unresolveableList)

    def resolve(self, label, depSetList):
        cu = self.db.cursor()

        self._createTmpTable(cu, "DepCheck")
        createDepTable(cu, 'TmpDependencies', isTemp = True)
        createRequiresTable(cu, 'TmpRequires', isTemp = True)

        depList = [ None ]
        for i, depSet in enumerate(depSetList):
            self._populateTmpTable(cu, "DepCheck", depList, -i - 1, 
                                   depSet, None, multiplier = -1)


        self._mergeTmpTable(cu, "DepCheck", "TmpDependencies", "TmpRequires",
                            None, ("Dependencies", "TmpDependencies"), 
                            multiplier = -1)

        full = """SELECT depNum, Items.item, Versions.version, flavor FROM 
                        (%s)
                      JOIN Instances ON
                        provInstanceId == Instances.instanceId
                      JOIN Items ON
                        Instances.itemId == Items.itemId
                      JOIN Versions ON
                        Instances.versionId == Versions.versionId
                      JOIN Flavors ON
                        Instances.flavorId == Flavors.flavorId
                      JOIN Nodes ON
                        Instances.itemId == Nodes.itemId AND
                        Instances.versionId == Nodes.versionId
                      ORDER BY
                        Nodes.finalTimestamp DESC
                    """ % self._resolveStmt( "TmpRequires", 
                                ("Provides",), ("Dependencies",),
                                providesLabel = label.asString())
                    
        cu.execute(full,start_transaction = False)

        # this depends intimately on things being sorted newest to oldest

        depSolutions = [ {} ] * len(depList)
        troveNameSolutions = {}
        solutionCount = {}

        saw = {}
        for (depId, troveName, versionStr, flavorStr) in cu:
            depId = -depId

            # only remember the first (newest) version of each trove for
            # a particular flavor
            sawVersion = saw.setdefault((troveName, flavorStr), versionStr)
            if sawVersion != versionStr:
                continue

            d = depSolutions[depId].setdefault(troveName, {}) 
            d[versionStr, flavorStr] = True

            if not troveNameSolutions.has_key((troveName, depId)):
                troveNameSolutions[(troveName, depId)] = True
                solutionCount.setdefault(troveName, 0)
                solutionCount[troveName] += 1

        result = {}

        #import lib
        #lib.epdb.st()

        for depId, troveNames in enumerate(depSolutions):
            if depId == 0: continue
            if not troveNames: 
                # no solutions for this depId
                continue

            countList = []
            for troveName in troveNames:
                countList.append((solutionCount[troveName], troveName))
            countList.sort()

            troveName = countList[-1][1]

            # XXX
            versionStr = troveNames[troveName].keys()[0][0]

            depNum = depList[depId][0]
            depSet = depSetList[depNum]
            l = result.get(depSet, None)
            if not l:
                result[depSet] = [ (troveName, versionStr) ]
            elif (troveName, versionStr) not in l:
                l.append((troveName, versionStr))

        cu.execute("DROP TABLE TmpDependencies", start_transaction= False)
        cu.execute("DROP TABLE TmpRequires", start_transaction= False)
        cu.execute("DROP TABLE DepCheck", start_transaction = False)

        assert(not self.db.inTransaction)

        return result

    def __init__(self, db):
        self.db = db
        DepTable(db, "Dependencies")
        DepProvides(db, 'Provides')
        DepRequires(db, 'Requires')
